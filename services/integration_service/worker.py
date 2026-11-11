"""integration-service worker: drains the webhook inbox and renews provider subscriptions.

    python -m services.integration_service.worker

Inbox. Every `pending` row is processed in its own transaction with `app.current_org` set to
the row's tenant (`FOR UPDATE SKIP LOCKED`, so two workers never take the same row). A row ends
`processed` or `skipped`; a failure rolls the work back and records the attempt in a separate
transaction, and after `MAX_ATTEMPTS` the row is parked as `failed` rather than retried forever.
A non-retryable answer from core-api (an action the tenant dismissed, say) parks it at once.

Renewals. Google push channels last ~7 days and are re-watched when they expire within a day;
Jira dynamic webhooks last 30 days and are refreshed when they expire within five. Run every
`RENEW_EVERY` seconds.

Tenants are enumerated from `organizations` the way billing's nightly job does it (the
application role may read its `id` column); everything after that is RLS-bound per tenant.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.metrics import serve_metrics, set_service
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError, get_clients
from services.integration_service.connectors import calendar, jira, slack
from services.integration_service.core_client import CoreApiClient, CoreApiError, get_core
from services.integration_service.redaction import get_logger, install_redaction, redact_text
from services.integration_service.storage import AttachmentStore, store_from_env

logger = get_logger(__name__)

MAX_ATTEMPTS = 5
BATCH = 50
POLL_SECONDS = 2.0
RENEW_EVERY = 1800.0
CHANNEL_RENEW_WINDOW = timedelta(days=1)
JIRA_RENEW_WINDOW = timedelta(days=5)


async def _org_ids() -> list[uuid.UUID]:
    async with get_sessionmaker()() as session:
        rows = (
            await session.execute(text("SELECT id FROM organizations WHERE deleted_at IS NULL"))
        ).scalars()
        return [uuid.UUID(str(r)) for r in rows]


async def _dispatch(
    session: Any,
    org_id: uuid.UUID,
    row: Any,
    clients: Clients,
    core: CoreApiClient,
    attachments: AttachmentStore | None,
) -> str:
    provider, payload = row["provider"], dict(row["payload"])
    if provider == config.GOOGLE_CALENDAR:
        return await calendar.process(
            session, org_id, payload, row["event_type"], clients, attachments
        )
    if provider == config.SLACK:
        return await slack.process(session, payload, org_id=org_id, clients=clients, core=core)
    if provider == config.JIRA:
        return await jira.process_webhook(session, org_id, payload, core, clients)
    return "skipped"


async def process_row(
    org_id: uuid.UUID,
    row_id: uuid.UUID,
    *,
    clients: Clients | None = None,
    core: CoreApiClient | None = None,
    attachments: AttachmentStore | None = None,
) -> str | None:
    """Process one inbox row. Returns its new status, or None if another worker holds it or it
    is no longer pending."""
    clients = clients or get_clients()
    core = core or get_core()
    sessionmaker = get_sessionmaker()
    try:
        async with sessionmaker() as session, session.begin():
            await set_rls(session, org_id)
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT id, provider, event_type, payload, attempts "
                            "FROM integration_webhook_inbox WHERE id = :id AND status = 'pending' "
                            "FOR UPDATE SKIP LOCKED"
                        ),
                        {"id": str(row_id)},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            outcome = await _dispatch(session, org_id, row, clients, core, attachments)
            await session.execute(
                text(
                    "UPDATE integration_webhook_inbox SET status = :s, processed_at = now(), "
                    "attempts = attempts + 1, last_error = NULL WHERE id = :id"
                ),
                {"s": outcome, "id": str(row_id)},
            )
            return outcome
    except (ProviderError, CoreApiError, store.NotConnected) as exc:
        park = isinstance(exc, store.NotConnected) or (
            isinstance(exc, CoreApiError) and not exc.retryable
        )
        return await _record_failure(org_id, row_id, exc, park=park)
    except Exception as exc:
        logger.exception("inbox row %s failed", row_id)
        return await _record_failure(org_id, row_id, exc, park=False)


async def _record_failure(
    org_id: uuid.UUID, row_id: uuid.UUID, exc: Exception, *, park: bool
) -> str:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        status = (
            await session.execute(
                text(
                    "UPDATE integration_webhook_inbox SET attempts = attempts + 1, "
                    "last_error = :e, status = CASE WHEN :park OR attempts + 1 >= :max "
                    "THEN 'failed' ELSE 'pending' END WHERE id = :id RETURNING status"
                ),
                {
                    "e": redact_text(f"{type(exc).__name__}: {exc}")[:500],
                    "park": park,
                    "max": MAX_ATTEMPTS,
                    "id": str(row_id),
                },
            )
        ).scalar()
    return str(status or "failed")


async def drain_once(
    *,
    clients: Clients | None = None,
    core: CoreApiClient | None = None,
    attachments: AttachmentStore | None = None,
    org_ids: list[uuid.UUID] | None = None,
) -> dict[str, int]:
    summary: dict[str, int] = {}
    for org_id in org_ids if org_ids is not None else await _org_ids():
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            ids = (
                (
                    await session.execute(
                        text(
                            "SELECT id FROM integration_webhook_inbox WHERE status = 'pending' "
                            "ORDER BY received_at LIMIT :n"
                        ),
                        {"n": BATCH},
                    )
                )
                .scalars()
                .all()
            )
        for row_id in ids:
            outcome = await process_row(
                org_id, row_id, clients=clients, core=core, attachments=attachments
            )
            if outcome:
                summary[outcome] = summary.get(outcome, 0) + 1
    return summary


async def renew_once(
    *, clients: Clients | None = None, org_ids: list[uuid.UUID] | None = None
) -> int:
    clients = clients or get_clients()
    renewed = 0
    now = datetime.now(UTC)
    for org_id in org_ids if org_ids is not None else await _org_ids():
        try:
            async with get_sessionmaker()() as session, session.begin():
                await set_rls(session, org_id)
                due = (
                    await session.execute(
                        text(
                            "SELECT c.kind, c.connection_id FROM integration_cursors c "
                            "JOIN integration_connections k ON k.id = c.connection_id "
                            "WHERE k.status = 'active' AND c.expires_at IS NOT NULL AND ("
                            "(c.kind = :ch AND c.expires_at < :ch_by) OR "
                            "(c.kind = :site AND c.expires_at < :site_by))"
                        ),
                        {
                            "ch": calendar.CHANNEL_KIND,
                            "ch_by": now + CHANNEL_RENEW_WINDOW,
                            "site": jira.SITE_KIND,
                            "site_by": now + JIRA_RENEW_WINDOW,
                        },
                    )
                ).all()
                for kind, _connection_id in due:
                    provider = (
                        config.GOOGLE_CALENDAR if kind == calendar.CHANNEL_KIND else config.JIRA
                    )
                    conn = await store.get_connection(session, provider, active_only=True)
                    if conn is None:
                        continue
                    if kind == calendar.CHANNEL_KIND:
                        await calendar.watch(session, conn, clients)
                    else:
                        await jira.renew(session, conn, clients)
                    renewed += 1
        except (ProviderError, store.NotConnected) as exc:
            logger.warning("renewal failed for org %s: %s", org_id, exc)
    return renewed


async def run() -> None:  # pragma: no cover - thin loop over drain_once/renew_once
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    install_redaction()
    set_service("integration-service-worker")
    serve_metrics()
    attachments = store_from_env()
    if attachments is None:
        logger.info("S3_* not configured: calendar attachments are recorded, not ingested")
    last_renew = 0.0
    loop = asyncio.get_running_loop()
    while True:
        try:
            await drain_once(attachments=attachments)
            if loop.time() - last_renew > RENEW_EVERY:
                await renew_once()
                last_renew = loop.time()
        except Exception:
            logger.exception("integration-service worker iteration failed")
        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run())
