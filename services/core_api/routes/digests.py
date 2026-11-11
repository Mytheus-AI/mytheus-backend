"""Weekly "what changed for you" digest routes (polish wave 3). The builder is `digest.py`.

The caller's own digests only:
- `GET /api/v1/digests/latest` -> `{digest | null, next_run_at}`
- `GET /api/v1/digests/preview`: this week so far, built now and not stored (`id: null`)
- `GET /api/v1/digests?limit=`: past weeks, newest first
- `GET /api/v1/digests/{id}`
- `GET|PUT /api/v1/digests/preferences` `{weekly_enabled, email_enabled}` (defaults: on, no
  email). Turning the digest off turns its email off too.

`POST /internal/digests/weekly` `{week_start}` (workflow-service, schedule `weekly-digest`,
Mondays 07:00 UTC, for the week that just ended): for every active person in the tenant whose
digest is on, build it and store it (`UNIQUE (user_id, week_start)`: a re-run builds nothing
new). A NEW digest with something in it gets one in-app notification
(`source_event_id = weekly-digest:{id}`, link `/digest`) and, if the person turned on digest
email, one email through notification-service `POST /internal/email/user` after the commit
(idempotency key `weekly-digest:{week_start}`). `email_status` records `sent`, `failed`,
`skipped` / `not_configured` (no provider on this server: the "not connected" state), or
`unavailable` (notification-service unreachable). A quiet week is stored (the page says so) but
sends nothing.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api.digest import (
    WeeklyDigest,
    build_digest,
    next_run_at,
    render_email,
    week_start_of,
)
from services.core_api.notification_client import (
    NotConfigured,
    NotificationServiceError,
    get_notifications,
)
from services.core_api.routes.internal_lifecycle import workflow_caller

logger = logging.getLogger(__name__)

router = APIRouter(tags=["digest"])

PRODUCER = "core-api"
MAX_LIST = 52


class DigestLatest(BaseModel):
    digest: WeeklyDigest | None = None
    next_run_at: datetime


class DigestSummary(BaseModel):
    id: uuid.UUID
    week_start: date
    item_count: int
    created_at: datetime


class DigestList(BaseModel):
    items: list[DigestSummary]


class DigestPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    weekly_enabled: bool = True
    email_enabled: bool = False


class WeeklyRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    week_start: date


class WeeklyRunResult(BaseModel):
    week_start: date
    built: int
    notified: int
    emailed: int
    skipped: int


def _stored(row: Any) -> WeeklyDigest:
    content = row["content"] if isinstance(row["content"], dict) else json.loads(row["content"])
    digest = WeeklyDigest.model_validate(content)
    digest.id = row["id"]
    digest.created_at = row["created_at"]
    return digest


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


async def _prefs(session: AsyncSession, user_id: uuid.UUID) -> DigestPreferences:
    row = (
        await session.execute(
            text("SELECT weekly_enabled, email_enabled FROM digest_preferences WHERE user_id = :u"),
            {"u": str(user_id)},
        )
    ).first()
    if row is None:
        return DigestPreferences()
    return DigestPreferences(weekly_enabled=row[0], email_enabled=row[1])


@router.get("/api/v1/digests/preferences", response_model=DigestPreferences)
async def get_preferences(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DigestPreferences:
    return await _prefs(session, _me(ctx))


@router.put("/api/v1/digests/preferences", response_model=DigestPreferences)
async def put_preferences(
    body: DigestPreferences,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DigestPreferences:
    user_id = _me(ctx)
    email = body.email_enabled and body.weekly_enabled
    await session.execute(
        text(
            "INSERT INTO digest_preferences (user_id, org_id, weekly_enabled, email_enabled) "
            "VALUES (:u, :o, :w, :e) ON CONFLICT (user_id) DO UPDATE SET "
            "weekly_enabled = EXCLUDED.weekly_enabled, email_enabled = EXCLUDED.email_enabled, "
            "updated_at = now()"
        ),
        {"u": str(user_id), "o": str(ctx.org_id), "w": body.weekly_enabled, "e": email},
    )
    return DigestPreferences(weekly_enabled=body.weekly_enabled, email_enabled=email)


@router.get("/api/v1/digests/latest", response_model=DigestLatest)
async def latest_digest(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DigestLatest:
    user_id = _me(ctx)
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, content, created_at FROM weekly_digests WHERE user_id = :u "
                    "ORDER BY week_start DESC LIMIT 1"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return DigestLatest(
        digest=_stored(row) if row else None, next_run_at=next_run_at(datetime.now(UTC))
    )


@router.get("/api/v1/digests/preview", response_model=WeeklyDigest)
async def preview_digest(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> WeeklyDigest:
    now = datetime.now(UTC)
    return await build_digest(session, _me(ctx), week_start_of(now.date()), until=now)


@router.get("/api/v1/digests", response_model=DigestList)
async def list_digests(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIST)] = 12,
) -> DigestList:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, week_start, item_count, created_at FROM weekly_digests "
                    "WHERE user_id = :u ORDER BY week_start DESC LIMIT :lim"
                ),
                {"u": str(_me(ctx)), "lim": limit},
            )
        )
        .mappings()
        .all()
    )
    return DigestList(items=[DigestSummary(**dict(r)) for r in rows])


@router.get("/api/v1/digests/{digest_id}", response_model=WeeklyDigest)
async def get_digest(
    digest_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> WeeklyDigest:
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, content, created_at FROM weekly_digests "
                    "WHERE id = :id AND user_id = :u"
                ),
                {"id": str(digest_id), "u": str(_me(ctx))},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return _stored(row)


# --- the weekly run ------------------------------------------------------------------------


async def _send_email(org_id: uuid.UUID, user_id: uuid.UUID, digest: WeeklyDigest) -> str:
    subject, body, html = render_email(digest, os.environ.get("APP_BASE_URL", ""))
    try:
        result = await get_notifications().send_user_email(
            org_id,
            user_id=user_id,
            kind="weekly_digest",
            idempotency_key=f"weekly-digest:{digest.week_start.isoformat()}",
            subject=subject,
            text=body,
            html=html,
        )
    except NotConfigured:
        return "not_configured"
    except NotificationServiceError:
        logger.warning("digest email: notification-service unavailable")
        return "unavailable"
    results = result.get("results") or []
    if not result.get("configured"):
        return "not_configured"
    return str(results[0].get("status")) if results else "failed"


@router.post("/internal/digests/weekly", response_model=WeeklyRunResult, tags=["core-internal"])
async def weekly_run(
    body: WeeklyRun,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> WeeklyRunResult:
    if body.week_start.weekday() != 0:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "week_start must be a Monday")
    if body.week_start > week_start_of(datetime.now(UTC).date()):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "week_start is in the future")
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        people = (
            await session.execute(
                text(
                    "SELECT u.id, COALESCE(p.weekly_enabled, true), "
                    "COALESCE(p.email_enabled, false) FROM users u "
                    "LEFT JOIN digest_preferences p ON p.user_id = u.id "
                    "WHERE u.deleted_at IS NULL ORDER BY u.id"
                )
            )
        ).all()
    built = notified = emailed = skipped = 0
    for user_id, weekly, email in people:
        if not weekly:
            skipped += 1
            continue
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            digest = await build_digest(session, user_id, body.week_start)
            created = (
                await session.execute(
                    text(
                        "INSERT INTO weekly_digests (org_id, user_id, week_start, content, "
                        "item_count) VALUES (:o, :u, :w, CAST(:c AS jsonb), :n) "
                        "ON CONFLICT (user_id, week_start) DO NOTHING RETURNING id"
                    ),
                    {
                        "o": str(org_id),
                        "u": str(user_id),
                        "w": body.week_start,
                        "c": digest.model_dump_json(exclude={"id", "created_at"}),
                        "n": digest.item_count,
                    },
                )
            ).scalar()
            if created is None:
                skipped += 1
                continue
            built += 1
            if digest.item_count == 0:
                continue
            digest.id = created
            await enqueue(
                session,
                EventEnvelope(
                    topic="notifications",
                    org_id=str(org_id),
                    producer=PRODUCER,
                    idempotency_key=f"notif:weekly-digest:{created}:in_app:{user_id}",
                    payload={
                        "source_event_id": f"weekly-digest:{created}",
                        "channel": "in_app",
                        "user_id": str(user_id),
                        "priority": 3,
                        "title": "What changed for you this week",
                        "body": _summary(digest),
                        "link": "/digest",
                    },
                ),
            )
            notified += 1
        if email:
            status = await _send_email(org_id, user_id, digest)
            async with get_sessionmaker()() as session, session.begin():
                await set_rls(session, org_id)
                await session.execute(
                    text("UPDATE weekly_digests SET email_status = :s WHERE id = :id"),
                    {"s": status, "id": str(created)},
                )
            if status == "sent":
                emailed += 1
    return WeeklyRunResult(
        week_start=body.week_start,
        built=built,
        notified=notified,
        emailed=emailed,
        skipped=skipped,
    )


def _summary(d: WeeklyDigest) -> str:
    parts = []
    for n, one, many in (
        (len(d.decisions), "decision", "decisions"),
        (len(d.decision_changes), "decision changed", "decisions changed"),
        (len(d.actions_assigned), "new task", "new tasks"),
        (len(d.actions_closed), "task closed", "tasks closed"),
        (len(d.promises_due), "promise due", "promises due"),
    ):
        if n:
            parts.append(f"{n} {one if n == 1 else many}")
    return (", ".join(parts) + ".") if parts else "A quiet week."
