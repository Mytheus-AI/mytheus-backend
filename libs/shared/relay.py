"""Transactional-outbox relay: the single sanctioned producer path.

Polls unpublished outbox rows (FOR UPDATE SKIP LOCKED so replicas do not double-publish),
Avro-serializes each row's value against {topic}-value, produces keyed by org_id (acks=all,
idempotent), then marks the row published. At-least-once to the bus: a crash after produce but
before mark re-publishes on restart, which the consumer side dedupes.

`relay_once` is decoupled from Kafka specifics via a small producer protocol so it is unit
testable with a fake producer. The concrete confluent-kafka producer lives in
services/relay/main.py.

RLS note. This drains rows across every tenant, which the fail-closed `tenant_isolation`
policy forbids by default: with no `app.current_org` set, the policy matches zero rows and
this function reports an empty queue forever (defect D4). The fix is NOT a BYPASSRLS role.
Section 3.3 gives `mytheus_relay` one permissive `outbox_relay_drain` policy on `outbox`
alone, so cross-tenant reach is scoped to a single table and stays inspectable in
`pg_policies`. Until that lands, this runs as a role that bypasses RLS by accident, and
the drain is unverified rather than proven.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.metrics import record_outbox

logger = logging.getLogger(__name__)

# How many times a row may fail to serialize or produce before it is dead-lettered. Low on
# purpose: these failures are almost always deterministic (a schema mismatch), so retrying
# many times only delays the DLQ while the row holds up its partition key's ordering.
MAX_ATTEMPTS = 5

_COUNT_PENDING = text("SELECT count(*) FROM outbox WHERE published_at IS NULL")

_SELECT_PENDING = text(
    "SELECT id, topic, partition_key, schema_subject, payload, attempts "
    "FROM outbox WHERE published_at IS NULL "
    "ORDER BY created_at "
    "FOR UPDATE SKIP LOCKED "
    "LIMIT :limit"
)

_MARK_PUBLISHED = text("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)")

_BUMP_ATTEMPTS = text("UPDATE outbox SET attempts = attempts + 1 WHERE id = ANY(:ids)")

# Dead-lettered rows are marked published so they stop being re-selected. `attempts` is what
# distinguishes them from a clean publish, which is why it must actually be written.
_MARK_DEAD_LETTERED = text(
    "UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = ANY(:ids)"
)


class RelayProducer(Protocol):
    def serialize(self, subject: str, value: dict[str, Any]) -> bytes: ...
    def produce(self, *, topic: str, key: bytes, value: bytes) -> None: ...
    def flush(self) -> int: ...  # returns messages still in-queue (0 == all delivered)


async def relay_once(session: AsyncSession, producer: RelayProducer, *, limit: int = 500) -> int:
    """Drain one batch of unpublished outbox rows. Returns the number published.

    The SELECT ... FOR UPDATE SKIP LOCKED and the UPDATE run in the caller's transaction, so the
    row locks are held until commit and concurrent relay replicas skip each other's rows. If
    flush reports undelivered messages, this raises before marking, leaving the rows to re-publish.

    Per-row failures (defect D5) are isolated: a row that cannot be serialized or produced
    does not abort the batch. Its `attempts` counter is incremented, and once it reaches
    MAX_ATTEMPTS the raw payload is routed to `{topic}.dlq` and the row is marked published
    so the queue drains. Without this, one bad row crash-looped the relay forever and
    blocked every tenant, because there is a single shared outbox.
    """
    rows = (await session.execute(_SELECT_PENDING, {"limit": limit})).mappings().all()
    if not rows:
        record_outbox(backlog=0)
        return 0

    produced_ids: list[int] = []
    retry_ids: list[int] = []
    dead_ids: list[int] = []

    for r in rows:
        try:
            value = producer.serialize(r["schema_subject"], r["payload"])
            producer.produce(topic=r["topic"], key=r["partition_key"].encode(), value=value)
        except Exception as exc:  # noqa: BLE001 - any produce-side failure is per-row, not fatal
            # attempts is read from the row we just SELECTed, so this is the count BEFORE
            # this failure; +1 is the attempt that just happened.
            if (r["attempts"] or 0) + 1 >= MAX_ATTEMPTS:
                _dead_letter(producer, r, exc)
                dead_ids.append(r["id"])
                logger.error(
                    "outbox relay: row %s dead-lettered to %s.dlq after %d attempts: %s",
                    r["id"],
                    r["topic"],
                    MAX_ATTEMPTS,
                    exc,
                )
            else:
                retry_ids.append(r["id"])
                logger.warning(
                    "outbox relay: row %s failed (attempt %d/%d), will retry: %s",
                    r["id"],
                    (r["attempts"] or 0) + 1,
                    MAX_ATTEMPTS,
                    exc,
                )
            continue
        produced_ids.append(r["id"])

    remaining = producer.flush()
    if remaining:
        # Broker-side non-delivery is NOT a per-row fault: we cannot tell which messages
        # landed, so nothing is marked and the whole batch re-publishes. Consumers dedupe.
        raise RuntimeError(
            f"outbox relay: {remaining} message(s) undelivered; not marking published"
        )

    if produced_ids:
        await session.execute(_MARK_PUBLISHED, {"ids": produced_ids})
    if retry_ids:
        await session.execute(_BUMP_ATTEMPTS, {"ids": retry_ids})
    if dead_ids:
        await session.execute(_MARK_DEAD_LETTERED, {"ids": dead_ids})

    backlog: int | None = None
    try:
        async with session.begin_nested():  # savepoint: a failure must not poison the txn
            backlog = int((await session.execute(_COUNT_PENDING)).scalar_one())
    except Exception:  # noqa: BLE001 - a metrics query must never fail a drain
        logger.debug("outbox relay: backlog count failed", exc_info=True)
    record_outbox(published=len(produced_ids), poisoned=len(dead_ids), backlog=backlog)

    return len(produced_ids)


def _dead_letter(producer: RelayProducer, row: Any, exc: Exception) -> None:
    """Publish a poison row's RAW payload to `{topic}.dlq`.

    Deliberately not Avro-encoded: the row is here precisely because it could not be
    serialized against its subject, so encoding it again would fail the same way. The DLQ
    carries the original payload plus the error for human triage, keyed by the same
    partition key so a tenant's dead letters stay together.
    """
    envelope = {
        "outbox_id": row["id"],
        "topic": row["topic"],
        "schema_subject": row["schema_subject"],
        "error": f"{type(exc).__name__}: {exc}",
        "payload": row["payload"],
    }
    producer.produce(
        topic=f"{row['topic']}.dlq",
        key=row["partition_key"].encode(),
        value=json.dumps(envelope, default=str).encode(),
    )
