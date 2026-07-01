"""Transactional-outbox relay: the single sanctioned producer path.

Polls unpublished outbox rows (FOR UPDATE SKIP LOCKED so replicas do not double-publish),
Avro-serializes each row's value against {topic}-value, produces keyed by org_id (acks=all,
idempotent), then marks the row published. At-least-once to the bus: a crash after produce but
before mark re-publishes on restart, which the consumer side dedupes.

`relay_once` is decoupled from Kafka specifics via a small producer protocol so it is unit
testable with a fake producer. The concrete confluent-kafka producer lives in
services/relay/main.py.
"""

from __future__ import annotations

from typing import Any, Protocol

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SELECT_PENDING = text(
    "SELECT id, topic, partition_key, schema_subject, payload "
    "FROM outbox WHERE published_at IS NULL "
    "ORDER BY created_at "
    "FOR UPDATE SKIP LOCKED "
    "LIMIT :limit"
)

_MARK_PUBLISHED = text("UPDATE outbox SET published_at = now() WHERE id = ANY(:ids)")


class RelayProducer(Protocol):
    def serialize(self, subject: str, value: dict[str, Any]) -> bytes: ...
    def produce(self, *, topic: str, key: bytes, value: bytes) -> None: ...
    def flush(self) -> int: ...  # returns messages still in-queue (0 == all delivered)


async def relay_once(session: AsyncSession, producer: RelayProducer, *, limit: int = 500) -> int:
    """Drain one batch of unpublished outbox rows. Returns the number published.

    The SELECT ... FOR UPDATE SKIP LOCKED and the UPDATE run in the caller's transaction, so the
    row locks are held until commit and concurrent relay replicas skip each other's rows. If
    flush reports undelivered messages, this raises before marking, leaving the rows to re-publish.
    """
    rows = (await session.execute(_SELECT_PENDING, {"limit": limit})).mappings().all()
    if not rows:
        return 0

    for r in rows:
        value = producer.serialize(r["schema_subject"], r["payload"])
        producer.produce(topic=r["topic"], key=r["partition_key"].encode(), value=value)

    remaining = producer.flush()
    if remaining:
        raise RuntimeError(
            f"outbox relay: {remaining} message(s) undelivered; not marking published"
        )

    await session.execute(_MARK_PUBLISHED, {"ids": [r["id"] for r in rows]})
    return len(rows)
