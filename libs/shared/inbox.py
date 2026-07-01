"""Inbox / dedupe for at-least-once event consumers.

Pattern:
  1. Check Redis dedupe key `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL).
  2. If seen -> drop (return True from is_duplicate).
  3. If new -> run handler + record_consumed (inbox insert) in the SAME transaction.

The Redis check is fast (avoids a DB round-trip for most redeliveries); the inbox table is the
durable authority when the cache has evicted. The inbox PK is (consumer_group, org_id,
idempotency_key), so each consumer group applies a given fact exactly once, independently.
"""

from __future__ import annotations

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_DEDUPE_TTL = 86_400  # 24 h


async def is_duplicate(
    redis: Redis[bytes],
    org_id: str,
    topic: str,
    idempotency_key: str,
) -> bool:
    key = f"nexus:{org_id}:dedupe:{topic}:{idempotency_key}"
    result = await redis.set(key, "1", ex=_DEDUPE_TTL, nx=True)
    return result is None  # None -> key already existed -> duplicate


async def record_consumed(
    session: AsyncSession,
    consumer_group: str,
    org_id: str,
    idempotency_key: str,
) -> None:
    """Durably record that this consumer group applied this fact. PK dedupes concurrent replays."""
    await session.execute(
        text("""
            INSERT INTO inbox (consumer_group, org_id, idempotency_key)
            VALUES (:consumer_group, :org_id, :idempotency_key)
            ON CONFLICT (consumer_group, org_id, idempotency_key) DO NOTHING
            """),
        {
            "consumer_group": consumer_group,
            "org_id": org_id,
            "idempotency_key": idempotency_key,
        },
    )
