"""Inbox / dedupe for at-least-once event consumers.

Pattern:
  1. Check Redis dedupe key `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL).
  2. If seen → drop (return False).
  3. If new → mark in Redis + insert inbox row + run handler → return True.

The Redis check is fast (avoids a DB round-trip for most redeliveries); the
inbox table is the durable record for audit / replay.
"""
from __future__ import annotations

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession


_DEDUPE_TTL = 86_400  # 24 h


async def is_duplicate(
    redis: Redis,
    org_id: str,
    topic: str,
    idempotency_key: str,
) -> bool:
    key = f"nexus:{org_id}:dedupe:{topic}:{idempotency_key}"
    result = await redis.set(key, "1", ex=_DEDUPE_TTL, nx=True)
    return result is None  # None → key already existed → duplicate


async def record_consumed(
    session: AsyncSession,
    org_id: str,
    topic: str,
    idempotency_key: str,
) -> None:
    from sqlalchemy import text
    from datetime import datetime, timezone

    await session.execute(
        text(
            """
            INSERT INTO inbox (org_id, topic, idempotency_key, consumed_at)
            VALUES (:org_id, :topic, :idempotency_key, :consumed_at)
            ON CONFLICT DO NOTHING
            """
        ),
        {
            "org_id": org_id,
            "topic": topic,
            "idempotency_key": idempotency_key,
            "consumed_at": datetime.now(timezone.utc),
        },
    )
