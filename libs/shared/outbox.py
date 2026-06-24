"""Transactional outbox base.

Usage pattern:
  async with session.begin():
      await session.execute(...)          # state change
      await OutboxRow.enqueue(session, envelope)   # atomic intent-to-emit

A relay process (or Temporal activity) reads pending outbox rows and publishes
to Redpanda, then marks them delivered. State change + emit commit atomically;
a crash before relay publication means the relay retries from the outbox.
"""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope


async def enqueue(session: AsyncSession, envelope: EventEnvelope) -> UUID:
    """Insert an outbox row in the current transaction. Returns the row id."""
    row_id = uuid4()
    await session.execute(
        text(
            """
            INSERT INTO outbox (id, org_id, topic, idempotency_key, payload, created_at)
            VALUES (:id, :org_id, :topic, :idempotency_key, :payload::jsonb, :created_at)
            """
        ),
        {
            "id": str(row_id),
            "org_id": envelope.org_id,
            "topic": envelope.topic,
            "idempotency_key": envelope.idempotency_key,
            "payload": __import__("json").dumps(
                {"schema_version": envelope.schema_version, "data": envelope.payload}
            ),
            "created_at": datetime.now(timezone.utc),
        },
    )
    return row_id
