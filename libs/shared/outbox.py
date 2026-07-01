"""Transactional outbox base.

Usage pattern:
  async with session.begin():
      await session.execute(...)                 # state change
      await enqueue(session, envelope)           # atomic intent-to-emit

A relay process reads pending outbox rows, Avro-serializes the stored value against
{topic}-value, publishes to Redpanda keyed by org_id, then marks the row published. State
change + emit commit atomically; a crash before relay publication means the relay retries.

The row carries the full canonical column set: org_id, topic, partition_key (the org_id string),
idempotency_key, schema_subject ({topic}-value), and payload (the full Avro value). id is
BIGSERIAL; the unique index (topic, idempotency_key) makes a double-confirm a no-op.
"""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope


async def enqueue(session: AsyncSession, envelope: EventEnvelope) -> int:
    """Insert an outbox row in the current transaction. Returns the BIGSERIAL row id.

    On a duplicate (topic, idempotency_key) the insert is a no-op and the existing id is
    returned (Guard 2 of the duplicate-decision stack).
    """
    result = await session.execute(
        text(
            "INSERT INTO outbox "
            "(org_id, topic, partition_key, idempotency_key, schema_subject, payload) "
            "VALUES (:org_id, :topic, :partition_key, :idempotency_key, :schema_subject, "
            "CAST(:payload AS jsonb)) "
            "ON CONFLICT (topic, idempotency_key) DO NOTHING "
            "RETURNING id"
        ),
        {
            "org_id": envelope.org_id,
            "topic": envelope.topic,
            "partition_key": envelope.partition_key,
            "idempotency_key": envelope.idempotency_key,
            "schema_subject": envelope.schema_subject,
            "payload": json.dumps(envelope.to_value()),
        },
    )
    row = result.first()
    if row is not None:
        return int(row[0])
    existing = await session.execute(
        text("SELECT id FROM outbox WHERE topic = :topic AND idempotency_key = :ik"),
        {"topic": envelope.topic, "ik": envelope.idempotency_key},
    )
    return int(existing.scalar_one())
