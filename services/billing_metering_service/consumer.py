"""`usage-events` consumer: meter every LLM call exactly once and charge the tenant's budget.

Plan 7-9 section 3.1 step 4. Only `agent_token` traverses this topic (producers
`agent-runtime-service` and `rag-service`, ADR-0011 section 3.1 and ADR-0029 D3), so any other
`event_type` here is a producer bug and goes to the DLQ rather than being metered.

Exactly-once in effect, three guards deep:
1. Redis dedupe `nexus:{org_id}:dedupe:usage-events:{idempotency_key}` (24h), the fast path.
2. `usage_events` UNIQUE `(org_id, idempotency_key, occurred_at)`: a redelivery that got past
   the cache inserts nothing, and then charges nothing (the budget bump only runs when the row
   was actually inserted).
3. The durable inbox row, written in the same transaction.

The Redis dedupe key is set BEFORE the transaction commits, so if the transaction fails the
key is deleted again; otherwise a crash between the two would make the redelivery look like a
duplicate and the tokens would never be metered, which is the one failure a hard cap cannot
tolerate.

Split like `services/core_api/consumer.py`: `handle_usage_event` is the tested core,
`run_consumer` the thin confluent-kafka harness.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.metrics import record_dlq, record_processed, serve_metrics, set_service
from libs.shared.tenancy import set_rls
from services.billing_metering_service.budgets import BudgetSnapshot, apply_tokens, write_mirror

logger = logging.getLogger(__name__)

TOPIC = "usage-events"
CONSUMER_GROUP = "billing-metering-service"
MODEL_TIERS = {"haiku", "sonnet", "opus"}


class PoisonMessage(Exception):
    """Cannot be applied safely. Routed to `usage-events.dlq`; the partition moves on."""


def dedupe_key(org_id: str, idempotency_key: str) -> str:
    return f"nexus:{org_id}:dedupe:{TOPIC}:{idempotency_key}"


def _parse(value: dict[str, Any]) -> dict[str, Any]:
    header = value.get("header") or {}
    raw_org, key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not key:
        raise PoisonMessage("usage-events message missing header org_id/idempotency_key")
    try:
        org_id = uuid.UUID(str(raw_org))
    except ValueError as exc:
        raise PoisonMessage(f"header.org_id is not a UUID: {raw_org!r}") from exc
    if value.get("event_type") != "agent_token":
        raise PoisonMessage(
            f"event_type {value.get('event_type')!r} on usage-events: only agent_token travels "
            "this topic (ADR-0029 D3)"
        )
    quantity = value.get("quantity")
    if not isinstance(quantity, int | float) or quantity < 0:
        raise PoisonMessage(f"quantity must be a non-negative number, got {quantity!r}")
    tier = value.get("model_tier")
    if tier is not None and tier not in MODEL_TIERS:
        raise PoisonMessage(f"model_tier {tier!r} is not one of {sorted(MODEL_TIERS)}")
    raw_user = value.get("user_id")
    try:
        user_id = uuid.UUID(str(raw_user)) if raw_user else None
    except ValueError as exc:
        raise PoisonMessage(f"user_id is not a UUID: {raw_user!r}") from exc
    # The confluent AvroDeserializer applies the `timestamp-micros` logical type and hands back
    # a timezone-aware datetime; a raw decode (fastavro without logical types, the golden
    # fixtures) gives the integer. Accept both, refuse anything else.
    raw_at = header.get("occurred_at")
    if isinstance(raw_at, datetime):
        occurred_at = raw_at if raw_at.tzinfo else raw_at.replace(tzinfo=UTC)
    elif isinstance(raw_at, int) and not isinstance(raw_at, bool):
        occurred_at = datetime.fromtimestamp(raw_at / 1_000_000, tz=UTC)
    else:
        raise PoisonMessage(f"header.occurred_at must be timestamp-micros, got {raw_at!r}")
    return {
        "org_id": org_id,
        "idempotency_key": str(key),
        "event_id": header.get("event_id"),
        "user_id": user_id,
        "tokens": int(round(quantity)),
        "model_tier": tier,
        "occurred_at": occurred_at,
    }


async def handle_usage_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> BudgetSnapshot | None:
    """Apply one decoded `usage-events` value inside the caller's transaction.

    Returns the new budget snapshot when tokens were charged (the caller writes it to the Redis
    mirror after commit), or None when the message was a duplicate.
    """
    msg = _parse(value)
    org = str(msg["org_id"])
    if await is_duplicate(redis, org, TOPIC, msg["idempotency_key"]):
        return None

    await set_rls(session, msg["org_id"])
    inserted = (
        await session.execute(
            text(
                "INSERT INTO usage_events (org_id, user_id, event_type, quantity, model_tier, "
                "idempotency_key, occurred_at) VALUES (:org, :user, 'agent_token', :qty, :tier, "
                ":key, :at) ON CONFLICT (org_id, idempotency_key, occurred_at) DO NOTHING "
                "RETURNING id"
            ),
            {
                "org": org,
                "user": str(msg["user_id"]) if msg["user_id"] else None,
                "qty": msg["tokens"],
                "tier": msg["model_tier"],
                "key": msg["idempotency_key"],
                "at": msg["occurred_at"],
            },
        )
    ).first()
    snapshot = None
    if inserted is not None:
        snapshot = await apply_tokens(
            session,
            msg["org_id"],
            msg["tokens"],
            msg["occurred_at"],
            causation_id=msg["event_id"],
        )
    await record_consumed(session, CONSUMER_GROUP, org, msg["idempotency_key"])
    return snapshot


async def process(session_factory: Any, redis: Redis[bytes], value: dict[str, Any]) -> str:
    """One message end to end: transaction, then mirror. Returns the outcome label."""
    header = value.get("header") or {}
    try:
        async with session_factory() as session, session.begin():
            snapshot = await handle_usage_event(session, redis, value)
    except PoisonMessage:
        raise
    except Exception:
        # The transaction rolled back; un-mark the message so its redelivery is applied.
        if header.get("org_id") and header.get("idempotency_key"):
            await redis.delete(dedupe_key(str(header["org_id"]), str(header["idempotency_key"])))
        raise
    if snapshot is None:
        return "duplicate"
    try:
        await write_mirror(redis, snapshot)
    except Exception:  # the row is committed; a stale mirror is refreshed by the next charge
        logger.exception("usage-events: budget mirror write failed for org=%s", snapshot.org_id)
    return "applied"


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("billing-metering-consumer")
    serve_metrics()
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")
    redis: Redis[bytes] = redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])
    dlq = KafkaProducer({"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True})
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
    session_factory = get_sessionmaker()
    try:
        while True:
            # Blocking poll, like core-api's consumer. This process does nothing else and is
            # stopped by signal; polling in a thread so the coroutine can be cancelled would race
            # `consumer.close()` against a thread still inside `poll` (it segfaults).
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("usage-events consumer error: %s", msg.error())
                continue
            raw = msg.value()
            try:
                value = deserializer(
                    raw, SerializationContext(msg.topic() or TOPIC, MessageField.VALUE)
                )
                outcome = await process(session_factory, redis, value)
                record_processed(CONSUMER_GROUP, TOPIC, outcome)
            except PoisonMessage as exc:
                logger.error("usage-events: poison message routed to DLQ: %s", exc)
                dlq.produce(
                    topic=f"{TOPIC}.dlq",
                    key=msg.key(),
                    value=json.dumps(
                        {"error": str(exc), "raw": raw.hex() if raw else None}
                    ).encode(),
                )
                dlq.flush(10)
                record_dlq(CONSUMER_GROUP, TOPIC, "POISON")
                record_processed(CONSUMER_GROUP, TOPIC, "dlq")
            except Exception:
                # Transient (database down, lock timeout): do not commit the offset, back off,
                # and let the redelivery retry. Never meters zero for a real call.
                logger.exception("usage-events: transient failure, will retry")
                await asyncio.sleep(2)
                topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
                if topic is not None and partition is not None and offset is not None:
                    consumer.seek(TopicPartition(topic, partition, offset))
                continue
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
