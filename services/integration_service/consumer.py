"""`actions` consumer for integration-service (consumer group `integration-service`).

    python -m services.integration_service.consumer

`actions` carries only user-confirmed actions (core-api's outbox, on confirm). Each one is pushed
to Jira when the tenant has Jira connected (`connectors/jira.push_action`); otherwise it is
consumed and recorded with no effect.

The recipe from CLAUDE.md: re-assert `org_id` from the header (poison if missing), fast-path
dedupe on `nexus:{org_id}:dedupe:actions:{idempotency_key}`, `SET app.current_org`, claim the
inbox row, apply. The handler is idempotent on its own as well (id map, then the
`nexus:{action_id}` label search), which is what makes "a redelivered `actions` event never
creates a second Jira issue" hold even after the dedupe key has expired.

A provider error propagates (the record is retried, Redis key released); a message that can
never apply goes to `actions.dlq`.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.inbox import is_duplicate
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.tenancy import set_rls
from services.integration_service.clients import Clients, get_clients
from services.integration_service.connectors import jira
from services.integration_service.redaction import get_logger, install_redaction

logger = get_logger(__name__)

TOPIC = "actions"
CONSUMER_GROUP = "integration-service"


class PoisonMessage(Exception):
    pass


def _enum(raw: Any) -> str:
    return str(getattr(raw, "name", raw))


async def handle_action(
    session: AsyncSession,
    redis: Redis[bytes],
    value: dict[str, Any],
    *,
    clients: Clients | None = None,
) -> str:
    """Apply one decoded `actions` message. Returns created|linked|exists|not_connected|
    duplicate|ignored."""
    header = value.get("header") or {}
    raw_org, key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not key:
        raise PoisonMessage("actions message missing header org_id/idempotency_key")
    try:
        org_id = uuid.UUID(str(raw_org))
        uuid.UUID(str(value.get("action_id")))
    except ValueError as exc:
        raise PoisonMessage("actions message has a non-UUID org_id or action_id") from exc
    if _enum(value.get("status")) != "confirmed":
        return "ignored"
    if await is_duplicate(redis, str(org_id), TOPIC, str(key)):
        return "duplicate"
    try:
        await set_rls(session, org_id)
        try:
            claimed = (
                await session.execute(
                    text(
                        "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                        "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, "
                        "idempotency_key) DO NOTHING RETURNING 1"
                    ),
                    {"g": CONSUMER_GROUP, "o": str(org_id), "k": str(key)},
                )
            ).first()
        except IntegrityError as exc:
            raise PoisonMessage(f"actions message names unknown tenant {org_id}") from exc
        if claimed is None:
            return "duplicate"
        return await jira.push_action(session, org_id, value, clients or get_clients())
    except BaseException:
        await redis.delete(f"nexus:{org_id}:dedupe:{TOPIC}:{key}")
        raise


def decode(deserializer: Any, raw_value: bytes | None, ctx: Any) -> Any:
    try:
        return deserializer(raw_value, ctx)
    except (ValueError, TypeError, EOFError, IndexError) as exc:
        raise PoisonMessage(f"undecodable record: {exc}") from exc


async def consume_one(
    msg: Any,
    *,
    decode_value: Callable[[bytes | None], Any],
    session_factory: Callable[[], Any],
    redis: Redis[bytes],
    dlq_producer: Any,
) -> str:
    raw_value = msg.value()
    try:
        value = decode_value(raw_value)
        async with session_factory() as session, session.begin():
            outcome = await handle_action(session, redis, value)
    except PoisonMessage as exc:
        logger.error("actions: poison message routed to DLQ: %s", exc)
        dlq_producer.produce(
            topic=f"{TOPIC}.dlq",
            key=msg.key(),
            value=json.dumps(
                {"error": str(exc), "raw": raw_value.hex() if raw_value else None}
            ).encode(),
        )
        dlq_producer.flush(10)
        record_dlq(CONSUMER_GROUP, TOPIC, "POISON")
        record_processed(CONSUMER_GROUP, TOPIC, "dlq")
        return "dlq"
    record_processed(CONSUMER_GROUP, TOPIC, "duplicate" if outcome == "duplicate" else "applied")
    return outcome


async def run_consumer() -> None:  # pragma: no cover - thin harness
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    install_redaction()
    set_service("integration-service-consumer")
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
    dlq_producer = KafkaProducer(
        {"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True}
    )
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
    session_factory = get_sessionmaker()
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("integration-service consumer error: %s", msg.error())
                continue
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, TOPIC, _ts if _ts_type else None)
            try:
                await consume_one(
                    msg,
                    decode_value=lambda raw: decode(
                        deserializer, raw, SerializationContext(TOPIC, MessageField.VALUE)
                    ),
                    session_factory=session_factory,
                    redis=redis,
                    dlq_producer=dlq_producer,
                )
            except Exception:
                # Jira or the database is unavailable: retry this record after a pause instead
                # of skipping it or crash-looping the process.
                logger.exception("actions: transient failure, retrying the record")
                await asyncio.sleep(5)
                consumer.seek(
                    TopicPartition(str(msg.topic()), msg.partition() or 0, msg.offset() or 0)
                )
                continue
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
