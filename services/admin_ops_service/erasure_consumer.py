"""`admin-events` consumer: record each store's finish of a data erasure.

    python -m services.admin_ops_service.erasure_consumer  # group admin-ops-service.erasure-status

An admin's erasure (`POST /admin/tenant/erasures`, `tenant_routes.py`) finishes core-api's share
inside the request and emits `erasure_requested`; the stores this repo does not own finish their
share asynchronously and each answers with one `admin-events` `erasure_completed`
(mytheus-events `schemas/admin-events/v1.avsc`): `target_type`/`target_id` copied from the
request, `detail_json` `{"erasure_id", "store", "service", "counts"}`, idempotency key
`erasure_completed:{erasure_id}:{store}`. Producers today: transcription-service
(`transcript_chunks`), embedding-service (`vectors`), search-service (`search_index`) and
knowledge-graph-service (`knowledge_graph`).

Every other `admin-events` record (this service's own `flag_changed`, `tenant_suspended`,
`erasure_requested`) is ignored before any database or Redis work.

Per `erasure_completed`, in one transaction:
1. Re-assert `org_id` from the envelope header (poison if missing or not a UUID); fast-path
   Redis dedupe `nexus:{org_id}:dedupe:admin-events:{CONSUMER_GROUP}:{idempotency_key}` (the
   group is in the key: `admin-events` is consumed by several groups, each applying a fact
   once); `SET app.current_org`; claim the inbox row (the durable authority).
2. Check the erasure exists in THIS tenant (its `data_erasure.completed` row, under RLS): an
   unknown id is poison, never a row pointing at nothing.
3. Append one `audit_logs` row `data_erasure.store_completed` (`actor_type` system,
   `target_type` data_erasure, `target_id` the erasure id, metadata `{store, service, counts,
   kind}`); `GET /admin/tenant/erasures` derives each request's status from these.

A failed transaction releases the Redis key, so the redelivery applies (the gap noted for
`BaseConsumer` in the other tiers does not exist here). Poison goes to `admin-events.dlq`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import uuid
from collections.abc import Callable
from typing import Any, Literal

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_DATA_ERASURE_COMPLETED,
    ACTION_DATA_ERASURE_STORE_COMPLETED,
    audit,
)
from libs.shared.inbox import is_duplicate
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.tenancy import set_rls

logger = logging.getLogger(__name__)

TOPIC = "admin-events"
DLQ_TOPIC = f"{TOPIC}.dlq"
CONSUMER_GROUP = "admin-ops-service.erasure-status"
COMPLETED = "erasure_completed"
_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

Outcome = Literal["applied", "duplicate", "ignored"]


class PoisonMessage(Exception):
    """Can never apply. Routed to `admin-events.dlq`; the partition moves on."""


def _enum_str(raw: Any) -> str:
    return str(getattr(raw, "name", raw))


def _uuid(raw: Any, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(raw))
    except ValueError as exc:
        raise PoisonMessage(f"{what} is not a UUID: {raw!r}") from exc


def _counts(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    return {
        str(k)[:64]: int(v)
        for k, v in raw.items()
        if not isinstance(v, bool) and isinstance(v, int | float)
    }


def dedupe_topic() -> str:
    """The `topic` segment of the Redis dedupe key: topic plus group (see the docstring)."""
    return f"{TOPIC}:{CONSUMER_GROUP}"


def parse(value: Any) -> dict[str, Any] | None:
    """The fields of an `erasure_completed`, or None for any other admin event."""
    if not isinstance(value, dict):
        raise PoisonMessage("admin-events value is not a record")
    if _enum_str(value.get("event_type")) != COMPLETED:
        return None
    header = value.get("header") or {}
    raw_org, key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not key:
        raise PoisonMessage("erasure_completed missing header org_id/idempotency_key")
    org_id = _uuid(raw_org, "header.org_id")
    raw = value.get("detail_json")
    try:
        detail = json.loads(raw) if isinstance(raw, str) else None
    except ValueError as exc:
        raise PoisonMessage("erasure_completed detail_json is not JSON") from exc
    if not isinstance(detail, dict):
        raise PoisonMessage("erasure_completed detail_json is not an object")
    erasure_id = _uuid(detail.get("erasure_id"), "detail.erasure_id")
    store, service = detail.get("store"), detail.get("service")
    if not isinstance(store, str) or not _NAME.match(store):
        raise PoisonMessage(f"erasure_completed store is not a store name: {store!r}")
    if not isinstance(service, str) or not _NAME.match(service):
        raise PoisonMessage(f"erasure_completed service is not a service name: {service!r}")
    kind = value.get("target_type")
    return {
        "org_id": org_id,
        "idempotency_key": str(key),
        "erasure_id": erasure_id,
        "store": store,
        "service": service,
        "counts": _counts(detail.get("counts")),
        "kind": kind if kind in ("person", "meeting") else None,
    }


async def _claim_inbox(session: AsyncSession, org_id: uuid.UUID, key: str) -> bool:
    try:
        row = (
            await session.execute(
                text(
                    "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                    "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, idempotency_key) "
                    "DO NOTHING RETURNING 1"
                ),
                {"g": CONSUMER_GROUP, "o": str(org_id), "k": key},
            )
        ).first()
    except IntegrityError as exc:
        raise PoisonMessage(f"message names unknown tenant {org_id}") from exc
    return row is not None


async def handle_message(session: AsyncSession, redis: Redis[bytes], value: Any) -> Outcome:
    """Apply one decoded `admin-events` value inside the caller's transaction.

    The caller releases the Redis key if the transaction fails (`process`)."""
    msg = parse(value)
    if msg is None:
        return "ignored"
    org_id: uuid.UUID = msg["org_id"]
    if await is_duplicate(redis, str(org_id), dedupe_topic(), msg["idempotency_key"]):
        return "duplicate"
    await set_rls(session, org_id)
    if not await _claim_inbox(session, org_id, msg["idempotency_key"]):
        return "duplicate"
    known = (
        await session.execute(
            text(
                "SELECT 1 FROM audit_logs WHERE action = :a AND target_type = 'data_erasure' "
                "AND target_id = :e LIMIT 1"
            ),
            {"a": ACTION_DATA_ERASURE_COMPLETED, "e": str(msg["erasure_id"])},
        )
    ).first()
    if known is None:
        raise PoisonMessage(f"erasure {msg['erasure_id']} is not a request of tenant {org_id}")
    await audit(
        session,
        org_id=org_id,
        actor_type="system",
        action=ACTION_DATA_ERASURE_STORE_COMPLETED,
        target_type="data_erasure",
        target_id=msg["erasure_id"],
        metadata={
            "store": msg["store"],
            "service": msg["service"],
            "counts": msg["counts"],
            "kind": msg["kind"],
        },
    )
    return "applied"


def _key_of(value: Any) -> str | None:
    if not isinstance(value, dict):
        return None
    header = value.get("header") or {}
    org, key = header.get("org_id"), header.get("idempotency_key")
    if not org or not key:
        return None
    return f"nexus:{org}:dedupe:{dedupe_topic()}:{key}"


async def process(session_factory: Callable[[], Any], redis: Redis[bytes], value: Any) -> Outcome:
    """One message end to end. A failed transaction releases its dedupe key."""
    try:
        async with session_factory() as session, session.begin():
            return await handle_message(session, redis, value)
    except BaseException:
        if (key := _key_of(value)) is not None:
            await redis.delete(key)
        raise


async def consume_one(
    msg: Any,
    *,
    decode_value: Callable[[bytes | None], Any],
    session_factory: Callable[[], Any],
    redis: Redis[bytes],
    dlq_producer: Any,
) -> str:
    """Decode and apply one record; poison goes to `admin-events.dlq`. Returns the outcome
    (`applied`, `duplicate`, `ignored` or `dlq`). Any other exception is transient and
    propagates: the harness retries the record without committing its offset."""
    raw = msg.value()
    try:
        try:
            value = decode_value(raw)
        except (ValueError, TypeError, EOFError, IndexError) as exc:
            raise PoisonMessage(f"undecodable record: {exc}") from exc
        outcome: str = await process(session_factory, redis, value)
    except PoisonMessage as exc:
        logger.error("admin-events: poison message routed to DLQ: %s", exc)
        dlq_producer.produce(
            topic=DLQ_TOPIC,
            key=msg.key(),
            value=json.dumps(
                {"error": str(exc), "group": CONSUMER_GROUP, "raw": raw.hex() if raw else None}
            ).encode(),
        )
        dlq_producer.flush(10)
        record_dlq(CONSUMER_GROUP, TOPIC, "POISON")
        outcome = "dlq"
    record_processed(CONSUMER_GROUP, TOPIC, outcome)
    return outcome


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("admin-ops-erasure-status")
    serve_metrics()  # no-op unless METRICS_PORT is set
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
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("admin-events consumer error: %s", msg.error())
                continue
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, TOPIC, _ts if _ts_type else None)
            try:
                await consume_one(
                    msg,
                    decode_value=lambda raw: deserializer(
                        raw, SerializationContext(TOPIC, MessageField.VALUE)
                    ),
                    session_factory=session_factory,
                    redis=redis,
                    dlq_producer=dlq,
                )
            except Exception:
                # Transient (database down): do not commit, back off, retry the same record.
                logger.exception("admin-events: transient failure, will retry")
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
