"""Kafka consumer for core-api.

Consumes: presence-events (analytics read model only; no domain write - Plan 3-4 section 3.3
step 5). Presence itself stays owned by `realtime-gateway`/Redis; this consumer never writes
`meeting_participants` or any table `core-api` treats as domain source of truth from this path.

Split the same way `libs/shared/relay.py` splits `relay_once` (testable core) from
`services/relay/main.py`'s `run()` (the confluent-kafka/asyncio harness): `handle_presence_event`
takes an already-decoded message and is exercised directly in tests against a real Postgres +
fakeredis, with no live broker or Schema Registry required. `run_consumer()` is the thin,
mostly-untested wiring around it.

Contract with `realtime-gateway`'s producer: CLOSED, both sides. `PresenceEvent`'s envelope
header, and this topic's `user_id` payload field, are documented as UUID-typed
(`mytheus-events/schemas/presence-events/v1.avsc`, `_canon/03-event-taxonomy.md`'s envelope
table: "org_id ... Tenant root; RLS/cell scoping anchor"), and `realtime-gateway`'s
`presenceEventsProducer.ts` now honours that: it resolves `internalOrgId`/`internalUserId` via
this service's own `GET /api/v1/meetings/:id/participants/:clerkUserId` (reading
`ParticipantCheck.org_id`, added in `ca9a8db`, and `Participant.user_id`, which Task 6 had
already been discarding until it started reading it) before ever producing, and additionally
validates `isUuid(orgId) && isUuid(userId)` immediately before `send()`. Verified end to end by
Task 6 against a real `core-api` process, real Postgres, real RS256 verification. A Clerk-native
id can no longer reach this topic.

History, for context only - NOT a current condition: for a window during this band, the
producer passed the raw JWT `org_id`/`sub` claims straight through unresolved, which would have
made every real message fail the parse below and land in `presence-events.dlq`. That window is
closed; nothing here is presently poison-messaging on this account.

`handle_presence_event`'s strict `uuid.UUID(header["org_id"])` parse and DLQ-on-failure below
are NOT a workaround for the now-closed gap and must not be relaxed as if they were. They are
the standing "add an event consumer" recipe from this repo's `CLAUDE.md` (re-assert `org_id`
from the header before any write), and now serve as defence against a FUTURE regression on the
producer side - this or another producer sending a malformed header again - not as a permanent
crutch for a bug that no longer exists.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.tenancy import set_rls

logger = logging.getLogger(__name__)

TOPIC = "presence-events"
CONSUMER_GROUP = "core-api"


class PoisonMessage(Exception):
    """The message cannot be safely applied (schema mismatch, header that names no real
    tenant). The caller routes the raw message to `{TOPIC}.dlq` and commits past it - never
    re-raised into the poll loop, per the "never block the partition" consumer recipe."""


async def handle_presence_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one already-Avro-decoded `presence-events` message.

    Returns True if it was newly recorded, False if it was a dedupe no-op. Raises
    `PoisonMessage` if the header does not carry a resolvable tenant - expected not to happen
    against `realtime-gateway`'s current producer (see the module docstring), but kept as the
    standing defence the "add an event consumer" recipe calls for regardless.
    """
    header = value.get("header") or {}
    raw_org_id = header.get("org_id")
    idempotency_key = header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"presence-events message missing header org_id/idempotency_key: {value!r}"
        )

    try:
        org_id = uuid.UUID(str(raw_org_id))
    except ValueError as exc:
        raise PoisonMessage(
            f"presence-events header.org_id is not a UUID ({raw_org_id!r}); the producer "
            "contract requires a resolved internal UUID here - see the module docstring"
        ) from exc

    if await is_duplicate(redis, str(org_id), TOPIC, idempotency_key):
        return False

    await set_rls(session, org_id)

    # Read-model only, per the plan text: no write to meeting_participants or any table this
    # service treats as domain source of truth. A durable analytics sink (its own table) is a
    # data-model decision this task does not have the authority to make unilaterally (CLAUDE.md
    # naming authority: a new table needs `_canon/02-data-model.md` plus an ADR first) - logged
    # structurally instead, and flagged as a concrete follow-up in the task-5 report.
    logger.info(
        "presence-events: %s org=%s meeting=%s user=%s",
        value.get("event_type"),
        org_id,
        value.get("meeting_id"),
        value.get("user_id"),
        extra={
            "org_id": str(org_id),
            "meeting_id": value.get("meeting_id"),
            "user_id": value.get("user_id"),
            "event_type": value.get("event_type"),
        },
    )
    await record_consumed(session, CONSUMER_GROUP, str(org_id), idempotency_key)
    return True


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("core-api-consumer")
    serve_metrics()  # no-op unless METRICS_PORT is set
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
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
                logger.error("presence-events consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            topic_name = msg.topic() or TOPIC
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, TOPIC, _ts if _ts_type else None)
            try:
                value = deserializer(
                    raw_value, SerializationContext(topic_name, MessageField.VALUE)
                )
                async with session_factory() as session, session.begin():
                    applied = await handle_presence_event(session, redis, value)
                record_processed(CONSUMER_GROUP, TOPIC, "applied" if applied else "duplicate")
            except PoisonMessage as exc:
                logger.error("presence-events: poison message routed to DLQ: %s", exc)
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
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
