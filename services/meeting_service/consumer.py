"""Kafka consumer for meeting-service: consumes `meeting-events` (produced by `core-api`) to
open and close LiveKit rooms as a meeting's lifecycle changes. Plan 3-4 section 3.1 step 6.

Split the same way `services/core_api/consumer.py` splits `handle_presence_event` (testable
core) from `run_consumer()` (the confluent-kafka/asyncio harness): `handle_meeting_event` takes
an already-Avro-decoded message and is exercised directly against a real Postgres + fakeredis,
no live broker or Schema Registry required. The DLQ routing is split out the same way
`libs/shared/relay.py` splits `relay_once` from a raw producer: a small `DlqProducer` protocol
plus a pure `route_to_dlq` function, and `process_decoded_message` (the poll-loop body,
extracted so it is directly testable with a fake producer) - so "a poison message lands in
`meeting-events.dlq` and does not block the partition" is proven the same way
`tests/test_relay.py::test_poison_row_is_dead_lettered_after_max_attempts` proves it for the
produce side, not asserted from a live-broker integration test.

PREREQUISITE FIX (Task 4 brief). `mytheus-events/schemas/meeting-events/v1.avsc` declares
`livekit_room` as `["null", "string"]` with `default: null` - the committed generated Python
client had drifted (fixed separately, in `mytheus-events`, commit-by-commit). This service does
not import that generated client at all: it is not an installable package (no `pyproject.toml`
under `clients/python/`) and is not a dependency here, matching how
`services/core_api/consumer.py` also parses the Avro-decoded dict directly. The same defect
class the stale client had - code that assumes `livekit_room` is always a non-null string - is
avoided here by construction, not by the fix upstream: `handle_meeting_event` never reads
`value.get("livekit_room")` at all. `meeting-service`, not `core-api`, owns the room-naming
convention (`services.meeting_service.rooms.room_name_for`), so this consumer computes the room
name itself and treats whatever `core-api` puts in the event's `livekit_room` field as
information this service has no use for, null or not.

Owns: `meeting_rooms` writes for `meeting.created` (pre-create the row) and `meeting.ended`
(mark it ended). `meeting.updated` is a recognized no-op today - no `meeting_rooms` column
tracks anything an "updated" meeting event would change yet.

ROOM METADATA FOLLOW-UP. `meeting.created` is one of the two places (with the token-mint
route) a `meeting_rooms` row is first established, so it is also one of the two places
`services.meeting_service.transport.MeetingTransport.ensure_room` must run to make
`services.meeting_service.webhook._resolve_org_id` resolvable against a real LiveKit
deployment.

FIX ROUND 1. The first version of this follow-up called `ensure_room` (a real LiveKit HTTP
call) SYNCHRONOUSLY from inside `handle_meeting_event`, inside the SAME `session.begin()`
block `process_decoded_message` wraps it in. That call only catches `PoisonMessage` - an
ordinary `Exception` from a LiveKit timeout/5xx/network error propagated straight out of
`process_decoded_message`, out of `run_consumer`'s bare `while True` (which also catches
nothing), and crashed the consumer. Because the crash happened before
`consumer.commit(message=msg)`, the same message got redelivered and crashed it again on
restart: a LiveKit blip could crash-loop the ENTIRE `meeting-events` partition, for every
tenant, until LiveKit recovered or someone manually skipped the offset - exactly what the
"poison messages must never block the partition" constraint exists to prevent, reached
through a different exception type than the DLQ path was built to catch.

Fixed by decoupling: `handle_meeting_event` never touches `transport` and makes no network
call. When `created` still needs a sync (`RoomUpsertResult.needs_livekit_sync`), it returns
`(org_id, room)` as `HandledMeetingEvent.pending_room_sync`; `process_decoded_message` fires
`rooms.sync_livekit_metadata` as a DETACHED `asyncio.Task` only AFTER its own `session.begin()`
block has already committed and returned. The task's own try/except (inside
`sync_livekit_metadata`) swallows every LiveKit failure, so nothing it does can propagate back
into the poll loop, and `consumer.commit(message=msg)` runs immediately regardless of whether
the sync succeeds. `run_consumer()` selects a transport via `select_transport()`, same as the
token-mint route's `get_transport` dependency, and passes it to `process_decoded_message`
(never to `handle_meeting_event`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass
from typing import Any, Protocol

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
from services.meeting_service.rooms import (
    ensure_meeting_room,
    mark_room_ended,
    sync_livekit_metadata,
)
from services.meeting_service.transport import MeetingTransport, select_transport

logger = logging.getLogger(__name__)

TOPIC = "meeting-events"
CONSUMER_GROUP = "meeting-service"

_RECOGNIZED_EVENT_TYPES = {"created", "updated", "ended"}

# Fire-and-forget `sync_livekit_metadata` tasks. A reference MUST be kept somewhere for the
# lifetime of the task - asyncio only holds a weak reference internally, so an unreferenced
# task can be garbage-collected mid-execution (a documented, easy-to-miss gotcha). Discarded
# via `add_done_callback` once each task finishes, so this never grows unbounded.
_background_sync_tasks: set[asyncio.Task[None]] = set()


class PoisonMessage(Exception):
    """The message cannot be safely applied (missing/malformed header, a `meeting_id` that is
    not a UUID, or an `event_type` outside the schema's enum). The caller routes the raw
    message to `{TOPIC}.dlq` and commits past it - never re-raised into the poll loop, per the
    "never block the partition" consumer recipe."""


class DlqProducer(Protocol):
    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None: ...
    def flush(self, timeout: float = 10) -> int: ...


def route_to_dlq(
    producer: DlqProducer,
    *,
    topic: str,
    key: bytes | None,
    raw_value: bytes | None,
    error: Exception,
) -> None:
    """Publish a poison message's RAW bytes to `{topic}.dlq`, for human triage. Deliberately not
    re-encoded against the topic's Avro subject: the message is here precisely because it could
    not be safely applied, so re-encoding it the same way would not help. Mirrors
    `libs/shared/relay.py`'s `_dead_letter` for the produce side of the same contract.
    """
    producer.produce(
        topic=f"{topic}.dlq",
        key=key,
        value=json.dumps(
            {"error": str(error), "raw": raw_value.hex() if raw_value else None}
        ).encode(),
    )
    producer.flush(10)


@dataclass(frozen=True)
class HandledMeetingEvent:
    applied: bool
    # (org_id, room) if this apply established a room that still needs a LiveKit metadata
    # sync; None otherwise (a dedupe no-op, "ended", "updated", or a room that was already
    # synced). `process_decoded_message` fires the sync AFTER this function's transaction has
    # committed - see the module docstring's FIX ROUND 1 note for why it cannot happen here.
    pending_room_sync: tuple[uuid.UUID, str] | None = None


async def handle_meeting_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> HandledMeetingEvent:
    """Apply one already-Avro-decoded `meeting-events` message. DB and Redis only - no LiveKit
    call happens in this function, ever (see the module docstring's FIX ROUND 1 note).

    Raises `PoisonMessage` if the header or body cannot be safely applied - see the module
    docstring.
    """
    header = value.get("header") or {}
    raw_org_id = header.get("org_id")
    idempotency_key = header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"meeting-events message missing header org_id/idempotency_key: {value!r}"
        )

    try:
        org_id = uuid.UUID(str(raw_org_id))
    except ValueError as exc:
        raise PoisonMessage(f"meeting-events header.org_id is not a UUID ({raw_org_id!r})") from exc

    raw_meeting_id = value.get("meeting_id")
    try:
        meeting_id = uuid.UUID(str(raw_meeting_id))
    except (TypeError, ValueError) as exc:
        raise PoisonMessage(
            f"meeting-events meeting_id is not a UUID ({raw_meeting_id!r})"
        ) from exc

    event_type = value.get("event_type")
    if event_type not in _RECOGNIZED_EVENT_TYPES:
        raise PoisonMessage(f"meeting-events event_type is not recognized ({event_type!r})")

    if await is_duplicate(redis, str(org_id), TOPIC, idempotency_key):
        return HandledMeetingEvent(applied=False)

    await set_rls(session, org_id)

    # See PREREQUISITE FIX above: `value.get("livekit_room")` is never read. `event_type` and
    # `meeting_id` are the only fields this handler needs, and both are validated above.
    pending_room_sync: tuple[uuid.UUID, str] | None = None
    if event_type == "created":
        result = await ensure_meeting_room(session, org_id, meeting_id)
        if result.needs_livekit_sync:
            pending_room_sync = (org_id, result.room)
    elif event_type == "ended":
        await mark_room_ended(session, org_id, meeting_id)
    else:  # "updated"
        logger.info(
            "meeting-events: updated (recognized no-op) org=%s meeting=%s", org_id, meeting_id
        )

    await record_consumed(session, CONSUMER_GROUP, str(org_id), idempotency_key)
    return HandledMeetingEvent(applied=True, pending_room_sync=pending_room_sync)


def _fire_and_forget_sync(
    session_factory: Any, transport: MeetingTransport, *, org_id: uuid.UUID, room: str
) -> None:
    """Schedule `rooms.sync_livekit_metadata` without awaiting it, keeping a reference so the
    task cannot be garbage-collected mid-execution. `sync_livekit_metadata` catches every
    exception itself, so nothing raised here can ever reach `run_consumer`'s poll loop."""
    task = asyncio.create_task(
        sync_livekit_metadata(session_factory, transport, org_id=org_id, room=room)
    )
    _background_sync_tasks.add(task)
    task.add_done_callback(_background_sync_tasks.discard)


async def process_decoded_message(
    value: dict[str, Any],
    *,
    raw_value: bytes | None,
    key: bytes | None,
    session_factory: Any,
    redis: Redis[bytes],
    dlq_producer: DlqProducer,
    transport: MeetingTransport,
) -> None:
    """One already-Avro-decoded message through to application, or to the DLQ.

    Extracted out of `run_consumer`'s poll loop so the DLQ path is directly testable without a
    live broker (see `tests/test_meeting_events_consumer.py`), the same way `relay_once` is
    unit-tested against a fake producer instead of a live Redpanda.
    """
    try:
        async with session_factory() as session, session.begin():
            result = await handle_meeting_event(session, redis, value)
    except PoisonMessage as exc:
        logger.error("meeting-events: poison message routed to DLQ: %s", exc)
        route_to_dlq(dlq_producer, topic=TOPIC, key=key, raw_value=raw_value, error=exc)
        record_dlq(CONSUMER_GROUP, TOPIC, "POISON")
        record_processed(CONSUMER_GROUP, TOPIC, "dlq")
        return

    record_processed(CONSUMER_GROUP, TOPIC, "applied" if result.applied else "duplicate")

    if result.pending_room_sync is not None:
        # By this point `session.begin()` above has already committed and the connection is
        # back in the pool - firing this now, not awaiting it, is what keeps a LiveKit blip
        # from ever reaching this function's caller (`run_consumer`'s `consumer.commit`, right
        # after this returns, is never delayed or skipped by a slow or failing LiveKit call).
        org_id, room = result.pending_room_sync
        _fire_and_forget_sync(session_factory, transport, org_id=org_id, room=room)


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

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
    set_service("meeting-service-consumer")
    serve_metrics()  # no-op unless METRICS_PORT is set
    session_factory = get_sessionmaker()
    transport = select_transport()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("meeting-events consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            topic_name = msg.topic() or TOPIC
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, TOPIC, _ts if _ts_type else None)
            value = deserializer(raw_value, SerializationContext(topic_name, MessageField.VALUE))
            await process_decoded_message(
                value,
                raw_value=raw_value,
                key=msg.key(),
                session_factory=session_factory,
                redis=redis,
                dlq_producer=dlq_producer,
                transport=transport,
            )
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
