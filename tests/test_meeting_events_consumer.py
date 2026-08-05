"""`services/meeting_service/consumer.py`'s `handle_meeting_event` and `process_decoded_message`:
Task 4, Plan 3-4 section 3.1 step 6, fix round 1.

Exercises the testable core directly against a real Postgres (`app.current_org` / RLS
re-assertion, the `meeting_rooms` writes, the inbox dedupe) and `fakeredis`, matching
`tests/test_presence_events_consumer.py`'s convention.

Covers: `meeting.created` pre-creates the `meeting_rooms` row and reports a pending LiveKit
sync (`HandledMeetingEvent.pending_room_sync`); `meeting.ended` marks it ended; redelivery of
either is a no-op (acceptance check 5); a `livekit_room: null` payload - exactly what the
schema's nullable field permits and the stale generated client didn't (the Task 4 brief's
PREREQUISITE) - does not break the consumer; a poison message (missing header, a non-UUID
`org_id`/`meeting_id`, an unrecognized `event_type`) is never applied; a poison message routed
through `process_decoded_message` lands in `meeting-events.dlq` without blocking a subsequent
healthy message (acceptance check 6); and `process_decoded_message` actually completes the
LiveKit sync for a `created` event via a detached background task, without `handle_meeting_event`
itself ever touching the transport (fix round 1's fix for the crash-loop/partition-block
finding - see the module docstring).
"""

from __future__ import annotations

import asyncio
import json
import uuid

import fakeredis.aioredis
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from services.meeting_service import consumer as consumer_mod
from services.meeting_service.consumer import (
    PoisonMessage,
    handle_meeting_event,
    process_decoded_message,
    route_to_dlq,
)
from services.meeting_service.transport import FakeTransport
from tests.conftest import APP_URL, seed_org


class _FakeDlqProducer:
    def __init__(self) -> None:
        self.produced: list[tuple[str, bytes | None, bytes]] = []

    def produce(self, *, topic: str, key: bytes | None, value: bytes) -> None:
        self.produced.append((topic, key, value))

    def flush(self, timeout: float = 10) -> int:
        return 0

    def topics(self) -> list[str]:
        return [t for t, _k, _v in self.produced]


async def _drain_background_sync_tasks() -> None:
    """`process_decoded_message` fires `sync_livekit_metadata` as a detached task and does NOT
    await it - that is the whole point of fix round 1 (a slow/failing LiveKit call must never
    delay `consumer.commit`). Tests that need to observe its effect await it explicitly here;
    production code (`run_consumer`) never does."""
    tasks = list(consumer_mod._background_sync_tasks)
    if tasks:
        await asyncio.gather(*tasks)


def _envelope(org_id: str, idempotency_key: str, **payload) -> dict:
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": idempotency_key,
            "org_id": org_id,
            "schema_version": 1,
            "producer": "core-api",
            "occurred_at": 0,
            "correlation_id": None,
            "causation_id": None,
        },
        **payload,
    }


async def _seed_meeting(engine, org_id: uuid.UUID, meeting_id: uuid.UUID) -> None:
    """`meeting_rooms.meeting_id` carries a real FK to `meetings(id)`: a `created` event about
    a meeting core-api never wrote is not a case this consumer needs to tolerate (core-api is
    the sole writer of `meetings`, and produces `meeting-events` only for rows it just wrote)."""
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO meetings (id, org_id, title) VALUES (:id, :org, 'M')"),
            {"id": str(meeting_id), "org": str(org_id)},
        )


async def _meeting_room(engine, org_id: uuid.UUID, meeting_id: uuid.UUID) -> dict | None:
    async with engine.connect() as conn, conn.begin():
        await conn.execute(
            text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_id)}
        )
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT livekit_room, status, livekit_metadata_synced_at "
                        "FROM meeting_rooms WHERE org_id = :o AND meeting_id = :m"
                    ),
                    {"o": str(org_id), "m": str(meeting_id)},
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row else None


# --- acceptance check 5: created pre-creates, ended marks ended, redelivery is a no-op --------


async def test_meeting_created_pre_creates_the_meeting_rooms_row(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    key = str(uuid.uuid4())
    value = _envelope(
        str(org_id), key, meeting_id=str(meeting_id), event_type="created", livekit_room=None
    )

    try:
        async with app_engine.connect() as conn, conn.begin():
            result = await handle_meeting_event(conn, redis, value)
        assert result.applied is True

        row = await _meeting_room(app_engine, org_id, meeting_id)
        assert row is not None
        assert row["livekit_room"] == f"meeting-{meeting_id}"
        assert row["status"] == "pending"
        assert row["livekit_metadata_synced_at"] is None, "not synced yet - no transport call"

        # `handle_meeting_event` makes NO transport call itself (fix round 1); it reports that
        # a sync is still needed, for `process_decoded_message` to act on afterward.
        assert result.pending_room_sync == (org_id, f"meeting-{meeting_id}")
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_meeting_ended_marks_status_ended(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me2")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)

    try:
        # Pre-create via a "created" event first, as the real lifecycle would.
        async with app_engine.connect() as conn, conn.begin():
            await handle_meeting_event(
                conn,
                redis,
                _envelope(
                    str(org_id),
                    str(uuid.uuid4()),
                    meeting_id=str(meeting_id),
                    event_type="created",
                    livekit_room=f"meeting-{meeting_id}",
                ),
            )

        async with app_engine.connect() as conn, conn.begin():
            result = await handle_meeting_event(
                conn,
                redis,
                _envelope(
                    str(org_id),
                    str(uuid.uuid4()),
                    meeting_id=str(meeting_id),
                    event_type="ended",
                    livekit_room=None,
                ),
            )
        assert result.applied is True
        assert result.pending_room_sync is None, "'ended' never establishes a room"

        row = await _meeting_room(app_engine, org_id, meeting_id)
        assert row["status"] == "ended"
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_meeting_ended_with_no_prior_room_is_a_harmless_no_op(migrated_db):
    """A meeting can end without this service ever having pre-created a room (e.g. it was
    cancelled before anyone joined). Must not raise."""
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me3")

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    meeting_id = uuid.uuid4()

    try:
        async with app_engine.connect() as conn, conn.begin():
            result = await handle_meeting_event(
                conn,
                redis,
                _envelope(
                    str(org_id), str(uuid.uuid4()), meeting_id=str(meeting_id), event_type="ended"
                ),
            )
        assert result.applied is True  # applied (dedupe recorded); no row to change
        assert await _meeting_room(app_engine, org_id, meeting_id) is None
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_redelivery_of_created_is_a_dedupe_no_op(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me4")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    key = str(uuid.uuid4())
    value = _envelope(str(org_id), key, meeting_id=str(meeting_id), event_type="created")

    try:
        async with app_engine.connect() as conn, conn.begin():
            first = await handle_meeting_event(conn, redis, value)
        async with app_engine.connect() as conn, conn.begin():
            second = await handle_meeting_event(conn, redis, value)
        assert first.applied is True
        assert first.pending_room_sync == (org_id, f"meeting-{meeting_id}")
        assert second.applied is False, "a redelivered idempotency_key must be a no-op"
        assert second.pending_room_sync is None

        async with app_engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_id)}
            )
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT count(*) AS n FROM meeting_rooms "
                            "WHERE org_id = :o AND meeting_id = :m"
                        ),
                        {"o": str(org_id), "m": str(meeting_id)},
                    )
                )
                .mappings()
                .first()
            )
        assert rows["n"] == 1, "redelivery must not create a second row"
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_redelivery_of_ended_is_a_dedupe_no_op(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me5")

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    meeting_id = uuid.uuid4()
    key = str(uuid.uuid4())
    value = _envelope(str(org_id), key, meeting_id=str(meeting_id), event_type="ended")

    try:
        async with app_engine.connect() as conn, conn.begin():
            first = await handle_meeting_event(conn, redis, value)
        async with app_engine.connect() as conn, conn.begin():
            second = await handle_meeting_event(conn, redis, value)
        assert first.applied is True
        assert second.applied is False
    finally:
        await app_engine.dispose()
        await su.dispose()


# --- PREREQUISITE fix regression: a null livekit_room must never break this consumer ----------


async def test_null_livekit_room_does_not_break_the_consumer(migrated_db):
    """Exactly the payload shape the schema permits (`["null", "string"]`, default null) and
    the STALE generated client (fixed separately, in mytheus-events) did not: this consumer
    parses the raw dict directly and never reads `livekit_room` at all, so this must succeed
    with no error regardless."""
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me6")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    value = _envelope(
        str(org_id),
        str(uuid.uuid4()),
        meeting_id=str(meeting_id),
        event_type="created",
        livekit_room=None,
    )

    try:
        async with app_engine.connect() as conn, conn.begin():
            result = await handle_meeting_event(conn, redis, value)
        assert result.applied is True
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_meeting_updated_is_a_recognized_no_op(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me7")

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    meeting_id = uuid.uuid4()

    try:
        async with app_engine.connect() as conn, conn.begin():
            result = await handle_meeting_event(
                conn,
                redis,
                _envelope(
                    str(org_id), str(uuid.uuid4()), meeting_id=str(meeting_id), event_type="updated"
                ),
            )
        assert result.applied is True
        assert result.pending_room_sync is None
        assert await _meeting_room(app_engine, org_id, meeting_id) is None
    finally:
        await app_engine.dispose()
        await su.dispose()


# --- poison messages ------------------------------------------------------------------------


async def test_a_missing_header_field_is_a_poison_message():
    redis = fakeredis.aioredis.FakeRedis()
    value = {"header": {"org_id": None, "idempotency_key": None}, "meeting_id": str(uuid.uuid4())}

    with pytest.raises(PoisonMessage):
        await handle_meeting_event(None, redis, value)


async def test_a_non_uuid_org_id_header_is_a_poison_message():
    redis = fakeredis.aioredis.FakeRedis()
    value = _envelope(
        "org_notAUuid", str(uuid.uuid4()), meeting_id=str(uuid.uuid4()), event_type="created"
    )

    with pytest.raises(PoisonMessage):
        await handle_meeting_event(None, redis, value)


async def test_a_non_uuid_meeting_id_is_a_poison_message():
    redis = fakeredis.aioredis.FakeRedis()
    value = _envelope(
        str(uuid.uuid4()), str(uuid.uuid4()), meeting_id="not-a-uuid", event_type="created"
    )

    with pytest.raises(PoisonMessage):
        await handle_meeting_event(None, redis, value)


async def test_an_unrecognized_event_type_is_a_poison_message():
    redis = fakeredis.aioredis.FakeRedis()
    value = _envelope(
        str(uuid.uuid4()), str(uuid.uuid4()), meeting_id=str(uuid.uuid4()), event_type="deleted"
    )

    with pytest.raises(PoisonMessage):
        await handle_meeting_event(None, redis, value)


# --- fix round 1: process_decoded_message completes the LiveKit sync, out of band -------------


async def test_process_decoded_message_syncs_livekit_metadata_for_a_created_event(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me9")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    session_factory = async_sessionmaker(app_engine, expire_on_commit=False)
    transport = FakeTransport()
    producer = _FakeDlqProducer()

    value = _envelope(
        str(org_id), str(uuid.uuid4()), meeting_id=str(meeting_id), event_type="created"
    )

    try:
        await process_decoded_message(
            value,
            raw_value=b"created-bytes",
            key=b"created-key",
            session_factory=session_factory,
            redis=redis,
            dlq_producer=producer,
            transport=transport,
        )
        # `process_decoded_message` does NOT await the sync (that is the point of fix round 1
        # - it must return immediately so `consumer.commit` is never delayed). Drain it
        # explicitly to observe what it did.
        await _drain_background_sync_tasks()

        room_calls = [c for c in transport.calls if c.method == "ensure_room"]
        assert len(room_calls) == 1
        assert room_calls[0].kwargs == {"org_id": org_id, "room": f"meeting-{meeting_id}"}
        assert transport.room_metadata[f"meeting-{meeting_id}"] == json.dumps(
            {"org_id": str(org_id)}
        )

        row = await _meeting_room(app_engine, org_id, meeting_id)
        assert row["livekit_metadata_synced_at"] is not None
    finally:
        await app_engine.dispose()
        await su.dispose()


async def test_a_failing_transport_does_not_raise_out_of_process_decoded_message(migrated_db):
    """The Critical finding, directly: a LiveKit failure during the out-of-band sync must
    never propagate back into `process_decoded_message` (and from there, `run_consumer`'s poll
    loop). The row stays unsynced - exactly the durable "retry on the next mint/consume" state
    - and no exception escapes."""
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me10")
    meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    session_factory = async_sessionmaker(app_engine, expire_on_commit=False)

    class _AlwaysFailsEnsureRoom(FakeTransport):
        async def ensure_room(self, *, org_id, room):  # type: ignore[override]
            raise ConnectionError("LiveKit is down")

    transport = _AlwaysFailsEnsureRoom()
    producer = _FakeDlqProducer()

    value = _envelope(
        str(org_id), str(uuid.uuid4()), meeting_id=str(meeting_id), event_type="created"
    )

    try:
        # Must not raise.
        await process_decoded_message(
            value,
            raw_value=b"created-bytes",
            key=b"created-key",
            session_factory=session_factory,
            redis=redis,
            dlq_producer=producer,
            transport=transport,
        )
        await _drain_background_sync_tasks()

        assert producer.produced == [], "a transient LiveKit failure is not a poison message"

        row = await _meeting_room(app_engine, org_id, meeting_id)
        assert row is not None, "the meeting_rooms row itself was still created"
        assert row["livekit_metadata_synced_at"] is None, "stays unsynced - retried later"
    finally:
        await app_engine.dispose()
        await su.dispose()


# --- acceptance check 6: poison -> DLQ, does not block the partition --------------------------


def test_route_to_dlq_publishes_the_raw_payload_and_error():
    producer = _FakeDlqProducer()
    route_to_dlq(
        producer,
        topic="meeting-events",
        key=b"some-key",
        raw_value=b"\x00garbage",
        error=PoisonMessage("bad header"),
    )

    assert "meeting-events.dlq" in producer.topics()
    topic, key, value = producer.produced[0]
    assert topic == "meeting-events.dlq"
    assert key == b"some-key"
    body = json.loads(value)
    assert "bad header" in body["error"]
    assert body["raw"] == b"\x00garbage".hex()


async def test_a_poison_message_lands_in_the_dlq_and_does_not_block_the_partition(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = uuid.uuid4()
    await seed_org(su, org_id, slug_prefix="me8")
    good_meeting_id = uuid.uuid4()
    await _seed_meeting(su, org_id, good_meeting_id)

    redis = fakeredis.aioredis.FakeRedis()
    app_engine = create_async_engine(APP_URL)
    session_factory = async_sessionmaker(app_engine, expire_on_commit=False)
    producer = _FakeDlqProducer()
    transport = FakeTransport()

    poison_value = _envelope(
        "not-a-uuid-org", str(uuid.uuid4()), meeting_id=str(uuid.uuid4()), event_type="created"
    )
    good_value = _envelope(
        str(org_id), str(uuid.uuid4()), meeting_id=str(good_meeting_id), event_type="created"
    )

    try:
        # The poison message: routed to the DLQ, no exception escapes.
        await process_decoded_message(
            poison_value,
            raw_value=b"poison-bytes",
            key=b"poison-key",
            session_factory=session_factory,
            redis=redis,
            dlq_producer=producer,
            transport=transport,
        )
        assert "meeting-events.dlq" in producer.topics()

        # A healthy message right after it is NOT blocked by the poison one.
        await process_decoded_message(
            good_value,
            raw_value=b"good-bytes",
            key=b"good-key",
            session_factory=session_factory,
            redis=redis,
            dlq_producer=producer,
            transport=transport,
        )
        await _drain_background_sync_tasks()

        row = await _meeting_room(app_engine, org_id, good_meeting_id)
        assert row is not None, "the message after the poison one must still be applied"
    finally:
        await app_engine.dispose()
        await su.dispose()
