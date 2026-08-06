"""Outbox relay: deterministic at-least-once semantics (fake producer) + a real-bus round-trip."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.events import EventEnvelope
from libs.shared.relay import relay_once


class _FakeProducer:
    def __init__(self, undelivered: int = 0, poison_subject: str | None = None):
        self.produced: list = []
        self._undelivered = undelivered
        self._poison_subject = poison_subject

    def serialize(self, subject, value):
        if self._poison_subject and subject == self._poison_subject:
            raise ValueError(f"cannot serialize against {subject}")
        return json.dumps(value).encode()

    def produce(self, *, topic, key, value):
        self.produced.append((topic, key, value))

    def flush(self):
        return self._undelivered

    def topics(self) -> list[str]:
        return [t for t, _k, _v in self.produced]


async def _seed_org(conn, org_id: uuid.UUID) -> None:
    """Outbox rows need a real tenant root since defect D6 was fixed: `org_id` now carries a
    foreign key to `organizations`, so an orphan row is no longer insertable."""
    await conn.execute(
        text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
        {"id": str(org_id), "slug": f"relay-{org_id}"},
    )


async def _seed(engine, n: int, topic: str = "meeting-events") -> None:
    async with engine.begin() as conn:
        for _ in range(n):
            org_id = uuid.uuid4()
            await _seed_org(conn, org_id)
            await conn.execute(
                text(
                    "INSERT INTO outbox (org_id, topic, partition_key, idempotency_key, "
                    "schema_subject, payload) VALUES (:o, :t, :pk, :ik, :s, CAST(:p AS jsonb))"
                ),
                {
                    "o": str(org_id),
                    "t": topic,
                    "pk": str(org_id),
                    "ik": str(uuid.uuid4()),
                    "s": f"{topic}-value",
                    "p": json.dumps({"header": {"org_id": str(org_id)}, "x": 1}),
                },
            )


async def test_relay_at_least_once(migrated_db):
    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM outbox"))
    await _seed(engine, 2)

    # Crash: flush reports an undelivered message -> relay_once raises, nothing is marked.
    with pytest.raises(RuntimeError):
        async with session_factory() as s, s.begin():
            await relay_once(s, _FakeProducer(undelivered=1))
    async with engine.begin() as conn:
        pending = (
            await conn.execute(text("SELECT count(*) FROM outbox WHERE published_at IS NULL"))
        ).scalar_one()
    assert pending == 2, "crash-before-mark must leave rows to re-publish"

    # Healthy drain marks them; a second pass publishes nothing.
    good = _FakeProducer()
    async with session_factory() as s, s.begin():
        n = await relay_once(s, good)
    assert n == 2 and len(good.produced) == 2
    async with session_factory() as s, s.begin():
        assert await relay_once(s, good) == 0


async def test_poison_row_does_not_block_healthy_rows(migrated_db):
    """Defect D5, first half. One unserializable row must not stall every other tenant.

    Plan 0's relay_once serialized the whole batch before marking any of it, with no
    per-row handling, so a single bad row raised, the transaction rolled back, the process
    died, and the supervisor restarted it onto the same row. A permanent head-of-line
    block across every tenant, because the outbox is shared.
    """
    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM outbox"))
    await _seed(engine, 2, topic="meeting-events")
    await _seed(engine, 1, topic="tenant-events")  # this one will fail to serialize

    producer = _FakeProducer(poison_subject="tenant-events-value")
    async with session_factory() as s, s.begin():
        published = await relay_once(s, producer)

    assert published == 2, "the two healthy rows must still publish"
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT attempts, published_at FROM outbox WHERE topic='tenant-events'")
                )
            )
            .mappings()
            .one()
        )
    assert row["attempts"] == 1, "the failed row must record the attempt (it never did before)"
    assert row["published_at"] is None, "a retryable failure stays pending"


async def test_poison_row_is_dead_lettered_after_max_attempts(migrated_db):
    """Defect D5, second half. A permanently bad row must leave the queue via the DLQ."""
    from libs.shared.relay import MAX_ATTEMPTS

    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as conn:
        await conn.execute(text("DELETE FROM outbox"))
    await _seed(engine, 1, topic="tenant-events")

    producer = _FakeProducer(poison_subject="tenant-events-value")
    for _ in range(MAX_ATTEMPTS):
        async with session_factory() as s, s.begin():
            await relay_once(s, producer)

    assert "tenant-events.dlq" in producer.topics(), "poison row must be routed to the DLQ"
    async with engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    text("SELECT attempts, published_at FROM outbox WHERE topic = 'tenant-events'")
                )
            )
            .mappings()
            .one()
        )
    assert row["attempts"] >= MAX_ATTEMPTS
    assert row["published_at"] is not None, "dead-lettered row must stop being re-selected"

    # And the queue is genuinely drained: another pass finds nothing.
    async with session_factory() as s, s.begin():
        assert await relay_once(s, producer) == 0


async def test_relay_real_bus_roundtrip(migrated_db):
    import httpx

    sr_url = os.environ["SCHEMA_REGISTRY_URL"]
    brokers = os.environ["REDPANDA_BROKERS"]
    try:
        r = httpx.get(f"{sr_url}/subjects/meeting-events-value/versions/latest", timeout=3)
        if r.status_code != 200:
            pytest.skip(
                "meeting-events-value subject not registered (run mytheus-events make register)"
            )
    except Exception:
        pytest.skip("schema registry unreachable")

    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    from services.relay.main import ConfluentRelayProducer

    topic = "meeting-events"
    admin = AdminClient({"bootstrap.servers": brokers})
    for _t, fut in admin.create_topics([NewTopic(topic, 1, 1)]).items():
        try:
            fut.result()
        except Exception:
            pass  # already exists

    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Subscribe and pin every assigned partition's start offset to its CURRENT high watermark
    # BEFORE the outbox row is even inserted - not merely before producing. `meeting-events` is
    # a long-lived shared topic (this test has been appending to it, across every run, all
    # day) - scanning from `earliest` is wrong in any topic with history: with hundreds of
    # prior messages spread across many partitions, a bounded scan from the beginning can
    # exhaust its budget before ever reaching the message this run just produced, and a bigger
    # budget only pushes the same failure out to a bigger topic. Pinning to "whatever is newest
    # right now, per partition" means this test's correctness never depends on how much history
    # the topic already has - it holds on an empty topic and on one with a million messages the
    # same way, because it only ever reads messages produced AFTER this point, never the
    # backlog. It must happen before the INSERT, not just before this test's own `relay_once`
    # call: this dev stack also runs a real relay worker (`python -m services.relay.main`)
    # against the same Postgres and broker, and that worker can drain a newly inserted row
    # before this test ever gets to it (see the race note below) - if the pin happened after
    # the INSERT, that worker's produce could land before the pin and be missed entirely.
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "latest",  # defensive fallback; the on_assign below is what
            # actually pins the offset, so this only matters if that somehow does not run
        }
    )
    assigned_partitions: list[int] = []

    def _pin_to_latest(consumer: Consumer, partitions: list) -> None:
        for p in partitions:
            _, high_watermark = consumer.get_watermark_offsets(p, timeout=10, cached=False)
            p.offset = high_watermark
        consumer.assign(partitions)
        assigned_partitions.extend(p.partition for p in partitions)

    consumer.subscribe([topic], on_assign=_pin_to_latest)

    # Block until the subscription above has actually run (on_assign fires from inside poll()),
    # so "pinned to latest" is resolved to real offsets before anything is produced.
    assign_deadline = time.monotonic() + 15
    while not assigned_partitions and time.monotonic() < assign_deadline:
        consumer.poll(0.5)
    assert assigned_partitions, (
        f"consumer group never received a partition assignment for {topic!r} within 15s - "
        "cannot prove the round trip without one"
    )

    org = str(uuid.uuid4())
    env = EventEnvelope(
        topic=topic,
        payload={
            "meeting_id": str(uuid.uuid4()),
            "event_type": "created",
            "livekit_room": "room-1",
        },
        org_id=org,
        producer="core-api",
        idempotency_key=f"meeting:{uuid.uuid4()}",
    )
    async with engine.begin() as conn:
        await _seed_org(conn, uuid.UUID(org))
        await conn.execute(
            text(
                "INSERT INTO outbox (org_id, topic, partition_key, idempotency_key, "
                "schema_subject, payload) VALUES (:o, :t, :pk, :ik, :s, CAST(:p AS jsonb))"
            ),
            {
                "o": org,
                "t": topic,
                "pk": org,
                "ik": env.idempotency_key,
                "s": "meeting-events-value",
                "p": json.dumps(env.to_value()),
            },
        )

    try:
        producer = ConfluentRelayProducer(brokers, sr_url)
        async with session_factory() as s, s.begin():
            published = await relay_once(s, producer)

        # `published == 0` here is NOT by itself proof this call failed: a real relay worker
        # (`python -m services.relay.main`) is part of the shared dev stack this test runs
        # against, and `relay_once`'s `SELECT ... FOR UPDATE SKIP LOCKED` means whichever of
        # the two processes reaches the row first drains it - confirmed by direct reproduction
        # (`published: 0` with the row's `published_at` already non-NULL microseconds later).
        # What this test actually needs is "the row got published, by *a* relay_once call, not
        # necessarily this one" - so on a 0 from this call, poll the row directly for a bounded
        # window before concluding it is stuck.
        if published == 0:
            publish_deadline = time.monotonic() + 10
            row_published = False
            while time.monotonic() < publish_deadline:
                async with engine.connect() as conn:
                    published_at = (
                        await conn.execute(
                            text("SELECT published_at FROM outbox WHERE idempotency_key = :ik"),
                            {"ik": env.idempotency_key},
                        )
                    ).scalar_one()
                if published_at is not None:
                    row_published = True
                    break
                await asyncio.sleep(0.2)
            assert row_published, (
                "outbox row was never published, by this call or a concurrent relay worker, "
                "within 10s"
            )

        # Consume the message back and confirm org_id survived the bus. Deadline-based, not a
        # fixed poll count: a fixed count still silently assumes how long delivery/visibility
        # takes, which is exactly the assumption that broke here once the topic grew - a wall-
        # clock budget is what "search until it's really not showing up, then say so
        # accurately" actually means.
        deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
        found = None
        scanned = 0
        partitions_seen: set[int] = set()
        search_deadline = time.monotonic() + 30
        while found is None and time.monotonic() < search_deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            scanned += 1
            partitions_seen.add(msg.partition())
            value = deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
            if value["header"]["idempotency_key"] == env.idempotency_key:
                found = value
    finally:
        consumer.close()

    # A message-not-found failure here used to read "relayed message not found on the bus",
    # which points at the relay - and is exactly why this test's real failure mode (the search
    # window exhausted, not the message ever being absent) got misread as a broker flake for
    # hours. Say what this test actually knows.
    assert found is not None, (
        f"did not find idempotency_key={env.idempotency_key!r} on {topic!r} within the 30s "
        f"search window - scanned {scanned} message(s) across {len(partitions_seen)} "
        f"partition(s) ({sorted(partitions_seen)}) of {len(assigned_partitions)} assigned. "
        "The search window was exhausted; this does NOT mean the message is absent from the "
        "topic - relay_once() reported published >= 1 above, so the produce itself succeeded."
    )
    assert str(found["header"]["org_id"]) == org  # uuid logicalType deserializes to UUID

    async with engine.begin() as conn:
        pending = (
            await conn.execute(
                text("SELECT published_at FROM outbox WHERE idempotency_key = :ik"),
                {"ik": env.idempotency_key},
            )
        ).scalar_one()
    assert pending is not None, "row should be marked published"
