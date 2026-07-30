"""Outbox relay: deterministic at-least-once semantics (fake producer) + a real-bus round-trip."""

from __future__ import annotations

import json
import os
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

    producer = ConfluentRelayProducer(brokers, sr_url)
    async with session_factory() as s, s.begin():
        published = await relay_once(s, producer)
    assert published >= 1

    # Consume the message back and confirm org_id survived the bus.
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
    found = None
    try:
        for _ in range(50):
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            value = deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
            if value["header"]["idempotency_key"] == env.idempotency_key:
                found = value
                break
    finally:
        consumer.close()
    assert found is not None, "relayed message not found on the bus"
    assert str(found["header"]["org_id"]) == org  # uuid logicalType deserializes to UUID

    async with engine.begin() as conn:
        pending = (
            await conn.execute(
                text("SELECT published_at FROM outbox WHERE idempotency_key = :ik"),
                {"ik": env.idempotency_key},
            )
        ).scalar_one()
    assert pending is not None, "row should be marked published"
