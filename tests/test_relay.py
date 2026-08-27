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

    from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    from services.relay.main import ConfluentRelayProducer

    # A PRIVATE, single-partition topic per run, instead of the shared `meeting-events`.
    #
    # This is the fix for the flake that dogged this test across three bands (green in
    # isolation, red in a full suite). Every previous attempt tuned the SEARCH - pin to the
    # high watermark, widen the budget - while leaving the test reading a long-lived topic that
    # this suite, a live `python -m services.relay.main` worker, and any other agent on the
    # shared dev stack all append to concurrently. That left the outcome dependent on how much
    # unrelated traffic happened to arrive during the window, which is exactly why load (a full
    # suite) flipped it red and quiet (one test) kept it green. No timeout can fix that; it is a
    # question of what the test is reading, not how long it waits.
    #
    # A private topic makes the round trip deterministic: exactly ONE message can ever exist on
    # it, so it is found on the first poll that returns anything. It also removes the whole
    # class of neighbouring problems at once - no backlog to scan past, no foreign message to
    # deserialize, no interference from the concurrent relay worker's other rows.
    #
    # Fidelity is preserved, because the destination topic and the Avro subject are independent
    # in the code under test: `relay_once` passes `r["schema_subject"]` to `serialize()` and
    # `r["topic"]` to `produce()` separately (libs/shared/relay.py), and
    # `ConfluentRelayProducer.serialize` derives its SerializationContext from the SUBJECT
    # (`subject[:-6]`), never from the destination. So the payload is still serialized against
    # the real, registered `meeting-events-value` schema - which is what this test exists to
    # prove survives the bus - and only the topic it lands on is private.
    topic = f"meeting-events-roundtrip-{uuid.uuid4().hex}"
    admin = AdminClient({"bootstrap.servers": brokers})
    for _t, fut in admin.create_topics([NewTopic(topic, 1, 1)]).items():
        fut.result()  # a fresh name cannot already exist; a failure here is real, so let it raise

    engine = create_async_engine(migrated_db)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # `assign`, not `subscribe`: with a known single-partition topic there is nothing to
    # discover, so this skips consumer-group coordination entirely - no rebalance, no
    # `on_assign` callback, no waiting to learn which partitions we got, and no chance of a
    # second rebalance re-pinning the start offset past the message. OFFSET_BEGINNING is
    # correct here precisely because the topic is private and empty: the one thing it can ever
    # return is the message this test produced.
    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": f"test-{uuid.uuid4()}",  # required by librdkafka even when assigning
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])

    org = str(uuid.uuid4())
    # Built against the CANONICAL topic, not the private destination, so `env.schema_subject`
    # resolves to the real `meeting-events-value` and matches the subject written to the outbox
    # row below. The private name belongs only in the row's `topic` column (the destination);
    # putting it here too would silently make `env.schema_subject` a subject that was never
    # registered - harmless today, since the INSERT names the subject explicitly, but a trap
    # for whoever next reads this and trusts the property.
    env = EventEnvelope(
        topic="meeting-events",
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

        # Consume the message back and confirm org_id survived the bus. The loop remains
        # deadline-bounded only so a broker that never delivers fails in 30s instead of hanging
        # forever - it is no longer a "search". Earlier versions of this test had to budget for
        # scanning past unrelated traffic on the shared topic, and that budget was the thing
        # that ran out under load; on a private topic the first message that arrives is the
        # message, so the deadline is now a backstop that a healthy run never approaches.
        deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
        found = None
        scanned = 0
        search_deadline = time.monotonic() + 30
        while found is None and time.monotonic() < search_deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            scanned += 1
            value = deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
            if value["header"]["idempotency_key"] == env.idempotency_key:
                found = value
    finally:
        consumer.close()
        # Private per-run topic, so delete it rather than leaving one behind on the dev broker
        # for every run this suite has ever made. Best-effort: a leaked topic is untidy, not a
        # test failure, and swallowing here keeps a cleanup hiccup from masking the real result.
        try:
            for _t, fut in admin.delete_topics([topic]).items():
                fut.result(timeout=30)
        except Exception:  # noqa: BLE001 - cleanup must never turn a passing test red
            pass

    # The 30s budget is now a liveness backstop, not a search window. On a private topic
    # holding exactly one message there is nothing to scan past, so anything other than an
    # immediate find means the message genuinely never arrived - which is a real failure worth
    # reporting, not the "we ran out of time looking" ambiguity this assertion used to have to
    # hedge about.
    assert found is not None, (
        f"did not find idempotency_key={env.idempotency_key!r} on the private topic {topic!r} "
        f"within 30s, having scanned {scanned} message(s). The topic is created empty by this "
        "test and only the relay writes to it, so the message is genuinely absent: the produce "
        "did not happen, or it went somewhere else."
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
