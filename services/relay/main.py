"""Standalone transactional-outbox relay process.

Run as a worker in the same deployable as the web process (a worker service on the VPS, not a
separate cluster): ``python -m services.relay.main``. It is the ONLY sanctioned producer:
services write the outbox inside their transaction, the relay drains it to Redpanda.

Serialization is Confluent-wire-format Avro (magic byte + schema id) against the {topic}-value
subject registered by mytheus-events. The relay connects with a role that bypasses RLS so it can
drain every tenant's rows.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.relay import relay_once

_IDLE_SLEEP_SECONDS = 1.0


class ConfluentRelayProducer:
    """RelayProducer backed by confluent-kafka + Schema Registry (use-latest-version)."""

    def __init__(self, brokers: str, sr_url: str) -> None:
        self._producer = KafkaProducer(
            {"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True}
        )
        self._sr = SchemaRegistryClient({"url": sr_url})
        self._serializers: dict[str, AvroSerializer] = {}

    def _serializer(self, subject: str) -> AvroSerializer:
        ser = self._serializers.get(subject)
        if ser is None:
            ser = AvroSerializer(
                self._sr,
                schema_str=None,
                conf={"auto.register.schemas": False, "use.latest.version": True},
            )
            self._serializers[subject] = ser
        return ser

    def serialize(self, subject: str, value: dict[str, Any]) -> bytes:
        topic = subject[:-6] if subject.endswith("-value") else subject
        ctx = SerializationContext(topic, MessageField.VALUE)
        result = self._serializer(subject)(value, ctx)
        assert result is not None  # AvroSerializer returns None only for a None input
        return result

    def produce(self, *, topic: str, key: bytes, value: bytes) -> None:
        self._producer.produce(topic=topic, key=key, value=value)

    def flush(self) -> int:
        return self._producer.flush(10)


async def run() -> None:
    engine = create_async_engine(os.environ["DATABASE_URL"])
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    producer = ConfluentRelayProducer(
        os.environ["REDPANDA_BROKERS"], os.environ["SCHEMA_REGISTRY_URL"]
    )
    while True:
        async with session_factory() as session:
            async with session.begin():
                published = await relay_once(session, producer)
        if published == 0:
            await asyncio.sleep(_IDLE_SLEEP_SECONDS)


if __name__ == "__main__":
    asyncio.run(run())
