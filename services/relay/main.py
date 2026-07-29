"""Standalone transactional-outbox relay process.

Run as a worker in the same deployable as the web process (a worker service on the VPS, not a
separate cluster): ``python -m services.relay.main``. It is the ONLY sanctioned producer:
services write the outbox inside their transaction, the relay drains it to Redpanda.

Serialization is Confluent-wire-format Avro (magic byte + schema id) against the {topic}-value
subject registered by mytheus-events.

Cross-tenant reach: this drains every tenant's rows, but it does NOT do so with a
BYPASSRLS role. Section 3.3 gives `mytheus_relay` a single permissive `outbox_relay_drain`
policy on `outbox` alone, so the reach is scoped to one table and visible in `pg_policies`.
An application role holding BYPASSRLS would make RLS decorative everywhere at once, and CI
asserts that none does. Until 3.3 lands this process runs as `postgres`, which bypasses RLS
by accident of the image (defect D1), so its drain is currently unverified.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from confluent_kafka import Producer as KafkaProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.relay import relay_once

logger = logging.getLogger(__name__)

_IDLE_SLEEP_SECONDS = 1.0
# Backoff after an unexpected failure of the whole drain (broker down, database blip). Per-row
# poison is handled inside relay_once and never reaches here.
_ERROR_SLEEP_SECONDS = 5.0
_MAX_ERROR_SLEEP_SECONDS = 60.0


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
    backoff = _ERROR_SLEEP_SECONDS
    while True:
        # Defect D5. Plan 0's loop had no try, so ANY exception killed the process and the
        # supervisor restarted it straight onto the same batch. relay_once now isolates
        # per-row poison, but a whole-drain failure (broker unreachable, undelivered flush,
        # database blip) still has to be survivable rather than fatal.
        try:
            async with session_factory() as session:
                async with session.begin():
                    published = await relay_once(session, producer)
        except Exception:
            logger.exception("outbox relay: drain failed, retrying in %.0fs", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_ERROR_SLEEP_SECONDS)
            continue

        backoff = _ERROR_SLEEP_SECONDS
        if published == 0:
            # NOTE: an empty batch is currently indistinguishable from "RLS hid every row"
            # (defect D4). Section 3.3's outbox_relay_drain policy is what makes this
            # genuinely mean idle; a queue-depth metric to prove it lands with 3.11.
            await asyncio.sleep(_IDLE_SLEEP_SECONDS)


if __name__ == "__main__":
    asyncio.run(run())
