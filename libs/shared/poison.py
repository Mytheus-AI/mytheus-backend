"""Which decode failures mean "this record can never be read" (dead-letter it and move on).

A record that fails like this fails identically on every retry, so a consumer that lets the
exception escape restarts, re-reads the same offset and stops for every tenant. The Avro reader
raises ValueError / TypeError / EOFError / IndexError for a malformed body; confluent_kafka's
AvroDeserializer raises SerializationError for a value that was not written by a Schema Registry
serializer at all (no magic byte / short frame: plain text, JSON, a stray probe). Registry
outages raise other types (SchemaRegistryError, transport errors) and are deliberately NOT here,
so those records are retried instead of being dead-lettered.
"""

from __future__ import annotations

try:
    from confluent_kafka.serialization import SerializationError
except ImportError:  # pragma: no cover - every consumer image ships confluent_kafka

    class SerializationError(Exception):  # type: ignore[no-redef]
        """Placeholder so this module imports without confluent_kafka."""


UNDECODABLE: tuple[type[BaseException], ...] = (
    ValueError,
    TypeError,
    EOFError,
    IndexError,
    SerializationError,
)
