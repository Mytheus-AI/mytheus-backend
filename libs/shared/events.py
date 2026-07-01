"""Canonical Redpanda event envelope.

An EventEnvelope is the frozen EventEnvelopeHeader (8 fields) plus a topic payload. `payload`
holds ONLY the topic-specific fields; `to_value()` composes the full Avro value
``{header, **payload}`` that the outbox stores and the relay serializes against ``{topic}-value``.
Topics are partitioned by org_id (the partition key).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def _now_micros() -> int:
    return int(datetime.now(UTC).timestamp() * 1_000_000)


@dataclass
class EventEnvelope:
    topic: str
    payload: dict[str, Any]  # topic-specific fields ONLY (no header)
    org_id: str
    producer: str  # owning service name (kebab-case)
    idempotency_key: str
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    schema_version: int = 1
    occurred_at: int = field(default_factory=_now_micros)  # timestamp-micros
    correlation_id: str | None = None
    causation_id: str | None = None

    @property
    def schema_subject(self) -> str:
        return f"{self.topic}-value"

    @property
    def partition_key(self) -> str:
        return self.org_id

    def header(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "idempotency_key": self.idempotency_key,
            "org_id": self.org_id,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "occurred_at": self.occurred_at,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
        }

    def to_value(self) -> dict[str, Any]:
        """The full Avro-serialisable event value: header + topic payload."""
        return {"header": self.header(), **self.payload}
