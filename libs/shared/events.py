"""Canonical Redpanda event envelope.

Every event produced by this repo is wrapped in EventEnvelope so consumers
always have org_id and idempotency_key without parsing the Avro payload.
Topics are partitioned by org_id (the partition key).
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class EventEnvelope:
    topic: str
    payload: Any                        # Avro-serialisable dict
    org_id: str
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    schema_version: int = 1

    def partition_key(self) -> bytes:
        return self.org_id.encode()
