"""workflow-service's `meeting-events` consumer: start and signal MeetingLifecycleWorkflow.

What the topic really carries (`mytheus-events/schemas/meeting-events/v1.avsc`, produced only by
core-api's outbox): `event_type` in `created | updated | ended`. `created` comes from
`POST /api/v1/meetings`; `ended` from `PATCH /api/v1/meetings/{id}` to `ended` and from
meeting-service reporting the LiveKit room finished (`POST /internal/meetings/{id}/room-state`);
`updated` from every other change, including a room going live and a cancel. There is no
`started` type, so the workflow starts on `created`.

- `created`: start `meetinglifecycle-{meeting_id}` with `REJECT_DUPLICATE`. A redelivery (or a
  replay after the run finished) raises `WorkflowAlreadyStartedError`, which is the dedupe:
  nothing else is needed because Temporal owns the state and this service owns no table.
- `ended`: signal-with-start `meeting_ended` on the same id with
  `ALLOW_DUPLICATE_FAILED_ONLY`. A running execution is signalled; one that never started (its
  `created` was lost or predates this consumer) starts already ended and goes straight to
  post-processing; one that FAILED (it timed out waiting with `MeetingNeverEnded`, or its close
  failed) is started again, already ended, so post-processing still happens; one that
  COMPLETED (post-processing done) is left alone, so a replayed `ended` never runs it twice.
  Re-signalling a running one is harmless (the signal sets a flag).
- `updated`: ignored.

A message that can never apply (no header org, a non-UUID, an unknown event type) is poison:
dead-lettered to `meeting-events.dlq` and committed past. Temporal being unreachable is NOT
poison: the exception propagates, the offset is not committed, and the message is retried.

Run: `python -m services.workflow_service.consumer` (consumer group `workflow-service`).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from typing import Any, Literal

from temporalio.client import Client, WorkflowHandle
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from services.workflow_service.workflows.meeting_lifecycle import (
    ENDED_SIGNAL,
    MeetingLifecycleWorkflow,
    workflow_id_for,
)

logger = logging.getLogger(__name__)

TOPIC = "meeting-events"
CONSUMER_GROUP = "workflow-service"
_EVENT_TYPES = frozenset({"created", "updated", "ended"})

Outcome = Literal["started", "duplicate", "signalled", "ignored"]


class PoisonMessage(Exception):
    """The message can never be applied; route it to `meeting-events.dlq`."""


def _parse(value: dict[str, Any]) -> tuple[str, str, str]:
    header = value.get("header") or {}
    try:
        org_id = str(uuid.UUID(str(header.get("org_id"))))
        meeting_id = str(uuid.UUID(str(value.get("meeting_id"))))
    except ValueError as exc:
        raise PoisonMessage(f"meeting-events org_id/meeting_id is not a UUID: {value!r}") from exc
    event_type = str(value.get("event_type"))
    if event_type not in _EVENT_TYPES:
        raise PoisonMessage(f"meeting-events event_type is not recognized ({event_type!r})")
    return org_id, meeting_id, event_type


async def handle_meeting_event(
    client: Client, value: dict[str, Any], *, task_queue: str
) -> Outcome:
    org_id, meeting_id, event_type = _parse(value)
    if event_type == "updated":
        return "ignored"
    start: dict[str, Any] = {
        "args": [meeting_id, org_id],
        "id": workflow_id_for(meeting_id),
        "task_queue": task_queue,
        "id_reuse_policy": WorkflowIDReusePolicy.REJECT_DUPLICATE,
    }
    if event_type == "ended":
        start["start_signal"] = ENDED_SIGNAL
        start["id_reuse_policy"] = WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY
    try:
        handle: WorkflowHandle[Any, Any] = await client.start_workflow(
            MeetingLifecycleWorkflow.run, **start
        )
    except WorkflowAlreadyStartedError:
        logger.info("meeting-events %s: %s already has a run", event_type, meeting_id)
        return "duplicate"
    logger.info("meeting-events %s: %s -> %s", event_type, meeting_id, handle.id)
    return "signalled" if event_type == "ended" else "started"


async def run_consumer() -> None:  # pragma: no cover - thin harness, like the other consumers
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext

    from libs.shared.metrics import record_dlq, record_processed, serve_metrics, set_service

    set_service("workflow-service-consumer")
    serve_metrics()
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    task_queue = os.environ.get("TEMPORAL_TASK_QUEUE", "mytheus-backend")
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
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
    dlq = KafkaProducer({"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True})
    deserializer = AvroDeserializer(
        SchemaRegistryClient(
            {"url": os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")}
        )
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("workflow-service consumer error: %s", msg.error())
                continue
            raw = msg.value()
            try:
                try:
                    value = deserializer(raw, SerializationContext(TOPIC, MessageField.VALUE))
                except (ValueError, TypeError, EOFError, IndexError) as exc:
                    raise PoisonMessage(f"undecodable record: {exc}") from exc
                outcome = await handle_meeting_event(client, value, task_queue=task_queue)
                record_processed(CONSUMER_GROUP, TOPIC, outcome)
            except PoisonMessage as exc:
                logger.error("meeting-events: poison message routed to DLQ: %s", exc)
                dlq.produce(
                    topic=f"{TOPIC}.dlq",
                    key=msg.key(),
                    value=json.dumps(
                        {"error": str(exc), "raw": raw.hex() if raw else None}
                    ).encode(),
                )
                dlq.flush(10)
                record_dlq(CONSUMER_GROUP, TOPIC, "POISON")
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
