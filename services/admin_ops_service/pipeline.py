"""Dead-letter counts and consumer lag, read straight from Redpanda for the ops console.

Aggregates only: topic names, group names and message counts, never a message body.

- Dead letters: every topic named `{topic}.dlq` (the relay's and every consumer's poison
  route), counted as the sum over partitions of `high - low` watermark. A DLQ topic is never
  consumed, so that is the number of messages sitting in it.
- Consumer lag: for every consumer group, `high watermark - committed offset` per partition,
  summed per (group, topic). A partition with no committed offset yet counts from `low`.

Both are best effort. `None` means "could not be read" (brokers unset or unreachable) and the
console says so; an empty list means "read, and there is nothing". The Kafka admin calls are
blocking, so they run in a worker thread with short timeouts and never hold a request for long.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from libs.shared.kafka_auth import kafka_conf
from services.admin_ops_service.schemas import ConsumerLag, DlqCount

log = logging.getLogger(__name__)

TIMEOUT_S = 4.0
DLQ_SUFFIX = ".dlq"


def _brokers() -> str | None:
    return os.environ.get("REDPANDA_BROKERS") or None


def _watermarks(consumer: Any, topic: str, partitions: list[int]) -> dict[int, tuple[int, int]]:
    from confluent_kafka import TopicPartition

    out: dict[int, tuple[int, int]] = {}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=TIMEOUT_S)
        out[p] = (int(low), int(high))
    return out


def _read(brokers: str) -> tuple[list[DlqCount], list[ConsumerLag]]:
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(kafka_conf({"bootstrap.servers": brokers}))
    probe = Consumer(
        kafka_conf(
            {
                "bootstrap.servers": brokers,
                "group.id": "admin-ops-service.pipeline-probe",
                "enable.auto.commit": False,
            }
        )
    )
    try:
        meta = admin.list_topics(timeout=TIMEOUT_S)
        partitions = {
            name: sorted(t.partitions)
            for name, t in meta.topics.items()
            if not name.startswith("_") and t.error is None
        }
        marks: dict[str, dict[int, tuple[int, int]]] = {}

        def marks_for(topic: str) -> dict[int, tuple[int, int]]:
            if topic not in marks:
                marks[topic] = _watermarks(probe, topic, partitions.get(topic, []))
            return marks[topic]

        dlq = [
            DlqCount(topic=name, count=sum(high - low for low, high in marks_for(name).values()))
            for name in sorted(partitions)
            if name.endswith(DLQ_SUFFIX)
        ]

        lag: list[ConsumerLag] = []
        groups = admin.list_consumer_groups(request_timeout=TIMEOUT_S).result(TIMEOUT_S)
        names = sorted(
            g.group_id for g in groups.valid if g.group_id != "admin-ops-service.pipeline-probe"
        )
        for group in names:
            futures = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group)], request_timeout=TIMEOUT_S
            )
            committed = futures[group].result(TIMEOUT_S).topic_partitions or []
            per_topic: dict[str, int] = {}
            for tp in committed:
                if tp.topic not in partitions:
                    continue
                low, high = marks_for(tp.topic).get(tp.partition, (0, 0))
                start = tp.offset if tp.offset >= 0 else low
                per_topic[tp.topic] = per_topic.get(tp.topic, 0) + max(0, high - start)
            lag.extend(
                ConsumerLag(group=group, topic=topic, lag=n)
                for topic, n in sorted(per_topic.items())
            )
        lag.sort(key=lambda x: (-x.lag, x.group, x.topic))
        return dlq, lag
    finally:
        probe.close()


async def read_pipeline() -> tuple[list[DlqCount] | None, list[ConsumerLag] | None]:
    """`(dlq, consumer_lag)`, or `(None, None)` when Redpanda cannot be read."""
    brokers = _brokers()
    if not brokers:
        return None, None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_read, brokers), timeout=TIMEOUT_S * 3)
    except Exception:  # noqa: BLE001 - best effort; the console shows "could not be read"
        log.warning("pipeline read from Redpanda failed", exc_info=True)
        return None, None
