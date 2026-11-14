"""The ops console's dead-letter and consumer-lag read (services/admin_ops_service/pipeline.py),
against fake Kafka clients: no broker needed."""

from __future__ import annotations

from types import SimpleNamespace

import confluent_kafka
import confluent_kafka.admin
import pytest

from services.admin_ops_service import pipeline

TOPICS = {
    "meeting-events": [0, 1],
    "meeting-events.dlq": [0],
    "usage-events": [0],
    "__consumer_offsets": [0],
}
MARKS = {
    ("meeting-events", 0): (0, 10),
    ("meeting-events", 1): (5, 8),
    ("meeting-events.dlq", 0): (2, 5),
    ("usage-events", 0): (0, 4),
}


class _Future:
    def __init__(self, value):
        self.value = value

    def result(self, _timeout=None):
        return self.value


class FakeAdmin:
    def __init__(self, _conf):
        pass

    def list_topics(self, timeout=None):
        return SimpleNamespace(
            topics={
                n: SimpleNamespace(partitions={p: None for p in ps}, error=None)
                for n, ps in TOPICS.items()
            }
        )

    def list_consumer_groups(self, request_timeout=None):
        return _Future(SimpleNamespace(valid=[SimpleNamespace(group_id="core-api.meeting")]))

    def list_consumer_group_offsets(self, requests, request_timeout=None):
        tps = [
            SimpleNamespace(topic="meeting-events", partition=0, offset=7),
            SimpleNamespace(topic="meeting-events", partition=1, offset=-1001),
            SimpleNamespace(topic="usage-events", partition=0, offset=4),
        ]
        return {requests[0].group_id: _Future(SimpleNamespace(topic_partitions=tps))}


class FakeConsumer:
    def __init__(self, _conf):
        pass

    def get_watermark_offsets(self, tp, timeout=None):
        return MARKS[(tp.topic, tp.partition)]

    def close(self):
        pass


@pytest.fixture
def fake_kafka(monkeypatch):
    monkeypatch.setattr(confluent_kafka.admin, "AdminClient", FakeAdmin)
    monkeypatch.setattr(confluent_kafka, "Consumer", FakeConsumer)
    monkeypatch.setenv("REDPANDA_BROKERS", "fake:9092")


async def test_counts_dead_letters_and_lag(fake_kafka):
    dlq, lag = await pipeline.read_pipeline()
    assert [(d.topic, d.count) for d in dlq] == [("meeting-events.dlq", 3)]
    # p0: 10 - 7 = 3; p1 has no commit, so counts from low: 8 - 5 = 3; usage-events caught up.
    assert [(x.group, x.topic, x.lag) for x in lag] == [
        ("core-api.meeting", "meeting-events", 6),
        ("core-api.meeting", "usage-events", 0),
    ]


async def test_unset_brokers_reads_as_unknown(monkeypatch):
    monkeypatch.delenv("REDPANDA_BROKERS", raising=False)
    assert await pipeline.read_pipeline() == (None, None)


async def test_a_broker_failure_reads_as_unknown(fake_kafka, monkeypatch):
    def boom(_conf):
        raise RuntimeError("unreachable")

    monkeypatch.setattr(confluent_kafka.admin, "AdminClient", boom)
    assert await pipeline.read_pipeline() == (None, None)
