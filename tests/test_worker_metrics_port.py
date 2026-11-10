"""Every non-HTTP worker exposes Prometheus metrics when METRICS_PORT is set, through the one
shared helper (`libs.shared.metrics.serve_metrics`): the core-api consumer, the meeting-service
consumer and the audio tap worker. Kafka and the exporter's socket are faked; nothing is bound
and no broker, Postgres or LiveKit is contacted."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from typing import Any

import pytest

from libs.shared import metrics


class _Stop(Exception):
    pass


class _FakeConsumer:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass

    def subscribe(self, *_: Any) -> None:
        pass

    def poll(self, *_: Any) -> None:
        raise _Stop  # first poll ends the harness

    def close(self) -> None:
        pass


class _Anything:
    def __init__(self, *_: Any, **__: Any) -> None:
        pass


@pytest.fixture
def exporter_ports(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    import confluent_kafka
    import confluent_kafka.schema_registry
    import confluent_kafka.schema_registry.avro

    started: list[int] = []
    monkeypatch.setattr(metrics, "start_http_server", lambda port: started.append(port))
    monkeypatch.setattr(confluent_kafka, "Consumer", _FakeConsumer)
    monkeypatch.setattr(confluent_kafka, "Producer", _Anything)
    monkeypatch.setattr(confluent_kafka.schema_registry, "SchemaRegistryClient", _Anything)
    monkeypatch.setattr(confluent_kafka.schema_registry.avro, "AvroDeserializer", _Anything)
    monkeypatch.delenv("LIVEKIT_API_KEY", raising=False)  # FakeTransport, no LiveKit
    previous = metrics.service_name()
    yield started
    metrics.set_service(previous)


async def _core_api() -> None:
    from services.core_api.consumer import run_consumer

    with pytest.raises(_Stop):
        await run_consumer()


async def _meeting_consumer() -> None:
    from services.meeting_service.consumer import run_consumer

    with pytest.raises(_Stop):
        await run_consumer()


async def _notification_consumer() -> None:
    from services.notification_service.consumer import run_consumer

    with pytest.raises(_Stop):
        await run_consumer()


async def _tap_worker() -> None:
    from services.meeting_service.audio_tap_worker import run_worker

    stop = asyncio.Event()
    stop.set()  # returns after start-up and a clean shutdown
    await run_worker(stop)


WORKERS: dict[str, tuple[Callable[[], Awaitable[None]], str]] = {
    "core-api-consumer": (_core_api, "core-api-consumer"),
    "meeting-service-consumer": (_meeting_consumer, "meeting-service-consumer"),
    "notification-service-consumer": (
        _notification_consumer,
        "notification-service-consumer",
    ),
    "audio-tap-worker": (_tap_worker, "meeting-service-audio-tap-worker"),
}


@pytest.mark.parametrize("name", list(WORKERS))
async def test_worker_serves_metrics_when_metrics_port_is_set(
    name: str, exporter_ports: list[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    run, service = WORKERS[name]
    monkeypatch.setenv("METRICS_PORT", "9137")
    await run()
    assert exporter_ports == [9137]
    assert metrics.service_name() == service


@pytest.mark.parametrize("name", list(WORKERS))
async def test_worker_serves_no_metrics_without_metrics_port(
    name: str, exporter_ports: list[int], monkeypatch: pytest.MonkeyPatch
) -> None:
    run, _ = WORKERS[name]
    monkeypatch.delenv("METRICS_PORT", raising=False)
    await run()
    assert exporter_ports == []
