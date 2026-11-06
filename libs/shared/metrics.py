"""Prometheus metrics: one small module, deliberately identical in mytheus-backend and
mytheus-intelligence (each repo owns its copy; no cross-repo imports).

Canonical metric names are documented in mytheus-backend/design/metrics.md.

Cardinality and privacy rules (do not relax):
  - HTTP series are labelled by the matched ROUTE TEMPLATE (``/api/v1/meetings/{meeting_id}``),
    never the raw path. Unmatched paths collapse to ``unmatched``.
  - No ``org_id``, ``user_id`` or any other tenant/person identifier is ever a label value.
  - Consumer series are labelled by group/topic/outcome only.
"""

from __future__ import annotations

import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from fastapi import FastAPI
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_SERVICE = os.environ.get("SERVICE_NAME", "unknown")

_LATENCY_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30)

HTTP_REQUESTS = Counter(
    "mytheus_http_requests_total",
    "HTTP requests handled, by route template and status code.",
    ["service", "method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "mytheus_http_request_duration_seconds",
    "HTTP request latency by route template.",
    ["service", "method", "route"],
    buckets=_LATENCY_BUCKETS,
)
HTTP_IN_FLIGHT = Gauge(
    "mytheus_http_requests_in_flight",
    "HTTP requests currently being served.",
    ["service"],
)

CONSUMER_PROCESSED = Counter(
    "mytheus_consumer_messages_processed_total",
    "Kafka messages processed, by outcome (applied|duplicate|dlq|error).",
    ["service", "group", "topic", "outcome"],
)
CONSUMER_DLQ = Counter(
    "mytheus_consumer_dlq_total",
    "Messages routed to a dead-letter topic.",
    ["service", "group", "topic", "failure_class"],
)
CONSUMER_LAG = Gauge(
    "mytheus_consumer_lag_seconds",
    "Age of the most recently consumed record (now - record timestamp) when processing began.",
    ["service", "group", "topic"],
)

OUTBOX_BACKLOG = Gauge(
    "mytheus_outbox_backlog",
    "Unpublished outbox rows observed by the relay after its last drain.",
    ["service"],
)
OUTBOX_PUBLISHED = Counter(
    "mytheus_outbox_published_total", "Outbox rows published to the bus.", ["service"]
)
OUTBOX_POISONED = Counter(
    "mytheus_outbox_poisoned_total",
    "Outbox rows dead-lettered after exhausting retries.",
    ["service"],
)

EMBEDDING_BATCH_DURATION = Histogram(
    "mytheus_embedding_batch_duration_seconds",
    "Wall-clock time to embed one batch.",
    ["service"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
EMBEDDING_BATCH_SIZE = Histogram(
    "mytheus_embedding_batch_size",
    "Number of texts per embedding batch.",
    ["service"],
    buckets=(1, 2, 4, 8, 16, 32, 64, 128, 256),
)


def set_service(name: str) -> None:
    global _SERVICE
    _SERVICE = name


def service_name() -> str:
    return _SERVICE


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return str(path) if path else "unmatched"


class MetricsMiddleware:
    """Pure-ASGI so it adds no BaseHTTPMiddleware body-buffering and sees ``scope['route']``
    once routing has run. ``/metrics`` itself is not measured."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return
        svc = _SERVICE
        status = {"code": 500}

        async def _send(message: Message) -> None:
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        start = time.perf_counter()
        HTTP_IN_FLIGHT.labels(svc).inc()
        try:
            await self.app(scope, receive, _send)
        finally:
            HTTP_IN_FLIGHT.labels(svc).dec()
            route = _route_template(scope)
            method = scope.get("method", "GET")
            HTTP_REQUESTS.labels(svc, method, route, str(status["code"])).inc()
            HTTP_LATENCY.labels(svc, method, route).observe(time.perf_counter() - start)


# Headers added by public-facing proxies. A scrape from inside the network carries none; if one
# is present the request came through ingress and /metrics must not answer.
_INGRESS_HEADERS = (
    "x-forwarded-for",
    "x-forwarded-host",
    "x-real-ip",
    "cf-connecting-ip",
    "forwarded",
)


def came_through_ingress(headers: Mapping[str, str]) -> bool:
    """True when any public-proxy header is present. ``headers`` must do case-insensitive
    lookup (Starlette ``Headers`` does). The single check every ``/metrics`` guard uses,
    including the api-gateway's ``edge_auth`` exemption."""
    return any(h in headers for h in _INGRESS_HEADERS)


def install(app: FastAPI, service_name_: str) -> None:
    """Mount the metrics middleware and the internal-only ``/metrics`` route on a FastAPI app."""
    set_service(service_name_)
    app.add_middleware(MetricsMiddleware)

    @app.get("/metrics", include_in_schema=False)
    async def metrics(request: Request) -> Response:
        if came_through_ingress(request.headers):
            return Response(status_code=404)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def serve_metrics(port: int | None = None) -> None:
    """Expose /metrics from a non-HTTP worker (consumer, relay). ``METRICS_PORT`` overrides;
    0 or unset-with-no-port disables. Safe to call once per process."""
    env = os.environ.get("METRICS_PORT")
    chosen = port if port is not None else (int(env) if env else 0)
    if chosen:
        start_http_server(chosen)


# --- consumer helpers -------------------------------------------------------------------------


def observe_consumer_lag(group: str, topic: str, timestamp_ms: int | None) -> None:
    if timestamp_ms is not None and timestamp_ms > 0:
        CONSUMER_LAG.labels(_SERVICE, group, topic).set(max(0.0, time.time() - timestamp_ms / 1000))


def record_processed(group: str, topic: str, outcome: str) -> None:
    CONSUMER_PROCESSED.labels(_SERVICE, group, topic, outcome).inc()


def record_dlq(group: str, topic: str, failure_class: str) -> None:
    CONSUMER_DLQ.labels(_SERVICE, group, topic, failure_class).inc()


# --- outbox / embedding helpers ---------------------------------------------------------------


def record_outbox(*, published: int = 0, poisoned: int = 0, backlog: int | None = None) -> None:
    if published:
        OUTBOX_PUBLISHED.labels(_SERVICE).inc(published)
    if poisoned:
        OUTBOX_POISONED.labels(_SERVICE).inc(poisoned)
    if backlog is not None:
        OUTBOX_BACKLOG.labels(_SERVICE).set(backlog)


def observe_embedding_batch(seconds: float, size: int) -> None:
    EMBEDDING_BATCH_DURATION.labels(_SERVICE).observe(seconds)
    EMBEDDING_BATCH_SIZE.labels(_SERVICE).observe(size)


@contextmanager
def time_embedding_batch(size: int) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        observe_embedding_batch(time.perf_counter() - start, size)
