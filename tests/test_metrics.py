"""Metrics: route-template labels, no tenant labels, /metrics not proxied by the gateway."""

from __future__ import annotations

import httpx
from fastapi import FastAPI
from prometheus_client import REGISTRY
from starlette.testclient import TestClient

from libs.shared import metrics
from services.api_gateway import proxy
from services.api_gateway.edge import is_public, route_for


def _app() -> FastAPI:
    app = FastAPI()
    metrics.install(app, "test-svc")

    @app.get("/api/v1/things/{thing_id}")
    async def thing(thing_id: str) -> dict[str, str]:
        return {"id": thing_id}

    return app


def _sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_route_template_label_not_raw_path() -> None:
    c = TestClient(_app())
    org = "11111111-2222-3333-4444-555555555555"
    assert c.get(f"/api/v1/things/{org}").status_code == 200
    assert c.get("/nope/zzz-secret-123").status_code == 404
    body = c.get("/metrics").text
    assert 'route="/api/v1/things/{thing_id}"' in body
    assert org not in body and "zzz-secret-123" not in body
    assert 'route="unmatched"' in body
    # No tenant/user LABEL. Matched as `name=` rather than the bare word: the registry is
    # process-global, so another test module's route template (e.g. `/bff/people/{user_id}`)
    # legitimately contains the word and made this order-dependent.
    assert "org_id=" not in body and "user_id=" not in body
    assert (
        _sample(
            "mytheus_http_requests_total",
            service="test-svc",
            method="GET",
            route="/api/v1/things/{thing_id}",
            status="200",
        )
        >= 1
    )
    assert _sample("mytheus_http_requests_in_flight", service="test-svc") == 0


def test_metrics_hidden_when_request_came_through_ingress() -> None:
    c = TestClient(_app())
    assert c.get("/metrics", headers={"X-Forwarded-For": "1.2.3.4"}).status_code == 404
    assert c.get("/metrics", headers={"CF-Connecting-IP": "1.2.3.4"}).status_code == 404


def test_consumer_and_outbox_helpers() -> None:
    metrics.set_service("t")
    metrics.record_processed("g", "tp", "applied")
    metrics.record_dlq("g", "tp", "DESERIALIZE")
    metrics.record_outbox(published=3, poisoned=1, backlog=7)
    metrics.observe_embedding_batch(0.05, 32)
    metrics.observe_consumer_lag("g", "tp", 1)  # ancient record -> large lag
    assert (
        _sample(
            "mytheus_consumer_messages_processed_total",
            service="t",
            group="g",
            topic="tp",
            outcome="applied",
        )
        == 1
    )
    assert (
        _sample(
            "mytheus_consumer_dlq_total",
            service="t",
            group="g",
            topic="tp",
            failure_class="DESERIALIZE",
        )
        == 1
    )
    assert _sample("mytheus_outbox_published_total", service="t") == 3
    assert _sample("mytheus_outbox_poisoned_total", service="t") == 1
    assert _sample("mytheus_outbox_backlog", service="t") == 7
    assert _sample("mytheus_embedding_batch_size_count", service="t") == 1
    assert _sample("mytheus_consumer_lag_seconds", service="t", group="g", topic="tp") > 1000


def test_gateway_does_not_proxy_metrics() -> None:
    assert route_for("/metrics") is None
    assert route_for("/metrics/anything") is None
    assert not is_public("/metrics")  # gateway's own /metrics stays behind edge_auth

    calls: list[str] = []
    up = FastAPI()

    @up.api_route("/{p:path}", methods=["GET"])
    async def _any(p: str) -> dict[str, str]:
        calls.append(p)
        return {}

    gw = FastAPI()
    proxy.install(gw)
    gw.state.proxy_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=up))
    r = TestClient(gw).get("/metrics")
    assert r.status_code == 404
    assert calls == []
