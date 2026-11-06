"""Metrics: route-template labels, no tenant labels, /metrics not proxied by the gateway."""

from __future__ import annotations

import httpx
from fastapi import FastAPI
from prometheus_client import REGISTRY
from starlette.testclient import TestClient

from libs.shared import metrics
from services.api_gateway import edge, proxy
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
    # Not public: the gateway's own /metrics is exempt from edge_auth only for an internal
    # scrape (see `test_gateway_metrics_*` below), never listed as a public path.
    assert not is_public("/metrics")

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


# --- api-gateway: /metrics vs edge_auth ---------------------------------------------------------

_INGRESS = [
    {"X-Forwarded-For": "1.2.3.4"},
    {"X-Forwarded-Host": "app.example.com"},
    {"X-Real-IP": "1.2.3.4"},
    {"CF-Connecting-IP": "1.2.3.4"},
    {"Forwarded": "for=1.2.3.4"},
]


def _gateway() -> tuple[TestClient, list[str]]:
    """The real gateway wiring (create_app + edge_auth + proxy catch-all) with a fake upstream
    that records every proxied path. No DB: every request here stops before tenant resolution."""
    from libs.shared.app_factory import create_app

    calls: list[str] = []
    up = FastAPI()

    @up.api_route("/{p:path}", methods=["GET", "POST"])
    async def _any(p: str) -> dict[str, str]:
        calls.append(p)
        return {}

    gw = create_app("api-gateway-metrics-test")
    gw.middleware("http")(edge.edge_auth)
    proxy.install(gw)
    gw.state.proxy_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=up))
    return TestClient(gw), calls


def test_gateway_metrics_internal_scrape_needs_no_token() -> None:
    c, calls = _gateway()
    r = c.get("/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "mytheus_http_requests_total" in r.text
    assert calls == []  # served by the gateway itself, never proxied


def test_gateway_metrics_through_ingress_is_404() -> None:
    c, calls = _gateway()
    for headers in _INGRESS:
        assert c.get("/metrics", headers=headers).status_code == 404, headers
        # A bearer token does not change that: ingress never reaches /metrics.
        r = c.get("/metrics", headers={**headers, "Authorization": "Bearer x"})
        assert r.status_code == 404, headers
    assert calls == []


def test_gateway_metrics_exemption_is_get_only_and_exact_path() -> None:
    c, calls = _gateway()
    assert c.post("/metrics").status_code == 404
    # Not the exempt path: falls into normal edge auth.
    assert c.get("/metrics/extra").status_code == 401
    assert calls == []
    assert "/metrics" not in edge.ROUTE_MAP and route_for("/metrics") is None


def test_gateway_other_routes_still_require_auth() -> None:
    c, calls = _gateway()
    for path in ("/api/v1/meetings", "/bff/dashboard", "/orgs"):
        r = c.get(path)
        assert r.status_code == 401, path
        assert r.json()["code"] == "AUTH_REQUIRED"
    assert calls == []


def test_came_through_ingress_is_case_insensitive() -> None:
    from starlette.datastructures import Headers

    assert metrics.came_through_ingress(Headers({"x-real-ip": "1.2.3.4"}))
    assert metrics.came_through_ingress(Headers({"Forwarded": "for=1.2.3.4"}))
    assert not metrics.came_through_ingress(Headers({"user-agent": "Prometheus/2.53"}))
