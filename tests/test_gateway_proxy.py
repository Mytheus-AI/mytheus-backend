"""The gateway forwards. Until this existed it authenticated a request and then 404'd it.

The security-relevant assertions here are the header ones. `edge_auth` strips any
client-supplied `X-Mytheus-*` and attaches its own signed set; if the proxy hop dropped those,
downstream could not distinguish a gateway-resolved tenant from an unauthenticated one, and if
it forwarded the client's copy instead, asserting another tenant would be a header away.
"""

from __future__ import annotations

import httpx
import pytest
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.testclient import TestClient

from services.api_gateway import proxy
from services.api_gateway.edge import ORG_HEADER, SIGNATURE_HEADER, USER_HEADER, route_for


def _gateway(upstream) -> TestClient:
    """A gateway wired to an in-process upstream, so no sockets are involved.

    ASGITransport, not MockTransport: the proxy streams the request body, and MockTransport
    re-reads a body that streaming has already consumed, which fails with StreamConsumed
    before the handler is even reached. An ASGI app is also the more faithful upstream - it
    exercises real request parsing rather than a callable handed an httpx.Request.
    """
    app = FastAPI()

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    proxy.install(app)
    app.state.proxy_client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=upstream), timeout=httpx.Timeout(5.0)
    )
    return TestClient(app)


def _echo_upstream(record: dict) -> FastAPI:
    """Records what actually arrived, and echoes a chosen status/body back."""
    up = FastAPI()

    @up.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def _any(request: Request, path: str) -> Response:  # noqa: ARG001
        record["method"] = request.method
        record["url"] = str(request.url)
        record["body"] = await request.body()
        record["headers"] = {k.lower(): v for k, v in request.headers.items()}
        status = record.get("respond_status", 200)
        return JSONResponse(status_code=status, content=record.get("respond_body", {"ok": True}))

    return up


def _failing_upstream(exc: Exception) -> httpx.AsyncClient:
    """A client whose transport always raises, for the unreachable/slow cases."""

    def _raise(request: httpx.Request) -> httpx.Response:
        raise exc

    return httpx.AsyncClient(transport=httpx.MockTransport(_raise), timeout=httpx.Timeout(5.0))


def _gateway_with_client(client: httpx.AsyncClient) -> TestClient:
    app = FastAPI()
    proxy.install(app)
    app.state.proxy_client = client
    return TestClient(app)


def test_an_unrouted_path_is_404_from_the_gateway_not_a_bad_gateway():
    """The gateway knows no service owns the path. That is a client error, and calling it 502
    would blame an upstream that was never contacted."""
    client = _gateway(_echo_upstream({}))
    r = client.get("/nope/nothing")
    assert r.status_code == 404
    assert r.json()["code"] == "NOT_FOUND"


def test_the_gateways_own_healthz_is_served_locally_not_proxied():
    """It has to answer while every upstream is down, which is when a proxied health check
    would be worthless."""

    rec: dict = {}
    assert _gateway(_echo_upstream(rec)).get("/healthz").json() == {"status": "ok"}
    assert rec == {}, "healthz must not be forwarded"


def test_method_path_query_and_body_reach_the_upstream_unchanged():
    seen: dict = {"respond_status": 201}
    client = _gateway(_echo_upstream(seen))
    r = client.post("/api/v1/meetings?limit=5", content=b'{"title":"x"}')

    assert r.status_code == 201
    assert seen["method"] == "POST"
    assert seen["url"] == "http://core-api:8000/api/v1/meetings?limit=5"
    assert seen["body"] == b'{"title":"x"}'


def test_the_signed_internal_headers_are_forwarded():
    """Without these the tenant the gateway resolved never reaches the service."""
    rec: dict = {}
    client = _gateway(_echo_upstream(rec))
    client.get(
        "/api/v1/meetings",
        headers={ORG_HEADER: "org-uuid", USER_HEADER: "user_1", SIGNATURE_HEADER: "sig"},
    )
    assert rec["headers"][ORG_HEADER] == "org-uuid"
    assert rec["headers"][USER_HEADER] == "user_1"
    assert rec["headers"][SIGNATURE_HEADER] == "sig"


def test_hop_by_hop_headers_are_not_forwarded():
    """Content-Length in particular describes the inbound body, not what httpx re-encodes, so
    forwarding it produces a response that disagrees with its own framing."""
    rec: dict = {}
    client = _gateway(_echo_upstream(rec))
    client.post(
        "/api/v1/meetings",
        content=b"abc",
        # `te` and `upgrade` are hop-by-hop and httpx does NOT set either itself, so their
        # absence upstream proves the strip happened. `Connection` is deliberately not
        # asserted on: httpx adds its own `Connection: keep-alive` for the new hop, which is
        # correct - the check that matters is that the CLIENT's copy did not survive.
        headers={"TE": "trailers", "Upgrade": "websocket", "Connection": "upgrade"},
    )
    assert "te" not in rec["headers"]
    assert "upgrade" not in rec["headers"]
    assert rec["headers"].get("connection") != "upgrade", "the client's Connection leaked"
    assert rec["headers"].get("host") == "core-api:8000", "Host must be rewritten upstream"
    # Content-Length must describe what httpx actually re-encoded, not the inbound framing.
    assert rec["body"] == b"abc"


def test_an_unreachable_upstream_is_503_and_a_slow_one_is_504():
    """Distinguishable on purpose: 'down' and 'slow' need different responses on call."""

    refused = _gateway_with_client(_failing_upstream(httpx.ConnectError("refused")))
    assert refused.get("/api/v1/meetings").status_code == 503

    r = _gateway_with_client(_failing_upstream(httpx.ReadTimeout("too slow"))).get(
        "/api/v1/meetings"
    )
    assert r.status_code == 504
    assert r.json()["code"] == "DEPENDENCY_UNAVAILABLE"


def test_the_upstream_status_and_body_are_passed_through():
    """A 404 from the service must not be laundered into a gateway 404, and an error body has
    to survive: it is the RFC 7807 problem the client needs."""

    rec: dict = {"respond_status": 409, "respond_body": {"code": "VERSION_CONFLICT"}}
    r = _gateway(_echo_upstream(rec)).get("/api/v1/meetings")
    assert r.status_code == 409
    assert r.json()["code"] == "VERSION_CONFLICT"


@pytest.mark.parametrize(
    ("path", "service"),
    [
        ("/api/meetings/abc/token", "meeting-service"),
        ("/livekit/webhook", "meeting-service"),
        # The near-collision: differs from the above by the version segment only.
        ("/api/v1/meetings", "core-api"),
        ("/api/v1/meetings/abc", "core-api"),
        ("/bff/dashboard", "bff"),
        ("/clerk/webhook", "identity-tenant-service"),
        ("/scim/v2/Users", "identity-tenant-service"),
        ("/context/user_1", "identity-tenant-service"),
        ("/users/user_1", "identity-tenant-service"),
        ("/taxonomy/role-topics", "identity-tenant-service"),
        ("/stripe/webhook", "billing-metering-service"),
        ("/admin/ops/flags", "admin-ops-service"),
    ],
)
def test_every_real_route_resolves_to_its_owning_service(path, service):
    """meeting-service was absent from the map entirely, so the only route that mints a
    LiveKit room token resolved to nothing."""
    assert route_for(path) == service


def test_an_env_override_moves_a_service_without_a_code_change(monkeypatch):
    monkeypatch.setenv("UPSTREAM_CORE_API", "http://elsewhere:9000/")
    assert proxy.upstream_base_url("core-api") == "http://elsewhere:9000"
    monkeypatch.delenv("UPSTREAM_CORE_API")
    assert proxy.upstream_base_url("core-api") == "http://core-api:8000"
