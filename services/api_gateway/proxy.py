"""Reverse-proxy forwarding for the gateway. The TODO in `main.py` made good.

Until now the gateway had `edge_auth` and `route_for()` but nothing that actually forwarded, so
it could authenticate a request and then had nowhere to send it. That was correct while every
service shared one process; once they are separate containers it is the difference between a
gateway and a 404.

What this preserves, and why each matters:

  - The internal headers. `edge_auth` strips any client-supplied copy of `X-Mytheus-Org` /
    `X-Mytheus-User` / `X-Mytheus-Signature` and attaches its own signed set. Those must reach
    the upstream intact or downstream cannot tell a gateway-resolved tenant from a forged one.
    They travel in `request.headers` because `edge_auth` rewrites `scope["headers"]` before
    calling on, so forwarding the header list forwards them.

  - Hop-by-hop headers are dropped (RFC 9110 section 7.6.1). Forwarding `Connection`,
    `Transfer-Encoding` or `Content-Length` verbatim onto a new connection produces responses
    that disagree with their own framing; `Content-Length` in particular would describe the
    inbound body, not what httpx re-encodes.

  - The body is streamed, not buffered. A meeting recording upload through a gateway that
    reads the whole body into memory first is a per-request memory multiplier.

Failure mapping is deliberate. An unroutable path is 404 rather than 502: the gateway knows
the path belongs to no service, which is a client error, not an upstream one. A refused
connection is 503 `DEPENDENCY_UNAVAILABLE` and a timeout is 504, so "the service is down" and
"the service is slow" are distinguishable in the logs instead of both surfacing as 502.
"""

from __future__ import annotations

import logging
import os

import httpx
from fastapi import FastAPI, Request
from starlette.background import BackgroundTask
from starlette.responses import Response, StreamingResponse

from libs.shared.errors import ErrorCode, problem_response
from services.api_gateway.edge import normalize_path, route_for

logger = logging.getLogger(__name__)

# RFC 9110 7.6.1. Connection-specific; meaningless or actively wrong on a new connection.
_HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        # Not hop-by-hop, but re-derived by httpx from the body it actually sends. Forwarding
        # the inbound value describes a different body.
        "content-length",
        # Rewritten per-upstream below.
        "host",
    }
)

_DEFAULT_PORT = 8000
_TIMEOUT = httpx.Timeout(connect=5.0, read=60.0, write=60.0, pool=5.0)


def upstream_base_url(service: str) -> str:
    """Where `service` lives. `UPSTREAM_<SERVICE>` wins, else the compose service name.

    The env override exists so a service can be moved off-box (or pointed at a local process
    in development) without a code change. The default matches the compose service names,
    which are the canonical service names from `_canon/01-service-catalog.md` - so a typo in
    the route map surfaces as an unresolvable host rather than a silent misroute.
    """
    env_name = "UPSTREAM_" + service.upper().replace("-", "_")
    override = os.environ.get(env_name)
    if override:
        return override.rstrip("/")
    return f"http://{service}:{_DEFAULT_PORT}"


def _forwardable(headers: list[tuple[bytes, bytes]]) -> dict[str, str]:
    return {k.decode(): v.decode() for k, v in headers if k.decode().lower() not in _HOP_BY_HOP}


def get_client(app: FastAPI) -> httpx.AsyncClient:
    """One pooled client for the process, created on first use.

    A client per request would open a fresh TCP connection every time and defeat keep-alive to
    every upstream, which on a gateway is the whole latency budget.
    """
    client = getattr(app.state, "proxy_client", None)
    if client is None:
        client = httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False)
        app.state.proxy_client = client
    return client


async def forward(request: Request) -> Response:
    """Resolve the upstream for this path and stream the exchange through."""
    path = request.url.path
    # `edge_auth` already refuses these; checked again so the proxy can never be the hop that
    # forwards a dot segment or a service's `/internal` surface.
    service = route_for(path) if normalize_path(path) == path else None
    if service is None:
        return problem_response(404, ErrorCode.NOT_FOUND, f"No service owns {path}", instance=path)

    url = f"{upstream_base_url(service)}{path}"
    client = get_client(request.app)
    upstream_request = client.build_request(
        request.method,
        url,
        params=dict(request.query_params),
        headers=_forwardable(request.scope["headers"]),
        content=request.stream(),
    )

    try:
        upstream_response = await client.send(upstream_request, stream=True)
    except httpx.TimeoutException:
        logger.warning("gateway: %s timed out serving %s", service, path)
        return problem_response(
            504, ErrorCode.DEPENDENCY_UNAVAILABLE, f"{service} did not respond in time"
        )
    except httpx.HTTPError:
        logger.exception("gateway: cannot reach %s for %s", service, path)
        return problem_response(503, ErrorCode.DEPENDENCY_UNAVAILABLE, f"{service} is unavailable")

    return StreamingResponse(
        upstream_response.aiter_raw(),
        status_code=upstream_response.status_code,
        headers={
            k: v for k, v in upstream_response.headers.items() if k.lower() not in _HOP_BY_HOP
        },
        # Releases the upstream connection back to the pool once the client has the last byte.
        # Without it the pool leaks a connection per request and the gateway wedges under load.
        background=BackgroundTask(upstream_response.aclose),
    )


def install(app: FastAPI) -> None:
    """Mount the catch-all. Registered LAST so the gateway's own routes still win.

    `/healthz` is served by this process, not proxied: it must answer while every upstream is
    down, which is precisely when a proxied health check would be useless.
    """

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def _proxy(request: Request, full_path: str) -> Response:  # noqa: ARG001
        return await forward(request)

    @app.on_event("shutdown")
    async def _close_client() -> None:
        client = getattr(app.state, "proxy_client", None)
        if client is not None:
            await client.aclose()
