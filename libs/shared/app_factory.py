"""Shared FastAPI app factory.

Every HTTP service is created via create_app so the Idempotency-Key middleware, the RFC 7807
error handlers, OpenTelemetry, and a /healthz probe are wired identically everywhere.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from redis.asyncio import from_url

from libs.shared.errors import add_exception_handlers
from libs.shared.idempotency import IdempotencyMiddleware
from libs.shared.metrics import install as install_metrics
from libs.shared.openapi import install as install_openapi
from libs.shared.pat import PatAuthMiddleware
from libs.shared.telemetry import setup_telemetry


def create_app(service_name: str, *, idempotency: bool = True, pat_auth: bool = True) -> FastAPI:
    """`idempotency=False` is for a pure proxy (api-gateway). The Idempotency-Key header is
    forwarded to the service behind it, which owns the replay. Two layers sharing one
    org-scoped key make the inner one see the outer's in-progress marker and answer 409 to a
    perfectly fresh request.

    `pat_auth=False` likewise for api-gateway: its edge middleware resolves personal access
    tokens itself (and rate limits them), before routing."""
    app = FastAPI(title=service_name, version="1.0.0")

    if idempotency:
        redis = from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
        )
        app.add_middleware(IdempotencyMiddleware, redis=redis)  # replay mutating POST/PUT/PATCH
    if pat_auth:
        # Added after the idempotency layer, so it is OUTSIDE it: the principal a personal
        # access token resolves to is known before the idempotency key is scoped to a tenant.
        # `pat_auth=False` is for api-gateway, whose edge middleware resolves tokens itself.
        app.add_middleware(PatAuthMiddleware)
    add_exception_handlers(app)
    setup_telemetry(app, service_name=service_name)  # no-op unless OTEL endpoint set

    # Documents what the three lines above actually do. Middleware, exception handlers and
    # dependency-injected auth are all invisible to FastAPI's schema generator, so without
    # this the emitted spec describes an API with no security, no idempotency header and no
    # error shape. See libs/shared/openapi.py.
    install_openapi(app, service_name)

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok", "service": service_name}

    install_metrics(app, service_name)  # /metrics + request metrics (internal scrape only)

    return app
