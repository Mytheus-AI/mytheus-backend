"""Service-to-service authentication for integration-service's `/internal/*` routes.

The P10 caller-token scheme (`services/core_api/routes/internal_meetings.py`), with this
service's own secret list: `X-Mytheus-Service` (caller name), `X-Mytheus-Service-Token` (its
secret from `MYTHEUS_INTEGRATION_CALLER_TOKENS`, `service=secret` pairs, 32+ characters) and the
tenant in `X-Mytheus-Org`. Unconfigured means every caller is refused. `/internal` is not in
api-gateway's route map, so these routes are reachable only inside the network.

Each route names the callers it accepts: notification-service may fetch the Slack bot token,
workflow-service may ask for a prep brief or a meeting summary, and neither can do the other's.
"""

from __future__ import annotations

import hmac
import os
import uuid
from typing import Annotated

from fastapi import Request, Security
from fastapi.security import APIKeyHeader

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id

TOKENS_ENV = "MYTHEUS_INTEGRATION_CALLER_TOKENS"
KNOWN_CALLERS = frozenset({"notification-service", "workflow-service"})
SERVICE_HEADER = "x-mytheus-service"
SERVICE_TOKEN_HEADER = "x-mytheus-service-token"
ORG_HEADER = "x-mytheus-org"

service_token_scheme = APIKeyHeader(
    name="X-Mytheus-Service-Token",
    scheme_name="MytheusServiceToken",
    description=(
        "INTERNAL ONLY. Per-caller shared secret from MYTHEUS_INTEGRATION_CALLER_TOKENS, sent "
        "with `X-Mytheus-Service` (caller name) and `X-Mytheus-Org` (tenant)."
    ),
    auto_error=False,
)


def parse_caller_tokens(raw: str) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError(f"{TOKENS_ENV} entries must be service=secret")
        if name not in KNOWN_CALLERS:
            raise ValueError(f"{name!r} is not an allowed integration-service caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


def _unauthenticated() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")


async def authenticate_caller(request: Request, allowed: frozenset[str]) -> uuid.UUID:
    try:
        tokens = parse_caller_tokens(os.environ.get(TOKENS_ENV, ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "integration-service caller config invalid"
        ) from exc
    if not tokens:
        raise _unauthenticated()
    service = request.headers.get(SERVICE_HEADER, "")
    presented = request.headers.get(SERVICE_TOKEN_HEADER, "").encode("utf-8")
    expected = tokens.get(service) if service in allowed else None
    if not hmac.compare_digest(presented, expected or b"\x00" * 32) or expected is None:
        raise _unauthenticated()
    try:
        org_id = uuid.UUID(request.headers.get(ORG_HEADER, ""))
    except ValueError as exc:
        raise _unauthenticated() from exc
    async with get_sessionmaker()() as session, session.begin():
        resolved = await resolve_org_by_id(session, org_id)
    if resolved is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return resolved


async def notification_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"notification-service"}))


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"workflow-service"}))
