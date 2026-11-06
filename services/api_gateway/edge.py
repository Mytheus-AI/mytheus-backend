"""Edge authentication and the routing map. Plan section 3.8 steps 1 and 2.

The gateway is a DDoS shield, not a trust delegation. It rejects garbage early so a service
worker is never woken by an expired token, and it resolves the tenant once so the whole
request has one answer. It does NOT authorize: the service tier re-verifies the token itself
and makes its own permit/deny decision from `rbac_grants`.

Why the forwarded header is signed. The gateway attaches `X-Mytheus-Org` so downstream can log
and route on the tenant without re-resolving it. Unsigned, that header is a cross-tenant
request waiting to happen: anything that can reach a service directly could simply assert a
different tenant. Signing it means a forged value is detectable. It is still not TRUSTED for
authorization, which is the point of re-verification, but it means a tampered header fails
loudly instead of being quietly believed.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os

from fastapi import Request, Response
from fastapi.security import HTTPAuthorizationCredentials
from starlette.middleware.base import RequestResponseEndpoint

from libs.shared.auth import clerk_org_id_from, verify_token
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail, problem_response
from libs.shared.metrics import came_through_ingress
from libs.shared.org_resolver import resolve_org_id

logger = logging.getLogger(__name__)

ORG_HEADER = "x-mytheus-org"
USER_HEADER = "x-mytheus-user"
SIGNATURE_HEADER = "x-mytheus-signature"

# Paths that must work before, or without, a tenant. Each is here for a stated reason, because
# an allowlist nobody can justify line by line becomes the way authentication gets bypassed.
PUBLIC_PATHS = frozenset(
    {
        "/healthz",  # liveness; must answer while the database is down
        "/openapi.json",
        "/docs",
        "/redoc",
    }
)
PUBLIC_PREFIXES = (
    # Signature-verified inbound webhooks. They authenticate by HMAC over the body, not by a
    # bearer token, and their sender has no Clerk session to present.
    "/clerk/webhook",
    "/stripe/webhook",
    "/livekit/webhook",
    "/oauth/",
)

# The gateway's own Prometheus endpoint (mounted by `create_app`). Deliberately NOT in
# PUBLIC_PATHS and never in ROUTE_MAP: it is exempt from the bearer check only for an internal
# scrape, i.e. a GET that carries none of the ingress headers (`metrics.came_through_ingress`).
METRICS_PATH = "/metrics"


def _signing_key() -> bytes:
    key = os.environ.get("GATEWAY_SIGNING_KEY")
    if not key:
        # Fail closed and loudly. A default key is worse than no signing at all, because it
        # looks like a control while being public knowledge.
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Gateway signing key not configured"
        )
    return key.encode()


def sign_context(org_id: str, user_id: str) -> str:
    """HMAC over both values together, not each separately.

    Signing them independently would let an attacker with two valid contexts mix and match:
    tenant A's org header with tenant B's user header, each individually well-signed.
    """
    payload = f"{org_id}:{user_id}".encode()
    return hmac.new(_signing_key(), payload, hashlib.sha256).hexdigest()


def verify_context(org_id: str, user_id: str, signature: str) -> bool:
    """Constant-time comparison: a timing-variable check on an HMAC leaks it byte by byte."""
    return hmac.compare_digest(sign_context(org_id, user_id), signature)


def is_public(path: str) -> bool:
    return path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES)


async def edge_auth(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """Pre-validate the token and resolve the tenant once, at the edge.

    Returns a response directly rather than raising, because middleware sits outside the
    exception handlers the app factory installs, so a raise here would escape as an unhandled
    500 carrying whatever the exception said.
    """
    if is_public(request.url.path):
        return await call_next(request)

    if request.url.path == METRICS_PATH:
        # Internal scrape: no token (Prometheus has no Clerk session). Anything that came
        # through ingress, or is not a GET, gets the same 404 as an unknown path, so the
        # endpoint's existence is not advertised publicly.
        if request.method == "GET" and not came_through_ingress(request.headers):
            return await call_next(request)
        return problem_response(404, ErrorCode.NOT_FOUND, f"No service owns {METRICS_PATH}")

    # Strip any inbound copy of the internal headers. Without this, a client can simply send
    # `X-Mytheus-Org` itself and, if anything downstream ever trusts the header without
    # checking the signature, it is a cross-tenant read with no exploit required.
    _strip_internal_headers(request)

    try:
        credentials = _bearer(request)
        claims = verify_token(credentials)
        clerk_org = clerk_org_id_from(claims)
        user_id = str(claims["sub"])

        sessionmaker = get_sessionmaker()
        async with sessionmaker() as session, session.begin():
            org_id = await resolve_org_id(session, clerk_org)
    except ProblemDetail as exc:
        # No service worker is touched. This is the DDoS-shield half of the gateway's job.
        return problem_response(exc.status, exc.code, exc.detail)

    _attach_internal_headers(request, str(org_id), user_id)
    return await call_next(request)


def _bearer(request: Request) -> HTTPAuthorizationCredentials:
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Missing bearer token")
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _strip_internal_headers(request: Request) -> None:
    raw = request.scope["headers"]
    blocked = {ORG_HEADER.encode(), USER_HEADER.encode(), SIGNATURE_HEADER.encode()}
    request.scope["headers"] = [(k, v) for k, v in raw if k.lower() not in blocked]


def _attach_internal_headers(request: Request, org_id: str, user_id: str) -> None:
    request.scope["headers"] = [
        *request.scope["headers"],
        (ORG_HEADER.encode(), org_id.encode()),
        (USER_HEADER.encode(), user_id.encode()),
        (SIGNATURE_HEADER.encode(), sign_context(org_id, user_id).encode()),
    ]


# Routing map. Screen-shaped reads go to `bff`; everything else goes straight to the owning
# service, per `_canon/01-service-catalog.md`. No two services share a prefix, which is what
# makes this a lookup rather than an ordered rule chain.
ROUTE_MAP: dict[str, str] = {
    "/bff": "bff",
    "/api/v1/meetings": "core-api",
    "/api/v1/teams": "core-api",
    "/api/v1/departments": "core-api",
    "/api/v1/worklogs": "core-api",
    "/api/v1/decisions": "core-api",
    "/api/v1/actions": "core-api",
    "/api/v1/chat": "core-api",
    "/api/v1/suggestions": "core-api",
    # meeting-service was missing from this map entirely, so `/api/meetings/:id/token` - the
    # only route that mints a LiveKit room token - resolved to no service and the whole
    # meeting transport was unreachable through the gateway. Note the deliberate near-collision
    # with core-api's `/api/v1/meetings` (meeting CRUD): the two differ by the version segment,
    # and longest-prefix matching keeps them apart because neither string prefixes the other.
    "/api/meetings": "meeting-service",
    "/livekit/webhook": "meeting-service",
    "/orgs": "identity-tenant-service",
    "/rbac": "identity-tenant-service",
    "/scim/v2": "identity-tenant-service",
    "/context": "identity-tenant-service",
    "/users": "identity-tenant-service",
    "/taxonomy": "identity-tenant-service",
    "/tenant-settings": "identity-tenant-service",
    "/clerk/webhook": "identity-tenant-service",
    "/billing": "billing-metering-service",
    "/usage": "billing-metering-service",
    "/plans": "billing-metering-service",
    "/stripe/webhook": "billing-metering-service",
    "/notifications": "notification-service",
    "/admin/tenant": "admin-ops-service",
    "/admin/ops": "admin-ops-service",
    "/oauth": "integration-service",
    "/webhooks": "integration-service",
}


def route_for(path: str) -> str | None:
    """Longest-prefix match, so `/api/v1/meetings` cannot be captured by a shorter entry."""
    best: str | None = None
    for prefix in ROUTE_MAP:
        if path.startswith(prefix) and (best is None or len(prefix) > len(best)):
            best = prefix
    return ROUTE_MAP[best] if best else None
