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
import time
from datetime import UTC, datetime
from urllib.parse import unquote

from fastapi import Request, Response
from fastapi.security import HTTPAuthorizationCredentials
from redis.asyncio import Redis
from redis.asyncio import from_url as redis_from_url
from redis.exceptions import RedisError
from starlette.middleware.base import RequestResponseEndpoint

from libs.shared import oauth_config, pat
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
    # The MCP server's OAuth surface (Claude custom connectors). An MCP client calls these
    # before anyone has signed in, so no bearer can exist yet; each authenticates another way
    # (see `OAUTH_PUBLIC_PATHS`). Exact paths only: `/oauth/{provider}` (integration-service)
    # stays bearer-gated.
    | {
        "/.well-known/oauth-protected-resource",  # RFC 9728, bff: static metadata
        "/.well-known/oauth-protected-resource/mcp",  # the path-suffixed form of the same
        "/.well-known/oauth-authorization-server",  # RFC 8414, identity: static metadata
        "/oauth/register",  # RFC 7591: a public client that can do nothing until consent
        "/oauth/authorize",  # sends the browser to the consent page; grants nothing itself
        "/oauth/token",  # the code + PKCE verifier, or a refresh token, is the credential
        "/oauth/revoke",  # RFC 7009: holding the token is the authority to revoke it
    }
)
PUBLIC_PREFIXES = (
    # Signature-verified inbound webhooks. They authenticate by HMAC over the body, not by a
    # bearer token, and their sender has no Clerk session to present.
    "/clerk/webhook",
    "/stripe/webhook",
    "/livekit/webhook",
    # integration-service (P12). `/webhooks/{provider}` is verified per provider (Slack v0
    # signature, Google channel token, Jira JWT). `/oauth/{provider}` is NOT here: the OAuth
    # completion is a signed-in POST from the web app, bound to the user who started it.
    # `/integrations` (the settings API) is JWT-gated like everything else.
    "/webhooks/",
    # Share links (Phase 4): the token in the path is the credential, an HMAC-signed,
    # expiring, revocable link a participant created; core-api verifies it before any read.
    "/api/v1/public/",
)

# Never forwarded, whatever the token: every service's service-to-service surface lives under
# `/internal`, authenticated by caller tokens that only exist inside the network.
INTERNAL_PREFIX = "/internal"


# The gateway's own Prometheus endpoint (mounted by `create_app`). Deliberately NOT in
# PUBLIC_PATHS and never in ROUTE_MAP: it is exempt from the bearer check only for an internal
# scrape, i.e. a GET that carries none of the ingress headers (`metrics.came_through_ingress`).
METRICS_PATH = "/metrics"

# Authenticated but tenant-exempt: the ONE route a signed-in user calls before their org has a
# tenant (identity-tenant-service `POST /orgs/self-serve`, which creates it). The token is
# still verified here, including that it carries an org claim; only `resolve_org_id` is
# skipped, because it would refuse the very org being created. No internal headers are
# attached, so nothing downstream can mistake this request for a tenant-scoped one; the route
# re-verifies the token and checks with Clerk that the caller administers that org.
SELF_SERVE_ORG_PATH = "/orgs/self-serve"


# The OAuth paths above, which also answer CORS. Claude calls them server to server, but a
# browser-based MCP client (an inspector, a web IDE) fetches discovery, registers and redeems
# from page JavaScript; these endpoints take no cookie and no ambient credential, so `*` with no
# `Allow-Credentials` exposes nothing a server-side caller could not already read.
OAUTH_PUBLIC_PATHS = frozenset(
    p for p in PUBLIC_PATHS if p.startswith(("/.well-known/oauth-", "/oauth/"))
)
_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Expose-Headers": "WWW-Authenticate, Retry-After",
}
_CORS_PREFLIGHT = {
    **_CORS_HEADERS,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type, MCP-Protocol-Version",
    "Access-Control-Max-Age": "86400",
}


def on_mcp(path: str) -> bool:
    return path == oauth_config.MCP_PATH or path.startswith(oauth_config.MCP_PATH + "/")


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


def normalize_path(raw_path: str) -> str | None:
    """The one path the gateway authenticates, routes and forwards, or None to refuse it.

    The edge used to match `is_public` / `route_for` against the raw request path while httpx
    normalized the upstream URL, so `/oauth/../internal/slack/token` passed as public and then
    arrived upstream as `/internal/slack/token`. Every decision is now made on one canonical
    form, decoded exactly once, and anything that could be read two ways is refused:

    - an encoded slash or backslash (`%2f`, `%5c`, any case) in the raw path;
    - a `%` left after one decode (double encoding, e.g. `%252e`);
    - a backslash, an empty segment (`//`), or a `.` / `..` segment after decoding;
    - anything under `/internal`.
    """
    lowered = raw_path.lower()
    if "%2f" in lowered or "%5c" in lowered:
        return None
    path = unquote(raw_path)
    if not path.startswith("/") or "%" in path or "\\" in path or "//" in path:
        return None
    if any(segment in (".", "..") for segment in path.split("/")):
        return None
    if path == INTERNAL_PREFIX or path.startswith(INTERNAL_PREFIX + "/"):
        return None
    return path


async def edge_auth(request: Request, call_next: RequestResponseEndpoint) -> Response:
    """Pre-validate the token and resolve the tenant once, at the edge.

    Returns a response directly rather than raising, because middleware sits outside the
    exception handlers the app factory installs, so a raise here would escape as an unhandled
    500 carrying whatever the exception said.
    """
    raw = request.scope.get("raw_path") or request.scope["path"].encode()
    path = normalize_path(raw.decode("latin-1") if isinstance(raw, bytes) else str(raw))
    if path is None:
        return problem_response(404, ErrorCode.NOT_FOUND, "Not found")
    # From here on the request carries only the canonical path, so the routing decision, the
    # auth decision and the forwarded URL can never disagree.
    request.scope["path"] = path
    request.scope["raw_path"] = path.encode("utf-8")

    if path in OAUTH_PUBLIC_PATHS:
        if request.method == "OPTIONS":
            return Response(status_code=204, headers=_CORS_PREFLIGHT)
        response = await call_next(request)
        response.headers.update(_CORS_HEADERS)
        return response

    if is_public(path):
        return await call_next(request)

    response = await _authenticated(request, call_next, path)
    if response.status_code == 401 and on_mcp(path):
        # MCP authorization (2025-06-18) and RFC 9728 section 5.1: the 401 tells the client where
        # the protected resource metadata is, which is how Claude finds the authorization server.
        # The problem+json body is kept.
        presented = pat.bearer_of(request.headers) is not None
        response.headers["WWW-Authenticate"] = oauth_config.www_authenticate(
            error="invalid_token" if presented else None
        )
    return response


async def _authenticated(
    request: Request, call_next: RequestResponseEndpoint, path: str
) -> Response:
    """Every non-public path: the metrics exemption, a personal access token, or a Clerk JWT."""

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

    token = pat.bearer_of(request.headers)
    if pat.is_pat(token):
        return await _pat_request(request, call_next, str(token))

    if request.method == "POST" and request.url.path == SELF_SERVE_ORG_PATH:
        try:
            verify_token(_bearer(request))
        except ProblemDetail as exc:
            return problem_response(exc.status, exc.code, exc.detail)
        return await call_next(request)

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


# --- personal access tokens (Phase 4) ----------------------------------------------------------

PAT_RATE_ENV = "PAT_RATE_LIMIT_PER_MINUTE"
DEFAULT_PAT_RATE = 120
LAST_USED_TTL_S = 90 * 86400


def pat_rate_limit() -> int:
    try:
        return max(1, int(os.environ.get(PAT_RATE_ENV, DEFAULT_PAT_RATE)))
    except ValueError:
        return DEFAULT_PAT_RATE


def _gateway_redis(request: Request) -> Redis[bytes]:
    """One client per app (and so per event loop), created on first use."""
    client: Redis[bytes] | None = getattr(request.app.state, "pat_redis", None)
    if client is None:
        client = redis_from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
        )
        request.app.state.pat_redis = client
    return client


def rate_limit_key(principal: pat.PatPrincipal, window: int) -> str:
    return f"nexus:{principal.org_id}:ratelimit:pat:{principal.token_id}:{window}"


def last_used_key(org_id: object, token_id: object) -> str:
    """Read by identity-tenant-service's token list (`last_used_at`)."""
    return f"nexus:{org_id}:pat:last_used:{token_id}"


async def _count_request(request: Request, principal: pat.PatPrincipal) -> tuple[int, int]:
    """(requests in this minute including this one, seconds until the window resets). Also
    stamps the token's last use. A fixed one-minute window per token, shared by every gateway
    replica because it lives in Redis."""
    now = time.time()
    window = int(now // 60)
    pipe = _gateway_redis(request).pipeline(transaction=False)
    pipe.incr(rate_limit_key(principal, window))
    pipe.expire(rate_limit_key(principal, window), 120)
    pipe.set(
        last_used_key(principal.org_id, principal.token_id),
        datetime.now(UTC).isoformat(),
        ex=LAST_USED_TTL_S,
    )
    count, _, _ = await pipe.execute()
    return int(count), max(1, 60 - int(now % 60))


async def _pat_request(
    request: Request, call_next: RequestResponseEndpoint, token: str
) -> Response:
    """A `mytheus_pat_` bearer: resolve it, gate the path and scope, rate limit, forward.

    Fails closed throughout: a bad token is the single 401, a path outside the public API or a
    write with a read token is 403, and a rate-limit store that cannot be reached is 503 rather
    than an unlimited token.
    """
    try:
        principal = await pat.authenticate(token, request.method, request.url.path)
    except ProblemDetail as exc:
        return problem_response(exc.status, exc.code, exc.detail, extra=exc.extra)
    if principal.kind == "oauth" and not on_mcp(request.url.path):
        # Audience (RFC 8707, MCP 2025-06-18): an OAuth access token was issued for the MCP
        # server and is accepted at `/mcp` only, never on the REST API. bff's tools still call
        # core-api with it, but that hop is internal and never passes this edge.
        return problem_response(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "This token was issued to an app for the Mytheus MCP server only",
            extra={"reason": "oauth_token_mcp_only"},
        )
    limit = pat_rate_limit()
    try:
        used, reset = await _count_request(request, principal)
    except RedisError:
        logger.exception("gateway: PAT rate-limit store unavailable")
        return problem_response(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Rate limiting is unavailable; retry shortly"
        )
    headers = {
        "X-RateLimit-Limit": str(limit),
        "X-RateLimit-Remaining": str(max(0, limit - used)),
        "X-RateLimit-Reset": str(reset),
    }
    if used > limit:
        response: Response = problem_response(
            429,
            ErrorCode.RATE_LIMITED,
            f"This API token is limited to {limit} requests per minute",
        )
        response.headers.update({**headers, "Retry-After": str(reset)})
        return response
    _attach_internal_headers(request, str(principal.org_id), principal.clerk_user_id)
    response = await call_next(request)
    response.headers.update(headers)
    return response


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
    "/api/v1/context-fills": "core-api",
    # ADR-0039 Part B: meeting types, promises and status requests are core-api's too.
    "/api/v1/meeting-templates": "core-api",
    "/api/v1/commitments": "core-api",
    # Polish wave 2: the caller's own memory across meetings (core-api's person_memory).
    "/api/v1/me/memory": "core-api",
    "/api/v1/status-requests": "core-api",
    # Polish wave 3: decision reversals and the weekly digest are core-api's.
    "/api/v1/decision-reversals": "core-api",
    "/api/v1/digests": "core-api",
    # Beyond the meeting, decisions that follow you: where a decision was challenged and the
    # requests to reopen one (the check itself is `/api/v1/decisions/check`, core-api's too).
    "/api/v1/decision-challenges": "core-api",
    "/api/v1/decision-reopen-requests": "core-api",
    # The A2A grant store (ADR-0013) is identity's, under a versioned prefix of its own.
    "/api/v1/agent-authz-scopes": "identity-tenant-service",
    # The company brain (ADR-0039 Part A), identity's too.
    "/api/v1/org": "identity-tenant-service",
    "/api/v1/users": "identity-tenant-service",
    # meeting-service was missing from this map entirely, so `/api/meetings/:id/token` - the
    # only route that mints a LiveKit room token - resolved to no service and the whole
    # meeting transport was unreachable through the gateway. Note the deliberate near-collision
    # with core-api's `/api/v1/meetings` (meeting CRUD): the two differ by the version segment,
    # and longest-prefix matching keeps them apart because neither string prefixes the other.
    "/api/meetings": "meeting-service",
    # rag-service owns `POST /api/rag/query` (canon service catalog section 6); it re-verifies
    # the token itself and reads the asker's meeting scope from core-api (ADR-0033 D1).
    "/api/rag": "rag-service",
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
    # integration-service's `POST /oauth/{provider}` (completing a provider connection), one
    # entry per provider in `services/integration_service/config.py`. Spelled out rather than a
    # bare `/oauth` because `/oauth/{register,authorize,token,revoke}` below belong to another
    # service, and no two services may share a prefix (`test_no_two_services_share_a_prefix`).
    # `test_every_integration_provider_is_routed` fails the build if a provider is added there
    # and not here.
    "/oauth/google_calendar": "integration-service",
    "/oauth/slack": "integration-service",
    "/oauth/jira": "integration-service",
    "/oauth/linear": "integration-service",
    "/oauth/hubspot": "integration-service",
    "/oauth/notion": "integration-service",
    "/oauth/confluence": "integration-service",
    # The MCP server's OAuth authorization server is identity-tenant-service (it writes the
    # `api_tokens` row an access token becomes).
    "/oauth/register": "identity-tenant-service",
    "/oauth/authorize": "identity-tenant-service",
    "/oauth/token": "identity-tenant-service",
    "/oauth/revoke": "identity-tenant-service",
    # The signed-in half: the consent page's backend and Connected apps.
    "/api/v1/oauth": "identity-tenant-service",
    # Discovery: the resource server's metadata (bff serves `/mcp`) and the authorization
    # server's (identity).
    "/.well-known/oauth-protected-resource": "bff",
    "/.well-known/oauth-authorization-server": "identity-tenant-service",
    "/webhooks": "integration-service",
    # The settings API (list, connect, disconnect). JWT-gated; owner/admin enforced by the
    # service. `/internal/*` is deliberately absent, as for every service.
    "/integrations": "integration-service",
    # Phase 4 reach. Personal access tokens are identity's (the token list a user manages in
    # settings; the gateway itself only resolves them, see `_pat_request`).
    "/api/v1/api-tokens": "identity-tenant-service",
    # The public read of a share link: no bearer token, the signed token is in the path.
    "/api/v1/public": "core-api",
    # Revoking a share link (creating and listing them is under meetings / decisions).
    "/api/v1/share-links": "core-api",
    # Polish wave 3, Recording playback: deleting and sharing a clip (making and listing them
    # is under meetings). The recording itself is meeting-service's `/api/meetings/{id}/recording`.
    "/api/v1/clips": "core-api",
    # The public API's search (core-api) and ask (bff, which calls rag-service as itself:
    # rag-service verifies Clerk sessions only, so an API token reaches it through here).
    "/api/v1/search": "core-api",
    "/api/v1/ask": "bff",
    # Wave 3: the phone's in-person recording (a capture token for transcription-service's
    # `/capture/audio`, minted with the caller's own Clerk session).
    "/api/v1/capture": "core-api",
    # The MCP server (Streamable HTTP, JSON-RPC over POST). A route group in bff: it is a
    # client-shaped aggregation over the same public API, like bff's screens.
    "/mcp": "bff",
}


def route_for(path: str) -> str | None:
    """Longest-prefix match, so `/api/v1/meetings` cannot be captured by a shorter entry."""
    best: str | None = None
    for prefix in ROUTE_MAP:
        if path.startswith(prefix) and (best is None or len(prefix) > len(best)):
            best = prefix
    return ROUTE_MAP[best] if best else None
