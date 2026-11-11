"""Personal access tokens (PATs): the second way to call Mytheus, for the public API and MCP.

A user creates a token in the app (`identity-tenant-service /api/v1/api-tokens`), copies it
once, and sends it as `Authorization: Bearer mytheus_pat_...` from a script, a CRM, or an MCP
client. It resolves to the SAME `RequestContext` a Clerk session does (tenant, user, RBAC
tier), so every route's own permission checks apply unchanged: a token can never do more than
its owner can.

Format. `mytheus_pat_<org_id hex, 32>_<secret, 43 url-safe chars>`. The tenant is in the token
so resolution needs no exception to row-level security: the resolver confirms the tenant is
active, sets `app.current_org` to it, and looks up the token's SHA-256 under RLS. A token whose
tenant part was edited finds no row in the tenant it names. Only the hash is stored; SHA-256
(not a slow KDF) is right for a 256-bit random secret, where there is no dictionary to slow
down.

What a token may reach, in addition to its owner's own permissions:

- Only the public API surface (`PUBLIC_API_PREFIXES`): meetings, decisions, actions, promises,
  search, ask-across-meetings (`/api/v1/ask`; rag-service's own `/api/rag/query` verifies Clerk
  sessions only, so tokens ask through bff) and the MCP endpoint. Token management, admin
  consoles, billing, RBAC, integrations and the screen-shaped `/bff` reads all refuse a token (403
  `FORBIDDEN_ROLE`). In particular a token can never mint or revoke tokens, so a leaked token
  cannot entrench itself.
- `read` tokens only GET / HEAD, plus the two POSTs that only read (`/api/v1/ask`, and
  `/mcp`, whose write tool checks the scope itself). `read_write` tokens may also write.

Every check fails closed with one undifferentiated 401 for any bad token (unknown, revoked,
expired, malformed, wrong tenant, suspended tenant, deleted user), so the response is never an
oracle for which of those it was.

Where it is enforced, twice, like the Clerk JWT: `api-gateway` (`edge.py`, which also rate
limits per token) and every service (`PatAuthMiddleware`, installed by the app factory; then
`auth.verify_token` and `authz.tenant_context` accept the principal it resolved).
"""

from __future__ import annotations

import hashlib
import re
import secrets
import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.types import ASGIApp, Receive, Scope, Send

from libs.shared.errors import ErrorCode, ProblemDetail

PAT_PREFIX = "mytheus_pat_"
PAT_CLAIM = "mytheus_pat"
_SECRET_BYTES = 32
_FORMAT = re.compile(r"^mytheus_pat_([0-9a-f]{32})_([A-Za-z0-9_\-]{43})$")

Scope_ = Literal["read", "read_write"]
SCOPES: tuple[Scope_, ...] = ("read", "read_write")

# The public API surface a token may reach. Matched on whole path segments.
PUBLIC_API_PREFIXES: tuple[str, ...] = (
    "/api/v1/meetings",
    "/api/v1/decisions",
    "/api/v1/actions",
    "/api/v1/commitments",
    "/api/v1/promises",
    "/api/v1/search",
    "/api/v1/ask",
    # Decisions that follow you: challenges and reopen requests.
    "/api/v1/decision-challenges",
    "/api/v1/decision-reopen-requests",
    # The company time machine (read only, participant-scoped): the MCP tool `company_as_of`.
    "/api/v1/time-machine",
    "/mcp",
)
_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
# POSTs that only read. `/mcp` is JSON-RPC over POST; its one write tool checks the scope.
# `/api/v1/decisions/check` checks a draft against decisions and stores nothing.
_READ_ONLY_POSTS = frozenset({"/api/v1/ask", "/api/v1/decisions/check", "/mcp"})


@dataclass(frozen=True)
class PatPrincipal:
    token_id: uuid.UUID
    org_id: uuid.UUID
    user_id: uuid.UUID
    clerk_user_id: str
    scope: str
    rbac_tier: str
    token_hash: str
    expires_at: datetime
    # `personal` (made in settings) or `oauth` (minted by the authorization server for an MCP
    # client such as Claude; bound to the MCP resource, so the gateway admits it on `/mcp` only).
    kind: str = "personal"

    def can_write(self) -> bool:
        return self.scope == "read_write"


_current: ContextVar[PatPrincipal | None] = ContextVar("mytheus_pat", default=None)


def current_principal() -> PatPrincipal | None:
    """The principal `PatAuthMiddleware` resolved for this request, if any."""
    return _current.get()


def is_pat(token: str | None) -> bool:
    return bool(token) and str(token).startswith(PAT_PREFIX)


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate(org_id: uuid.UUID) -> tuple[str, str, str]:
    """A fresh token for `org_id` -> (token, sha256 hex, last four characters)."""
    while True:
        secret = secrets.token_urlsafe(_SECRET_BYTES)
        if len(secret) == 43:
            break
    token = f"{PAT_PREFIX}{org_id.hex}_{secret}"
    return token, hash_token(token), token[-4:]


def org_of(token: str) -> uuid.UUID | None:
    match = _FORMAT.match(token)
    return uuid.UUID(hex=match.group(1)) if match else None


def bearer_of(headers: Any) -> str | None:
    """The bearer credential from an `Authorization` header, or None."""
    raw = headers.get("authorization", "") if headers is not None else ""
    scheme, _, token = str(raw).partition(" ")
    return token.strip() if scheme.lower() == "bearer" and token.strip() else None


def _on_prefix(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def allows(method: str, path: str, scope: str) -> Literal["ok", "not_public", "scope"]:
    """Whether a token of `scope` may make this call at all (its owner's RBAC still applies)."""
    if not any(_on_prefix(path, p) for p in PUBLIC_API_PREFIXES):
        return "not_public"
    method = method.upper()
    if method in _READ_METHODS or (method == "POST" and path in _READ_ONLY_POSTS):
        return "ok"
    return "ok" if scope == "read_write" else "scope"


def invalid() -> ProblemDetail:
    """The one answer for every bad token."""
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Invalid token")


def refused(reason: Literal["not_public", "scope"]) -> ProblemDetail:
    if reason == "not_public":
        return ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "API tokens can only call the public API; sign in to use this route",
            extra={"reason": "not_public_api"},
        )
    return ProblemDetail(
        403,
        ErrorCode.FORBIDDEN_ROLE,
        "This API token is read-only",
        extra={"reason": "read_only_token"},
    )


# An OAuth access token (`kind = 'oauth'`) also dies with its client: a client an operator
# revoked (`oauth_clients.revoked_at`) stops every token it holds on the next request.
_LOOKUP = text(
    "SELECT t.id, t.user_id, t.scope, t.expires_at, t.token_hash, t.kind, u.clerk_user_id, "
    "g.tier "
    "FROM api_tokens t "
    "JOIN users u ON u.id = t.user_id AND u.deleted_at IS NULL "
    "LEFT JOIN rbac_grants g ON g.user_id = u.id AND g.org_id = u.org_id "
    "LEFT JOIN oauth_clients c ON c.client_id = t.oauth_client_id "
    "WHERE t.token_hash = :h AND t.revoked_at IS NULL AND t.expires_at > now() "
    "AND (t.oauth_client_id IS NULL OR c.revoked_at IS NULL)"
)


async def resolve(session: AsyncSession, token: str) -> PatPrincipal | None:
    """Resolve a presented token inside `session`'s transaction, or None. Sets
    `app.current_org` (transaction-locally) to the token's tenant as a side effect."""
    from libs.shared.org_resolver import resolve_org_by_id
    from libs.shared.tenancy import set_rls

    claimed = org_of(token)
    if claimed is None:
        return None
    org_id = await resolve_org_by_id(session, claimed)
    if org_id is None:
        return None
    await set_rls(session, org_id)
    digest = hash_token(token)
    row = (await session.execute(_LOOKUP, {"h": digest})).mappings().first()
    if row is None or not secrets.compare_digest(str(row["token_hash"]), digest):
        return None
    return PatPrincipal(
        token_id=row["id"],
        org_id=org_id,
        user_id=row["user_id"],
        clerk_user_id=str(row["clerk_user_id"]),
        scope=str(row["scope"]),
        rbac_tier=str(row["tier"] or ""),
        token_hash=digest,
        expires_at=row["expires_at"],
        kind=str(row["kind"] or "personal"),
    )


async def authenticate(token: str, method: str, path: str) -> PatPrincipal:
    """Resolve and gate one request. Raises the 401 / 403 ProblemDetail on refusal."""
    from libs.shared.db import get_sessionmaker

    async with get_sessionmaker()() as session, session.begin():
        principal = await resolve(session, token)
    if principal is None:
        raise invalid()
    verdict = allows(method, path, principal.scope)
    if verdict != "ok":
        raise refused(verdict)
    return principal


def claims_for(principal: PatPrincipal) -> dict[str, Any]:
    """What `auth.verify_token` returns for a resolved token: a `sub` like a Clerk token's, and
    the principal under `PAT_CLAIM`. Deliberately NO organization claim: code that resolves a
    tenant from a Clerk org claim fails closed on a token instead of guessing."""
    return {
        "sub": principal.clerk_user_id,
        PAT_CLAIM: {
            "token_id": str(principal.token_id),
            "org_id": str(principal.org_id),
            "user_id": str(principal.user_id),
            "scope": principal.scope,
        },
    }


class PatAuthMiddleware:
    """Pure ASGI: resolves a `mytheus_pat_` bearer once per request for the service tier.

    Anything else passes through untouched (Clerk JWTs are verified by the route dependencies
    as before). A bad token is answered here with the RFC 7807 401/403, before any route runs.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        token = bearer_of(headers)
        if not is_pat(token):
            await self.app(scope, receive, send)
            return
        try:
            principal = await authenticate(str(token), scope["method"], scope["path"])
        except ProblemDetail as exc:
            await _send_problem(scope, receive, send, exc)
            return
        reset = _current.set(principal)
        try:
            await self.app(scope, receive, send)
        finally:
            _current.reset(reset)


async def _send_problem(scope: Scope, receive: Receive, send: Send, exc: ProblemDetail) -> None:
    from libs.shared.errors import problem_response

    response = problem_response(
        exc.status, exc.code, exc.detail, instance=scope["path"], extra=exc.extra
    )
    await response(scope, receive, send)
