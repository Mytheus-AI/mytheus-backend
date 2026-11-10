"""Idempotency-Key middleware for mutating requests.

Every state-changing POST/PUT/PATCH may carry an `Idempotency-Key` header. If the same key is
replayed within the TTL window (24h) BY THE SAME TENANT, the stored response is returned with
`X-Idempotent-Replayed: true` without re-executing the handler. Mounted on every service via
`libs.shared.app_factory.create_app`.

Defect D2. The Plan 0 version keyed the cache on the client-supplied `Idempotency-Key` header
alone, on all nine services:

    cache_key = f"nexus:idem:{key}"

The header is chosen by the caller, so two tenants sending the same value shared one cache
entry, and the second one received the first one's full response body. It is a cross-tenant
leak that never touches Postgres, so row-level security cannot mitigate it: the data is
served out of Redis before a query is ever issued. It also violated the frozen
`nexus:{org_id}:` prefix, which exists to make precisely this class of mistake impossible.

The key is now `nexus:{org_id}:idem:{key}`, and the `org_id` is resolved the same way every
other tenant decision is: verify the token, then resolve its opaque Clerk organization id
through `organizations.clerk_org_id`. A request whose tenant cannot be established is passed
straight through UNCACHED - never served from cache, never written to it. That is fail-closed:
an unauthenticated request has no tenant, so it has no cache namespace it could legitimately
read, and it is going to be rejected by the route's own auth dependency anyway.

Two further defects, both of which produced a WRONG RESPONSE rather than an error.

Cross-endpoint collision. Scoping by tenant fixed the cross-tenant leak but left the key
shared across every route in the service, so one tenant sending `Idempotency-Key: 1` to
`POST /meetings` and then the same value to `POST /decisions/{id}/confirm` received the
meeting response to the confirm call - a success body for an operation that never ran. The
stored entry now carries a fingerprint of the request it belongs to, and a key replayed
against a different request is rejected with 409 `IDEMPOTENCY_KEY_REUSED`. That error code
has been in the frozen enum since Plan 0 with nothing emitting it; this is what it is for.

Rejecting is deliberate, rather than silently giving each route its own key space. A client
reusing one key for two different operations has a bug, and splitting the namespace would
hide it while quietly removing the guarantee the client thinks it has. The fingerprint covers
method, path, query string AND a SHA-256 of the body (gap 3): the same key replayed against the
same route with a different payload is a different operation and is rejected with the same 409,
never answered with the first payload's response. Reading the body here is safe on Starlette's
BaseHTTPMiddleware, which caches it and re-feeds it to the route (`_CachedRequest`); a test
pins that the route still sees the full body.

Concurrent duplicates (gap 3). Two requests carrying the same key that arrive before the first
has finished used to both miss the cache and both run the handler - the double-execution the
header exists to prevent. The first request now claims the key with an atomic `SET NX`
in-progress marker; a duplicate that arrives while it is held gets 409
`IDEMPOTENCY_KEY_REUSED` with `Retry-After`, and a retry after completion replays normally.
The marker has a short TTL so a crashed worker cannot wedge a key for 24h, and it is released
when the handler fails or returns a non-2xx, so the client's retry reaches the handler.

Error caching. Every response was cached regardless of status, so a 500 from a transient
dependency failure was pinned for the full 24h TTL and every retry with that key replayed the
error without ever reaching the handler again - the exact opposite of what a client retrying
with an idempotency key is trying to achieve. Only 2xx responses are stored now.
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
from collections.abc import Awaitable, Callable
from typing import cast
from uuid import UUID

from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp

from libs.shared.errors import ErrorCode, problem_response

logger = logging.getLogger(__name__)

_TTL = 86_400  # 24 h
# How long an in-progress claim survives without completing. Long enough to cover any handler
# we run synchronously; short enough that a worker killed mid-request frees the key quickly.
_IN_PROGRESS_TTL = 120
_IN_PROGRESS = "in_progress"
_MUTATING = ("POST", "PUT", "PATCH")

# Resolves the tenant for a request, or None if there is not one. Injected rather than
# imported so the middleware stays unit-testable without a database, and so a service with a
# different tenant source can supply its own without forking this file.
TenantResolver = Callable[[Request], Awaitable[UUID | None]]


def cache_key(org_id: UUID, idempotency_key: str) -> str:
    """The frozen tenant prefix, applied. `org_id` first so a tenant's keys are contiguous
    and a per-tenant purge (RTBF) is one `SCAN` over `nexus:{org_id}:*`.

    The route is NOT part of the key. It belongs in the fingerprint instead, so that reusing
    one key for two operations is an error the client sees rather than two silently separate
    caches that make the reuse invisible.
    """
    return f"nexus:{org_id}:idem:{idempotency_key}"


def request_fingerprint(request: Request, body: bytes = b"") -> str:
    """Identifies the request a stored response belongs to: method, path, query string and a
    SHA-256 of the body. Two requests with the same key and any difference in these are two
    operations, and the second is rejected rather than replayed."""
    query = request.url.query
    target = f"{request.method} {request.url.path}" + (f"?{query}" if query else "")
    return f"{target} sha256={hashlib.sha256(body).hexdigest()}"


async def resolve_tenant_from_token(request: Request) -> UUID | None:
    """Default resolver: verify the bearer token, then resolve the tenant root.

    Returns None on every failure rather than raising. The middleware's job is to decide
    whether caching is safe, not to authenticate the request: rejecting it is the route's
    auth dependency's job, and doing it here would change which errors the API returns.
    """
    from fastapi.security import HTTPAuthorizationCredentials

    from libs.shared.auth import clerk_org_id_from, verify_token
    from libs.shared.db import get_sessionmaker
    from libs.shared.org_resolver import resolve_org_id

    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None

    from libs.shared import pat

    if pat.is_pat(token):
        # Resolved by `PatAuthMiddleware`, which sits outside this middleware.
        principal = pat.current_principal()
        if principal is None or principal.token_hash != pat.hash_token(token):
            return None
        return principal.org_id

    try:
        claims = verify_token(HTTPAuthorizationCredentials(scheme="Bearer", credentials=token))
        clerk_org = clerk_org_id_from(claims)
        async with get_sessionmaker()() as session:
            return await resolve_org_id(session, clerk_org)
    except Exception:  # noqa: BLE001 - any failure means "no tenant", which means "no cache"
        logger.debug("idempotency: no resolvable tenant; passing through uncached", exc_info=True)
        return None


class IdempotencyMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app: ASGIApp,
        redis: Redis[bytes],
        tenant_resolver: TenantResolver | None = None,
    ) -> None:
        super().__init__(app)
        self.redis = redis
        self.tenant_resolver = tenant_resolver or resolve_tenant_from_token

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.method not in _MUTATING:
            return await call_next(request)

        key = request.headers.get("Idempotency-Key")
        if not key:
            return await call_next(request)

        org_id = await self.tenant_resolver(request)
        if org_id is None:
            # No tenant, no namespace, no cache. Deliberately not an error response: the
            # route decides what an unauthenticated request gets.
            return await call_next(request)

        redis_key = cache_key(org_id, key)
        body_in = await request.body()  # cached by BaseHTTPMiddleware; the route re-reads it
        fingerprint = request_fingerprint(request, body_in)
        token = secrets.token_hex(16)
        marker = json.dumps({"state": _IN_PROGRESS, "fingerprint": fingerprint, "token": token})

        # Atomic claim. Exactly one of N concurrent requests with this key wins the SET NX and
        # runs the handler; every other one falls through to the stored-entry branch below.
        claimed = await self.redis.set(redis_key, marker, nx=True, ex=_IN_PROGRESS_TTL)
        if not claimed:
            cached = await self.redis.get(redis_key)
            if cached is None:
                # The holder released between our SET and GET (it failed). One more attempt;
                # losing again means another request claimed it, which is an in-flight dup.
                claimed = await self.redis.set(redis_key, marker, nx=True, ex=_IN_PROGRESS_TTL)
                cached = None if claimed else await self.redis.get(redis_key)
            if not claimed:
                return self._from_stored(cached, fingerprint)

        try:
            response = cast(StreamingResponse, await call_next(request))
            body = b""
            async for chunk in response.body_iterator:
                body += chunk.encode() if isinstance(chunk, str) else chunk
        except BaseException:
            await self._release(redis_key, token)
            raise

        # Successes only. Caching an error pinned it for the full TTL, so a client retrying
        # after a transient 500 replayed that 500 without the handler ever running again.
        if 200 <= response.status_code < 300:
            await self.redis.set(
                redis_key,
                json.dumps(
                    {
                        "body": body.decode(),
                        "status": response.status_code,
                        "fingerprint": fingerprint,
                    }
                ),
                ex=_TTL,
            )
        else:
            await self._release(redis_key, token)

        headers = dict(response.headers)
        headers.pop("content-length", None)  # recomputed by the new Response
        return Response(
            content=body,
            status_code=response.status_code,
            media_type=response.media_type,
            headers=headers,
        )

    async def _release(self, redis_key: str, token: str) -> None:
        """Drop our in-progress claim, and only ours: if it expired and another request has
        since claimed the key, deleting it would let a third request run concurrently."""
        current = await self.redis.get(redis_key)
        if current is None:
            return
        try:
            data = json.loads(current)
        except ValueError:
            return
        if data.get("state") == _IN_PROGRESS and data.get("token") == token:
            await self.redis.delete(redis_key)

    @staticmethod
    def _from_stored(cached: bytes | None, fingerprint: str) -> Response:
        data = json.loads(cached) if cached else {}
        stored = data.get("fingerprint")
        # `stored is None` on a completed entry is one written before fingerprints existed.
        # Replay it rather than 409-ing every in-flight key during a rollout; they age out
        # with the 24h TTL.
        if stored is not None and stored != fingerprint:
            return problem_response(
                409,
                ErrorCode.IDEMPOTENCY_KEY_REUSED,
                detail=(
                    "This Idempotency-Key was already used for a different request. "
                    "Use a new key per operation."
                ),
            )
        if not data or data.get("state") == _IN_PROGRESS:
            response = problem_response(
                409,
                ErrorCode.IDEMPOTENCY_KEY_REUSED,
                detail=(
                    "A request with this Idempotency-Key is still in progress. "
                    "Retry after it completes to receive its response."
                ),
            )
            response.headers["Retry-After"] = "1"
            return response
        return Response(
            content=data["body"],
            status_code=data["status"],
            media_type="application/json",
            headers={"X-Idempotent-Replayed": "true"},
        )
