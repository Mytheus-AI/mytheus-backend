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
method, path and query string, not the body: reading the body here would consume the ASGI
receive stream that the route still has to read.

Error caching. Every response was cached regardless of status, so a 500 from a transient
dependency failure was pinned for the full 24h TTL and every retry with that key replayed the
error without ever reaching the handler again - the exact opposite of what a client retrying
with an idempotency key is trying to achieve. Only 2xx responses are stored now.
"""

from __future__ import annotations

import json
import logging
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


def request_fingerprint(request: Request) -> str:
    """Identifies the request a stored response belongs to.

    Method, path and query string only. The body is excluded because reading it here would
    consume the ASGI receive stream before the route can read it; that is a real failure mode,
    whereas the case this misses (same key, same route, different body) still replays a
    response for the same operation rather than a different one.
    """
    query = request.url.query
    return f"{request.method} {request.url.path}" + (f"?{query}" if query else "")


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
        fingerprint = request_fingerprint(request)
        cached = await self.redis.get(redis_key)
        if cached:
            data = json.loads(cached)
            stored = data.get("fingerprint")
            # `stored is None` is an entry written before fingerprints existed. Replay it
            # rather than 409-ing every in-flight key during a rollout; they age out with the
            # 24h TTL.
            if stored is not None and stored != fingerprint:
                return problem_response(
                    409,
                    ErrorCode.IDEMPOTENCY_KEY_REUSED,
                    detail=(
                        "This Idempotency-Key was already used for a different request. "
                        "Use a new key per operation."
                    ),
                    extra={"expected": stored, "received": fingerprint},
                )
            return Response(
                content=data["body"],
                status_code=data["status"],
                media_type="application/json",
                headers={"X-Idempotent-Replayed": "true"},
            )

        response = cast(StreamingResponse, await call_next(request))

        body = b""
        async for chunk in response.body_iterator:
            body += chunk.encode() if isinstance(chunk, str) else chunk

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

        headers = dict(response.headers)
        headers.pop("content-length", None)  # recomputed by the new Response
        return Response(
            content=body,
            status_code=response.status_code,
            media_type=response.media_type,
            headers=headers,
        )
