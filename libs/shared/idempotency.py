"""Idempotency-Key middleware for mutating requests.

Every state-changing POST/PUT/PATCH may carry an `Idempotency-Key` header. If the same key is
replayed within the TTL window (24h), the stored response is returned with
`X-Idempotent-Replayed: true` without re-executing the handler. Mounted on every service via
`libs.shared.app_factory.create_app`.
"""

from __future__ import annotations

import json
from typing import cast

from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.types import ASGIApp

_TTL = 86_400  # 24 h
_MUTATING = ("POST", "PUT", "PATCH")


class IdempotencyMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, redis: Redis[bytes]) -> None:
        super().__init__(app)
        self.redis = redis

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        if request.method not in _MUTATING:
            return await call_next(request)

        key = request.headers.get("Idempotency-Key")
        if not key:
            return await call_next(request)

        cache_key = f"nexus:idem:{key}"
        cached = await self.redis.get(cache_key)
        if cached:
            data = json.loads(cached)
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

        await self.redis.set(
            cache_key,
            json.dumps({"body": body.decode(), "status": response.status_code}),
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
