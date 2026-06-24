"""Idempotency-Key middleware for mutating POSTs.

Every state-changing POST must accept an `Idempotency-Key` header. If the same
key is replayed within the TTL window (24h), the stored response is returned
without re-executing the handler.
"""
from __future__ import annotations

import json
from typing import Callable

from fastapi import Request, Response
from redis.asyncio import Redis

_TTL = 86_400  # 24 h


async def idempotency_middleware(
    request: Request,
    call_next: Callable,
    redis: Redis,
) -> Response:
    if request.method not in ("POST", "PUT", "PATCH"):
        return await call_next(request)

    key = request.headers.get("Idempotency-Key")
    if not key:
        return await call_next(request)

    cache_key = f"nexus:idem:{key}"
    cached = await redis.get(cache_key)
    if cached:
        data = json.loads(cached)
        return Response(
            content=data["body"],
            status_code=data["status"],
            media_type="application/json",
            headers={"X-Idempotent-Replayed": "true"},
        )

    response = await call_next(request)

    body = b""
    async for chunk in response.body_iterator:
        body += chunk

    await redis.set(
        cache_key,
        json.dumps({"body": body.decode(), "status": response.status_code}),
        ex=_TTL,
    )

    return Response(
        content=body,
        status_code=response.status_code,
        media_type=response.media_type,
        headers=dict(response.headers),
    )
