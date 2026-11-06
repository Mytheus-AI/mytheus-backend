"""Shared Redis client + the frozen `nexus:{org_id}:` key builder.

Mirrors `libs.shared.db.get_engine`/`get_sessionmaker`: lazy, process-wide, `lru_cache`d so
importing this module never requires `REDIS_URL` to be set, and so every caller shares one
connection pool rather than opening its own. `REDIS_URL` for local dev is `redis://localhost:6390`
(NOT the 6379 default - see `.env.example`).

`redis_key` raises if `org_id` is falsy. A cache key missing its tenant is exactly how one
tenant's data leaks into another's response through Redis rather than through Postgres, where
RLS cannot help because no query was ever issued on that path (the same class of bug
`libs/shared/idempotency.py`'s defect-D2 fix addresses for the `nexus:{org_id}:idem:` prefix).

Socket timeouts are set deliberately short: a cache is an optimization, never a dependency
(Plan 5-6 section 3.3). A Redis call that hangs indefinitely would turn "Redis is slow" into
"the request never completes", which is worse than a fast, catchable failure that the caller
falls back to Postgres from.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Any
from uuid import UUID

from redis.asyncio import Redis, from_url

_SOCKET_TIMEOUT = 1.0  # seconds; bounds how long a caller waits before falling back to Postgres


@lru_cache(maxsize=1)
def get_redis() -> Redis[bytes]:
    """The process-wide Redis client, built lazily from `REDIS_URL`.

    Tests that need to simulate an unreachable Redis should not rely on this singleton (its
    `lru_cache` would make the broken client leak into every other test in the process);
    monkeypatch the `get_redis` name where it is imported instead.
    """
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    return from_url(
        url,
        decode_responses=False,
        socket_connect_timeout=_SOCKET_TIMEOUT,
        socket_timeout=_SOCKET_TIMEOUT,
    )


def redis_key(org_id: UUID | str | None, *parts: Any) -> str:
    """Build a key under the frozen `nexus:{org_id}:` prefix.

    `org_id` first (not last) so a tenant's keys are contiguous and a per-tenant purge (RTBF)
    is one `SCAN` over `nexus:{org_id}:*` - same reasoning as `idempotency.cache_key`.

    Raises `ValueError` if `org_id` is absent. This is not defensive decoration: every caller
    of this function is about to read or write Redis, and there is no such thing as a
    tenant-less cache entry that is safe to serve to anyone.
    """
    if not org_id:
        raise ValueError("redis_key requires org_id: a cache key missing its tenant is a leak")
    return "nexus:" + ":".join((str(org_id), *(str(p) for p in parts)))
