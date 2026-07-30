"""IdempotencyMiddleware: replay within a tenant, and NEVER across tenants (defect D2).

The cross-tenant test is the one that matters. Plan 0 keyed the cache on the client-supplied
`Idempotency-Key` header alone, so two tenants picking the same key - "1", a retry counter, a
UUID a client library reuses - shared a cache entry, and the second caller was handed the
first one's full response body. No query is issued on that path, so RLS cannot mitigate it.
"""

from __future__ import annotations

import uuid

import fakeredis.aioredis
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from libs.shared.idempotency import IdempotencyMiddleware, cache_key

ORG_A = uuid.uuid4()
ORG_B = uuid.uuid4()


def _client(redis, tenant_header: str = "x-test-org") -> TestClient:
    """A test resolver keyed off a header, standing in for verify-token-then-resolve.

    The middleware takes an injected resolver precisely so this test needs no Clerk and no
    database. What is under test is the KEYING, not how the tenant was established.
    """

    async def resolver(request):
        raw = request.headers.get(tenant_header)
        return uuid.UUID(raw) if raw else None

    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, redis=redis, tenant_resolver=resolver)
    counter = {"n": 0}

    @app.post("/echo")
    async def echo() -> dict[str, int]:
        counter["n"] += 1
        return {"count": counter["n"]}

    return TestClient(app)


def test_replay_within_one_tenant():
    client = _client(fakeredis.aioredis.FakeRedis())

    hdrs = {"Idempotency-Key": "k1", "x-test-org": str(ORG_A)}
    r1 = client.post("/echo", headers=hdrs)
    r2 = client.post("/echo", headers=hdrs)
    assert r1.status_code == 200 and r1.json()["count"] == 1
    assert r2.headers.get("X-Idempotent-Replayed") == "true"
    assert r2.json()["count"] == 1  # handler did not run again


def test_same_key_in_two_tenants_never_shares_a_response():
    """Defect D2. The same Idempotency-Key in two tenants must run the handler twice."""
    client = _client(fakeredis.aioredis.FakeRedis())

    a = client.post("/echo", headers={"Idempotency-Key": "shared", "x-test-org": str(ORG_A)})
    b = client.post("/echo", headers={"Idempotency-Key": "shared", "x-test-org": str(ORG_B)})

    assert a.json()["count"] == 1
    assert "X-Idempotent-Replayed" not in b.headers, "tenant B was served tenant A's cache entry"
    assert b.json()["count"] == 2, "tenant B's handler must actually run"


def test_no_resolvable_tenant_is_passed_through_uncached():
    """Fail closed: with no tenant there is no namespace, so nothing is read or written."""
    redis = fakeredis.aioredis.FakeRedis()
    client = _client(redis)

    a = client.post("/echo", headers={"Idempotency-Key": "anon"})
    b = client.post("/echo", headers={"Idempotency-Key": "anon"})
    assert a.json()["count"] != b.json()["count"], "an untenanted request must not be cached"
    assert "X-Idempotent-Replayed" not in b.headers


def test_no_header_means_no_caching():
    client = _client(fakeredis.aioredis.FakeRedis())
    a = client.post("/echo", headers={"x-test-org": str(ORG_A)})
    b = client.post("/echo", headers={"x-test-org": str(ORG_A)})
    assert a.json()["count"] != b.json()["count"]
    assert "X-Idempotent-Replayed" not in b.headers


@pytest.mark.parametrize("key", ["k1", "a:b:c", ""])
def test_cache_key_carries_the_frozen_tenant_prefix(key):
    """The `nexus:{org_id}:` prefix is frozen canon, and it is what makes a per-tenant RTBF
    purge expressible as one SCAN."""
    assert cache_key(ORG_A, key).startswith(f"nexus:{ORG_A}:")
    assert cache_key(ORG_A, key) != cache_key(ORG_B, key)
