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
from fastapi.responses import JSONResponse
from starlette.responses import Response
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

    # A second route, so cross-endpoint reuse of one key is expressible.
    @app.post("/other")
    async def other() -> dict[str, str]:
        counter["n"] += 1
        return {"route": "other"}

    # Fails on the first call and succeeds afterwards, standing in for a transient dependency
    # failure that a client retries with the same key.
    @app.post("/flaky")
    async def flaky() -> Response:
        counter["n"] += 1
        if counter["n"] == 1:
            return JSONResponse(status_code=500, content={"error": "transient"})
        return JSONResponse(status_code=200, content={"ok": True})

    return TestClient(app, raise_server_exceptions=False)


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


def test_one_key_reused_on_a_different_route_is_rejected_not_replayed():
    """The cross-endpoint collision. Before the fingerprint, the second call was served the
    FIRST route's body with a 200: a success response for an operation that never ran."""
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "reused", "x-test-org": str(ORG_A)}

    first = client.post("/echo", headers=hdrs)
    second = client.post("/other", headers=hdrs)

    assert first.status_code == 200 and first.json() == {"count": 1}
    assert second.status_code == 409, "reuse across routes must be an error, not a replay"
    assert second.json()["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert second.headers["content-type"].startswith("application/problem+json")
    assert "route" not in second.json(), "must not leak the other route's response"
    assert second.json().get("count") is None


def test_the_same_route_still_replays():
    """The fingerprint must not break the feature it is protecting."""
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "same", "x-test-org": str(ORG_A)}

    a = client.post("/echo", headers=hdrs)
    b = client.post("/echo", headers=hdrs)
    assert b.headers.get("X-Idempotent-Replayed") == "true"
    assert a.json() == b.json()


def test_a_differing_query_string_is_a_different_request():
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "q", "x-test-org": str(ORG_A)}

    assert client.post("/echo?a=1", headers=hdrs).status_code == 200
    assert client.post("/echo?a=2", headers=hdrs).status_code == 409


def test_an_error_response_is_not_cached_so_a_retry_can_succeed():
    """Caching every status pinned a transient 500 for the full 24h TTL, so every retry with
    that key replayed the error and the handler never ran again."""
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "retry-me", "x-test-org": str(ORG_A)}

    first = client.post("/flaky", headers=hdrs)
    assert first.status_code == 500

    second = client.post("/flaky", headers=hdrs)
    assert second.status_code == 200, "the retry must reach the handler, not replay the 500"
    assert "X-Idempotent-Replayed" not in second.headers
    assert second.json() == {"ok": True}

    third = client.post("/flaky", headers=hdrs)
    assert third.headers.get("X-Idempotent-Replayed") == "true", "the success IS cached"


@pytest.mark.parametrize("key", ["k1", "a:b:c", ""])
def test_cache_key_carries_the_frozen_tenant_prefix(key):
    """The `nexus:{org_id}:` prefix is frozen canon, and it is what makes a per-tenant RTBF
    purge expressible as one SCAN."""
    assert cache_key(ORG_A, key).startswith(f"nexus:{ORG_A}:")
    assert cache_key(ORG_A, key) != cache_key(ORG_B, key)
