"""IdempotencyMiddleware: replay within a tenant, and NEVER across tenants (defect D2).

The cross-tenant test is the one that matters. Plan 0 keyed the cache on the client-supplied
`Idempotency-Key` header alone, so two tenants picking the same key - "1", a retry counter, a
UUID a client library reuses - shared a cache entry, and the second caller was handed the
first one's full response body. No query is issued on that path, so RLS cannot mitigate it.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import fakeredis.aioredis
import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.responses import Response
from starlette.testclient import TestClient

from libs.shared.errors import ErrorCode
from libs.shared.idempotency import IdempotencyMiddleware, cache_key, request_fingerprint

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

    # Echoes its body back, so a test can prove the route still receives the full payload
    # after the middleware has read it for the fingerprint.
    @app.post("/body")
    async def body(request: Request) -> dict[str, object]:
        counter["n"] += 1
        return {"count": counter["n"], "got": await request.json()}

    @app.post("/boom")
    async def boom() -> dict[str, str]:
        counter["n"] += 1
        raise RuntimeError("handler crashed")

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


def test_409_body_is_a_problem_document_with_the_frozen_code():
    """The 409 path, asserted on its own: status, the frozen enum value, RFC 7807 shape."""
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "k409", "x-test-org": str(ORG_A)}
    assert client.post("/echo", headers=hdrs).status_code == 200

    r = client.post("/other", headers=hdrs)
    assert r.status_code == 409
    body = r.json()
    assert body["code"] == "IDEMPOTENCY_KEY_REUSED" == ErrorCode.IDEMPOTENCY_KEY_REUSED.value
    assert body["status"] == 409
    assert body["type"].endswith("/IDEMPOTENCY_KEY_REUSED")
    assert "X-Idempotent-Replayed" not in r.headers


def test_same_route_different_body_is_rejected_not_replayed():
    """Gap 3: same key, same route, different payload. Replaying would answer the second
    operation with the first one's result."""
    client = _client(fakeredis.aioredis.FakeRedis())
    hdrs = {"Idempotency-Key": "body-k", "x-test-org": str(ORG_A)}

    first = client.post("/body", headers=hdrs, json={"title": "one"})
    second = client.post("/body", headers=hdrs, json={"title": "two"})
    assert first.status_code == 200 and first.json()["got"] == {"title": "one"}
    assert second.status_code == 409
    assert second.json()["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert "got" not in second.json()

    again = client.post("/body", headers=hdrs, json={"title": "one"})
    assert again.headers.get("X-Idempotent-Replayed") == "true"
    assert again.json() == first.json()


def test_route_still_receives_the_body_the_middleware_hashed():
    client = _client(fakeredis.aioredis.FakeRedis())
    payload = {"title": "x" * 10_000, "n": [1, 2, 3]}
    r = client.post(
        "/body", headers={"Idempotency-Key": "big", "x-test-org": str(ORG_A)}, json=payload
    )
    assert r.status_code == 200 and r.json()["got"] == payload


def test_fingerprint_covers_method_path_query_and_body():
    def req(method: str, path: str, query: str = "") -> Request:
        return Request(
            {
                "type": "http",
                "method": method,
                "path": path,
                "query_string": query.encode(),
                "headers": [],
                "scheme": "http",
                "server": ("t", 80),
            }
        )

    base = request_fingerprint(req("POST", "/a"), b"{}")
    assert base != request_fingerprint(req("PUT", "/a"), b"{}")
    assert base != request_fingerprint(req("POST", "/b"), b"{}")
    assert base != request_fingerprint(req("POST", "/a", "x=1"), b"{}")
    assert base != request_fingerprint(req("POST", "/a"), b'{"a":1}')
    assert base == request_fingerprint(req("POST", "/a"), b"{}")


def test_stored_entries_use_the_frozen_tenant_prefix():
    redis = fakeredis.aioredis.FakeRedis()
    client = _client(redis)
    client.post("/echo", headers={"Idempotency-Key": "pfx", "x-test-org": str(ORG_A)})

    async def keys() -> list[bytes]:
        return [k async for k in redis.scan_iter("*")]

    found = asyncio.run(keys())
    assert found == [f"nexus:{ORG_A}:idem:pfx".encode()]


def test_concurrent_duplicate_runs_the_handler_once():
    """Two in-flight requests with one key: exactly one runs the handler, the other gets a
    409 with Retry-After rather than a second execution."""
    redis = fakeredis.aioredis.FakeRedis()
    gate = asyncio.Event()
    runs = {"n": 0}

    async def resolver(request):
        return ORG_A

    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, redis=redis, tenant_resolver=resolver)

    @app.post("/slow")
    async def slow() -> dict[str, int]:
        runs["n"] += 1
        await gate.wait()
        return {"runs": runs["n"]}

    async def scenario() -> tuple[httpx.Response, httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            hdrs = {"Idempotency-Key": "race"}
            first = asyncio.create_task(c.post("/slow", headers=hdrs, json={}))
            while runs["n"] == 0:  # wait until the first request is inside the handler
                await asyncio.sleep(0)
            dup = await c.post("/slow", headers=hdrs, json={})
            gate.set()
            done = await first
            replay = await c.post("/slow", headers=hdrs, json={})
            return done, dup, replay

    done, dup, replay = asyncio.run(scenario())
    assert runs["n"] == 1, "the handler ran twice for one idempotency key"
    assert done.status_code == 200
    assert dup.status_code == 409 and dup.json()["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert dup.headers.get("Retry-After") == "1"
    assert replay.headers.get("X-Idempotent-Replayed") == "true"
    assert replay.json() == done.json()


def test_in_progress_claim_is_released_when_the_handler_raises():
    redis = fakeredis.aioredis.FakeRedis()
    client = _client(redis)
    hdrs = {"Idempotency-Key": "crash", "x-test-org": str(ORG_A)}

    assert client.post("/boom", headers=hdrs).status_code == 500
    assert asyncio.run(redis.get(cache_key(ORG_A, "crash"))) is None
    # A retry reaches the handler again instead of a stuck in-progress 409.
    assert client.post("/boom", headers=hdrs).status_code == 500


def test_in_progress_claim_is_released_on_non_2xx():
    redis = fakeredis.aioredis.FakeRedis()
    client = _client(redis)
    hdrs = {"Idempotency-Key": "flaky-lock", "x-test-org": str(ORG_A)}
    assert client.post("/flaky", headers=hdrs).status_code == 500
    assert asyncio.run(redis.get(cache_key(ORG_A, "flaky-lock"))) is None


def test_legacy_entry_without_fingerprint_still_replays():
    redis = fakeredis.aioredis.FakeRedis()
    asyncio.run(
        redis.set(cache_key(ORG_A, "old"), json.dumps({"body": '{"legacy":1}', "status": 201}))
    )
    client = _client(redis)
    r = client.post("/echo", headers={"Idempotency-Key": "old", "x-test-org": str(ORG_A)})
    assert r.status_code == 201 and r.json() == {"legacy": 1}
    assert r.headers.get("X-Idempotent-Replayed") == "true"
