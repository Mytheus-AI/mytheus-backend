"""IdempotencyMiddleware: a repeated Idempotency-Key replays the cached response (no re-run)."""

from __future__ import annotations

import fakeredis.aioredis
from fastapi import FastAPI
from starlette.testclient import TestClient

from libs.shared.idempotency import IdempotencyMiddleware


def _client() -> TestClient:
    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, redis=fakeredis.aioredis.FakeRedis())
    counter = {"n": 0}

    @app.post("/echo")
    async def echo() -> dict[str, int]:
        counter["n"] += 1
        return {"count": counter["n"]}

    return TestClient(app)


def test_idempotency_replay_and_passthrough():
    client = _client()

    r1 = client.post("/echo", headers={"Idempotency-Key": "k1"})
    r2 = client.post("/echo", headers={"Idempotency-Key": "k1"})
    assert r1.status_code == 200 and r1.json()["count"] == 1
    assert r2.headers.get("X-Idempotent-Replayed") == "true"
    assert r2.json()["count"] == 1  # handler did not run again

    # Without the header nothing is cached: each call runs the handler.
    a = client.post("/echo")
    b = client.post("/echo")
    assert a.json()["count"] != b.json()["count"]
    assert "X-Idempotent-Replayed" not in b.headers
