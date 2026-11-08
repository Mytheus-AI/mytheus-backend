"""`worklogs`: Plan 7-9 section 3.3 step 3 acceptance checks, against a real Postgres.

Consumer (`handle_agent_result`, the testable core of `core-api`'s `agent-results` loop):
1. a `> 0.7` note is written with `surfaced = true`; a `0.4-0.7` note with `surfaced = false`;
   a `< 0.4` note writes nothing;
2. a redelivered message writes nothing more;
3. the tenant's `agent_confidence_overrides` decide the band, not the payload (ADR-0030 D3);
4. a message naming another tenant's meeting is poison, never a row (gap 31: an FK alone
   would have accepted it);
5. other agent kinds are consumed without a row until their band.

Route (`GET /api/v1/meetings/:id/worklogs?user_id=me`):
6. the caller sees only their own surfaced notes, oldest first;
7. another tenant's meeting is a 404; asking for anyone but `me` is a 422;
8. RLS: under tenant B, tenant A's rows do not exist.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import fakeredis.aioredis
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from services.core_api.consumer import PoisonMessage, handle_agent_result
from services.core_api.routes import worklogs
from tests.conftest import APP_URL
from tests.test_chat_api import (  # same seeding helpers and auth override pattern
    Tenant,
    _create_test_app,
    _sql,
    seed_meeting,
    seed_tenant,
    sign_in,
)


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    from starlette.testclient import TestClient

    from libs.shared import auth as auth_mod
    from libs.shared import db as db_mod
    from tests.test_chat_api import PRINCIPAL

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    app = _create_test_app("core-api-worklogs-test")
    app.include_router(worklogs.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    with TestClient(app) as c:
        yield c
    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def result(
    t: Tenant,
    meeting_id: uuid.UUID,
    confidence: float,
    *,
    user_id: uuid.UUID | None = None,
    window: str = "w0",
    agent: str = "PersonalListenerAgent",
    kind: str = "note",
    key: str | None = None,
) -> dict[str, Any]:
    uid = user_id or t.user_id
    return {
        "header": {
            "event_id": str(uuid.uuid4()),
            "idempotency_key": key or f"agentresult:{meeting_id}:{window}:{uid}:{agent}",
            "org_id": str(t.org_id),
            "schema_version": 1,
            "producer": "agent-runtime-service",
            "occurred_at": 0,
            "correlation_id": None,
            "causation_id": None,
        },
        "meeting_id": str(meeting_id),
        "window_id": f"{meeting_id}:{window}",
        "user_id": str(uid),
        "agent": agent,
        "kind": kind,
        "relevance_score": 0.8,
        "confidence": confidence,
        "content": f"- note at {confidence}",
        "payload_json": json.dumps({"surfaced": confidence > 0.7}),
        "source_chunk_ids": [str(uuid.uuid4())],
    }


async def apply(value: dict[str, Any], redis: Any | None = None) -> bool:
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            return await handle_agent_result(conn, redis or fakeredis.aioredis.FakeRedis(), value)  # type: ignore[arg-type]
    finally:
        await engine.dispose()


def notes(meeting_id: uuid.UUID) -> list[dict[str, Any]]:
    return _sql(
        "SELECT confidence, surfaced, window_id, user_id FROM worklogs "
        "WHERE meeting_id = %(m)s ORDER BY created_at",
        {"m": str(meeting_id)},
    )


# --- consumer ------------------------------------------------------------------------------


async def test_the_confidence_band_decides_what_is_written(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    assert await apply(result(t, m, 0.91, window="w1")) is True
    assert await apply(result(t, m, 0.55, window="w2")) is True
    assert await apply(result(t, m, 0.39, window="w3")) is False
    rows = notes(m)
    assert [(round(r["confidence"], 2), r["surfaced"]) for r in rows] == [
        (0.91, True),
        (0.55, False),
    ]
    assert rows[0]["window_id"] == f"{m}:w1"


async def test_a_redelivered_result_writes_nothing_more(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    value = result(t, m, 0.9)
    redis = fakeredis.aioredis.FakeRedis()
    assert await apply(value, redis) is True
    assert await apply(value, redis) is False
    assert await apply(value) is False  # cache evicted: the inbox row is the authority
    assert len(notes(m)) == 1


async def test_tenant_overrides_decide_the_band(migrated_db):
    t = seed_tenant()
    _sql(
        "INSERT INTO tenant_settings (org_id, agent_confidence_overrides) "
        "VALUES (%(o)s, %(j)s) ON CONFLICT (org_id) DO UPDATE "
        "SET agent_confidence_overrides = EXCLUDED.agent_confidence_overrides",
        {
            "o": str(t.org_id),
            "j": json.dumps({"PersonalListenerAgent": {"surface": 0.95, "silent": 0.6}}),
        },
    )
    m = seed_meeting(t.org_id)
    await apply(result(t, m, 0.9, window="a"))  # surfaced by default, silent here
    await apply(result(t, m, 0.5, window="b"))  # silent by default, dropped here
    assert [(r["surfaced"]) for r in notes(m)] == [False]


async def test_another_tenants_meeting_or_user_is_poison(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    foreign = seed_meeting(b.org_id)
    with pytest.raises(PoisonMessage):
        await apply(result(a, foreign, 0.9))
    own = seed_meeting(a.org_id)
    with pytest.raises(PoisonMessage):
        await apply(result(a, own, 0.9, user_id=b.user_id))
    assert notes(foreign) == [] and notes(own) == []
    bad = result(a, own, 0.9)
    bad["header"]["org_id"] = "org_2abc"
    with pytest.raises(PoisonMessage):
        await apply(bad)


async def test_an_unknown_tenant_is_poison_not_a_crash(migrated_db):
    """Found by the 2026-11-08 end-to-end run: a record for a tenant with no `organizations`
    row raised IntegrityError from the inbox FK and crash-looped the consumer."""
    ghost = Tenant(uuid.uuid4(), "org_ghost", uuid.uuid4(), "user_ghost")
    with pytest.raises(PoisonMessage):
        await apply(result(ghost, uuid.uuid4(), 0.9))


async def test_a_failed_apply_does_not_poison_the_dedupe_cache(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    redis = fakeredis.aioredis.FakeRedis()
    value = result(a, seed_meeting(b.org_id), 0.9)
    with pytest.raises(PoisonMessage):
        await apply(value, redis)
    assert await redis.keys("nexus:*:dedupe:agent-results:*") == []


async def test_other_agent_kinds_are_consumed_without_a_row(migrated_db):
    t = seed_tenant()
    m = seed_meeting(t.org_id)
    assert (
        await apply(result(t, m, 0.95, agent="DecisionAgent", kind="decision_candidate")) is False
    )
    assert notes(m) == []


# --- route ---------------------------------------------------------------------------------


def test_my_notes_returns_only_my_surfaced_notes_in_order(client, migrated_db):
    import asyncio

    me = seed_tenant()
    m = seed_meeting(me.org_id)
    colleague = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'c@x.com')",
        {"u": str(colleague), "o": str(me.org_id), "c": f"user_{colleague.hex[:12]}"},
    )
    for value in (
        result(me, m, 0.8, window="1"),
        result(me, m, 0.6, window="2"),
        result(me, m, 0.95, window="3"),
        result(me, m, 0.9, user_id=colleague, window="1"),
    ):
        asyncio.run(apply(value))

    sign_in(me)
    resp = client.get(f"/api/v1/meetings/{m}/worklogs?user_id=me")
    assert resp.status_code == 200, resp.text
    items = resp.json()["items"]
    assert [i["window_id"] for i in items] == [f"{m}:1", f"{m}:3"]
    assert {i["user_id"] for i in items} == {str(me.user_id)}
    assert client.get(f"/api/v1/meetings/{m}/worklogs").status_code == 200


def test_cross_tenant_is_404_and_other_users_are_refused(client, migrated_db):
    a, b = seed_tenant(), seed_tenant()
    theirs = seed_meeting(b.org_id)
    sign_in(a)
    assert client.get(f"/api/v1/meetings/{theirs}/worklogs").status_code == 404
    own = seed_meeting(a.org_id)
    resp = client.get(f"/api/v1/meetings/{own}/worklogs?user_id={b.user_id}")
    assert resp.status_code == 422


async def test_rls_hides_another_tenants_worklogs(migrated_db):
    a, b = seed_tenant(), seed_tenant()
    m = seed_meeting(a.org_id)
    await apply(result(a, m, 0.9))
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(b.org_id)}
            )
            assert (await conn.execute(text("SELECT count(*) FROM worklogs"))).scalar() == 0
        async with engine.connect() as conn, conn.begin():
            assert (await conn.execute(text("SELECT count(*) FROM worklogs"))).scalar() == 0
    finally:
        await engine.dispose()
