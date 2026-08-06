"""`get_user_context()` + its Redis cache: Plan 5-6 section 3.3.

Structural pattern mirrors `tests/test_org_model_reconciliation.py`: one module-scoped
`TestClient` with `verify_token` overridden (no live Clerk needed), everything downstream real
- `app.current_org`, RLS, the actual route and resolver bodies, and a REAL Redis at
`REDIS_URL` (localhost:6380 for local dev / CI - this band's infrastructure is real, not
faked).

The acceptance checks from the plan, each covered by name below:
  1. cache miss -> Postgres compute; a second call within TTL is served from cache, not
     recomputed (`test_second_call_is_served_from_cache_not_recomputed`).
  2. `TTL nexus:{org}:user_ctx:{user}` <= 300s (`test_ttl_on_key_is_at_most_300_seconds`).
  3. `PATCH /users/{id}` changing `role` evicts the key; the next context reflects the new
     role AND new topics (`test_patch_evicts_cache_and_next_read_reflects_new_role_and_topics`).
  4. cross-org isolation: a JWT for org B cannot fetch org A's user context
     (`test_cross_org_jwt_cannot_fetch_another_orgs_user_context`).
  5. gRPC `GetUserContext` with `org_id` in metadata returns the same payload as REST
     (`test_grpc_and_rest_return_the_same_context`).

Plus the property the brief called out as the one most likely to be built subtly wrong: the
cache is an optimization, never a dependency
(`test_full_path_survives_an_unreachable_redis_and_still_returns_correct_data`,
`test_a_corrupt_cache_entry_is_treated_as_a_miss_not_an_error`), and the tenancy guarantee that
makes the whole cache safe to have at all
(`test_redis_key_never_shared_between_two_orgs_for_the_same_user_id_value`).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from fastapi import FastAPI
from redis.asyncio import from_url as redis_from_url
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from libs.shared.cache import redis_key
from services.identity_tenant_service import grpc_server as grpc_server_mod
from services.identity_tenant_service.context import service as context_service
from services.identity_tenant_service.routes import context as context_routes
from services.identity_tenant_service.routes import users as users_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL

# `asyncio_mode = "auto"` (pyproject.toml) runs any `async def test_...` directly - no marker
# needed, matching every other async test file in this suite (e.g. tests/test_grpc.py).


# --- fixtures & seeding, mirroring tests/test_org_model_reconciliation.py exactly -----------


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


@pytest.fixture(scope="module")
def client(migrated_db):
    import os as _os

    previous = _os.environ.get("DATABASE_URL")
    _os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app: FastAPI = create_app("identity-test")
    app.include_router(users_routes.router)
    app.include_router(context_routes.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }

    with TestClient(app) as c:
        yield c

    if previous is None:
        _os.environ.pop("DATABASE_URL", None)
    else:
        _os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


class Tenant:
    def __init__(self, org_id, clerk_org, user_id, clerk_user, tier):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user
        self.tier = tier


def _seed_org_row(org_id: uuid.UUID, slug_prefix: str) -> str:
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"{slug_prefix}-{org_id}", "cid": clerk_org},
    )
    return clerk_org


def _seed_user_row(org_id: uuid.UUID, tier: str, role: str | None = None) -> Tenant:
    user_id = uuid.uuid4()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email, role) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com', %(role)s)",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user, "role": role},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return Tenant(org_id, "", user_id, clerk_user, tier)


def seed_tenant(
    tier: str = "member", slug_prefix: str = "usr-ctx", role: str | None = None
) -> Tenant:
    org_id = uuid.uuid4()
    clerk_org = _seed_org_row(org_id, slug_prefix)
    t = _seed_user_row(org_id, tier, role=role)
    t.clerk_org = clerk_org
    return t


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user


def _seed_team_and_department(org_id: uuid.UUID, user_id: uuid.UUID) -> tuple[uuid.UUID, uuid.UUID]:
    dept_id, team_id = uuid.uuid4(), uuid.uuid4()
    _sql(
        "INSERT INTO departments (id, org_id, name) VALUES (%(i)s, %(o)s, 'Engineering')",
        {"i": str(dept_id), "o": str(org_id)},
    )
    _sql(
        "INSERT INTO teams (id, org_id, name, department_id) "
        "VALUES (%(i)s, %(o)s, 'Platform', %(d)s)",
        {"i": str(team_id), "o": str(org_id), "d": str(dept_id)},
    )
    _sql(
        "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s, %(u)s, %(o)s)",
        {"t": str(team_id), "u": str(user_id), "o": str(org_id)},
    )
    return dept_id, team_id


def _seed_meeting(org_id: uuid.UUID, user_id: uuid.UUID, title: str = "Standup") -> uuid.UUID:
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title, status) "
        "VALUES (%(i)s, %(o)s, %(title)s, 'scheduled')",
        {"i": str(meeting_id), "o": str(org_id), "title": title},
    )
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": str(meeting_id), "u": str(user_id), "o": str(org_id)},
    )
    return meeting_id


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6380")


async def _ttl(key: str) -> int:
    r = redis_from_url(_redis_url(), decode_responses=False)
    try:
        return await r.ttl(key)
    finally:
        await r.aclose()


# --- 1 & 2: cache miss computes, second call is served from cache ---------------------------


def test_get_user_context_returns_role_skills_team_department_meetings_topics(client):
    """Happy path over HTTP: everything the plan lists is present and correct."""
    t = seed_tenant(tier="member", role="engineer")
    dept_id, team_id = _seed_team_and_department(t.org_id, t.user_id)
    meeting_id = _seed_meeting(t.org_id, t.user_id)
    sign_in(t)

    resp = client.get(f"/context/{t.user_id}")
    assert resp.status_code == 200, resp.text
    body = resp.json()

    assert body["user_id"] == str(t.user_id)
    assert body["org_id"] == str(t.org_id)
    assert body["role"] == "engineer"
    assert [tm["id"] for tm in body["team"]] == [str(team_id)]
    assert body["department"]["id"] == str(dept_id)
    assert [m["id"] for m in body["recent_meetings"]] == [str(meeting_id)]
    assert body["topics"], "engineer must resolve to a non-empty topic list"


async def test_second_call_is_served_from_cache_not_recomputed(migrated_db):
    """Acceptance check 1: a second call within the TTL does not touch Postgres again."""
    t = seed_tenant(tier="member", role="engineer")
    _seed_team_and_department(t.org_id, t.user_id)

    calls = {"n": 0}
    real_compute = context_service._compute

    async def counting_compute(session, org_id, user_id):
        calls["n"] += 1
        return await real_compute(session, org_id, user_id)

    orig = context_service._compute
    context_service._compute = counting_compute
    try:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from libs.shared.tenancy import set_rls

        engine = create_async_engine(APP_URL)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session, session.begin():
                await set_rls(session, t.org_id)
                first = await context_service.get_user_context(session, t.org_id, t.user_id)
            async with sessionmaker() as session, session.begin():
                await set_rls(session, t.org_id)
                second = await context_service.get_user_context(session, t.org_id, t.user_id)
        finally:
            await engine.dispose()
    finally:
        context_service._compute = orig

    assert calls["n"] == 1, "second call must be served from Redis, not recomputed"
    assert first == second


# --- 2: TTL -----------------------------------------------------------------------------


async def test_ttl_on_key_is_at_most_300_seconds(client):
    t = seed_tenant(tier="member")
    sign_in(t)

    resp = client.get(f"/context/{t.user_id}")
    assert resp.status_code == 200

    key = redis_key(t.org_id, "user_ctx", str(t.user_id))
    ttl = await _ttl(key)
    assert 0 < ttl <= 300


# --- 3: PATCH evicts, next read reflects new role + topics -----------------------------


def test_patch_evicts_cache_and_next_read_reflects_new_role_and_topics(client):
    t = seed_tenant(tier="member", role="engineer")
    sign_in(t)

    first = client.get(f"/context/{t.user_id}")
    assert first.status_code == 200
    assert first.json()["role"] == "engineer"
    engineer_topics = first.json()["topics"]

    patch_resp = client.patch(f"/users/{t.user_id}", json={"role": "designer"})
    assert patch_resp.status_code == 200, patch_resp.text

    second = client.get(f"/context/{t.user_id}")
    assert second.status_code == 200
    assert second.json()["role"] == "designer"
    assert second.json()["topics"] != engineer_topics, "eviction must also refresh topics"


# --- 4: cross-org isolation --------------------------------------------------------------


def test_cross_org_jwt_cannot_fetch_another_orgs_user_context(client):
    victim = seed_tenant(tier="member", slug_prefix="usr-ctx-victim")
    caller = seed_tenant(tier="member", slug_prefix="usr-ctx-caller")
    sign_in(caller)

    resp = client.get(f"/context/{victim.user_id}")
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_unknown_user_id_is_404(client):
    t = seed_tenant(tier="member")
    sign_in(t)
    resp = client.get(f"/context/{uuid.uuid4()}")
    assert resp.status_code == 404


# --- tenancy guarantee on the cache key itself --------------------------------------------


def test_redis_key_never_shared_between_two_orgs_for_the_same_user_id_value():
    org_a, org_b, uid = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    key_a = redis_key(org_a, "user_ctx", str(uid))
    key_b = redis_key(org_b, "user_ctx", str(uid))
    assert key_a != key_b
    assert key_a == f"nexus:{org_a}:user_ctx:{uid}"
    assert key_a.startswith(f"nexus:{org_a}:")


def test_redis_key_raises_without_org_id():
    with pytest.raises(ValueError):
        redis_key(None, "user_ctx", "x")


# --- the cache is an optimization, never a dependency -------------------------------------


async def test_full_path_survives_an_unreachable_redis_and_still_returns_correct_data(
    migrated_db, monkeypatch
):
    """The property the brief called out explicitly: a completely unreachable Redis must
    degrade latency, not correctness. Points `get_redis()` (as imported into
    `context.service`) at a closed port with a short timeout and drives the FULL path -
    `get_user_context` against a real Postgres session - through to a correct result rather
    than an exception.
    """
    t = seed_tenant(tier="member", role="engineer")
    dept_id, team_id = _seed_team_and_department(t.org_id, t.user_id)
    meeting_id = _seed_meeting(t.org_id, t.user_id)

    broken = redis_from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.3, socket_timeout=0.3)
    monkeypatch.setattr(context_service, "get_redis", lambda: broken)

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from libs.shared.tenancy import set_rls

    engine = create_async_engine(APP_URL)
    try:
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session, session.begin():
            await set_rls(session, t.org_id)
            ctx = await context_service.get_user_context(session, t.org_id, t.user_id)
    finally:
        await engine.dispose()
        await broken.aclose()

    assert ctx is not None
    assert ctx["role"] == "engineer"
    assert [tm["id"] for tm in ctx["team"]] == [str(team_id)]
    assert ctx["department"]["id"] == str(dept_id)
    assert [m["id"] for m in ctx["recent_meetings"]] == [str(meeting_id)]


def test_full_path_over_http_survives_an_unreachable_redis(client, monkeypatch):
    """Same property, but through the actual REST route (`GET /context/{user_id}`) rather than
    calling the resolver directly - proves the FastAPI/session wiring does not swallow this
    differently than the unit-level test above."""
    t = seed_tenant(tier="member", role="engineer")
    sign_in(t)

    broken = redis_from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.3, socket_timeout=0.3)
    monkeypatch.setattr(context_service, "get_redis", lambda: broken)

    resp = client.get(f"/context/{t.user_id}")
    assert resp.status_code == 200, resp.text
    assert resp.json()["role"] == "engineer"


async def test_invalidate_user_context_survives_an_unreachable_redis(monkeypatch):
    """The write side of the same property: an invalidation DEL must not raise even when
    Redis cannot be reached (see `invalidate_user_context`'s docstring - best-effort by
    design)."""
    broken = redis_from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.3, socket_timeout=0.3)
    monkeypatch.setattr(context_service, "get_redis", lambda: broken)
    try:
        await context_service.invalidate_user_context(uuid.uuid4(), uuid.uuid4())
    finally:
        await broken.aclose()


async def test_a_corrupt_cache_entry_is_treated_as_a_miss_not_an_error(migrated_db):
    """Garbage in Redis (not valid JSON) must not raise - it must be treated exactly like a
    miss and recomputed from Postgres."""
    t = seed_tenant(tier="member", role="engineer")

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from libs.shared.tenancy import set_rls

    key = redis_key(t.org_id, "user_ctx", str(t.user_id))
    r = redis_from_url(_redis_url(), decode_responses=False)
    try:
        await r.set(key, b"not-json-at-all", ex=60)
    finally:
        await r.aclose()

    engine = create_async_engine(APP_URL)
    try:
        sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
        async with sessionmaker() as session, session.begin():
            await set_rls(session, t.org_id)
            ctx = await context_service.get_user_context(session, t.org_id, t.user_id)
    finally:
        await engine.dispose()

    assert ctx is not None
    assert ctx["role"] == "engineer"


# --- 5: gRPC and REST agree ----------------------------------------------------------------


async def test_grpc_and_rest_return_the_same_context(client):
    """Acceptance check 5. Same seeded user, both surfaces, compared on the fields REST's
    `UserContext` and gRPC's `UserContext` both carry (role/skills/team/department/org/user)."""
    import grpc

    import identity_pb2
    import identity_pb2_grpc

    t = seed_tenant(tier="member", role="engineer")
    dept_id, team_id = _seed_team_and_department(t.org_id, t.user_id)
    sign_in(t)

    rest_resp = client.get(f"/context/{t.user_id}")
    assert rest_resp.status_code == 200
    rest_body = rest_resp.json()

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(APP_URL)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    server = grpc.aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        grpc_server_mod.IdentityService(session_factory), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        async with grpc.aio.insecure_channel(f"localhost:{port}") as channel:
            stub = identity_pb2_grpc.IdentityServiceStub(channel)
            grpc_resp = await stub.GetUserContext(
                identity_pb2.GetUserContextRequest(user_id=str(t.user_id)),
                metadata=(("org-id", str(t.org_id)),),
            )
    finally:
        await server.stop(None)
        await engine.dispose()

    assert grpc_resp.user_id == rest_body["user_id"]
    assert grpc_resp.org_id == rest_body["org_id"]
    assert grpc_resp.role == rest_body["role"]
    assert list(grpc_resp.skills) == rest_body["skills"]
    assert list(grpc_resp.team_ids) == [tm["id"] for tm in rest_body["team"]]
    assert grpc_resp.department_id == rest_body["department"]["id"]
    assert list(grpc_resp.team_ids) == [str(team_id)]
    assert grpc_resp.department_id == str(dept_id)
