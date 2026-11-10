"""Plan 5-6 Task 1 (3.1): the org-model reconciliation.

`teams`, `team_members`, `departments` and the `users` role/skills/bio/avatar_url columns were
NOT new here - they already exist from Plan 1-2 (migrations `82d44163c933` and
`b7d9f3a55c21`), with RLS FORCE and `tenant_isolation` already live. This file proves that
directly against the database (the same principle `test_consent_columns_migration.py` and
`test_rls.py` apply) rather than assuming it from migration source, and then covers the two
genuine gaps this task closes:

1. The two lookup indexes the plan's outline names that neither prior migration added
   (`departments.parent_id`, `team_members.user_id`) - added in `56f72ac73fa9`.
2. `PATCH /users/{user_id}` (role/skills/bio) - identity-tenant-service owns `users` and had no
   route that could write these columns; the Clerk webhook only ever sets `email`/`avatar_url`.

This file does NOT test `GET /orgs/{org_id}/departments` / `/teams` / `/teams/{team_id}/members`
from the plan's step 3: those routes were deliberately not built here. See the Task 1 report and
`services/identity_tenant_service/routes/users.py`'s module docstring for the ownership conflict
between `_canon/01-service-catalog.md` (core-api owns `/teams`, `/departments`) and
`_canon/02-data-model.md` section 2.1 (lists `teams`/`team_members`/`departments` as owned by
"identity-service") that makes building them here premature.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.routes import users as users_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL, seed_org

# --- schema-level: prove what already existed, and what this task added --------------------


async def test_org_hierarchy_tables_force_rls_with_the_canonical_policy(migrated_db):
    """Acceptance check 2: `relrowsecurity, relforcerowsecurity` is `t,t` for all three - this
    was already true before this task; nothing here recreates it."""
    su = create_async_engine(migrated_db)
    try:
        async with su.begin() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class "
                        "WHERE relname IN ('teams', 'team_members', 'departments') "
                        "ORDER BY relname"
                    )
                )
            ).all()
        assert [(r.relname, r.relrowsecurity, r.relforcerowsecurity) for r in rows] == [
            ("departments", True, True),
            ("team_members", True, True),
            ("teams", True, True),
        ]
    finally:
        await su.dispose()


async def test_org_hierarchy_tables_cannot_read_across_tenants(migrated_db):
    """Acceptance check 3, for all three tables, via `SET LOCAL app.current_org` directly (not
    through an HTTP route): tenant B sees zero of tenant A's rows."""
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    dept_id, team_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    try:
        await seed_org(su, org_a, slug_prefix="orgmodel-a")
        await seed_org(su, org_b, slug_prefix="orgmodel-b")
        async with su.begin() as conn:
            await conn.execute(
                text("INSERT INTO departments (id, org_id, name) VALUES (:i, :o, 'Eng')"),
                {"i": str(dept_id), "o": str(org_a)},
            )
            await conn.execute(
                text(
                    "INSERT INTO teams (id, org_id, name, department_id) "
                    "VALUES (:i, :o, 'Platform', :d)"
                ),
                {"i": str(team_id), "o": str(org_a), "d": str(dept_id)},
            )
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email) "
                    "VALUES (:i, :o, :c, 'x@example.com')"
                ),
                {"i": str(user_id), "o": str(org_a), "c": f"cu-{user_id}"},
            )
            await conn.execute(
                text("INSERT INTO team_members (team_id, user_id, org_id) VALUES (:t, :u, :o)"),
                {"t": str(team_id), "u": str(user_id), "o": str(org_a)},
            )

        app = create_async_engine(APP_URL)
        try:
            async with app.connect() as conn, conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
                )
                assert (
                    await conn.execute(text("SELECT count(*) FROM departments"))
                ).scalar_one() >= 1
                assert (await conn.execute(text("SELECT count(*) FROM teams"))).scalar_one() >= 1
                assert (
                    await conn.execute(text("SELECT count(*) FROM team_members"))
                ).scalar_one() >= 1

            async with app.connect() as conn, conn.begin():
                await conn.execute(
                    text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
                )
                assert (
                    await conn.execute(text("SELECT count(*) FROM departments"))
                ).scalar_one() == 0
                assert (await conn.execute(text("SELECT count(*) FROM teams"))).scalar_one() == 0
                assert (
                    await conn.execute(text("SELECT count(*) FROM team_members"))
                ).scalar_one() == 0
        finally:
            await app.dispose()
    finally:
        await su.dispose()


async def test_users_role_skills_bio_avatar_url_columns_already_exist(migrated_db):
    """These were plan-0/1-2 P2 stubs, not something this task adds. Proven by round-tripping
    all four through a real INSERT/SELECT rather than trusting the model source."""
    su = create_async_engine(migrated_db)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    try:
        await seed_org(su, org_id, slug_prefix="orgmodel-users")
        async with su.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email, role, skills, bio, "
                    "avatar_url) VALUES (:i, :o, :c, 'x@example.com', 'engineer', "
                    "ARRAY['rust','postgres'], 'hi', 'https://img/x')"
                ),
                {"i": str(user_id), "o": str(org_id), "c": f"cu-{user_id}"},
            )
            row = (
                await conn.execute(
                    text("SELECT role, skills, bio, avatar_url FROM users WHERE id = :i"),
                    {"i": str(user_id)},
                )
            ).one()
        assert row.role == "engineer"
        assert list(row.skills) == ["rust", "postgres"]
        assert row.bio == "hi"
        assert row.avatar_url == "https://img/x"
    finally:
        await su.dispose()


async def test_new_lookup_indexes_exist(migrated_db):
    """Task 1's genuine schema addition: the two indexes the plan's outline named that neither
    `82d44163c933` nor `b7d9f3a55c21` created."""
    su = create_async_engine(migrated_db)
    try:
        async with su.begin() as conn:
            names = (
                (
                    await conn.execute(
                        text(
                            "SELECT indexname FROM pg_indexes WHERE tablename IN "
                            "('departments', 'team_members') "
                            "AND indexname IN "
                            "('ix_departments_parent_id', 'ix_team_members_user_id')"
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert set(names) == {"ix_departments_parent_id", "ix_team_members_user_id"}
    finally:
        await su.dispose()


# --- PATCH /users/{user_id} -----------------------------------------------------------------


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app: FastAPI = create_app("identity-test")
    app.include_router(users_routes.router)
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


def _seed_user_row(org_id: uuid.UUID, tier: str) -> Tenant:
    user_id = uuid.uuid4()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return Tenant(org_id, "", user_id, clerk_user, tier)


def seed_tenant(tier: str = "member", slug_prefix: str = "user-profile") -> Tenant:
    org_id = uuid.uuid4()
    clerk_org = _seed_org_row(org_id, slug_prefix)
    t = _seed_user_row(org_id, tier)
    t.clerk_org = clerk_org
    return t


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user


@pytest.fixture
def member():
    t = seed_tenant(tier="member")
    sign_in(t)
    return t


def test_patch_updates_skills_and_round_trips_the_array(client, member):
    """Acceptance check 4."""
    resp = client.patch(f"/users/{member.user_id}", json={"skills": ["python", "postgres", "rust"]})
    assert resp.status_code == 200, resp.text
    assert resp.json()["skills"] == ["python", "postgres", "rust"]

    rows = _sql("SELECT skills FROM users WHERE id = %(i)s", {"i": str(member.user_id)})
    assert list(rows[0]["skills"]) == ["python", "postgres", "rust"]


def test_patch_updates_role_and_bio_and_returns_the_canonical_resource(client, member):
    resp = client.patch(
        f"/users/{member.user_id}", json={"role": "staff engineer", "bio": "builds things"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["role"] == "staff engineer"
    assert body["bio"] == "builds things"
    assert body["id"] == str(member.user_id)
    # Not a partial echo: avatar_url/email/created_at/updated_at all come back too, and since
    # ADR-0038 the working profile and the agent persona.
    assert {
        "id",
        "email",
        "role",
        "skills",
        "bio",
        "avatar_url",
        "listen_for",
        "ignore_topics",
        "agent_name",
        "agent_avatar_seed",
        "agent_personality",
        "agent_instructions",
        "created_at",
        "updated_at",
    } == set(body)


def test_avatar_url_is_not_a_writable_field(client, member):
    """Clerk-managed via the webhook; a second write path here would race it."""
    resp = client.patch(f"/users/{member.user_id}", json={"avatar_url": "https://evil/x"})
    assert resp.status_code == 422


def test_empty_body_is_rejected(client, member):
    resp = client.patch(f"/users/{member.user_id}", json={})
    assert resp.status_code == 422


def test_a_member_may_edit_their_own_profile(client, member):
    resp = client.patch(f"/users/{member.user_id}", json={"bio": "self-service"})
    assert resp.status_code == 200


def test_a_member_may_not_edit_someone_elses_profile(client, member):
    other = _seed_user_row(member.org_id, tier="member")
    resp = client.patch(f"/users/{other.user_id}", json={"bio": "hijacked"})
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN_ROLE"
    assert (
        _sql("SELECT bio FROM users WHERE id = %(i)s", {"i": str(other.user_id)})[0]["bio"] is None
    )


def test_an_admin_may_edit_another_users_profile_in_the_same_tenant(client):
    admin = seed_tenant(tier="admin", slug_prefix="user-profile-admin")
    other = _seed_user_row(admin.org_id, tier="member")
    sign_in(admin)

    resp = client.patch(f"/users/{other.user_id}", json={"role": "assigned by admin"})
    assert resp.status_code == 200
    assert resp.json()["role"] == "assigned by admin"


def test_cross_tenant_patch_is_404_not_403(client, member):
    """Existence is tenant-confidential, same as every other resource route."""
    other_tenant = seed_tenant(tier="admin", slug_prefix="user-profile-cross")
    sign_in(member)

    resp = client.patch(f"/users/{other_tenant.user_id}", json={"bio": "cross-tenant"})
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_unknown_field_is_rejected(client, member):
    resp = client.patch(f"/users/{member.user_id}", json={"role": "x", "roll": "typo"})
    assert resp.status_code == 422


def test_patch_writes_an_audit_row(client, member):
    resp = client.patch(f"/users/{member.user_id}", json={"skills": ["go"]})
    assert resp.status_code == 200

    rows = _sql(
        "SELECT action, actor_id, metadata FROM audit_logs "
        "WHERE org_id = %(o)s AND action = 'user.profile_updated' AND target_id = %(t)s",
        {"o": str(member.org_id), "t": str(member.user_id)},
    )
    assert len(rows) == 1
    assert str(rows[0]["actor_id"]) == str(member.user_id)
    assert rows[0]["metadata"]["fields"] == ["skills"]
