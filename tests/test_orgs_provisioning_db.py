"""`POST /orgs` and `/rbac/grants` against real Postgres: atomicity, RLS, and the real gate chain.

Plan 1-2 section 3.4 acceptance check: "`POST /api/v1/orgs` creates `organizations` +
`tenant_settings` + `data_residency` + an `outbox` `tenant-events` row atomically (rollback
test: force a failure, assert no partial rows)."

Two layers, because the request-path role cannot write the tenant root today:

- `provision_tenant` is driven directly as `mytheus_admin` (the operator seam, which holds DML
  on `organizations`), proving the six-table birth commits and rolls back as one unit.
- The HTTP route runs as `mytheus_app`. Migration `1afe4928b214` (ADR-0025) grants it a
  column-level INSERT on `organizations`, so `POST /orgs` answers `201` and creates the whole
  tenant in one commit.

Real Clerk is never called: `CLERK_SECRET_KEY` is unset in tests, so
`sync_internal_org_id_to_clerk` logs and returns False (see `libs/shared/clerk_admin.py`).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.provisioning import (
    TenantConflict,
    TenantOwner,
    provision_tenant,
)
from services.identity_tenant_service.routes import orgs as orgs_routes
from services.identity_tenant_service.routes import rbac as rbac_routes
from tests.conftest import ADMIN_URL, APP_URL, BOOTSTRAP_URL

pytestmark = pytest.mark.usefixtures("migrated_db")


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _admin_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(create_async_engine(ADMIN_URL), expire_on_commit=False)


def _counts(org_id: uuid.UUID) -> dict[str, int]:
    o = {"o": str(org_id)}
    return {
        t: _sql(f"SELECT count(*) AS n FROM {t} WHERE {col} = %(o)s", o)[0]["n"]
        for t, col in [
            ("organizations", "id"),
            ("tenant_settings", "org_id"),
            ("data_residency", "org_id"),
            ("users", "org_id"),
            ("rbac_grants", "org_id"),
            ("outbox", "org_id"),
        ]
    }


def _owner() -> TenantOwner:
    return TenantOwner(clerk_user_id=f"user_{uuid.uuid4().hex[:12]}", email="owner@acme.test")


async def test_provision_tenant_writes_the_whole_tenant_in_one_commit() -> None:
    org_id = uuid.uuid4()
    async with _admin_sessionmaker()() as session, session.begin():
        result = await provision_tenant(
            session,
            org_id=org_id,
            name="Acme",
            slug=f"p-{org_id.hex[:10]}",
            clerk_org_id=f"org_{org_id.hex[:16]}",
            owner=_owner(),
        )

    assert _counts(org_id) == {
        "organizations": 1,
        "tenant_settings": 1,
        "data_residency": 1,
        "users": 1,
        "rbac_grants": 1,
        "outbox": 1,
    }
    grant = _sql("SELECT user_id, tier FROM rbac_grants WHERE org_id = %(o)s", {"o": str(org_id)})
    assert [{"user_id": str(g["user_id"]), "tier": g["tier"]} for g in grant] == [
        {"user_id": str(result.owner_user_id), "tier": "owner"}
    ]
    residency = _sql("SELECT region FROM data_residency WHERE org_id = %(o)s", {"o": str(org_id)})
    assert residency == [{"region": "us"}]


async def test_a_failure_after_provisioning_leaves_no_partial_rows() -> None:
    org_id = uuid.uuid4()

    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with _admin_sessionmaker()() as session, session.begin():
            await provision_tenant(
                session,
                org_id=org_id,
                name="Acme",
                slug=f"r-{org_id.hex[:10]}",
                clerk_org_id=f"org_{org_id.hex[:16]}",
                owner=_owner(),
            )
            raise Boom

    assert set(_counts(org_id).values()) == {0}


async def test_a_taken_slug_or_clerk_org_is_a_conflict_and_never_relinks() -> None:
    org_id = uuid.uuid4()
    slug, clerk = f"c-{org_id.hex[:10]}", f"org_{org_id.hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug) VALUES (%(id)s, 'Existing', %(s)s)",
        {"id": str(org_id), "s": slug},
    )
    async with _admin_sessionmaker()() as session:
        with pytest.raises(TenantConflict):
            async with session.begin():
                await provision_tenant(
                    session, name="X", slug=slug, clerk_org_id=clerk, owner=_owner()
                )
    # The existing row was NOT given the caller's Clerk org (link_existing=False).
    row = _sql("SELECT clerk_org_id FROM organizations WHERE id = %(i)s", {"i": str(org_id)})
    assert row == [{"clerk_org_id": None}]


# --- the HTTP surface, as `mytheus_app` -------------------------------------------------------


class _Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = _Principal()


@pytest.fixture(autouse=True)
def _never_call_real_clerk(monkeypatch) -> None:
    # A developer's .env may hold a real CLERK_SECRET_KEY; provisioning would then hit Clerk.
    monkeypatch.delenv("CLERK_SECRET_KEY", raising=False)


@pytest.fixture(scope="module")
def client():
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("identity-tenant-service-test")
    app.include_router(orgs_routes.router)
    app.include_router(rbac_routes.router)
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


def _seed(tier: str | None) -> dict[str, Any]:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org, clerk_user = f"org_{org_id.hex[:16]}", f"user_{user_id.hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) VALUES (%(i)s,'O',%(s)s,%(c)s)",
        {"i": str(org_id), "s": f"h-{org_id}", "c": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s,%(o)s,%(c)s,'a@b.c')",
        {"u": str(user_id), "o": str(org_id), "c": clerk_user},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s,%(u)s,%(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return {"org_id": org_id, "user_id": user_id, "clerk_org": clerk_org, "clerk_user": clerk_user}


def _add_user(org_id: uuid.UUID, tier: str | None) -> uuid.UUID:
    uid = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) VALUES (%(u)s,%(o)s,%(c)s,'x@y.z')",
        {"u": str(uid), "o": str(org_id), "c": f"user_{uid.hex[:16]}"},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s,%(u)s,%(t)s)",
            {"o": str(org_id), "u": str(uid), "t": tier},
        )
    return uid


def _sign_in(t: dict[str, Any]) -> None:
    PRINCIPAL.clerk_org, PRINCIPAL.clerk_user = t["clerk_org"], t["clerk_user"]


def test_post_orgs_as_mytheus_app_creates_the_tenant(client, monkeypatch) -> None:
    operator = _seed("owner")
    monkeypatch.setenv(orgs_routes.OPERATOR_ORGS_ENV, str(operator["org_id"]))
    _sign_in(operator)
    slug = f"n-{uuid.uuid4().hex[:10]}"
    before = _sql(
        "SELECT count(*) AS n FROM audit_logs WHERE org_id = %(o)s", {"o": str(operator["org_id"])}
    )[0]["n"]

    r = client.post(
        "/orgs",
        json={
            "name": "New",
            "slug": slug,
            "clerk_org_id": f"org_{uuid.uuid4().hex[:16]}",
            "owner": {"clerk_user_id": f"user_{uuid.uuid4().hex[:12]}", "email": "o@n.test"},
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["slug"] == slug
    created = uuid.UUID(body["id"])
    assert _counts(created) == {
        "organizations": 1,
        "tenant_settings": 1,
        "data_residency": 1,
        "users": 1,
        "rbac_grants": 1,
        "outbox": 1,
    }
    after = _sql(
        "SELECT count(*) AS n FROM audit_logs WHERE org_id = %(o)s", {"o": str(operator["org_id"])}
    )[0]["n"]
    assert after == before + 1  # the operator-side org.provisioned audit row committed with it


def test_get_orgs_returns_the_callers_tenant(client) -> None:
    t = _seed("member")
    _sign_in(t)
    r = client.get("/orgs")
    assert r.status_code == 200, r.text
    assert r.json()["id"] == str(t["org_id"])


def test_owner_promotes_member_and_it_is_audited(client) -> None:
    t = _seed("owner")
    target = _add_user(t["org_id"], "member")
    _sign_in(t)
    r = client.put(f"/rbac/grants/{target}", json={"tier": "admin"})
    assert r.status_code == 200, r.text
    assert r.json()["tier"] == "admin"
    rows = _sql(
        "SELECT action, metadata FROM audit_logs WHERE org_id = %(o)s AND target_id = %(t)s",
        {"o": str(t["org_id"]), "t": str(target)},
    )
    assert [r["action"] for r in rows] == ["rbac.granted"]


def test_admin_cannot_mint_an_owner_and_the_denial_writes_nothing(client) -> None:
    t = _seed("admin")
    target = _add_user(t["org_id"], "member")
    _sign_in(t)
    assert client.put(f"/rbac/grants/{target}", json={"tier": "owner"}).status_code == 403
    tier = _sql("SELECT tier FROM rbac_grants WHERE user_id = %(u)s", {"u": str(target)})
    assert tier == [{"tier": "member"}]


def test_a_user_in_another_tenant_is_404(client) -> None:
    mine, theirs = _seed("owner"), _seed("member")
    _sign_in(mine)
    r = client.put(f"/rbac/grants/{theirs['user_id']}", json={"tier": "admin"})
    assert r.status_code == 404
    assert client.delete(f"/rbac/grants/{theirs['user_id']}").status_code == 404


def test_the_last_owner_cannot_be_revoked_but_one_of_two_can(client) -> None:
    t = _seed("owner")
    other_owner = _add_user(t["org_id"], "owner")
    _sign_in(t)
    assert client.delete(f"/rbac/grants/{other_owner}").status_code == 204
    # Now `t` is the only owner; another owner would be needed to remove them, and self-change
    # is refused regardless.
    assert client.delete(f"/rbac/grants/{t['user_id']}").status_code == 403


def test_list_grants_shows_only_this_tenant(client) -> None:
    t, other = _seed("admin"), _seed("owner")
    _sign_in(t)
    r = client.get("/rbac/grants")
    assert r.status_code == 200
    ids = {g["user_id"] for g in r.json()["items"]}
    assert str(t["user_id"]) in ids and str(other["user_id"]) not in ids
