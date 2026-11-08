"""`POST /orgs/self-serve` against real Postgres, as `mytheus_app`.

The org's own Clerk admin provisions its first tenant. Clerk is faked at `org_for_admin` (the
one place the route asks it anything) and at `sync_internal_org_id_to_clerk` via the unset
`CLERK_SECRET_KEY`; the token is faked at `verify_token`, exactly as the operator-route tests do.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from libs.shared.clerk_admin import ClerkMembership, ClerkOrgForAdmin, ClerkUnavailable
from services.identity_tenant_service.routes import orgs as orgs_routes
from services.identity_tenant_service.routes import users as users_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL

pytestmark = pytest.mark.usefixtures("migrated_db")

PATH = orgs_routes.SELF_SERVE_PATH


class _Principal:
    clerk_org = ""
    clerk_user = ""


PRINCIPAL = _Principal()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


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
            ("audit_logs", "org_id"),
        ]
    }


@pytest.fixture(autouse=True)
def _never_call_real_clerk(monkeypatch) -> None:
    monkeypatch.delenv("CLERK_SECRET_KEY", raising=False)


@pytest.fixture(scope="module")
def client():
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("identity-tenant-service-self-serve-test")
    app.include_router(orgs_routes.router)
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


def _new_principal() -> None:
    PRINCIPAL.clerk_org = f"org_{uuid.uuid4().hex[:20]}"
    PRINCIPAL.clerk_user = f"user_{uuid.uuid4().hex[:20]}"


def _clerk_says(monkeypatch, answer: ClerkOrgForAdmin | None | Exception) -> list[tuple[str, str]]:
    calls: list[tuple[str, str]] = []

    async def fake(clerk_org: str, clerk_user: str) -> ClerkOrgForAdmin | None:
        calls.append((clerk_org, clerk_user))
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(orgs_routes, "org_for_admin", fake)
    return calls


def test_an_org_admin_provisions_the_whole_tenant_and_owns_it(client, monkeypatch) -> None:
    _new_principal()
    calls = _clerk_says(
        monkeypatch,
        ClerkOrgForAdmin(
            name="Yash's Org", slug=f"yash_gooners-{uuid.uuid4().int}", owner_email="y@x.test"
        ),
    )

    r = client.post(PATH)

    assert r.status_code == 201, r.text
    body = r.json()
    assert calls == [(PRINCIPAL.clerk_org, PRINCIPAL.clerk_user)]
    assert body["clerk_org_id"] == PRINCIPAL.clerk_org
    assert body["name"] == "Yash's Org"
    assert "_" not in body["slug"] and len(body["slug"]) <= 63  # Clerk slug normalized
    org_id = uuid.UUID(body["id"])
    assert _counts(org_id) == {
        "organizations": 1,
        "tenant_settings": 1,
        "data_residency": 1,
        "users": 1,
        "rbac_grants": 1,
        "outbox": 1,
        "audit_logs": 1,
    }
    owner = _sql(
        "SELECT u.clerk_user_id, u.email, g.tier FROM users u JOIN rbac_grants g "
        "ON g.user_id = u.id WHERE u.org_id = %(o)s",
        {"o": str(org_id)},
    )
    assert owner == [{"clerk_user_id": PRINCIPAL.clerk_user, "email": "y@x.test", "tier": "owner"}]


def test_a_second_call_returns_the_same_tenant_without_asking_clerk(client, monkeypatch) -> None:
    _new_principal()
    _clerk_says(monkeypatch, ClerkOrgForAdmin(name="Twice", slug="", owner_email="t@x.test"))
    first = client.post(PATH)
    assert first.status_code == 201, first.text

    calls = _clerk_says(monkeypatch, None)  # would 403 if consulted
    second = client.post(PATH)

    assert second.status_code == 200, second.text
    assert second.json()["id"] == first.json()["id"]
    assert calls == []
    assert _counts(uuid.UUID(first.json()["id"]))["organizations"] == 1


def test_a_caller_clerk_does_not_list_as_admin_gets_403_and_nothing_is_written(
    client, monkeypatch
) -> None:
    _new_principal()
    _clerk_says(monkeypatch, None)

    r = client.post(PATH)

    assert r.status_code == 403
    assert r.json()["code"] == "FORBIDDEN_ROLE"
    assert _sql(
        "SELECT count(*) AS n FROM organizations WHERE clerk_org_id = %(c)s",
        {"c": PRINCIPAL.clerk_org},
    ) == [{"n": 0}]


def test_clerk_unavailable_is_503_and_nothing_is_written(client, monkeypatch) -> None:
    _new_principal()
    _clerk_says(monkeypatch, ClerkUnavailable("down"))

    r = client.post(PATH)

    assert r.status_code == 503
    assert _sql(
        "SELECT count(*) AS n FROM organizations WHERE clerk_org_id = %(c)s",
        {"c": PRINCIPAL.clerk_org},
    ) == [{"n": 0}]


@pytest.mark.parametrize(
    ("clerk_slug", "expected"),
    [
        ("shawn-s-team-1794076332404077102", "shawn-s-team-1794076332404077102"),
        ("yash_gooners-1794068405854966776", "yash-gooners-1794068405854966776"),
        ("Acme Inc!", "acme-inc"),
        ("", "org-abc123"),
        ("x" * 80, "x" * 63),
    ],
)
def test_self_serve_slug_fits_the_tenant_slug_rule(clerk_slug: str, expected: str) -> None:
    assert orgs_routes.self_serve_slug(clerk_slug, "org_ABC123") == expected


# --- the join path: an existing workspace, a member who just accepted an invitation ------------


def _existing_org() -> tuple[uuid.UUID, str]:
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:20]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(i)s, 'Team', %(s)s, %(c)s)",
        {"i": str(org_id), "s": f"t-{org_id}", "c": clerk_org},
    )
    return org_id, clerk_org


def _membership_says(monkeypatch, answer: ClerkMembership | None | Exception) -> list[str]:
    calls: list[str] = []

    async def fake(clerk_org: str, clerk_user: str) -> ClerkMembership | None:
        calls.append(clerk_user)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(orgs_routes, "membership_of", fake)
    return calls


def test_a_clerk_member_joining_an_existing_workspace_becomes_a_member(client, monkeypatch) -> None:
    org_id, clerk_org = _existing_org()
    PRINCIPAL.clerk_org, PRINCIPAL.clerk_user = clerk_org, f"user_{uuid.uuid4().hex[:20]}"
    _membership_says(
        monkeypatch, ClerkMembership(role="org:member", email="j@x.test", avatar_url=None)
    )

    r = client.post(PATH)

    assert r.status_code == 200, r.text
    assert r.json()["id"] == str(org_id)
    assert _sql(
        "SELECT u.email, g.tier FROM users u JOIN rbac_grants g ON g.user_id = u.id "
        "WHERE u.org_id = %(o)s AND u.clerk_user_id = %(c)s",
        {"o": str(org_id), "c": PRINCIPAL.clerk_user},
    ) == [{"email": "j@x.test", "tier": "member"}]

    calls = _membership_says(monkeypatch, None)  # would 403 if consulted
    assert client.post(PATH).status_code == 200
    assert calls == [], "a caller already in the tenant costs no Clerk call"


def test_someone_clerk_does_not_list_cannot_join(client, monkeypatch) -> None:
    org_id, clerk_org = _existing_org()
    PRINCIPAL.clerk_org, PRINCIPAL.clerk_user = clerk_org, f"user_{uuid.uuid4().hex[:20]}"
    _membership_says(monkeypatch, None)

    r = client.post(PATH)

    assert r.status_code == 403
    assert _sql("SELECT count(*) AS n FROM users WHERE org_id = %(o)s", {"o": str(org_id)}) == [
        {"n": 0}
    ]


# --- GET /users: the members list --------------------------------------------------------------


def test_members_lists_this_tenant_with_tiers_and_nothing_else(client, monkeypatch) -> None:
    _new_principal()
    _clerk_says(monkeypatch, ClerkOrgForAdmin(name="Mine", slug="", owner_email="me@x.test"))
    mine = uuid.UUID(client.post(PATH).json()["id"])
    _sql(
        "INSERT INTO users (org_id, clerk_user_id, email) "
        "VALUES (%(o)s, 'user_gone', 'gone@x.test')",
        {"o": str(mine)},
    )
    _sql("UPDATE users SET deleted_at = now() WHERE clerk_user_id = 'user_gone'")
    _, other_clerk = _existing_org()
    _sql(
        "INSERT INTO users (org_id, clerk_user_id, email) SELECT id, 'user_other', 'other@x.test' "
        "FROM organizations WHERE clerk_org_id = %(c)s",
        {"c": other_clerk},
    )

    r = client.get("/users")

    assert r.status_code == 200, r.text
    assert [(m["email"], m["tier"], m["clerk_user_id"]) for m in r.json()["items"]] == [
        ("me@x.test", "owner", PRINCIPAL.clerk_user)
    ]
