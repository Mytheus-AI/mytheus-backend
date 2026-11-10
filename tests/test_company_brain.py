"""The company brain: ADR-0039 Part A (A1 to A9) in identity-tenant-service.

- RLS: every new table is isolated per tenant and fails closed with no `app.current_org`.
- CHECK constraints mirror the API's limits in the database.
- Roles: reads open to members; writes and readiness need owner/admin (403 for a member).
- Limits: 422 naming the field, both body limits and per-org row caps.
- Duplicates are 409 (case-insensitive); another tenant's id is 404.
- Glossary import upserts by `lower(term)`, all or nothing.
- Readiness scoring, including the `roles` check; the welcome brief; topic watches self only.
- gRPC `GetOrgContext`: contents, empty for an org with no brain, cached at
  `nexus:{org_id}:org_ctx` (TTL <= 300 s) and evicted by every Part A write.

Same harness as `tests/test_user_context.py`: real Postgres as `mytheus_app`, real Redis,
`verify_token` overridden.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import pytest
from fastapi import FastAPI
from redis.asyncio import from_url as redis_from_url
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service import grpc_server as grpc_server_mod
from services.identity_tenant_service.brain import context as brain_context
from services.identity_tenant_service.brain import routes as brain_routes
from tests.conftest import APP_URL
from tests.test_user_context import PRINCIPAL, _seed_user_row, _sql, seed_tenant, sign_in

ORG = "/api/v1/org"
W = "/api/v1/users/me/topic-watches"


def _redis_url() -> str:
    return os.environ.get("REDIS_URL", "redis://localhost:6390")


@pytest.fixture(autouse=True)
def _fresh_redis_per_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Redis client per call, so the TestClient's loop and the test's own loop never share a
    connection pool (a pool bound to one loop fails on another, and eviction is best-effort,
    which would make a broken eviction look like a passing one)."""
    monkeypatch.setattr(
        brain_context, "get_redis", lambda: redis_from_url(_redis_url(), decode_responses=False)
    )


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app: FastAPI = create_app("identity-brain-test")
    app.include_router(brain_routes.router)
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


def _locs(resp: Any) -> list[list[str]]:
    assert resp.status_code == 422, resp.text
    return [e["loc"] for e in resp.json()["errors"]]


def _admin() -> Any:
    t = seed_tenant(tier="admin", slug_prefix="brain", role="Founder")
    sign_in(t)
    return t


# --- RLS and CHECK constraints -------------------------------------------------------------

_TABLE_SEEDS = {
    "org_profiles": "INSERT INTO org_profiles (org_id, description) VALUES (%(o)s, 'x')",
    "org_glossary_terms": "INSERT INTO org_glossary_terms (org_id, term) VALUES (%(o)s, 'Nexus')",
    "topic_owners": (
        "INSERT INTO topic_owners (org_id, topic, owner_user_id) VALUES (%(o)s, 'billing', %(u)s)"
    ),
    "org_goals": "INSERT INTO org_goals (org_id, title) VALUES (%(o)s, 'Ship it')",
    "topic_watches": (
        "INSERT INTO topic_watches (org_id, user_id, topic) VALUES (%(o)s, %(u)s, 'billing')"
    ),
}


@pytest.mark.parametrize("table", sorted(_TABLE_SEEDS))
async def test_every_brain_table_is_isolated_and_fails_closed(migrated_db, table):
    a, b = seed_tenant(slug_prefix="brain-rls"), seed_tenant(slug_prefix="brain-rls")
    for t in (a, b):
        _sql(_TABLE_SEEDS[table], {"o": str(t.org_id), "u": str(t.user_id)})

    engine = create_async_engine(APP_URL)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(a.org_id)}
            )
            orgs = (await conn.execute(text(f"SELECT DISTINCT org_id FROM {table}"))).scalars()
            assert {str(o) for o in orgs} == {str(a.org_id)}
        async with engine.begin() as conn:
            assert (await conn.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one() == 0
        async with engine.begin() as conn:
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(a.org_id)}
            )
            with pytest.raises(Exception, match="row-level security"):
                await conn.execute(
                    text(
                        _TABLE_SEEDS[table]
                        .replace("%(o)s", ":o")
                        .replace("%(u)s", ":u")
                        .replace("'Nexus'", "'Other'")
                        .replace("'billing'", "'other'")
                    ),
                    {"o": str(b.org_id), "u": str(b.user_id)},
                )
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO org_profiles (org_id, products) "
        "VALUES (%(o)s, array_fill('p'::text, ARRAY[31]))",
        "INSERT INTO org_profiles (org_id, customers) VALUES (%(o)s, ARRAY[repeat('c', 81)])",
        "INSERT INTO org_profiles (org_id, description) VALUES (%(o)s, repeat('d', 2001))",
        "INSERT INTO org_glossary_terms (org_id, term) VALUES (%(o)s, '')",
        "INSERT INTO org_glossary_terms (org_id, term, kind) VALUES (%(o)s, 'x', 'thing')",
        "INSERT INTO org_glossary_terms (org_id, term, aliases) "
        "VALUES (%(o)s, 'x', array_fill('a'::text, ARRAY[11]))",
        "INSERT INTO org_goals (org_id, title, status) VALUES (%(o)s, 'g', 'paused')",
        "INSERT INTO org_goals (org_id, title, period) VALUES (%(o)s, 'g', repeat('p', 21))",
        "INSERT INTO tenant_settings (org_id, never_record_topics) "
        "VALUES (%(o)s, array_fill('t'::text, ARRAY[31]))",
    ],
)
def test_check_constraints_mirror_the_limits(migrated_db, sql):
    t = seed_tenant(slug_prefix="brain-ck")
    with pytest.raises(psycopg2.errors.CheckViolation):
        _sql(sql, {"o": str(t.org_id)})


def test_glossary_is_unique_per_org_case_insensitively(migrated_db):
    a, b = seed_tenant(slug_prefix="brain-ux"), seed_tenant(slug_prefix="brain-ux")
    _sql(_TABLE_SEEDS["org_glossary_terms"], {"o": str(a.org_id)})
    _sql(_TABLE_SEEDS["org_glossary_terms"], {"o": str(b.org_id)})  # another org: fine
    with pytest.raises(psycopg2.errors.UniqueViolation):
        _sql(
            "INSERT INTO org_glossary_terms (org_id, term) VALUES (%(o)s, 'NEXUS')",
            {"o": str(a.org_id)},
        )


# --- roles -----------------------------------------------------------------------------------

_WRITES = [
    ("put", f"{ORG}/profile", {"description": "x"}),
    ("post", f"{ORG}/glossary", {"term": "Nexus"}),
    ("patch", f"{ORG}/glossary/{uuid.uuid4()}", {"kind": "product"}),
    ("delete", f"{ORG}/glossary/{uuid.uuid4()}", None),
    ("post", f"{ORG}/glossary/import", {"terms": [{"term": "Nexus"}]}),
    ("post", f"{ORG}/topic-owners", {"topic": "billing", "owner_user_id": str(uuid.uuid4())}),
    ("patch", f"{ORG}/topic-owners/{uuid.uuid4()}", {"topic": "x"}),
    ("delete", f"{ORG}/topic-owners/{uuid.uuid4()}", None),
    ("post", f"{ORG}/goals", {"title": "g"}),
    ("patch", f"{ORG}/goals/{uuid.uuid4()}", {"title": "g"}),
    ("delete", f"{ORG}/goals/{uuid.uuid4()}", None),
    ("put", f"{ORG}/ground-rules", {"never_record_topics": []}),
    ("get", f"{ORG}/readiness", None),
]


@pytest.mark.parametrize("method,path,body", _WRITES)
def test_member_cannot_write_company_data(client, method, path, body):
    sign_in(seed_tenant(tier="member", slug_prefix="brain-role"))
    kwargs = {"json": body} if body is not None else {}
    resp = client.request(method.upper(), path, **kwargs)
    assert resp.status_code == 403, resp.text


@pytest.mark.parametrize(
    "path",
    [
        f"{ORG}/profile",
        f"{ORG}/glossary",
        f"{ORG}/topic-owners",
        f"{ORG}/goals",
        f"{ORG}/ground-rules",
        f"{ORG}/welcome",
        W,
    ],
)
def test_member_can_read(client, path):
    sign_in(seed_tenant(tier="member", slug_prefix="brain-read"))
    assert client.get(path).status_code == 200


def test_owner_can_write(client):
    sign_in(seed_tenant(tier="owner", slug_prefix="brain-owner"))
    assert client.put(f"{ORG}/profile", json={"industry": "SaaS"}).status_code == 200
    assert client.get(f"{ORG}/readiness").status_code == 200


# --- A1 profile ------------------------------------------------------------------------------


def test_profile_round_trip_and_empty_default(client):
    t = _admin()
    empty = client.get(f"{ORG}/profile").json()
    assert empty == {
        "description": "",
        "industry": "",
        "products": [],
        "customers": [],
        "updated_by": None,
        "updated_at": None,
    }
    resp = client.put(
        f"{ORG}/profile",
        json={
            "description": "  We build meeting software.  ",
            "industry": "SaaS",
            "products": ["Mytheus", " mytheus ", "Atlas"],
            "customers": ["Acme"],
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["description"] == "We build meeting software."
    assert body["products"] == ["Mytheus", "Atlas"]
    assert body["updated_by"] == str(t.user_id) and body["updated_at"]
    assert client.get(f"{ORG}/profile").json() == body
    # PUT replaces: an omitted field is cleared.
    assert client.put(f"{ORG}/profile", json={"industry": "Fintech"}).json()["products"] == []


@pytest.mark.parametrize(
    "body,field",
    [
        ({"description": "x" * 2001}, "description"),
        ({"industry": "x" * 101}, "industry"),
        ({"products": [f"p{i}" for i in range(31)]}, "products"),
        ({"products": ["x" * 81]}, "products"),
        ({"customers": [f"c{i}" for i in range(51)]}, "customers"),
        ({"customers": ["  "]}, "customers"),
    ],
)
def test_profile_limits_are_422_naming_the_field(client, body, field):
    _admin()
    assert any(loc[:2] == ["body", field] for loc in _locs(client.put(f"{ORG}/profile", json=body)))


# --- A2 glossary -----------------------------------------------------------------------------


def test_glossary_crud_conflict_and_404(client):
    _admin()
    resp = client.post(
        f"{ORG}/glossary",
        json={"term": " Nexus ", "kind": "codename", "definition": "", "aliases": ["NX", "nx"]},
    )
    assert resp.status_code == 201, resp.text
    term = resp.json()
    assert (term["term"], term["kind"], term["definition"], term["aliases"]) == (
        "Nexus",
        "codename",
        None,
        ["NX"],
    )
    assert client.post(f"{ORG}/glossary", json={"term": "NEXUS"}).status_code == 409
    client.post(f"{ORG}/glossary", json={"term": "Atlas"})
    assert client.patch(f"{ORG}/glossary/{term['id']}", json={"term": "atlas"}).status_code == 409

    resp = client.patch(f"{ORG}/glossary/{term['id']}", json={"definition": "Internal codename"})
    assert resp.status_code == 200 and resp.json()["definition"] == "Internal codename"
    assert [i["term"] for i in client.get(f"{ORG}/glossary").json()["items"]] == ["Atlas", "Nexus"]

    # Another tenant's id is 404 for read-modify and delete.
    _admin()
    assert client.patch(f"{ORG}/glossary/{term['id']}", json={"kind": "other"}).status_code == 404
    assert client.delete(f"{ORG}/glossary/{term['id']}").status_code == 404


@pytest.mark.parametrize(
    "body,field",
    [
        ({"term": ""}, "term"),
        ({"term": "x" * 81}, "term"),
        ({"term": "x", "kind": "thing"}, "kind"),
        ({"term": "x", "definition": "d" * 301}, "definition"),
        ({"term": "x", "aliases": [f"a{i}" for i in range(11)]}, "aliases"),
    ],
)
def test_glossary_limits_are_422_naming_the_field(client, body, field):
    _admin()
    assert any(
        loc[:2] == ["body", field] for loc in _locs(client.post(f"{ORG}/glossary", json=body))
    )


def test_glossary_caps_at_500_terms(client):
    t = _admin()
    _sql(
        "INSERT INTO org_glossary_terms (org_id, term) "
        "SELECT %(o)s, 'term ' || g FROM generate_series(1, 500) g",
        {"o": str(t.org_id)},
    )
    assert ["body", "term"] in _locs(client.post(f"{ORG}/glossary", json={"term": "one more"}))
    # An import that only updates existing terms is still allowed at the cap.
    resp = client.post(f"{ORG}/glossary/import", json={"terms": [{"term": "TERM 1"}]})
    assert resp.status_code == 200 and resp.json()["updated"] == 1
    resp = client.post(f"{ORG}/glossary/import", json={"terms": [{"term": "brand new"}]})
    assert ["body", "terms"] in _locs(resp)


def test_glossary_import_upserts_by_lower_term(client):
    _admin()
    client.post(f"{ORG}/glossary", json={"term": "Nexus", "kind": "other", "definition": "old"})
    resp = client.post(
        f"{ORG}/glossary/import",
        json={
            "terms": [
                {"term": "NEXUS", "kind": "codename", "definition": "Our architecture"},
                {"term": "Atlas", "kind": "product"},
                {"term": "atlas", "kind": "customer", "aliases": ["ATL"]},
            ]
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["created"], body["updated"]) == (1, 1)
    items = {i["term"]: i for i in body["items"]}
    assert set(items) == {"NEXUS", "atlas"}
    assert items["NEXUS"]["kind"] == "codename"
    assert items["NEXUS"]["definition"] == "Our architecture"
    assert items["atlas"]["kind"] == "customer" and items["atlas"]["aliases"] == ["ATL"]
    assert client.get(f"{ORG}/glossary").json()["items"] == body["items"]


def test_glossary_import_limits(client):
    _admin()
    too_many = {"terms": [{"term": f"t{i}"} for i in range(201)]}
    assert any(
        loc[:2] == ["body", "terms"]
        for loc in _locs(client.post(f"{ORG}/glossary/import", json=too_many))
    )
    assert client.post(f"{ORG}/glossary/import", json={"terms": []}).status_code == 422


# --- A3 topic owners -------------------------------------------------------------------------


def test_topic_owners_crud_and_validation(client):
    t = _admin()
    other = _seed_user_row(t.org_id, "member")
    resp = client.post(
        f"{ORG}/topic-owners", json={"topic": "Billing", "owner_user_id": str(t.user_id)}
    )
    assert resp.status_code == 201, resp.text
    owner = resp.json()
    assert (
        client.post(
            f"{ORG}/topic-owners", json={"topic": "billing", "owner_user_id": str(other.user_id)}
        ).status_code
        == 409
    )
    resp = client.patch(
        f"{ORG}/topic-owners/{owner['id']}", json={"owner_user_id": str(other.user_id)}
    )
    assert resp.status_code == 200 and resp.json()["owner_user_id"] == str(other.user_id)

    stranger = seed_tenant(slug_prefix="brain-stranger")
    resp = client.post(
        f"{ORG}/topic-owners", json={"topic": "Security", "owner_user_id": str(stranger.user_id)}
    )
    assert ["body", "owner_user_id"] in _locs(resp)
    assert client.delete(f"{ORG}/topic-owners/{owner['id']}").status_code == 204
    assert client.delete(f"{ORG}/topic-owners/{owner['id']}").status_code == 404


def test_topic_owners_cap_at_200(client):
    t = _admin()
    _sql(
        "INSERT INTO topic_owners (org_id, topic, owner_user_id) "
        "SELECT %(o)s, 'topic ' || g, %(u)s FROM generate_series(1, 200) g",
        {"o": str(t.org_id), "u": str(t.user_id)},
    )
    resp = client.post(
        f"{ORG}/topic-owners", json={"topic": "one more", "owner_user_id": str(t.user_id)}
    )
    assert ["body", "topic"] in _locs(resp)


# --- A4 goals --------------------------------------------------------------------------------


def test_goals_crud_positions_and_active_cap(client):
    t = _admin()
    first = client.post(f"{ORG}/goals", json={"title": "Grow", "period": "2026-Q4"}).json()
    second = client.post(
        f"{ORG}/goals", json={"title": "Hire", "owner_user_id": str(t.user_id)}
    ).json()
    assert (first["position"], second["position"]) == (0, 1)
    assert first["status"] == "active" and first["period"] == "2026-Q4"

    resp = client.patch(f"{ORG}/goals/{first['id']}", json={"status": "achieved", "period": None})
    assert resp.status_code == 200 and resp.json()["period"] is None

    _sql(
        "INSERT INTO org_goals (org_id, title) "
        "SELECT %(o)s, 'g' || g FROM generate_series(1, 29) g",
        {"o": str(t.org_id)},
    )  # 30 active now
    assert ["body", "status"] in _locs(client.post(f"{ORG}/goals", json={"title": "too many"}))
    resp = client.post(f"{ORG}/goals", json={"title": "parked", "status": "dropped"})
    assert resp.status_code == 201
    assert ["body", "status"] in _locs(
        client.patch(f"{ORG}/goals/{first['id']}", json={"status": "active"})
    )
    assert client.patch(f"{ORG}/goals/{uuid.uuid4()}", json={"status": "active"}).status_code == 404
    assert client.delete(f"{ORG}/goals/{second['id']}").status_code == 204


@pytest.mark.parametrize(
    "body,field",
    [
        ({"title": ""}, "title"),
        ({"title": "x" * 201}, "title"),
        ({"title": "g", "description": "d" * 1001}, "description"),
        ({"title": "g", "period": "p" * 21}, "period"),
        ({"title": "g", "status": "paused"}, "status"),
        ({"title": "g", "position": -1}, "position"),
    ],
)
def test_goal_limits_are_422_naming_the_field(client, body, field):
    _admin()
    assert any(loc[:2] == ["body", field] for loc in _locs(client.post(f"{ORG}/goals", json=body)))


# --- A5 ground rules -------------------------------------------------------------------------


def test_ground_rules_round_trip_and_limits(client):
    _admin()
    assert client.get(f"{ORG}/ground-rules").json() == {"never_record_topics": []}
    resp = client.put(
        f"{ORG}/ground-rules", json={"never_record_topics": ["Salaries", "salaries", " M&A "]}
    )
    assert resp.status_code == 200 and resp.json() == {"never_record_topics": ["Salaries", "M&A"]}
    assert client.get(f"{ORG}/ground-rules").json() == resp.json()
    for body in (
        {"never_record_topics": [f"t{i}" for i in range(31)]},
        {"never_record_topics": ["x" * 81]},
    ):
        assert ["body", "never_record_topics"] in [
            loc[:2] for loc in _locs(client.put(f"{ORG}/ground-rules", json=body))
        ]


# --- A6 topic watches ------------------------------------------------------------------------


def test_topic_watches_are_self_only(client):
    a = seed_tenant(tier="member", slug_prefix="brain-watch")
    b = _seed_user_row(a.org_id, "member")
    b.clerk_org = a.clerk_org

    sign_in(a)
    resp = client.post(W, json={"topic": " Pricing "})
    assert resp.status_code == 201, resp.text
    watch = resp.json()
    assert watch["topic"] == "Pricing" and watch["user_id"] == str(a.user_id)
    assert client.post(W, json={"topic": "pricing"}).status_code == 409

    sign_in(b)
    assert client.get(W).json() == {"items": []}
    assert client.delete(f"{W}/{watch['id']}").status_code == 404
    assert client.post(W, json={"topic": "pricing"}).status_code == 201  # per user, not per org

    sign_in(a)
    assert [w["id"] for w in client.get(W).json()["items"]] == [watch["id"]]
    assert client.delete(f"{W}/{watch['id']}").status_code == 204


def test_topic_watches_cap_at_20_per_person(client):
    t = seed_tenant(tier="member", slug_prefix="brain-watch-cap")
    sign_in(t)
    for i in range(20):
        assert client.post(W, json={"topic": f"topic {i}"}).status_code == 201
    assert ["body", "topic"] in _locs(client.post(W, json={"topic": "one more"}))
    assert any(
        loc[:2] == ["body", "topic"] for loc in _locs(client.post(W, json={"topic": "x" * 81}))
    )


# --- A7 readiness ----------------------------------------------------------------------------


def test_readiness_scores_each_check_equally(client):
    t = _admin()
    no_role = _seed_user_row(t.org_id, "member")  # role NULL
    body = client.get(f"{ORG}/readiness").json()
    checks = {c["key"]: c for c in body["checks"]}
    assert list(checks) == ["profile", "vocabulary", "owners", "goals", "ground_rules", "roles"]
    assert body["score"] == 0 and not any(c["done"] for c in checks.values())
    assert checks["roles"]["detail"] == "1 member has no role"

    client.put(f"{ORG}/profile", json={"description": "x" * 40})
    client.post(f"{ORG}/glossary/import", json={"terms": [{"term": f"t{i}"} for i in range(10)]})
    for i in range(5):
        client.post(
            f"{ORG}/topic-owners", json={"topic": f"topic {i}", "owner_user_id": str(t.user_id)}
        )
    client.post(f"{ORG}/goals", json={"title": "Grow"})
    body = client.get(f"{ORG}/readiness").json()
    assert body["score"] == round(100 * 4 / 6)

    # A7 as amended: an empty list is indistinguishable from never set, so it does not count.
    client.put(f"{ORG}/ground-rules", json={"never_record_topics": []})
    assert client.get(f"{ORG}/readiness").json()["score"] == round(100 * 4 / 6)
    client.put(f"{ORG}/ground-rules", json={"never_record_topics": ["Salaries"]})
    body = client.get(f"{ORG}/readiness").json()
    assert body["score"] == round(100 * 5 / 6)
    assert {c["key"]: c["detail"] for c in body["checks"]}["ground_rules"] == (
        "1 never-record topic"
    )

    _sql("UPDATE users SET role = 'Engineer' WHERE id = %(u)s", {"u": str(no_role.user_id)})
    body = client.get(f"{ORG}/readiness").json()
    assert body["score"] == 100
    assert {c["key"]: c["detail"] for c in body["checks"]}["roles"] == "Every member has a role"


# --- A8 welcome ------------------------------------------------------------------------------


def test_welcome_brief(client):
    t = _admin()
    mate = _seed_user_row(t.org_id, "member")
    outsider = _seed_user_row(t.org_id, "member")
    team, other_team = uuid.uuid4(), uuid.uuid4()
    for tid in (team, other_team):
        _sql(
            "INSERT INTO teams (id, org_id, name) VALUES (%(i)s, %(o)s, 'T')",
            {"i": str(tid), "o": str(t.org_id)},
        )
    for tid, uid in ((team, t.user_id), (team, mate.user_id), (other_team, outsider.user_id)):
        _sql(
            "INSERT INTO team_members (team_id, user_id, org_id) VALUES (%(t)s, %(u)s, %(o)s)",
            {"t": str(tid), "u": str(uid), "o": str(t.org_id)},
        )
    client.put(f"{ORG}/profile", json={"description": "About us"})
    client.post(
        f"{ORG}/glossary/import", json={"terms": [{"term": f"t{i:02d}"} for i in range(45)]}
    )
    client.post(
        f"{ORG}/topic-owners", json={"topic": "Billing", "owner_user_id": str(mate.user_id)}
    )
    client.post(f"{ORG}/goals", json={"title": "Active"})
    client.post(f"{ORG}/goals", json={"title": "Old", "status": "achieved"})

    mate.clerk_org = t.clerk_org
    sign_in(mate)
    body = client.get(f"{ORG}/welcome").json()
    assert body["profile"]["description"] == "About us"
    assert [g["title"] for g in body["goals"]] == ["Active"]
    assert body["owners"] == [{"topic": "Billing", "owner_user_id": str(mate.user_id)}]
    assert [g["term"] for g in body["glossary"]] == [f"t{i:02d}" for i in range(40)]
    assert body["team"] == [{"user_id": str(t.user_id)}]


# --- A9 GetOrgContext ------------------------------------------------------------------------


async def _get_org_context(org_id: Any, metadata: Any = None) -> Any:
    import identity_pb2
    import identity_pb2_grpc

    engine = create_async_engine(APP_URL)
    server = __import__("grpc").aio.server()
    identity_pb2_grpc.add_IdentityServiceServicer_to_server(
        grpc_server_mod.IdentityService(async_sessionmaker(engine, expire_on_commit=False)), server
    )
    port = server.add_insecure_port("[::]:0")
    await server.start()
    try:
        async with __import__("grpc").aio.insecure_channel(f"localhost:{port}") as channel:
            return await identity_pb2_grpc.IdentityServiceStub(channel).GetOrgContext(
                identity_pb2.GetOrgContextRequest(),
                metadata=metadata if metadata is not None else (("org-id", str(org_id)),),
            )
    finally:
        await server.stop(None)
        await engine.dispose()


async def _redis_get(key: str) -> tuple[bytes | None, int]:
    r = redis_from_url(_redis_url(), decode_responses=False)
    try:
        return await r.get(key), await r.ttl(key)
    finally:
        await r.aclose()


async def test_get_org_context_is_empty_for_an_org_with_no_brain(client):
    t = seed_tenant(slug_prefix="brain-grpc-empty")
    resp = await _get_org_context(t.org_id)
    assert resp.description == "" and resp.industry == ""
    assert not resp.products and not resp.customers and not resp.glossary
    assert not resp.topic_owners and not resp.goals and not resp.never_record_topics
    assert not resp.topic_watches


async def test_get_org_context_requires_org_metadata(client):
    import grpc

    with pytest.raises(grpc.aio.AioRpcError) as err:
        await _get_org_context(None, metadata=())
    assert err.value.code() == grpc.StatusCode.UNAUTHENTICATED
    with pytest.raises(grpc.aio.AioRpcError) as err:
        await _get_org_context(None, metadata=(("org-id", "not-a-uuid"),))
    assert err.value.code() == grpc.StatusCode.UNAUTHENTICATED


async def test_get_org_context_contents_cache_and_eviction(client):
    t = _admin()
    client.put(
        f"{ORG}/profile",
        json={
            "description": "D",
            "industry": "SaaS",
            "products": ["Mytheus"],
            "customers": ["Acme"],
        },
    )
    client.post(f"{ORG}/glossary", json={"term": "Nexus", "kind": "codename", "aliases": ["NX"]})
    client.post(f"{ORG}/topic-owners", json={"topic": "Billing", "owner_user_id": str(t.user_id)})
    goal = client.post(f"{ORG}/goals", json={"title": "Grow", "period": "2026-Q4"}).json()
    client.post(f"{ORG}/goals", json={"title": "Done", "status": "achieved"})
    client.put(f"{ORG}/ground-rules", json={"never_record_topics": ["Salaries"]})
    client.post(W, json={"topic": "Pricing"})

    # Another tenant's brain never leaks in.
    other = _admin()
    client.post(f"{ORG}/glossary", json={"term": "Secret"})
    sign_in(t)

    key = f"nexus:{t.org_id}:org_ctx"
    assert (await _redis_get(key))[0] is None

    resp = await _get_org_context(t.org_id)
    assert (resp.description, resp.industry) == ("D", "SaaS")
    assert list(resp.products) == ["Mytheus"] and list(resp.customers) == ["Acme"]
    assert [(g.term, g.kind, g.definition, list(g.aliases)) for g in resp.glossary] == [
        ("Nexus", "codename", "", ["NX"])
    ]
    assert [(o.topic, o.owner_user_id) for o in resp.topic_owners] == [("Billing", str(t.user_id))]
    assert [(g.id, g.title, g.period) for g in resp.goals] == [(goal["id"], "Grow", "2026-Q4")]
    assert list(resp.never_record_topics) == ["Salaries"]
    assert [(w.user_id, w.topic) for w in resp.topic_watches] == [(str(t.user_id), "Pricing")]

    cached, ttl = await _redis_get(key)
    assert cached is not None and 0 < ttl <= 300
    assert (await _redis_get(f"nexus:{other.org_id}:org_ctx"))[0] is None

    # Every Part A write evicts.
    writes = [
        lambda: client.put(f"{ORG}/profile", json={"description": "D2"}),
        lambda: client.post(f"{ORG}/glossary", json={"term": "Atlas"}),
        lambda: client.post(f"{ORG}/glossary/import", json={"terms": [{"term": "Orion"}]}),
        lambda: client.post(
            f"{ORG}/topic-owners", json={"topic": "Sales", "owner_user_id": str(t.user_id)}
        ),
        lambda: client.patch(f"{ORG}/goals/{goal['id']}", json={"title": "Grow more"}),
        lambda: client.put(
            f"{ORG}/ground-rules", json={"never_record_topics": ["Salaries", "M&A"]}
        ),
        lambda: client.post(W, json={"topic": "Churn"}),
    ]
    for write in writes:
        await _get_org_context(t.org_id)
        assert (await _redis_get(key))[0] is not None
        resp = write()
        assert resp.status_code in (200, 201), resp.text
        assert (await _redis_get(key))[0] is None, resp.request.url

    resp = await _get_org_context(t.org_id)
    assert resp.description == "D2"
    assert [g.term for g in resp.glossary] == ["Atlas", "Nexus", "Orion"]
    assert [g.title for g in resp.goals] == ["Grow more"]
    assert list(resp.never_record_topics) == ["Salaries", "M&A"]
    assert sorted(w.topic for w in resp.topic_watches) == ["Churn", "Pricing"]


async def test_get_org_context_survives_an_unreachable_redis(client, monkeypatch):
    t = _admin()
    client.put(f"{ORG}/profile", json={"industry": "SaaS"})
    monkeypatch.setattr(
        brain_context,
        "get_redis",
        lambda: redis_from_url("redis://localhost:1", socket_connect_timeout=0.2),
    )
    resp = await _get_org_context(t.org_id)
    assert resp.industry == "SaaS"


def test_proto_field_numbers_are_fixed():
    import identity_pb2

    def numbers(msg: Any) -> dict[str, int]:
        return {f.name: f.number for f in msg.DESCRIPTOR.fields}

    assert numbers(identity_pb2.GetOrgContextRequest) == {}
    assert numbers(identity_pb2.OrgContext) == {
        "description": 1,
        "industry": 2,
        "products": 3,
        "customers": 4,
        "glossary": 5,
        "topic_owners": 6,
        "goals": 7,
        "never_record_topics": 8,
        "topic_watches": 9,
    }
    assert numbers(identity_pb2.GlossaryTerm) == {
        "term": 1,
        "kind": 2,
        "definition": 3,
        "aliases": 4,
    }
    assert numbers(identity_pb2.TopicOwner) == {"topic": 1, "owner_user_id": 2}
    assert numbers(identity_pb2.Goal) == {"id": 1, "title": 2, "period": 3}
    assert numbers(identity_pb2.TopicWatch) == {"user_id": 1, "topic": 2}
