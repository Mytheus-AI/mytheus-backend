"""Plan 5-6 Task 2 (3.2): the role -> topics taxonomy, version controlled.

Covers the plan's three acceptance checks:

1. `GET /taxonomy/role-topics?version=v1` returns the full map; an unknown version 404s
   (`test_get_role_topics_returns_the_full_map_for_v1`,
   `test_unknown_version_404s_no_silent_fallback`).
2. `topics_for("engineer")` is deterministic and matches the YAML
   (`test_topics_for_engineer_is_deterministic_and_matches_the_yaml`,
   `test_topics_for_is_case_and_whitespace_insensitive`).
3. Topic-node seeding into the AGE `nexus_knowledge` graph is explicitly OUT OF SCOPE for this
   task/repo and therefore has no test here - see the task report for why (in short:
   `mytheus-backend/CLAUDE.md` states this repo reads `nexus_knowledge` but never writes it,
   `knowledge-graph-service` is documented as "the ONLY writer" and is a bare scaffold with no
   write endpoint yet, and the plan's own text assigns the seeding step to the seeder, section
   3.4, not to this section).

Does not re-test the `get_user_context()` machinery itself (cache, RLS, gRPC parity, PATCH
eviction) - that is `tests/test_user_context.py`'s job, and those 13 tests already cover the
integration point (they seed `role="engineer"` and assert `topics` is non-empty / changes on a
role change, both of which still hold against the real taxonomy - see the report for why no
literal topic values needed updating).
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.routes import taxonomy as taxonomy_routes
from services.identity_tenant_service.taxonomy import loader
from tests.conftest import APP_URL, BOOTSTRAP_URL

# --- 2: topics_for() is a pure function, no HTTP/DB needed for the resolution itself --------


def test_topics_for_engineer_is_deterministic_and_matches_the_yaml():
    expected = [
        "architecture",
        "api-design",
        "infra",
        "incidents",
        "code-review",
        "performance",
    ]
    assert loader.topics_for("engineer") == expected
    assert loader.topics_for("engineer") == expected  # same call again: still identical


def test_topics_for_matches_every_role_in_the_v1_yaml():
    taxonomy = loader.load_taxonomy("v1")
    assert taxonomy.version == "v1"
    for role, expected_topics in taxonomy.roles.items():
        assert loader.topics_for(role) == expected_topics


def test_topics_for_is_case_and_whitespace_insensitive():
    assert loader.topics_for("Engineer") == loader.topics_for("engineer")
    assert loader.topics_for("product manager") == loader.topics_for("product_manager")


def test_topics_for_unknown_or_absent_role_is_general():
    assert loader.topics_for(None) == ["general"]
    assert loader.topics_for("") == ["general"]
    assert loader.topics_for("astronaut") == ["general"]
    # `member`'s own explicit YAML row lands on the same answer, for a different reason (an
    # explicit entry, not the fallback) - see loader.topics_for's docstring.
    assert loader.topics_for("member") == ["general"]


def test_topics_for_result_is_not_the_cached_models_own_list_object():
    """A caller mutating the returned list must not corrupt the `lru_cache`d taxonomy for
    every other caller."""
    first = loader.topics_for("engineer")
    first.append("not-a-real-topic")
    assert "not-a-real-topic" not in loader.topics_for("engineer")


def test_load_taxonomy_unknown_version_raises_taxonomy_version_not_found():
    with pytest.raises(loader.TaxonomyVersionNotFound):
        loader.load_taxonomy("v999-does-not-exist")


def test_active_version_defaults_to_v1_and_is_overridable(monkeypatch):
    monkeypatch.delenv("ROLE_TOPICS_TAXONOMY_VERSION", raising=False)
    assert loader.active_version() == "v1"
    monkeypatch.setenv("ROLE_TOPICS_TAXONOMY_VERSION", "v2-not-published-yet")
    assert loader.active_version() == "v2-not-published-yet"
    # load_taxonomy() with no explicit version follows active_version()
    with pytest.raises(loader.TaxonomyVersionNotFound):
        loader.load_taxonomy()


# --- 1: the REST surface ---------------------------------------------------------------------


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
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app: FastAPI = create_app("identity-test")
    app.include_router(taxonomy_routes.router)
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
    def __init__(self, org_id: uuid.UUID, clerk_org: str, user_id: uuid.UUID, clerk_user: str):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user


def seed_member(slug_prefix: str = "role-topics") -> Tenant:
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"{slug_prefix}-{org_id}", "cid": clerk_org},
    )
    user_id = uuid.uuid4()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, 'member')",
        {"o": str(org_id), "u": str(user_id)},
    )
    return Tenant(org_id, clerk_org, user_id, clerk_user)


@pytest.fixture
def member(client):
    t = seed_member()
    PRINCIPAL.clerk_org = t.clerk_org
    PRINCIPAL.clerk_user = t.clerk_user
    return t


def test_get_role_topics_returns_the_full_map_for_v1(client, member):
    """Acceptance check 1 (first half)."""
    resp = client.get("/taxonomy/role-topics", params={"version": "v1"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["version"] == "v1"
    assert body["roles"]["engineer"] == [
        "architecture",
        "api-design",
        "infra",
        "incidents",
        "code-review",
        "performance",
    ]
    assert body["roles"]["member"] == ["general"]
    assert set(body["roles"]) == {
        "engineer",
        "designer",
        "product_manager",
        "sales",
        "finance",
        "member",
    }


def test_get_role_topics_defaults_to_the_active_pinned_version(client, member):
    resp = client.get("/taxonomy/role-topics")
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == "v1"


def test_unknown_version_404s_no_silent_fallback(client, member):
    """Acceptance check 1 (second half): no silent fallback to a known version."""
    resp = client.get("/taxonomy/role-topics", params={"version": "v999-does-not-exist"})
    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"
