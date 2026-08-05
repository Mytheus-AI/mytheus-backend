"""`GET /api/v1/meetings/{id}/participants/{clerk_user_id}`: Task 5, contract-verification
section of the brief. `realtime-gateway` (Task 6) calls this on every Socket.io `join` and
resolves ONLY an explicit HTTP 200 to "allowed" - see `coreApiClient.ts`'s `isParticipant`,
which treats any other status (403, 404, 5xx, a network error) as `false` and denies the join.

These tests assert exactly the three response shapes the gateway's contract distinguishes:
200 (participant), 403 (known user, not on this meeting), 404 (unknown meeting / cross-tenant).
The path parameter is the caller's Clerk-native user id (`sub` claim), matching what the
gateway actually has - see `routes.meetings.check_participant`'s docstring for why.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import meetings
from tests.conftest import APP_URL, BOOTSTRAP_URL


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

    app = create_app("core-api-test")
    app.include_router(meetings.router)
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


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id, clerk_org, user_id, clerk_user):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user


def seed_tenant() -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"pc-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    return Tenant(org_id, clerk_org, user_id, clerk_user)


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user


def seed_meeting(org_id: uuid.UUID) -> uuid.UUID:
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title) VALUES (%(id)s, %(org)s, 'M')",
        {"id": str(meeting_id), "org": str(org_id)},
    )
    return meeting_id


def add_participant(meeting_id: uuid.UUID, org_id: uuid.UUID, user_id: uuid.UUID) -> None:
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": str(meeting_id), "u": str(user_id), "o": str(org_id)},
    )


@pytest.fixture
def tenant_a():
    t = seed_tenant()
    sign_in(t)
    return t


@pytest.fixture
def tenant_b():
    return seed_tenant()


def test_participant_gets_200_with_the_resource(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_a.clerk_user}")

    assert resp.status_code == 200, resp.text
    assert resp.json()["user_id"] == str(tenant_a.user_id)


def test_non_participant_gets_403(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    # tenant_a's user row exists but was never added as a participant.

    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_a.clerk_user}")

    assert resp.status_code == 403
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"


def test_unknown_meeting_gets_404(client, tenant_a):
    resp = client.get(f"/api/v1/meetings/{uuid.uuid4()}/participants/{tenant_a.clerk_user}")
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_cross_tenant_meeting_gets_404_not_403(client, tenant_a, tenant_b):
    meeting_id = seed_meeting(tenant_b.org_id)
    add_participant(meeting_id, tenant_b.org_id, tenant_b.user_id)

    sign_in(tenant_a)
    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_a.clerk_user}")

    assert resp.status_code == 404, "must be 404, never 403 - existence is tenant-confidential"
    assert resp.json()["code"] == "NOT_FOUND"


def test_unknown_clerk_user_id_gets_403_not_500(client, tenant_a):
    """A well-formed but nonexistent Clerk user id must deny closed, not error."""
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/user_doesNotExist")

    assert resp.status_code == 403
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"


def test_org_b_user_is_never_reported_a_participant_of_an_org_a_meeting(client, tenant_a, tenant_b):
    """Team lead's mid-task concern, verified directly: `realtime-gateway` scopes tenancy via
    an `X-Org-Id` header it sets from its own verified JWT, but that header is not the safety
    boundary here - this route never reads it (it isn't declared as a parameter at all; see
    `check_participant`'s docstring). Tenancy for BOTH the meeting-existence check and this
    participant lookup comes from `tenant_session`, which resolves `org_id` from the caller's
    own verified bearer token via the same `tenant_context` chain every other route in this
    service uses.

    Concretely: org A's meeting, but the path names org B's REAL clerk_user_id (a genuine,
    existing user - not the "doesn't exist anywhere" case the previous test covers). RLS on
    `users` scopes the lookup to `app.current_org` (org A, from tenant_a's own verified
    token), so org B's user row is invisible to that query - not merely "not a participant",
    genuinely not found - and the JOIN in `check_participant` returns nothing regardless of
    whether org B's user happens to be a participant of some OTHER meeting. If this route
    instead resolved participants by `meeting_id` alone (ignoring tenancy), org B's user could
    be reported as a participant of org A's meeting and `realtime-gateway` would let them into
    the room - exactly the breach this test rules out.
    """
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)
    # tenant_b is a real org with a real user, participant of nothing in tenant_a.

    sign_in(tenant_a)
    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_b.clerk_user}")

    assert resp.status_code == 403, "org B's real clerk_user_id must not be found under org A"
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"
