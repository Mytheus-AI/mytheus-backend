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
    body = resp.json()
    assert body["user_id"] == str(tenant_a.user_id)
    assert body["org_id"] == str(tenant_a.org_id)


def test_returned_org_id_is_always_the_callers_own_tenant(client, tenant_a, tenant_b):
    """mytheus-realtime's fixed presence-events producer (Task 6 follow-up) needs this field:
    it has no database and cannot resolve its own JWT's opaque Clerk org id into our internal
    UUID any other way. Safe to add because `org_id` in the response is `ctx.org_id` - derived
    solely from re-verifying the CALLER's own bearer token (never a lookup on the meeting or
    the target user) - so it can only ever be the org that owns this HTTP request.

    Proven twice here: once as org A asking about its own meeting, once as org B asking about
    ITS OWN (different) meeting on the SAME running client - the returned `org_id` tracks
    whichever tenant is actually calling, never a fixed or leaked value from the other case.
    """
    meeting_a = seed_meeting(tenant_a.org_id)
    add_participant(meeting_a, tenant_a.org_id, tenant_a.user_id)
    resp_a = client.get(f"/api/v1/meetings/{meeting_a}/participants/{tenant_a.clerk_user}")
    assert resp_a.status_code == 200, resp_a.text
    assert resp_a.json()["org_id"] == str(tenant_a.org_id)

    sign_in(tenant_b)
    meeting_b = seed_meeting(tenant_b.org_id)
    add_participant(meeting_b, tenant_b.org_id, tenant_b.user_id)
    resp_b = client.get(f"/api/v1/meetings/{meeting_b}/participants/{tenant_b.clerk_user}")
    assert resp_b.status_code == 200, resp_b.text
    assert resp_b.json()["org_id"] == str(tenant_b.org_id)
    assert resp_b.json()["org_id"] != resp_a.json()["org_id"]


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


def test_an_unrelated_orgs_real_user_is_not_spuriously_reported_a_participant(
    client, tenant_a, tenant_b
):
    """A narrower, honest claim than this test used to make (correction recorded in the
    task-5 report and `mytheus-docs/work-done-till-now`): org A's real meeting, but the path
    names org B's REAL, existing `clerk_user_id`, who is a participant of nothing anywhere ->
    403. This shows the lookup does not spuriously cross-match an unrelated real user; it does
    NOT by itself prove RLS is what is doing the work, because no `meeting_participants` row
    exists for org B's user at all - the JOIN would return nothing with or without RLS. The
    test that actually exercises RLS on the cross-tenant boundary is
    `test_cross_tenant_meeting_gets_404_not_403` above (a real org-B meeting WITH a real
    org-B participant, queried as org A, must 404 via `_fetch_meeting`'s RLS-scoped existence
    check before the participant JOIN ever runs). The test directly below,
    `test_the_participant_join_result_depends_on_users_rls_not_just_on_row_existence`, isolates
    the JOIN layer specifically, by planting a row that a non-RLS-scoped version of this same
    query WOULD find.
    """
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)
    # tenant_b is a real org with a real user, participant of nothing anywhere.

    sign_in(tenant_a)
    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_b.clerk_user}")

    assert resp.status_code == 403, "org B's real clerk_user_id must not be found under org A"
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"


def test_the_participant_join_result_depends_on_users_rls_not_just_on_row_existence(
    client, tenant_a, tenant_b
):
    """The rigorous version of the test above, per the team lead's option 2: proves
    `check_participant`'s `meeting_participants JOIN users` result is actually gated by RLS on
    `users`, not merely by the absence of a matching row.

    Plants, AS THE SCHEMA OWNER (bypassing RLS - this exact row is unrepresentable through the
    API, since the ordinary insert path sets `org_id` from the caller's own verified token),
    a `meeting_participants` row wiring org B's real user directly to org A's real meeting,
    with `org_id = org A` so the row itself passes `meeting_participants`' OWN RLS check when
    later queried as org A. That isolates the claim precisely: if `users`' RLS did not scope
    the JOIN, org A's `check_participant` call would find this row and wrongly report org B's
    user as a participant of its meeting.

    Step 1 proves the row is real and the raw, unscoped query WOULD match it (a bootstrap
    connection bypasses RLS entirely - same technique `tests/test_roles_and_policies.py` uses
    to seed cross-tenant fixtures no application role could write). Step 2 runs the actual
    endpoint, as the real application role, as org A - and must still get 403, because `users`'
    RLS hides org B's user row from the JOIN even though `meeting_participants`' own row is
    visible.
    """
    meeting_id = seed_meeting(tenant_a.org_id)

    # As the schema owner: the exact row that would make org B's user look like a participant
    # of org A's meeting if this endpoint's queries were not RLS-scoped. org_id matches the
    # meeting's real tenant (org A), so this row passes meeting_participants' OWN RLS check
    # under app.current_org = org A - isolating that users' RLS is what has to save us.
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",
        {"m": str(meeting_id), "u": str(tenant_b.user_id), "o": str(tenant_a.org_id)},
    )

    # Step 1: the planted row is real, and the same JOIN check_participant runs - over an
    # RLS-bypassing connection - genuinely matches it. If this assertion ever fails, the test
    # below would be proving nothing (there'd be no false positive to prevent).
    unscoped = _sql(
        "SELECT mp.user_id FROM meeting_participants mp "
        "JOIN users u ON u.id = mp.user_id "
        "WHERE mp.meeting_id = %(m)s AND u.clerk_user_id = %(cuid)s",
        {"m": str(meeting_id), "cuid": tenant_b.clerk_user},
    )
    assert len(unscoped) == 1, "the planted row must be visible to an RLS-bypassing connection"

    # Step 2: the real endpoint, as the real application role, as org A. users' RLS must hide
    # org B's user row from the JOIN despite the meeting_participants row itself being
    # org-A-visible.
    sign_in(tenant_a)
    resp = client.get(f"/api/v1/meetings/{meeting_id}/participants/{tenant_b.clerk_user}")

    assert resp.status_code == 403, "users' RLS must prevent this false positive"
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"
