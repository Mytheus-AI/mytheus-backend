"""Meetings CRUD: the plan's section 3.6 and 3.8 acceptance checks, end to end over HTTP.

These run against the real database as `mytheus_app`, with a real transaction per request, so
they exercise the actual RLS path rather than a mocked one. Auth is the one thing stubbed:
`verify_token` is overridden so the tests do not need a live Clerk. Everything downstream of
the claims - the `clerk_org_id` resolution, the tier lookup, `app.current_org`, the policies -
is real.

Two structural choices, both to avoid the async-engine-across-event-loops trap:

- ONE module-scoped TestClient. Each TestClient runs its own event loop, and the process-wide
  engine pools connections; a second client reuses a connection bound to the first client's
  now-dead loop. Switching the signed-in principal on one client sidesteps that entirely.
- Database assertions use psycopg2 synchronously, so no test touches an event loop at all.
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
from services.core_api.routes import meetings, org_hierarchy
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    """The signed-in caller. Mutated between tests to switch tenants on the one client."""

    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()


@pytest.fixture(scope="module")
def client(migrated_db):
    """One app, one event loop, engine pointed at the request-path role."""
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("core-api-test")
    app.include_router(meetings.router)
    app.include_router(org_hierarchy.router)
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
    """Synchronous query as the schema owner. Test scaffolding only."""
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


class Tenant:
    def __init__(self, org_id, clerk_org, user_id, clerk_user, tier):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user
        self.tier = tier


def seed_tenant(tier: str = "member") -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"api-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) " "VALUES (%(o)s, %(u)s, %(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return Tenant(org_id, clerk_org, user_id, clerk_user, tier)


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user


@pytest.fixture
def tenant_a():
    t = seed_tenant(tier="admin")
    sign_in(t)
    return t


@pytest.fixture
def tenant_b():
    return seed_tenant(tier="admin")


# --- creation -------------------------------------------------------------------------------


def test_create_persists_event_and_audit_in_one_transaction(client, tenant_a):
    """Plan 3.6 acceptance 1: the row, the outbox row and the audit row are one fact."""
    resp = client.post(
        "/api/v1/meetings",
        json={"title": "Sprint planning", "participant_ids": [str(tenant_a.user_id)]},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["title"] == "Sprint planning"
    assert body["status"] == "scheduled"
    assert body["created_by"] == str(tenant_a.user_id)
    assert [p["user_id"] for p in body["participants"]] == [str(tenant_a.user_id)]
    assert resp.headers["Location"] == f"/api/v1/meetings/{body['id']}"

    outbox = _sql(
        "SELECT topic, partition_key FROM outbox WHERE idempotency_key = %(ik)s",
        {"ik": f"meeting:{body['id']}:created"},
    )
    assert len(outbox) == 1
    assert outbox[0]["topic"] == "meeting-events"
    assert outbox[0]["partition_key"] == str(tenant_a.org_id), "partition key must be org_id"

    audit_rows = _sql(
        "SELECT action, actor_type, actor_id FROM audit_logs WHERE target_id = %(t)s",
        {"t": body["id"]},
    )
    assert len(audit_rows) == 1
    assert audit_rows[0]["action"] == "meeting.created"
    assert str(audit_rows[0]["actor_id"]) == str(tenant_a.user_id)


def test_creator_is_the_host_without_being_listed(client, tenant_a):
    """The caller who creates a meeting can join it. The token route is deny-by-default and
    admits only `meeting_participants` rows, so a creator who had to remember to list
    themselves got NOT_A_PARTICIPANT from their own meeting."""
    resp = client.post("/api/v1/meetings", json={"title": "Solo"})
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert [(p["user_id"], p["role_in_meeting"]) for p in body["participants"]] == [
        (str(tenant_a.user_id), "host")
    ]
    rows = _sql(
        "SELECT user_id, role_in_meeting FROM meeting_participants WHERE meeting_id = %(m)s",
        {"m": body["id"]},
    )
    assert [(str(r["user_id"]), r["role_in_meeting"]) for r in rows] == [
        (str(tenant_a.user_id), "host")
    ]


def test_creator_listed_explicitly_is_not_duplicated(client, tenant_a):
    resp = client.post(
        "/api/v1/meetings",
        json={"title": "Listed", "participant_ids": [str(tenant_a.user_id)]},
    )
    assert resp.status_code == 201, resp.text
    participants = resp.json()["participants"]
    assert [p["user_id"] for p in participants] == [str(tenant_a.user_id)]
    assert participants[0]["role_in_meeting"] == "host"


def test_creator_comes_first_and_invitees_follow(client, tenant_a):
    invitee = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'invitee@example.com')",
        {"u": str(invitee), "o": str(tenant_a.org_id), "c": f"user_{uuid.uuid4().hex[:16]}"},
    )
    resp = client.post(
        "/api/v1/meetings", json={"title": "Pair", "participant_ids": [str(invitee)]}
    )
    assert resp.status_code == 201, resp.text
    got = [(p["user_id"], p["role_in_meeting"]) for p in resp.json()["participants"]]
    # ADR-0038 D5: every participant who is not the host is an `attendee`.
    assert got == [(str(tenant_a.user_id), "host"), (str(invitee), "attendee")]


def test_validation_failure_writes_nothing(client, tenant_a):
    """422 before any write, in the RFC 7807 shape with a machine-readable code."""
    resp = client.post(
        "/api/v1/meetings",
        json={
            "title": "Backwards",
            "scheduled_start": "2026-08-01T10:00:00Z",
            "scheduled_end": "2026-08-01T09:00:00Z",
        },
    )
    assert resp.status_code == 422
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert resp.json()["code"] == "VALIDATION_FAILED"
    assert _sql("SELECT 1 FROM meetings WHERE org_id = %(o)s", {"o": str(tenant_a.org_id)}) == []


def test_unknown_field_is_rejected(client, tenant_a):
    """extra="forbid": a typo'd field must not be silently dropped."""
    assert client.post("/api/v1/meetings", json={"title": "x", "titel": "typo"}).status_code == 422


# --- tenant isolation -----------------------------------------------------------------------


def test_cross_tenant_get_is_404_not_403(client, tenant_a, tenant_b):
    """Plan 3.6 acceptance 2 and 3.8 acceptance 4. Existence is tenant-confidential."""
    created = client.post("/api/v1/meetings", json={"title": "A's meeting"}).json()

    sign_in(tenant_b)
    resp = client.get(f"/api/v1/meetings/{created['id']}")
    assert resp.status_code == 404, "a cross-tenant read must not reveal that the id exists"
    assert resp.json()["code"] == "NOT_FOUND"


def test_cross_tenant_patch_is_404(client, tenant_a, tenant_b):
    created = client.post("/api/v1/meetings", json={"title": "A's meeting"}).json()
    sign_in(tenant_b)
    resp = client.patch(f"/api/v1/meetings/{created['id']}", json={"title": "hijacked"})
    assert resp.status_code == 404
    # And nothing was written: the title in the database is still A's.
    rows = _sql("SELECT title FROM meetings WHERE id = %(i)s", {"i": created["id"]})
    assert rows[0]["title"] == "A's meeting"


def test_list_never_shows_another_tenants_meetings(client, tenant_a, tenant_b):
    for i in range(3):
        client.post("/api/v1/meetings", json={"title": f"A-{i}"})

    sign_in(tenant_b)
    client.post("/api/v1/meetings", json={"title": "B-only"})
    titles = [m["title"] for m in client.get("/api/v1/meetings").json()["items"]]
    assert titles == ["B-only"]


def test_another_tenants_user_cannot_be_named_as_a_participant(client, tenant_a, tenant_b):
    """The foreign key does NOT enforce this, contrary to what this route used to claim.

    PostgreSQL runs referential-integrity checks as the referenced table's owner with RLS
    bypassed, so `fk_meeting_participants_user_id_users` resolves another tenant's `users` row
    even though this transaction cannot SELECT it. `meeting_participants`' own WITH CHECK
    passes too, because the `org_id` written is the caller's own - the column crossing the
    boundary is `user_id`, which no policy constrains.

    Observed before the fix: `201`, with a persisted row whose `org_id` was the caller's tenant
    and whose `user_id` belonged to the other tenant. It is not a read leak (every read path
    filters on the denormalized `org_id`), but it is a reference that resolves outside the
    tenant, which is the property the Plan 1-2 D6 foreign-key work exists to guarantee and
    which right-to-be-forgotten relies on to find everything.
    """
    sign_in(tenant_a)

    resp = client.post(
        "/api/v1/meetings",
        json={"title": "Cross-tenant invite", "participant_ids": [str(tenant_b.user_id)]},
    )

    assert resp.status_code == 404, resp.text
    assert resp.json()["code"] == "NOT_FOUND"
    assert (
        _sql(
            "SELECT 1 FROM meeting_participants WHERE user_id = %(u)s",
            {"u": str(tenant_b.user_id)},
        )
        == []
    ), "nothing may persist, including the meeting's own row"
    # Scoped to this run's org, not to the title. The suite shares a long-lived dev database
    # and does not truncate between runs, so a title-only predicate also matches rows a
    # PREVIOUS run committed - which is exactly how this assertion first appeared to fail
    # against a fix that was in fact working.
    assert (
        _sql(
            "SELECT 1 FROM meetings WHERE org_id = %(o)s",
            {"o": str(tenant_a.org_id)},
        )
        == []
    ), "the meeting must roll back with the participant that made it invalid"


# --- authorization --------------------------------------------------------------------------


def test_member_cannot_create_a_team_and_the_denial_is_audited(client):
    """Plan 3.8 acceptance 3: a 403 for the role case, plus an `authz.deny` audit row.

    Distinct from the cross-tenant 404 above. That one hides whether a resource exists; this
    one is about what the caller may do and says nothing about any resource.
    """
    member = seed_tenant(tier="member")
    sign_in(member)

    resp = client.post("/api/v1/teams", json={"name": "Platform"})
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN_ROLE"

    rows = _sql(
        "SELECT action, metadata FROM audit_logs WHERE org_id = %(o)s AND action = 'authz.deny'",
        {"o": str(member.org_id)},
    )
    assert len(rows) == 1
    assert rows[0]["metadata"]["required"] == "admin"
    assert rows[0]["metadata"]["held"] == "member"
    # The denial survived the 403: it was written on its own transaction, not the discarded one.
    assert _sql("SELECT 1 FROM teams WHERE org_id = %(o)s", {"o": str(member.org_id)}) == []


def test_a_user_with_no_grant_is_permitted_nothing(client):
    """No grant means no tier, and no tier satisfies any route. Authenticated is not authorized."""
    ungranted = seed_tenant(tier="")
    sign_in(ungranted)
    assert client.get("/api/v1/meetings").status_code == 403


def test_admin_can_create_a_team(client, tenant_a):
    resp = client.post("/api/v1/teams", json={"name": "Platform"})
    assert resp.status_code == 201
    assert resp.json()["name"] == "Platform"


def test_suspended_tenant_is_refused_at_the_checkpoint(client, tenant_a):
    """The resolver is the authorization checkpoint, so suspension takes effect immediately."""
    assert client.get("/api/v1/meetings").status_code == 200
    _sql(
        "UPDATE organizations SET status = 'suspended' WHERE id = %(o)s",
        {"o": str(tenant_a.org_id)},
    )
    resp = client.get("/api/v1/meetings")
    assert resp.status_code == 401, "a suspended tenant must stop at the edge, not at a policy"
    assert resp.json()["detail"] == "Token carries no usable organization"


# --- pagination -----------------------------------------------------------------------------


def test_cursor_pagination_is_stable_and_complete(client, tenant_a):
    """Plan 3.6 acceptance 5. Every meeting appears exactly once across the pages."""
    created = {
        client.post("/api/v1/meetings", json={"title": f"M-{i}"}).json()["id"] for i in range(7)
    }

    seen: list[str] = []
    cursor = None
    for _ in range(10):  # bounded so a broken cursor loops finitely rather than forever
        url = f"/api/v1/meetings?limit=3{f'&cursor={cursor}' if cursor else ''}"
        page = client.get(url).json()
        seen.extend(m["id"] for m in page["items"])
        cursor = page["next_cursor"]
        if not cursor:
            break

    assert len(seen) == len(set(seen)), "a row was returned on two pages"
    assert created <= set(seen), "paging dropped a row"


def test_malformed_cursor_is_400_not_500(client, tenant_a):
    resp = client.get("/api/v1/meetings?cursor=not-a-real-cursor")
    assert resp.status_code == 400
    assert resp.json()["code"] == "INVALID_CURSOR"


def test_limit_is_bounded(client, tenant_a):
    assert client.get("/api/v1/meetings?limit=201").status_code == 422
    assert client.get("/api/v1/meetings?limit=0").status_code == 422


# --- update ---------------------------------------------------------------------------------


def test_patch_returns_the_canonical_resource(client, tenant_a):
    created = client.post("/api/v1/meetings", json={"title": "Before"}).json()

    resp = client.patch(f"/api/v1/meetings/{created['id']}", json={"title": "After"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == "After"
    assert body["id"] == created["id"]
    # Not a partial echo: every field of the persisted resource comes back.
    assert set(body) == set(created)


def test_two_updates_both_reach_the_outbox(client, tenant_a):
    """The `updated` idempotency key carries updated_at.

    Keyed on the meeting id alone, the outbox's unique index would swallow every update after
    the first and the bus would silently lose them.
    """
    created = client.post("/api/v1/meetings", json={"title": "v1"}).json()
    client.patch(f"/api/v1/meetings/{created['id']}", json={"title": "v2"})
    client.patch(f"/api/v1/meetings/{created['id']}", json={"status": "live"})

    rows = _sql(
        "SELECT idempotency_key FROM outbox WHERE topic = 'meeting-events' "
        "AND idempotency_key LIKE %(pat)s",
        {"pat": f"meeting:{created['id']}:updated:%"},
    )
    assert len(rows) == 2


def test_status_ended_emits_event_type_ended_not_updated(client, tenant_a):
    """Task 5, Plan 3-4 section 3.3 step 4 ("verify [meeting-events] emits
    created/updated/ended"): before this task every PATCH emitted `event_type: "updated"`
    unconditionally, even one that set `status` to `"ended"`, so a consumer had no way to tell
    a meeting had actually ended without separately fetching and diffing the resource.
    `MeetingEventType` (mytheus-events schemas/meeting-events/v1.avsc) has always carried all
    three symbols; this asserts the producer now actually uses the third one.
    """
    created = client.post("/api/v1/meetings", json={"title": "Standup"}).json()

    resp = client.patch(f"/api/v1/meetings/{created['id']}", json={"status": "ended"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "ended"

    rows = _sql(
        "SELECT payload FROM outbox WHERE topic = 'meeting-events' "
        "AND idempotency_key LIKE %(pat)s",
        {"pat": f"meeting:{created['id']}:ended:%"},
    )
    assert len(rows) == 1, "the ended transition must use the 'ended' idempotency-key shape"
    assert rows[0]["payload"]["event_type"] == "ended"

    # An ordinary field update on the same meeting still emits "updated", not "ended".
    client.patch(f"/api/v1/meetings/{created['id']}", json={"title": "renamed post-hoc"})
    updated_rows = _sql(
        "SELECT payload FROM outbox WHERE topic = 'meeting-events' "
        "AND idempotency_key LIKE %(pat)s",
        {"pat": f"meeting:{created['id']}:updated:%"},
    )
    assert len(updated_rows) == 1
    assert updated_rows[0]["payload"]["event_type"] == "updated"


# --- inviting into an existing meeting ---------------------------------------------------------


def _add_member(tenant: Tenant) -> uuid.UUID:
    uid = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(u)s, %(o)s, %(c)s, 'm@x.test')",
        {"u": str(uid), "o": str(tenant.org_id), "c": f"user_{uid.hex[:16]}"},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, 'member')",
        {"o": str(tenant.org_id), "u": str(uid)},
    )
    return uid


def test_a_participant_invites_a_member_and_it_is_audited(client, tenant_a):
    meeting = client.post("/api/v1/meetings", json={"title": "Invite me"}).json()
    invitee = _add_member(tenant_a)

    r = client.post(
        f"/api/v1/meetings/{meeting['id']}/participants", json={"user_ids": [str(invitee)]}
    )

    assert r.status_code == 200, r.text
    assert {p["user_id"] for p in r.json()["participants"]} == {
        str(tenant_a.user_id),
        str(invitee),
    }
    assert _sql(
        "SELECT count(*) AS n FROM audit_logs WHERE target_id = %(m)s "
        "AND action = 'meeting.participants_added'",
        {"m": meeting["id"]},
    ) == [{"n": 1}]


def test_inviting_someone_already_in_is_a_no_op(client, tenant_a):
    invitee = _add_member(tenant_a)
    meeting = client.post(
        "/api/v1/meetings", json={"title": "Already in", "participant_ids": [str(invitee)]}
    ).json()

    r = client.post(
        f"/api/v1/meetings/{meeting['id']}/participants", json={"user_ids": [str(invitee)]}
    )

    assert r.status_code == 200
    assert len(r.json()["participants"]) == 2
    assert _sql(
        "SELECT count(*) AS n FROM audit_logs WHERE target_id = %(m)s "
        "AND action = 'meeting.participants_added'",
        {"m": meeting["id"]},
    ) == [{"n": 0}]


def test_someone_not_in_the_meeting_cannot_invite(client, tenant_a):
    meeting = client.post("/api/v1/meetings", json={"title": "Closed"}).json()
    outsider = _add_member(tenant_a)
    _sql("UPDATE rbac_grants SET tier='member' WHERE user_id=%(u)s", {"u": str(outsider)})
    clerk = _sql("SELECT clerk_user_id FROM users WHERE id=%(u)s", {"u": str(outsider)})[0]
    PRINCIPAL.clerk_user = clerk["clerk_user_id"]

    r = client.post(
        f"/api/v1/meetings/{meeting['id']}/participants", json={"user_ids": [str(outsider)]}
    )

    assert r.status_code == 403
    sign_in(tenant_a)


def test_another_tenants_user_cannot_be_invited_into_an_existing_meeting(
    client, tenant_a, tenant_b
):
    meeting = client.post("/api/v1/meetings", json={"title": "Mine"}).json()

    r = client.post(
        f"/api/v1/meetings/{meeting['id']}/participants", json={"user_ids": [str(tenant_b.user_id)]}
    )

    assert r.status_code == 404
    assert _sql(
        "SELECT count(*) AS n FROM meeting_participants WHERE meeting_id = %(m)s",
        {"m": meeting["id"]},
    ) == [{"n": 1}]


def test_another_tenants_meeting_is_404_for_invites(client, tenant_a, tenant_b):
    sign_in(tenant_b)
    theirs = client.post("/api/v1/meetings", json={"title": "Theirs"}).json()
    sign_in(tenant_a)

    r = client.post(
        f"/api/v1/meetings/{theirs['id']}/participants", json={"user_ids": [str(tenant_a.user_id)]}
    )

    assert r.status_code == 404


def test_create_records_where_the_call_happens(client, tenant_a):
    """meetings.source: omitted is a Mytheus room; a captured Meet/Teams/Zoom call says so; an
    unknown source is a 422, never a silent default."""
    plain = client.post("/api/v1/meetings", json={"title": "Standup"})
    assert plain.status_code == 201, plain.text
    assert plain.json()["source"] == "mytheus"

    captured = client.post("/api/v1/meetings", json={"title": "Vendor call", "source": "meet"})
    assert captured.status_code == 201, captured.text
    assert captured.json()["source"] == "meet"
    got = client.get(f"/api/v1/meetings/{captured.json()['id']}").json()
    assert got["source"] == "meet"

    assert (
        client.post("/api/v1/meetings", json={"title": "x", "source": "skype"}).status_code == 422
    )
