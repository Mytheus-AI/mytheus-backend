"""`POST /api/meetings/{meeting_id}/token`: Task 3, Plan 3-4 section 3.1 step 4.

Same structural pattern as `tests/test_meetings_api.py`: one module-scoped `TestClient` (an
async engine cannot cross event loops, and each `TestClient` runs its own), `verify_token`
overridden so no live Clerk is needed, everything downstream real — `clerk_org_id`
resolution, `app.current_org`, RLS, the actual token-mint route body. `FakeTransport` stands
in for LiveKit (no account exists for this project; see `services/meeting_service/
transport.py`), injected via `app.dependency_overrides[routes.get_transport]` so each test
can assert on `fake.calls` or leave it as a fresh instance per test.

Acceptance checks covered here (Task 3 brief):
  1. seeded participant -> {token, room, livekit_url}
  2. non-participant -> 403 NOT_A_PARTICIPANT
  3. participant of an org-B meeting, asking as org A -> 404 (never 403, never 200)
  4. consent_required=false (default, no tenant_settings row at all) -> mint succeeds
  5. consent_required=true, consented_at NULL -> 428 with an RFC 7807 body
  6. consent_required=true, consented_at set -> mint succeeds
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.meeting_service import routes
from services.meeting_service.transport import FakeTransport
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    """The signed-in caller. Mutated between tests to switch tenants on the one client."""

    clerk_org = ""
    clerk_user = ""


PRINCIPAL = Principal()
FAKE = FakeTransport()


@pytest.fixture(scope="module")
def client(migrated_db):
    """One app, one event loop, engine pointed at the request-path role (`mytheus_app`)."""
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = create_app("meeting-service-test")
    app.include_router(routes.router)
    app.dependency_overrides[auth_mod.verify_token] = lambda: {
        "sub": PRINCIPAL.clerk_user,
        "org_id": PRINCIPAL.clerk_org,
    }
    app.dependency_overrides[routes.get_transport] = lambda: FAKE

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
        {"id": str(org_id), "slug": f"tok-{org_id}", "cid": clerk_org},
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


def add_participant(
    meeting_id: uuid.UUID, org_id: uuid.UUID, user_id: uuid.UUID, role: str = "host"
) -> None:
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
        "VALUES (%(m)s, %(u)s, %(o)s, %(r)s)",
        {"m": str(meeting_id), "u": str(user_id), "o": str(org_id), "r": role},
    )


@pytest.fixture
def tenant_a():
    t = seed_tenant()
    sign_in(t)
    return t


@pytest.fixture
def tenant_b():
    return seed_tenant()


@pytest.fixture(autouse=True)
def _fresh_fake_calls():
    """Each test starts with an empty call log, without paying for a fresh FastAPI app."""
    FAKE.calls.clear()
    FAKE.room_metadata.clear()
    FAKE.verify_webhook_result = None
    FAKE.verify_webhook_error = None


# --- acceptance check 1: happy path ----------------------------------------------------------


def test_seeded_participant_receives_token_room_and_livekit_url(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id, role="host")

    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["room"] == f"meeting-{meeting_id}"
    assert body["token"].startswith("fake.")
    assert "livekit_url" in body

    # Fix round 1: `transport.ensure_room` no longer runs inline inside the route (it made two
    # real LiveKit HTTP calls that, before the fix, ran inside this request's open Postgres
    # transaction). It is scheduled via `BackgroundTasks`, which Starlette runs AFTER the
    # response is sent and every dependency (including the DB session) has already closed -
    # so on `FAKE.calls` it now lands AFTER `mint_join_token`, not before. `TestClient` awaits
    # the background task as part of the same ASGI call, so it has already run by the time
    # this `client.post(...)` returns - no sleep/poll needed to observe it.
    assert len(FAKE.calls) == 2
    mint_call, room_call = FAKE.calls
    assert mint_call.method == "mint_join_token"
    assert mint_call.kwargs["org_id"] == tenant_a.org_id
    assert mint_call.kwargs["meeting_id"] == meeting_id
    assert mint_call.kwargs["user_id"] == tenant_a.user_id
    assert mint_call.kwargs["role_in_meeting"] == "host"
    assert mint_call.kwargs["ttl_seconds"] == 600

    assert room_call.method == "ensure_room"
    assert room_call.kwargs["org_id"] == tenant_a.org_id
    assert room_call.kwargs["room"] == f"meeting-{meeting_id}"

    # The room's LiveKit metadata carries the INTERNAL org_id, not a Clerk id.
    assert FAKE.room_metadata[f"meeting-{meeting_id}"] == json.dumps(
        {"org_id": str(tenant_a.org_id)}
    )

    # The upsert actually landed a row, not just a response body.
    rows = _sql(
        "SELECT livekit_room, status FROM meeting_rooms WHERE meeting_id = %(m)s",
        {"m": str(meeting_id)},
    )
    assert len(rows) == 1
    assert rows[0]["livekit_room"] == f"meeting-{meeting_id}"
    assert rows[0]["status"] == "pending"


def test_role_defaults_to_attendee_when_unset(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    _sql(
        "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
        "VALUES (%(m)s, %(u)s, %(o)s)",  # role_in_meeting left NULL
        {"m": str(meeting_id), "u": str(tenant_a.user_id), "o": str(tenant_a.org_id)},
    )

    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 200, resp.text
    mint_calls = [c for c in FAKE.calls if c.method == "mint_join_token"]
    assert mint_calls[-1].kwargs["role_in_meeting"] == "attendee"


def test_repeated_mint_reuses_the_same_room(client, tenant_a):
    """`ensure_meeting_room` is an upsert: the room name must not change between calls."""
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    first = client.post(f"/api/meetings/{meeting_id}/token").json()
    second = client.post(f"/api/meetings/{meeting_id}/token").json()

    assert first["room"] == second["room"]
    rows = _sql(
        "SELECT count(*) AS n FROM meeting_rooms WHERE meeting_id = %(m)s", {"m": str(meeting_id)}
    )
    assert rows[0]["n"] == 1, "the upsert must not create a second row"

    # Fix round 1, "consider whether they need to happen on every mint at all": once a room
    # has synced successfully, a later mint for the SAME meeting must not call
    # `transport.ensure_room` again - `livekit_metadata_synced_at` makes the second mint a
    # pure DB upsert with no LiveKit round trip at all.
    ensure_room_calls = [c for c in FAKE.calls if c.method == "ensure_room"]
    assert len(ensure_room_calls) == 1, "a synced room must not be re-synced on a later mint"


# --- acceptance check 2: non-participant -------------------------------------------------------


def test_non_participant_in_own_org_gets_403_not_a_participant(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    # tenant_a is signed in but was never added as a participant.

    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 403
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"
    assert FAKE.calls == [], "no token may be minted for a denied caller"


def test_authenticated_user_with_no_local_user_row_gets_403(client, tenant_a):
    """A verified Clerk token with no mirrored `users` row cannot be a participant of
    anything (the FK requires a real `users.id`); same denial as the ordinary case."""
    meeting_id = seed_meeting(tenant_a.org_id)
    PRINCIPAL.clerk_org = tenant_a.clerk_org
    PRINCIPAL.clerk_user = f"user_{uuid.uuid4().hex[:16]}"  # never inserted into users

    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 403
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"


# --- acceptance check 3: cross-tenant is 404, never 403 ----------------------------------------


def test_cross_tenant_meeting_is_404_not_403(client, tenant_a, tenant_b):
    """A participant of an org-B meeting, asking as org A, must get 404 - existence of
    another tenant's meeting is confidential, and a 403 here would confirm the id exists."""
    meeting_id = seed_meeting(tenant_b.org_id)
    add_participant(meeting_id, tenant_b.org_id, tenant_b.user_id)  # tenant_b IS a participant

    sign_in(tenant_a)  # ask as org A instead
    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 404, "must be 404, never 403 and never 200"
    assert resp.json()["code"] == "NOT_FOUND"
    assert FAKE.calls == []


def test_cross_tenant_meeting_for_a_non_participant_is_still_404_not_403(
    client, tenant_a, tenant_b
):
    """Same 404 even when the caller (in org A) is also not a participant of org B's meeting -
    proves the order is existence-then-participation, not the reverse (see routes.py docstring)."""
    meeting_id = seed_meeting(tenant_b.org_id)
    # no participant row for anyone in org A on this meeting

    sign_in(tenant_a)
    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


# --- acceptance checks 4-6: the consent gate ----------------------------------------------------


def test_consent_not_required_by_default_mints_with_no_consent_row(client, tenant_a):
    """No `tenant_settings` row exists for this org at all (nothing creates one yet - the
    `/orgs` route is a later task). The gate must be genuinely inert, not merely defaulted:
    the mint succeeds exactly as if the column did not exist."""
    assert (
        _sql("SELECT 1 FROM tenant_settings WHERE org_id = %(o)s", {"o": str(tenant_a.org_id)})
        == []
    )

    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    resp = client.post(f"/api/meetings/{meeting_id}/token")
    assert resp.status_code == 200, resp.text


def test_consent_required_true_and_unconsented_is_428(client, tenant_a):
    _sql(
        "INSERT INTO tenant_settings (org_id, consent_required) VALUES (%(o)s, true)",
        {"o": str(tenant_a.org_id)},
    )
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)  # consented_at left NULL

    resp = client.post(f"/api/meetings/{meeting_id}/token")

    assert resp.status_code == 428, resp.text
    assert resp.headers["content-type"].startswith("application/problem+json")
    body = resp.json()
    assert body["code"] == "CONSENT_REQUIRED"
    assert body["status"] == 428
    assert FAKE.calls == [], "no token may be minted before consent is acknowledged"


def test_consent_required_true_and_consented_mints_successfully(client, tenant_a):
    _sql(
        "INSERT INTO tenant_settings (org_id, consent_required) VALUES (%(o)s, true)",
        {"o": str(tenant_a.org_id)},
    )
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)
    _sql(
        "UPDATE meeting_participants SET consented_at = now(), consent_version = 'v1' "
        "WHERE meeting_id = %(m)s AND user_id = %(u)s",
        {"m": str(meeting_id), "u": str(tenant_a.user_id)},
    )

    resp = client.post(f"/api/meetings/{meeting_id}/token")
    assert resp.status_code == 200, resp.text


def test_consent_flag_flip_does_not_affect_a_different_tenant(client, tenant_a, tenant_b):
    """The flag is per-tenant. Flipping it for A must not gate B's unrelated meeting."""
    _sql(
        "INSERT INTO tenant_settings (org_id, consent_required) VALUES (%(o)s, true)",
        {"o": str(tenant_a.org_id)},
    )
    meeting_b = seed_meeting(tenant_b.org_id)
    add_participant(meeting_b, tenant_b.org_id, tenant_b.user_id)

    sign_in(tenant_b)
    resp = client.post(f"/api/meetings/{meeting_b}/token")
    assert resp.status_code == 200, resp.text


# --- the statement_timeout bound on this path (brief scope item 2) -----------------------------


async def test_statement_timeout_is_bound_and_enforced_on_the_token_mint_session(migrated_db):
    """Direct proof the exact statement the route runs actually caps query time at ~2s,
    rather than trusting that `SET LOCAL statement_timeout` was spelled correctly. Goes
    around HTTP (there is no way to make FakeTransport's mint slow - it does no I/O) and
    exercises the identical constant `routes._BIND_STATEMENT_TIMEOUT` against a real slow
    query in the same transaction shape `tenant_session` uses."""
    engine = create_async_engine(APP_URL)
    try:
        async with engine.connect() as conn, conn.begin():
            await conn.execute(routes._BIND_STATEMENT_TIMEOUT)
            with pytest.raises(Exception) as exc_info:
                await conn.execute(text("SELECT pg_sleep(2.5)"))
            assert "statement timeout" in str(exc_info.value).lower()
    finally:
        await engine.dispose()
