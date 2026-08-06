"""`POST /api/v1/chat`: Task 5, Plan 3-4 section 3.3 step 2.

Same structural pattern as `tests/test_meeting_token_route.py` and `tests/test_meetings_api.py`:
one module-scoped `TestClient`, `verify_token` overridden so no live Clerk is needed, everything
downstream real - `clerk_org_id` resolution, `app.current_org`, RLS, the actual route body.

Acceptance checks covered here (task-5 brief):
  1. happy path: persists exactly one canonical `chat_messages` row, response is that row.
  2. `Idempotency-Key` replay: the same key posted twice persists exactly one row.
  3. RLS isolation: a chat message for tenant B's meeting is invisible/unwritable from tenant A.
  4. deny-by-default: an org member who is not a participant of the meeting gets 403.
  5. the request body's `user_id` (the shape `realtime-gateway` actually sends - see
     `schemas.ChatMessageCreate`'s docstring) is accepted but ignored; the row's author is
     always the caller's own verified identity.
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
from libs.shared.errors import add_exception_handlers
from libs.shared.idempotency import IdempotencyMiddleware
from services.core_api.routes import chat
from tests.conftest import APP_URL, BOOTSTRAP_URL


class Principal:
    clerk_org = ""
    clerk_user = ""
    org_id: uuid.UUID | None = None


PRINCIPAL = Principal()


async def _tenant_resolver(request: Any) -> uuid.UUID | None:
    return PRINCIPAL.org_id


def _create_test_app(service_name: str) -> FastAPI:
    """Mirrors `libs.shared.app_factory.create_app`, except the `IdempotencyMiddleware`'s
    tenant resolver is `_tenant_resolver` (a plain lookup on `PRINCIPAL`) instead of the real
    default (`resolve_tenant_from_token`, which calls `verify_token` and `resolve_org_id`
    directly - NOT through FastAPI's `Depends`, so `app.dependency_overrides` cannot reach it,
    and there is no live Clerk here to satisfy it for real). Real Redis, the real RFC 7807
    handlers: this still proves the actual middleware's caching/replay behavior end to end,
    only substituting how *this test* resolves the tenant for a request.
    """
    app = FastAPI(title=service_name, version="1.0.0")
    redis = redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )
    app.add_middleware(IdempotencyMiddleware, redis=redis, tenant_resolver=_tenant_resolver)
    add_exception_handlers(app)
    return app


@pytest.fixture(scope="module")
def client(migrated_db):
    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app = _create_test_app("core-api-test")
    app.include_router(chat.router)
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


def seed_tenant(tier: str = "member") -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"chat-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    _sql(
        "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
        {"o": str(org_id), "u": str(user_id), "t": tier},
    )
    return Tenant(org_id, clerk_org, user_id, clerk_user)


def sign_in(tenant: Tenant) -> None:
    PRINCIPAL.clerk_org = tenant.clerk_org
    PRINCIPAL.clerk_user = tenant.clerk_user
    PRINCIPAL.org_id = tenant.org_id


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


# --- acceptance check 1: happy path -----------------------------------------------------------


def test_participant_can_post_chat_and_gets_the_canonical_row_back(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    resp = client.post(
        "/api/v1/chat",
        json={"meeting_id": str(meeting_id), "content": "hello team"},
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["meeting_id"] == str(meeting_id)
    assert body["user_id"] == str(tenant_a.user_id)
    assert body["content"] == "hello team"
    assert "id" in body and "created_at" in body

    rows = _sql(
        "SELECT org_id, meeting_id, user_id, content FROM chat_messages WHERE id = %(id)s",
        {"id": body["id"]},
    )
    assert len(rows) == 1
    assert rows[0]["org_id"] == str(tenant_a.org_id)
    assert rows[0]["meeting_id"] == str(meeting_id)
    assert rows[0]["user_id"] == str(tenant_a.user_id)
    assert rows[0]["content"] == "hello team"


# --- acceptance check 2: Idempotency-Key replay persists exactly once --------------------------


def test_same_idempotency_key_persists_exactly_one_row(client, tenant_a):
    """Full-stack proof of the acceptance check: the shared IdempotencyMiddleware (mounted by
    `create_app`) replays the cached HTTP response on the second call, so the route body never
    runs twice - exercised here as one `chat_messages` row, not just a mocked replay."""
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)
    key = str(uuid.uuid4())
    payload = {"meeting_id": str(meeting_id), "content": "only once"}

    first = client.post("/api/v1/chat", json=payload, headers={"Idempotency-Key": key})
    second = client.post("/api/v1/chat", json=payload, headers={"Idempotency-Key": key})

    assert first.status_code == 201, first.text
    assert second.status_code == 201, second.text
    assert second.headers.get("X-Idempotent-Replayed") == "true"
    assert first.json()["id"] == second.json()["id"]

    rows = _sql(
        "SELECT count(*) AS n FROM chat_messages WHERE meeting_id = %(m)s AND content = %(c)s",
        {"m": str(meeting_id), "c": "only once"},
    )
    assert rows[0]["n"] == 1, "a replayed Idempotency-Key must not persist a second row"


# --- acceptance check 3: RLS isolation ----------------------------------------------------------


def test_cross_tenant_meeting_is_404_not_403(client, tenant_a, tenant_b):
    meeting_id = seed_meeting(tenant_b.org_id)
    add_participant(meeting_id, tenant_b.org_id, tenant_b.user_id)

    sign_in(tenant_a)  # ask as org A about org B's meeting
    resp = client.post("/api/v1/chat", json={"meeting_id": str(meeting_id), "content": "leaking?"})

    assert resp.status_code == 404, "must be 404, never 403 and never 201"
    assert resp.json()["code"] == "NOT_FOUND"


def test_chat_message_is_not_readable_from_the_other_tenant(client, tenant_a, tenant_b):
    """Same proof as `test_meeting_rooms_rls.py`'s isolation test, applied to chat_messages:
    a real cross-tenant read as `mytheus_app`, via a plain `psycopg2` connection so no event
    loop nesting with the `TestClient`'s own loop is needed (matches this file's sync `_sql`
    helper for every other assertion)."""
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)
    resp = client.post(
        "/api/v1/chat", json={"meeting_id": str(meeting_id), "content": "tenant A only"}
    )
    assert resp.status_code == 201, resp.text
    message_id = resp.json()["id"]

    conn = psycopg2.connect(APP_URL.replace("+asyncpg", ""))
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT set_config('app.current_org', %s, true)", (str(tenant_b.org_id),))
            cur.execute("SELECT id FROM chat_messages WHERE id = %s", (message_id,))
            leaked = cur.fetchall()
        conn.rollback()
    finally:
        conn.close()
    assert leaked == [], "tenant B must not see tenant A's chat_messages row"


# --- acceptance check 4: deny-by-default ---------------------------------------------------------


def test_non_participant_gets_403_not_a_participant(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    # tenant_a is a member of the org but was never added as a participant of this meeting.

    resp = client.post(
        "/api/v1/chat", json={"meeting_id": str(meeting_id), "content": "can I post?"}
    )

    assert resp.status_code == 403
    assert resp.headers["content-type"].startswith("application/problem+json")
    assert resp.json()["code"] == "NOT_A_PARTICIPANT"
    assert (
        _sql(
            "SELECT count(*) AS n FROM chat_messages WHERE meeting_id = %(m)s",
            {"m": str(meeting_id)},
        )[0]["n"]
        == 0
    )


# --- acceptance check 5: body user_id is accepted but ignored -----------------------------------


def test_body_user_id_is_accepted_but_ignored(client, tenant_a, tenant_b):
    """Matches the exact shape `realtime-gateway`'s `postChat` sends
    (`{ meeting_id, content, user_id }`, see `coreApiClient.ts`) so the request does not 422,
    but the persisted author is always the caller's own verified identity - never the body's
    `user_id`, which on the wire is a Clerk `sub` string this service must not trust."""
    meeting_id = seed_meeting(tenant_a.org_id)
    add_participant(meeting_id, tenant_a.org_id, tenant_a.user_id)

    resp = client.post(
        "/api/v1/chat",
        json={
            "meeting_id": str(meeting_id),
            "content": "spoof attempt",
            "user_id": "user_someClerkSubString",  # not even a UUID; must not 422 either
        },
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["user_id"] == str(tenant_a.user_id)
