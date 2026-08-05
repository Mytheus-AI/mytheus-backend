"""`PATCH /api/v1/meetings/{id}/speakers/{speaker_id}`: Task 5, Plan 3-4 section 3.3 step 3.

Same structural pattern as `tests/test_meeting_token_route.py`.

Acceptance checks covered here (task-5 brief):
  1. the route flips `assignment` to `manual` and pins `user_id`, whether or not a "suggested"
     row already existed for that `speaker_id` (the upsert case).
  2. R7, the structural half: once a row is `manual`, no UPDATE - not just this route's, ANY
     UPDATE - can move it back to `suggested`. Proven directly against the database trigger,
     not just through this one route, since the whole point is that it holds regardless of
     which future writer attempts it.
  3. RLS isolation.
  4. cross-tenant meeting is 404.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.core_api.routes import speakers
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
    app.include_router(speakers.router)
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
        {"id": str(org_id), "slug": f"spk-{org_id}", "cid": clerk_org},
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


def seed_meeting(org_id: uuid.UUID) -> uuid.UUID:
    meeting_id = uuid.uuid4()
    _sql(
        "INSERT INTO meetings (id, org_id, title) VALUES (%(id)s, %(org)s, 'M')",
        {"id": str(meeting_id), "org": str(org_id)},
    )
    return meeting_id


@pytest.fixture
def tenant_a():
    t = seed_tenant()
    sign_in(t)
    return t


@pytest.fixture
def tenant_b():
    return seed_tenant()


# --- acceptance check 1: upsert, manual, pins user_id -------------------------------------------


def test_patch_creates_a_manual_row_when_none_existed(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    other_user = uuid.uuid4()
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(id)s, %(org)s, %(cuid)s, 'sp@example.com')",
        {"id": str(other_user), "org": str(tenant_a.org_id), "cuid": f"user_{other_user.hex}"},
    )

    resp = client.patch(
        f"/api/v1/meetings/{meeting_id}/speakers/0",
        json={"user_id": str(other_user)},
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["speaker_id"] == "0"
    assert body["user_id"] == str(other_user)
    assert body["assignment"] == "manual"

    rows = _sql(
        "SELECT user_id, assignment FROM meeting_speakers "
        "WHERE meeting_id = %(m)s AND speaker_id = '0'",
        {"m": str(meeting_id)},
    )
    assert len(rows) == 1
    assert rows[0]["user_id"] == str(other_user)
    assert rows[0]["assignment"] == "manual"


def test_patch_flips_an_existing_suggested_row_to_manual(client, tenant_a):
    """The row a diarization suggestion would have produced, confirmed by a human."""
    meeting_id = seed_meeting(tenant_a.org_id)
    guessed_user = uuid.uuid4()
    confirmed_user = uuid.uuid4()
    for uid, suffix in ((guessed_user, "g"), (confirmed_user, "c")):
        _sql(
            "INSERT INTO users (id, org_id, clerk_user_id, email) "
            "VALUES (%(id)s, %(org)s, %(cuid)s, 'x@example.com')",
            {"id": str(uid), "org": str(tenant_a.org_id), "cuid": f"user_{uid.hex}_{suffix}"},
        )
    _sql(
        "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id, confidence, "
        "assignment) VALUES (%(o)s, %(m)s, '1', %(u)s, 0.62, 'suggested')",
        {"o": str(tenant_a.org_id), "m": str(meeting_id), "u": str(guessed_user)},
    )

    resp = client.patch(
        f"/api/v1/meetings/{meeting_id}/speakers/1",
        json={"user_id": str(confirmed_user)},
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["user_id"] == str(confirmed_user)
    assert body["assignment"] == "manual"

    rows = _sql(
        "SELECT count(*) AS n FROM meeting_speakers "
        "WHERE meeting_id = %(m)s AND speaker_id = '1'",
        {"m": str(meeting_id)},
    )
    assert rows[0]["n"] == 1, "the upsert must not create a second row for the same speaker_id"


def test_unknown_user_id_is_a_422_not_a_500(client, tenant_a):
    meeting_id = seed_meeting(tenant_a.org_id)
    resp = client.patch(
        f"/api/v1/meetings/{meeting_id}/speakers/0",
        json={"user_id": str(uuid.uuid4())},  # no such user anywhere
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["code"] == "VALIDATION_FAILED"


# --- acceptance check 2: R7 - manual is structurally authoritative -------------------------------


async def test_database_trigger_refuses_to_downgrade_a_manual_assignment(migrated_db):
    """Direct proof against the database, independent of any route: an UPDATE attempting to
    move a `manual` row back to `suggested` - the shape an automatic diarization-suggestion
    writer would produce if it raced or ignored a manual assignment - is refused by
    `meeting_speakers_manual_is_authoritative`, not merely discouraged by convention."""
    su = create_async_engine(migrated_db)
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    meeting_id = uuid.uuid4()
    async with su.begin() as conn:
        await conn.execute(
            text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
            {"id": str(org_id), "slug": f"trg-{org_id}"},
        )
        await conn.execute(
            text(
                "INSERT INTO users (id, org_id, clerk_user_id, email) "
                "VALUES (:id, :org, :cuid, 'a@b.co')"
            ),
            {"id": str(user_id), "org": str(org_id), "cuid": f"user_{user_id}"},
        )
        await conn.execute(
            text("INSERT INTO meetings (id, org_id) VALUES (:id, :org)"),
            {"id": str(meeting_id), "org": str(org_id)},
        )
        await conn.execute(
            text(
                "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id, "
                "assignment) VALUES (:org, :meeting, '0', :user, 'manual')"
            ),
            {"org": str(org_id), "meeting": str(meeting_id), "user": str(user_id)},
        )

    try:
        with pytest.raises(DBAPIError) as exc_info:
            async with su.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE meeting_speakers SET assignment = 'suggested', confidence = 0.4 "
                        "WHERE org_id = :org AND meeting_id = :meeting AND speaker_id = '0'"
                    ),
                    {"org": str(org_id), "meeting": str(meeting_id)},
                )
        assert "cannot downgrade a manual assignment" in str(exc_info.value)

        # And the row is genuinely untouched - the trigger raised before the write landed.
        async with su.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT assignment FROM meeting_speakers "
                            "WHERE org_id = :org AND meeting_id = :meeting AND speaker_id = '0'"
                        ),
                        {"org": str(org_id), "meeting": str(meeting_id)},
                    )
                )
                .mappings()
                .one()
            )
        assert row["assignment"] == "manual"
    finally:
        await su.dispose()


async def test_database_trigger_allows_manual_to_manual_reassignment(migrated_db):
    """The trigger only refuses leaving `manual`, not re-confirming a different user under an
    already-manual row - the PATCH route's own repeat-call case."""
    su = create_async_engine(migrated_db)
    org_id, user_a, user_b = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    meeting_id = uuid.uuid4()
    async with su.begin() as conn:
        await conn.execute(
            text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
            {"id": str(org_id), "slug": f"trg2-{org_id}"},
        )
        for uid in (user_a, user_b):
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email) "
                    "VALUES (:id, :org, :cuid, 'a@b.co')"
                ),
                {"id": str(uid), "org": str(org_id), "cuid": f"user_{uid}"},
            )
        await conn.execute(
            text("INSERT INTO meetings (id, org_id) VALUES (:id, :org)"),
            {"id": str(meeting_id), "org": str(org_id)},
        )
        await conn.execute(
            text(
                "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id, "
                "assignment) VALUES (:org, :meeting, '0', :user, 'manual')"
            ),
            {"org": str(org_id), "meeting": str(meeting_id), "user": str(user_a)},
        )

    try:
        async with su.begin() as conn:
            await conn.execute(
                text(
                    "UPDATE meeting_speakers SET user_id = :user "
                    "WHERE org_id = :org AND meeting_id = :meeting AND speaker_id = '0'"
                ),
                {"org": str(org_id), "meeting": str(meeting_id), "user": str(user_b)},
            )
        async with su.connect() as conn:
            row = (
                (
                    await conn.execute(
                        text(
                            "SELECT user_id, assignment FROM meeting_speakers "
                            "WHERE org_id = :org AND meeting_id = :meeting AND speaker_id = '0'"
                        ),
                        {"org": str(org_id), "meeting": str(meeting_id)},
                    )
                )
                .mappings()
                .one()
            )
        assert row["user_id"] == user_b
        assert row["assignment"] == "manual"
    finally:
        await su.dispose()


# --- acceptance checks 3-4: RLS + cross-tenant 404 ------------------------------------------------


def test_cross_tenant_meeting_is_404_not_403(client, tenant_a, tenant_b):
    meeting_id = seed_meeting(tenant_b.org_id)

    sign_in(tenant_a)
    resp = client.patch(
        f"/api/v1/meetings/{meeting_id}/speakers/0",
        json={"user_id": str(tenant_a.user_id)},
    )

    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_meeting_speakers_is_isolated_between_tenants(migrated_db):
    su = create_async_engine(migrated_db)
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    user_a = uuid.uuid4()
    meeting_a = uuid.uuid4()
    async_setup = su

    async def _seed() -> None:
        async with async_setup.begin() as conn:
            for org, slug in ((org_a, "spk-rls-a"), (org_b, "spk-rls-b")):
                await conn.execute(
                    text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
                    {"id": str(org), "slug": f"{slug}-{org}"},
                )
            await conn.execute(
                text(
                    "INSERT INTO users (id, org_id, clerk_user_id, email) "
                    "VALUES (:id, :org, :cuid, 'a@b.co')"
                ),
                {"id": str(user_a), "org": str(org_a), "cuid": f"user_{user_a}"},
            )
            await conn.execute(
                text("INSERT INTO meetings (id, org_id) VALUES (:id, :org)"),
                {"id": str(meeting_a), "org": str(org_a)},
            )
            await conn.execute(
                text(
                    "INSERT INTO meeting_speakers (org_id, meeting_id, speaker_id, user_id, "
                    "assignment) VALUES (:org, :meeting, '0', :user, 'manual')"
                ),
                {"org": str(org_a), "meeting": str(meeting_a), "user": str(user_a)},
            )

    import asyncio

    asyncio.run(_seed())
    asyncio.run(su.dispose())

    app_engine = create_async_engine(APP_URL)

    async def _counts() -> tuple[int, int]:
        async with app_engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_a)}
            )
            mine = (await conn.execute(text("SELECT count(*) FROM meeting_speakers"))).scalar_one()
        async with app_engine.connect() as conn, conn.begin():
            await conn.execute(
                text("SELECT set_config('app.current_org', :o, true)"), {"o": str(org_b)}
            )
            leaked = (
                await conn.execute(text("SELECT count(*) FROM meeting_speakers"))
            ).scalar_one()
        return mine, leaked

    mine, leaked = asyncio.run(_counts())
    asyncio.run(app_engine.dispose())
    assert mine >= 1
    assert leaked == 0
