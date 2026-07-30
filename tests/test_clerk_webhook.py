"""Clerk Svix webhook user-sync and the reconciliation sweep. Plan section 3.5 acceptance.

The webhook tests sign real Svix payloads with a real secret, so signature verification is
genuinely exercised rather than stubbed. Only Clerk's delivery is simulated.
"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.testclient import TestClient
from svix.webhooks import Webhook

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.reconciliation import ClerkUser, reconcile_tenant
from services.identity_tenant_service.routes import auth as auth_routes
from tests.conftest import APP_URL, BOOTSTRAP_URL

# A syntactically valid Svix secret. Meaningless outside this test.
SECRET = "whsec_" + "A" * 32


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


def _seed_org() -> tuple[uuid.UUID, str]:
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"wh-{org_id}", "cid": clerk_org},
    )
    return org_id, clerk_org


@pytest.fixture(scope="module")
def client(migrated_db):
    import os

    previous = os.environ.get("DATABASE_URL")
    os.environ["DATABASE_URL"] = APP_URL
    os.environ["CLERK_WEBHOOK_SECRET"] = SECRET
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()

    app: FastAPI = create_app("identity-test")
    app.include_router(auth_routes.router)
    with TestClient(app) as c:
        yield c

    if previous is None:
        os.environ.pop("DATABASE_URL", None)
    else:
        os.environ["DATABASE_URL"] = previous
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


def _user_event(clerk_org: str, clerk_user: str, event_type: str, email: str) -> dict[str, Any]:
    return {
        "type": event_type,
        "data": {
            "id": clerk_user,
            "primary_email_address_id": "idn_1",
            "email_addresses": [{"id": "idn_1", "email_address": email}],
            "image_url": "https://img.clerk.com/x",
            "organization_memberships": [{"organization": {"id": clerk_org}}],
        },
    }


def _sign(msg_id: str, body: bytes) -> tuple[str, str]:
    """Sign a body the way Svix does. Returns (timestamp header, signature header).

    `Webhook.sign` takes a datetime, while the wire header is epoch seconds, so the two are
    produced together here to keep them consistent.
    """
    now = datetime.now(UTC)
    signature = Webhook(SECRET).sign(msg_id, now, body.decode())
    return str(int(now.timestamp())), signature


def _post(client: TestClient, payload: dict[str, Any], *, msg_id: str | None = None):
    """Sign the payload the way Svix does and deliver it."""
    body = json.dumps(payload).encode()
    msg_id = msg_id or f"msg_{uuid.uuid4().hex}"
    timestamp, signature = _sign(msg_id, body)
    return client.post(
        "/clerk/webhook",
        content=body,
        headers={
            "svix-id": msg_id,
            "svix-timestamp": timestamp,
            "svix-signature": signature,
            "content-type": "application/json",
        },
    )


# --- signature ------------------------------------------------------------------------------


def test_unsigned_webhook_is_401(client):
    """Plan 3.5 acceptance 1, spoofing half. Verification happens before any parsing."""
    resp = client.post(
        "/clerk/webhook",
        content=b"{}",
        headers={
            "svix-id": "msg_1",
            "svix-timestamp": str(int(time.time())),
            "svix-signature": "v1,deadbeef",
        },
    )
    assert resp.status_code == 401
    assert resp.json()["code"] == "AUTH_REQUIRED"


def test_a_valid_signature_over_tampered_content_is_rejected(client):
    """The signature covers the body, so changing the body after signing must invalidate it."""
    org_id, clerk_org = _seed_org()
    body = json.dumps(_user_event(clerk_org, "user_x", "user.created", "a@b.co")).encode()
    msg_id = f"msg_{uuid.uuid4().hex}"
    timestamp, signature = _sign(msg_id, body)

    tampered = body.replace(b"a@b.co", b"attacker@evil.co")
    resp = client.post(
        "/clerk/webhook",
        content=tampered,
        headers={
            "svix-id": msg_id,
            "svix-timestamp": timestamp,
            "svix-signature": signature,
            "content-type": "application/json",
        },
    )
    assert resp.status_code == 401


# --- apply ----------------------------------------------------------------------------------


def test_user_created_writes_a_tenant_scoped_row(client):
    """Plan 3.5 acceptance 1, happy half."""
    org_id, clerk_org = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"

    resp = _post(client, _user_event(clerk_org, clerk_user, "user.created", "new@example.com"))
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "ok"

    rows = _sql(
        "SELECT org_id, email, avatar_url FROM users WHERE clerk_user_id = %(c)s",
        {"c": clerk_user},
    )
    assert len(rows) == 1
    assert str(rows[0]["org_id"]) == str(org_id)
    assert rows[0]["email"] == "new@example.com"

    audit_rows = _sql(
        "SELECT action, actor_type FROM audit_logs WHERE org_id = %(o)s", {"o": str(org_id)}
    )
    assert audit_rows[0]["action"] == "user.synced"
    assert audit_rows[0]["actor_type"] == "system"


def test_redelivery_of_the_same_message_is_a_no_op(client):
    """Plan 3.5 acceptance 2. Clerk retries; the second delivery must change nothing."""
    org_id, clerk_org = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    payload = _user_event(clerk_org, clerk_user, "user.created", "dupe@example.com")
    msg_id = f"msg_{uuid.uuid4().hex}"

    first = _post(client, payload, msg_id=msg_id)
    second = _post(client, payload, msg_id=msg_id)

    assert first.json()["status"] == "ok"
    assert second.json()["status"] == "duplicate"
    assert len(_sql("SELECT 1 FROM users WHERE clerk_user_id = %(c)s", {"c": clerk_user})) == 1
    # Exactly one inbox record, and exactly one audit row: the replay wrote nothing.
    assert len(_sql("SELECT 1 FROM inbox WHERE idempotency_key = %(k)s", {"k": msg_id})) == 1
    assert len(_sql("SELECT 1 FROM audit_logs WHERE org_id = %(o)s", {"o": str(org_id)})) == 1


def test_a_later_update_for_the_same_user_is_applied(client):
    """Dedupe is on the SVIX MESSAGE id, not the user id.

    Keyed on the user id, this second, legitimately different event would have been dropped
    as a duplicate and the mirror would never see another update for that person again.
    """
    org_id, clerk_org = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"

    _post(client, _user_event(clerk_org, clerk_user, "user.created", "before@example.com"))
    _post(client, _user_event(clerk_org, clerk_user, "user.updated", "after@example.com"))

    rows = _sql("SELECT email FROM users WHERE clerk_user_id = %(c)s", {"c": clerk_user})
    assert rows[0]["email"] == "after@example.com"


def test_user_deleted_soft_deletes(client):
    org_id, clerk_org = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _post(client, _user_event(clerk_org, clerk_user, "user.created", "gone@example.com"))
    _post(client, _user_event(clerk_org, clerk_user, "user.deleted", "gone@example.com"))

    rows = _sql("SELECT deleted_at FROM users WHERE clerk_user_id = %(c)s", {"c": clerk_user})
    assert len(rows) == 1, "a hard delete would break created_by and every audit reference"
    assert rows[0]["deleted_at"] is not None


def test_the_same_person_syncs_into_two_organizations(client):
    """The per-tenant unique makes dual membership representable; a global one did not."""
    _org_a, clerk_org_a = _seed_org()
    _org_b, clerk_org_b = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"

    for clerk_org in (clerk_org_a, clerk_org_b):
        resp = _post(client, _user_event(clerk_org, clerk_user, "user.created", "d@x.co"))
        assert resp.status_code == 200, resp.text

    rows = _sql("SELECT org_id FROM users WHERE clerk_user_id = %(c)s", {"c": clerk_user})
    assert len(rows) == 2


def test_a_user_with_no_organization_is_ignored_not_retried(client):
    """A personal account has no tenant. 200, because a retry will never grow one."""
    payload = {
        "type": "user.created",
        "data": {
            "id": "user_personal",
            "email_addresses": [{"id": "i", "email_address": "solo@example.com"}],
            "primary_email_address_id": "i",
            "organization_memberships": [],
        },
    }
    resp = _post(client, payload)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ignored"


def test_an_unknown_organization_is_refused(client):
    """The resolver is the checkpoint here too: a webhook for a tenant we do not have is 401."""
    payload = _user_event(f"org_{uuid.uuid4().hex}", "user_ghost", "user.created", "g@x.co")
    assert _post(client, payload).status_code == 401


# --- reconciliation ---------------------------------------------------------------------------


async def test_reconciliation_creates_missing_and_soft_deletes_departed(migrated_db):
    """Plan 3.5 acceptance 3, both directions."""
    org_id, _clerk_org = _seed_org()
    stale = f"user_{uuid.uuid4().hex[:16]}"
    fresh = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (org_id, clerk_user_id, email) "
        "VALUES (%(o)s, %(c)s, 'stale@example.com')",
        {"o": str(org_id), "c": stale},
    )

    sessions = async_sessionmaker(create_async_engine(APP_URL), expire_on_commit=False)
    async with sessions() as session, session.begin():
        result = await reconcile_tenant(
            session, org_id, [ClerkUser(clerk_user_id=fresh, email="fresh@example.com")]
        )

    assert result.created == [fresh]
    assert result.soft_deleted == [stale]
    assert (
        _sql("SELECT deleted_at FROM users WHERE clerk_user_id = %(c)s", {"c": stale})[0][
            "deleted_at"
        ]
        is not None
    )
    assert (
        _sql("SELECT email FROM users WHERE clerk_user_id = %(c)s", {"c": fresh})[0]["email"]
        == "fresh@example.com"
    )


async def test_reconciliation_is_idempotent(migrated_db):
    """A diff, not a replay: the second run must find nothing left to do."""
    org_id, _clerk_org = _seed_org()
    roster = [ClerkUser(clerk_user_id=f"user_{uuid.uuid4().hex[:16]}", email="a@example.com")]

    sessions = async_sessionmaker(create_async_engine(APP_URL), expire_on_commit=False)
    async with sessions() as session, session.begin():
        first = await reconcile_tenant(session, org_id, roster)
    async with sessions() as session, session.begin():
        second = await reconcile_tenant(session, org_id, roster)

    assert first.changed
    assert not second.changed, "a second sweep over an unchanged roster must be a no-op"


async def test_reconciliation_leaves_matched_users_alone(migrated_db):
    """Membership is this sweep's business; field edits are the webhook's.

    Overwriting here would let a slow hourly sweep silently revert a just-delivered webhook
    update, with the winner decided by timing.
    """
    org_id, _clerk_org = _seed_org()
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO users (org_id, clerk_user_id, email) "
        "VALUES (%(o)s, %(c)s, 'webhook-just-set-this@example.com')",
        {"o": str(org_id), "c": clerk_user},
    )

    sessions = async_sessionmaker(create_async_engine(APP_URL), expire_on_commit=False)
    async with sessions() as session, session.begin():
        result = await reconcile_tenant(
            session, org_id, [ClerkUser(clerk_user_id=clerk_user, email="stale-roster@example.com")]
        )

    assert not result.changed
    assert (
        _sql("SELECT email FROM users WHERE clerk_user_id = %(c)s", {"c": clerk_user})[0]["email"]
        == "webhook-just-set-this@example.com"
    )
