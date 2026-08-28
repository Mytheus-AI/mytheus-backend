"""`GET`/`PATCH /tenant-settings`: the write path for `tenant_settings.pii_redaction_enabled`.

The load-bearing assertion in this file is NOT that the column changes. It is that the Redis
key `nexus:{org_id}:tenant_settings:pii_redaction_enabled` changes, because that key, and not
the column, is what `ingestion-service` reads before deciding whether to redact. A test that
only checked the column would pass against the exact bug this route exists to fix: a redaction
gate no tenant could ever turn on.

Real Postgres and real Redis on both paths. `fakeredis` is deliberately not used for the mirror
assertions: the point is that the value lands in the store the other repo connects to.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg2
import psycopg2.extras
import pytest
import redis as redis_sync
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod
from libs.shared import cache as cache_mod
from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.identity_tenant_service.routes import tenant_settings as ts_routes
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
    # Same reason the two engine caches are cleared, and easy to miss because it only bites in
    # a full-suite run: `get_redis` is `lru_cache(maxsize=1)`, so the async client belongs to
    # whichever TestClient's event loop built it first. This module's client runs its own loop,
    # and reusing a connection whose transport is attached to a closed loop fails inside
    # `send_packed_command` rather than at connect time, which reads as a Redis outage instead
    # of as loop reuse. Cleared on the way in and on the way out so neither direction leaks.
    cache_mod.get_redis.cache_clear()

    app = create_app("identity-tenant-service-test")
    app.include_router(ts_routes.router)
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
    cache_mod.get_redis.cache_clear()


def _sql(query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    conn = psycopg2.connect(BOOTSTRAP_URL.replace("+asyncpg", ""))
    conn.autocommit = True
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or {})
            return [dict(r) for r in cur.fetchall()] if cur.description else []
    finally:
        conn.close()


@pytest.fixture(scope="module")
def real_redis():
    """The same Redis the app writes to, read synchronously. Skips rather than silently
    passing if it is unreachable: a green run that never checked the mirror is worthless here."""
    import os

    url = os.environ.get("REDIS_URL", "redis://localhost:6380")
    r = redis_sync.from_url(url)
    try:
        r.ping()
    except redis_sync.RedisError:
        pytest.skip(f"real Redis not reachable at {url}; the mirror assertions need it")
    return r


class Tenant:
    def __init__(self, org_id, clerk_org, user_id, clerk_user):
        self.org_id = org_id
        self.clerk_org = clerk_org
        self.user_id = user_id
        self.clerk_user = clerk_user


def seed_tenant(tier: str = "admin") -> Tenant:
    org_id, user_id = uuid.uuid4(), uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    clerk_user = f"user_{uuid.uuid4().hex[:16]}"
    _sql(
        "INSERT INTO organizations (id, name, slug, clerk_org_id) "
        "VALUES (%(id)s, 'Org', %(slug)s, %(cid)s)",
        {"id": str(org_id), "slug": f"ts-{org_id}", "cid": clerk_org},
    )
    _sql(
        "INSERT INTO users (id, org_id, clerk_user_id, email) "
        "VALUES (%(uid)s, %(org)s, %(cuid)s, 'u@example.com')",
        {"uid": str(user_id), "org": str(org_id), "cuid": clerk_user},
    )
    if tier:
        _sql(
            "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (%(o)s, %(u)s, %(t)s)",
            {"o": str(org_id), "u": str(user_id), "t": tier},
        )
    return Tenant(org_id, clerk_org, user_id, clerk_user)


def sign_in(t: Tenant) -> None:
    PRINCIPAL.clerk_org = t.clerk_org
    PRINCIPAL.clerk_user = t.clerk_user


def mirror_key(org_id) -> str:
    return f"nexus:{org_id}:tenant_settings:pii_redaction_enabled"


@pytest.fixture
def admin():
    t = seed_tenant(tier="admin")
    sign_in(t)
    return t


# --- the gap this route closes ---------------------------------------------------------------


def test_enabling_writes_the_redis_key_ingestion_actually_reads(client, admin, real_redis):
    """The whole point. `ingestion-service/pii.py` reads this exact key and accepts b"1"/b"true".

    Before this route existed, nothing in the program wrote it, so the redaction layer that
    shipped in Plan 5-6 was unreachable for every tenant.
    """
    real_redis.delete(mirror_key(admin.org_id))

    resp = client.patch("/tenant-settings", json={"pii_redaction_enabled": True})
    assert resp.status_code == 200, resp.text
    assert resp.json()["pii_redaction_enabled"] is True

    assert real_redis.get(mirror_key(admin.org_id)) in (b"1", b"true")


def test_the_mirror_value_satisfies_the_readers_own_predicate(client, admin, real_redis):
    """Asserted against `ingestion-service`'s literal acceptance set rather than against "1".

    The two repos cannot import each other, so this predicate is copied. If the reader's set
    ever changes, this test is the thing that should fail.
    """
    reader_accepts_as_true = (b"1", "1", b"true", "true")

    client.patch("/tenant-settings", json={"pii_redaction_enabled": True})
    assert real_redis.get(mirror_key(admin.org_id)) in reader_accepts_as_true

    client.patch("/tenant-settings", json={"pii_redaction_enabled": False})
    assert real_redis.get(mirror_key(admin.org_id)) not in reader_accepts_as_true


def test_upsert_works_for_an_org_with_no_settings_row(client, admin, real_redis):
    """`provision_organization()` creates no `tenant_settings` row, so every org that exists
    today has none. An UPDATE would have affected zero rows and reported success."""
    assert (
        _sql("SELECT 1 FROM tenant_settings WHERE org_id = %(o)s", {"o": str(admin.org_id)}) == []
    )

    resp = client.patch("/tenant-settings", json={"pii_redaction_enabled": False})
    assert resp.status_code == 200, resp.text

    rows = _sql(
        "SELECT pii_redaction_enabled FROM tenant_settings WHERE org_id = %(o)s",
        {"o": str(admin.org_id)},
    )
    assert rows and rows[0]["pii_redaction_enabled"] is False


def test_column_and_mirror_agree_after_a_write(client, admin, real_redis):
    client.patch("/tenant-settings", json={"pii_redaction_enabled": True})

    column = _sql(
        "SELECT pii_redaction_enabled FROM tenant_settings WHERE org_id = %(o)s",
        {"o": str(admin.org_id)},
    )[0]["pii_redaction_enabled"]
    mirror_is_on = real_redis.get(mirror_key(admin.org_id)) in (b"1", b"true")

    assert column is True and mirror_is_on, "the column and the mirror must not diverge"


# --- tenancy and authz ------------------------------------------------------------------------


def test_one_tenants_write_does_not_touch_anothers_mirror(client, real_redis):
    a, b = seed_tenant("admin"), seed_tenant("admin")
    real_redis.delete(mirror_key(a.org_id))
    real_redis.delete(mirror_key(b.org_id))

    sign_in(a)
    client.patch("/tenant-settings", json={"pii_redaction_enabled": True})

    assert real_redis.get(mirror_key(a.org_id)) in (b"1", b"true")
    assert real_redis.get(mirror_key(b.org_id)) is None, "B's mirror must be untouched"

    rows = _sql(
        "SELECT pii_redaction_enabled FROM tenant_settings WHERE org_id = %(o)s",
        {"o": str(b.org_id)},
    )
    assert rows == [], "B must have no settings row written by A's request"


def test_member_tier_cannot_change_a_compliance_setting(client):
    """Turning redaction off is a compliance decision, so it is admin and above. A member
    getting 403 here is the difference between a profile edit and a policy change."""
    sign_in(seed_tenant(tier="member"))
    resp = client.patch("/tenant-settings", json={"pii_redaction_enabled": False})
    assert resp.status_code == 403, resp.text


def test_read_reports_the_column_default_when_no_row_exists(client, admin):
    """Not a 404. An org with no row still HAS an effective value, because the column carries a
    default the next insert will apply, and a reader needs to see that rather than an error.

    Worth reading alongside the divergence this route was written to expose: Postgres defaults
    `pii_redaction_enabled` to TRUE, while `ingestion-service` treats a missing Redis key as
    FALSE. Until a tenant is explicitly written, those two disagree about every organization.
    """
    resp = client.get("/tenant-settings")
    assert resp.status_code == 200, resp.text
    assert resp.json()["pii_redaction_enabled"] is True


def test_no_org_id_is_accepted_from_the_caller(client, admin):
    """`org_id` is derived from the verified token. There is no path or query parameter for it,
    so a caller cannot name a tenant; the route's signature is the enforcement."""
    other = uuid.uuid4()
    resp = client.patch(f"/tenant-settings?org_id={other}", json={"pii_redaction_enabled": True})
    assert resp.status_code == 200, resp.text

    assert (
        _sql("SELECT 1 FROM tenant_settings WHERE org_id = %(o)s", {"o": str(other)}) == []
    ), "a query parameter must never select the tenant"


# --- audit -------------------------------------------------------------------------------------


def test_the_change_is_audited_with_the_value_that_was_set(client, admin):
    client.patch("/tenant-settings", json={"pii_redaction_enabled": False})

    rows = _sql(
        "SELECT action, actor_id, target_type, metadata FROM audit_logs "
        "WHERE org_id = %(o)s AND action = %(a)s",
        {"o": str(admin.org_id), "a": ts_routes.ACTION_TENANT_SETTINGS_UPDATED},
    )
    assert len(rows) == 1, rows
    assert rows[0]["target_type"] == "tenant_settings"
    # str() on both sides: psycopg2 hands back `uuid` columns as text here.
    assert str(rows[0]["actor_id"]) == str(admin.user_id)
    assert rows[0]["metadata"]["pii_redaction_enabled"] is False


def test_a_failed_mirror_write_rolls_the_column_back(client, admin, monkeypatch):
    """The consistency property the route is ordered around.

    The mirror write is the last statement inside the transaction, so if it raises, the column
    write rolls back with it and the two stores cannot disagree. Without this the route would
    report a setting as saved while ingestion kept using the old one.
    """

    class DeadRedis:
        async def set(self, *_args, **_kwargs):
            raise redis_sync.RedisError("simulated Redis outage")

    # Patch the CLIENT, not `_publish_pii_mirror` itself, so the real function runs and its
    # RedisError-to-503 conversion is exercised rather than skipped.
    monkeypatch.setattr(ts_routes, "get_redis", lambda: DeadRedis())

    resp = client.patch("/tenant-settings", json={"pii_redaction_enabled": False})
    assert resp.status_code == 503, resp.text
    body = resp.json()
    assert body["code"] == "DEPENDENCY_UNAVAILABLE", body
    assert "NOT" in body["detail"], "the caller must be told the setting is not in effect"

    assert (
        _sql("SELECT 1 FROM tenant_settings WHERE org_id = %(o)s", {"o": str(admin.org_id)}) == []
    ), "the column write must not survive a failed mirror write"
