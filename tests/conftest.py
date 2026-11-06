"""Shared fixtures for the backend tests.

Tests run against the local dev stack: an isolated test Postgres (default localhost:5443), the
compose Redpanda (localhost:9092 / :8081), and fakeredis for Redis-only unit tests. Env is set
here BEFORE any app module imports. Tests that need a service they cannot reach skip gracefully.

Roles. Section 3.3 replaced the single superuser connection (defect D1) with three
least-privilege roles, none holding `rolsuper` or `rolbypassrls`:

- `mytheus_app`    the request path. Subject to every `tenant_isolation` policy.
- `mytheus_relay`  the outbox drain, and nothing else. One permissive policy on `outbox`.
- `mytheus_admin`  the operator seam (seed script, RTBF purge). Not reachable from a handler.

`BOOTSTRAP_URL` is the privileged credential that owns the schema and runs migrations. It is
test scaffolding and the deliberate exception: fixtures use it to seed cross-tenant rows that
no application role is allowed to write. Assertions about isolation must use a role URL, never
this one, because proving isolation for a role the application never uses proves nothing. That
was exactly the flaw in the retired `app_rls` role.
"""

from __future__ import annotations

import os

import psycopg2
import pytest

_DEFAULT_BOOTSTRAP = "postgresql+asyncpg://postgres:postgres@localhost:5443/mytheus"

# Developer-.env values that must never reach a test. A sourced `.env` sets real upstream
# addresses (the gateway proxy would route to them instead of the compose names the tests
# assert), the internal audio token, and Clerk keys (a real CLERK_SECRET_KEY means real Clerk
# calls). They are stripped once here, at import, before any app module reads them, and again
# per test by `_isolate_developer_env` in case anything re-injects the developer's value. A
# test that needs one sets it itself (monkeypatch.setenv, or a fixture), and that wins.
LEAKY_ENV_VARS = (
    "UPSTREAM_BFF",
    "UPSTREAM_IDENTITY_TENANT_SERVICE",
    "UPSTREAM_CORE_API",
    "UPSTREAM_MEETING_SERVICE",
    "INTERNAL_AUDIO_TOKEN",
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "CLERK_SECRET_KEY",
    "CLERK_WEBHOOK_SECRET",
)
_DEVELOPER_ENV = {k: os.environ.pop(k) for k in LEAKY_ENV_VARS if k in os.environ}


@pytest.fixture(autouse=True)
def _isolate_developer_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove a developer-.env value if it is present again; keep any value a test set."""
    for key, leaked in _DEVELOPER_ENV.items():
        if os.environ.get(key) == leaked:
            monkeypatch.delenv(key)


os.environ.setdefault("REDPANDA_BROKERS", "localhost:9092")
os.environ.setdefault("SCHEMA_REGISTRY_URL", "http://localhost:8085")
os.environ.setdefault("REDIS_URL", "redis://localhost:6390")

# The schema owner, read from BOOTSTRAP_DATABASE_URL and NOT from DATABASE_URL. Since
# section 3.3, `DATABASE_URL` names `mytheus_app` in any correctly configured environment,
# and that role deliberately cannot ALTER a role, create a tenant root, or write across
# tenants - all of which the fixtures below must do. Deriving the owner from it would leave
# the whole database suite erroring for any developer whose `.env` is right, while passing in
# CI where only `DATABASE_URL` happens to be set to the owner.
#
# Falls back to DATABASE_URL and then to the compose default so a bare checkout still runs.
BOOTSTRAP_URL = (
    os.environ.get("BOOTSTRAP_DATABASE_URL") or os.environ.get("DATABASE_URL") or _DEFAULT_BOOTSTRAP
)
os.environ.setdefault("DATABASE_URL", BOOTSTRAP_URL)
_SYNC_URL = BOOTSTRAP_URL.replace("+asyncpg", "")

# Dev-only passwords. The migration creates these roles with no password (so they cannot
# authenticate until an operator sets one); scripts/bootstrap_roles.py does it for a real
# environment from the secrets manager. Here the fixture does it directly.
ROLE_PASSWORDS = {
    "mytheus_app": "mytheus_app",
    "mytheus_relay": "mytheus_relay",
    "mytheus_admin": "mytheus_admin",
}


def _role_url(role: str) -> str:
    """Swap the credential in the bootstrap URL for a role's own.

    Parsed rather than string-replaced, because the owner credential is not necessarily
    `postgres:postgres`: a `.env` pointing at a real database would have left every role URL
    silently unchanged and every isolation test running as the owner, which is the exact
    class of false green that defect D1 was.
    """
    scheme, _, rest = BOOTSTRAP_URL.partition("://")
    _credentials, _, host_and_db = rest.rpartition("@")
    return f"{scheme}://{role}:{ROLE_PASSWORDS[role]}@{host_and_db}"


APP_URL = _role_url("mytheus_app")
RELAY_URL = _role_url("mytheus_relay")
ADMIN_URL = _role_url("mytheus_admin")


async def seed_org(engine: object, org_id: object, *, slug_prefix: str = "t") -> None:
    """Insert a tenant root as the schema owner.

    Since defect D6 was fixed, `org_id` on every tenant table carries a foreign key to
    `organizations`, so a test can no longer conjure rows for a tenant that does not exist.
    Every fixture that seeds tenant data needs its root first.
    """
    from sqlalchemy import text as _text

    async with engine.begin() as conn:  # type: ignore[attr-defined]
        await conn.execute(
            _text("INSERT INTO organizations (id, name, slug) VALUES (:id, 'Org', :slug)"),
            {"id": str(org_id), "slug": f"{slug_prefix}-{org_id}"},
        )


def _db_reachable() -> bool:
    try:
        psycopg2.connect(_SYNC_URL, connect_timeout=2).close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def migrated_db() -> str:
    """Apply migrations once and give the three roles a dev password.

    Returns the BOOTSTRAP url (schema owner). Skips the whole session's DB tests if the DB is
    unreachable, rather than reporting green on tests that never ran.
    """
    if not _db_reachable():
        # In CI a missing database is a BROKEN BUILD, not a reason to pass. Defect D7 was
        # exactly this: the ports disagreed, every database test skipped, and the suite
        # reported green for a year of sessions while the RLS-isolation tests had never once
        # executed. A skip is only ever acceptable on a developer laptop with no stack up.
        if os.environ.get("MYTHEUS_REQUIRE_DB") == "1":
            raise RuntimeError(
                f"MYTHEUS_REQUIRE_DB=1 but Postgres is unreachable at {_SYNC_URL}. "
                "Refusing to skip: skipped isolation tests report green without proving "
                "anything, which is defect D7."
            )
        pytest.skip("test Postgres not reachable (expected on localhost:5443)")

    from alembic import command
    from alembic.config import Config

    cfg = Config("migrations/alembic.ini")
    command.upgrade(cfg, "head")

    conn = psycopg2.connect(_SYNC_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        for role, password in ROLE_PASSWORDS.items():
            # The migration created the role; only the password is environment-specific.
            cur.execute(f"ALTER ROLE {role} WITH LOGIN PASSWORD %s", (password,))
        # The retired test-only role. Its existence is what let Plan 0 report isolation as
        # proven while the application connected as a superuser. Grants must go first: a role
        # still holding privileges cannot be dropped.
        cur.execute(
            "DO $$ BEGIN "
            "IF EXISTS (SELECT FROM pg_roles WHERE rolname='app_rls') THEN "
            "  REVOKE ALL ON ALL TABLES IN SCHEMA public FROM app_rls; "
            "  REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM app_rls; "
            "  REVOKE ALL ON SCHEMA public FROM app_rls; "
            "  DROP ROLE app_rls; "
            "END IF; END $$;"
        )
    conn.close()
    return BOOTSTRAP_URL
