"""Shared fixtures for the Plan 0 backend tests.

Tests run against the local dev stack: an isolated test Postgres (default localhost:5433), the
compose Redpanda (localhost:9092 / :8081), and fakeredis for Redis-only unit tests. Env is set
here BEFORE any app module imports. Tests that need a service they cannot reach skip gracefully.
"""

from __future__ import annotations

import os

import psycopg2
import pytest

os.environ.setdefault(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5433/mytheus"
)
os.environ.setdefault("REDPANDA_BROKERS", "localhost:9092")
os.environ.setdefault("SCHEMA_REGISTRY_URL", "http://localhost:8081")
os.environ.setdefault("REDIS_URL", "redis://localhost:6380")

SUPERUSER_URL = os.environ["DATABASE_URL"]
APP_RLS_URL = SUPERUSER_URL.replace("postgres:postgres@", "app_rls:app_rls@")
_SYNC_URL = SUPERUSER_URL.replace("+asyncpg", "")


def _db_reachable() -> bool:
    try:
        psycopg2.connect(_SYNC_URL, connect_timeout=2).close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def migrated_db() -> str:
    """Apply migrations to the test DB once and provision a non-superuser RLS role. Returns the
    superuser async URL. Skips the whole session's DB tests if the DB is unreachable."""
    if not _db_reachable():
        pytest.skip("test Postgres not reachable (expected on localhost:5433)")

    from alembic import command
    from alembic.config import Config

    cfg = Config("migrations/alembic.ini")
    command.upgrade(cfg, "head")

    conn = psycopg2.connect(_SYNC_URL)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname='app_rls') "
            "THEN CREATE ROLE app_rls LOGIN PASSWORD 'app_rls'; END IF; END $$;"
        )
        cur.execute("GRANT USAGE ON SCHEMA public TO app_rls;")
        cur.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_rls;"
        )
        cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_rls;")
    conn.close()
    return SUPERUSER_URL
