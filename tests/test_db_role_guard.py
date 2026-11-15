"""The pool startup guard (`libs/shared/db_guard.py`): refuse a role that makes RLS inert."""

from __future__ import annotations

import os
import uuid

import psycopg2
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from libs.shared.db_guard import (
    RISKY_PREDEFINED_ROLES,
    PrivilegedRoleError,
    install_role_guard,
    verify_role,
    violations,
)
from tests.conftest import APP_URL, BOOTSTRAP_URL, RELAY_URL


def test_violations_names_every_problem() -> None:
    assert violations([("mytheus_app", False, False, [])]) == []
    assert violations([("root", True, False, [])]) == ["root is a superuser"]
    assert violations([("svc", False, True, [])]) == ["svc holds BYPASSRLS"]
    assert violations([("ro", False, False, ["pg_read_all_data"])]) == [
        "ro is a member of pg_read_all_data"
    ]
    assert violations([]) == ["could not resolve the connected role in pg_roles"]


def test_the_risky_list_covers_the_acl_and_filesystem_bypasses() -> None:
    assert {
        "pg_read_all_data",
        "pg_write_all_data",
        "pg_execute_server_program",
        "pg_read_server_files",
        "pg_write_server_files",
    } <= set(RISKY_PREDEFINED_ROLES)


@pytest.mark.parametrize("url", [APP_URL, RELAY_URL], ids=["mytheus_app", "mytheus_relay"])
async def test_guard_admits_the_least_privilege_roles(migrated_db, url) -> None:
    engine = install_role_guard(create_async_engine(url))
    try:
        await verify_role(engine)  # type: ignore[arg-type]
        async with engine.connect() as conn:  # type: ignore[union-attr]
            assert (await conn.execute(text("SELECT 1"))).scalar_one() == 1
    finally:
        await engine.dispose()  # type: ignore[union-attr]


async def test_guard_refuses_the_schema_owner(migrated_db) -> None:
    """`postgres` is a superuser on postgres:15 and BYPASSRLS + pg_read_all_data on
    supabase/postgres. Either way the guard must refuse it."""
    engine = install_role_guard(create_async_engine(BOOTSTRAP_URL))
    try:
        with pytest.raises(PrivilegedRoleError, match="refusing to use this database"):
            await verify_role(engine)  # type: ignore[arg-type]
    finally:
        await engine.dispose()  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "grant",
    ["BYPASSRLS", "pg_read_all_data", "pg_write_all_data", "MEMBER_OF_BYPASS"],
)
async def test_guard_refuses_each_way_of_bypassing_isolation(migrated_db, grant) -> None:
    """A throwaway login role given one dangerous property at a time."""
    role = f"guard_probe_{uuid.uuid4().hex[:8]}"
    helper = f"{role}_bypass"
    # Granting BYPASSRLS or a predefined role takes a superuser. On supabase/postgres the
    # schema owner `postgres` is not one, so CI passes SUPERUSER_DATABASE_URL (supabase_admin).
    su_url = os.environ.get("SUPERUSER_DATABASE_URL") or BOOTSTRAP_URL
    conn = psycopg2.connect(su_url.replace("+asyncpg", ""))
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")
        if not cur.fetchone()[0]:
            conn.close()
            pytest.skip("needs SUPERUSER_DATABASE_URL to create a privileged probe role")
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'probe' NOBYPASSRLS")
            if grant == "BYPASSRLS":
                cur.execute(f"ALTER ROLE {role} BYPASSRLS")
            elif grant == "MEMBER_OF_BYPASS":
                # Not bypassing itself, but one `SET ROLE` away from a role that does.
                cur.execute(f"CREATE ROLE {helper} NOLOGIN BYPASSRLS")
                cur.execute(f"GRANT {helper} TO {role}")
            else:
                cur.execute(f"GRANT {grant} TO {role}")
        scheme, _, rest = BOOTSTRAP_URL.partition("://")
        url = f"{scheme}://{role}:probe@{rest.rpartition('@')[2]}"
        engine = install_role_guard(create_async_engine(url))
        try:
            with pytest.raises(PrivilegedRoleError):
                await verify_role(engine)  # type: ignore[arg-type]
        finally:
            await engine.dispose()  # type: ignore[union-attr]
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP ROLE IF EXISTS {role}")
            cur.execute(f"DROP ROLE IF EXISTS {helper}")
        conn.close()


def test_the_request_path_engine_is_guarded(monkeypatch) -> None:
    """`get_engine()` / `get_admin_engine()` carry the guard, so no handler can bypass it."""
    from sqlalchemy import event

    from libs.shared import db, db_guard

    monkeypatch.setenv("DATABASE_URL", APP_URL)
    monkeypatch.setenv("ADMIN_DATABASE_URL", APP_URL)
    db.get_engine.cache_clear()
    db.get_admin_engine.cache_clear()
    try:
        for engine in (db.get_engine(), db.get_admin_engine()):
            assert event.contains(engine.sync_engine, "connect", db_guard._check)
    finally:
        db.get_engine.cache_clear()
        db.get_admin_engine.cache_clear()
