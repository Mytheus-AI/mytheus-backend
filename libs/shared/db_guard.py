"""Refuse to run as a database role that switches tenant isolation off.

Every tenant guarantee in this system is a Postgres row-level-security policy, and a policy
is inert for a role that is a superuser, holds BYPASSRLS, or can reach every table through a
predefined role. `supabase/postgres` ships three such login roles (`supabase_admin`,
`postgres` with BYPASSRLS + `pg_read_all_data`, `supabase_read_only_user` with the same), so
one wrong `DATABASE_URL` silently disables isolation everywhere at once. Defect D1 was exactly
that, and the CI role checks only cover roles the CI database knows about.

`install_role_guard(engine)` adds a pool `connect` listener: every new physical connection
asks Postgres who it is and raises `PrivilegedRoleError` before the pool hands it out if the
role (current_user or session_user) is a superuser, holds BYPASSRLS, is a member of a
predefined role that bypasses table ACLs or reaches the server's filesystem, or is a member of
any role that is a superuser or holds BYPASSRLS (one `SET ROLE` away). The pool therefore never
yields a usable connection: no request, consumer or relay can touch a table as such a role.
`verify_role(engine)` opens one connection eagerly so a process can refuse to start.

The schema owner that runs migrations and test fixtures is deliberately privileged; those
paths build their own engines and never call this.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

# Predefined roles that bypass table ACLs (read/write every table) or reach the server's
# files and programs, which is a superuser in all but name.
RISKY_PREDEFINED_ROLES: tuple[str, ...] = (
    "pg_read_all_data",
    "pg_write_all_data",
    "pg_execute_server_program",
    "pg_read_server_files",
    "pg_write_server_files",
)

_RISKY = ", ".join(f"'{r}'" for r in RISKY_PREDEFINED_ROLES)

# No bind parameters: the same text runs on psycopg2 and on SQLAlchemy's asyncpg adapter,
# whose placeholder styles differ.
GUARD_SQL = f"""
SELECT r.rolname, r.rolsuper, r.rolbypassrls,
       ARRAY(SELECT b.rolname::text FROM pg_roles b
             WHERE b.oid <> r.oid
               AND pg_has_role(r.oid, b.oid, 'MEMBER')
               AND (b.rolsuper OR b.rolbypassrls OR b.rolname IN ({_RISKY}))
             ORDER BY 1) AS risky_memberships
FROM pg_roles r
WHERE r.rolname IN (current_user, session_user)
"""


class PrivilegedRoleError(RuntimeError):
    """The connected role would make every row-level-security policy inert."""


def violations(rows: list[tuple[Any, ...]]) -> list[str]:
    """Human-readable reasons, one per problem, for the rows GUARD_SQL returned."""
    reasons: list[str] = []
    for rolname, rolsuper, rolbypassrls, memberships in rows:
        if rolsuper:
            reasons.append(f"{rolname} is a superuser")
        if rolbypassrls:
            reasons.append(f"{rolname} holds BYPASSRLS")
        for member_of in memberships or []:
            reasons.append(f"{rolname} is a member of {member_of}")
    if not rows:
        reasons.append("could not resolve the connected role in pg_roles")
    return reasons


def _check(dbapi_connection: Any, _record: Any) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(GUARD_SQL)
        rows = [tuple(r) for r in cursor.fetchall()]
    finally:
        cursor.close()
    reasons = violations(rows)
    if reasons:
        raise PrivilegedRoleError(
            "refusing to use this database connection: "
            + "; ".join(reasons)
            + ". Point DATABASE_URL at the service's least-privilege role."
        )


def install_role_guard(engine: AsyncEngine | Engine) -> AsyncEngine | Engine:
    """Check every new physical connection of `engine`'s pool. Returns the engine."""
    sync = engine.sync_engine if isinstance(engine, AsyncEngine) else engine
    if not event.contains(sync, "connect", _check):
        event.listen(sync, "connect", _check)
    return engine


async def verify_role(engine: AsyncEngine) -> None:
    """Open one connection now, so a process with the wrong role fails at startup."""
    async with engine.connect():
        pass
