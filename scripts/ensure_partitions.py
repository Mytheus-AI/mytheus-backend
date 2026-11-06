"""Create the next N monthly partitions for every backend-owned partitioned table. Idempotent.

Why this exists: `chat_messages` shipped with one month and no catch-all, the calendar moved
on, and every chat insert failed (fixed in migration `9817d41e0603`, which also documents the
DEFAULT-partition trade-off). Each table now has a DEFAULT partition so an insert never fails,
but rows in the default are a slow-query and maintenance smell, so the months must keep being
created ahead of time. PLATFORM OWNER: run this monthly (e.g. the 1st, `--months 3`).

Usage, as the schema owner (partitions are DDL; no application role can create them):

    BOOTSTRAP_DATABASE_URL=postgresql://<owner>:<pw>@host:5432/mytheus \\
      uv run python -m scripts.ensure_partitions --months 3

Every partition created here gets exactly what its siblings carry (migration `9817d41e0603`):
the per-partition indexes, and an owner-only ACL. A partition is NEVER granted to an
application role; it is reached through its parent, where the grants and the RLS policy live.

`transcript_chunks` is partitioned too but belongs to `mytheus-realtime`; it is not managed
here.
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg2


@dataclass(frozen=True)
class PartitionedTable:
    name: str
    key: str
    # Per-partition index DDL, `{p}` is the partition name. Mirrors the migrations.
    indexes: tuple[str, ...]


BACKEND_PARTITIONED_TABLES: tuple[PartitionedTable, ...] = (
    PartitionedTable(
        name="chat_messages",
        key="created_at",
        indexes=(
            "CREATE INDEX IF NOT EXISTS ix_{p}_org_meeting_created "
            "ON {p} (org_id, meeting_id, created_at)",
            "CREATE INDEX IF NOT EXISTS ix_{p}_content_fts "
            "ON {p} USING GIN (to_tsvector('english', content))",
        ),
    ),
    PartitionedTable(
        name="audit_logs",
        key="occurred_at",
        indexes=(
            "CREATE INDEX IF NOT EXISTS ix_{p}_org_occurred ON {p} (org_id, occurred_at)",
            "CREATE INDEX IF NOT EXISTS ix_{p}_org_target "
            "ON {p} (org_id, target_type, target_id)",
        ),
    ),
)

_LOCKDOWN_ROLES = ("mytheus_app", "mytheus_admin", "mytheus_relay")


def add_months(year: int, month: int, n: int) -> tuple[int, int]:
    m0 = year * 12 + (month - 1) + n
    return m0 // 12, m0 % 12 + 1


def month_bounds(year: int, month: int) -> tuple[date, date]:
    ny, nm = add_months(year, month, 1)
    return date(year, month, 1), date(ny, nm, 1)


def partition_name(table: str, year: int, month: int) -> str:
    return f"{table}_{year:04d}_{month:02d}"


def _exists(cur: Any, rel: str) -> bool:
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{rel}",))
    return bool(cur.fetchone()[0])


def _lock_down(cur: Any, rel: str) -> None:
    """Owner-only ACL, like every sibling: revoke the app roles and PUBLIC, then sweep any
    other non-owner grantee (a creator with a non-standard default ACL)."""
    existing = [r for r in _LOCKDOWN_ROLES if _role_exists(cur, r)]
    cur.execute(f"REVOKE ALL ON {rel} FROM {', '.join([*existing, 'PUBLIC'])}")
    cur.execute(
        "SELECT DISTINCT pg_get_userbyid(a.grantee) FROM pg_class c "
        "CROSS JOIN LATERAL aclexplode(c.relacl) a "
        "WHERE c.oid = %s::regclass AND a.grantee <> c.relowner AND a.grantee <> 0",
        (rel,),
    )
    for (grantee,) in cur.fetchall():
        cur.execute(f'REVOKE ALL ON {rel} FROM "{grantee}"')


def _role_exists(cur: Any, role: str) -> bool:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
    return cur.fetchone() is not None


def ensure_default(cur: Any, spec: PartitionedTable) -> bool:
    default = f"{spec.name}_default"
    if _exists(cur, default):
        return False
    cur.execute(f"CREATE TABLE {default} PARTITION OF {spec.name} DEFAULT")
    for tmpl in spec.indexes:
        cur.execute(tmpl.format(p=default))
    _lock_down(cur, default)
    return True


def ensure_month(cur: Any, spec: PartitionedTable, year: int, month: int) -> bool:
    """Create one month's partition if missing, moving any rows that spilled into the DEFAULT
    for that range first (otherwise the CREATE fails re-validating the default). Returns True
    if it created the partition."""
    p = partition_name(spec.name, year, month)
    if _exists(cur, p):
        return False
    start, end = month_bounds(year, month)
    default = f"{spec.name}_default"
    has_default = _exists(cur, default)
    if has_default:
        cur.execute(f"LOCK TABLE {default} IN ACCESS EXCLUSIVE MODE")
        cur.execute(f"CREATE TEMP TABLE _spill (LIKE {spec.name}) ON COMMIT DROP")
        cur.execute(
            f"WITH d AS (DELETE FROM {default} WHERE {spec.key} >= %s AND {spec.key} < %s "
            f"RETURNING *) INSERT INTO _spill SELECT * FROM d",
            (start, end),
        )
    cur.execute(
        f"CREATE TABLE {p} PARTITION OF {spec.name} FOR VALUES FROM (%s) TO (%s)",
        (start.isoformat(), end.isoformat()),
    )
    if has_default:
        cur.execute(f"INSERT INTO {p} SELECT * FROM _spill")
        cur.execute("DROP TABLE _spill")
    for tmpl in spec.indexes:
        cur.execute(tmpl.format(p=p))
    _lock_down(cur, p)
    return True


def ensure_partitions(
    conn: Any,
    *,
    months: int,
    today: date | None = None,
    tables: tuple[PartitionedTable, ...] = BACKEND_PARTITIONED_TABLES,
) -> list[str]:
    """Ensure the DEFAULT plus the current month and the next `months` exist for every table.
    Each partition is its own transaction. Returns the names created."""
    today = today or date.today()
    created: list[str] = []
    for spec in tables:
        with conn, conn.cursor() as cur:
            if ensure_default(cur, spec):
                created.append(f"{spec.name}_default")
        for i in range(months + 1):
            y, m = add_months(today.year, today.month, i)
            with conn, conn.cursor() as cur:
                if ensure_month(cur, spec, y, m):
                    created.append(partition_name(spec.name, y, m))
    return created


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--months", type=int, default=3, help="months ahead of the current one")
    args = parser.parse_args()
    url = os.environ.get("BOOTSTRAP_DATABASE_URL") or os.environ.get("DATABASE_URL")
    if not url:
        print("BOOTSTRAP_DATABASE_URL (or DATABASE_URL) is required", file=sys.stderr)
        return 2
    conn = psycopg2.connect(url.replace("+asyncpg", ""))
    try:
        created = ensure_partitions(conn, months=args.months)
    finally:
        conn.close()
    print("created: " + (", ".join(created) if created else "nothing (already covered)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
