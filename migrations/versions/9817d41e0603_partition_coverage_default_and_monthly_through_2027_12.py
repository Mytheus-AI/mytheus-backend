"""partition coverage: DEFAULT catch-all + monthly partitions through 2027-12

Incident, 2026-11-06: every `POST /chat` failed with `no partition of relation "chat_messages"
found for row`. `5ed978467cad` created only `chat_messages_2026_08` and deferred maintenance
(an explicitly flagged risk); the calendar moved on and nothing created the next month.
`audit_logs` (`c3e8b1d47f92`) was not yet broken - twelve months, 2026-07..2027-06, plus a
DEFAULT - but was on the same clock.

Inventory of every partitioned table in `public` (pg_inherits, at 1afe4928b214):

    audit_logs         2026_07 .. 2027_06 + audit_logs_default        backend-owned
    chat_messages      2026_08 only, NO default                      backend-owned  <- broken
    transcript_chunks  2026_08 .. 2026_10 + transcript_chunks_default  mytheus-realtime

`transcript_chunks` is owned and migrated by `mytheus-realtime` (CLAUDE.md, `_grants.py`) and
is deliberately NOT touched here: its DEFAULT partition keeps inserts working, and creating
its monthly partitions is that repo's job (recorded as a gap for its owner).

WHAT THIS MIGRATION DOES (backend-owned tables only)

1. `chat_messages_default`: a DEFAULT partition, so an INSERT past the last explicit month is
   a slow query and an alert rather than a refused write - the same backstop `audit_logs`
   has carried since `c3e8b1d47f92`. This is a reversal of `5ed978467cad`'s "current month
   only, no catch-all" decision, made because that decision's stated risk materialised.
2. Explicit monthly partitions through 2027-12: `chat_messages_2026_09 .. 2027_12` and
   `audit_logs_2027_07 .. 2027_12`. Fixed constants, not `date.today()`, for the same
   replay-determinism reason the two earlier migrations give.
3. Every new partition carries exactly what its siblings carry: the same per-partition
   indexes (chat: `(org_id, meeting_id, created_at)` + GIN FTS on `content`; audit:
   `(org_id, occurred_at)` + `(org_id, target_type, target_id)`), and an ACL of owner-only.
   ADR-0021 / `9b2c4d1e6f30`: no schema-wide default privileges, and a partition must NEVER
   be granted to an application role - it is reached through its parent, where the grants
   and the `tenant_isolation` policy live (RLS ENABLE+FORCE is on the parent and propagates;
   no partition has relrowsecurity set, so none is set here). A partition born after
   `9b2c4d1e6f30` has a NULL (empty) ACL; the explicit `REVOKE ALL ... FROM mytheus_app,
   mytheus_admin, mytheus_relay, PUBLIC` materialises it to the same owner-only ACL the
   older siblings hold, and the non-owner sweep removes anything a non-standard creator's
   default ACL might have added. `tests/test_roles_and_policies.py::
   test_no_application_role_can_reach_a_partition_directly` asserts it.

THE COST OF A DEFAULT PARTITION - NOTE FOR WHOEVER CREATES THE NEXT MONTH

Once a DEFAULT partition exists, `CREATE TABLE ... PARTITION OF parent FOR VALUES ...`
scans the default under an ACCESS EXCLUSIVE lock and FAILS if the default already holds any
row in the new range ("updated partition constraint for default partition would be violated").
So creating an explicit month that rows have already spilled into requires MOVING those rows
first: lock the default, DELETE ... RETURNING them into a holding table, create the partition,
insert them into it. This migration does exactly that for every month it adds (a no-op when
the default is empty for that range), and `scripts/ensure_partitions.py` does the same.

`scripts/ensure_partitions.py` is idempotent and creates the next N months for each
backend-owned partitioned table. PLATFORM OWNER: schedule it MONTHLY (e.g. on the 1st, with
`--months 3`), as the schema owner (`BOOTSTRAP_DATABASE_URL`). Until it is scheduled, rows past
2027-12 land in the DEFAULT partitions and `tests/test_partition_coverage.py` turns red three
months before that happens. This repo does not edit mytheus-platform; the scheduling is theirs.

Revision ID: 9817d41e0603
Revises: 1afe4928b214
Create Date: 2026-11-06

"""

from __future__ import annotations

from datetime import date
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9817d41e0603"
down_revision: Union[str, None] = "1afe4928b214"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Frozen copy of the spec in scripts/ensure_partitions.py. A migration must replay identically
# forever, so it does not import a module that may change after it ships.
_LAST_YEAR, _LAST_MONTH = 2027, 12
_TABLES: dict[str, dict[str, object]] = {
    "chat_messages": {
        "key": "created_at",
        "first": (2026, 9),
        "indexes": (
            "CREATE INDEX ix_{p}_org_meeting_created ON {p} (org_id, meeting_id, created_at)",
            "CREATE INDEX ix_{p}_content_fts ON {p} USING GIN (to_tsvector('english', content))",
        ),
        "create_default": True,
    },
    "audit_logs": {
        "key": "occurred_at",
        "first": (2027, 7),
        "indexes": (
            "CREATE INDEX ix_{p}_org_occurred ON {p} (org_id, occurred_at)",
            "CREATE INDEX ix_{p}_org_target ON {p} (org_id, target_type, target_id)",
        ),
        "create_default": False,  # audit_logs_default exists since c3e8b1d47f92
    },
}

_LOCKDOWN_ROLES = "mytheus_app, mytheus_admin, mytheus_relay, PUBLIC"

_SWEEP_ONE = """
DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT DISTINCT a.grantee AS g
        FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a
        WHERE c.oid = '{p}'::regclass AND a.grantee <> c.relowner AND a.grantee <> 0
    LOOP
        EXECUTE format('REVOKE ALL ON {p} FROM %I', pg_get_userbyid(r.g));
    END LOOP;
END $$;
"""


def _months(first: tuple[int, int]) -> list[tuple[int, int]]:
    y, m = first
    out = []
    while (y, m) <= (_LAST_YEAR, _LAST_MONTH):
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _bounds(y: int, m: int) -> tuple[str, str]:
    end = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
    return date(y, m, 1).isoformat(), end.isoformat()


def _lock_down(p: str) -> None:
    op.execute(f"REVOKE ALL ON {p} FROM {_LOCKDOWN_ROLES};")
    op.execute(_SWEEP_ONE.format(p=p))


def upgrade() -> None:
    for table, spec in _TABLES.items():
        key = str(spec["key"])
        indexes = spec["indexes"]
        assert isinstance(indexes, tuple)
        default = f"{table}_default"

        if spec["create_default"]:
            op.execute(f"CREATE TABLE {default} PARTITION OF {table} DEFAULT;")
            for tmpl in indexes:
                op.execute(tmpl.format(p=default) + ";")
            _lock_down(default)
            op.execute(
                f"COMMENT ON TABLE {default} IS "
                "'Catch-all so an INSERT past the last monthly partition degrades to a slow "
                "query and an alert rather than a refused write (9817d41e0603). Rows here mean "
                "scripts/ensure_partitions.py is overdue; creating a month that rows already "
                "spilled into requires moving them out first.';"
            )

        first = spec["first"]
        assert isinstance(first, tuple)
        for y, m in _months(first):
            start, end = _bounds(y, m)
            p = f"{table}_{y:04d}_{m:02d}"
            # Move any rows that already spilled into the DEFAULT for this range, else the
            # CREATE below fails on the default's re-validated partition constraint.
            op.execute(f"LOCK TABLE {default} IN ACCESS EXCLUSIVE MODE;")
            op.execute(f"CREATE TEMP TABLE _spill (LIKE {table});")
            op.execute(
                f"WITH d AS (DELETE FROM {default} WHERE {key} >= '{start}' AND {key} < '{end}' "
                f"RETURNING *) INSERT INTO _spill SELECT * FROM d;"
            )
            op.execute(
                f"CREATE TABLE {p} PARTITION OF {table} FOR VALUES FROM ('{start}') TO ('{end}');"
            )
            op.execute(f"INSERT INTO {p} SELECT * FROM _spill;")
            op.execute("DROP TABLE _spill;")
            for tmpl in indexes:
                op.execute(tmpl.format(p=p) + ";")
            _lock_down(p)

    op.execute(
        """
        COMMENT ON TABLE chat_messages IS
        'Canonical chat_messages, monthly range-partitioned on created_at, with a DEFAULT '
        'catch-all since 9817d41e0603. scripts/ensure_partitions.py creates upcoming months '
        'and must run monthly; tests/test_partition_coverage.py fails when coverage lapses.';
        """
    )


def downgrade() -> None:
    # Forward-only in spirit: dropping a partition drops its rows. Downgrade detaches nothing
    # that could hold data written after upgrade without saying so; it drops the added
    # partitions only, which is acceptable solely on a disposable dev database.
    for table, spec in _TABLES.items():
        first = spec["first"]
        assert isinstance(first, tuple)
        for y, m in _months(first):
            op.execute(f"DROP TABLE IF EXISTS {table}_{y:04d}_{m:02d};")
        if spec["create_default"]:
            op.execute(f"DROP TABLE IF EXISTS {table}_default;")
    op.execute(
        "COMMENT ON TABLE chat_messages IS 'Canonical chat_messages, monthly range-partitioned "
        "on created_at.';"
    )
