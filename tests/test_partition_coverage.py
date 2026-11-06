"""Partition coverage: the CI tripwire for the 2026-11-06 chat outage.

`chat_messages` had a single month and no DEFAULT; when the month ended every chat insert
raised. Migration `9817d41e0603` added DEFAULT partitions and months through 2027-12, and
`scripts/ensure_partitions.py` keeps them coming. This file fails LOUDLY the moment explicit
coverage of the current month or any of the next three lapses for a backend-owned table, so
the lapse is a red build months before it is a slow query or an outage.
"""

from __future__ import annotations

from datetime import UTC, datetime

import psycopg2
import pytest

from scripts.ensure_partitions import (
    BACKEND_PARTITIONED_TABLES,
    PartitionedTable,
    add_months,
    ensure_month,
    ensure_partitions,
    month_bounds,
)

MONTHS_AHEAD = 3

_BOUNDS_SQL = """
SELECT c.relname,
       pg_get_expr(c.relpartbound, c.oid) AS bound,
       (regexp_match(pg_get_expr(c.relpartbound, c.oid),
                     'FROM \\(''([^'']+)''\\) TO \\(''([^'']+)''\\)')) AS r
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
WHERE i.inhparent = %s::regclass
"""


def _connect(url: str):  # type: ignore[no-untyped-def]
    return psycopg2.connect(url.replace("+asyncpg", ""))


def _ranges(cur, table: str) -> tuple[list[tuple[datetime, datetime]], bool]:  # type: ignore[no-untyped-def]
    cur.execute(_BOUNDS_SQL, (table,))
    ranges: list[tuple[datetime, datetime]] = []
    has_default = False
    for _name, bound, r in cur.fetchall():
        if bound == "DEFAULT":
            has_default = True
        elif r:
            ranges.append((datetime.fromisoformat(r[0]), datetime.fromisoformat(r[1])))
    return ranges, has_default


@pytest.mark.parametrize("spec", BACKEND_PARTITIONED_TABLES, ids=lambda s: s.name)
def test_explicit_partitions_cover_this_month_and_the_next_three(migrated_db, spec):
    now = datetime.now(UTC)
    conn = _connect(migrated_db)
    try:
        with conn.cursor() as cur:
            ranges, has_default = _ranges(cur, spec.name)
    finally:
        conn.close()

    assert has_default, f"{spec.name} has no DEFAULT partition: inserts past coverage will FAIL"

    missing = []
    for i in range(MONTHS_AHEAD + 1):
        y, m = add_months(now.year, now.month, i)
        start, end = month_bounds(y, m)
        s = datetime(start.year, start.month, 1, tzinfo=UTC)
        e = datetime(end.year, end.month, 1, tzinfo=UTC)
        if not any(lo <= s and e <= hi for lo, hi in ranges):
            missing.append(f"{y:04d}-{m:02d}")
    assert not missing, (
        f"PARTITION COVERAGE LAPSED for {spec.name}: no explicit partition for {missing}. "
        "Run `uv run python -m scripts.ensure_partitions --months 3` as the schema owner "
        "(and check the monthly schedule for it in mytheus-platform)."
    )


def test_ensure_partitions_is_a_no_op_when_covered(migrated_db):
    conn = _connect(migrated_db)
    try:
        first = ensure_partitions(conn, months=MONTHS_AHEAD)
        second = ensure_partitions(conn, months=MONTHS_AHEAD)
    finally:
        conn.close()
    assert second == [], f"second run must create nothing, created {second} (first: {first})"


_PROBE = PartitionedTable(name="zz_partition_probe", key="k", indexes=())


def test_creating_a_month_moves_rows_out_of_the_default(migrated_db):
    """The DEFAULT-partition cost: creating a month that rows already spilled into fails
    unless they are moved first. The helper must move them, and lock the new partition down
    to an owner-only ACL like its siblings."""
    conn = _connect(migrated_db)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS zz_partition_probe")
            cur.execute(
                "CREATE TABLE zz_partition_probe (id int, k timestamptz, PRIMARY KEY (id, k)) "
                "PARTITION BY RANGE (k)"
            )
            cur.execute(
                "CREATE TABLE zz_partition_probe_default PARTITION OF zz_partition_probe DEFAULT"
            )
            cur.execute(
                "INSERT INTO zz_partition_probe VALUES (1, '2030-05-15T00:00:00Z'), "
                "(2, '2030-06-15T00:00:00Z')"
            )
        with conn, conn.cursor() as cur:
            assert ensure_month(cur, _PROBE, 2030, 5) is True
        with conn, conn.cursor() as cur:
            cur.execute("SELECT id, tableoid::regclass::text FROM zz_partition_probe ORDER BY id")
            assert cur.fetchall() == [
                (1, "zz_partition_probe_2030_05"),
                (2, "zz_partition_probe_default"),
            ]
            cur.execute(
                "SELECT count(*) FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
                "WHERE c.oid = 'zz_partition_probe_2030_05'::regclass "
                "AND a.grantee <> c.relowner"
            )
            assert cur.fetchone()[0] == 0
            assert ensure_month(cur, _PROBE, 2030, 5) is False
    finally:
        with conn, conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS zz_partition_probe")
        conn.close()
