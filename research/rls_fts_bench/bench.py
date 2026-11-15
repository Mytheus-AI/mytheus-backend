"""FTS-under-RLS benchmark: EXPLAIN (ANALYZE, BUFFERS) for the real FTS query shapes.

Usage (against a THROWAWAY supabase/postgres:15.8.1.060 seeded with seed.sql):

    BENCH_DSN=postgresql://supabase_admin:postgres@localhost:55101/mytheus \
      uv run python research/rls_fts_bench/bench.py <label> <out_dir>

Connects as a superuser only to `SET ROLE` into the application role each query really runs
as, so RLS applies exactly as it does in production (the superuser itself bypasses RLS, which is
what the `owner` rows measure: the same query with RLS out of the way).

Writes `<out_dir>/<label>.csv` (one row per query x tenant x repetition) and
`<out_dir>/<label>_plans.txt` (the first plan of each cell, verbatim).
"""

from __future__ import annotations

import csv
import json
import os
import pathlib
import subprocess
import sys
import time
from typing import Any

import psycopg2

BIG = "00000000-0000-4000-8000-000000000001"  # 20% of every table
SMALL = "00000000-0000-4000-8000-000000000002"  # ~0.4% of every table
REPS = 5

# Every full-text GIN index the migrations create, before and after the fix.
GIN_INDEXES = {
    "ix_intelligence_transcript_chunk_search_tsv",
    "ix_intelligence_transcript_chunk_search_org_tsv",
    "ix_decisions_fts",
    "ix_decisions_org_search_tsv",
}

# Each query is the production SQL shape with its bind parameters inlined.
QUERIES: dict[str, tuple[str, str]] = {
    # mytheus-intelligence services/search_service/fts.py (match_any=False, org-wide).
    "transcript_fts": (
        "mytheus_ai_app",
        """SELECT chunk_id, meeting_id, speaker_id, start_ms, end_ms, text,
                  ts_rank(tsv, websearch_to_tsquery('english', 'zephyr')) AS rank
           FROM intelligence.transcript_chunk_search
           WHERE org_id = '{org}'
             AND tsv @@ websearch_to_tsquery('english', 'zephyr')
             AND NOT confidential
           ORDER BY rank DESC LIMIT 20""",
    ),
    # mytheus-backend core_api/routes/internal_decision_check.py lexical arm: the decisions
    # predicate that ix_decisions_fts (f8a9b0c1d2e3) was built for. The meetings join and the
    # participant EXISTS are omitted because the seed has no meetings; they do not change
    # whether the GIN index is reachable.
    "decision_fts": (
        "mytheus_app",
        """SELECT d.id, ts_rank_cd(to_tsvector('english', d.content || ' ' ||
                  coalesce(d.rationale, '')), to_tsquery('english', 'zephyr')) AS rank
           FROM decisions d
           WHERE d.status = 'confirmed' AND d.deleted_at IS NULL
             AND to_tsvector('english', d.content || ' ' || coalesce(d.rationale, ''))
                 @@ to_tsquery('english', 'zephyr')
           ORDER BY rank DESC, d.created_at DESC, d.id LIMIT 20""",
    ),
    # The same lexical arm after b8e2c4f6a1d3: the stored `search_tsv` column, which is what
    # internal_decision_check.py and internal_bets.py query from that revision on.
    "decision_fts_tsv": (
        "mytheus_app",
        """SELECT d.id, ts_rank_cd(d.search_tsv, to_tsquery('english', 'zephyr')) AS rank
           FROM decisions d
           WHERE d.status = 'confirmed' AND d.deleted_at IS NULL
             AND d.search_tsv @@ to_tsquery('english', 'zephyr')
           ORDER BY rank DESC, d.created_at DESC, d.id LIMIT 20""",
    ),
    # chat_messages GIN index from 5ed978467cad: to_tsvector('english', content).
    "chat_fts": (
        "mytheus_app",
        """SELECT id, content FROM chat_messages
           WHERE to_tsvector('english', content) @@ to_tsquery('english', 'zephyr')
             AND deleted_at IS NULL
           ORDER BY created_at DESC LIMIT 20""",
    ),
    # A non-FTS control: an org-scoped lookup that RLS is cheap for.
    "transcript_lookup": (
        "mytheus_ai_app",
        """SELECT count(*) FROM intelligence.transcript_chunk_search
           WHERE meeting_id = '00000000-0000-0000-0000-000000000000'""",
    ),
}


def walk(node: dict[str, Any], out: list[str]) -> None:
    out.append(f"{node['Node Type']}:{node.get('Index Name', '')}")
    for child in node.get("Plans", []):
        walk(child, out)


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:  # noqa: BLE001
        return "unknown"


def main() -> None:
    label, out_dir = sys.argv[1], pathlib.Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    conn = psycopg2.connect(os.environ["BENCH_DSN"])
    conn.autocommit = True
    cur = conn.cursor()
    rows: list[dict[str, object]] = []
    plans: list[str] = []
    for name, (role, sql_tpl) in QUERIES.items():
        for org_label, org in (("big", BIG), ("small", SMALL)):
            sql = sql_tpl.format(org=org)
            for mode in ("rls", "owner"):
                for rep in range(REPS):
                    cur.execute("BEGIN")
                    cur.execute("SELECT set_config('app.current_org', %s, true)", (org,))
                    if mode == "rls":
                        cur.execute(f"SET LOCAL ROLE {role}")
                    else:
                        # The owner run adds the tenant predicate the policy would have added,
                        # so both modes return the same rows (the differential baseline).
                        pass
                    q = sql
                    if mode == "owner" and "org_id = '" not in q:
                        q = q.replace("WHERE ", f"WHERE org_id = '{org}' AND ", 1)
                    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {q}")
                    plan = cur.fetchone()[0][0]
                    cur.execute("ROLLBACK")
                    nodes: list[str] = []
                    walk(plan["Plan"], nodes)
                    top = plan["Plan"]
                    shared = top.get("Shared Hit Blocks", 0) + top.get("Shared Read Blocks", 0)
                    rows.append(
                        {
                            "label": label,
                            "query": name,
                            "tenant": org_label,
                            "mode": mode,
                            "role": role if mode == "rls" else "supabase_admin(owner,bypass)",
                            "rep": rep,
                            "execution_ms": plan["Execution Time"],
                            "planning_ms": plan["Planning Time"],
                            "shared_blocks": shared,
                            "rows": top.get("Actual Rows"),
                            "gin_used": any(
                                n.split(":")[1] in GIN_INDEXES
                                or n.split(":")[1].endswith("_content_fts")
                                for n in nodes
                            ),
                            "nodes": " > ".join(nodes),
                            "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                            "git_sha": git_sha(),
                        }
                    )
                    if rep == 0:
                        cur.execute("BEGIN")
                        cur.execute("SELECT set_config('app.current_org', %s, true)", (org,))
                        if mode == "rls":
                            cur.execute(f"SET LOCAL ROLE {role}")
                        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {q}")
                        text_plan = "\n".join(r[0] for r in cur.fetchall())
                        cur.execute("ROLLBACK")
                        plans.append(
                            f"=== {label} {name} tenant={org_label} mode={mode}\n{text_plan}\n"
                        )
    with open(out_dir / f"{label}.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    (out_dir / f"{label}_plans.txt").write_text("\n".join(plans))
    times: dict[str, list[float]] = {}
    first: dict[str, dict[str, object]] = {}
    for r in rows:
        key = f"{r['query']}/{r['tenant']}/{r['mode']}"
        times.setdefault(key, []).append(float(r["execution_ms"]))  # type: ignore[arg-type]
        first.setdefault(key, r)
    for key, ms in times.items():
        ms.sort()
        f = first[key]
        print(
            f"{key:40s} median={ms[len(ms) // 2]:9.2f} ms  "
            f"gin={f['gin_used']}  blocks={f['shared_blocks']}"
        )
    print(json.dumps({"label": label, "n_rows": len(rows)}))


if __name__ == "__main__":
    main()
