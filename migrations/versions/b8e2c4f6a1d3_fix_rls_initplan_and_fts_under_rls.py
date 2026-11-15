"""fix: every tenant policy in InitPlan form, and full-text search that survives RLS

Two measured problems, one migration (journal plan Part 3 fix 1; numbers and plans in
`research/rls_fts_bench/` and the research metrics folder).

1. THE POLICY FORM. Every policy that reads `app.current_org` compared each row against
   `NULLIF(current_setting('app.current_org', true), '')::uuid`, an expression the planner
   re-evaluates per row whenever the policy qual ends up as a filter. It is rewritten to the
   InitPlan form

       org_id = (SELECT NULLIF(current_setting('app.current_org', true), '')::uuid)

   which Postgres evaluates once per statement and then treats as a constant parameter. The
   semantics are identical: the same NULLIF makes an unset OR empty setting yield NULL, and
   `org_id = NULL` is never true, so the policy still fails closed. Only the expression inside
   each USING / WITH CHECK changes; ALTER POLICY leaves the command, the roles, the
   permissive/restrictive kind, ENABLE and FORCE ROW LEVEL SECURITY untouched, and a policy
   that had a WITH CHECK keeps one. The rewrite is driven from the catalog so it covers every
   such policy in `public` (116 at the time of writing, including the `org_id IS NULL OR ...`
   and the `tenant_insert` variants), except `transcript_chunks`, which belongs to
   transcription-service in mytheus-realtime and is rewritten by that repo's own migration.

2. FULL-TEXT SEARCH UNDER RLS. The InitPlan form alone does NOT bring back the GIN index (the
   benchmark shows it). The real cause is the security-barrier rule: under RLS, a user qual
   may only become an index condition if it is LEAKPROOF, and `tsvector @@ tsquery`
   (`ts_match_vq`) is not marked leakproof in core, while `to_tsvector(...)` over a column can
   genuinely raise data-dependent errors and must never be. So:

   - `decisions` gains a stored generated column `search_tsv` holding exactly the expression
     `ix_decisions_fts` indexed. Queries then test `search_tsv @@ q`, a plain column, so the
     only function evaluated before the policy is `ts_match_vq`.
   - The composite GIN index `(org_id, search_tsv)` (btree_gin) replaces `ix_decisions_fts`,
     so one index scan intersects "this tenant" with "matches the query" instead of matching
     every tenant's rows and filtering. It is not partial: the evidence search reads
     `status IN ('confirmed', 'superseded')`, which the old partial predicate never matched.
   - `ts_match_vq` and its commutator `ts_match_qv` are marked LEAKPROOF, if this migration
     runs as a superuser. Neither raises an error that depends on the tsvector's contents (a
     binary search over lexemes; the only errors are stack depth and interrupts, which depend
     on the query constant, not on another tenant's row). On supabase/postgres the backend
     migrates as `postgres`, which is not a superuser, so there the same statement is applied
     by mytheus-platform's `deploy/postgres-init/harden-roles.sql` (new volumes) and the
     operator script (existing databases). The ALTER is idempotent and database-wide, so
     whichever runs first wins.

`chat_messages`' per-partition GIN index (5ed978467cad) is left alone: no query in any repo
searches chat today, and its expression is `to_tsvector(content)`, which can never be used
under RLS for the reason above. A chat search, when one is written, needs a stored tsv column.

Revision ID: b8e2c4f6a1d3
Revises: a7c3e9f1b2d4
Create Date: 2026-11-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "b8e2c4f6a1d3"
down_revision: str | None = "a7c3e9f1b2d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FTS_EXPRESSION = "to_tsvector('english', content || ' ' || coalesce(rationale, ''))"

# The deparsed (pg_get_expr) spelling of the per-row predicate, and its InitPlan rewrite.
_PER_ROW = "(NULLIF(current_setting('app.current_org'::text, true), ''::text))::uuid"
_INITPLAN = f"( SELECT {_PER_ROW})"
# Deparsed InitPlan, as pg_get_expr prints it back (with the output alias).
_INITPLAN_DEPARSED = f'( SELECT {_PER_ROW} AS "nullif")'

# Tables in `public` whose policies another repository owns.
_NOT_OURS = ("transcript_chunks",)


def _rewrite_policies(old: str, new: str, *, skip_if_contains: str) -> None:
    not_ours = ", ".join(f"'{t}'" for t in _NOT_OURS)
    op.execute(f"""
        DO $$
        DECLARE
          p record;
          q text;
          c text;
          clause text;
        BEGIN
          FOR p IN
            SELECT pol.polname, n.nspname, cl.relname,
                   pg_get_expr(pol.polqual, pol.polrelid) AS qual,
                   pg_get_expr(pol.polwithcheck, pol.polrelid) AS chk
            FROM pg_policy pol
            JOIN pg_class cl ON cl.oid = pol.polrelid
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            WHERE n.nspname = 'public'
              AND cl.relname NOT IN ({not_ours})
              AND coalesce(pg_get_expr(pol.polqual, pol.polrelid), '')
                  || coalesce(pg_get_expr(pol.polwithcheck, pol.polrelid), '')
                  LIKE '%app.current_org%'
          LOOP
            q := p.qual;
            c := p.chk;
            IF q IS NOT NULL AND position($s${skip_if_contains}$s$ IN q) = 0 THEN
              q := replace(q, $o${old}$o$, $n${new}$n$);
            END IF;
            IF c IS NOT NULL AND position($s${skip_if_contains}$s$ IN c) = 0 THEN
              c := replace(c, $o${old}$o$, $n${new}$n$);
            END IF;
            clause := '';
            IF q IS DISTINCT FROM p.qual THEN
              clause := clause || ' USING (' || q || ')';
            END IF;
            IF c IS DISTINCT FROM p.chk THEN
              clause := clause || ' WITH CHECK (' || c || ')';
            END IF;
            IF clause <> '' THEN
              EXECUTE format('ALTER POLICY %I ON %I.%I', p.polname, p.nspname, p.relname)
                      || clause;
            END IF;
          END LOOP;
        END $$;
        """)


def _mark_leakproof_if_superuser() -> None:
    op.execute("""
        DO $$
        BEGIN
          IF (SELECT bool_and(proleakproof) FROM pg_proc
              WHERE oid IN ('ts_match_vq(tsvector,tsquery)'::regprocedure,
                            'ts_match_qv(tsquery,tsvector)'::regprocedure)) THEN
            RETURN;
          END IF;
          IF (SELECT rolsuper FROM pg_roles WHERE rolname = current_user) THEN
            ALTER FUNCTION ts_match_vq(tsvector, tsquery) LEAKPROOF;
            ALTER FUNCTION ts_match_qv(tsquery, tsvector) LEAKPROOF;
          ELSE
            RAISE WARNING 'ts_match_vq is not LEAKPROOF and % is not a superuser: full-text '
              'search under RLS cannot use its GIN index until mytheus-platform '
              'deploy/postgres-init/harden-roles.sql runs as a superuser', current_user;
          END IF;
        END $$;
        """)


def upgrade() -> None:
    _rewrite_policies(_PER_ROW, _INITPLAN, skip_if_contains="SELECT")

    # btree_gin lets a GIN index carry the uuid org_id next to the tsvector. A trusted
    # extension, so the non-superuser `postgres` on supabase/postgres may create it; it goes
    # in `extensions` where that schema exists (Supabase's convention), else the default.
    op.execute("""
        DO $$
        BEGIN
          IF EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'extensions') THEN
            CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA extensions;
          ELSE
            CREATE EXTENSION IF NOT EXISTS btree_gin;
          END IF;
        END $$;
        """)
    op.execute(
        f"ALTER TABLE decisions ADD COLUMN search_tsv tsvector "
        f"GENERATED ALWAYS AS ({FTS_EXPRESSION}) STORED;"
    )
    op.execute("CREATE INDEX ix_decisions_org_search_tsv ON decisions USING gin (org_id, search_tsv);")
    op.execute("DROP INDEX IF EXISTS ix_decisions_fts;")
    _mark_leakproof_if_superuser()


def downgrade() -> None:
    # The LEAKPROOF marking and the extension are database-wide and may be relied on by
    # another repository's objects, so they are deliberately left in place.
    op.execute(
        f"CREATE INDEX IF NOT EXISTS ix_decisions_fts ON decisions USING gin ({FTS_EXPRESSION}) "
        "WHERE status = 'confirmed' AND deleted_at IS NULL;"
    )
    op.execute("DROP INDEX IF EXISTS ix_decisions_org_search_tsv;")
    op.execute("ALTER TABLE decisions DROP COLUMN IF EXISTS search_tsv;")
    _rewrite_policies(_INITPLAN_DEPARSED, _PER_ROW, skip_if_contains="#never-present#")
