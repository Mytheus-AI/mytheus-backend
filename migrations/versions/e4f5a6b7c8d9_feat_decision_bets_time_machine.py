"""feat: decisions as bets and the company time machine

"Beyond the meeting", Layer 3. Every table is written only by core-api.

- `decision_bets`: what one decision expects to happen, at most one live bet per decision
  ("Moving to annual billing will cut churn by 3 points by March"). A bet is a metric or an
  event, a direction and size, a check date and a source. DecisionAgent proposes one after
  the meeting (`proposed_by = 'agent'`, `status = 'proposed'`) when the meeting said so; the
  owner accepts or edits it (`open`), or adds one by hand. On the check date the daily
  `DecisionBetsWorkflow` gathers evidence and moves it to `due`; the owner resolves it as
  `hit`, `partly` or `missed` (`resolved`). `suggested_outcome` is what a connected metric
  says, only ever shown for confirmation: no bet is resolved without a person.
  `assumption` is what had to be true; `assumption_held` and `lesson` are what the owner
  writes at resolution, the material of the company page ("How good are our calls").
- `decision_bet_evidence`: what the check found, one row per piece: a metric reading, a later
  decision or meeting that mentions it, a Jira outcome, a note. UNIQUE (bet_id, kind, ref)
  makes the daily job's re-run a no-op.
- `record_status_history`: append-only status transitions of `decisions` and `action_items`
  (one row per insert and per status change), by trigger, so the time machine can say what
  was in force on any date. Backfilled from `created_at` and `status_changed_at`. Append-only
  by GRANT (SELECT, INSERT), like `audit_logs`. It holds ids and statuses, never content.

Every table: org_id, RLS ENABLE + FORCE with the canonical NULLIF `tenant_isolation` policy.

Revision ID: e4f5a6b7c8d9
Revises: e7f8a9b0c1d2
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_append_only_table, grant_tenant_table

revision: str = "e4f5a6b7c8d9"
down_revision: Union[str, None] = "e7f8a9b0c1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _rls(table: str, comment: str, *, append_only: bool = False) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    if append_only:
        grant_append_only_table(op, table)
    else:
        grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    # --- decision_bets --------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE decision_bets (
          id                uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id            uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          decision_id       uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          meeting_id        uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          owner_user_id     uuid        REFERENCES users(id) ON DELETE SET NULL,
          team_id           uuid        REFERENCES teams(id) ON DELETE SET NULL,
          status            text        NOT NULL DEFAULT 'open',
          statement         text        NOT NULL,
          kind              text        NOT NULL DEFAULT 'metric',
          metric            text        NOT NULL,
          direction         text        NOT NULL,
          magnitude         numeric,
          unit              text,
          baseline          numeric,
          check_date        date        NOT NULL,
          source            text        NOT NULL DEFAULT 'meetings',
          metric_key        text,
          category          text        NOT NULL DEFAULT 'other',
          confidence_pct    smallint,
          assumption        text,
          quote             text,
          quote_speaker_id  uuid        REFERENCES users(id) ON DELETE SET NULL,
          quote_start_ms    integer,
          proposed_by       text        NOT NULL DEFAULT 'person',
          prompt_version    text,
          agent_confidence  real,
          evidence_status   text        NOT NULL DEFAULT 'pending',
          checked_at        timestamptz,
          suggested_outcome text,
          suggested_reason  text,
          observed_value    numeric,
          outcome           text,
          assumption_held   boolean,
          lesson            text,
          resolved_by       uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at       timestamptz,
          created_by        uuid        REFERENCES users(id) ON DELETE SET NULL,
          created_at        timestamptz NOT NULL DEFAULT now(),
          updated_at        timestamptz NOT NULL DEFAULT now(),
          deleted_at        timestamptz,
          CONSTRAINT pk_decision_bets PRIMARY KEY (id),
          CONSTRAINT ck_decision_bets_status
            CHECK (status IN ('proposed', 'open', 'due', 'resolved', 'dismissed')),
          CONSTRAINT ck_decision_bets_kind CHECK (kind IN ('metric', 'event')),
          CONSTRAINT ck_decision_bets_direction
            CHECK (direction IN ('increase', 'decrease', 'reach', 'hold', 'happen', 'not_happen')),
          CONSTRAINT ck_decision_bets_source
            CHECK (source IN ('crm', 'metrics', 'meetings', 'jira', 'manual')),
          CONSTRAINT ck_decision_bets_category CHECK (category IN (
            'pricing', 'product', 'growth', 'customers', 'hiring', 'operations',
            'engineering', 'finance', 'other')),
          CONSTRAINT ck_decision_bets_statement CHECK (char_length(statement) BETWEEN 1 AND 300),
          CONSTRAINT ck_decision_bets_metric CHECK (char_length(metric) BETWEEN 1 AND 120),
          CONSTRAINT ck_decision_bets_unit CHECK (unit IS NULL OR char_length(unit) <= 20),
          CONSTRAINT ck_decision_bets_metric_key
            CHECK (metric_key IS NULL OR char_length(metric_key) <= 60),
          CONSTRAINT ck_decision_bets_confidence
            CHECK (confidence_pct IS NULL OR confidence_pct BETWEEN 1 AND 99),
          CONSTRAINT ck_decision_bets_agent_confidence
            CHECK (agent_confidence IS NULL OR agent_confidence BETWEEN 0 AND 1),
          CONSTRAINT ck_decision_bets_assumption
            CHECK (assumption IS NULL OR char_length(assumption) <= 300),
          CONSTRAINT ck_decision_bets_quote CHECK (quote IS NULL OR char_length(quote) <= 300),
          CONSTRAINT ck_decision_bets_proposed_by CHECK (proposed_by IN ('agent', 'person')),
          CONSTRAINT ck_decision_bets_evidence_status
            CHECK (evidence_status IN ('pending', 'gathered')),
          CONSTRAINT ck_decision_bets_suggested
            CHECK (suggested_outcome IS NULL OR suggested_outcome IN ('hit', 'partly', 'missed')),
          CONSTRAINT ck_decision_bets_suggested_reason
            CHECK (suggested_reason IS NULL OR char_length(suggested_reason) <= 300),
          CONSTRAINT ck_decision_bets_outcome
            CHECK (outcome IS NULL OR outcome IN ('hit', 'partly', 'missed')),
          CONSTRAINT ck_decision_bets_lesson CHECK (lesson IS NULL OR char_length(lesson) <= 500),
          CONSTRAINT ck_decision_bets_resolved
            CHECK ((status = 'resolved') = (outcome IS NOT NULL AND resolved_at IS NOT NULL))
        );
        """)
    op.execute(
        "CREATE UNIQUE INDEX ux_decision_bets_decision ON decision_bets (decision_id) "
        "WHERE deleted_at IS NULL;"
    )
    op.execute(
        "CREATE INDEX ix_decision_bets_status_check ON decision_bets (status, check_date) "
        "WHERE deleted_at IS NULL;"
    )
    op.execute("CREATE INDEX ix_decision_bets_owner ON decision_bets (owner_user_id, status);")
    op.execute("CREATE INDEX ix_decision_bets_meeting ON decision_bets (meeting_id);")
    _rls(
        "decision_bets",
        "What a decision expects to happen, checked on a date and resolved by its owner "
        "(Decisions as bets). Written only by core-api.",
    )

    # --- decision_bet_evidence -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE decision_bet_evidence (
          id             uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          bet_id         uuid        NOT NULL REFERENCES decision_bets(id) ON DELETE CASCADE,
          kind           text        NOT NULL,
          ref            text        NOT NULL,
          title          text        NOT NULL,
          detail         text,
          url            text,
          meeting_id     uuid        REFERENCES meetings(id) ON DELETE CASCADE,
          decision_id    uuid        REFERENCES decisions(id) ON DELETE CASCADE,
          observed_value numeric,
          signal         text,
          occurred_at    timestamptz,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_bet_evidence PRIMARY KEY (id),
          CONSTRAINT uq_decision_bet_evidence_ref UNIQUE (bet_id, kind, ref),
          CONSTRAINT ck_decision_bet_evidence_kind
            CHECK (kind IN ('metric', 'meeting', 'decision', 'jira', 'note')),
          CONSTRAINT ck_decision_bet_evidence_signal
            CHECK (signal IS NULL OR signal IN ('supports', 'against', 'neutral')),
          CONSTRAINT ck_decision_bet_evidence_ref CHECK (char_length(ref) BETWEEN 1 AND 120),
          CONSTRAINT ck_decision_bet_evidence_title CHECK (char_length(title) BETWEEN 1 AND 300),
          CONSTRAINT ck_decision_bet_evidence_detail
            CHECK (detail IS NULL OR char_length(detail) <= 600),
          CONSTRAINT ck_decision_bet_evidence_url CHECK (url IS NULL OR char_length(url) <= 500)
        );
        """)
    op.execute(
        "CREATE INDEX ix_decision_bet_evidence_bet ON decision_bet_evidence (bet_id, created_at);"
    )
    _rls(
        "decision_bet_evidence",
        "What the check of a bet found: a metric, a later meeting or decision, a Jira outcome. "
        "Written only by core-api.",
    )

    # --- record_status_history -----------------------------------------------------------------
    op.execute("""
        CREATE TABLE record_status_history (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          record_kind text        NOT NULL,
          record_id   uuid        NOT NULL,
          from_status text,
          to_status   text        NOT NULL,
          changed_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_record_status_history PRIMARY KEY (id),
          CONSTRAINT ck_record_status_history_kind CHECK (record_kind IN ('decision', 'action'))
        );
        """)
    op.execute(
        "CREATE INDEX ix_record_status_history_record "
        "ON record_status_history (record_id, changed_at);"
    )
    op.execute(
        "CREATE INDEX ix_record_status_history_kind_time "
        "ON record_status_history (org_id, record_kind, changed_at);"
    )
    _rls(
        "record_status_history",
        "Append-only status transitions of decisions and action_items, by trigger (the time "
        "machine). Ids and statuses only, never content.",
        append_only=True,
    )
    op.execute("""
        CREATE FUNCTION mx_record_status_history() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF TG_OP = 'INSERT' OR NEW.status IS DISTINCT FROM OLD.status THEN
            INSERT INTO record_status_history
              (org_id, record_kind, record_id, from_status, to_status, changed_at)
            VALUES (
              NEW.org_id, TG_ARGV[0], NEW.id,
              CASE WHEN TG_OP = 'INSERT' THEN NULL ELSE OLD.status END,
              NEW.status, now()
            );
          END IF;
          RETURN NEW;
        END $$;
        """)
    for table, kind in (("decisions", "decision"), ("action_items", "action")):
        op.execute(
            f"CREATE TRIGGER trg_{table}_status_history AFTER INSERT OR UPDATE OF status "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION mx_record_status_history('{kind}');"
        )
        # Backfill: the row as created, then its current status when it moved since.
        op.execute(f"""
            INSERT INTO record_status_history
              (org_id, record_kind, record_id, from_status, to_status, changed_at)
            SELECT org_id, '{kind}', id, NULL,
                   CASE WHEN status_changed_at IS NULL THEN status ELSE 'proposed' END,
                   created_at
            FROM {table};
            """)
        op.execute(f"""
            INSERT INTO record_status_history
              (org_id, record_kind, record_id, from_status, to_status, changed_at)
            SELECT org_id, '{kind}', id, 'proposed', status, status_changed_at
            FROM {table} WHERE status_changed_at IS NOT NULL AND status <> 'proposed';
            """)


def downgrade() -> None:
    for table in ("decisions", "action_items"):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_status_history ON {table};")
    op.execute("DROP FUNCTION IF EXISTS mx_record_status_history();")
    op.execute("DROP TABLE IF EXISTS record_status_history;")
    op.execute("DROP TABLE IF EXISTS decision_bet_evidence;")
    op.execute("DROP TABLE IF EXISTS decision_bets;")
