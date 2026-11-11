"""feat: decision reversals, delegated answers, promise drift and the weekly digest

Polish program, wave 3, "Only Mytheus can do these". Every table is written only by core-api
(`email_deliveries` stays notification-service's; only its kind CHECK widens).

- `decision_reversals`: one row per (replacing decision, linked open action). When a confirmed
  decision is superseded (`POST /decisions/{id}/confirm` with `supersedes`), each open
  (`confirmed`) action of the replaced decision's meeting that has an owner gets a row and its
  owner an in-app notification ("Decision X was replaced by Y: re-scope, keep or close this
  task?"). `status` moves once from `pending` to `kept`, `rescoped` or `closed`.
  UNIQUE (new_decision_id, action_id) makes a re-confirm a no-op.
- `delegated_answer_statements`: what a person who sent their agent (ADR-0039 B5) pre-approved
  for one meeting, at most 10 live statements of up to 300 characters. Replaced as a list;
  a removed statement is soft-deleted so the log keeps pointing at what was approved.
- `delegated_answer_log`: every answer that person's agent gave in the meeting, keyed by the
  agent's own id (`ON CONFLICT (id) DO NOTHING` makes the agent-results replay safe): who
  asked, which statement, the answer, and whether a statement covered it.
- `promise_drift_nudges`: one row per (action, stage, due date). Stage `owner` is the private
  nudge, `host` the note to the meeting host two days later. UNIQUE (action_id, stage,
  due_date) is the daily job's idempotency; a new due date starts a new cycle.
- `weekly_digests`: one per (user, ISO week start), the digest content as JSON.
- `digest_preferences`: the weekly digest toggle and its email opt-in, one row per user,
  defaults (on, no email) when absent.
- `action_items.status_changed_at`, `decisions.status_changed_at`: set by a trigger whenever
  `status` changes, so the digest can say what was confirmed or closed THIS week without every
  writer remembering a column.
- `action_status_requests.requested_by`: a status request a person asked for from the
  "Should this be a meeting?" hint (null for the workflow's pre-meeting requests).
- `email_deliveries.kind` also allows `weekly_digest`.

Revision ID: b4c5d6e7f8a9
Revises: c5d6e7f8a9b0
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

from migrations._grants import grant_tenant_table

revision: str = "b4c5d6e7f8a9"
down_revision: Union[str, None] = "c5d6e7f8a9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_TABLES = (
    "decision_reversals",
    "delegated_answer_statements",
    "delegated_answer_log",
    "promise_drift_nudges",
    "weekly_digests",
    "digest_preferences",
)


def _rls(table: str, comment: str) -> None:
    op.execute(f"CREATE INDEX ix_{table}_org_id ON {table} (org_id);")
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )
    grant_tenant_table(op, table)
    op.execute(f"COMMENT ON TABLE {table} IS '{comment}';")


def upgrade() -> None:
    # --- decision_reversals ---------------------------------------------------------------------
    op.execute("""
        CREATE TABLE decision_reversals (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          old_decision_id uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          new_decision_id uuid        NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
          action_id       uuid        NOT NULL REFERENCES action_items(id) ON DELETE CASCADE,
          owner_user_id   uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          status          text        NOT NULL DEFAULT 'pending',
          resolved_by     uuid        REFERENCES users(id) ON DELETE SET NULL,
          resolved_at     timestamptz,
          created_at      timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_decision_reversals PRIMARY KEY (id),
          CONSTRAINT uq_decision_reversals_new_action UNIQUE (new_decision_id, action_id),
          CONSTRAINT ck_decision_reversals_status
            CHECK (status IN ('pending', 'kept', 'rescoped', 'closed')),
          CONSTRAINT ck_decision_reversals_resolved
            CHECK ((status = 'pending') = (resolved_at IS NULL))
        );
        """)
    op.execute(
        "CREATE INDEX ix_decision_reversals_owner_status "
        "ON decision_reversals (owner_user_id, status, created_at DESC);"
    )
    op.execute("CREATE INDEX ix_decision_reversals_action ON decision_reversals (action_id);")
    _rls(
        "decision_reversals",
        "An open action whose decision was superseded, and what its owner chose (keep, "
        "re-scope or close). Written only by core-api.",
    )

    # --- delegated_answer_statements -----------------------------------------------------------
    op.execute("""
        CREATE TABLE delegated_answer_statements (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id  uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          text        text        NOT NULL,
          position    integer     NOT NULL DEFAULT 0,
          created_at  timestamptz NOT NULL DEFAULT now(),
          updated_at  timestamptz NOT NULL DEFAULT now(),
          deleted_at  timestamptz,
          CONSTRAINT pk_delegated_answer_statements PRIMARY KEY (id),
          CONSTRAINT ck_delegated_answer_statements_text
            CHECK (char_length(text) BETWEEN 1 AND 300),
          CONSTRAINT ck_delegated_answer_statements_position CHECK (position BETWEEN 0 AND 99)
        );
        """)
    op.execute(
        "CREATE INDEX ix_delegated_answer_statements_meeting_user "
        "ON delegated_answer_statements (meeting_id, user_id) WHERE deleted_at IS NULL;"
    )
    _rls(
        "delegated_answer_statements",
        "Answers a person pre-approved for their agent to give in one meeting they sent it "
        "to. Written only by core-api.",
    )

    # --- delegated_answer_log -------------------------------------------------------------------
    op.execute("""
        CREATE TABLE delegated_answer_log (
          id             uuid        NOT NULL,
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          meeting_id     uuid        NOT NULL REFERENCES meetings(id) ON DELETE CASCADE,
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          statement_id   uuid        REFERENCES delegated_answer_statements(id) ON DELETE SET NULL,
          statement_text text,
          question       text        NOT NULL,
          answer         text        NOT NULL,
          covered        boolean     NOT NULL,
          asked_by       uuid        REFERENCES users(id) ON DELETE SET NULL,
          window_id      text,
          created_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_delegated_answer_log PRIMARY KEY (id),
          CONSTRAINT ck_delegated_answer_log_question CHECK (char_length(question) BETWEEN 1 AND 1000),
          CONSTRAINT ck_delegated_answer_log_answer CHECK (char_length(answer) BETWEEN 1 AND 1000)
        );
        """)
    op.execute(
        "CREATE INDEX ix_delegated_answer_log_meeting_user "
        "ON delegated_answer_log (meeting_id, user_id, created_at);"
    )
    _rls(
        "delegated_answer_log",
        "Every answer a person''s agent gave in a meeting from what they pre-approved, or its "
        "refusal. Written only by core-api (agent-results consumer).",
    )

    # --- promise_drift_nudges -------------------------------------------------------------------
    op.execute("""
        CREATE TABLE promise_drift_nudges (
          id          uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id      uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          action_id   uuid        NOT NULL REFERENCES action_items(id) ON DELETE CASCADE,
          stage       text        NOT NULL,
          user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          due_date    date        NOT NULL,
          issue_key   text        NOT NULL,
          issue_url   text,
          created_at  timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_promise_drift_nudges PRIMARY KEY (id),
          CONSTRAINT uq_promise_drift_nudges_action_stage_due UNIQUE (action_id, stage, due_date),
          CONSTRAINT ck_promise_drift_nudges_stage CHECK (stage IN ('owner', 'host'))
        );
        """)
    _rls(
        "promise_drift_nudges",
        "Nudges about a promise whose Jira issue is still open after its due date: the owner "
        "first, the meeting host two days later. Written only by core-api.",
    )

    # --- weekly_digests -------------------------------------------------------------------------
    op.execute("""
        CREATE TABLE weekly_digests (
          id           uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id       uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id      uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          week_start   date        NOT NULL,
          content      jsonb       NOT NULL,
          item_count   integer     NOT NULL DEFAULT 0,
          email_status text,
          created_at   timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_weekly_digests PRIMARY KEY (id),
          CONSTRAINT uq_weekly_digests_user_week UNIQUE (user_id, week_start),
          CONSTRAINT ck_weekly_digests_item_count CHECK (item_count >= 0)
        );
        """)
    op.execute(
        "CREATE INDEX ix_weekly_digests_user_week ON weekly_digests (user_id, week_start DESC);"
    )
    _rls(
        "weekly_digests",
        "One what-changed-for-you digest per person per week, built from the decision graph. "
        "Written only by core-api.",
    )

    # --- digest_preferences ---------------------------------------------------------------------
    op.execute("""
        CREATE TABLE digest_preferences (
          user_id        uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          org_id         uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          weekly_enabled boolean     NOT NULL DEFAULT true,
          email_enabled  boolean     NOT NULL DEFAULT false,
          updated_at     timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT pk_digest_preferences PRIMARY KEY (user_id)
        );
        """)
    _rls(
        "digest_preferences",
        "Whether a person gets the weekly digest, and by email too. Written only by core-api.",
    )

    # --- status_changed_at on decisions and action_items ---------------------------------------
    op.execute("ALTER TABLE decisions ADD COLUMN status_changed_at timestamptz;")
    op.execute("ALTER TABLE action_items ADD COLUMN status_changed_at timestamptz;")
    op.execute("""
        CREATE FUNCTION mx_touch_status_changed_at() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.status IS DISTINCT FROM OLD.status THEN
            NEW.status_changed_at := now();
          END IF;
          RETURN NEW;
        END $$;
        """)
    for table in ("decisions", "action_items"):
        op.execute(
            f"CREATE TRIGGER trg_{table}_status_changed_at BEFORE UPDATE OF status ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION mx_touch_status_changed_at();"
        )

    # --- action_status_requests.requested_by ----------------------------------------------------
    op.execute(
        "ALTER TABLE action_status_requests "
        "ADD COLUMN requested_by uuid REFERENCES users(id) ON DELETE SET NULL;"
    )

    # --- email_deliveries.kind ------------------------------------------------------------------
    op.execute("""
        ALTER TABLE email_deliveries
          DROP CONSTRAINT ck_email_deliveries_kind,
          ADD CONSTRAINT ck_email_deliveries_kind
            CHECK (kind IN ('meeting_followup', 'weekly_digest'));
        """)


def downgrade() -> None:
    op.execute("DELETE FROM email_deliveries WHERE kind = 'weekly_digest';")
    op.execute("""
        ALTER TABLE email_deliveries
          DROP CONSTRAINT ck_email_deliveries_kind,
          ADD CONSTRAINT ck_email_deliveries_kind CHECK (kind IN ('meeting_followup'));
        """)
    op.execute("ALTER TABLE action_status_requests DROP COLUMN IF EXISTS requested_by;")
    for table in ("decisions", "action_items"):
        op.execute(f"DROP TRIGGER IF EXISTS trg_{table}_status_changed_at ON {table};")
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS status_changed_at;")
    op.execute("DROP FUNCTION IF EXISTS mx_touch_status_changed_at();")
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table};")
