"""feat(admin-ops): feedback_reports, bug reports and feedback to Mytheus staff (ADR-0046 D6)

A member writes a bug report or feedback from Settings; Mytheus staff read every report in the
ops Feedback inbox. Three policies, so each reach is stated:

- `tenant_insert` (FOR INSERT): a workspace session may add rows for its own workspace only.
- `tenant_isolation` (FOR SELECT): a workspace session reads its own workspace's rows (the route
  narrows that to the caller's own reports).
- `ops_console` (ALL): full access, only inside a transaction that set `app.ops_console = 'on'`
  transaction-locally, the same seam `feature_flags` uses. admin-ops-service's staff-only
  feedback routes are the only code that sets it for this table.

There is no tenant UPDATE or DELETE policy, so a member cannot change a report's status or
remove one once sent; with RLS forced, an UPDATE from a workspace session matches no row.

Revision ID: c7d2e3f4a5b6
Revises: b6c1d2e3f4a5
Create Date: 2026-11-13
"""

from typing import Sequence, Union

from alembic import op

from migrations._grants import ADMIN_ROLE, APP_ROLE

revision: str = "c7d2e3f4a5b6"
down_revision: Union[str, None] = "b6c1d2e3f4a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TENANT = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"
_OPS_CONSOLE = "current_setting('app.ops_console', true) = 'on'"


def upgrade() -> None:
    op.execute("""
        CREATE TABLE feedback_reports (
          id              uuid        NOT NULL DEFAULT gen_random_uuid(),
          org_id          uuid        NOT NULL REFERENCES organizations(id) ON DELETE RESTRICT,
          user_id         uuid        NOT NULL,
          reporter_email  text        NOT NULL,
          kind            text        NOT NULL,
          message         text        NOT NULL,
          page            text,
          status          text        NOT NULL DEFAULT 'new',
          created_at      timestamptz NOT NULL DEFAULT now(),
          updated_at      timestamptz NOT NULL DEFAULT now(),
          handled_by      uuid,
          CONSTRAINT pk_feedback_reports PRIMARY KEY (id),
          CONSTRAINT ck_feedback_reports_kind CHECK (kind IN ('bug', 'feedback')),
          CONSTRAINT ck_feedback_reports_status CHECK (status IN ('new', 'seen', 'resolved')),
          CONSTRAINT ck_feedback_reports_message CHECK (char_length(message) BETWEEN 1 AND 4000),
          CONSTRAINT ck_feedback_reports_page CHECK (page IS NULL OR char_length(page) <= 300)
        );
        """)
    op.execute(
        "CREATE INDEX ix_feedback_reports_status_created ON feedback_reports (status, created_at);"
    )
    op.execute(
        "CREATE INDEX ix_feedback_reports_org_user "
        "ON feedback_reports (org_id, user_id, created_at);"
    )
    op.execute("ALTER TABLE feedback_reports ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE feedback_reports FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_insert ON feedback_reports FOR INSERT WITH CHECK ({_TENANT});"
    )
    op.execute(f"CREATE POLICY tenant_isolation ON feedback_reports FOR SELECT USING ({_TENANT});")
    op.execute(
        f"CREATE POLICY ops_console ON feedback_reports USING ({_OPS_CONSOLE}) "
        f"WITH CHECK ({_OPS_CONSOLE});"
    )
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON feedback_reports TO {APP_ROLE}, {ADMIN_ROLE};")
    op.execute(
        "COMMENT ON TABLE feedback_reports IS "
        "'Bug reports and feedback members send to Mytheus staff (ADR-0046 D6).';"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS feedback_reports CASCADE;")
