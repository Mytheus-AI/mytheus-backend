"""fix: ops_audit_log is readable only inside a staff console transaction

`ops_audit_log` (e2b6c4a8f913) records Mytheus staff actions about every tenant (`target_org_id`,
free-form `details` such as a suspension reason). It was created with no row-level security and
`SELECT, INSERT` for `mytheus_app`, the role every request handler connects as, so any tenant's
request session could read the staff log about every other tenant (the isolation probe found it;
`research/metrics/fixes/probes/ops_audit_probe.sql` shows tenant A reading a row about tenant B).

It is operator data, not tenant data: no tenant ever reads it, only admin-ops-service's staff-only
routes do. There is no separate ops database role in this deployment (every service connects as
`mytheus_app`; `mytheus_admin` is deliberately absent from the web process), so the "ops path"
is the one this codebase already uses for `feature_flags` and `feedback_reports`: the transaction-
local `app.ops_console = 'on'` that only `enable_ops_console()` sets, inside routes that have
already passed `require_staff`. So:

- ENABLE + FORCE ROW LEVEL SECURITY;
- `ops_console_read` FOR SELECT, only when `app.ops_console` is 'on' in this transaction;
- `ops_append` FOR INSERT WITH CHECK (true): every caller that holds the INSERT grant may append,
  because a refused staff call must be logged BEFORE the caller is known to be staff
  (`require_staff` -> `ops_log_now`). Appending reveals nothing.
- Grants unchanged: SELECT, INSERT (append-only by GRANT, as before). No UPDATE/DELETE policy is
  needed because no role holds those privileges.

Residual risk, the same one e2b6c4a8f913 records for `set_organization_status`: code running as
`mytheus_app` could set the GUC itself. The control is that `enable_ops_console` is the only
setter and every caller is behind `require_staff`; the upgrade path is a dedicated ops role.

Revision ID: d2e4f6a8b0c1
Revises: c9f1d3e5a7b2
Create Date: 2026-11-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "d2e4f6a8b0c1"
down_revision: str | None = "c9f1d3e5a7b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OPS_CONSOLE = "current_setting('app.ops_console', true) = 'on'"


def upgrade() -> None:
    op.execute("ALTER TABLE ops_audit_log ENABLE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE ops_audit_log FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY ops_console_read ON ops_audit_log FOR SELECT USING ({_OPS_CONSOLE});"
    )
    op.execute("CREATE POLICY ops_append ON ops_audit_log FOR INSERT WITH CHECK (true);")


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS ops_append ON ops_audit_log;")
    op.execute("DROP POLICY IF EXISTS ops_console_read ON ops_audit_log;")
    op.execute("ALTER TABLE ops_audit_log NO FORCE ROW LEVEL SECURITY;")
    op.execute("ALTER TABLE ops_audit_log DISABLE ROW LEVEL SECURITY;")
