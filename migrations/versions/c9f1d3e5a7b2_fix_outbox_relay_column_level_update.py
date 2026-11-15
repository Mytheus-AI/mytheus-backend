"""fix: the outbox relay may UPDATE only the drain columns

Journal plan Part 3 fix 4 (outbox). `a1c4e7f20b18` granted `mytheus_relay` table-level
`SELECT, UPDATE ON outbox`, paired with the permissive `outbox_relay_drain` policy
(`USING (true) WITH CHECK (true)`) that lets it see every tenant's rows. Together they let the
relay rewrite any pending event's `org_id`, `topic` or `payload` before publishing it: an
event could be re-keyed into another tenant's partition or forged in place, and the probe in
the research metrics folder shows all three UPDATEs succeeding.

`libs/shared/relay.py` writes exactly two columns, `published_at` and `attempts`. The table
grant becomes a column grant on those two, so every other column is immutable to the relay at
the ACL layer, which no policy expression can widen. SELECT stays table-level (the relay reads
the row to publish it), and `SELECT ... FOR UPDATE SKIP LOCKED` still works because it needs
UPDATE on at least one column.

Revision ID: c9f1d3e5a7b2
Revises: b8e2c4f6a1d3
Create Date: 2026-11-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c9f1d3e5a7b2"
down_revision: str | None = "b8e2c4f6a1d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RELAY_ROLE = "mytheus_relay"
DRAIN_COLUMNS = "published_at, attempts"


def upgrade() -> None:
    op.execute(f"REVOKE UPDATE ON outbox FROM {RELAY_ROLE};")
    op.execute(f"GRANT UPDATE ({DRAIN_COLUMNS}) ON outbox TO {RELAY_ROLE};")


def downgrade() -> None:
    op.execute(f"REVOKE UPDATE ({DRAIN_COLUMNS}) ON outbox FROM {RELAY_ROLE};")
    op.execute(f"GRANT UPDATE ON outbox TO {RELAY_ROLE};")
