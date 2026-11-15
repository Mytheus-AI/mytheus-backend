"""feat: cards record the prompt version and model that produced them

Journal plan Part 3 fix 11 (attributable, reproducible outputs). mytheus-events added the
optional `prompt_version` and `model_id` to `agent-results` (BACKWARD: default null), and
agent-runtime-service fills them where results are built. core-api stores them on the four
tables a card lives in: `worklogs`, `decisions`, `action_items`, `agent_suggestions`.

Additive only: two nullable text columns per table, no default, no backfill (rows written
before this read null, which the API documents as "unknown"). `agent_suggestions` is
partitioned; a column added to the parent is added to every partition. No grant changes: the
tables' existing grants are table-level, so the new columns are covered.

Revision ID: e3f5a7b9c1d2
Revises: d2e4f6a8b0c1
Create Date: 2026-11-15
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e3f5a7b9c1d2"
down_revision: str | None = "d2e4f6a8b0c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CARD_TABLES = ("worklogs", "decisions", "action_items", "agent_suggestions")


def upgrade() -> None:
    for table in CARD_TABLES:
        op.execute(
            f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS prompt_version text, "
            f"ADD COLUMN IF NOT EXISTS model_id text;"
        )


def downgrade() -> None:
    for table in CARD_TABLES:
        op.execute(
            f"ALTER TABLE {table} DROP COLUMN IF EXISTS model_id, "
            f"DROP COLUMN IF EXISTS prompt_version;"
        )
