"""feat: what departments and teams do (People structure)

Adds `description` to `departments` and `teams`: what the unit does and what it is responsible
for, written by a workspace admin on the People page. Agents read it through the company brain
(`GetOrgContext`, `OrgContext.departments` / `OrgContext.teams`) to tune notes to each person's
role and to notice when the team responsible for a topic is missing from a meeting.

Nullable text, at most 1000 characters, enforced here as well as in the request schema so a
write path that skips the schema still cannot store an essay into every agent's prompt.

Both tables already carry `org_id`, RLS ENABLE + FORCE and the `tenant_isolation` policy (0001 /
0003), and their grants; adding a column changes none of that.

Revision ID: c8d9e0f1a2b3
Revises: 9c8d7e6f5a4b (users.display_name; itself off e4f5a6b7c8d9)
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op

revision: str = "c8d9e0f1a2b3"
down_revision: Union[str, None] = "9c8d7e6f5a4b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

MAX_DESCRIPTION = 1000


def upgrade() -> None:
    for table in ("departments", "teams"):
        op.execute(f"ALTER TABLE {table} ADD COLUMN description text;")
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT ck_{table}_description_length "
            f"CHECK (description IS NULL OR char_length(description) <= {MAX_DESCRIPTION});"
        )
        op.execute(
            f"COMMENT ON COLUMN {table}.description IS "
            f"'What it does and is responsible for (admin-written, at most {MAX_DESCRIPTION} "
            "characters). Agents read it via GetOrgContext.';"
        )


def downgrade() -> None:
    for table in ("teams", "departments"):
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS ck_{table}_description_length;")
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS description;")
