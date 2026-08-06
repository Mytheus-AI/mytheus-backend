"""org_hierarchy_missing_indexes_parent_and_member_lookup

Plan 5-6 Task 1 (3.1). `teams`, `team_members` and `departments` are NOT new here - they were
created in `82d44163c933` (0001) and extended in `b7d9f3a55c21` (0003), with RLS FORCE and the
canonical `tenant_isolation` policy already live on all three (verified against the running dev
DB, not re-created). The only genuine gap against the plan's outline is two lookup indexes it
names that neither of those migrations added: `departments.parent_id` (org-tree parent walk)
and `team_members.user_id` (a user's team memberships, read heavily by `get_user_context` in
Task 3). Both tables already carry a plain (non-partitioned) `ix_<table>_org_id` index from
0001, so no RLS-on-partition or default-privilege concern applies here - see migration
`c3e8b1d47f92`'s comment for that hazard; it is not this migration's shape.

Named `ix_departments_parent_id` / `ix_team_members_user_id` to match this repo's existing
`ix_<table>_<column>` convention (see `ix_departments_org_id`, `ix_teams_org_id`,
`ix_team_members_org_id`, all from 0001), not the plan text's literal
`departments_parent_idx` / `team_members_user_idx` - the plan's "Canonical names" section
freezes table/column/`org_id`/`app.current_org` spellings, not index names, so this is a
same-repo consistency choice rather than a canon deviation.

Revision ID: 56f72ac73fa9
Revises: f3a7c9e0b21d
Create Date: 2026-08-06 07:30:31.215265

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = '56f72ac73fa9'
down_revision: Union[str, None] = 'f3a7c9e0b21d'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_departments_parent_id", "departments", ["parent_id"], unique=False, if_not_exists=True
    )
    op.create_index(
        "ix_team_members_user_id", "team_members", ["user_id"], unique=False, if_not_exists=True
    )


def downgrade() -> None:
    op.drop_index("ix_team_members_user_id", table_name="team_members", if_exists=True)
    op.drop_index("ix_departments_parent_id", table_name="departments", if_exists=True)
