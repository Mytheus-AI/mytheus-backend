"""feat: users.display_name, the person's real name

Mytheus showed people by a name guessed from their email (`nnm23ad071@nmamit.in` ->
"Nnm23ad071") because `users` had no name column: Clerk owns the name and the backend never
learned it. This adds a nullable `display_name`, written by identity-tenant-service only (its
one writer, like every other `users` column):

- the `/clerk/webhook` sync (`user.created` / `user.updated`) stores Clerk's first + last name;
- `PATCH /users/{user_id}` lets the person set their own (self only).

Readers prefer it and fall back to the email-derived name (`core_api.records.person_name`).
NULL means "not known yet", never an empty string.

No grant change: `users` already has its table-level grants, and a new column inherits them.
No RLS change: the existing `tenant_isolation` policy covers the row.

Revision ID: 9c8d7e6f5a4b
Revises: e4f5a6b7c8d9
Create Date: 2026-11-11
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "9c8d7e6f5a4b"
down_revision: Union[str, None] = "e4f5a6b7c8d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("users", sa.Column("display_name", sa.Text(), nullable=True))
    op.create_check_constraint(
        "ck_users_display_name_length",
        "users",
        "display_name IS NULL OR (char_length(display_name) BETWEEN 1 AND 80)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_users_display_name_length", "users", type_="check")
    op.drop_column("users", "display_name")
