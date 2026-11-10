"""merge the two ADR-0039 heads: company brain (identity) and meetings (core-api)

Revision ID: e3c4d5e6f7a8
Revises: c1a2b3d4e5f6, d2b3c4d5e6f7
Create Date: 2026-11-10

Both migrations were built in parallel off 0b91a00079a7 and touch disjoint tables, so this
revision only joins the history back into a single head.
"""

from collections.abc import Sequence

revision: str = "e3c4d5e6f7a8"
down_revision: str | Sequence[str] | None = ("c1a2b3d4e5f6", "d2b3c4d5e6f7")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
