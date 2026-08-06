"""feat(meeting-service): add meeting_rooms

Plan 3-4 section 3.1, step 2 (Task 2 of the band). `meeting_rooms` is the LiveKit room
materialization for a `core-api` meeting: one row per meeting, holding the LiveKit room
name, room lifecycle status, and (later, once Tasks 3/4 land) the egress/recording
reference. This migration only creates the table and its RLS policy; the token-mint route,
webhook handler, and `meeting-events` consumer that populate and read it are out of scope
for this migration and land in Tasks 3 and 4.

Schema and RLS shape match the plan text verbatim: `PRIMARY KEY (id)` plus two UNIQUE
constraints (`(org_id, meeting_id)` and `(org_id, livekit_room)`), not a composite PK
including `org_id`. Every other tenant table in this schema (`meetings`, `rbac_grants`,
`audit_logs`, ...) also keys on a bare `id` with `org_id` as a plain indexed column plus the
`tenant_isolation` policy, so this is not a divergence from house style, just confirmation
that the plan text already matches it.

Revision ID: 3d13f46b960f
Revises: c3e8b1d47f92
Create Date: 2026-08-05 23:03:19.972426

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '3d13f46b960f'
down_revision: Union[str, None] = 'c3e8b1d47f92'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Identical to 82d44163c933's and b7d9f3a55c21's helper: NULLIF(current_setting(..., true), '')
# so an unset OR reset-to-empty app.current_org yields NULL, the predicate is false, and the
# query returns zero rows (fail-closed) rather than raising.
_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _force_rls(table: str) -> None:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )


def upgrade() -> None:
    op.create_table(
        "meeting_rooms",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("meeting_id", sa.UUID(), nullable=False),
        sa.Column("livekit_room", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'pending'"), nullable=False),
        sa.Column("egress_id", sa.Text(), nullable=True),
        sa.Column("recording_ref", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name=op.f("fk_meeting_rooms_org_id_organizations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["meeting_id"], ["meetings.id"], name=op.f("fk_meeting_rooms_meeting_id_meetings")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_meeting_rooms")),
        sa.UniqueConstraint("org_id", "meeting_id", name="uq_meeting_rooms_org_id_meeting_id"),
        sa.UniqueConstraint(
            "org_id", "livekit_room", name="uq_meeting_rooms_org_id_livekit_room"
        ),
    )
    op.create_index(op.f("ix_meeting_rooms_org_id"), "meeting_rooms", ["org_id"], unique=False)
    _force_rls("meeting_rooms")


def downgrade() -> None:
    op.drop_index(op.f("ix_meeting_rooms_org_id"), table_name="meeting_rooms")
    op.drop_table("meeting_rooms")
