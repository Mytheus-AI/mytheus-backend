"""0003 rbac_grants + the canonical columns the Plan 0 shells were missing

Plan 1-2 section 3.4, plus gap 13 of the band record.

Two separate things land here and they are related. `rbac_grants` is the authorization store,
and it exists as its own table specifically so that `tier` (what you may do) can never be
confused with `users.role` (what you do for a living). Everything else in this migration is
filling in columns that canon defines and Plan 0 stubbed out, which matters now rather than
later because `grpc_server.GetUserContext` has been returning a hardcoded `member` tier since
Plan 0 for want of a table to read.

Revision ID: b7d9f3a55c21
Revises: a1c4e7f20b18
Create Date: 2026-07-30

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b7d9f3a55c21"
down_revision: Union[str, None] = "a1c4e7f20b18"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_RLS_PREDICATE = "org_id = NULLIF(current_setting('app.current_org', true), '')::uuid"


def _force_rls(table: str) -> None:
    """The canonical policy, identical to 82d44163c933's helper.

    `NULLIF(current_setting(..., true), '')` is the fail-closed form: an unset or
    reset-to-empty GUC yields NULL, the predicate is false, and the query returns zero rows
    rather than raising. Stricter than the literal SQL in ADR-0002 D3, which the ADR was
    amended to record.
    """
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY;")
    op.execute(
        f"CREATE POLICY tenant_isolation ON {table} "
        f"USING ({_RLS_PREDICATE}) WITH CHECK ({_RLS_PREDICATE});"
    )


def upgrade() -> None:
    # --- rbac_grants ---------------------------------------------------------------------
    op.create_table(
        "rbac_grants",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("org_id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("tier", sa.Text(), nullable=False),
        sa.Column("granted_by", sa.UUID(), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["org_id"],
            ["organizations.id"],
            name=op.f("fk_rbac_grants_org_id_organizations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name=op.f("fk_rbac_grants_user_id_users")),
        sa.ForeignKeyConstraint(
            ["granted_by"], ["users.id"], name=op.f("fk_rbac_grants_granted_by_users")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_rbac_grants")),
        # One tier per user per tenant. Without this a user can hold two grants and the
        # answer to "what may they do" depends on which row the query happens to return.
        sa.UniqueConstraint("org_id", "user_id", name="uq_rbac_grants_org_id_user_id"),
        sa.CheckConstraint("tier IN ('owner','admin','member')", name="ck_rbac_grants_tier_valid"),
    )
    op.create_index(op.f("ix_rbac_grants_org_id"), "rbac_grants", ["org_id"], unique=False)
    _force_rls("rbac_grants")

    # --- users: per-tenant uniqueness, non-null skills ------------------------------------
    #
    # The global unique on clerk_user_id made one Clerk user a member of exactly one Mytheus
    # organization for all time. Clerk does not work that way: the same person can belong to
    # several organizations, and the second INSERT would have raised a constraint violation
    # from inside the webhook handler.
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS uq_users_clerk_user_id;")
    op.create_unique_constraint(
        "uq_users_org_id_clerk_user_id", "users", ["org_id", "clerk_user_id"]
    )
    op.execute("UPDATE users SET skills = '{}' WHERE skills IS NULL;")
    op.alter_column(
        "users", "skills", nullable=False, server_default=sa.text("'{}'"), existing_type=postgresql.ARRAY(sa.Text())
    )

    # --- tenant_settings: named compliance columns, not a jsonb bag -----------------------
    op.add_column(
        "tenant_settings",
        sa.Column("default_locale", sa.Text(), server_default=sa.text("'en'"), nullable=False),
    )
    op.add_column(
        "tenant_settings",
        sa.Column(
            "retention_days_transcripts", sa.Integer(), server_default=sa.text("365"), nullable=False
        ),
    )
    op.add_column(
        "tenant_settings",
        sa.Column(
            "retention_days_worklogs", sa.Integer(), server_default=sa.text("365"), nullable=False
        ),
    )
    op.add_column(
        "tenant_settings",
        sa.Column(
            "pii_redaction_enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False
        ),
    )
    op.add_column("tenant_settings", sa.Column("sso_config", postgresql.JSONB(), nullable=True))
    op.add_column(
        "tenant_settings",
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column(
        "tenant_settings",
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column(
        "tenant_settings",
        sa.Column("deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )
    # The generic bag goes: it was never read, and leaving it invites writes that shadow the
    # named columns above.
    op.drop_column("tenant_settings", "settings")
    op.execute("UPDATE tenant_settings SET agent_confidence_overrides = '{}'::jsonb "
               "WHERE agent_confidence_overrides IS NULL;")
    op.alter_column(
        "tenant_settings",
        "agent_confidence_overrides",
        nullable=False,
        server_default=sa.text("'{}'::jsonb"),
        existing_type=postgresql.JSONB(),
    )

    # --- data_residency: the full placement record ----------------------------------------
    op.add_column(
        "data_residency",
        sa.Column("db_cell", sa.Text(), server_default=sa.text("'pool-us-1'"), nullable=False),
    )
    op.add_column(
        "data_residency",
        sa.Column("qdrant_region", sa.Text(), server_default=sa.text("'us'"), nullable=False),
    )
    op.add_column(
        "data_residency",
        sa.Column(
            "object_bucket_region", sa.Text(), server_default=sa.text("'us'"), nullable=False
        ),
    )
    op.add_column(
        "data_residency",
        sa.Column("locked_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "region_valid", "data_residency", "region IN ('us','eu','ap')"
    )

    # --- teams / team_members -------------------------------------------------------------
    op.add_column("teams", sa.Column("department_id", sa.UUID(), nullable=True))
    op.create_foreign_key(
        op.f("fk_teams_department_id_departments"),
        "teams",
        "departments",
        ["department_id"],
        ["id"],
    )
    op.add_column("team_members", sa.Column("role", sa.Text(), nullable=True))

    # --- meetings / meeting_participants ---------------------------------------------------
    op.add_column(
        "meetings", sa.Column("scheduled_start", postgresql.TIMESTAMP(timezone=True), nullable=True)
    )
    op.add_column(
        "meetings", sa.Column("scheduled_end", postgresql.TIMESTAMP(timezone=True), nullable=True)
    )
    op.add_column("meetings", sa.Column("livekit_room", sa.Text(), nullable=True))
    op.add_column("meetings", sa.Column("created_by", sa.UUID(), nullable=True))
    op.create_foreign_key(
        op.f("fk_meetings_created_by_users"), "meetings", "users", ["created_by"], ["id"]
    )
    op.create_check_constraint(
        "status_valid", "meetings", "status IN ('scheduled','live','ended','cancelled')"
    )
    op.create_check_constraint(
        "schedule_ordered",
        "meetings",
        "scheduled_end IS NULL OR scheduled_start IS NULL OR scheduled_end > scheduled_start",
    )
    op.add_column("meeting_participants", sa.Column("role_in_meeting", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("meeting_participants", "role_in_meeting")
    op.drop_constraint("ck_meetings_schedule_ordered", "meetings", type_="check")
    op.drop_constraint("ck_meetings_status_valid", "meetings", type_="check")
    op.drop_constraint(op.f("fk_meetings_created_by_users"), "meetings", type_="foreignkey")
    op.drop_column("meetings", "created_by")
    op.drop_column("meetings", "livekit_room")
    op.drop_column("meetings", "scheduled_end")
    op.drop_column("meetings", "scheduled_start")

    op.drop_column("team_members", "role")
    op.drop_constraint(op.f("fk_teams_department_id_departments"), "teams", type_="foreignkey")
    op.drop_column("teams", "department_id")

    op.drop_constraint("ck_data_residency_region_valid", "data_residency", type_="check")
    op.drop_column("data_residency", "locked_at")
    op.drop_column("data_residency", "object_bucket_region")
    op.drop_column("data_residency", "qdrant_region")
    op.drop_column("data_residency", "db_cell")

    op.alter_column(
        "tenant_settings",
        "agent_confidence_overrides",
        nullable=True,
        server_default=None,
        existing_type=postgresql.JSONB(),
    )
    op.add_column("tenant_settings", sa.Column("settings", postgresql.JSONB(), nullable=True))
    op.drop_column("tenant_settings", "deleted_at")
    op.drop_column("tenant_settings", "updated_at")
    op.drop_column("tenant_settings", "created_at")
    op.drop_column("tenant_settings", "sso_config")
    op.drop_column("tenant_settings", "pii_redaction_enabled")
    op.drop_column("tenant_settings", "retention_days_worklogs")
    op.drop_column("tenant_settings", "retention_days_transcripts")
    op.drop_column("tenant_settings", "default_locale")

    op.alter_column(
        "users",
        "skills",
        nullable=True,
        server_default=None,
        existing_type=postgresql.ARRAY(sa.Text()),
    )
    op.drop_constraint("uq_users_org_id_clerk_user_id", "users", type_="unique")
    op.create_unique_constraint(op.f("uq_users_clerk_user_id"), "users", ["clerk_user_id"])

    op.drop_index(op.f("ix_rbac_grants_org_id"), table_name="rbac_grants")
    op.drop_table("rbac_grants")
