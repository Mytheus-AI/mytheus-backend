"""identity-tenant-service models.

Full DDL this band: Organization (tenant root; id IS org_id), User (clerk_user_id unique).
Stub DDL for P2 (canon-named, minimal columns) so FKs, the Clerk webhook upserts, and later
bands have real targets: Team, TeamMember, Department, TenantSettings, DataResidency.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base, TenantMixin, TimestampMixin


class Organization(Base, TimestampMixin):
    """Tenant root. organizations.id IS org_id - there is no second tenant identifier.

    Deliberately NOT `tenant_isolation`-policied (canon `_canon/02-data-model.md` section 7
    rule 7). It carries no `org_id` column, so the canonical policy is not expressible on it,
    and the edge must read `clerk_org_id -> id` BEFORE any tenant is known. Reads are bounded
    by column-level grants plus the single sanctioned query shape in
    `libs/shared/org_resolver.py`, not by RLS.
    """

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    # Clerk's native organization id: an opaque `org_...` string, never a UUID. This column is
    # the join the edge resolves the token's claim through, and that lookup IS the
    # authorization checkpoint (ADR-0010 D2), not overhead on the way to one.
    clerk_org_id: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    # `active` or `suspended`. Checked by the resolver, so a suspended tenant stops at the
    # edge rather than reaching a tenant-scoped store.
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'active'"))
    plan_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)

    __table_args__ = (CheckConstraint("status IN ('active','suspended')", name="status_valid"),)


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    clerk_user_id: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    # DOMAIN role (job title, function). NOT a permission tier - authorization reads
    # `rbac_grants.tier` and nothing else (resolved decision, plan section 8 item 2). Keeping
    # them in one column is how "promote someone to team lead" silently becomes "grant admin".
    role: Mapped[str | None] = mapped_column(Text, nullable=True)
    skills: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    bio: Mapped[str | None] = mapped_column(Text, nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    # ADR-0038 D1: the working profile PersonalListenerAgent listens with.
    listen_for: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    ignore_topics: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default=text("'{}'")
    )
    # ADR-0038 D2: the person's agent persona. A display label and a wording style, never an
    # agent class or an identifier in events, logs or metrics.
    agent_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    agent_avatar_seed: Mapped[str | None] = mapped_column(Text, nullable=True)
    agent_personality: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'balanced'")
    )
    agent_instructions: Mapped[str | None] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "agent_personality IN ('balanced', 'concise', 'detailed', 'direct', 'friendly')",
            name="agent_personality",
        ),
        # Per tenant, not globally (gap 13). A global unique on clerk_user_id makes one Clerk
        # user a member of exactly one Mytheus organization for all time, which Clerk itself
        # does not require: the same person can belong to several organizations, and the
        # second INSERT would have failed with a constraint violation.
        UniqueConstraint("org_id", "clerk_user_id", name="uq_users_org_id_clerk_user_id"),
    )


class RbacGrant(Base):
    """The authorization store. The ONLY thing the authz path reads (plan section 8 item 2).

    Separate from `users.role` on purpose, and the separation is the decision: `tier` answers
    "what may this person do", `users.role` answers "what does this person do". One is a
    security boundary, the other is a profile field an ordinary user may well be allowed to
    edit.
    """

    __tablename__ = "rbac_grants"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=False
    )
    tier: Mapped[str] = mapped_column(Text, nullable=False)
    granted_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )

    __table_args__ = (
        UniqueConstraint("org_id", "user_id", name="uq_rbac_grants_org_id_user_id"),
        CheckConstraint("tier IN ('owner','admin','member')", name="tier_valid"),
    )


# --- P2 stubs (extended in Plan 1-2) ---


class Team(Base, TenantMixin, TimestampMixin):
    __tablename__ = "teams"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    department_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("departments.id"), nullable=True
    )


class TeamMember(Base, TenantMixin):
    __tablename__ = "team_members"
    team_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("teams.id"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), primary_key=True, index=True
    )
    # Membership role within the team (lead, member). Distinct again from rbac_grants.tier.
    role: Mapped[str | None] = mapped_column(Text, nullable=True)


class Department(Base, TenantMixin, TimestampMixin):
    __tablename__ = "departments"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("departments.id"), nullable=True, index=True
    )


class TenantSettings(Base, TimestampMixin):
    """Named columns, not a `settings jsonb` bag (gap 13).

    Retention periods and the PII-redaction switch are compliance controls: they are queried
    by retention jobs, appear in a DPA, and must be constrainable. Inside a jsonb blob none
    of that is true, and a typo in a key silently means "no retention limit".
    """

    __tablename__ = "tenant_settings"
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="RESTRICT"), primary_key=True
    )
    default_locale: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'en'"))
    retention_days_transcripts: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("365")
    )
    retention_days_worklogs: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("365")
    )
    agent_confidence_overrides: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    pii_redaction_enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    # Envelope-encrypted at rest under a per-tenant KMS data key in a later band. Nullable
    # because SSO/SAML is deferred; the column lands now so enabling it is not a migration.
    sso_config: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    # ADR-0018: defaulted false so no tenant's meeting-join behavior changes as a result of
    # this column landing. Flipping it to true for a tenant is a legal/compliance decision
    # (which jurisdictions require affirmative recording consent), not an engineering default.
    # Gates the 428 check on `POST /api/meetings/:id/token` in meeting-service.
    consent_required: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )


class DataResidency(Base):
    """Compliance-by-construction: populated for every tenant even though the MVP is
    single-region `us`. A residency table that only exists once a second region does is a
    table nobody can backfill correctly."""

    __tablename__ = "data_residency"
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="RESTRICT"), primary_key=True
    )
    region: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'us'"))
    db_cell: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'pool-us-1'"))
    qdrant_region: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'us'"))
    object_bucket_region: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'us'")
    )
    # Immutable once set: moving a tenant's data between regions is a migration workflow,
    # not an UPDATE.
    locked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    __table_args__ = (CheckConstraint("region IN ('us','eu','ap')", name="region_valid"),)
