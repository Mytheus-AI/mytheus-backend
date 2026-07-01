"""identity-tenant-service models.

Full DDL this band: Organization (tenant root; id IS org_id), User (clerk_user_id unique).
Stub DDL for P2 (canon-named, minimal columns) so FKs, the Clerk webhook upserts, and later
bands have real targets: Team, TeamMember, Department, TenantSettings, DataResidency.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base, TenantMixin, TimestampMixin


class Organization(Base, TimestampMixin):
    """Tenant root. organizations.id IS org_id — there is no second tenant identifier."""

    __tablename__ = "organizations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    plan_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False, index=True
    )
    clerk_user_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str | None] = mapped_column(Text, nullable=True)  # domain/skills attribute
    skills: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    bio: Mapped[str | None] = mapped_column(Text, nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(Text, nullable=True)


# --- P2 stubs (extended in Plan 1-2) ---


class Team(Base, TenantMixin, TimestampMixin):
    __tablename__ = "teams"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)


class TeamMember(Base, TenantMixin):
    __tablename__ = "team_members"
    team_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("teams.id"), primary_key=True
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id"), primary_key=True
    )


class Department(Base, TenantMixin, TimestampMixin):
    __tablename__ = "departments"
    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("departments.id"), nullable=True
    )


class TenantSettings(Base):
    __tablename__ = "tenant_settings"
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), primary_key=True
    )
    settings: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    agent_confidence_overrides: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class DataResidency(Base):
    __tablename__ = "data_residency"
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("organizations.id"), primary_key=True
    )
    region: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'us'"))
