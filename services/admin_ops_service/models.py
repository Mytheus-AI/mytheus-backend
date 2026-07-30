"""admin-ops-service models: the compliance audit trail.

`audit_logs` is stood up in P2 rather than P13 so that RLS and authz events are recorded from
the moment RLS goes live (`11-security-architecture.md` sections 8.3 and 9). An audit table
that arrives after the thing it audits has a hole in it exactly where the first incident is.

Append-only is enforced by GRANT, not by convention: `mytheus_app` holds INSERT and SELECT
and has UPDATE and DELETE revoked, so tampering fails at the database rather than depending
on every future author remembering not to write an UPDATE.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, ForeignKey, Text, text
from sqlalchemy.dialects.postgresql import INET, JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    org_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("organizations.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    # Nullable: a `system` actor has no user row, and an authz denial may be recorded before
    # the principal is resolved to one.
    actor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    actor_type: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    target_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    audit_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    # Part of the primary key because it is the partition key: PostgreSQL requires every
    # partition-key column to appear in a unique constraint on a partitioned table.
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), primary_key=True, server_default=text("now()")
    )

    __table_args__ = (
        CheckConstraint("actor_type IN ('user','agent','system')", name="actor_type_valid"),
        {"postgresql_partition_by": "RANGE (occurred_at)"},
    )
