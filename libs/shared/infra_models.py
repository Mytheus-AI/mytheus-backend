"""Infrastructure tables the shared libs write to: the transactional outbox and the inbox.

These are RLS-scoped tenant tables (they carry org_id). The outbox relay drains them with a
role that bypasses RLS; request/consumer code writes them inside the tenant transaction.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Index, Integer, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from libs.shared.db import Base


class Outbox(Base):
    __tablename__ = "outbox"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    partition_key: Mapped[str] = mapped_column(Text, nullable=False)  # the org_id string
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    schema_subject: Mapped[str] = mapped_column(Text, nullable=False)  # {topic}-value
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
    published_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, server_default=text("0"))

    __table_args__ = (
        UniqueConstraint("topic", "idempotency_key", name="outbox_idem_uq"),
        Index(
            "outbox_unpublished_idx",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
    )


class Inbox(Base):
    __tablename__ = "inbox"

    consumer_group: Mapped[str] = mapped_column(Text, primary_key=True)
    org_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text, primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
