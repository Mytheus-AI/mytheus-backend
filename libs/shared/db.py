"""SQLAlchemy 2.0 async declarative base, shared column mixins, and the process engine.

One declarative Base for the whole modular monolith; every service's models import it so a
single Alembic history sees all tables. The stable naming convention makes autogenerate emit
deterministic constraint names (so migrations and models never drift).
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from functools import lru_cache

from sqlalchemy import ForeignKey, MetaData, text
from sqlalchemy.dialects.postgresql import TIMESTAMP, UUID
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, declared_attr, mapped_column

# Deterministic constraint/index names for reproducible autogenerate output.
NAMING = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING)


@lru_cache(maxsize=1)
def get_engine() -> AsyncEngine:
    """The process-wide async engine, built lazily from `DATABASE_URL`.

    Lazy and cached so importing any module never requires database env to be set, and so a
    single pool is shared rather than one per caller. `DATABASE_URL` must name `mytheus_app`
    in every request-path process: it is the role subject to every `tenant_isolation` policy.
    Pointing it at a superuser or any role holding BYPASSRLS silently disables tenant
    isolation everywhere at once, which is exactly what defect D1 was.
    """
    return create_async_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


@lru_cache(maxsize=1)
def get_sessionmaker() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


class TenantMixin:
    """org_id on every tenant-scoped row (the RLS anchor + Kafka partition key).

    The foreign key is not decoration (defect D6). Without it an orphan `org_id` is
    insertable, and right-to-be-forgotten works by deleting everything carrying a given
    `org_id`: rows pointing at a tenant root that never existed, or that has already been
    purged, are exactly the rows an RTBF sweep fails to find. `ON DELETE RESTRICT` is
    deliberate over CASCADE, because tenant deletion is the RTBF saga's job and must be
    ordered and audited, not a side effect of one DELETE.

    `declared_attr` is required rather than stylistic: a ForeignKey object cannot be shared
    between mapped classes, so each subclass needs its own instance.
    """

    @declared_attr
    def org_id(cls) -> Mapped[uuid.UUID]:  # noqa: N805 - SQLAlchemy mixin convention
        return mapped_column(
            UUID(as_uuid=True),
            ForeignKey("organizations.id", ondelete="RESTRICT"),
            nullable=False,
            index=True,
        )


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()")
    )
    deleted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
