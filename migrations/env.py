"""Alembic env.py — one migration history for all services.

All SQLAlchemy models from every service must be imported here so
Alembic's autogenerate can detect table additions/removals.
"""
from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool, engine_from_config

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import all models so metadata is populated.
# TODO: add model imports here as services add SQLAlchemy models, e.g.:
# from services.core_api.models import Base as CoreBase
# from services.identity_tenant_service.models import Base as IdentityBase

target_metadata = None  # replace with Base.metadata once models exist

_DATABASE_URL = os.environ["DATABASE_URL"].replace("+asyncpg", "")  # sync URL for Alembic


def run_migrations_offline() -> None:
    context.configure(
        url=_DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        {"sqlalchemy.url": _DATABASE_URL},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
