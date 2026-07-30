"""Alembic env.py — one migration history for all services.

All SQLAlchemy models from every service must be imported here so
Alembic's autogenerate can detect table additions/removals.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import every service's models so autogenerate sees all tables.
# (later bands append their service model imports here)
import libs.shared.infra_models  # noqa: F401,E402  (outbox, inbox)
import services.admin_ops_service.models  # noqa: F401,E402  (audit_logs)
import services.core_api.models  # noqa: F401,E402
import services.identity_tenant_service.models  # noqa: F401,E402
from libs.shared.db import Base  # noqa: E402

target_metadata = Base.metadata

# Migrations run as the SCHEMA OWNER, not as the application role. `mytheus_app` deliberately
# holds no DDL privilege and cannot create roles, so pointing this at DATABASE_URL would fail
# on the first CREATE TABLE. The fallback keeps a plain `DATABASE_URL=...` dev setup working.
_DATABASE_URL = (os.environ.get("BOOTSTRAP_DATABASE_URL") or os.environ["DATABASE_URL"]).replace(
    "+asyncpg", ""
)  # sync URL for Alembic


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
