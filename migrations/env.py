"""Alembic env.py — one migration history for all services.

All SQLAlchemy models from every service must be imported here so
Alembic's autogenerate can detect table additions/removals.

Autogenerate is filtered — see `include_object` below. Not every table in this database is
mapped as a SQLAlchemy model, and one of them is not even owned by this repository, so an
unfiltered autogenerate proposes dropping them. `866f7eedb0e7`'s docstring records hitting
exactly that and hand-writing the migration body instead; this filter fixes the cause rather
than working around it once per migration.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name is not None:
    # disable_existing_loggers=False: the default disables every logger created before this
    # runs, so any process that migrates in-process (the test session's `migrated_db`
    # fixture) silently lost all application logging - and every caplog assertion after it.
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Import every service's models so autogenerate sees all tables.
# (later bands append their service model imports here)
import libs.shared.infra_models  # noqa: F401,E402  (outbox, inbox)
import services.admin_ops_service.models  # noqa: F401,E402  (audit_logs)
import services.core_api.models  # noqa: F401,E402
import services.identity_tenant_service.models  # noqa: F401,E402
from libs.shared.db import Base  # noqa: E402
from migrations.autogen_filter import include_object  # noqa: E402

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
        include_object=include_object,
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
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
