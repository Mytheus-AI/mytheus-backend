"""Tenant isolation helpers: org_id context var + RLS session setter.

Rules:
- `org_id` is ALWAYS derived from the verified JWT, never from path/query params.
- `set_rls` MUST be called with `transaction_local=True` (the default). With a
  pooled connection (Supabase pooler / PgBouncer) a non-local GUC leaks one
  tenant's app.current_org into another tenant's connection reuse.
"""

from __future__ import annotations

from contextvars import ContextVar
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_org_id_ctx: ContextVar[UUID | None] = ContextVar("org_id", default=None)


def set_org_id(org_id: UUID) -> None:
    _org_id_ctx.set(org_id)


def get_org_id() -> UUID:
    org_id = _org_id_ctx.get()
    if org_id is None:
        raise RuntimeError("org_id not set — call set_org_id before any tenant-scoped query")
    return org_id


async def set_rls(session: AsyncSession, org_id: UUID) -> None:
    """Set app.current_org transaction-locally so RLS policies activate."""
    await session.execute(
        text("SELECT set_config('app.current_org', :org_id, true)"),
        {"org_id": str(org_id)},
    )
