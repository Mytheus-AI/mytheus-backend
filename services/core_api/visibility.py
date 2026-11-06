"""RLS visibility checks for caller-supplied ids that become foreign keys (gap 31).

A foreign key does NOT confine a reference to the caller's tenant. PostgreSQL runs
referential-integrity checks as the referenced table's owner with row-level security
BYPASSED, so `fk_*_user_id_users` (and every other FK into a tenant table) resolves another
tenant's row even though this transaction cannot SELECT it. The referencing row's own WITH
CHECK passes too, because the `org_id` being written is the caller's own; the column that
crosses the boundary is the foreign key, which no policy constrains. Proven twice against a
live database (commits 9f51192 and 602bff4): both routes returned 2xx and persisted a row
pointing into another tenant.

So every write path that takes an id from the request and stores it as a foreign key must
first SELECT the referenced row through the tenant session. RLS makes "does not exist" and
"exists in another tenant" indistinguishable, which is exactly the answer the caller gets.

The table set is a closed literal, not a string parameter, so no caller can turn this into
SQL built from request data.
"""

from __future__ import annotations

import uuid
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

Referenced = Literal["users", "teams", "departments", "meetings"]

# `users` has no soft-delete column; the others are soft-deleted, and a deleted row is not a
# valid reference target either.
_SQL: dict[Referenced, str] = {
    "users": "SELECT 1 FROM users WHERE id = :id",
    "teams": "SELECT 1 FROM teams WHERE id = :id AND deleted_at IS NULL",
    "departments": "SELECT 1 FROM departments WHERE id = :id AND deleted_at IS NULL",
    "meetings": "SELECT 1 FROM meetings WHERE id = :id AND deleted_at IS NULL",
}


async def is_visible(session: AsyncSession, table: Referenced, row_id: uuid.UUID) -> bool:
    """True iff this tenant-scoped session can see `table.id = row_id`.

    MUST run on a `tenant_session` (RLS active). On a superuser or BYPASSRLS connection it
    would see every tenant's rows and the check would silently pass.
    """
    row = (await session.execute(text(_SQL[table]), {"id": str(row_id)})).first()
    return row is not None
