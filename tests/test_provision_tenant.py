"""`provision_tenant`: what it asks Postgres to do, in what order, and when it refuses. No DB.

The DB-backed proof that these statements really commit and roll back together is
`test_orgs_provisioning_db.py`; this file pins the statement plan so a refactor that drops a row
from the tenant birth, or reorders RLS after a tenant-scoped write, fails without a database.
"""

from __future__ import annotations

import uuid
from typing import Any, cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from services.identity_tenant_service.provisioning import (
    TenantConflict,
    TenantOwner,
    provision_tenant,
)
from tests.test_orgs_rbac_routes import FakeSession, Handler

OWNER = TenantOwner(clerk_user_id="user_owner1", email="owner@acme.test")
OWNER_ID = uuid.uuid4()


def _handler(
    *, clerk_taken: bool = False, slug_taken: bool = False, insert_wins: bool = True
) -> Handler:
    def handler(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        if "WHERE clerk_org_id" in sql:
            return [{"x": 1}] if clerk_taken else []
        if sql.startswith("SELECT id FROM organizations WHERE slug"):
            return [{"id": uuid.uuid4()}] if slug_taken else []
        if sql.startswith("INSERT INTO organizations"):
            return [{"id": params["id"]}] if insert_wins else []
        if sql.startswith("INSERT INTO outbox"):
            return [{"id": 1}]
        if sql.startswith("INSERT INTO users"):
            return [{"id": OWNER_ID}]
        return []

    return handler


async def _run(session: FakeSession, org_id: uuid.UUID) -> Any:
    return await provision_tenant(
        cast(AsyncSession, session),
        org_id=org_id,
        name="Acme",
        slug="acme",
        clerk_org_id="org_a",
        owner=OWNER,
    )


async def test_writes_every_row_of_the_tenant_birth_in_order() -> None:
    session = FakeSession(_handler())
    org_id = uuid.uuid4()
    result = await _run(session, org_id)
    assert result.org_id == org_id and result.owner_user_id == OWNER_ID

    sql = session.sql()
    order = [
        "INSERT INTO organizations",
        "set_config('app.current_org'",
        "INSERT INTO outbox",
        "INSERT INTO tenant_settings",
        "INSERT INTO data_residency",
        "INSERT INTO users",
        "INSERT INTO rbac_grants",
    ]
    positions = [sql.find(fragment) for fragment in order]
    assert all(p >= 0 for p in positions), list(zip(order, positions, strict=True))
    assert positions == sorted(positions)

    grant = next(p for s, p in session.statements if s.startswith("INSERT INTO rbac_grants"))
    assert grant == {"o": str(org_id), "u": str(OWNER_ID)}
    assert "'owner'" in next(s for s, _ in session.statements if "INTO rbac_grants" in s)


@pytest.mark.parametrize(
    "kw", [{"clerk_taken": True}, {"slug_taken": True}, {"insert_wins": False}]
)
async def test_any_existing_tenant_is_a_conflict_and_nothing_tenant_scoped_is_written(
    kw: dict[str, bool],
) -> None:
    session = FakeSession(_handler(**kw))
    with pytest.raises(TenantConflict):
        await _run(session, uuid.uuid4())
    sql = session.sql()
    for table in ("outbox", "tenant_settings", "data_residency", "users", "rbac_grants"):
        assert f"INSERT INTO {table}" not in sql
    # Never links a Clerk org onto an existing row (link_existing=False).
    assert "UPDATE organizations" not in sql
