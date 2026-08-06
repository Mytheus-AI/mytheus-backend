"""The tenant-claim checkpoint: `clerk_org_id` -> `org_id`, run as `mytheus_app`.

This is the query the whole resolved tenant-claim decision rests on (ADR-0010 D2), and it is
the only sanctioned reader of `organizations`. It runs as the request-path role here on
purpose: under Plan 0's `tenant_isolation` policy on the tenant root, every one of these
lookups returned zero rows, so the checkpoint had no executable path at all.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id, resolve_org_id
from tests.conftest import APP_URL


async def _make_org(engine, *, clerk_org_id, status="active", deleted=False) -> uuid.UUID:
    org_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO organizations (id, name, slug, clerk_org_id, status, deleted_at) "
                "VALUES (:id, 'Org', :slug, :cid, :st, :dl)"
            ),
            {
                "id": str(org_id),
                "slug": f"res-{org_id}",
                "cid": clerk_org_id,
                "st": status,
                "dl": datetime(2026, 1, 1, tzinfo=UTC) if deleted else None,
            },
        )
    return org_id


def _app_sessions():
    return async_sessionmaker(create_async_engine(APP_URL), expire_on_commit=False)


async def test_resolves_an_active_org(migrated_db):
    su = create_async_engine(migrated_db)
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    org_id = await _make_org(su, clerk_org_id=clerk_org)
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session:
        assert await resolve_org_id(session, clerk_org) == org_id


@pytest.mark.parametrize(
    "case",
    ["unknown", "suspended", "soft_deleted", "empty_claim"],
)
async def test_all_negative_outcomes_are_byte_identical(migrated_db, case):
    """ADR-0010 D2: one response for all of them.

    Distinguishing "no such organization" from "suspended" turns this into an oracle for
    which Clerk organizations are Mytheus customers and which have been cut off. Tenant
    existence is confidential, which is the same reason cross-tenant REST access returns 404
    and never 403.
    """
    su = create_async_engine(migrated_db)
    if case == "unknown":
        claim = f"org_{uuid.uuid4().hex}"
    elif case == "suspended":
        claim = f"org_{uuid.uuid4().hex[:16]}"
        await _make_org(su, clerk_org_id=claim, status="suspended")
    elif case == "soft_deleted":
        claim = f"org_{uuid.uuid4().hex[:16]}"
        await _make_org(su, clerk_org_id=claim, deleted=True)
    else:
        claim = ""
    await su.dispose()

    sessions = _app_sessions()
    with pytest.raises(ProblemDetail) as exc:
        async with sessions() as session:
            await resolve_org_id(session, claim)

    assert exc.value.status == 401
    assert exc.value.code is ErrorCode.AUTH_REQUIRED
    assert exc.value.detail == "Token carries no usable organization"


async def test_resolution_needs_no_tenant_context(migrated_db):
    """The point of dropping the policy: this must work with `app.current_org` unset.

    It is the pre-tenant read. If it required a tenant to already be established, the
    resolved tenant-claim decision would be circular.
    """
    su = create_async_engine(migrated_db)
    clerk_org = f"org_{uuid.uuid4().hex[:16]}"
    org_id = await _make_org(su, clerk_org_id=clerk_org)
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session:
        current = (
            await session.execute(text("SELECT current_setting('app.current_org', true)"))
        ).scalar_one()
        assert current in (None, ""), "precondition: no tenant is set"
        assert await resolve_org_id(session, clerk_org) == org_id


# --- resolve_org_by_id: the second sanctioned reader (fix round 1, webhook.py's tenant
# resolution from room metadata) ----------------------------------------------------------


async def test_resolve_org_by_id_resolves_an_active_org(migrated_db):
    su = create_async_engine(migrated_db)
    org_id = await _make_org(su, clerk_org_id=f"org_{uuid.uuid4().hex[:16]}")
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session:
        assert await resolve_org_by_id(session, org_id) == org_id


@pytest.mark.parametrize("case", ["unknown", "suspended", "soft_deleted"])
async def test_resolve_org_by_id_returns_none_for_every_negative_case(migrated_db, case):
    """Unlike `resolve_org_id` (a token holder's error), `resolve_org_by_id` never raises - the
    caller (a webhook, not a JWT holder) decides its own error framing. `None` for every
    negative case, never a distinguishable value per case, for the same directory-
    confidentiality reason `resolve_org_id`'s `_denied()` gives one response for all of them.
    """
    su = create_async_engine(migrated_db)
    if case == "unknown":
        org_id = uuid.uuid4()  # never inserted
    elif case == "suspended":
        org_id = await _make_org(
            su, clerk_org_id=f"org_{uuid.uuid4().hex[:16]}", status="suspended"
        )
    else:
        org_id = await _make_org(su, clerk_org_id=f"org_{uuid.uuid4().hex[:16]}", deleted=True)
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session:
        assert await resolve_org_by_id(session, org_id) is None


async def test_resolve_org_by_id_needs_no_tenant_context(migrated_db):
    """Same pre-tenant read property as `resolve_org_id`: must work with `app.current_org`
    unset, since the webhook has not established a tenant until this returns."""
    su = create_async_engine(migrated_db)
    org_id = await _make_org(su, clerk_org_id=f"org_{uuid.uuid4().hex[:16]}")
    await su.dispose()

    sessions = _app_sessions()
    async with sessions() as session:
        current = (
            await session.execute(text("SELECT current_setting('app.current_org', true)"))
        ).scalar_one()
        assert current in (None, ""), "precondition: no tenant is set"
        assert await resolve_org_by_id(session, org_id) == org_id
