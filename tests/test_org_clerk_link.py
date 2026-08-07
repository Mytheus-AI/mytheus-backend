"""`organizations.clerk_org_id` is what makes a tenant reachable, and nothing used to write it.

`libs/shared/org_resolver.py` resolves every authenticated request by looking the token's
Clerk organization id up through this column. Before this, only test fixtures ever populated
it: `provision_organization` did not accept it, and no other production path set it. A
provisioned tenant therefore existed in the database and could not be resolved by any request
- the whole API was unreachable for a real Clerk-authenticated caller, and the AI tier's
`internal_org_id` claim had nothing publishing it either.

These tests pin both halves: the column is written at provisioning time, and it is never
silently repointed at a different Clerk organization once set (which would hand one tenant's
data to another tenant's users).
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from libs.shared import clerk_admin
from services.identity_tenant_service.provisioning import (
    provision_organization,
    sync_internal_org_id_to_clerk,
)
from tests.test_seed import _admin_sessionmaker  # same admin session helper the seeder tests use

# asyncio_mode = "auto" collects the coroutines; only the DB-backed tests below need a
# migrated schema, so the fixture is applied per test rather than to the module - the
# clerk_admin tests must stay runnable with no database at all.
_needs_db = pytest.mark.usefixtures("migrated_db")


async def _clerk_org_id_of(sessionmaker, org_id: uuid.UUID) -> str | None:
    async with sessionmaker() as session:
        row = (
            await session.execute(
                text("SELECT clerk_org_id FROM organizations WHERE id = :id"),
                {"id": str(org_id)},
            )
        ).first()
    return None if row is None else row[0]


@_needs_db
async def test_provisioning_stores_the_clerk_org_id_so_the_tenant_is_resolvable() -> None:
    sessionmaker = _admin_sessionmaker()
    slug = f"clerklink-{uuid.uuid4().hex[:8]}"
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:12]}"

    async with sessionmaker() as session, session.begin():
        result = await provision_organization(
            session,
            org_id=org_id,
            name="Clerk Link Org",
            slug=slug,
            plan_code="free",
            clerk_org_id=clerk_org,
        )
    assert result.created is True
    assert await _clerk_org_id_of(sessionmaker, org_id) == clerk_org


@_needs_db
async def test_the_resolver_can_actually_find_a_provisioned_tenant() -> None:
    """The point of the column. Provision, then resolve exactly as the request path does."""
    from libs.shared.org_resolver import resolve_org_id

    sessionmaker = _admin_sessionmaker()
    slug = f"resolvable-{uuid.uuid4().hex[:8]}"
    org_id = uuid.uuid4()
    clerk_org = f"org_{uuid.uuid4().hex[:12]}"

    async with sessionmaker() as session, session.begin():
        await provision_organization(
            session,
            org_id=org_id,
            name="Resolvable Org",
            slug=slug,
            plan_code="free",
            clerk_org_id=clerk_org,
        )

    async with sessionmaker() as session:
        assert await resolve_org_id(session, clerk_org) == org_id


@_needs_db
async def test_a_rerun_fills_a_null_link_but_never_repoints_an_established_one() -> None:
    """The seeder can create a tenant before it exists in Clerk, so filling a NULL later is
    legitimate. Repointing an established tenant is not: it would route one tenant's Clerk
    organization at another tenant's data."""
    sessionmaker = _admin_sessionmaker()
    slug = f"relink-{uuid.uuid4().hex[:8]}"
    org_id = uuid.uuid4()

    # Created with no Clerk organization behind it.
    async with sessionmaker() as session, session.begin():
        await provision_organization(
            session, org_id=org_id, name="Relink Org", slug=slug, plan_code="free"
        )
    assert await _clerk_org_id_of(sessionmaker, org_id) is None

    # A later run learns the Clerk id: the NULL is filled.
    first_clerk = f"org_{uuid.uuid4().hex[:12]}"
    async with sessionmaker() as session, session.begin():
        await provision_organization(
            session,
            org_id=org_id,
            name="Relink Org",
            slug=slug,
            plan_code="free",
            clerk_org_id=first_clerk,
        )
    assert await _clerk_org_id_of(sessionmaker, org_id) == first_clerk

    # A run naming a DIFFERENT Clerk organization must not move the link.
    async with sessionmaker() as session, session.begin():
        await provision_organization(
            session,
            org_id=org_id,
            name="Relink Org",
            slug=slug,
            plan_code="free",
            clerk_org_id=f"org_{uuid.uuid4().hex[:12]}",
        )
    assert (
        await _clerk_org_id_of(sessionmaker, org_id) == first_clerk
    ), "an established clerk_org_id must never be repointed"


async def test_publishing_to_clerk_is_a_logged_noop_without_a_real_secret(monkeypatch) -> None:
    """Provisioning must not require Clerk credentials: local development and CI create
    tenants with no Clerk account at all. The failure has to be visible in logs rather than
    silent, because the symptom otherwise appears much later as an AI-tier 401."""
    from services.identity_tenant_service.provisioning import ProvisionResult

    result = ProvisionResult(org_id=uuid.uuid4(), created=True)

    for placeholder in ("", "CHANGE_ME", "sk_test_CHANGE_ME"):
        monkeypatch.setenv("CLERK_SECRET_KEY", placeholder)
        assert await sync_internal_org_id_to_clerk(result, "org_abc") is False

    monkeypatch.delenv("CLERK_SECRET_KEY", raising=False)
    assert await sync_internal_org_id_to_clerk(result, "org_abc") is False

    # No Clerk organization to publish to is a no-op, not an error.
    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_real")
    assert await sync_internal_org_id_to_clerk(result, None) is False


async def test_a_real_secret_publishes_the_resolved_org_id_as_public_metadata(monkeypatch) -> None:
    """The value and the shape both matter: the AI tier reads a UUID string from
    `public_metadata.internal_org_id`, and Clerk's metadata endpoint merges, so writing a
    nested object under any other key would leave the claim unpopulated."""
    import httpx

    org_id = uuid.uuid4()
    seen: dict[str, object] = {}

    class _FakeResponse:
        status_code = 200
        text = "{}"

    class _FakeClient:
        def __init__(self, *a, **kw) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def patch(self, url, *, headers, json):
            seen["url"] = url
            seen["auth"] = headers["Authorization"]
            seen["json"] = json
            return _FakeResponse()

    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_real")
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)

    assert await clerk_admin.set_internal_org_id("org_xyz", org_id) is True
    assert seen["url"] == "https://api.clerk.com/v1/organizations/org_xyz/metadata"
    assert seen["auth"] == "Bearer sk_live_real"
    assert seen["json"] == {"public_metadata": {"internal_org_id": str(org_id)}}


async def test_a_clerk_rejection_is_reported_not_raised(monkeypatch) -> None:
    """A third party being unavailable must not fail provisioning: the tenant is already
    committed, and the publish is idempotent, so it can simply be re-run."""
    import httpx

    class _Rejected:
        status_code = 422
        text = '{"errors":[{"message":"unknown organization"}]}'

    class _FakeClient:
        def __init__(self, *a, **kw) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def patch(self, *a, **kw):
            return _Rejected()

    monkeypatch.setenv("CLERK_SECRET_KEY", "sk_live_real")
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    assert await clerk_admin.set_internal_org_id("org_gone", uuid.uuid4()) is False

    class _Boom:
        def __init__(self, *a, **kw) -> None:
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc) -> None:
            return None

        async def patch(self, *a, **kw):
            raise httpx.ConnectError("clerk unreachable")

    monkeypatch.setattr(httpx, "AsyncClient", _Boom)
    assert await clerk_admin.set_internal_org_id("org_x", uuid.uuid4()) is False
