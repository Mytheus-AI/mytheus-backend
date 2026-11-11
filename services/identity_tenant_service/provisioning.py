"""Organization provisioning: the org-lifecycle write path. Plan 5-6 section 3.4.

An audit of the previous foundation band (2026-08-05) found tenant provisioning had NO
reachable path in the running system: `POST /api/v1/orgs` does not exist, so nothing but a
manual `INSERT INTO organizations` as `postgres` could create a tenant. As a direct
consequence, the org-lifecycle `outbox` -> `tenant-events` producer path had never been
exercised outside a hand-seeded test row.

This module is that path: a real, transaction-atomic org creation plus its `tenant-events`
`provisioned` outbox emission, in ONE transaction (the same "state change + outbox row, same
transaction" rule every other producer in this codebase follows -
`services/core_api/routes/meetings.py::create_meeting` is the sibling example). It is callable
today by the seeder (`seed/seed.py`) and, unchanged, by a future `POST /orgs` route - that
route is NOT built here, because this task's brief is the seeder, not a public
tenant-provisioning API (request/response shape, idempotency-key header handling, and
authorization for a *public* provisioning endpoint are a separate, undispatched piece of
work). Building this as a reusable function rather than inlining the INSERT + outbox call in
`seed.py` is what makes that future route a thin wrapper instead of a second implementation of
the same atomic write.

`organizations` carries no `tenant_isolation` policy (canon `_canon/02-data-model.md` section
7 rule 7 - it is the tenant root, read before any tenant is known), so creating the row itself
needs no `app.current_org`. Everything downstream of it in this module - the `tenant-events`
outbox row - is a tenant-scoped table, so RLS is set before that write, not skipped because
"it's just the outbox."
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls

PRODUCER = "identity-tenant-service"

_INSERT_ORG = text(
    "INSERT INTO organizations (id, name, slug, status, plan_id, clerk_org_id) "
    "VALUES (:id, :name, :slug, 'active', NULL, :clerk_org_id) "
    "ON CONFLICT (slug) DO NOTHING "
    "RETURNING id"
)
_SELECT_ORG_BY_SLUG = text("SELECT id FROM organizations WHERE slug = :slug")
# An idempotent re-run may be the first call that knows the Clerk id (the row can be created
# by the seeder before the tenant exists in Clerk). Only ever fills a NULL: it must not
# repoint an established tenant at a different Clerk organization, which would hand one
# tenant's data to another's users.
_LINK_CLERK_ORG = text(
    "UPDATE organizations SET clerk_org_id = :clerk_org_id "
    "WHERE id = :id AND clerk_org_id IS NULL"
)


@dataclass(frozen=True)
class ProvisionResult:
    org_id: uuid.UUID
    created: bool  # False when `slug` already named an existing tenant (idempotent re-run)


async def provision_organization(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    name: str,
    slug: str,
    plan_code: str,
    clerk_org_id: str | None = None,
    link_existing: bool = True,
) -> ProvisionResult:
    """Create the tenant root plus its `tenant-events` `provisioned` outbox row, atomically.

    Idempotent: a second call with the same `slug` is a no-op. `organizations.slug` carries a
    UNIQUE constraint, so the INSERT itself is race-safe (`ON CONFLICT (slug) DO NOTHING`); the
    returned `created=False` additionally tells the caller not to re-emit the lifecycle event.
    `provisioned` is a once-per-tenant fact, not one to replay on every idempotent re-run, even
    though the outbox's own `(topic, idempotency_key)` unique index would make a duplicate
    emission harmless too (belt and suspenders, same reasoning `enqueue()`'s own docstring
    gives for that index).

    `org_id` is caller-supplied rather than left to `organizations.id`'s
    `gen_random_uuid()` default, so a caller that needs a stable id across re-runs - the
    seeder, keying every other row it writes off this same id - can pass one deterministically;
    a caller that does not care (a future `POST /orgs` route) passes `uuid.uuid4()`.

    `plan_code` is the `TenantEvent` payload field (`mytheus-events` `schemas/tenant-events/
    v1.avsc`), a human-facing plan identifier ("free", "team", ...). It is NOT
    `organizations.plan_id` (a UUID FK to `billing-metering-service`'s `plans` table, a store
    this repo does not own and cannot resolve from here) - that column is left NULL, matching
    reality: no billing plan has been assigned yet at provisioning time.

    `clerk_org_id` is Clerk's opaque `org_...` identifier, and it is what makes the tenant
    REACHABLE. `libs/shared/org_resolver.py` resolves every authenticated request by looking
    the token's claim up through this exact column, so a row written without it can never be
    resolved: the tenant exists in the database and is unreachable through the API. Until now
    nothing in the running system wrote this column at all - only test fixtures did - so the
    provisioning path could not produce a usable tenant. It stays OPTIONAL because the seeder
    legitimately creates tenants that have no Clerk organization behind them.

    Setting the column is only half of the wiring. The AI tier
    (`mytheus-intelligence/libs/shared/auth.py`) reads an ALREADY-RESOLVED internal `org_id`
    from an `internal_org_id` token claim, which Clerk populates from the organization's
    `public_metadata`. Pushing that value into Clerk is a network call and therefore not part
    of this transaction: call `sync_internal_org_id_to_clerk` after commit.

    `link_existing=False` turns off the fill-a-NULL `clerk_org_id` behaviour on an idempotent
    re-run. The seeder wants it (it re-runs against tenants it created itself); a request-path
    caller (`POST /orgs`, via `provision_tenant`) must not have it, because there a slug
    collision is somebody ELSE's tenant and a request must never be able to attach a Clerk
    organization to a tenant it did not just create.
    """
    row = (
        await session.execute(
            _INSERT_ORG,
            {"id": str(org_id), "name": name, "slug": slug, "clerk_org_id": clerk_org_id},
        )
    ).first()
    if row is not None:
        created = True
        resolved_id = uuid.UUID(str(row[0]))
    else:
        created = False
        existing = (await session.execute(_SELECT_ORG_BY_SLUG, {"slug": slug})).first()
        resolved_id = uuid.UUID(str(existing[0])) if existing else org_id

    if not created and clerk_org_id is not None and link_existing:
        # The row predates this Clerk organization (the seeder can create a tenant before it
        # exists in Clerk). Fill the link so the tenant becomes resolvable, but only when the
        # column is still NULL - see _LINK_CLERK_ORG.
        await session.execute(
            _LINK_CLERK_ORG, {"id": str(resolved_id), "clerk_org_id": clerk_org_id}
        )

    if created:
        # Every tenant-scoped write from here on (starting with the outbox row itself) needs
        # `app.current_org` set - `organizations` is the one exception, and it is already
        # written.
        await set_rls(session, resolved_id)
        await enqueue(
            session,
            EventEnvelope(
                topic="tenant-events",
                payload={
                    "event_type": "provisioned",
                    "org_slug": slug,
                    "plan_code": plan_code,
                },
                org_id=str(resolved_id),
                producer=PRODUCER,
                idempotency_key=f"org:{resolved_id}:provisioned",
            ),
        )

    return ProvisionResult(org_id=resolved_id, created=created)


async def sync_internal_org_id_to_clerk(result: ProvisionResult, clerk_org_id: str | None) -> bool:
    """Publish the resolved internal `org_id` to Clerk, so the AI tier's claim is populated.

    Call AFTER the provisioning transaction has committed. Two reasons, both load-bearing: a
    network call to a third party must not hold a database transaction open, and publishing an
    `org_id` that a later rollback erases would leave Clerk pointing at a tenant that does not
    exist.

    Returns False and logs when it is skipped or fails, rather than raising - see
    `libs/shared/clerk_admin.py`. Safe to re-run: the underlying write is a merge.
    """
    if clerk_org_id is None:
        # A tenant with no Clerk organization behind it (the seeder makes these). There is
        # nothing to publish to, and that is not a failure.
        return False
    from libs.shared.clerk_admin import set_internal_org_id

    return await set_internal_org_id(clerk_org_id, result.org_id)


# --- The full tenant birth: Plan 1-2 section 3.4 step 3 ---------------------------------------
#
# `provision_organization` above is the tenant ROOT plus its lifecycle event, and is what the
# seeder has always called. `provision_tenant` is the complete birth the plan specifies for
# `POST /orgs`: the root, its `tenant_settings` defaults, its `data_residency` row, the initial
# owner's mirror row and `rbac_grants` owner grant, and the `tenant-events` outbox row, all in
# the CALLER'S transaction. Nothing here commits; the caller's `session.begin()` does, so any
# failure below rolls every one of these rows back together (the plan's rollback acceptance
# check).

_SELECT_ORG_BY_CLERK_ID = text("SELECT 1 FROM organizations WHERE clerk_org_id = :c")
# Defaults come from the column defaults, not from this file: the region default (`us`, MVP
# single-region per ADR-0016) and `db_cell` are schema facts, and restating them here would be
# a second place to change when the residency decision moves.
_INSERT_TENANT_SETTINGS = text(
    "INSERT INTO tenant_settings (org_id) VALUES (:o) ON CONFLICT (org_id) DO NOTHING"
)
_INSERT_DATA_RESIDENCY = text(
    "INSERT INTO data_residency (org_id) VALUES (:o) ON CONFLICT (org_id) DO NOTHING"
)
# The owner's mirror row. Clerk stays the source of truth: the first `user.created` /
# `user.updated` webhook for this person upserts on the same `(org_id, clerk_user_id)` key
# (`routes/auth.py::_apply`) and overwrites `email` with Clerk's value. The row has to exist
# NOW because `rbac_grants.user_id` is a foreign key to it, and a tenant born with no owner
# grant is a tenant nobody can administer: every `require_tier` check in it would deny.
_INSERT_OWNER_USER = text(
    "INSERT INTO users (org_id, clerk_user_id, email) VALUES (:o, :cuid, :email) "
    "ON CONFLICT (org_id, clerk_user_id) DO UPDATE SET updated_at = now() "
    "RETURNING id"
)
_INSERT_OWNER_GRANT = text(
    "INSERT INTO rbac_grants (org_id, user_id, tier, granted_by) " "VALUES (:o, :u, 'owner', NULL)"
)


class TenantConflict(Exception):
    """The slug or the Clerk organization already names a tenant.

    Deliberately carries no detail about WHICH one collided: unique indexes are not subject to
    RLS, so a 409 that named the constraint or echoed the value would be an existence oracle
    over the customer base.
    """


@dataclass(frozen=True)
class TenantOwner:
    clerk_user_id: str
    email: str


@dataclass(frozen=True)
class TenantProvisioned:
    org_id: uuid.UUID
    owner_user_id: uuid.UUID


async def provision_tenant(
    session: AsyncSession,
    *,
    name: str,
    slug: str,
    clerk_org_id: str,
    owner: TenantOwner,
    plan_code: str = "free",
    org_id: uuid.UUID | None = None,
) -> TenantProvisioned:
    """Create a complete tenant atomically, or raise `TenantConflict`. Never links or reuses.

    Unlike `provision_organization`, a slug that already exists is an error here, not an
    idempotent no-op: on the request path a matching slug is somebody else's tenant. Request
    retries are made safe by the `Idempotency-Key` middleware replaying the stored 201, not by
    re-running this function into an existing row.

    Leaves `app.current_org` set to the NEW tenant (that is what makes the tenant-scoped
    inserts below pass RLS). A caller that writes anything for another tenant afterwards must
    `set_rls` back first.
    """
    if (await session.execute(_SELECT_ORG_BY_CLERK_ID, {"c": clerk_org_id})).first():
        raise TenantConflict
    if (await session.execute(_SELECT_ORG_BY_SLUG, {"slug": slug})).first():
        raise TenantConflict

    result = await provision_organization(
        session,
        org_id=org_id or uuid.uuid4(),
        name=name,
        slug=slug,
        plan_code=plan_code,
        clerk_org_id=clerk_org_id,
        link_existing=False,
    )
    if not result.created:
        # Lost a race on the slug between the check above and the INSERT.
        raise TenantConflict

    # `provision_organization` set RLS to the new tenant before writing its outbox row; it is
    # re-asserted here so this function does not depend on that ordering detail.
    await set_rls(session, result.org_id)
    params = {"o": str(result.org_id)}
    await session.execute(_INSERT_TENANT_SETTINGS, params)
    await session.execute(_INSERT_DATA_RESIDENCY, params)
    owner_row = (
        await session.execute(
            _INSERT_OWNER_USER,
            {**params, "cuid": owner.clerk_user_id, "email": owner.email},
        )
    ).one()
    owner_user_id = uuid.UUID(str(owner_row[0]))
    await session.execute(_INSERT_OWNER_GRANT, {**params, "u": str(owner_user_id)})

    return TenantProvisioned(org_id=result.org_id, owner_user_id=owner_user_id)


# --- workspace membership: one mirror row + one grant per Clerk membership -------------------

_UPSERT_MEMBER = text(
    "INSERT INTO users (org_id, clerk_user_id, email, avatar_url, display_name) "
    "VALUES (:o, :cuid, :email, :avatar, :name) "
    "ON CONFLICT (org_id, clerk_user_id) DO UPDATE SET "
    "  email = EXCLUDED.email, avatar_url = EXCLUDED.avatar_url, "
    # A membership event may carry no name; it never erases one the user sync stored.
    "  display_name = COALESCE(EXCLUDED.display_name, users.display_name), "
    "  updated_at = now(), deleted_at = NULL "
    "RETURNING id"
)
# Clerk's role decides admin vs member. An `owner` grant is never lowered from here: ownership
# is this system's own fact (the tenant's creator, or a promotion through /rbac), not Clerk's.
_UPSERT_MEMBER_GRANT = text(
    "INSERT INTO rbac_grants (org_id, user_id, tier) VALUES (:o, :u, :t) "
    "ON CONFLICT (org_id, user_id) DO UPDATE SET tier = EXCLUDED.tier "
    "WHERE rbac_grants.tier <> 'owner'"
)
_SOFT_DELETE_MEMBER = text(
    "UPDATE users SET deleted_at = now(), updated_at = now() "
    "WHERE org_id = :o AND clerk_user_id = :cuid AND deleted_at IS NULL RETURNING id"
)
_REVOKE_MEMBER_GRANT = text(
    "DELETE FROM rbac_grants WHERE org_id = :o AND user_id = :u AND tier <> 'owner'"
)


def tier_for_clerk_role(role: str) -> str:
    return "admin" if role == "org:admin" else "member"


async def ensure_member(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    clerk_user_id: str,
    email: str,
    avatar_url: str | None,
    tier: str,
    display_name: str | None = None,
) -> uuid.UUID:
    """Make a Clerk member of this tenant's organization a member here too: the `users` mirror
    row and an rbac grant. Idempotent; never lowers an `owner`. Caller has set RLS to `org_id`."""
    params = {"o": str(org_id)}
    user_id = (
        await session.execute(
            _UPSERT_MEMBER,
            {
                **params,
                "cuid": clerk_user_id,
                "email": email,
                "avatar": avatar_url,
                "name": display_name,
            },
        )
    ).scalar_one()
    await session.execute(_UPSERT_MEMBER_GRANT, {**params, "u": str(user_id), "t": tier})
    return uuid.UUID(str(user_id))


async def remove_member(session: AsyncSession, *, org_id: uuid.UUID, clerk_user_id: str) -> bool:
    """The member left (or was removed from) the Clerk organization: soft-delete the mirror row
    in THIS tenant only and drop a non-owner grant. Returns whether anything changed."""
    params = {"o": str(org_id)}
    row = (await session.execute(_SOFT_DELETE_MEMBER, {**params, "cuid": clerk_user_id})).first()
    if row is None:
        return False
    await session.execute(_REVOKE_MEMBER_GRANT, {**params, "u": str(row[0])})
    return True
