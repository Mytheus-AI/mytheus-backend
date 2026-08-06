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
    "INSERT INTO organizations (id, name, slug, status, plan_id) "
    "VALUES (:id, :name, :slug, 'active', NULL) "
    "ON CONFLICT (slug) DO NOTHING "
    "RETURNING id"
)
_SELECT_ORG_BY_SLUG = text("SELECT id FROM organizations WHERE slug = :slug")


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
    """
    row = (
        await session.execute(_INSERT_ORG, {"id": str(org_id), "name": name, "slug": slug})
    ).first()
    if row is not None:
        created = True
        resolved_id = uuid.UUID(str(row[0]))
    else:
        created = False
        existing = (await session.execute(_SELECT_ORG_BY_SLUG, {"slug": slug})).first()
        resolved_id = uuid.UUID(str(existing[0])) if existing else org_id

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
