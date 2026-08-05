"""The single sanctioned way to read `organizations`, and the tenant-claim checkpoint.

`organizations` is the one table in the system with no `tenant_isolation` policy: it is the
tenant root, its `id` IS the `org_id`, and the edge must read it BEFORE any tenant is known in
order to turn a token's opaque Clerk organization id into our `org_id`. Canon
`_canon/02-data-model.md` section 7 rule 7 and ADR-0002 D3 record why; the accepted risk is
that a forgotten `WHERE` here exposes the customer directory.

This module is compensating control 2 for that decision: exactly one query shape PER lookup
key, in one place, reviewed once. Two things bound the risk alongside it:

- `mytheus_app` holds column-level `SELECT` on this table and nothing else, so `SELECT *` is
  refused by the database rather than by convention.
- Every query below always carries an equality predicate on its lookup key. Do not add a
  third reader, and do not hand-write a raw `SELECT ... FROM organizations` anywhere else in
  the codebase: if a new caller needs the tenant root from the verified JWT's opaque Clerk
  claim, it calls `resolve_org_id`; if it already holds a candidate INTERNAL `org_id` from a
  source that is not the verified JWT (e.g. LiveKit webhook room metadata -
  `services.meeting_service.webhook._resolve_org_id`) and needs to confirm it names a real,
  active, non-deleted tenant before trusting it, it calls `resolve_org_by_id`. Two functions,
  not one, because they take different keys and answer to different callers with different
  failure shapes (`resolve_org_id` raises a `ProblemDetail`, the correct behavior for a token
  holder; `resolve_org_by_id` returns `None`, leaving the caller to decide its OWN error
  framing - a webhook is not a token holder, and Task 4's ambiguity resolution #1 wants a
  webhook-specific 400, not this module's tenant-directory-confidentiality-focused 401).

On caching. ADR-0010 D2 says cache the MAPPING, never the VERDICT. No cache is implemented
here yet, and that is deliberate rather than an omission: `clerk_org_id`, `id` and `status`
all live in the same row, so one indexed lookup already returns both the mapping and the
verdict, and a mapping cache in front of it would save nothing while adding a poisoning
surface. The caching in the ADR only starts paying once a suspension arrives as a
`tenant-events` message that can invalidate a cached entry, because only then can the status
read be skipped safely. Exit trigger: build the cache in the same change that adds the
`tenant-events` consumer, never before. Until then a suspension takes effect on the next
request, which is the property that actually matters.
"""

from __future__ import annotations

import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.errors import ErrorCode, ProblemDetail

# The equality predicate on clerk_org_id is not optional: it is what keeps a bug in this
# module from becoming a directory dump.
_RESOLVE = text(
    "SELECT id, status, deleted_at FROM organizations WHERE clerk_org_id = :clerk_org_id"
)

# Same column set, keyed by our OWN internal id instead. Same reasoning: the equality
# predicate is what keeps this from becoming a directory dump.
_RESOLVE_BY_ID = text("SELECT id, status, deleted_at FROM organizations WHERE id = :id")


def _denied() -> ProblemDetail:
    """One byte-identical response for all three negative outcomes (ADR-0010 D2).

    Unknown organization, suspended organization and soft-deleted organization must be
    indistinguishable to the caller. Differentiating them turns this path into an oracle for
    which Clerk organizations are Mytheus customers and which have been suspended, and tenant
    existence is confidential. Same status, same code, same detail, and no branch that
    returns before the query runs.
    """
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Token carries no usable organization")


async def resolve_org_id(session: AsyncSession, clerk_org_id: str) -> uuid.UUID:
    """Resolve Clerk's opaque organization id to our `org_id`, or fail closed.

    Raises the single denial ProblemDetail if the organization is unknown, suspended or
    soft-deleted. A caller may set `app.current_org` from the returned value and not before:
    until this returns, no tenant is established and no tenant-scoped query may run.
    """
    if not clerk_org_id:
        raise _denied()

    row = (await session.execute(_RESOLVE, {"clerk_org_id": clerk_org_id})).mappings().first()
    if row is None or row["deleted_at"] is not None or row["status"] != "active":
        raise _denied()

    return row["id"] if isinstance(row["id"], uuid.UUID) else uuid.UUID(str(row["id"]))


async def resolve_org_by_id(session: AsyncSession, org_id: uuid.UUID) -> uuid.UUID | None:
    """Confirm an INTERNAL `org_id` names a real, active, non-deleted tenant, or return None.

    The second sanctioned reader of `organizations` (see module docstring). Unlike
    `resolve_org_id`, this never raises: the caller here does not hold a verified JWT (a
    webhook does not carry one), so there is no single "the token is bad" response to return
    on its behalf - it decides its own error framing. Returning `None` for every negative case
    (unknown, suspended, deleted) - never a different value per case - keeps this table's
    directory-confidentiality property intact regardless of who calls it.
    """
    row = (await session.execute(_RESOLVE_BY_ID, {"id": str(org_id)})).mappings().first()
    if row is None or row["deleted_at"] is not None or row["status"] != "active":
        return None

    return row["id"] if isinstance(row["id"], uuid.UUID) else uuid.UUID(str(row["id"]))
