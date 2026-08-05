"""Token-mint route: `POST /api/meetings/{meeting_id}/token`.

Plan 3-4 section 3.1 step 4. Server-mediated, participant-verified, deny-by-default:
`org_id` and `user_id` come only from `RequestContext` (the Clerk-verified token, via
`libs.shared.authz`), never from the path, query, or a request body — this route accepts no
body at all. Three invariants hold here, each enforced structurally rather than by review,
matching `core_api.routes.meetings`'s pattern:

- **Existence check before participation check.** `_fetch_meeting_or_404` runs first and is
  RLS-scoped with no hand-written `org_id` predicate, so a meeting belonging to another
  tenant is genuinely invisible and returns 404 exactly like a meeting that does not exist.
  Only once the meeting is confirmed to be in the caller's own tenant does the route ask
  "is this caller a participant of it" — and answers that with 403, not 404, because at that
  point existence is no longer the secret being protected. Checking these in the reverse
  order would let a non-participant distinguish "this id belongs to another org" (404) from
  "this id is in my org but I'm not on it" (403) for ids in OTHER tenants too, which leaks
  cross-tenant existence through the status code alone — see `libs.shared.authz.not_found`'s
  docstring and Task 3's brief (ambiguity resolution #3).
- **Deny-by-default.** No participant row (including the case where the caller has no local
  `users` mirror row yet, so `ctx.user_id is None`) is 403 `NOT_A_PARTICIPANT`, never a silent
  200 with a token the caller should not have received.
- **The consent gate is inert unless explicitly turned on.** ADR-0018: `tenant_settings.
  consent_required` defaults false for every tenant, and when it is false this route's
  behavior is byte-for-byte what it would be if the gate did not exist — same query shape,
  same code path, the `WHERE` clause just never trips. Only `consent_required = true` AND a
  NULL `consented_at` for that participant produces 428. There is no tenant_settings row for
  a freshly-seeded org until identity-tenant-service's `/orgs` route exists (a later task),
  so the join below is a LEFT JOIN with `COALESCE(..., false)`: a missing row must mean "not
  required", not "500", or every meeting in a tenant that has not yet had a settings row
  created would be permanently unjoinable.
"""

from __future__ import annotations

import os
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, tenant_context, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.meeting_service.rooms import ensure_meeting_room
from services.meeting_service.schemas import TokenResponse
from services.meeting_service.transport import MeetingTransport, select_transport

router = APIRouter(tags=["meetings"])

# Plan 05 section 3.5: a tight cap on this path so the token-mint p95 < 250 ms SLO holds even
# under a slow query plan or lock wait, rather than a stuck request holding a connection open
# indefinitely. Transaction-local (`SET LOCAL`), matching the `set_config(..., true)` pattern
# `libs.shared.tenancy.set_rls` already uses on the same session/transaction: it reverts at
# commit/rollback and is never visible to the next request on a pooled connection.
_BIND_STATEMENT_TIMEOUT = text("SET LOCAL statement_timeout = '2000ms'")

_DEFAULT_ROLE = "attendee"
_TTL_SECONDS = 600


def get_transport() -> MeetingTransport:
    """FastAPI dependency seam so tests can override the transport without touching env vars."""
    return select_transport()


def _not_a_participant() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.NOT_A_PARTICIPANT,
        "You are not a participant of this meeting",
    )


def _consent_required() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_428_PRECONDITION_REQUIRED,
        ErrorCode.CONSENT_REQUIRED,
        "This meeting is recorded and transcribed by AI agents; acknowledge consent before joining",
    )


async def _fetch_meeting_or_404(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    """RLS-scoped existence check. No `org_id` predicate: RLS already scopes it (matches
    `core_api.routes.meetings._fetch_meeting`), so a cross-tenant id is invisible rather than
    excluded by a query this route could forget to write."""
    row = (
        await session.execute(
            text("SELECT id FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if row is None:
        raise not_found()


async def _fetch_participant(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> dict[str, Any] | None:
    """One round trip for the participant row AND the tenant's consent flag.

    `tenant_settings` carries its own `tenant_isolation` policy, so the LEFT JOIN needs no
    hand-written `org_id` predicate either — both sides are already scoped to `app.
    current_org` by RLS. `COALESCE(ts.consent_required, false)` is the fail-open-to-"not
    required" default for a tenant with no settings row yet, not a security-relevant
    fail-open: the alternative (failing the whole mint on a missing settings row) would make
    consent enforcement break meetings for every tenant that has never touched
    `tenant_settings`, which is every tenant today.
    """
    row = (
        (
            await session.execute(
                text(
                    "SELECT mp.role_in_meeting, mp.consented_at, "
                    "COALESCE(ts.consent_required, false) AS consent_required "
                    "FROM meeting_participants mp "
                    "LEFT JOIN tenant_settings ts ON ts.org_id = mp.org_id "
                    "WHERE mp.meeting_id = :meeting_id AND mp.user_id = :user_id"
                ),
                {"meeting_id": str(meeting_id), "user_id": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row is not None else None


@router.post(
    "/api/meetings/{meeting_id}/token",
    status_code=status.HTTP_200_OK,
    response_model=TokenResponse,
)
async def mint_token(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(tenant_context)],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    transport: Annotated[MeetingTransport, Depends(get_transport)],
) -> TokenResponse:
    await session.execute(_BIND_STATEMENT_TIMEOUT)

    # Existence (tenant-scoped) before participation: see the module docstring.
    await _fetch_meeting_or_404(session, meeting_id)

    if ctx.user_id is None:
        # A verified Clerk user with no local `users` mirror row cannot be a participant of
        # anything (the FK requires a real `users.id`). Same denial, same code, as the
        # ordinary non-participant case below — there is nothing more specific to say.
        raise _not_a_participant()

    participant = await _fetch_participant(session, meeting_id, ctx.user_id)
    if participant is None:
        raise _not_a_participant()

    if participant["consent_required"] and participant["consented_at"] is None:
        raise _consent_required()

    room = await ensure_meeting_room(session, ctx.org_id, meeting_id, transport)

    token = await transport.mint_join_token(
        org_id=ctx.org_id,
        meeting_id=meeting_id,
        user_id=ctx.user_id,
        room=room,
        role_in_meeting=participant["role_in_meeting"] or _DEFAULT_ROLE,
        ttl_seconds=_TTL_SECONDS,
    )

    return TokenResponse(token=token, room=room, livekit_url=os.environ.get("LIVEKIT_URL", ""))
