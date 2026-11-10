"""Token-mint route: `POST /api/meetings/{meeting_id}/token`.

Plan 3-4 section 3.1 step 4. Server-mediated, participant-verified, deny-by-default:
`org_id` and `user_id` come only from `RequestContext` (the Clerk-verified token, via
`libs.shared.authz`), never from the path, query, or a request body. The only body this route
accepts is the optional consent acknowledgment (`TokenRequest`), which carries no identity.
Three invariants hold here, each enforced structurally rather than by review, matching
`core_api.routes.meetings`'s pattern:

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

FIX ROUND 1 - why this route manages its own session instead of `Depends(tenant_session)`.
The room-metadata follow-up first tried `background_tasks.add_task(transport.ensure_room, ...)`
on top of the existing `Depends(tenant_session)` shape, reasoning that `BackgroundTasks` runs
after the request's dependencies close their session. That reasoning was backwards: verified
empirically (a minimal FastAPI app with a `yield`-dependency and a background task) that
Starlette runs background tasks BEFORE a `yield`-dependency's post-`yield` cleanup, not after
- so `tenant_session`'s transaction, and the row lock `ensure_meeting_room`'s upsert had just
taken, was STILL OPEN while the background task tried to UPDATE that same row. Two requests
for the same meeting close together self-deadlocked: the second mint's upsert waited on the
first mint's still-open transaction, which would not commit until ITS background task (which
was itself blocked on the second mint's lock wait) finished - neither could ever proceed.
Confirmed by reproducing the hang with `tests/test_meeting_token_route.py::
test_repeated_mint_reuses_the_same_room` (two mints for one meeting, back to back).

The fix: this route depends on `tenant_context` only (auth/tenant resolution; it opens and
closes its own short session internally and returns no session to the caller), and opens ONE
explicit, short-lived session of its own for the meeting/participant/consent checks and the
room upsert - closed and committed BEFORE `background_tasks.add_task` is ever called, so there
is no yield-dependency left open across the background task's own DB use.

CONSENT THAT IS REAL (polish program, Phase 0). ADR-0018 records consent at token mint, but
nothing ever wrote it: the pre-join's checkbox was never sent, so a tenant with
`consent_required = true` answered 428 forever and the room looped the person back to the
pre-join. Now the pre-join sends `{"consent": {"version": "<notice version>"}}` with its token
request; the mint stamps `consented_at = now()` and `consent_version` the first time (never
overwritten: consent is asked once per person per meeting), in the same short transaction as
the checks, before the 428 gate is evaluated. Recorded whether or not the tenant requires it,
because the person did agree and the record should say so. `GET .../session` tells the client
whether it still has to ask. The first successful mint also stamps `meeting_rooms.started_at`
(the meeting's clock). `mint_join_token`
(the LiveKit call the token-mint route always made) needs no session at all, so it runs after,
outside of any transaction, same as before.
"""

from __future__ import annotations

import os
import uuid
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, tenant_context
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.meeting_service.rooms import (
    ensure_meeting_room,
    mark_room_started,
    meeting_has_ended,
    meeting_is_visible,
    sync_livekit_metadata,
)
from services.meeting_service.schemas import RoomSession, TokenRequest, TokenResponse
from services.meeting_service.storage import RecordingConfig
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
    if not await meeting_is_visible(session, meeting_id):
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
                    "SELECT mp.role_in_meeting, mp.consented_at, mp.consent_version, "
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


async def _record_consent(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID, version: str
) -> dict[str, Any]:
    """Stamp this participant's consent once. An existing `consented_at` is never moved and its
    `consent_version` never rewritten: the first acknowledgment is the record."""
    row = (
        (
            await session.execute(
                text(
                    "UPDATE meeting_participants SET consented_at = COALESCE(consented_at, now()), "
                    "consent_version = CASE WHEN consented_at IS NULL THEN :v "
                    "ELSE consent_version END "
                    "WHERE meeting_id = :m AND user_id = :u "
                    "RETURNING consented_at, consent_version"
                ),
                {"v": version, "m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .one()
    )
    return dict(row)


async def _room_started_at(session: AsyncSession, meeting_id: uuid.UUID) -> Any:
    return (
        await session.execute(
            text("SELECT started_at FROM meeting_rooms WHERE meeting_id = :m"),
            {"m": str(meeting_id)},
        )
    ).scalar()


@router.get(
    "/api/meetings/{meeting_id}/session",
    status_code=status.HTTP_200_OK,
    response_model=RoomSession,
)
async def get_room_session(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(tenant_context)],
) -> RoomSession:
    """The caller's standing in this meeting's call: whether consent is required and already
    given (so the pre-join asks once per person per meeting), whether audio/video is recorded
    (so the notice says exactly what is kept), and when the call started (the meeting clock).
    Same existence-then-participation order and codes as the token mint."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await session.execute(_BIND_STATEMENT_TIMEOUT)
        await set_rls(session, ctx.org_id)
        await _fetch_meeting_or_404(session, meeting_id)
        if ctx.user_id is None:
            raise _not_a_participant()
        participant = await _fetch_participant(session, meeting_id, ctx.user_id)
        if participant is None:
            raise _not_a_participant()
        started_at = await _room_started_at(session, meeting_id)
        ended = await meeting_has_ended(session, meeting_id)
    return RoomSession(
        consent_required=bool(participant["consent_required"]),
        consented_at=participant["consented_at"],
        consent_version=participant["consent_version"],
        recording=RecordingConfig.from_env().enabled,
        started_at=started_at,
        ended=ended,
    )


@router.post(
    "/api/meetings/{meeting_id}/token",
    status_code=status.HTTP_200_OK,
    response_model=TokenResponse,
)
async def mint_token(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(tenant_context)],
    transport: Annotated[MeetingTransport, Depends(get_transport)],
    background_tasks: BackgroundTasks,
    body: TokenRequest | None = None,
) -> TokenResponse:
    sessionmaker = get_sessionmaker()

    # One explicit, short-lived session/transaction for every DB read and write this route
    # needs - closed and committed before `background_tasks.add_task` is ever reached. See the
    # module docstring's FIX ROUND 1 note for why this is no longer `Depends(tenant_session)`.
    async with sessionmaker() as session, session.begin():
        await session.execute(_BIND_STATEMENT_TIMEOUT)
        await set_rls(session, ctx.org_id)

        # Existence (tenant-scoped) before participation: see the module docstring.
        await _fetch_meeting_or_404(session, meeting_id)

        # An ended meeting stays ended: its old Join link must not open a fresh room (which
        # would also report it live again). 409 with the frozen VERSION_CONFLICT code.
        if await meeting_has_ended(session, meeting_id):
            raise ProblemDetail(
                status.HTTP_409_CONFLICT, ErrorCode.VERSION_CONFLICT, "This meeting has ended"
            )

        if ctx.user_id is None:
            # A verified Clerk user with no local `users` mirror row cannot be a participant
            # of anything (the FK requires a real `users.id`). Same denial, same code, as the
            # ordinary non-participant case below — there is nothing more specific to say.
            raise _not_a_participant()

        participant = await _fetch_participant(session, meeting_id, ctx.user_id)
        if participant is None:
            raise _not_a_participant()

        if body is not None and body.consent is not None:
            participant.update(
                await _record_consent(session, meeting_id, ctx.user_id, body.consent.version)
            )

        if participant["consent_required"] and participant["consented_at"] is None:
            raise _consent_required()

        result = await ensure_meeting_room(session, ctx.org_id, meeting_id)
        started_at = await mark_room_started(session, ctx.org_id, meeting_id)
    # session/transaction closed and committed here - the connection is back in the pool
    # before anything below this line runs.

    # `transport.ensure_room` is a real LiveKit HTTP call; it must never run while a Postgres
    # transaction from this request is open (see module docstring). Skipped once
    # `needs_livekit_sync` is false: a synced room never pays this cost again on a later mint.
    if result.needs_livekit_sync:
        background_tasks.add_task(
            sync_livekit_metadata,
            sessionmaker,
            transport,
            org_id=ctx.org_id,
            room=result.room,
        )

    token = await transport.mint_join_token(
        org_id=ctx.org_id,
        meeting_id=meeting_id,
        user_id=ctx.user_id,
        room=result.room,
        role_in_meeting=participant["role_in_meeting"] or _DEFAULT_ROLE,
        ttl_seconds=_TTL_SECONDS,
    )

    return TokenResponse(
        token=token,
        room=result.room,
        livekit_url=os.environ.get("LIVEKIT_URL", ""),
        consented_at=participant["consented_at"],
        consent_version=participant["consent_version"],
        started_at=started_at,
    )
