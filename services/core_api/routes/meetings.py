"""Meetings CRUD. Plan section 3.6.

Three invariants hold on every route here, and each is enforced structurally rather than by
review:

- `org_id` is never a path or query parameter. It comes from `RequestContext`, which comes
  from the verified token. There is no code path by which a caller supplies it.
- The row write, the `meeting-events` outbox row and the `audit_logs` row commit in ONE
  transaction, because they are one fact. `tenant_session` yields inside `session.begin()`,
  so a handler cannot accidentally split them.
- A meeting in another tenant is a 404, never a 403. RLS makes it genuinely invisible, so the
  handler is not choosing to hide it; a 403 would confirm the id exists and turn any id into
  a probe.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_MEETING_CREATED, ACTION_MEETING_UPDATED, audit
from libs.shared.authz import (
    RequestContext,
    not_found,
    require_tier,
    tenant_session,
)
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.schemas import (
    Meeting,
    MeetingCreate,
    MeetingPage,
    MeetingUpdate,
    Participant,
)

router = APIRouter(prefix="/api/v1", tags=["meetings"])

PRODUCER = "core-api"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

_MEETING_COLUMNS = (
    "id, title, status, scheduled_start, scheduled_end, livekit_room, created_by, "
    "created_at, updated_at"
)


# --- cursor helpers -------------------------------------------------------------------------


def _encode_cursor(created_at: datetime, row_id: uuid.UUID) -> str:
    """Keyset cursor over (created_at, id).

    `id` is in the key, not decoration: `created_at` alone is not unique, and two meetings
    created in the same microsecond would make one of them unreachable by paging. Base64 so
    clients treat it as opaque and the encoding stays free to change.
    """
    raw = json.dumps({"c": created_at.isoformat(), "i": str(row_id)}).encode()
    return base64.urlsafe_b64encode(raw).decode()


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(raw["c"]), uuid.UUID(raw["i"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        # A malformed cursor is the caller's error, and it gets its own code so a client can
        # tell "your cursor expired or was mangled" from "your request body was wrong".
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed pagination cursor") from exc


def _row_to_meeting(row: Any, participants: list[Participant] | None = None) -> Meeting:
    return Meeting(**dict(row), participants=participants or [])


def _not_a_participant() -> ProblemDetail:
    # Mirrors services/meeting_service/routes.py's `_not_a_participant`: same code, same
    # status, same message, for the same reason - deny-by-default, no local users-mirror row
    # is indistinguishable from an ordinary non-participant. Not shared across services
    # because it is three lines and pulling a cross-service import in for it would cost more
    # than it saves.
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.NOT_A_PARTICIPANT,
        "You are not a participant of this meeting",
    )


async def _load_participants(
    session: AsyncSession, meeting_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[Participant]]:
    """One query for the whole page, not one per meeting."""
    if not meeting_ids:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT meeting_id, user_id, role_in_meeting FROM meeting_participants "
                    "WHERE meeting_id = ANY(:ids)"
                ),
                {"ids": meeting_ids},
            )
        )
        .mappings()
        .all()
    )
    out: dict[uuid.UUID, list[Participant]] = {}
    for r in rows:
        out.setdefault(r["meeting_id"], []).append(
            Participant(user_id=r["user_id"], role_in_meeting=r["role_in_meeting"])
        )
    return out


async def _fetch_meeting(session: AsyncSession, meeting_id: uuid.UUID) -> Any:
    """Fetch or 404. No `org_id` predicate: RLS already scopes it, and adding one by hand
    invites a route that forgets it and quietly works anyway on a superuser connection."""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_MEETING_COLUMNS} FROM meetings "
                    "WHERE id = :id AND deleted_at IS NULL"
                ),
                {"id": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


# --- routes ---------------------------------------------------------------------------------


@router.post("/meetings", status_code=status.HTTP_201_CREATED, response_model=Meeting)
async def create_meeting(
    body: MeetingCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Meeting:
    """Create a meeting, its participants, its event and its audit record, atomically."""
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meetings (org_id, title, scheduled_start, scheduled_end, "
                    "created_by) VALUES (:org, :title, :start, :end, :by) "
                    f"RETURNING {_MEETING_COLUMNS}"
                ),
                {
                    "org": str(ctx.org_id),
                    "title": body.title,
                    "start": body.scheduled_start,
                    "end": body.scheduled_end,
                    "by": str(ctx.user_id) if ctx.user_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    meeting_id = row["id"]

    participants: list[Participant] = []
    for user_id in dict.fromkeys(body.participant_ids):  # de-duplicated, order preserved
        # The FK to users plus RLS means a participant id belonging to another tenant is
        # simply not insertable: the row is invisible, so the reference fails. A cross-tenant
        # participant is unrepresentable rather than merely rejected.
        await session.execute(
            text(
                "INSERT INTO meeting_participants (meeting_id, user_id, org_id) "
                "VALUES (:m, :u, :org) ON CONFLICT DO NOTHING"
            ),
            {"m": str(meeting_id), "u": str(user_id), "org": str(ctx.org_id)},
        )
        participants.append(Participant(user_id=user_id))

    await enqueue(
        session,
        EventEnvelope(
            topic="meeting-events",
            payload={
                "meeting_id": str(meeting_id),
                "event_type": "created",
                "livekit_room": row["livekit_room"],
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"meeting:{meeting_id}:created",
        ),
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_CREATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"title": body.title},
        ip=ctx.ip,
    )

    response.headers["Location"] = f"/api/v1/meetings/{meeting_id}"
    return _row_to_meeting(row, participants)


@router.get("/meetings", response_model=MeetingPage)
async def list_meetings(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
    status_filter: Annotated[str | None, Query(alias="status")] = None,
) -> MeetingPage:
    """Keyset-paginated listing. No `org_id` parameter: RLS scopes it to the caller's tenant."""
    params: dict[str, Any] = {"limit": limit + 1}  # +1 to detect a next page without counting
    where = ["deleted_at IS NULL"]

    if status_filter:
        where.append("status = :status")
        params["status"] = status_filter
    if cursor:
        created_at, row_id = _decode_cursor(cursor)
        # Strict keyset comparison on the same tuple the ORDER BY uses, so a row is never
        # skipped or repeated when rows are inserted mid-page.
        where.append("(created_at, id) < (:cur_created, :cur_id)")
        params["cur_created"] = created_at
        params["cur_id"] = str(row_id)

    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_MEETING_COLUMNS} FROM meetings WHERE {' AND '.join(where)} "
                    "ORDER BY created_at DESC, id DESC LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )

    has_more = len(rows) > limit
    page = list(rows[:limit])
    by_meeting = await _load_participants(session, [r["id"] for r in page])
    return MeetingPage(
        items=[_row_to_meeting(r, by_meeting.get(r["id"], [])) for r in page],
        next_cursor=(
            _encode_cursor(page[-1]["created_at"], page[-1]["id"]) if has_more and page else None
        ),
    )


@router.get("/meetings/{meeting_id}", response_model=Meeting)
async def get_meeting(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Meeting:
    row = await _fetch_meeting(session, meeting_id)
    participants = await _load_participants(session, [meeting_id])
    return _row_to_meeting(row, participants.get(meeting_id, []))


@router.get(
    "/meetings/{meeting_id}/participants/{clerk_user_id}",
    status_code=status.HTTP_200_OK,
    response_model=Participant,
)
async def check_participant(
    meeting_id: uuid.UUID,
    clerk_user_id: str,
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Participant:
    """Deny-by-default participant check. `realtime-gateway` (Task 6) calls this on every
    Socket.io `join` to decide FORBIDDEN vs allow before joining the room.

    No `require_tier` gate, matching `meeting_service.routes.mint_token`'s precedent rather
    than the `require_tier("member")` used elsewhere in this file: a genuine participant is
    not guaranteed to hold an `rbac_grants` row (see that route's "no local mirror row" test
    case), and gating this specific check on RBAC tier would wrongly deny a real participant
    who has none - exactly the failure mode the token-mint route was written to avoid.
    `tenant_session` alone still fails closed on a bad/missing token and on a cross-tenant id
    (via the existence check below), which is the authz this endpoint actually needs.

    **Contract-verification note (task-5 report, "contract verification" section):** the path
    parameter is the caller's Clerk-native user id (the JWT `sub` claim), NOT our internal
    `users.id`. `realtime-gateway` has no database and therefore no way to know our internal
    UUID - its entire identity model runs on the Clerk claims already in the socket's verified
    JWT (see `mytheus-realtime`'s `libs/auth.ts`). Resolving that opaque id to `users.id`
    HERE, the same way `libs.shared.authz.tenant_context` resolves the caller's own principal
    via `clerk_user_id`, is what lets this route accept the identifier the gateway actually
    has rather than one it has no way to produce.
    """
    await _fetch_meeting(session, meeting_id)  # 404 before anything else; RLS-scoped

    row = (
        (
            await session.execute(
                text(
                    "SELECT mp.user_id, mp.role_in_meeting FROM meeting_participants mp "
                    "JOIN users u ON u.id = mp.user_id "
                    "WHERE mp.meeting_id = :meeting_id AND u.clerk_user_id = :clerk_user_id"
                ),
                {"meeting_id": str(meeting_id), "clerk_user_id": clerk_user_id},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise _not_a_participant()
    return Participant(user_id=row["user_id"], role_in_meeting=row["role_in_meeting"])


@router.patch("/meetings/{meeting_id}", response_model=Meeting)
async def update_meeting(
    meeting_id: uuid.UUID,
    body: MeetingUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Meeting:
    await _fetch_meeting(session, meeting_id)  # 404 before anything is written

    fields = body.model_dump(exclude_unset=True)
    assignments = ", ".join(f"{name} = :{name}" for name in fields)
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE meetings SET {assignments}, updated_at = now() "
                    f"WHERE id = :id RETURNING {_MEETING_COLUMNS}"
                ),
                {**fields, "id": str(meeting_id)},
            )
        )
        .mappings()
        .one()
    )

    # Plan 3-4 section 3.3 step 4: "verify [the meeting-events producer] emits
    # created/updated/ended via the outbox". It did not - every update emitted `event_type:
    # "updated"` unconditionally, even when `status` became `"ended"`, so a consumer could
    # never distinguish an ordinary field edit from the meeting actually ending without
    # separately fetching and diffing the resource. `MeetingEventType`
    # (mytheus-events schemas/meeting-events/v1.avsc) has always had all three symbols; this
    # was a gap in this route, not a schema gap, and squarely core-api's own file to fix.
    event_type = "ended" if fields.get("status") == "ended" else "updated"

    await enqueue(
        session,
        EventEnvelope(
            topic="meeting-events",
            payload={
                "meeting_id": str(meeting_id),
                "event_type": event_type,
                "livekit_room": row["livekit_room"],
            },
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            # Neither `updated` nor `ended` is a single fact the way `created` is, so the key
            # carries the updated_at timestamp. Keying on the meeting id alone would make the
            # outbox's unique index swallow every update after the first.
            idempotency_key=f"meeting:{meeting_id}:{event_type}:{row['updated_at'].isoformat()}",
        ),
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"changed": sorted(fields)},
        ip=ctx.ip,
    )

    participants = await _load_participants(session, [meeting_id])
    return _row_to_meeting(row, participants.get(meeting_id, []))
