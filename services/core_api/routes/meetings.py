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

from libs.shared.audit import (
    ACTION_MEETING_CREATED,
    ACTION_MEETING_PARTICIPANTS_ADDED,
    ACTION_MEETING_UPDATED,
    audit,
)
from libs.shared.authz import (
    RequestContext,
    not_found,
    require_tier,
    tenant_context,
    tenant_session,
)
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.brain import latest_statuses, may_see_source
from services.core_api.schemas import (
    MAX_AGENDA_ITEMS,
    AgendaDraft,
    AgendaDraftItem,
    AgendaDraftRequest,
    AgendaItem,
    AgendaItemCreate,
    AgendaItemStatusUpdate,
    AgendaReplace,
    AttendanceUpdate,
    CarryForward,
    CarryForwardAction,
    CarryForwardDecision,
    CarryForwardQuestion,
    Meeting,
    MeetingCreate,
    MeetingDetail,
    MeetingPage,
    MeetingRef,
    MeetingUpdate,
    Participant,
    ParticipantCheck,
    ParticipantsAdd,
)
from services.core_api.visibility import is_visible

router = APIRouter(prefix="/api/v1", tags=["meetings"])

PRODUCER = "core-api"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

_MEETING_COLUMNS = (
    "id, title, status, scheduled_start, scheduled_end, livekit_room, created_by, "
    "previous_meeting_id, confidential, template_id, source, created_at, updated_at"
)
_AGENDA_COLUMNS = (
    "id, position, title, owner_user_id, duration_minutes, status, status_source, agent_note, "
    "updated_at, goal_id"
)
# ADR-0039 B5: a meeting that is over takes no attendance change.
_CLOSED = frozenset({"ended", "cancelled"})
# ADR-0038 D5 caps an agenda item's title at 200 characters; a drafted "Status: ..." item too.
_MAX_TITLE = 200


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


def _host_of(participants: list[Participant]) -> uuid.UUID | None:
    return next((p.user_id for p in participants if p.role_in_meeting == "host"), None)


def _row_to_meeting(row: Any, participants: list[Participant] | None = None) -> Meeting:
    participants = participants or []
    return Meeting(**dict(row), participants=participants, host_user_id=_host_of(participants))


def _field_error(loc: list[str], message: str) -> ProblemDetail:
    """A 422 in the same shape as a request-validation failure, naming the offending field in
    `errors[].loc`, for rules only the database can check (ADR-0038 D3/D5: "422 naming the
    field")."""
    return ProblemDetail(
        422,
        ErrorCode.VALIDATION_FAILED,
        message,
        extra={"errors": [{"loc": ["body", *loc], "msg": message, "type": "value_error"}]},
    )


async def _load_agenda(session: AsyncSession, meeting_id: uuid.UUID) -> list[AgendaItem]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_AGENDA_COLUMNS} FROM meeting_agenda_items "
                    "WHERE meeting_id = :m ORDER BY position, id"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    return [AgendaItem(**dict(r)) for r in rows]


async def _meeting_ref(session: AsyncSession, meeting_id: uuid.UUID | None) -> MeetingRef | None:
    """The linked meeting, if this tenant can still see it (a soft-deleted one reads as none)."""
    if meeting_id is None:
        return None
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, title, scheduled_start FROM meetings "
                    "WHERE id = :id AND deleted_at IS NULL"
                ),
                {"id": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    return MeetingRef(**dict(row)) if row is not None else None


async def _detail(session: AsyncSession, row: Any, viewer: uuid.UUID | None) -> MeetingDetail:
    meeting_id = row["id"]
    participants = (await _load_participants(session, [meeting_id], viewer)).get(meeting_id, [])
    return MeetingDetail(
        **_row_to_meeting(row, participants).model_dump(),
        previous_meeting=await _meeting_ref(session, row["previous_meeting_id"]),
        agenda=await _load_agenda(session, meeting_id),
    )


async def _participant_ids(session: AsyncSession, meeting_id: uuid.UUID) -> set[uuid.UUID]:
    rows = await session.execute(
        text("SELECT user_id FROM meeting_participants WHERE meeting_id = :m"),
        {"m": str(meeting_id)},
    )
    return {r[0] for r in rows}


async def _require_host_or_creator(
    session: AsyncSession, ctx: RequestContext, row: Any, what: str
) -> None:
    """ADR-0038 D6: only the current host or the creator may change the host, the follow-up
    link or the agenda. Runs after `_fetch_meeting`, so a meeting the caller cannot see is
    already a 404 and this 403 says nothing about existence."""
    if ctx.user_id is not None:
        if row["created_by"] == ctx.user_id:
            return
        is_host = (
            await session.execute(
                text(
                    "SELECT 1 FROM meeting_participants WHERE meeting_id = :m AND user_id = :u "
                    "AND role_in_meeting = 'host'"
                ),
                {"m": str(row["id"]), "u": str(ctx.user_id)},
            )
        ).first()
        if is_host:
            return
    raise ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.FORBIDDEN_ROLE,
        f"Only the meeting's host or its creator can change {what}",
    )


async def _check_previous_meeting(
    session: AsyncSession, meeting_id: uuid.UUID | None, previous_id: uuid.UUID
) -> None:
    """The follow-up link must name ANOTHER meeting this caller can see in this tenant. 422,
    not 404, per ADR-0038 D5: the request is wrong, and RLS already makes another tenant's
    meeting indistinguishable from a missing one. The FK alone would accept a cross-tenant id
    (gap 31, `visibility.py`), which is why this check runs under the tenant session first."""
    if previous_id == meeting_id:
        raise _field_error(["previous_meeting_id"], "A meeting cannot follow up itself")
    if not await is_visible(session, "meetings", previous_id):
        raise _field_error(["previous_meeting_id"], "No such meeting to follow up")


def _check_owners(items: list[AgendaItemCreate], participants: set[uuid.UUID], prefix: str) -> None:
    for i, item in enumerate(items):
        if item.owner_user_id is not None and item.owner_user_id not in participants:
            raise _field_error(
                [prefix, str(i), "owner_user_id"],
                "An agenda item's owner must be a participant of the meeting",
            )


async def _insert_agenda_item(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    position: int,
    item: AgendaItemCreate,
) -> None:
    await session.execute(
        text(
            "INSERT INTO meeting_agenda_items (org_id, meeting_id, position, title, "
            "owner_user_id, duration_minutes, goal_id) VALUES (:o, :m, :p, :t, :owner, :d, :g)"
        ),
        {
            "o": str(org_id),
            "m": str(meeting_id),
            "p": position,
            "t": item.title,
            "owner": str(item.owner_user_id) if item.owner_user_id else None,
            "d": item.duration_minutes,
            "g": str(item.goal_id) if item.goal_id else None,
        },
    )


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
    session: AsyncSession, meeting_ids: list[uuid.UUID], viewer: uuid.UUID | None
) -> dict[uuid.UUID, list[Participant]]:
    """One query for the whole page, not one per meeting.

    ADR-0039 B5: `attendance` and `proxy_update` go to everyone who can see the meeting (the
    update is meant for the room); `proxy_questions` only to the person themself and to the
    meeting's host, `null` for anyone else."""
    if not meeting_ids:
        return {}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT meeting_id, user_id, role_in_meeting, attendance, proxy_update, "
                    "proxy_questions FROM meeting_participants WHERE meeting_id = ANY(:ids) "
                    # Deterministic: the host first, then by user id. Heap order (what the
                    # list relied on before) is not an ordering Postgres promises.
                    "ORDER BY meeting_id, (role_in_meeting = 'host') DESC, user_id"
                ),
                {"ids": meeting_ids},
            )
        )
        .mappings()
        .all()
    )
    hosts = {r["meeting_id"]: r["user_id"] for r in rows if r["role_in_meeting"] == "host"}
    out: dict[uuid.UUID, list[Participant]] = {}
    for r in rows:
        sees_questions = viewer is not None and viewer in (r["user_id"], hosts.get(r["meeting_id"]))
        out.setdefault(r["meeting_id"], []).append(
            Participant(
                user_id=r["user_id"],
                role_in_meeting=r["role_in_meeting"],
                attendance=r["attendance"],
                proxy_update=r["proxy_update"],
                proxy_questions=list(r["proxy_questions"] or []) if sees_questions else None,
            )
        )
    return out


async def _fetch_meeting(
    session: AsyncSession, meeting_id: uuid.UUID, *, for_update: bool = False
) -> Any:
    """Fetch or 404. No `org_id` predicate: RLS already scopes it, and adding one by hand
    invites a route that forgets it and quietly works anyway on a superuser connection.

    `for_update` row-locks the meeting so two concurrent agenda replaces serialize instead of
    interleaving their deletes and inserts."""
    lock = " FOR UPDATE" if for_update else ""
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_MEETING_COLUMNS} FROM meetings "
                    f"WHERE id = :id AND deleted_at IS NULL{lock}"
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


@router.post("/meetings", status_code=status.HTTP_201_CREATED, response_model=MeetingDetail)
async def create_meeting(
    body: MeetingCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingDetail:
    """Create a meeting, its participants, its agenda, its event and its audit record,
    atomically.

    ADR-0038 D5: exactly one host, `host_user_id` (default: the creator), which must be the
    creator or one of `participant_ids`; the creator is always a participant (`attendee` when
    not the host). Agenda owners must be participants. Every 422 below is raised before the
    first write.
    """
    creator = ctx.user_id
    # De-duplicated, order preserved; the creator is added separately, so listing them is a
    # no-op.
    invitees = [u for u in dict.fromkeys(body.participant_ids) if u != creator]
    host = body.host_user_id or creator
    if body.host_user_id is not None and body.host_user_id != creator:
        if body.host_user_id not in invitees:
            raise _field_error(
                ["host_user_id"], "The host must be the creator or one of participant_ids"
            )
    members = set(invitees) | ({creator} if creator is not None else set())
    _check_owners(list(body.agenda), members, "agenda")
    if body.previous_meeting_id is not None:
        await _check_previous_meeting(session, None, body.previous_meeting_id)
    confidential = bool(body.confidential)
    if body.template_id is not None:
        # ADR-0039 B2: the template's `confidential` is the default. Checked under RLS first,
        # because the FK alone would accept another tenant's template (gap 31).
        template = (
            await session.execute(
                text("SELECT confidential FROM meeting_templates WHERE id = :t"),
                {"t": str(body.template_id)},
            )
        ).first()
        if template is None:
            raise _field_error(["template_id"], "No such meeting type")
        if body.confidential is None:
            confidential = bool(template[0])

    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO meetings (org_id, title, scheduled_start, scheduled_end, "
                    "created_by, previous_meeting_id, confidential, template_id, source) "
                    "VALUES (:org, :title, :start, :end, :by, :prev, :conf, :tpl, :src) "
                    f"RETURNING {_MEETING_COLUMNS}"
                ),
                {
                    "org": str(ctx.org_id),
                    "title": body.title,
                    "start": body.scheduled_start,
                    "end": body.scheduled_end,
                    "by": str(creator) if creator else None,
                    "prev": str(body.previous_meeting_id) if body.previous_meeting_id else None,
                    "conf": confidential,
                    "tpl": str(body.template_id) if body.template_id else None,
                    "src": body.source or "mytheus",
                },
            )
        )
        .mappings()
        .one()
    )
    meeting_id = row["id"]

    if creator is not None:
        # The creator is always in. `meeting-service` mints a join token only for a
        # `meeting_participants` row, so a creator who was not listed could not enter their own
        # meeting. No visibility check: `ctx.user_id` is the verified caller, resolved from this
        # tenant's `users` table already.
        await session.execute(
            text(
                "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
                "VALUES (:m, :u, :org, :r) ON CONFLICT DO NOTHING"
            ),
            {
                "m": str(meeting_id),
                "u": str(creator),
                "org": str(ctx.org_id),
                "r": "host" if host == creator else "attendee",
            },
        )

    for user_id in invitees:
        # The existence check is what keeps a participant inside the tenant. The foreign key
        # does NOT: PostgreSQL runs referential-integrity checks as the referenced table's
        # owner with RLS bypassed, so `fk_meeting_participants_user_id_users` resolves another
        # tenant's `users` row even though this transaction cannot SELECT it, and
        # `meeting_participants`' WITH CHECK passes because the `org_id` written is the
        # caller's own. This comment previously asserted the opposite and was wrong: the route
        # returned 201 and persisted a row referencing another tenant's user. See
        # `test_another_tenants_user_cannot_be_named_as_a_participant`.
        if not await is_visible(session, "users", user_id):
            # 404, not 422: whether a user id exists at all is tenant-confidential, and this
            # route must not become an oracle for user ids in other tenants. Raising rolls the
            # whole transaction back, so the meeting does not survive without its participant.
            raise ProblemDetail(
                status.HTTP_404_NOT_FOUND,
                ErrorCode.NOT_FOUND,
                "No such user in this organization",
            )
        await session.execute(
            text(
                "INSERT INTO meeting_participants (meeting_id, user_id, org_id, role_in_meeting) "
                "VALUES (:m, :u, :org, :r) ON CONFLICT DO NOTHING"
            ),
            {
                "m": str(meeting_id),
                "u": str(user_id),
                "org": str(ctx.org_id),
                "r": "host" if user_id == host else "attendee",
            },
        )

    for position, item in enumerate(body.agenda):
        await _insert_agenda_item(session, ctx.org_id, meeting_id, position, item)

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
    return await _detail(session, row, ctx.user_id)


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
    by_meeting = await _load_participants(session, [r["id"] for r in page], ctx.user_id)
    return MeetingPage(
        items=[_row_to_meeting(r, by_meeting.get(r["id"], [])) for r in page],
        next_cursor=(
            _encode_cursor(page[-1]["created_at"], page[-1]["id"]) if has_more and page else None
        ),
    )


@router.get("/meetings/{meeting_id}", response_model=MeetingDetail)
async def get_meeting(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingDetail:
    """The meeting, the meeting it follows up (`previous_meeting`) and its agenda ordered by
    `position` (ADR-0038 D6). A client re-hydrates the live agenda panel from here."""
    return await _detail(session, await _fetch_meeting(session, meeting_id), ctx.user_id)


@router.post("/meetings/{meeting_id}/participants", response_model=Meeting)
async def add_participants(
    meeting_id: uuid.UUID,
    body: ParticipantsAdd,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Meeting:
    """Invite tenant users into an existing meeting. Only someone already in it may invite.

    Existence before participation, as in `meeting-service`'s token route: a meeting in another
    tenant is 404 (RLS hides it); a visible meeting the caller is not part of is 403. Each
    invitee gets the same `is_visible` check `create_meeting` applies, for the same reason: the
    foreign key alone would accept another tenant's user. Already-present users are a no-op, so
    a repeated invite is harmless.
    """
    await _fetch_meeting(session, meeting_id)
    if (
        ctx.user_id is None
        or not (
            await session.execute(
                text("SELECT 1 FROM meeting_participants WHERE meeting_id = :m AND user_id = :u"),
                {"m": str(meeting_id), "u": str(ctx.user_id)},
            )
        ).first()
    ):
        raise ProblemDetail(
            status.HTTP_403_FORBIDDEN,
            ErrorCode.FORBIDDEN_ROLE,
            "Only someone in this meeting can invite others to it",
        )

    added: list[str] = []
    for user_id in dict.fromkeys(body.user_ids):
        if not await is_visible(session, "users", user_id):
            raise ProblemDetail(
                status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such user in this organization"
            )
        inserted = (
            await session.execute(
                text(
                    "INSERT INTO meeting_participants (meeting_id, user_id, org_id, "
                    "role_in_meeting) VALUES (:m, :u, :org, 'attendee') "
                    "ON CONFLICT DO NOTHING RETURNING user_id"
                ),
                {"m": str(meeting_id), "u": str(user_id), "org": str(ctx.org_id)},
            )
        ).first()
        if inserted:
            added.append(str(user_id))

    if added:
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_MEETING_PARTICIPANTS_ADDED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={"user_ids": added},
            ip=ctx.ip,
        )

    row = await _fetch_meeting(session, meeting_id)
    participants = await _load_participants(session, [meeting_id], ctx.user_id)
    return _row_to_meeting(row, participants.get(meeting_id, []))


@router.get(
    "/meetings/{meeting_id}/participants/{clerk_user_id}",
    status_code=status.HTTP_200_OK,
    response_model=ParticipantCheck,
)
async def check_participant(
    meeting_id: uuid.UUID,
    clerk_user_id: str,
    ctx: Annotated[RequestContext, Depends(tenant_context)],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ParticipantCheck:
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

    **Tenancy source, spelled out because `realtime-gateway` also sends an `X-Org-Id` header
    on this call:** that header is NOT read here - it isn't even declared as a parameter. The
    ONLY source of `org_id` for this route is `tenant_session` -> `tenant_context` ->
    `verify_token`, i.e. the caller's OWN re-verified bearer token, exactly like every other
    route in this file. Both the meeting-existence check below and the `meeting_participants
    JOIN users` query are RLS-scoped by that resolved `org_id` with no hand-written predicate,
    so a request naming an org-B `clerk_user_id` while `app.current_org` is org A finds
    nothing - org B's `users` row is invisible under RLS - rather than "found, but not a
    participant of THIS meeting" from a lookup that forgot to scope by tenant. See
    `tests/test_participant_check_api.py::test_the_participant_join_result_depends_on_users_
    rls_not_just_on_row_existence` for the direct proof (the JOIN layer, isolated). `X-Org-Id`
    is transport-layer context the gateway sets from its own verified JWT for its own
    bookkeeping; it is never the authorization boundary on this side of the call, matching
    `CLAUDE.md`'s "the service tier RE-VERIFIES the JWT (the gateway is a DDoS shield, not a
    trust delegation)".

    **`org_id` in the response (`ParticipantCheck`, not `Participant` - see that schema's
    docstring for why a separate model):** `realtime-gateway`'s fixed `presence-events`
    producer needs our internal `org_id` and has no database to resolve its own JWT's opaque
    Clerk org id into one; this route already resolves it (as `ctx.org_id`, above) for its own
    RLS scoping, so returning it is serializing an already-computed value, not a new query or
    lookup. Safe to return to THIS caller specifically because `ctx.org_id` is, by
    construction, always the caller's own tenant - it comes from re-verifying THEIR OWN bearer
    token, never from a lookup on the target meeting or user - and every 200 response is
    additionally consistent with that value: `_fetch_meeting`'s and this query's RLS scoping
    guarantee that the meeting and the resolved participant/user rows all carry
    `org_id = ctx.org_id`, or the route would already have 404'd/403'd before reaching here.
    There is no code path that reaches `return ParticipantCheck(...)` with an `org_id` that is
    not the caller's own.
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
    return ParticipantCheck(
        user_id=row["user_id"], role_in_meeting=row["role_in_meeting"], org_id=ctx.org_id
    )


@router.patch("/meetings/{meeting_id}", response_model=MeetingDetail)
async def update_meeting(
    meeting_id: uuid.UUID,
    body: MeetingUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingDetail:
    current = await _fetch_meeting(session, meeting_id)  # 404 before anything is written

    fields = body.model_dump(exclude_unset=True)
    new_host: uuid.UUID | None = fields.pop("host_user_id", None)
    if new_host is not None or "previous_meeting_id" in fields:
        # ADR-0038 D6: the host and the follow-up link are the host's or the creator's call.
        await _require_host_or_creator(session, ctx, current, "the host or the follow-up link")
    if fields.get("previous_meeting_id") is not None:
        await _check_previous_meeting(session, meeting_id, fields["previous_meeting_id"])
    if new_host is not None:
        if new_host not in await _participant_ids(session, meeting_id):
            raise _field_error(["host_user_id"], "The new host must be a participant")
        # Exactly one host: demote whoever holds it, then promote. Two statements in this
        # transaction, so no reader ever sees zero or two hosts.
        await session.execute(
            text(
                "UPDATE meeting_participants SET role_in_meeting = 'attendee' "
                "WHERE meeting_id = :m AND role_in_meeting = 'host' AND user_id <> :h"
            ),
            {"m": str(meeting_id), "h": str(new_host)},
        )
        await session.execute(
            text(
                "UPDATE meeting_participants SET role_in_meeting = 'host' "
                "WHERE meeting_id = :m AND user_id = :h"
            ),
            {"m": str(meeting_id), "h": str(new_host)},
        )

    assignments = "".join(f"{name} = :{name}, " for name in fields)
    params: dict[str, Any] = {
        k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in fields.items()
    }
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE meetings SET {assignments}updated_at = now() "
                    f"WHERE id = :id RETURNING {_MEETING_COLUMNS}"
                ),
                {**params, "id": str(meeting_id)},
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
    if fields.get("status") in _CLOSED:
        # ADR-0039 B7: a status request nobody answered before the meeting is over expires.
        await expire_status_requests(session, meeting_id)

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
        metadata={"changed": sorted(body.model_fields_set)},
        ip=ctx.ip,
    )

    return await _detail(session, row, ctx.user_id)


async def expire_status_requests(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    """ADR-0039 B7: once a meeting is over, its pending status requests are `expired`."""
    await session.execute(
        text(
            "UPDATE action_status_requests SET state = 'expired' "
            "WHERE meeting_id = :m AND state = 'pending'"
        ),
        {"m": str(meeting_id)},
    )


# --- ADR-0038 D6: agenda and carry-forward -------------------------------------------------


@router.put("/meetings/{meeting_id}/agenda", response_model=list[AgendaItem])
async def replace_agenda(
    meeting_id: uuid.UUID,
    body: AgendaReplace,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[AgendaItem]:
    """Replace the agenda, host or creator only, and not once the meeting has ended (409).

    Items sent with the `id` of an existing item of this meeting keep it, and its `status`,
    `status_source` and `agent_note`; items without an `id` are new and `pending`; items left
    out are deleted. Order is the order sent (`position` 0..n-1). The meeting row is locked for
    the replace, so two concurrent PUTs apply one after the other, never interleaved.
    """
    row = await _fetch_meeting(session, meeting_id, for_update=True)
    await _require_host_or_creator(session, ctx, row, "the agenda")
    if row["status"] == "ended":
        raise ProblemDetail(
            status.HTTP_409_CONFLICT,
            ErrorCode.VERSION_CONFLICT,
            "The meeting has ended; its agenda can no longer change",
        )

    existing = {
        r[0]
        for r in await session.execute(
            text("SELECT id FROM meeting_agenda_items WHERE meeting_id = :m"),
            {"m": str(meeting_id)},
        )
    }
    keep: list[uuid.UUID] = []
    for i, item in enumerate(body.items):
        if item.id is None:
            continue
        if item.id not in existing:
            raise _field_error(["items", str(i), "id"], "No such agenda item in this meeting")
        if item.id in keep:
            raise _field_error(["items", str(i), "id"], "An agenda item is listed twice")
        keep.append(item.id)
    _check_owners(list(body.items), await _participant_ids(session, meeting_id), "items")

    await session.execute(
        text("DELETE FROM meeting_agenda_items WHERE meeting_id = :m AND NOT (id = ANY(:keep))"),
        {"m": str(meeting_id), "keep": keep},
    )
    for position, item in enumerate(body.items):
        if item.id is None:
            await _insert_agenda_item(session, ctx.org_id, meeting_id, position, item)
            continue
        await session.execute(
            text(
                "UPDATE meeting_agenda_items SET position = :p, title = :t, "
                "owner_user_id = :owner, duration_minutes = :d, goal_id = :g, "
                "updated_at = now() WHERE id = :id AND meeting_id = :m"
            ),
            {
                "p": position,
                "t": item.title,
                "owner": str(item.owner_user_id) if item.owner_user_id else None,
                "d": item.duration_minutes,
                "g": str(item.goal_id) if item.goal_id else None,
                "id": str(item.id),
                "m": str(meeting_id),
            },
        )

    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"changed": ["agenda"], "items": len(body.items)},
        ip=ctx.ip,
    )
    return await _load_agenda(session, meeting_id)


@router.patch("/meetings/{meeting_id}/agenda/{item_id}", response_model=AgendaItem)
async def set_agenda_item_status(
    meeting_id: uuid.UUID,
    item_id: uuid.UUID,
    body: AgendaItemStatusUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AgendaItem:
    """The host marks an item (host or creator). Sets `status_source = 'host'`: from then on
    the MeetingAgent's updates to this item are ignored (the host wins, ADR-0038 D7)."""
    row = await _fetch_meeting(session, meeting_id)
    await _require_host_or_creator(session, ctx, row, "the agenda")
    item = (
        (
            await session.execute(
                text(
                    "UPDATE meeting_agenda_items SET status = :s, status_source = 'host', "
                    "updated_at = now() WHERE id = :id AND meeting_id = :m "
                    f"RETURNING {_AGENDA_COLUMNS}"
                ),
                {"s": body.status, "id": str(item_id), "m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    if item is None:
        raise not_found()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={
            "changed": ["agenda_item_status"],
            "item_id": str(item_id),
            "status": body.status,
        },
        ip=ctx.ip,
    )
    return AgendaItem(**dict(item))


@router.get("/meetings/{meeting_id}/carry-forward", response_model=CarryForward)
async def carry_forward(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CarryForward:
    """What the previous meeting left open (ADR-0038 D6): its confirmed decisions, its open
    action items (confirmed, not done; proposed candidates and dismissed ones are not
    commitments), and the CALLER's own suggestions saved from it. Empty when there is no link
    or the linked meeting is gone.

    ADR-0039: each open action carries `latest_status` (its assignee's most recent answer to a
    status request, or null). A confidential previous meeting is never a source for a caller
    who was not in it (B2): they get the same empty answer as for no link."""
    row = await _fetch_meeting(session, meeting_id)
    previous = await _meeting_ref(session, row["previous_meeting_id"])
    if previous is not None and not await may_see_source(session, previous.id, ctx.user_id):
        previous = None
    if previous is None:
        return CarryForward(
            previous_meeting=None, decisions=[], open_actions=[], saved_questions=[]
        )
    prev = {"m": str(previous.id)}
    decisions = await session.execute(
        text(
            "SELECT id, content FROM decisions WHERE meeting_id = :m AND status = 'confirmed' "
            "AND deleted_at IS NULL ORDER BY created_at, id"
        ),
        prev,
    )
    actions = await session.execute(
        text(
            "SELECT id, content, assignee_id, due_date FROM action_items WHERE meeting_id = :m "
            "AND status = 'confirmed' AND deleted_at IS NULL ORDER BY created_at, id"
        ),
        prev,
    )
    action_rows = list(actions)
    statuses = await latest_statuses(session, [r[0] for r in action_rows])
    saved: list[CarryForwardQuestion] = []
    if ctx.user_id is not None:
        saved = [
            CarryForwardQuestion(id=r[0], question=r[1])
            for r in await session.execute(
                text(
                    "SELECT id, question FROM agent_suggestions WHERE meeting_id = :m "
                    "AND user_id = :u AND status = 'saved' AND deleted_at IS NULL "
                    "ORDER BY created_at, id"
                ),
                {**prev, "u": str(ctx.user_id)},
            )
        ]
    return CarryForward(
        previous_meeting=previous,
        decisions=[CarryForwardDecision(id=r[0], content=r[1]) for r in decisions],
        open_actions=[
            CarryForwardAction(
                id=r[0],
                title=r[1],
                owner_user_id=r[2],
                due_date=r[3],
                latest_status=statuses.get(r[0]),
            )
            for r in action_rows
        ],
        saved_questions=saved,
    )


# --- ADR-0039 B4: drafted agenda ----------------------------------------------------------------


def _status_title(content: str) -> str:
    title = "Status: " + " ".join(content.split())
    return title if len(title) <= _MAX_TITLE else title[: _MAX_TITLE - 3].rstrip() + "..."


@router.post("/meetings/agenda-draft", response_model=AgendaDraft)
async def draft_agenda(
    body: AgendaDraftRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> AgendaDraft:
    """A drafted agenda for a new meeting (ADR-0039 B4). Saves nothing; deterministic.

    In order, at most 20: the template's items (`template`); the previous meeting's agenda items
    not `covered`, by position (`unfinished`); its open confirmed action items, oldest first, as
    `Status: {content}` owned by the assignee (`open_action`). Both ids must name rows of this
    tenant (422 naming the field). A confidential previous meeting contributes nothing for a
    caller who was not in it (B2)."""
    items: list[AgendaDraftItem] = []
    if body.template_id is not None:
        agenda = (
            await session.execute(
                text("SELECT agenda FROM meeting_templates WHERE id = :t"),
                {"t": str(body.template_id)},
            )
        ).first()
        if agenda is None:
            raise _field_error(["template_id"], "No such meeting type")
        for entry in agenda[0] or []:
            if isinstance(entry, dict) and isinstance(entry.get("title"), str):
                duration = entry.get("duration_minutes")
                items.append(
                    AgendaDraftItem(
                        title=entry["title"],
                        owner_user_id=None,
                        duration_minutes=duration if isinstance(duration, int) else None,
                        goal_id=None,
                        source="template",
                    )
                )
    previous = body.previous_meeting_id
    if previous is not None:
        if not await is_visible(session, "meetings", previous):
            raise _field_error(["previous_meeting_id"], "No such meeting to follow up")
        if await may_see_source(session, previous, ctx.user_id):
            unfinished = await session.execute(
                text(
                    "SELECT title, owner_user_id, duration_minutes, goal_id "
                    "FROM meeting_agenda_items WHERE meeting_id = :m AND status <> 'covered' "
                    "ORDER BY position, id"
                ),
                {"m": str(previous)},
            )
            items.extend(
                AgendaDraftItem(
                    title=r[0],
                    owner_user_id=r[1],
                    duration_minutes=r[2],
                    goal_id=r[3],
                    source="unfinished",
                )
                for r in unfinished
            )
            actions = await session.execute(
                text(
                    "SELECT content, assignee_id FROM action_items WHERE meeting_id = :m "
                    "AND status = 'confirmed' AND deleted_at IS NULL ORDER BY created_at, id"
                ),
                {"m": str(previous)},
            )
            items.extend(
                AgendaDraftItem(
                    title=_status_title(r[0]),
                    owner_user_id=r[1],
                    duration_minutes=None,
                    goal_id=None,
                    source="open_action",
                )
                for r in actions
            )
    return AgendaDraft(items=items[:MAX_AGENDA_ITEMS])


# --- ADR-0039 B5: send your agent instead ---------------------------------------------------


@router.put("/meetings/{meeting_id}/attendance", response_model=Participant)
async def set_attendance(
    meeting_id: uuid.UUID,
    body: AttendanceUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Participant:
    """The caller's own attendance: `live`, or `agent` with an update for the room and up to
    five questions. Self only (the caller's own participant row; there is no user id to name),
    a participant only (403), and not once the meeting is over (409)."""
    row = await _fetch_meeting(session, meeting_id)
    if ctx.user_id is None:
        raise _not_a_participant()
    if row["status"] in _CLOSED:
        raise ProblemDetail(
            status.HTTP_409_CONFLICT,
            ErrorCode.VERSION_CONFLICT,
            "The meeting is over; attendance can no longer change",
        )
    updated = (
        (
            await session.execute(
                text(
                    "UPDATE meeting_participants SET attendance = :a, proxy_update = :u, "
                    "proxy_questions = CAST(:q AS text[]) "
                    "WHERE meeting_id = :m AND user_id = :me "
                    "RETURNING user_id, role_in_meeting, attendance, proxy_update, proxy_questions"
                ),
                {
                    "a": body.attendance,
                    "u": body.proxy_update,
                    "q": body.proxy_questions,
                    "m": str(meeting_id),
                    "me": str(ctx.user_id),
                },
            )
        )
        .mappings()
        .first()
    )
    if updated is None:
        raise _not_a_participant()
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_MEETING_UPDATED,
        target_type="meeting",
        target_id=meeting_id,
        metadata={"changed": ["attendance"], "attendance": body.attendance},
        ip=ctx.ip,
    )
    return Participant(
        user_id=updated["user_id"],
        role_in_meeting=updated["role_in_meeting"],
        attendance=updated["attendance"],
        proxy_update=updated["proxy_update"],
        proxy_questions=list(updated["proxy_questions"] or []),
    )
