"""The promise ledger (`/api/v1/promises`): every promise a person made, from every channel, with
the capacity check at the moment of commitment.

A promise is private to the person who made it (`commitments.user_id`), like a note. Rows come
from four places: meetings and phone recordings (core-api's `agent-results` consumer, as
before), Slack and email (integration-service hands what the person's agent extracted to
`internal_promises.py`; those land `pending` until the person confirms or dismisses them), and
the person themselves (`POST`, here and through the MCP server). The older
`/api/v1/commitments` keeps serving meeting promises in its published shape.

Routes (all owner-only; anyone else's promise is a 404):

- `GET /promises?status=&channel=&recipient=` the ledger, newest-due first is the UI's job.
- `POST /promises` a manual add (`channel: manual`, confirmed), answered with the capacity check.
- `PATCH /promises/{id}` kept (`done`), dropped (`dismissed`), back to `open` (undo), or an edit
  of the text, the date or who it was promised to.
- `POST /promises/{id}/confirm` a `pending` (extracted or handed-off) promise becomes the
  person's own, answered with the capacity check for its date.
- `POST /promises/{id}/hand-off` gives the promise to a colleague: the original is closed
  (`dismissed`, `handed_off_to`) and the colleague gets a `pending` copy they confirm or
  dismiss, with an in-app notification. `POST /promises/{id}/hand-off/undo` takes it back while
  the colleague has not confirmed it.
- `GET /promises/capacity` the person's days (default this week), `GET /promises/capacity/check`
  what one more thing due on a date does to that day ("That's your seventh promise due Friday.
  Offer Monday instead?").
- `POST /promises/date-proposals` renegotiate a promise's or an action's date. When someone else
  asked for it (the promise's recipient, the action's assigner), they are asked and decide
  (`accept` / `decline`); otherwise the new date applies at once. `withdraw` is the proposer's
  undo. `GET /promises/date-proposals?role=incoming|outgoing`.
- `GET|PUT /promises/capture-preferences` may the agent look in my Slack messages and email.
"""

from __future__ import annotations

import uuid
from datetime import date, timedelta
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api import capacity as cap
from services.core_api.promise_dates import day_label, short_date
from services.core_api.promise_models import (
    Capacity,
    CapacityCheck,
    CapacityDay,
    CapturePreferencesUpdate,
    CaptureSettings,
    DateProposal,
    DateProposalCreate,
    DateProposalList,
    HandOff,
    HandOffResult,
    LedgerPromise,
    PromiseChannel,
    PromiseConfirm,
    PromiseCreate,
    PromiseLedger,
    PromiseSource,
    PromiseStatus,
    PromiseUpdate,
    PromiseWithCapacity,
    ProposalBrief,
    RenegotiateResult,
)
from services.core_api.records import display_name

router = APIRouter(prefix="/api/v1/promises", tags=["promises"])

MAX_PROMISES = 500
PRODUCER = "core-api"
EMAIL_CONNECTED = False  # no mail-reading integration exists yet (see internal_promises.py)

_SELECT = """
SELECT c.id, c.user_id, c.content, c.status, c.review_state, c.channel, c.due_at, c.due_hint,
       c.recipient_user_id, ru.email AS recipient_email, c.recipient_label,
       c.meeting_id, m.title AS meeting_title, m.scheduled_start AS meeting_start,
       c.moment_id, c.action_item_id, c.confidence,
       c.handed_off_to, hu.email AS handed_off_to_email, c.handed_off_from,
       c.created_by, cu.email AS created_by_email,
       c.source_link, c.source_quote, c.source_at,
       c.created_at, c.updated_at, c.done_at,
       p.id AS p_id, p.proposed_due AS p_due, p.state AS p_state, pu.email AS p_requester_email
FROM commitments c
LEFT JOIN meetings m ON m.id = c.meeting_id
LEFT JOIN users ru ON ru.id = c.recipient_user_id
LEFT JOIN users hu ON hu.id = c.handed_off_to
LEFT JOIN users cu ON cu.id = c.created_by
LEFT JOIN LATERAL (
  SELECT d.id, d.proposed_due, d.state, d.requester_id FROM promise_date_proposals d
  WHERE d.commitment_id = c.id AND d.state = 'pending' ORDER BY d.created_at DESC LIMIT 1
) p ON true
LEFT JOIN users pu ON pu.id = p.requester_id
"""
_VISIBLE = "c.user_id = :me AND (c.meeting_id IS NULL OR m.deleted_at IS NULL)"


# --- helpers ---------------------------------------------------------------------------------


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


def _require_write(ctx: RequestContext) -> uuid.UUID:
    if not ctx.can_write:
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "This token is read-only",
            extra={"reason": "read_only_token"},
        )
    return _me(ctx)


def _source(row: dict[str, Any], me: uuid.UUID) -> PromiseSource:
    channel: PromiseChannel = row["channel"]
    link = row["source_link"]
    if channel in ("meeting", "in_person"):
        return PromiseSource(
            channel=channel,
            label=(row["meeting_title"] or "").strip() or "An untitled meeting",
            link=f"/meetings/{row['meeting_id']}",
            external=False,
            quote=row["source_quote"],
            at=row["source_at"] or row["meeting_start"] or row["created_at"],
        )
    if channel == "slack":
        label = "Slack"
    elif channel == "email":
        label = "Email"
    elif row["handed_off_from"] is not None:
        label = f"Handed off by {display_name(row['created_by_email']) or 'a teammate'}"
    elif row["created_by"] is None or row["created_by"] == me:
        label = "Added by you"
    else:
        label = f"Added by {display_name(row['created_by_email']) or 'a teammate'}"
    return PromiseSource(
        channel=channel,
        label=label,
        link=link,
        external=bool(link and str(link).startswith(("https://", "http://"))),
        quote=row["source_quote"],
        at=row["source_at"] or row["created_at"],
    )


def ledger_item(row: dict[str, Any], me: uuid.UUID) -> LedgerPromise:
    recipient = (
        display_name(row["recipient_email"])
        if row["recipient_user_id"]
        else (row["recipient_label"] or None)
    )
    proposal = (
        ProposalBrief(
            id=row["p_id"],
            proposed_due=row["p_due"],
            state=row["p_state"],
            requester_name=display_name(row["p_requester_email"]),
        )
        if row["p_id"]
        else None
    )
    return LedgerPromise(
        id=row["id"],
        content=row["content"],
        status=row["status"],
        review_state=row["review_state"],
        channel=row["channel"],
        due_at=row["due_at"],
        due_hint=row["due_hint"],
        recipient_user_id=row["recipient_user_id"],
        recipient_name=recipient,
        source=_source(row, me),
        meeting_id=row["meeting_id"],
        moment_id=row["moment_id"],
        action_item_id=row["action_item_id"],
        confidence=row["confidence"],
        handed_off_to=row["handed_off_to"],
        handed_off_to_name=display_name(row["handed_off_to_email"]),
        handed_off_from=row["handed_off_from"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        done_at=row["done_at"],
        pending_proposal=proposal,
    )


async def _load(session: AsyncSession, me: uuid.UUID, promise_id: uuid.UUID) -> LedgerPromise:
    row = (
        (
            await session.execute(
                text(f"{_SELECT} WHERE c.id = :id AND {_VISIBLE}"),
                {"id": str(promise_id), "me": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return ledger_item(dict(row), me)


async def _locked(session: AsyncSession, me: uuid.UUID, promise_id: uuid.UUID) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                text(
                    "SELECT c.* FROM commitments c LEFT JOIN meetings m ON m.id = c.meeting_id "
                    f"WHERE c.id = :id AND {_VISIBLE} FOR UPDATE OF c"
                ),
                {"id": str(promise_id), "me": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return dict(row)


async def _live_colleague(session: AsyncSession, user_id: uuid.UUID) -> str:
    """The colleague's email (for their name); 404 when not a live user of this workspace."""
    email = (
        await session.execute(
            text("SELECT email FROM users WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(user_id)},
        )
    ).scalar()
    if email is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "No such user in this organization")
    return str(email)


async def _my_name(session: AsyncSession, me: uuid.UUID) -> str:
    email = (
        await session.execute(text("SELECT email FROM users WHERE id = :id"), {"id": str(me)})
    ).scalar()
    return display_name(str(email) if email else None) or "A teammate"


async def notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    user_id: uuid.UUID,
    source: str,
    title: str,
    body: str,
    link: str,
    priority: int = 3,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": title[:200],
                "body": body[:2000],
                "link": link,
            },
        ),
    )


def _short(value: str, limit: int = 120) -> str:
    text_ = " ".join(value.split())
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


def _check_model(result: cap.Check) -> CapacityCheck:
    return CapacityCheck(
        due_date=result.due,
        due_label=result.due_label,
        level=result.level,
        items_on_day=result.items_on_day,
        meeting_minutes=result.meeting_minutes,
        warn=result.warn,
        message=result.message,
        suggested_date=result.suggested,
        suggested_label=result.suggested_label,
    )


async def capture_settings(session: AsyncSession, me: uuid.UUID) -> CaptureSettings:
    row = (
        await session.execute(
            text(
                "SELECT slack_enabled, email_enabled FROM promise_capture_preferences "
                "WHERE user_id = :u"
            ),
            {"u": str(me)},
        )
    ).first()
    return CaptureSettings(
        slack_enabled=bool(row[0]) if row else True,
        email_enabled=bool(row[1]) if row else True,
        email_connected=EMAIL_CONNECTED,
    )


# --- the ledger ------------------------------------------------------------------------------


@router.get("", response_model=PromiseLedger, summary="My promise ledger")
async def list_promises(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_filter: Annotated[
        Literal["open", "done", "dismissed", "all"], Query(alias="status")
    ] = "all",
    channel: Annotated[list[PromiseChannel] | None, Query()] = None,
    recipient: Annotated[
        str | None,
        Query(max_length=200, description="A colleague's user id, or `none` for no recipient."),
    ] = None,
) -> PromiseLedger:
    """Every promise you made, from every channel (meetings, phone recordings, Slack, email,
    added by hand), with its source and any date you asked to move. Only you see it."""
    if ctx.user_id is None:
        return PromiseLedger(
            items=[],
            pending_review=0,
            capture=CaptureSettings(
                slack_enabled=True, email_enabled=True, email_connected=EMAIL_CONNECTED
            ),
        )
    me = ctx.user_id
    where = [_VISIBLE]
    params: dict[str, Any] = {"me": str(me), "lim": MAX_PROMISES}
    if status_filter != "all":
        where.append("c.status = :st")
        params["st"] = status_filter
    if channel:
        where.append("c.channel = ANY(:ch)")
        params["ch"] = list(channel)
    if recipient:
        if recipient == "none":
            where.append("c.recipient_user_id IS NULL AND c.recipient_label IS NULL")
        else:
            try:
                params["rc"] = str(uuid.UUID(recipient))
            except ValueError:
                raise ProblemDetail(
                    422, ErrorCode.VALIDATION_FAILED, "recipient must be a user id or none"
                ) from None
            where.append("c.recipient_user_id = :rc")
    rows = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY (c.status = 'open') DESC, c.due_at NULLS LAST, c.created_at DESC "
                    "LIMIT :lim"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    items = [ledger_item(dict(r), me) for r in rows]
    pending = (
        await session.execute(
            text(
                "SELECT count(*) FROM commitments WHERE user_id = :me AND status = 'open' "
                "AND review_state = 'pending'"
            ),
            {"me": str(me)},
        )
    ).scalar()
    return PromiseLedger(
        items=items,
        pending_review=int(pending or 0),
        capture=await capture_settings(session, me),
    )


@router.post(
    "",
    response_model=PromiseWithCapacity,
    status_code=status.HTTP_201_CREATED,
    summary="Add a promise",
)
async def create_promise(
    body: PromiseCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    tz: Annotated[str | None, Query(max_length=64)] = None,
) -> PromiseWithCapacity:
    """Write down a promise you made anywhere (a call, a hallway, a message). It is yours at
    once (you said so); the answer carries the capacity check for its date. Honours
    `Idempotency-Key`. Needs a session or a `read_write` token."""
    me = _require_write(ctx)
    if body.recipient_user_id is not None:
        if body.recipient_user_id == me:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "A promise to yourself needs no recipient"
            )
        await _live_colleague(session, body.recipient_user_id)
    new_id = uuid.uuid4()
    zone = cap.resolve_tz(tz)
    check = (
        await cap.check(session, me, body.due_at, zone, exclude=("commitment", new_id))
        if body.due_at
        else None
    )
    await session.execute(
        text(
            "INSERT INTO commitments (id, org_id, user_id, content, due_at, status, review_state, "
            "channel, source_window_id, recipient_user_id, recipient_label, source_link, "
            "created_by, source_at) VALUES (:id, :o, :u, :c, :d, 'open', 'confirmed', 'manual', "
            ":w, :ru, :rl, :sl, :u, now())"
        ),
        {
            "id": str(new_id),
            "o": str(ctx.org_id),
            "u": str(me),
            "c": body.content,
            "d": body.due_at,
            "w": f"manual:{new_id}",
            "ru": str(body.recipient_user_id) if body.recipient_user_id else None,
            "rl": None if body.recipient_user_id else body.recipient_label,
            "sl": body.source_link,
        },
    )
    return PromiseWithCapacity(
        promise=await _load(session, me, new_id),
        capacity=_check_model(check) if check else None,
    )


@router.patch("/{promise_id}", response_model=LedgerPromise, summary="Update a promise")
async def update_promise(
    promise_id: uuid.UUID,
    body: PromiseUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> LedgerPromise:
    """Kept (`done`), dropped (`dismissed`), back to `open` (the undo of either), or an edit.
    Owner only. A promise you handed off is reopened through the hand-off undo."""
    me = _require_write(ctx)
    row = await _locked(session, me, promise_id)
    sets: list[str] = ["updated_at = now()"]
    params: dict[str, Any] = {"id": str(promise_id)}
    next_status: PromiseStatus | None = body.status
    if next_status is not None and next_status != row["status"]:
        if row["handed_off_to"] is not None and next_status == "open":
            raise ProblemDetail(
                409,
                ErrorCode.VERSION_CONFLICT,
                "This promise was handed off; take it back with the hand-off undo",
                extra={"reason": "handed_off"},
            )
        sets.append("status = :st")
        sets.append("done_at = CASE WHEN CAST(:st AS text) = 'done' THEN now() ELSE NULL END")
        params["st"] = next_status
    if body.content is not None:
        sets.append("content = :c")
        params["c"] = body.content
    if body.clear_due:
        sets.append("due_at = NULL")
    elif body.due_at is not None:
        sets.append("due_at = :d")
        params["d"] = body.due_at
    if body.recipient_user_id is not None:
        if body.recipient_user_id == me:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "A promise to yourself needs no recipient"
            )
        await _live_colleague(session, body.recipient_user_id)
        sets.append("recipient_user_id = :ru, recipient_label = NULL")
        params["ru"] = str(body.recipient_user_id)
    elif body.recipient_label is not None:
        label = " ".join(body.recipient_label.split()) or None
        sets.append("recipient_label = :rl, recipient_user_id = NULL")
        params["rl"] = label
    await session.execute(text(f"UPDATE commitments SET {', '.join(sets)} WHERE id = :id"), params)
    if next_status is not None and next_status != "open":
        # Closing a promise ends any date you were still asking about.
        await session.execute(
            text(
                "UPDATE promise_date_proposals SET state = 'withdrawn', decided_at = now() "
                "WHERE commitment_id = :id AND state = 'pending'"
            ),
            {"id": str(promise_id)},
        )
    return await _load(session, me, promise_id)


@router.post(
    "/{promise_id}/confirm", response_model=PromiseWithCapacity, summary="Confirm a promise"
)
async def confirm_promise(
    promise_id: uuid.UUID,
    body: PromiseConfirm,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    tz: Annotated[str | None, Query(max_length=64)] = None,
) -> PromiseWithCapacity:
    """A promise your agent found (or a colleague handed you) becomes yours. Idempotent."""
    me = _require_write(ctx)
    row = await _locked(session, me, promise_id)
    if row["status"] != "open":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"A {row['status']} promise cannot be confirmed"
        )
    due = body.due_at or row["due_at"]
    check = (
        await cap.check(session, me, due, cap.resolve_tz(tz), exclude=("commitment", promise_id))
        if due
        else None
    )
    await session.execute(
        text(
            "UPDATE commitments SET review_state = 'confirmed', due_at = :d, updated_at = now() "
            "WHERE id = :id"
        ),
        {"id": str(promise_id), "d": due},
    )
    return PromiseWithCapacity(
        promise=await _load(session, me, promise_id),
        capacity=_check_model(check) if check else None,
    )


@router.post("/{promise_id}/hand-off", response_model=HandOffResult, summary="Hand off a promise")
async def hand_off(
    promise_id: uuid.UUID,
    body: HandOff,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> HandOffResult:
    """Give an open promise to a colleague. They get it to confirm or dismiss (agents propose,
    people decide); yours closes as handed off. Undo while they have not confirmed it."""
    me = _require_write(ctx)
    if body.to_user_id == me:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Hand it to someone else")
    row = await _locked(session, me, promise_id)
    if row["status"] != "open" or row["review_state"] != "confirmed":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "Only an open, confirmed promise can move"
        )
    email = await _live_colleague(session, body.to_user_id)
    copy_id = uuid.uuid4()
    external = row["channel"] in ("slack", "email")
    quote = row["source_quote"]
    note = " ".join((body.note or "").split()) or None
    await session.execute(
        text(
            "INSERT INTO commitments (id, org_id, user_id, content, due_hint, due_at, status, "
            "review_state, channel, source_window_id, recipient_user_id, recipient_label, "
            "source_link, source_quote, source_at, created_by, handed_off_from) VALUES "
            "(:id, :o, :u, :c, :dh, :d, 'open', 'pending', 'manual', :w, :ru, :rl, :sl, :q, "
            "now(), :me, :orig)"
        ),
        {
            "id": str(copy_id),
            "o": str(ctx.org_id),
            "u": str(body.to_user_id),
            "c": row["content"],
            "dh": row["due_hint"],
            "d": row["due_at"],
            "w": f"handoff:{promise_id}:{copy_id}",
            # The recipient stays who it was promised to, unless that is the new owner.
            "ru": (
                str(row["recipient_user_id"])
                if row["recipient_user_id"] and row["recipient_user_id"] != body.to_user_id
                else None
            ),
            "rl": row["recipient_label"],
            "sl": row["source_link"] if external else None,
            "q": note or quote,
            "me": str(me),
            "orig": str(promise_id),
        },
    )
    await session.execute(
        text(
            "UPDATE commitments SET status = 'dismissed', handed_off_to = :to, done_at = NULL, "
            "updated_at = now() WHERE id = :id"
        ),
        {"id": str(promise_id), "to": str(body.to_user_id)},
    )
    await session.execute(
        text(
            "UPDATE promise_date_proposals SET state = 'withdrawn', decided_at = now() "
            "WHERE commitment_id = :id AND state = 'pending'"
        ),
        {"id": str(promise_id)},
    )
    giver = await _my_name(session, me)
    due = f" (due {short_date(row['due_at'])})" if row["due_at"] else ""
    await notify(
        session,
        ctx.org_id,
        user_id=body.to_user_id,
        source=f"promise-handoff:{copy_id}",
        title=f"{giver} handed you a promise: {_short(row['content'], 100)}",
        body=(f"{row['content']}{due}" + (f"\n\n{giver}: {note}" if note else ""))
        + "\n\nConfirm it to make it yours, or dismiss it.",
        link=f"/work/promises?focus={copy_id}",
        priority=2,
    )
    return HandOffResult(
        promise=await _load(session, me, promise_id),
        handed_to_name=display_name(email),
        copy_id=copy_id,
    )


@router.post(
    "/{promise_id}/hand-off/undo", response_model=LedgerPromise, summary="Take a promise back"
)
async def undo_hand_off(
    promise_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> LedgerPromise:
    """Take back a promise you handed off, while the colleague has not confirmed it yet."""
    me = _require_write(ctx)
    row = await _locked(session, me, promise_id)
    if row["handed_off_to"] is None:
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "This promise was not handed off")
    removed = (
        await session.execute(
            text(
                "DELETE FROM commitments WHERE handed_off_from = :id AND review_state = 'pending' "
                "AND status = 'open' RETURNING id"
            ),
            {"id": str(promise_id)},
        )
    ).first()
    if removed is None:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "They already confirmed or dismissed it",
            extra={"reason": "handoff_settled"},
        )
    await session.execute(
        text(
            "UPDATE commitments SET status = 'open', handed_off_to = NULL, updated_at = now() "
            "WHERE id = :id"
        ),
        {"id": str(promise_id)},
    )
    return await _load(session, me, promise_id)


# --- capacity --------------------------------------------------------------------------------


@router.get("/capacity", response_model=Capacity, summary="My capacity")
async def get_capacity(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    start: Annotated[date | None, Query(description="First day; default this Monday.")] = None,
    days: Annotated[int, Query(ge=1, le=cap.MAX_RANGE_DAYS)] = 7,
    tz: Annotated[str | None, Query(max_length=64, description="IANA zone, default UTC.")] = None,
) -> Capacity:
    """Your days: what is due (promises and actions), meeting and focus time, and how full each
    day is. Private to you."""
    me = _me(ctx)
    zone = cap.resolve_tz(tz)
    today = cap.today_in(zone)
    first = start or (today - timedelta(days=today.weekday()))
    loads = await cap.day_loads(session, me, first, days, zone)
    overdue, undated = await cap.counts_outside(session, me, today)
    return Capacity(
        start=first,
        today=today,
        tz=zone,
        days=[
            CapacityDay(
                date=d.day,
                label=day_label(d.day, today),
                promises=d.promises,
                actions=d.actions,
                items=d.items,
                meeting_minutes=d.meeting_minutes,
                focus_minutes=d.focus_minutes,
                away=d.away,
                load_minutes=d.load_minutes(),
                capacity_minutes=d.capacity_minutes,
                level=d.level(),
            )
            for d in loads
        ],
        overdue=overdue,
        undated=undated,
        calendar_synced=await cap.calendar_synced(session, me),
        workday_minutes=cap.WORKDAY_MINUTES,
        item_minutes=cap.ITEM_MINUTES,
    )


@router.get("/capacity/check", response_model=CapacityCheck, summary="Check my capacity")
async def check_capacity(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    due_date: Annotated[date, Query()],
    tz: Annotated[str | None, Query(max_length=64)] = None,
    exclude_kind: Annotated[Literal["commitment", "action"] | None, Query()] = None,
    exclude_id: Annotated[uuid.UUID | None, Query()] = None,
) -> CapacityCheck:
    """What one more thing due on `due_date` does to that day, before you say yes: "That's your
    seventh promise due Friday. Offer Monday instead?" `exclude_*` leaves out the item being
    accepted when it is already stored."""
    me = _me(ctx)
    exclude = (exclude_kind, exclude_id) if exclude_kind and exclude_id else None
    result = await cap.check(session, me, due_date, cap.resolve_tz(tz), exclude=exclude)
    return _check_model(result)


# --- renegotiate: date proposals -------------------------------------------------------------

_PROPOSAL_SELECT = """
SELECT d.id, d.commitment_id, d.action_id, d.content, d.current_due, d.proposed_due, d.note,
       d.state, d.proposer_id, pu.email AS proposer_email, d.requester_id,
       ru.email AS requester_email, d.created_at, d.decided_at
FROM promise_date_proposals d
LEFT JOIN users pu ON pu.id = d.proposer_id
LEFT JOIN users ru ON ru.id = d.requester_id
"""


def _proposal(row: dict[str, Any]) -> DateProposal:
    is_commitment = row["commitment_id"] is not None
    return DateProposal(
        id=row["id"],
        target="commitment" if is_commitment else "action",
        target_id=row["commitment_id"] if is_commitment else row["action_id"],
        content=row["content"],
        current_due=row["current_due"],
        proposed_due=row["proposed_due"],
        note=row["note"],
        state=row["state"],
        proposer_id=row["proposer_id"],
        proposer_name=display_name(row["proposer_email"]),
        requester_id=row["requester_id"],
        requester_name=display_name(row["requester_email"]),
        created_at=row["created_at"],
        decided_at=row["decided_at"],
    )


async def _proposal_by_id(session: AsyncSession, proposal_id: uuid.UUID) -> DateProposal:
    row = (
        (
            await session.execute(
                text(f"{_PROPOSAL_SELECT} WHERE d.id = :id"), {"id": str(proposal_id)}
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return _proposal(dict(row))


async def _target(
    session: AsyncSession, me: uuid.UUID, body: DateProposalCreate
) -> tuple[str, date | None, uuid.UUID | None]:
    """(content, current due, requester) of a target the caller owes, locked; 404 otherwise."""
    if body.target == "commitment":
        row = await _locked(session, me, body.target_id)
        if row["status"] != "open" or row["review_state"] != "confirmed":
            raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "Only an open promise can move")
        requester = row["recipient_user_id"]
        return str(row["content"]), row["due_at"], (requester if requester != me else None)
    action = (
        (
            await session.execute(
                text(
                    "SELECT a.content, a.due_date, a.confirmed_by, a.status FROM action_items a "
                    "WHERE a.id = :id AND a.assignee_id = :me AND a.deleted_at IS NULL "
                    "FOR UPDATE"
                ),
                {"id": str(body.target_id), "me": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if action is None:
        raise not_found()
    if action["status"] not in ("confirmed", "proposed"):
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "Only an open action can move")
    requester = action["confirmed_by"]
    if requester is not None:
        live = (
            await session.execute(
                text("SELECT 1 FROM users WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(requester)},
            )
        ).first()
        requester = requester if live else None
    return str(action["content"]), action["due_date"], (requester if requester != me else None)


async def _apply_due(
    session: AsyncSession, target: str, target_id: uuid.UUID, due: date | None
) -> None:
    if target == "commitment":
        await session.execute(
            text("UPDATE commitments SET due_at = :d, updated_at = now() WHERE id = :id"),
            {"d": due, "id": str(target_id)},
        )
    else:
        await session.execute(
            text("UPDATE action_items SET due_date = :d WHERE id = :id"),
            {"d": due, "id": str(target_id)},
        )


@router.post(
    "/date-proposals",
    response_model=RenegotiateResult,
    status_code=status.HTTP_201_CREATED,
    summary="Renegotiate a date",
)
async def renegotiate(
    body: DateProposalCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> RenegotiateResult:
    """Offer another date for something you owe. When a colleague asked for it (the person you
    promised it to, or who assigned you the action) they are asked, with one notification, and
    decide; a new proposal replaces a pending one. Otherwise the date moves at once."""
    me = _require_write(ctx)
    if body.proposed_due < date.today() - timedelta(days=1):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Pick a date from today on")
    content, current, requester = await _target(session, me, body)
    if current == body.proposed_due:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "That is already the date")
    column = "commitment_id" if body.target == "commitment" else "action_id"
    await session.execute(
        text(
            "UPDATE promise_date_proposals SET state = 'withdrawn', decided_at = now() "
            f"WHERE {column} = :t AND state = 'pending'"
        ),
        {"t": str(body.target_id)},
    )
    if requester is None:
        await _apply_due(session, body.target, body.target_id, body.proposed_due)
        return RenegotiateResult(
            mode="applied",
            proposal=None,
            target=body.target,
            target_id=body.target_id,
            due_at=body.proposed_due,
            previous_due=current,
        )
    proposal_id = (
        await session.execute(
            text(
                f"INSERT INTO promise_date_proposals (org_id, {column}, proposer_id, "
                "requester_id, content, current_due, proposed_due, note) VALUES "
                "(:o, :t, :me, :r, :c, :cur, :p, :n) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "t": str(body.target_id),
                "me": str(me),
                "r": str(requester),
                "c": content[:500],
                "cur": current,
                "p": body.proposed_due,
                "n": body.note,
            },
        )
    ).scalar_one()
    name = await _my_name(session, me)
    was = f" instead of {short_date(current)}" if current else ""
    await notify(
        session,
        ctx.org_id,
        user_id=requester,
        source=f"promise-date:{proposal_id}:asked",
        title=f"{name} asks to move a promise to {short_date(body.proposed_due)}",
        body=f"{content}\n\n{short_date(body.proposed_due)}{was}."
        + (f"\n\n{name}: {body.note}" if body.note else "")
        + "\n\nAccept or decline in Mytheus.",
        link=f"/work/promises?proposal={proposal_id}",
        priority=2,
    )
    return RenegotiateResult(
        mode="proposed",
        proposal=await _proposal_by_id(session, proposal_id),
        target=body.target,
        target_id=body.target_id,
        due_at=current,
        previous_due=current,
    )


@router.get("/date-proposals", response_model=DateProposalList, summary="Date proposals")
async def list_proposals(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    role: Annotated[Literal["incoming", "outgoing"], Query()] = "incoming",
    state: Annotated[Literal["pending", "all"], Query()] = "pending",
) -> DateProposalList:
    """`incoming`: what people who owe you something asked to move. `outgoing`: what you
    asked."""
    if ctx.user_id is None:
        return DateProposalList(items=[])
    column = "d.requester_id" if role == "incoming" else "d.proposer_id"
    where = f"{column} = :me" + (" AND d.state = 'pending'" if state == "pending" else "")
    rows = (
        (
            await session.execute(
                text(f"{_PROPOSAL_SELECT} WHERE {where} ORDER BY d.created_at DESC LIMIT 100"),
                {"me": str(ctx.user_id)},
            )
        )
        .mappings()
        .all()
    )
    return DateProposalList(items=[_proposal(dict(r)) for r in rows])


async def _locked_proposal(
    session: AsyncSession, proposal_id: uuid.UUID, me: uuid.UUID, as_role: str
) -> dict[str, Any]:
    column = "requester_id" if as_role == "requester" else "proposer_id"
    row = (
        (
            await session.execute(
                text(
                    f"SELECT * FROM promise_date_proposals WHERE id = :id AND {column} = :me "
                    "FOR UPDATE"
                ),
                {"id": str(proposal_id), "me": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    if row["state"] != "pending":
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            f"This proposal was already {row['state']}",
            extra={"reason": "decided"},
        )
    return dict(row)


async def _decide(
    session: AsyncSession,
    ctx: RequestContext,
    proposal_id: uuid.UUID,
    outcome: Literal["accepted", "declined"],
) -> DateProposal:
    me = _require_write(ctx)
    row = await _locked_proposal(session, proposal_id, me, "requester")
    if outcome == "accepted":
        target = "commitment" if row["commitment_id"] else "action"
        target_id = row["commitment_id"] or row["action_id"]
        await _apply_due(session, target, target_id, row["proposed_due"])
    await session.execute(
        text("UPDATE promise_date_proposals SET state = :s, decided_at = now() WHERE id = :id"),
        {"s": outcome, "id": str(proposal_id)},
    )
    name = await _my_name(session, me)
    when = short_date(row["proposed_due"])
    if outcome == "accepted":
        title = f"{name} accepted {when}: {_short(row['content'], 100)}"
        body = f"{row['content']}\n\nThe new date is {when}."
    else:
        kept = short_date(row["current_due"]) if row["current_due"] else "the original date"
        title = f"{name} kept {kept}: {_short(row['content'], 100)}"
        body = (
            f"{row['content']}\n\n{name} declined {when}; it stays due {kept}. Talk to them if "
            "it cannot be done by then."
        )
    await notify(
        session,
        ctx.org_id,
        user_id=row["proposer_id"],
        source=f"promise-date:{proposal_id}:{outcome}",
        title=title,
        body=body,
        link=(
            f"/work/promises?focus={row['commitment_id']}"
            if row["commitment_id"]
            else f"/work/tasks?focus={row['action_id']}"
        ),
    )
    return await _proposal_by_id(session, proposal_id)


@router.post(
    "/date-proposals/{proposal_id}/accept", response_model=DateProposal, summary="Accept a date"
)
async def accept_proposal(
    proposal_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DateProposal:
    """The person who asked accepts the new date; it applies and the proposer is told."""
    return await _decide(session, ctx, proposal_id, "accepted")


@router.post(
    "/date-proposals/{proposal_id}/decline", response_model=DateProposal, summary="Decline a date"
)
async def decline_proposal(
    proposal_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DateProposal:
    """The person who asked keeps the original date; the proposer is told."""
    return await _decide(session, ctx, proposal_id, "declined")


@router.post(
    "/date-proposals/{proposal_id}/withdraw",
    response_model=DateProposal,
    summary="Withdraw a date proposal",
)
async def withdraw_proposal(
    proposal_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DateProposal:
    """The proposer's undo, while the other person has not decided."""
    me = _require_write(ctx)
    await _locked_proposal(session, proposal_id, me, "proposer")
    await session.execute(
        text(
            "UPDATE promise_date_proposals SET state = 'withdrawn', decided_at = now() "
            "WHERE id = :id"
        ),
        {"id": str(proposal_id)},
    )
    return await _proposal_by_id(session, proposal_id)


# --- capture preferences ---------------------------------------------------------------------


@router.get("/capture-preferences", response_model=CaptureSettings, summary="Where my agent looks")
async def get_capture_preferences(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CaptureSettings:
    """Whether your agent may look for promises in your own Slack messages and email."""
    return await capture_settings(session, _me(ctx))


@router.put(
    "/capture-preferences", response_model=CaptureSettings, summary="Set where my agent looks"
)
async def put_capture_preferences(
    body: CapturePreferencesUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CaptureSettings:
    """Turn promise capture from Slack or email on or off, for you only."""
    me = _require_write(ctx)
    await session.execute(
        text(
            "INSERT INTO promise_capture_preferences (org_id, user_id, slack_enabled, "
            "email_enabled) VALUES (:o, :u, :s, :e) ON CONFLICT (user_id) DO UPDATE SET "
            "slack_enabled = EXCLUDED.slack_enabled, email_enabled = EXCLUDED.email_enabled, "
            "updated_at = now()"
        ),
        {
            "o": str(ctx.org_id),
            "u": str(me),
            "s": body.slack_enabled,
            "e": body.email_enabled,
        },
    )
    return await capture_settings(session, me)
