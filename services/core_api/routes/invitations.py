"""Meeting invitations by email, and the meeting as a calendar event (ADR-0043).

Routes:

- `POST /api/v1/meetings/{id}/invitations` `{emails?, user_ids?, message?}` (the meeting's host,
  its creator, or a workspace admin): email an invitation to each person. Recipients are
  de-duplicated (an address and a user id naming the same person count once), at most 50 per
  call, and rate-limited per meeting (`MEETING_INVITE_HOURLY_LIMIT` emails an hour, default 100;
  429 with `Retry-After`). Each one is added to the meeting as an attendee if they were not in it
  (an invitation you cannot use would be a dead link), gets one `meeting_invitations` row (the
  latest delivery state), and the send is audited. Sending to someone already invited sends
  again (Resend); a personal message left out keeps the one sent before.
- `GET /api/v1/meetings/{id}/invitations` (participants, the creator, admins): the join link, the
  add-to-calendar links, and, for those who may invite, who was invited and each delivery's
  state (`pending`, `sent`, `failed`, `not_configured`) plus whether email is set up at all.
- `GET /api/v1/meetings/invitations/status` (member): `{email_configured}`, for the New meeting
  window, which has no meeting yet.
- `GET /api/v1/meetings/{id}/calendar.ics` (participants, the creator, admins): the meeting as
  an RFC 5545 VEVENT (METHOD:PUBLISH) with the agenda and the attendees.
- `GET /api/v1/public/calendar/{token}.ics` (no sign-in; the HMAC-signed token is the
  credential, `meeting_calendar.py`): the same event with calendar data only, no agenda and no
  attendee address. The link every invitation email carries.

Guests. Only people in the workspace can be invited. Joining a call needs a signed-in
participant (meeting-service mints a LiveKit token only for a `meeting_participants` row), and
there is no guest join path, so an address that is not a member of this workspace is a 422
naming it rather than an email whose Join button would lead nowhere.

Delivery. core-api renders each email for its recipient (their time zone) and hands the batch to
notification-service (`POST /internal/email/invitations`, the sole external fan-out), which
claims each message idempotently, sends it with retries and reports per recipient. Not
configured (no provider, or no caller secret for notification-service) records every row
`not_configured` and says so; notification-service unreachable records them `failed`
(`email_service_unavailable`) so the host can retry. Never a 5xx for either.

Reschedule and cancel. `schedule_change_after_commit` is called by `PATCH /meetings/{id}` when
the time changes or the meeting is cancelled (the SEQUENCE is bumped in the same statement).
After the request's transaction commits, everyone who was emailed gets an update (`.ics` with
the new SEQUENCE, METHOD:REQUEST) or a cancellation (METHOD:CANCEL), keyed by the SEQUENCE so a
repeat is a no-op, and only when email is configured.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_MEETING_INVITATIONS_SENT,
    ACTION_MEETING_PARTICIPANTS_ADDED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.cache import get_redis, redis_key
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail, problem_response
from libs.shared.tenancy import set_rls
from services.core_api import invitation_email
from services.core_api import meeting_calendar as cal
from services.core_api.notification_client import (
    NotConfigured,
    NotificationServiceError,
    get_notifications,
)
from services.core_api.records import person_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["meeting-invitations"])
public_router = APIRouter(prefix="/api/v1/public", tags=["public-calendar"])

MAX_RECIPIENTS = 50
MAX_MESSAGE = 500
RATE_ENV = "MEETING_INVITE_HOURLY_LIMIT"
DEFAULT_RATE = 100
EMAIL_RE = re.compile(
    r"^[^@\s<>,;\"']{1,64}@[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?\.[A-Za-z]{2,63}$"
)
CALENDAR_MEDIA = "text/calendar; charset=utf-8"
_CLOSED = ("ended", "cancelled")

InvitationStatus = Literal["pending", "sent", "failed", "not_configured"]
InvitationKind = Literal["invite", "update", "cancel"]
_NOTIFY_KIND = {
    "invite": "meeting_invitation",
    "update": "meeting_update",
    "cancel": "meeting_cancel",
}


# --- models -------------------------------------------------------------------------------


class InvitationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    emails: list[str] = Field(
        default_factory=list,
        max_length=MAX_RECIPIENTS,
        description="Addresses of people in this workspace (lowercased, de-duplicated).",
    )
    user_ids: list[uuid.UUID] = Field(default_factory=list, max_length=MAX_RECIPIENTS)
    message: str | None = Field(
        default=None,
        max_length=MAX_MESSAGE,
        description="A short personal note shown in the email. Left out on a resend, the note "
        "sent before is kept.",
    )

    @field_validator("emails")
    @classmethod
    def _emails(cls, value: list[str]) -> list[str]:
        out: list[str] = []
        for raw in value:
            email = raw.strip().lower()
            if len(email) > 320 or not EMAIL_RE.match(email):
                raise ValueError(f"not an email address: {raw!r}")
            if email not in out:
                out.append(email)
        return out

    @field_validator("message")
    @classmethod
    def _message(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @model_validator(mode="after")
    def _someone(self) -> InvitationCreate:
        if not self.emails and not self.user_ids:
            raise ValueError("name at least one person in emails or user_ids")
        return self


class Invitation(BaseModel):
    id: uuid.UUID
    email: str
    user_id: uuid.UUID | None
    name: str | None = Field(description="The person's name as the workspace shows it.")
    status: InvitationStatus
    last_error: str | None = Field(
        default=None, description="Why the last delivery failed, e.g. `resend_http_500`."
    )
    last_kind: InvitationKind = Field(
        description="What was sent last: the invitation, an "
        "update after a reschedule, or the cancellation."
    )
    send_count: int
    sent_at: datetime | None
    invited_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class CalendarLinks(BaseModel):
    ics_path: str = Field(
        description="The signed-in download, `/api/v1/meetings/{id}/calendar.ics`."
    )
    public_ics_url: str | None = Field(
        default=None,
        description="The no-sign-in calendar link from the emails; null when the calendar "
        "token key is not configured.",
    )
    google_url: str
    outlook_url: str


class InvitationList(BaseModel):
    meeting_id: uuid.UUID
    can_invite: bool = Field(description="The host, the creator or a workspace admin.")
    email_configured: bool | None = Field(
        default=None,
        description="Whether this server can send email; null for someone who cannot invite, "
        "or when notification-service could not be asked.",
    )
    join_url: str
    calendar: CalendarLinks | None = Field(
        default=None, description="Null for a meeting with no scheduled time."
    )
    items: list[Invitation] = Field(
        default_factory=list, description="Who was invited; empty for someone who cannot invite."
    )


class InvitationSendResult(BaseModel):
    email_configured: bool
    invitations: list[Invitation] = Field(description="The rows this call sent, in order.")
    sent: int
    failed: int
    not_configured: int
    participants_added: int = Field(description="People who were not in the meeting before.")


class InvitationEmailStatus(BaseModel):
    email_configured: bool | None = Field(
        description="Whether this server can send email; null when it could not be asked."
    )


# --- loading -------------------------------------------------------------------------------


@dataclass
class _Person:
    id: uuid.UUID
    email: str
    name: str | None
    role: str | None = None


@dataclass
class _Meeting:
    id: uuid.UUID
    title: str
    status: str
    start: datetime | None
    end: datetime | None
    created_by: uuid.UUID | None
    sequence: int
    participants: list[_Person]
    agenda: list[str]
    workspace: str | None

    @property
    def host(self) -> _Person | None:
        return next((p for p in self.participants if p.role == "host"), None)


async def _load(session: AsyncSession, meeting_id: uuid.UUID, *, lock: bool = False) -> _Meeting:
    row = (
        (
            await session.execute(
                text(
                    "SELECT id, title, status, scheduled_start, scheduled_end, created_by, "
                    "calendar_sequence FROM meetings WHERE id = :m AND deleted_at IS NULL"
                    + (" FOR UPDATE" if lock else "")
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    people = (
        (
            await session.execute(
                text(
                    "SELECT u.id, u.email, u.display_name, p.role_in_meeting "
                    "FROM meeting_participants p JOIN users u ON u.id = p.user_id "
                    "AND u.deleted_at IS NULL WHERE p.meeting_id = :m "
                    "ORDER BY (p.role_in_meeting = 'host') DESC, u.id"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .all()
    )
    agenda = (
        (
            await session.execute(
                text(
                    "SELECT title FROM meeting_agenda_items WHERE meeting_id = :m "
                    "ORDER BY position, id"
                ),
                {"m": str(meeting_id)},
            )
        )
        .scalars()
        .all()
    )
    workspace = (await session.execute(text("SELECT name FROM organizations LIMIT 1"))).scalar()
    return _Meeting(
        id=row["id"],
        title=(str(row["title"]).strip() if row["title"] else "") or "Untitled meeting",
        status=row["status"],
        start=row["scheduled_start"],
        end=row["scheduled_end"],
        created_by=row["created_by"],
        sequence=int(row["calendar_sequence"] or 0),
        participants=[
            _Person(
                id=p["id"],
                email=str(p["email"] or "").lower(),
                name=person_name(p["display_name"], p["email"]),
                role=p["role_in_meeting"],
            )
            for p in people
        ],
        agenda=[str(t) for t in agenda],
        workspace=str(workspace) if workspace else None,
    )


def _can_invite(ctx: RequestContext, meeting: _Meeting) -> bool:
    if ctx.user_id is None:
        return False
    if ctx.user_id == meeting.created_by:
        return True
    host = meeting.host
    if host is not None and host.id == ctx.user_id:
        return True
    return ctx.satisfies("admin")


def _can_see(ctx: RequestContext, meeting: _Meeting) -> bool:
    if _can_invite(ctx, meeting):
        return True
    return ctx.user_id is not None and any(p.id == ctx.user_id for p in meeting.participants)


def _event(
    org_id: uuid.UUID, meeting: _Meeting, *, cancelled: bool = False
) -> cal.CalendarEvent | None:
    span = cal.times(meeting.start, meeting.end)
    if span is None:
        return None
    host = meeting.host
    return cal.CalendarEvent(
        meeting_id=meeting.id,
        title=meeting.title,
        start=span[0],
        end=span[1],
        sequence=meeting.sequence,
        join_url=cal.join_url(meeting.id),
        organizer=cal.Person(host.name, host.email or None) if host else None,
        attendees=[cal.Person(p.name, p.email or None) for p in meeting.participants],
        agenda=meeting.agenda,
        cancelled=cancelled or meeting.status == "cancelled",
    )


def _links(org_id: uuid.UUID, meeting: _Meeting) -> CalendarLinks | None:
    event_ = _event(org_id, meeting)
    if event_ is None:
        return None
    return CalendarLinks(
        ics_path=f"/api/v1/meetings/{meeting.id}/calendar.ics",
        public_ics_url=cal.public_ics_url(org_id, meeting.id),
        google_url=cal.google_url(event_),
        outlook_url=cal.outlook_url(event_),
    )


_INVITATION_COLS = (
    "i.id, i.email, i.user_id, i.status, i.last_error, i.last_kind, i.send_count, i.sent_at, "
    "i.invited_by, i.created_at, i.updated_at, u.display_name"
)


def _invitation(row: Any) -> Invitation:
    return Invitation(
        id=row["id"],
        email=row["email"],
        user_id=row["user_id"],
        name=person_name(row["display_name"], row["email"]),
        status=row["status"],
        last_error=row["last_error"],
        last_kind=row["last_kind"],
        send_count=row["send_count"],
        sent_at=row["sent_at"],
        invited_by=row["invited_by"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def _invitations(
    session: AsyncSession, meeting_id: uuid.UUID, ids: list[uuid.UUID] | None = None
) -> list[Invitation]:
    where = "i.meeting_id = :m" + (" AND i.id = ANY(:ids)" if ids is not None else "")
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_INVITATION_COLS} FROM meeting_invitations i "  # noqa: S608
                    f"LEFT JOIN users u ON u.id = i.user_id WHERE {where} "
                    "ORDER BY i.created_at, i.email"
                ),
                {"m": str(meeting_id), **({"ids": ids} if ids is not None else {})},
            )
        )
        .mappings()
        .all()
    )
    out = [_invitation(r) for r in rows]
    if ids is not None:
        order = {v: i for i, v in enumerate(ids)}
        out.sort(key=lambda inv: order.get(inv.id, 0))
    return out


# --- rate limit ----------------------------------------------------------------------------


def hourly_limit() -> int:
    try:
        return max(1, int(os.environ.get(RATE_ENV, DEFAULT_RATE)))
    except ValueError:
        return DEFAULT_RATE


async def take_rate(org_id: uuid.UUID, meeting_id: uuid.UUID, count: int) -> tuple[bool, int]:
    """A fixed one-hour window per meeting, in Redis so every replica shares it. Unreachable ->
    503: unlimited sending is not an option (each call can mail 50 people)."""
    now = time.time()
    key = redis_key(org_id, "ratelimit", "meeting-invites", meeting_id, int(now // 3600))
    try:
        redis = get_redis()
        total = int(await redis.incrby(key, count))
        if total == count:
            await redis.expire(key, 7200)
    except Exception:  # noqa: BLE001 - any Redis failure fails closed
        logger.exception("meeting invitations: rate-limit store unavailable")
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Invitations are unavailable; retry shortly"
        ) from None
    reset = max(1, 3600 - int(now % 3600))
    if total > hourly_limit():
        try:
            await get_redis().decrby(key, count)  # a refused call does not use the budget
        except Exception:  # noqa: BLE001 - the window expires on its own
            logger.warning("meeting invitations: could not return the refused budget")
        return False, reset
    return True, reset


# --- delivery ------------------------------------------------------------------------------


@dataclass
class _Outgoing:
    invitation_id: uuid.UUID
    person: _Person
    key: str


async def _time_zones(session: AsyncSession, user_ids: list[uuid.UUID]) -> dict[uuid.UUID, str]:
    if not user_ids:
        return {}
    rows = await session.execute(
        text("SELECT user_id, time_zone FROM listening_preferences WHERE user_id = ANY(:ids)"),
        {"ids": user_ids},
    )
    return {r[0]: str(r[1]) for r in rows if r[1]}


async def _deliver(
    org_id: uuid.UUID,
    meeting: _Meeting,
    outgoing: list[_Outgoing],
    *,
    kind: InvitationKind,
    inviter: _Person,
    messages: dict[uuid.UUID, str | None],
    zones: dict[uuid.UUID, str],
) -> tuple[bool, dict[uuid.UUID, tuple[InvitationStatus, str | None]]]:
    """Render and send. Returns (email_configured, invitation id -> (status, last_error))."""
    event_ = _event(org_id, meeting, cancelled=kind == "cancel")
    links = _links(org_id, meeting) if kind != "cancel" else None
    host = meeting.host
    rendered: list[dict[str, Any]] = []
    for item in outgoing:
        content = invitation_email.InvitationContent(
            kind=kind,
            title=meeting.title,
            inviter_name=inviter.name or "Someone",
            host_name=host.name if host else None,
            start=event_.start if event_ else None,
            end=event_.end if event_ else None,
            time_zone=zones.get(item.person.id),
            join_url=cal.join_url(meeting.id),
            recipient_name=item.person.name,
            agenda=meeting.agenda,
            message=messages.get(item.invitation_id),
            google_url=links.google_url if links else None,
            outlook_url=links.outlook_url if links else None,
            ics_url=links.public_ics_url if links else None,
            workspace=meeting.workspace,
        )
        mail = invitation_email.render(content)
        rendered.append(
            {
                "idempotency_key": item.key,
                "recipient": item.person.email,
                "subject": mail.subject,
                "text": mail.text,
                "html": mail.html,
                "reply_to": inviter.email or None,
            }
        )
    attachment: dict[str, str] | None = None
    if event_ is not None:
        method: cal.Method = "CANCEL" if kind == "cancel" else "REQUEST"
        ics = cal.build_ics(event_, method)
        attachment = {
            "filename": "invite.ics",
            "content_type": f"text/calendar; method={method}",
            "content_base64": base64.b64encode(ics.encode("utf-8")).decode("ascii"),
        }

    outcome: dict[uuid.UUID, tuple[InvitationStatus, str | None]] = {}
    try:
        answer = await get_notifications().send_invitations(
            org_id,
            meeting_id=meeting.id,
            kind=_NOTIFY_KIND[kind],
            messages=rendered,
            attachment=attachment,
        )
    except NotConfigured:
        return False, {o.invitation_id: ("not_configured", None) for o in outgoing}
    except NotificationServiceError as exc:
        logger.warning("invitations for %s: notification-service failed: %s", meeting.id, exc)
        return True, {o.invitation_id: ("failed", "email_service_unavailable") for o in outgoing}
    configured = bool(answer.get("configured"))
    results = list(answer.get("results", []))
    for item, result in zip(outgoing, results, strict=False):
        state = str(result.get("status"))
        detail = result.get("detail")
        if state == "sent":
            outcome[item.invitation_id] = ("sent", None)
        elif state == "skipped":
            outcome[item.invitation_id] = ("not_configured", None)
        elif state == "pending":
            outcome[item.invitation_id] = ("pending", None)
        else:
            outcome[item.invitation_id] = (
                "failed",
                (str(detail) if detail else "send_failed")[:200],
            )
    for item in outgoing[len(results) :]:
        outcome[item.invitation_id] = ("failed", "no_result")
    return configured, outcome


async def _record(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    outcome: dict[uuid.UUID, tuple[InvitationStatus, str | None]],
) -> None:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        for invitation_id, (state, error) in outcome.items():
            await session.execute(
                text(
                    "UPDATE meeting_invitations SET status = :s, last_error = :e, "
                    "sent_at = CASE WHEN :s = 'sent' THEN now() ELSE sent_at END, "
                    "updated_at = now() WHERE id = :id AND meeting_id = :m"
                ),
                {"s": state, "e": error, "id": str(invitation_id), "m": str(meeting_id)},
            )


# --- routes --------------------------------------------------------------------------------


@router.get("/meetings/invitations/status", response_model=InvitationEmailStatus)
async def invitation_email_status(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> InvitationEmailStatus:
    """Whether this server can email invitations (the New meeting window asks before there is
    a meeting). Reveals no address or secret."""
    return InvitationEmailStatus(
        email_configured=await get_notifications().email_status(ctx.org_id)
    )


@router.get("/meetings/{meeting_id}/invitations", response_model=InvitationList)
async def list_invitations(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> InvitationList:
    """The join link and calendar links, and (for those who may invite) who was invited and how
    each delivery went. 404 across tenants; 403 for someone not in the meeting."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        meeting = await _load(session, meeting_id)
        if not _can_see(ctx, meeting):
            raise ProblemDetail(
                403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
            )
        can_invite = _can_invite(ctx, meeting)
        items = await _invitations(session, meeting_id) if can_invite else []
    configured = await get_notifications().email_status(ctx.org_id) if can_invite else None
    return InvitationList(
        meeting_id=meeting_id,
        can_invite=can_invite,
        email_configured=configured,
        join_url=cal.join_url(meeting_id),
        calendar=_links(ctx.org_id, meeting),
        items=items,
    )


@router.post(
    "/meetings/{meeting_id}/invitations",
    response_model=InvitationSendResult,
    responses={
        403: {"description": "Only the host, the creator or a workspace admin can invite."},
        409: {"description": "The meeting has ended or was cancelled."},
        429: {"description": "Too many invitations for this meeting this hour (`Retry-After`)."},
    },
)
async def send_invitations(
    meeting_id: uuid.UUID,
    body: InvitationCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Any:
    """Email the meeting to people in this workspace (and add them to it). See the module doc.
    Honours `Idempotency-Key`."""
    email_configured = await get_notifications().email_status(ctx.org_id)
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        meeting = await _load(session, meeting_id, lock=True)
        if not _can_invite(ctx, meeting):
            raise ProblemDetail(
                403,
                ErrorCode.FORBIDDEN_ROLE,
                "Only the meeting's host, its creator or a workspace admin can send invitations",
            )
        if meeting.status in _CLOSED:
            raise ProblemDetail(
                409, ErrorCode.VERSION_CONFLICT, f"This meeting is {meeting.status}"
            )

        recipients: dict[uuid.UUID, _Person] = {}
        for user_id in dict.fromkeys(body.user_ids):
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT id, email, display_name FROM users "
                            "WHERE id = :u AND deleted_at IS NULL"
                        ),
                        {"u": str(user_id)},
                    )
                )
                .mappings()
                .first()
            )
            if row is None or not row["email"]:
                raise ProblemDetail(
                    status.HTTP_404_NOT_FOUND,
                    ErrorCode.NOT_FOUND,
                    "No such user in this organization",
                )
            recipients.setdefault(
                row["id"],
                _Person(
                    row["id"],
                    str(row["email"]).lower(),
                    person_name(row["display_name"], row["email"]),
                ),
            )
        unknown: list[int] = []
        for index, email in enumerate(body.emails):
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT id, email, display_name FROM users "
                            "WHERE lower(email) = :e AND deleted_at IS NULL "
                            "ORDER BY created_at LIMIT 1"
                        ),
                        {"e": email},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                unknown.append(index)
                continue
            recipients.setdefault(
                row["id"],
                _Person(row["id"], email, person_name(row["display_name"], row["email"])),
            )
        if unknown:
            raise ProblemDetail(
                422,
                ErrorCode.VALIDATION_FAILED,
                "Only people in this workspace can be invited: guests can't join a meeting by "
                "link yet. Add them to the workspace first, or share the link with a member.",
                extra={
                    "errors": [
                        {
                            "loc": ["body", "emails", i],
                            "msg": "not a member of this workspace",
                            "type": "value_error",
                        }
                        for i in unknown
                    ]
                },
            )
        if len(recipients) > MAX_RECIPIENTS:
            raise ProblemDetail(
                422,
                ErrorCode.VALIDATION_FAILED,
                f"At most {MAX_RECIPIENTS} people per call",
            )
        allowed, reset = await take_rate(ctx.org_id, meeting_id, len(recipients))
        if not allowed:  # nothing written yet in this transaction
            response = problem_response(
                429,
                ErrorCode.RATE_LIMITED,
                "Too many invitations for this meeting this hour. Try again later.",
                extra={"retry_after": reset},
            )
            response.headers["Retry-After"] = str(reset)
            return response

        inviter_row = (
            (
                await session.execute(
                    text("SELECT email, display_name FROM users WHERE id = :u"),
                    {"u": str(ctx.user_id)},
                )
            )
            .mappings()
            .first()
        )
        inviter = _Person(
            ctx.user_id or uuid.UUID(int=0),
            str(inviter_row["email"] or "").lower() if inviter_row else "",
            person_name(inviter_row["display_name"], inviter_row["email"]) if inviter_row else None,
        )

        in_meeting = {p.id for p in meeting.participants}
        added: list[str] = []
        for person in recipients.values():
            if person.id in in_meeting:
                continue
            await session.execute(
                text(
                    "INSERT INTO meeting_participants (meeting_id, user_id, org_id, "
                    "role_in_meeting) VALUES (:m, :u, :o, 'attendee') ON CONFLICT DO NOTHING"
                ),
                {"m": str(meeting_id), "u": str(person.id), "o": str(ctx.org_id)},
            )
            added.append(str(person.id))
            meeting.participants.append(_Person(person.id, person.email, person.name, "attendee"))
        if added:
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_MEETING_PARTICIPANTS_ADDED,
                target_type="meeting",
                target_id=meeting_id,
                metadata={"user_ids": added, "via": "invitation"},
                ip=ctx.ip,
            )

        initial: InvitationStatus = "not_configured" if email_configured is False else "pending"
        outgoing: list[_Outgoing] = []
        messages: dict[uuid.UUID, str | None] = {}
        for person in recipients.values():
            row = (
                (
                    await session.execute(
                        text(
                            "INSERT INTO meeting_invitations (org_id, meeting_id, email, user_id, "
                            "invited_by, message, status, last_error, last_kind, send_count) "
                            "VALUES (:o, :m, :e, :u, :by, :msg, :s, NULL, 'invite', 1) "
                            "ON CONFLICT (meeting_id, email) DO UPDATE SET "
                            "user_id = EXCLUDED.user_id, invited_by = EXCLUDED.invited_by, "
                            "message = COALESCE(EXCLUDED.message, meeting_invitations.message), "
                            "status = EXCLUDED.status, last_error = NULL, last_kind = 'invite', "
                            "send_count = meeting_invitations.send_count + 1, updated_at = now() "
                            "RETURNING id, send_count, message"
                        ),
                        {
                            "o": str(ctx.org_id),
                            "m": str(meeting_id),
                            "e": person.email,
                            "u": str(person.id),
                            "by": str(ctx.user_id) if ctx.user_id else None,
                            "msg": body.message,
                            "s": initial,
                        },
                    )
                )
                .mappings()
                .one()
            )
            outgoing.append(
                _Outgoing(
                    invitation_id=row["id"],
                    person=person,
                    key=f"invite:{meeting_id}:{row['id']}:{row['send_count']}",
                )
            )
            messages[row["id"]] = row["message"]
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_MEETING_INVITATIONS_SENT,
            target_type="meeting",
            target_id=meeting_id,
            metadata={
                "kind": "invite",
                "user_ids": [str(p.id) for p in recipients.values()],
                "count": len(recipients),
                "email_configured": email_configured,
                "with_message": body.message is not None,
            },
            ip=ctx.ip,
        )
        zones = await _time_zones(session, [p.id for p in recipients.values()])

    configured = email_configured is not False
    if configured:
        configured, outcome = await _deliver(
            ctx.org_id,
            meeting,
            outgoing,
            kind="invite",
            inviter=inviter,
            messages=messages,
            zones=zones,
        )
        await _record(ctx.org_id, meeting_id, outcome)

    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        rows = await _invitations(session, meeting_id, [o.invitation_id for o in outgoing])
    return InvitationSendResult(
        email_configured=configured,
        invitations=rows,
        sent=sum(1 for r in rows if r.status == "sent"),
        failed=sum(1 for r in rows if r.status == "failed"),
        not_configured=sum(1 for r in rows if r.status == "not_configured"),
        participants_added=len(added),
    )


def _ics_response(body: str, title: str) -> Response:
    return Response(
        content=body,
        media_type=CALENDAR_MEDIA,
        headers={
            "Content-Disposition": f'attachment; filename="{cal.filename(title)}"',
            "Cache-Control": "private, no-store",
        },
    )


_ICS_RESPONSES: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "An RFC 5545 calendar with one VEVENT.",
        "content": {"text/calendar": {"schema": {"type": "string"}}},
    },
    404: {"description": "Unknown meeting (or invalid token)."},
    409: {"description": "The meeting has no scheduled time yet."},
}


@router.get(
    "/meetings/{meeting_id}/calendar.ics",
    response_class=Response,
    responses=_ICS_RESPONSES,
    summary="The meeting as a calendar file",
)
async def meeting_calendar(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Response:
    """The meeting as an `.ics` (METHOD:PUBLISH): the agenda, the join link, the host and every
    participant. Participants, the creator and admins; 404 across tenants."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        meeting = await _load(session, meeting_id)
        if not _can_see(ctx, meeting):
            raise ProblemDetail(
                403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
            )
    event_ = _event(ctx.org_id, meeting)
    if event_ is None:
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "This meeting has no time set yet")
    return _ics_response(cal.build_ics(event_, "PUBLISH"), meeting.title)


@public_router.get(
    # authz: public - the HMAC-signed token in the path is the credential (calendar data only).
    "/calendar/{token}.ics",
    response_class=Response,
    responses=_ICS_RESPONSES,
    summary="A meeting as a calendar file, from an invitation's link",
    description=(
        "No authentication: the signed token in the path is the credential. It grants the "
        "meeting's calendar data only: the title, the time, the join link and the host's name, "
        "never the agenda, the attendees or anything said in the meeting. 404 for a token that "
        "does not verify or a meeting that is gone."
    ),
)
async def public_meeting_calendar(token: str) -> Response:
    verified = cal.verify_token(token)
    if verified is None:
        raise not_found()
    org_id, meeting_id = verified
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        meeting = await _load(session, meeting_id)
    event_ = _event(org_id, meeting)
    if event_ is None:
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "This meeting has no time set yet")
    return _ics_response(cal.build_ics(cal.public_event(event_), "PUBLISH"), meeting.title)


# --- reschedule and cancel --------------------------------------------------------------------

_PENDING_KEY = "meeting_invitation_changes"
_TASKS: set[asyncio.Task[None]] = set()


async def send_schedule_change(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    kind: Literal["update", "cancel"],
    actor_id: uuid.UUID | None,
) -> None:
    """Email everyone who received the invitation the new time or the cancellation. Keyed by the
    SEQUENCE, so running twice for one change sends once. Only when email is configured."""
    if await get_notifications().email_status(org_id) is False:
        return
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        meeting = await _load(session, meeting_id)
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT i.id, i.email, i.user_id, u.display_name, i.message "
                        "FROM meeting_invitations i LEFT JOIN users u ON u.id = i.user_id "
                        "WHERE i.meeting_id = :m AND i.sent_at IS NOT NULL ORDER BY i.created_at"
                    ),
                    {"m": str(meeting_id)},
                )
            )
            .mappings()
            .all()
        )
        if not rows:
            return
        actor_row = (
            (
                await session.execute(
                    text("SELECT id, email, display_name FROM users WHERE id = :u"),
                    {"u": str(actor_id)},
                )
            )
            .mappings()
            .first()
            if actor_id
            else None
        )
        host = meeting.host
        inviter = (
            _Person(
                actor_row["id"],
                str(actor_row["email"] or "").lower(),
                person_name(actor_row["display_name"], actor_row["email"]),
            )
            if actor_row
            else (host or _Person(uuid.UUID(int=0), "", "The host"))
        )
        outgoing = [
            _Outgoing(
                invitation_id=r["id"],
                person=_Person(
                    r["user_id"] or uuid.UUID(int=0),
                    r["email"],
                    person_name(r["display_name"], r["email"]),
                ),
                key=f"invite-{kind}:{meeting_id}:{r['id']}:{meeting.sequence}",
            )
            for r in rows
        ]
        await session.execute(
            text(
                "UPDATE meeting_invitations SET last_kind = :k, send_count = send_count + 1, "
                "updated_at = now() WHERE meeting_id = :m AND sent_at IS NOT NULL"
            ),
            {"k": kind, "m": str(meeting_id)},
        )
        await audit(
            session,
            org_id=org_id,
            actor_id=actor_id,
            actor_type="user" if actor_id else "system",
            action=ACTION_MEETING_INVITATIONS_SENT,
            target_type="meeting",
            target_id=meeting_id,
            metadata={"kind": kind, "count": len(outgoing), "sequence": meeting.sequence},
        )
        zones = await _time_zones(session, [o.person.id for o in outgoing])
    _configured, outcome = await _deliver(
        org_id,
        meeting,
        outgoing,
        kind=kind,
        inviter=inviter,
        messages={},
        zones=zones,
    )
    await _record(org_id, meeting_id, outcome)


async def _run_change(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    kind: Literal["update", "cancel"],
    actor_id: uuid.UUID | None,
) -> None:
    try:
        await send_schedule_change(org_id, meeting_id, kind, actor_id)
    except Exception:  # noqa: BLE001 - a failed update email must never surface anywhere else
        logger.exception("meeting %s: the %s email could not be sent", meeting_id, kind)


def schedule_change_after_commit(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    kind: Literal["update", "cancel"],
    actor_id: uuid.UUID | None,
) -> None:
    """Send the update or cancellation once the caller's transaction commits (never before:
    the email must describe what was saved, and a rolled-back PATCH sends nothing)."""
    pending: dict[uuid.UUID, tuple[str, uuid.UUID | None]] | None = session.info.get(_PENDING_KEY)
    if pending is not None:
        previous = pending.get(meeting_id)
        if previous is None or previous[0] != "cancel":
            pending[meeting_id] = (kind, actor_id)
        return
    session.info[_PENDING_KEY] = {meeting_id: (kind, actor_id)}

    def _after_commit(_sync_session: Any) -> None:
        changes = session.info.pop(_PENDING_KEY, {})
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # no loop: nothing to schedule on (not the request path)
            return
        for mid, (change, actor) in changes.items():
            task = loop.create_task(_run_change(org_id, mid, change, actor))
            _TASKS.add(task)
            task.add_done_callback(_TASKS.discard)

    def _after_rollback(_sync_session: Any) -> None:
        session.info.pop(_PENDING_KEY, None)

    event.listen(session.sync_session, "after_commit", _after_commit, once=True)
    event.listen(session.sync_session, "after_soft_rollback", _after_rollback, once=True)
