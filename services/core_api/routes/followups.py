"""The meeting follow-up: a recap sent after the meeting to its participants and to the
external guests the host lists (polish plan Phase 4, "shareable recaps").

Routes:

- `GET /api/v1/meetings/{id}/follow-up` (member, participant): the settings, the host's recap
  share link if there is one, the last dispatch's result, whether this server can email guests
  at all (`email_configured`), and a `draft` (subject and body written from the meeting's shared
  minutes and confirmed records) for the composer.
- `PUT /api/v1/meetings/{id}/follow-up` (member, the HOST only) `{enabled, guest_emails}`:
  turn the follow-up on or off and list up to 20 external guests (validated, lowercased,
  de-duplicated).
- `POST /api/v1/meetings/{id}/follow-up/send` (member, the HOST only), optional body
  `{subject, body, guest_emails}` from the composer: send now (409 until the meeting has
  ended; the host's on/off setting governs only the automatic send). `guest_emails`, when
  given, also replaces the saved guest list. Safe to press again: participants are keyed
  per meeting and person, guests per meeting and address, so a second send only reaches guests
  added since the first.
- `POST /internal/meetings/{id}/follow-up/dispatch` (workflow-service caller token):
  `MeetingLifecycleWorkflow` calls it once the meeting has ended. `{status: "disabled"}` when
  the host turned the follow-up off. Minutes for everyone: when the meeting has a transcript and
  its shared minutes have not landed yet, it answers `{status: "unavailable",
  waiting_for_minutes: true}` (nothing sent yet) and records
  `awaiting_minutes_since`; core-api's `agent-results` consumer sends it when they land.

What one dispatch does (`dispatch`), in order:

1. Builds the recap AS THE HOST sees it (`records.build_recap`): confirmed decisions and
   actions and the meeting's SHARED minutes, never anything private (no personal minutes, no
   promises). A confidential meeting is shareable by its host, and
   the follow-up is the host's; guests of a confidential meeting are mailed only because the
   host listed them, which is the host allowing it.
2. Ensures the host's live recap share link (`share_tokens`), created by the host, 30 days.
   With share links not configured on the server the follow-up still goes out, without a link.
   A confidential meeting gets no NEW link here: only one the host created explicitly
   (`confirm_confidential`) is reused.
3. Enqueues one `notifications` event per participant through the outbox (channel `email`,
   key `notif:followup:{meeting_id}:{user_id}`). notification-service then delivers it in-app
   for everyone and by email only to people who turned email on: ADR-0036 makes email opt-in
   per user, and a follow-up from a meeting is no exception. That is the decision recorded
   here: "participants" means everyone gets the recap, by the channels they chose.
4. Emails the external guests through notification-service (`POST /internal/email/send`, the
   sole external fan-out), with the share link and the headline items. No email provider, or
   no caller secret for notification-service, records each guest as skipped; the host's screen
   shows it. notification-service unreachable is a 503, so the workflow activity retries.
5. Stores `last_sent_at` / `last_result` on `meeting_followups` and audits
   `meeting.followup_sent`.
"""

from __future__ import annotations

import html
import json
import logging
import re
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_MEETING_FOLLOWUP_SENT, ACTION_MEETING_UPDATED, audit
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api import minutes, records, share_tokens
from services.core_api.notification_client import (
    NotConfigured,
    NotificationServiceError,
    get_notifications,
)
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

logger = logging.getLogger(__name__)

router = APIRouter(tags=["follow-ups"])

PRODUCER = "core-api"
MAX_GUESTS = 20
HEADLINE_ITEMS = 5
RECAP_LINK_DAYS = 30
EMAIL_RE = re.compile(
    r"^[^@\s<>,;\"']{1,64}@[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?\.[A-Za-z]{2,63}$"
)

DispatchStatus = Literal["sent", "disabled", "unavailable"]
MAX_SUBJECT = 200
MAX_BODY = 10000
GuestStatus = Literal["pending", "sent", "failed", "skipped"]


# --- models -------------------------------------------------------------------------------


class FollowUpUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(description="Send the follow-up when the meeting ends.")
    guest_emails: list[str] = Field(
        default_factory=list,
        max_length=MAX_GUESTS,
        description="External guests to email the recap to (people who are not users).",
    )

    @field_validator("guest_emails")
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


class GuestDelivery(BaseModel):
    recipient: str
    status: GuestStatus
    detail: str | None = Field(
        default=None,
        description="Why a guest was skipped or failed, e.g. `not_configured` (no email "
        "provider on the server).",
    )


class FollowUpDispatch(BaseModel):
    """What one dispatch did."""

    status: DispatchStatus = Field(
        description="`sent`; `disabled` (the host turned the follow-up off); `unavailable` "
        "(the meeting has no host who can share its recap, or, with `waiting_for_minutes`, "
        "the automatic send is waiting for the meeting's minutes)."
    )
    waiting_for_minutes: bool = Field(
        default=False,
        description="Automatic send only: deferred until the meeting's minutes land; core-api "
        "sends it then. `status` is `unavailable` meanwhile (nothing was sent).",
    )
    sent_at: datetime | None = None
    participants_notified: int = 0
    guests: list[GuestDelivery] = Field(default_factory=list)
    email_configured: bool | None = Field(
        default=None,
        description="Whether the server could email the guests; null when there were none.",
    )
    share_url: str | None = Field(
        default=None, description="The public recap page, or null when links are not configured."
    )


class FollowUpShareLink(BaseModel):
    id: uuid.UUID
    url: str
    expires_at: datetime
    view_count: int


class FollowUpSend(BaseModel):
    """What the host's composer sends. Every field optional: missing ones use the draft."""

    model_config = ConfigDict(extra="forbid")

    subject: str | None = Field(default=None, min_length=1, max_length=MAX_SUBJECT)
    body: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_BODY,
        description="Plain text. Lines starting with '- ' become a list in the HTML email.",
    )
    guest_emails: list[str] | None = Field(
        default=None,
        max_length=MAX_GUESTS,
        description="Replaces the saved external guests before sending.",
    )

    @field_validator("guest_emails")
    @classmethod
    def _emails(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else FollowUpUpdate._emails(value)


class FollowUpDraft(BaseModel):
    """The composer's starting point, written from the shared minutes and confirmed records."""

    subject: str
    body: str
    participant_count: int = Field(description="Participants who get it (in app, email if on).")


class FollowUp(BaseModel):
    """A meeting's follow-up settings as one participant sees them."""

    meeting_id: uuid.UUID
    enabled: bool
    guest_emails: list[str]
    is_host: bool = Field(description="Whether the caller may change settings and send.")
    can_send: bool = Field(description="Host, meeting ended, follow-up on.")
    meeting_status: str
    share_link: FollowUpShareLink | None = None
    last_sent_at: datetime | None = None
    last_result: FollowUpDispatch | None = None
    email_configured: bool | None = Field(
        default=None,
        description="Whether this server can email external guests: false shows the "
        "composer's not-connected state; null when it could not be determined.",
    )
    confidential: bool = False
    minutes_ready: bool = Field(
        default=False, description="Whether the meeting's shared minutes exist."
    )
    awaiting_minutes: bool = Field(
        default=False,
        description="The automatic follow-up is waiting for the minutes (sent when they land).",
    )
    draft: FollowUpDraft | None = Field(
        default=None, description="For the host, once the meeting ended; null otherwise."
    )


# --- helpers ------------------------------------------------------------------------------


def _not_a_participant() -> ProblemDetail:
    return ProblemDetail(
        403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
    )


def _host_only() -> ProblemDetail:
    return ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Only the meeting's host can do this")


def _conflict(detail: str) -> ProblemDetail:
    return ProblemDetail(409, ErrorCode.VERSION_CONFLICT, detail)


async def _settings(session: AsyncSession, meeting_id: uuid.UUID) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                text(
                    "SELECT enabled, guest_emails, share_link_id, last_sent_at, last_result, "
                    "awaiting_minutes_since FROM meeting_followups WHERE meeting_id = :m"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return {
            "enabled": True,
            "guest_emails": [],
            "share_link_id": None,
            "last_sent_at": None,
            "last_result": None,
            "awaiting_minutes_since": None,
        }
    return dict(row)


async def _share_link(
    session: AsyncSession, org_id: uuid.UUID, link_id: uuid.UUID | None
) -> FollowUpShareLink | None:
    if link_id is None or not share_tokens.configured():
        return None
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {share_tokens.LINK_COLS} FROM share_links WHERE id = :id"  # noqa: S608
                ),
                {"id": str(link_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    link = share_tokens.row_from(row)
    if not link.active:
        return None
    return FollowUpShareLink(
        id=link.id,
        url=share_tokens.share_url(share_tokens.mint(org_id, link.id)),
        expires_at=link.expires_at,
        view_count=link.view_count,
    )


async def _access(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> records.MeetingAccess:
    """404 for an invisible meeting, 403 for a non-participant."""
    viewer = ctx.user_id or uuid.UUID(int=0)
    access = await records.access_for(session, meeting_id, viewer)
    if access is None:
        raise not_found()
    if ctx.user_id is None or not access.is_participant:
        raise _not_a_participant()
    return access


def _when(value: datetime | None) -> str:
    return value.strftime("%a %d %b %Y") if value else ""


def _action_line(a: records.RecordAction) -> str:
    extra = [
        x for x in (a.owner_name, f"due {a.due_date.isoformat()}" if a.due_date else None) if x
    ]
    return a.content + (f" ({', '.join(extra)})" if extra else "")


def _decision_line(d: records.RecapMinutesDecision) -> str:
    return d.text if d.status == "agreed" else f"{d.text} ({_STATUS_WORDS.get(d.status, d.status)})"


_STATUS_WORDS = {"tentative": "tentative", "deferred": "deferred", "reversed": "reversed"}


def _item_line(o: records.RecapMinutesItem) -> str:
    extra = [x for x in (o.owner_name, f"by {o.due}" if o.due else None) if x]
    return o.text + (f" ({', '.join(extra)})" if extra else "")


def _question_line(q: records.RecapMinutesQuestion) -> str:
    return q.text + (f" (asked by {q.asked_by_name})" if q.asked_by_name else "")


def _sections(recap: records.MeetingRecap) -> list[tuple[str, list[str], int]]:
    """(title, the first HEADLINE_ITEMS lines, how many more) per non-empty section. With
    shared minutes: their decisions (with status), the confirmed actions, the open items and
    the unanswered questions; without, the confirmed decisions and actions as before."""

    def cut(title: str, lines: list[str]) -> tuple[str, list[str], int]:
        return title, lines[:HEADLINE_ITEMS], len(lines) - HEADLINE_ITEMS

    m = recap.minutes
    decisions = (
        [_decision_line(d) for d in m.decisions] if m else [d.content for d in recap.decisions]
    )
    out = [
        cut("Decisions", decisions),
        cut("Actions", [_action_line(a) for a in recap.actions]),
    ]
    if m:
        out.append(cut("Still open", [_item_line(o) for o in m.open_items]))
        out.append(cut("Open questions", [_question_line(q) for q in m.unanswered_questions]))
    return [section for section in out if section[1]]


def headline_text(recap: records.MeetingRecap, url: str | None) -> str:
    """The plain-text body: the minutes' summary, then each section's headline items, then
    the link. Also the composer's draft body."""
    lines = [
        f"Recap of {recap.title}"
        + (f", {_when(recap.scheduled_start)}" if recap.scheduled_start else "")
    ]
    if recap.minutes and recap.minutes.summary:
        lines += ["", recap.minutes.summary]
    sections = _sections(recap)
    for title, items, more in sections:
        lines += ["", title] + [f"- {i}" for i in items]
        if more > 0:
            lines.append(f"- and {more} more")
    if not sections:
        lines += ["", "No decisions or actions were confirmed in this meeting."]
    if url:
        lines += ["", f"Full recap: {url}"]
    return "\n".join(lines)


_INK, _MUTED, _ACCENT = "#1f1d1a", "#6b645c", "#c96442"


def _html_shell(inner: list[str], url: str | None) -> str:
    parts = [
        f'<div style="font-family:-apple-system,Segoe UI,Helvetica,Arial,sans-serif;'
        f'color:{_INK};max-width:560px;margin:0 auto;padding:24px;line-height:1.5">',
        *inner,
    ]
    if url:
        parts.append(
            f'<p style="margin:28px 0 0"><a href="{html.escape(url, quote=True)}" '
            f'style="display:inline-block;background:{_ACCENT};color:#fff;'
            "text-decoration:none;padding:10px 18px;border-radius:8px;font-weight:600"
            '">Open the full recap</a></p>'
        )
    parts.append(
        f'<p style="margin:28px 0 0;color:{_MUTED};font-size:12px">Sent with Mytheus on behalf '
        f"of the meeting&#39;s host.</p></div>"
    )
    return "".join(parts)


def headline_html(recap: records.MeetingRecap, url: str | None) -> str:
    """The same content as simple, inline-styled HTML (email clients ignore stylesheets)."""
    e = html.escape
    parts = [
        f'<p style="margin:0 0 4px;color:{_MUTED};font-size:13px">Meeting recap</p>',
        f'<h1 style="margin:0 0 4px;font-size:22px;font-weight:600">{e(recap.title)}</h1>',
    ]
    if recap.scheduled_start:
        parts.append(
            f'<p style="margin:0 0 20px;color:{_MUTED};font-size:14px">'
            f"{e(_when(recap.scheduled_start))}</p>"
        )
    if recap.minutes and recap.minutes.summary:
        parts.append(f'<p style="margin:0 0 8px">{e(recap.minutes.summary)}</p>')
    sections = _sections(recap)
    for title, items, more in sections:
        parts.append(f'<h2 style="margin:20px 0 8px;font-size:15px;font-weight:600">{title}</h2>')
        parts.append('<ul style="margin:0;padding-left:20px">')
        parts.extend(f'<li style="margin:0 0 6px">{e(i)}</li>' for i in items)
        if more > 0:
            parts.append(f'<li style="margin:0 0 6px;color:{_MUTED}">and {more} more</li>')
        parts.append("</ul>")
    if not sections:
        parts.append(
            f'<p style="color:{_MUTED}">No decisions or actions were confirmed in this meeting.</p>'
        )
    return _html_shell(parts, url)


def body_html(body: str, url: str | None) -> str:
    """The host's edited plain-text body as HTML: blank lines split paragraphs, '- ' lines
    become a list, a line followed by a list is its heading. Everything is escaped."""
    e = html.escape
    parts: list[str] = []
    for block in [b for b in body.replace("\r\n", "\n").split("\n\n") if b.strip()]:
        lines = [ln.rstrip() for ln in block.split("\n") if ln.strip()]
        items = [ln.strip()[2:] for ln in lines if ln.strip().startswith("- ")]
        head = [ln for ln in lines if not ln.strip().startswith("- ")]
        if items and len(head) <= 1:
            if head:
                parts.append(
                    f'<h2 style="margin:20px 0 8px;font-size:15px;font-weight:600">'
                    f"{e(head[0].strip())}</h2>"
                )
            parts.append('<ul style="margin:0;padding-left:20px">')
            parts.extend(f'<li style="margin:0 0 6px">{e(i)}</li>' for i in items)
            parts.append("</ul>")
        else:
            parts.append(
                '<p style="margin:0 0 12px">' + "<br>".join(e(ln) for ln in lines) + "</p>"
            )
    return _html_shell(parts, url)


def draft_subject(recap: records.MeetingRecap) -> str:
    return f"Recap: {recap.title}"[:MAX_SUBJECT]


# --- the dispatch ------------------------------------------------------------------------


def _with_link(body: str, url: str | None) -> str:
    return body if not url or url in body else f"{body.rstrip()}\n\nFull recap: {url}"


async def _minutes_on_the_way(session: AsyncSession, meeting_id: uuid.UUID) -> bool:
    """True when the meeting has a transcript (so minutes will be written) and its shared
    minutes have not landed yet."""
    if await minutes.shared_minutes(session, meeting_id) is not None:
        return False
    has_table = (
        await session.execute(text("SELECT to_regclass('public.transcript_chunks') IS NOT NULL"))
    ).scalar()
    if not has_table:
        return False
    spoken = (
        await session.execute(
            text("SELECT 1 FROM transcript_chunks WHERE meeting_id = :m AND is_final LIMIT 1"),
            {"m": str(meeting_id)},
        )
    ).first()
    return spoken is not None


async def dispatch(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    *,
    actor_id: uuid.UUID | None,
    ip: str | None = None,
    subject: str | None = None,
    body: str | None = None,
    automatic: bool = False,
) -> FollowUpDispatch:
    """Send the follow-up once (idempotent per participant and per guest). See module doc.

    `subject` / `body` are the host's composer text (defaults: the draft). `automatic` (the
    workflow's send at the meeting's end, and the consumer's deferred one) honours the host's
    on/off setting and, the first time, defers while minutes are still on their way; a host
    pressing Send is neither: they asked for it now."""
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        access = await records.access_for(session, meeting_id, uuid.UUID(int=0))
        if access is None:
            raise not_found()
        settings = await _settings(session, meeting_id)
        if automatic and not settings["enabled"]:
            return FollowUpDispatch(status="disabled")
        host = access.host_id
        recap = await records.build_recap(session, meeting_id, host) if host else None
        if host is None or recap is None:
            return FollowUpDispatch(status="unavailable")
        if (
            automatic
            and settings["last_sent_at"] is None
            and settings["awaiting_minutes_since"] is None
            and await _minutes_on_the_way(session, meeting_id)
        ):
            await session.execute(
                text(
                    "INSERT INTO meeting_followups (meeting_id, org_id, awaiting_minutes_since) "
                    "VALUES (:m, :o, now()) ON CONFLICT (meeting_id) DO UPDATE SET "
                    "awaiting_minutes_since = COALESCE(meeting_followups.awaiting_minutes_since, "
                    "now()), updated_at = now()"
                ),
                {"m": str(meeting_id), "o": str(org_id)},
            )
            return FollowUpDispatch(status="unavailable", waiting_for_minutes=True)

        url: str | None = None
        link_id: uuid.UUID | None = None
        if share_tokens.configured():
            link = await share_tokens.active_recap_link(session, meeting_id, host)
            if link is None and not access.confidential:
                link = await share_tokens.create_link(
                    session,
                    org_id=org_id,
                    kind="meeting_recap",
                    meeting_id=meeting_id,
                    decision_id=None,
                    created_by=host,
                    ttl_days=RECAP_LINK_DAYS,
                )
            if link is not None:
                link_id = link.id
                url = share_tokens.share_url(share_tokens.mint(org_id, link.id))

        await session.execute(
            text(
                "INSERT INTO meeting_followups (meeting_id, org_id, share_link_id) "
                "VALUES (:m, :o, :l) ON CONFLICT (meeting_id) DO UPDATE SET "
                "share_link_id = COALESCE(EXCLUDED.share_link_id, "
                "meeting_followups.share_link_id), awaiting_minutes_since = NULL, "
                "updated_at = now()"
            ),
            {"m": str(meeting_id), "o": str(org_id), "l": str(link_id) if link_id else None},
        )

        participants = (
            (
                await session.execute(
                    text(
                        "SELECT p.user_id FROM meeting_participants p JOIN users u "
                        "ON u.id = p.user_id AND u.deleted_at IS NULL "
                        "WHERE p.meeting_id = :m ORDER BY p.user_id"
                    ),
                    {"m": str(meeting_id)},
                )
            )
            .scalars()
            .all()
        )
        title = (subject or draft_subject(recap))[:MAX_SUBJECT]
        in_app = (body or headline_text(recap, None))[:2000]
        for user_id in participants:
            await enqueue(
                session,
                EventEnvelope(
                    topic="notifications",
                    org_id=str(org_id),
                    producer=PRODUCER,
                    idempotency_key=f"notif:followup:{meeting_id}:{user_id}",
                    payload={
                        "source_event_id": f"followup:{meeting_id}",
                        "channel": "email",
                        "user_id": str(user_id),
                        "priority": 2,
                        "title": title,
                        "body": in_app,
                        "link": f"/meetings/{meeting_id}",
                    },
                ),
            )
        guests: list[str] = list(settings["guest_emails"] or [])

    deliveries: list[GuestDelivery] = []
    email_configured: bool | None = None
    if guests:
        try:
            answer = await get_notifications().send_email(
                org_id,
                meeting_id=meeting_id,
                idempotency_key=f"followup:{meeting_id}",
                recipients=guests,
                subject=title,
                text=_with_link(body, url) if body else headline_text(recap, url),
                html=body_html(body, url) if body else headline_html(recap, url),
            )
            email_configured = bool(answer.get("configured"))
            deliveries = [
                GuestDelivery(recipient=r["recipient"], status=r["status"], detail=r.get("detail"))
                for r in answer.get("results", [])
            ]
        except NotConfigured:
            email_configured = False
            deliveries = [
                GuestDelivery(recipient=g, status="skipped", detail="not_configured")
                for g in guests
            ]
        except NotificationServiceError as exc:
            logger.warning("follow-up for %s: notification-service failed: %s", meeting_id, exc)
            raise ProblemDetail(
                503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Email delivery is unavailable"
            ) from None

    result = FollowUpDispatch(
        status="sent",
        sent_at=datetime.now(UTC),
        participants_notified=len(participants),
        guests=deliveries,
        email_configured=email_configured,
        share_url=url,
    )
    async with sessionmaker() as session, session.begin():
        await set_rls(session, org_id)
        await session.execute(
            text(
                "UPDATE meeting_followups SET last_sent_at = :t, "
                "last_result = CAST(:r AS jsonb), updated_at = now() WHERE meeting_id = :m"
            ),
            {"t": result.sent_at, "r": result.model_dump_json(), "m": str(meeting_id)},
        )
        await audit(
            session,
            org_id=org_id,
            actor_id=actor_id,
            actor_type="user" if actor_id else "system",
            action=ACTION_MEETING_FOLLOWUP_SENT,
            target_type="meeting",
            target_id=meeting_id,
            metadata={
                "participants": len(participants),
                "guests": [d.model_dump() for d in deliveries],
                "share_link_id": str(link_id) if link_id else None,
            },
            ip=ip,
        )
    return result


# --- routes --------------------------------------------------------------------------------


@router.get("/api/v1/meetings/{meeting_id}/follow-up", response_model=FollowUp)
async def get_follow_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> FollowUp:
    """The meeting's follow-up settings, recap link and last result. Participants only."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        access = await _access(session, ctx, meeting_id)
        settings = await _settings(session, meeting_id)
        link = await _share_link(session, ctx.org_id, settings["share_link_id"])
        if link is None and ctx.user_id is not None and share_tokens.configured():
            # The host's own live recap link, made from the minutes card, counts too.
            host_id = access.host_id
            own = (
                await share_tokens.active_recap_link(session, meeting_id, host_id)
                if host_id is not None
                else None
            )
            link = await _share_link(session, ctx.org_id, own.id) if own else None
        is_host = ctx.user_id is not None and access.viewer_is_host(ctx.user_id)
        shared = await minutes.shared_minutes(session, meeting_id)
        draft: FollowUpDraft | None = None
        if is_host and access.status == "ended" and access.host_id is not None:
            recap = await records.build_recap(session, meeting_id, access.host_id)
            if recap is not None:
                participant_count = len(recap.participant_names)
                draft = FollowUpDraft(
                    subject=draft_subject(recap),
                    body=headline_text(recap, link.url if link else None),
                    participant_count=participant_count,
                )
    email_configured = await get_notifications().email_status(ctx.org_id) if is_host else None
    raw = settings["last_result"]
    last = (
        FollowUpDispatch.model_validate(json.loads(raw) if isinstance(raw, str) else raw)
        if raw
        else None
    )
    return FollowUp(
        meeting_id=meeting_id,
        enabled=bool(settings["enabled"]),
        guest_emails=list(settings["guest_emails"] or []),
        is_host=is_host,
        can_send=is_host and access.status == "ended",
        meeting_status=access.status,
        share_link=link,
        last_sent_at=settings["last_sent_at"],
        last_result=last,
        email_configured=email_configured,
        confidential=access.confidential,
        minutes_ready=shared is not None and shared.status == "ready",
        awaiting_minutes=settings["awaiting_minutes_since"] is not None,
        draft=draft,
    )


@router.put("/api/v1/meetings/{meeting_id}/follow-up", response_model=FollowUp)
async def put_follow_up(
    meeting_id: uuid.UUID,
    body: FollowUpUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> FollowUp:
    """Turn the follow-up on or off and set the external guests. The host only."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        access = await _access(session, ctx, meeting_id)
        if ctx.user_id is None or not access.viewer_is_host(ctx.user_id):
            raise _host_only()
        await session.execute(
            text(
                "INSERT INTO meeting_followups (meeting_id, org_id, enabled, guest_emails, "
                "updated_by) VALUES (:m, :o, :e, :g, :u) ON CONFLICT (meeting_id) DO UPDATE SET "
                "enabled = EXCLUDED.enabled, guest_emails = EXCLUDED.guest_emails, "
                "updated_by = EXCLUDED.updated_by, updated_at = now()"
            ),
            {
                "m": str(meeting_id),
                "o": str(ctx.org_id),
                "e": body.enabled,
                "g": body.guest_emails,
                "u": str(ctx.user_id),
            },
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_MEETING_UPDATED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={
                "follow_up": {"enabled": body.enabled, "guest_count": len(body.guest_emails)}
            },
            ip=ctx.ip,
        )
    return await get_follow_up(meeting_id, ctx)


@router.post("/api/v1/meetings/{meeting_id}/follow-up/send", response_model=FollowUpDispatch)
async def send_follow_up(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    body: FollowUpSend | None = None,
) -> FollowUpDispatch:
    """Send the follow-up now, with the composer's subject, body and guests (each optional:
    the draft and the saved guests otherwise). The host only, once the meeting has ended;
    pressing it again reaches only guests added since. Honours `Idempotency-Key`."""
    req = body or FollowUpSend()
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        access = await _access(session, ctx, meeting_id)
        if ctx.user_id is None or not access.viewer_is_host(ctx.user_id):
            raise _host_only()
        if access.status != "ended":
            raise _conflict("The follow-up can be sent once the meeting has ended")
        if req.guest_emails is not None:
            await session.execute(
                text(
                    "INSERT INTO meeting_followups (meeting_id, org_id, guest_emails, updated_by) "
                    "VALUES (:m, :o, :g, :u) ON CONFLICT (meeting_id) DO UPDATE SET "
                    "guest_emails = EXCLUDED.guest_emails, updated_by = EXCLUDED.updated_by, "
                    "updated_at = now()"
                ),
                {
                    "m": str(meeting_id),
                    "o": str(ctx.org_id),
                    "g": req.guest_emails,
                    "u": str(ctx.user_id),
                },
            )
    return await dispatch(
        ctx.org_id,
        meeting_id,
        actor_id=ctx.user_id,
        ip=ctx.ip,
        subject=req.subject,
        body=req.body,
    )


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, frozenset({"workflow-service"}))


@router.post(
    "/internal/meetings/{meeting_id}/follow-up/dispatch",
    response_model=FollowUpDispatch,
    tags=["core-internal"],
)
async def dispatch_follow_up(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> FollowUpDispatch:
    """workflow-service, after the meeting ended: send the follow-up (or `disabled`, or
    `waiting_for_minutes` while the meeting's minutes are on their way)."""
    return await dispatch(org_id, meeting_id, actor_id=None, automatic=True)
