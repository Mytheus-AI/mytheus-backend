"""A meeting as a calendar event (ADR-0043): RFC 5545 `.ics`, the signed calendar link, and the
Google Calendar and Outlook "add event" URLs.

The `.ics` is one VEVENT:

- `UID` is stable per meeting (`meeting-<id>@mytheus.cloud`), so every update replaces the same
  event; `SEQUENCE` is `meetings.calendar_sequence`, bumped by core-api when the time changes or
  the meeting is cancelled; `DTSTART` / `DTEND` / `DTSTAMP` are UTC (`...Z`).
- `SUMMARY` the title, `DESCRIPTION` the agenda and the join link, `URL` and `LOCATION` the join
  link, `ORGANIZER` the host, one `ATTENDEE` per participant, and a `VALARM` ten minutes before.
- `METHOD` is `REQUEST` for an emailed invitation or update (Gmail and Outlook show their RSVP
  card), `CANCEL` (with `STATUS:CANCELLED`) for a cancellation, and `PUBLISH` for a download.

The calendar link. A recipient adds the meeting to a calendar without signing in through
`GET /api/v1/public/calendar/{token}.ics`. The token is `<payload>.<signature>`, unpadded
base64url: the 16-byte tenant id and the 16-byte meeting id, signed with the first 16 bytes of
HMAC-SHA256 under `MEETING_INVITE_TOKEN_SECRET` (or, unset, a key derived from
`GATEWAY_SIGNING_KEY` for the purpose "meeting-calendar"). The key is separate from share links'
on purpose: a share token never opens a calendar and a calendar token never opens a recap. It
grants calendar data only (title, time, join link, the host's name): `public_event` leaves out
the agenda and every attendee address. With neither key configured the link is not offered and
the public read is 404.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal
from urllib.parse import quote, urlencode

KEY_ENV = "MEETING_INVITE_TOKEN_SECRET"
FALLBACK_ENV = "GATEWAY_SIGNING_KEY"
MIN_KEY = 32
SIG_BYTES = 16
UID_DOMAIN = "mytheus.cloud"
DEFAULT_MINUTES = 30
ALARM_MINUTES = 10
PRODID = "-//Mytheus//Meetings//EN"

Method = Literal["REQUEST", "CANCEL", "PUBLISH"]


# --- the signed calendar link -----------------------------------------------------------------


def _signing_key() -> bytes | None:
    own = os.environ.get(KEY_ENV, "").strip()
    if len(own) >= MIN_KEY:
        return hmac.new(own.encode("utf-8"), b"mytheus/meeting-calendar/v1", "sha256").digest()
    gateway = os.environ.get(FALLBACK_ENV, "").strip()
    if len(gateway) >= MIN_KEY:
        return hmac.new(gateway.encode("utf-8"), b"mytheus/meeting-calendar/v1", "sha256").digest()
    return None


def token_configured() -> bool:
    return _signing_key() is not None


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def mint_token(org_id: uuid.UUID, meeting_id: uuid.UUID) -> str | None:
    """The calendar token for a meeting, or None when no key is configured."""
    key = _signing_key()
    if key is None:
        return None
    payload = org_id.bytes + meeting_id.bytes
    sig = hmac.new(key, payload, hashlib.sha256).digest()[:SIG_BYTES]
    return f"{_b64(payload)}.{_b64(sig)}"


def verify_token(token: str) -> tuple[uuid.UUID, uuid.UUID] | None:
    """(org_id, meeting_id) for a token this server signed, else None. Never raises."""
    key = _signing_key()
    if key is None or len(token) > 200 or token.count(".") != 1:
        return None
    raw_payload, raw_sig = token.split(".")
    try:
        payload, sig = _unb64(raw_payload), _unb64(raw_sig)
    except (binascii.Error, ValueError):
        return None
    if len(payload) != 32 or len(sig) != SIG_BYTES:
        return None
    expected = hmac.new(key, payload, hashlib.sha256).digest()[:SIG_BYTES]
    if not hmac.compare_digest(expected, sig):
        return None
    return uuid.UUID(bytes=payload[:16]), uuid.UUID(bytes=payload[16:])


def api_public_url() -> str:
    return (os.environ.get("API_PUBLIC_URL") or "https://api.mytheus.cloud").strip().rstrip("/")


def app_base_url() -> str:
    return os.environ.get("APP_BASE_URL", "http://localhost:3000").strip().rstrip("/")


def join_url(meeting_id: uuid.UUID) -> str:
    """The web app's pre-join page for the meeting (sign-in, then the device check)."""
    return f"{app_base_url()}/meetings/{meeting_id}/join"


def public_ics_url(org_id: uuid.UUID, meeting_id: uuid.UUID) -> str | None:
    token = mint_token(org_id, meeting_id)
    return f"{api_public_url()}/api/v1/public/calendar/{token}.ics" if token else None


# --- the event ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class Person:
    name: str | None
    email: str | None


@dataclass(frozen=True)
class CalendarEvent:
    meeting_id: uuid.UUID
    title: str
    start: datetime
    end: datetime
    sequence: int
    join_url: str
    organizer: Person | None = None
    attendees: list[Person] = field(default_factory=list)
    agenda: list[str] = field(default_factory=list)
    cancelled: bool = False

    @property
    def uid(self) -> str:
        return f"meeting-{self.meeting_id}@{UID_DOMAIN}"


def times(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime] | None:
    """UTC start and end, or None for a meeting with no scheduled start."""
    if start is None:
        return None
    s = start.astimezone(UTC) if start.tzinfo else start.replace(tzinfo=UTC)
    e = end.astimezone(UTC) if end and end.tzinfo else (end.replace(tzinfo=UTC) if end else None)
    if e is None or e <= s:
        e = s + timedelta(minutes=DEFAULT_MINUTES)
    return s, e


def public_event(event: CalendarEvent) -> CalendarEvent:
    """What the unauthenticated calendar link may show: no agenda, no attendee address, and the
    organizer by name only."""
    organizer = Person(event.organizer.name, None) if event.organizer else None
    return CalendarEvent(
        meeting_id=event.meeting_id,
        title=event.title,
        start=event.start,
        end=event.end,
        sequence=event.sequence,
        join_url=event.join_url,
        organizer=organizer,
        attendees=[],
        agenda=[],
        cancelled=event.cancelled,
    )


def description(event: CalendarEvent) -> str:
    lines: list[str] = []
    if event.agenda:
        lines.append("Agenda")
        lines.extend(f"{i}. {item}" for i, item in enumerate(event.agenda, 1))
        lines.append("")
    lines.append(f"Join the meeting: {event.join_url}")
    return "\n".join(lines)


# --- RFC 5545 ------------------------------------------------------------------------------------


def _utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def escape_text(value: str) -> str:
    """TEXT value escaping (RFC 5545 3.3.11)."""
    return (
        value.replace("\\", "\\\\")
        .replace(";", "\\;")
        .replace(",", "\\,")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\n")
    )


def _param(value: str) -> str:
    """A parameter value: quoted, with characters a quoted value cannot hold removed."""
    cleaned = "".join(ch for ch in value if ch not in '"\r\n' and (ch >= " " or ch == "\t"))
    return f'"{cleaned}"'


def fold(line: str) -> str:
    """Fold a content line at 75 octets (RFC 5545 3.1), never inside a UTF-8 sequence."""
    out: list[str] = []
    current = ""
    limit = 75
    for ch in line:
        if len((current + ch).encode("utf-8")) > limit:
            out.append(current)
            current = " " + ch
            limit = 75
        else:
            current += ch
    out.append(current)
    return "\r\n".join(out)


def _person(prop: str, person: Person, extra: str = "") -> str | None:
    if not person.email:
        if prop == "ORGANIZER" and person.name:
            return f"ORGANIZER;CN={_param(person.name)}:mailto:noreply@{UID_DOMAIN}"
        return None
    cn = f";CN={_param(person.name)}" if person.name else ""
    return f"{prop}{cn}{extra}:mailto:{person.email}"


def build_ics(event: CalendarEvent, method: Method, *, now: datetime | None = None) -> str:
    stamp = _utc(now or datetime.now(UTC))
    cancelled = event.cancelled or method == "CANCEL"
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{PRODID}",
        "CALSCALE:GREGORIAN",
        f"METHOD:{method}",
        "BEGIN:VEVENT",
        f"UID:{event.uid}",
        f"SEQUENCE:{max(0, event.sequence)}",
        f"DTSTAMP:{stamp}",
        f"DTSTART:{_utc(event.start)}",
        f"DTEND:{_utc(event.end)}",
        f"SUMMARY:{escape_text(event.title)}",
        f"DESCRIPTION:{escape_text(description(event))}",
        f"URL:{event.join_url}",
        f"LOCATION:{escape_text(event.join_url)}",
        f"STATUS:{'CANCELLED' if cancelled else 'CONFIRMED'}",
        "TRANSP:OPAQUE",
    ]
    if event.organizer:
        organizer = _person("ORGANIZER", event.organizer)
        if organizer:
            lines.append(organizer)
    for attendee in event.attendees:
        line = _person(
            "ATTENDEE",
            attendee,
            ";CUTYPE=INDIVIDUAL;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE",
        )
        if line:
            lines.append(line)
    if not cancelled:
        lines += [
            "BEGIN:VALARM",
            "ACTION:DISPLAY",
            f"DESCRIPTION:{escape_text(event.title)}",
            f"TRIGGER:-PT{ALARM_MINUTES}M",
            "END:VALARM",
        ]
    lines += ["END:VEVENT", "END:VCALENDAR"]
    return "\r\n".join(fold(line) for line in lines) + "\r\n"


# --- add-to-calendar links -------------------------------------------------------------------


def google_url(event: CalendarEvent) -> str:
    query = urlencode(
        {
            "action": "TEMPLATE",
            "text": event.title,
            "dates": f"{_utc(event.start)}/{_utc(event.end)}",
            "details": description(event),
            "location": event.join_url,
        },
        quote_via=quote,
    )
    return f"https://calendar.google.com/calendar/render?{query}"


def outlook_url(event: CalendarEvent) -> str:
    def iso(value: datetime) -> str:
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    query = urlencode(
        {
            "path": "/calendar/action/compose",
            "rru": "addevent",
            "subject": event.title,
            "startdt": iso(event.start),
            "enddt": iso(event.end),
            "body": description(event),
            "location": event.join_url,
        },
        quote_via=quote,
    )
    return f"https://outlook.office.com/calendar/0/deeplink/compose?{query}"


def filename(title: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in title).strip("-")
    while "--" in slug:
        slug = slug.replace("--", "-")
    return f"{(slug or 'meeting')[:60].strip('-') or 'meeting'}.ics"
