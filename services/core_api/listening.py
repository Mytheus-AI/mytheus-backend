"""In every meeting at once ("Beyond the meeting", Layer 3): the domain rules core-api applies.

A person invited to many meetings marks each one `attend`, `agent_listens` (their agent is a
passive listener and covers it) or `skip`, by hand or with standing rules. While a meeting they
are not in is live, their agent (agent-runtime-service's live watch) proposes interruptions;
core-api decides whether one reaches them now, keeps every one, and builds the evening digest.

The pieces, all pure or session-scoped (routes live in `routes/listening.py` and
`routes/internal_listening.py`):

- Choices and rules. `meeting_participants.attendance_choice` with its source (`manual` beats
  `rule`; no choice reads as attend). `apply_rules` materializes rules into upcoming meetings
  and keeps the published `attendance` column in step (`agent_listens` is `agent`), which is the
  column the runtime's proxies, the room's stand-ins and "N agents listening" read. A rule never
  puts an agent into a confidential meeting, and never applies to a meeting the person hosts
  unless the rule says so.
- When an interruption may land (`gate`): interruptions off, quiet hours, a focus block (when
  "never interrupt me during focus blocks" is on), and the throttle (at most `max_per_hour`
  delivered per rolling hour, and one per meeting per `MEETING_GAP`). A blocked interruption is
  `held`: kept, listed, and in the evening digest, but no toast, no inbox row and no push.
- The evening digest (`build_evening`): one paragraph per local day about the meetings the agent
  listened to, from the person's own minutes (or the shared ones), decisions with links to the
  moment they were made, and what the agent raised or held.

Privacy: only meetings the person is a participant of are ever read; a confidential meeting has
no listening agent, raises no interruption and contributes its title only to a digest.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key
from services.core_api import records

logger = logging.getLogger(__name__)

Choice = Literal["attend", "agent_listens", "skip"]
Source = Literal["manual", "rule", "default"]
HeldReason = Literal["disabled", "quiet_hours", "focus_block", "throttled"]

CHOICES: tuple[str, ...] = ("attend", "agent_listens", "skip")
OPEN_STATUSES = ("scheduled", "live")
MEETING_GAP = timedelta(minutes=8)  # one interruption per meeting per this long
RULE_HORIZON = timedelta(days=21)  # rules are materialized this far ahead
DEFAULT_MINUTES = 30  # a meeting with no scheduled end counts as this long
PRESENCE_FRESH_MS = 45_000  # realtime-gateway's stale-after for presence heartbeats
MAX_RULES = 20
MAX_POSITIONS = 25
MAX_FOCUS_BLOCKS = 14


# --- preferences ----------------------------------------------------------------------------


class FocusBlock(BaseModel):
    """A weekly window: `days` 0 (Monday) to 6, `start` / `end` as HH:MM local time."""

    model_config = ConfigDict(extra="forbid")

    days: list[int] = Field(min_length=1, max_length=7)
    start: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    end: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$|^24:00$")
    label: str | None = Field(default=None, max_length=40)

    @field_validator("days")
    @classmethod
    def _days(cls, value: list[int]) -> list[int]:
        if any(d < 0 or d > 6 for d in value):
            raise ValueError("days are 0 (Monday) to 6 (Sunday)")
        return sorted(set(value))

    @model_validator(mode="after")
    def _ordered(self) -> FocusBlock:
        if _minutes(self.end) <= _minutes(self.start):
            raise ValueError("a focus block ends after it starts")
        return self


class ListeningPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interruptions_enabled: bool = True
    max_per_hour: int = Field(default=3, ge=1, le=12)
    quiet_start: str | None = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    quiet_end: str | None = Field(default=None, pattern=r"^([01]\d|2[0-3]):[0-5]\d$")
    focus_blocks: list[FocusBlock] = Field(default_factory=list, max_length=MAX_FOCUS_BLOCKS)
    never_during_focus: bool = True
    digest_enabled: bool = True
    digest_hour: int = Field(default=18, ge=0, le=23)
    time_zone: str = Field(default="UTC", min_length=1, max_length=64)

    @field_validator("time_zone")
    @classmethod
    def _tz(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("time_zone must be an IANA time zone, e.g. Europe/London") from exc
        return value

    @model_validator(mode="after")
    def _quiet_pair(self) -> ListeningPreferences:
        if (self.quiet_start is None) != (self.quiet_end is None):
            raise ValueError("quiet hours need both a start and an end")
        if self.quiet_start is not None and self.quiet_start == self.quiet_end:
            raise ValueError("quiet hours start and end at different times")
        return self

    def zone(self) -> ZoneInfo:
        return zone_of(self.time_zone)


def zone_of(name: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(name or "UTC")
    except (ZoneInfoNotFoundError, ValueError):
        return ZoneInfo("UTC")


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def _hhmm(value: time | None) -> str | None:
    return None if value is None else value.strftime("%H:%M")


async def load_preferences(session: AsyncSession, user_id: uuid.UUID) -> ListeningPreferences:
    row = (
        (
            await session.execute(
                text(
                    "SELECT interruptions_enabled, max_per_hour, quiet_start, quiet_end, "
                    "focus_blocks, never_during_focus, digest_enabled, digest_hour, time_zone "
                    "FROM listening_preferences WHERE user_id = :u"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return ListeningPreferences()
    blocks = row["focus_blocks"]
    if isinstance(blocks, str):
        blocks = json.loads(blocks)
    return ListeningPreferences.model_construct(
        interruptions_enabled=row["interruptions_enabled"],
        max_per_hour=row["max_per_hour"],
        quiet_start=_hhmm(row["quiet_start"]),
        quiet_end=_hhmm(row["quiet_end"]),
        focus_blocks=[FocusBlock.model_validate(b) for b in blocks or []],
        never_during_focus=row["never_during_focus"],
        digest_enabled=row["digest_enabled"],
        digest_hour=row["digest_hour"],
        time_zone=row["time_zone"],
    )


async def save_preferences(
    session: AsyncSession, org_id: uuid.UUID, user_id: uuid.UUID, prefs: ListeningPreferences
) -> None:
    await session.execute(
        text(
            "INSERT INTO listening_preferences (user_id, org_id, interruptions_enabled, "
            "max_per_hour, quiet_start, quiet_end, focus_blocks, never_during_focus, "
            "digest_enabled, digest_hour, time_zone) VALUES (:u, :o, :e, :m, "
            ":qs, :qe, CAST(:f AS jsonb), :n, :d, :h, :tz) "
            "ON CONFLICT (user_id) DO UPDATE SET interruptions_enabled = EXCLUDED."
            "interruptions_enabled, max_per_hour = EXCLUDED.max_per_hour, quiet_start = "
            "EXCLUDED.quiet_start, quiet_end = EXCLUDED.quiet_end, focus_blocks = "
            "EXCLUDED.focus_blocks, never_during_focus = EXCLUDED.never_during_focus, "
            "digest_enabled = EXCLUDED.digest_enabled, digest_hour = EXCLUDED.digest_hour, "
            "time_zone = EXCLUDED.time_zone, updated_at = now()"
        ),
        {
            "u": str(user_id),
            "o": str(org_id),
            "e": prefs.interruptions_enabled,
            "m": prefs.max_per_hour,
            "qs": time.fromisoformat(prefs.quiet_start) if prefs.quiet_start else None,
            "qe": time.fromisoformat(prefs.quiet_end) if prefs.quiet_end else None,
            "f": json.dumps([b.model_dump() for b in prefs.focus_blocks]),
            "n": prefs.never_during_focus,
            "d": prefs.digest_enabled,
            "h": prefs.digest_hour,
            "tz": prefs.time_zone,
        },
    )


# --- when an interruption may land ----------------------------------------------------------


def _window_end(
    local: datetime, start_min: int, end_min: int, now_min: int, zone: ZoneInfo
) -> datetime | None:
    """If `now_min` falls in [start, end) (wrapping past midnight when end <= start), the UTC
    instant the window ends; None when outside it."""
    if start_min < end_min:
        inside = start_min <= now_min < end_min
        days = 0
    else:  # wraps midnight: 22:00-07:00
        inside = now_min >= start_min or now_min < end_min
        days = 1 if now_min >= start_min else 0
    if not inside:
        return None
    base = local.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days)
    return (base + timedelta(minutes=end_min)).replace(tzinfo=zone).astimezone(UTC)


def quiet_until(prefs: ListeningPreferences, now: datetime) -> tuple[HeldReason, datetime] | None:
    """Quiet hours or a focus block covering `now`: the reason and when it ends (UTC)."""
    zone = prefs.zone()
    local = now.astimezone(zone).replace(tzinfo=None)
    now_min = local.hour * 60 + local.minute
    if prefs.quiet_start and prefs.quiet_end:
        end = _window_end(
            local, _minutes(prefs.quiet_start), _minutes(prefs.quiet_end), now_min, zone
        )
        if end is not None:
            return "quiet_hours", end
    if prefs.never_during_focus:
        for block in prefs.focus_blocks:
            if local.weekday() not in block.days:
                continue
            end = _window_end(local, _minutes(block.start), _minutes(block.end), now_min, zone)
            if end is not None:
                return "focus_block", end
    return None


@dataclass(frozen=True)
class Gate:
    reason: HeldReason | None
    next_allowed_at: datetime | None

    @property
    def open(self) -> bool:
        return self.reason is None


async def gate(
    session: AsyncSession,
    user_id: uuid.UUID,
    meeting_id: uuid.UUID | None,
    now: datetime,
    prefs: ListeningPreferences | None = None,
) -> Gate:
    """Whether an interruption may reach this person now, and if not why and until when."""
    prefs = prefs or await load_preferences(session, user_id)
    if not prefs.interruptions_enabled:
        return Gate("disabled", None)
    quiet = quiet_until(prefs, now)
    if quiet is not None:
        return Gate(quiet[0], quiet[1])
    recent = [
        r[0]
        for r in await session.execute(
            text(
                "SELECT created_at FROM agent_interruptions WHERE user_id = :u "
                "AND status = 'delivered' AND created_at > :since ORDER BY created_at"
            ),
            {"u": str(user_id), "since": now - timedelta(hours=1)},
        )
    ]
    if len(recent) >= prefs.max_per_hour:
        return Gate("throttled", recent[len(recent) - prefs.max_per_hour] + timedelta(hours=1))
    if meeting_id is not None:
        last = (
            await session.execute(
                text(
                    "SELECT max(created_at) FROM agent_interruptions WHERE user_id = :u "
                    "AND meeting_id = :m AND status = 'delivered'"
                ),
                {"u": str(user_id), "m": str(meeting_id)},
            )
        ).scalar()
        if last is not None and now - last < MEETING_GAP:
            return Gate("throttled", last + MEETING_GAP)
    return Gate(None, None)


async def in_room(org_id: uuid.UUID, meeting_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    """Whether the person is in the meeting's room right now (realtime-gateway's presence
    heartbeat, `nexus:{org_id}:presence:{meeting_id}`). Unknown (Redis down) is False: the
    worst case is a toast for a meeting they just joined."""
    try:
        score = await get_redis().zscore(redis_key(org_id, "presence", meeting_id), str(user_id))
    except Exception:  # noqa: BLE001
        logger.warning("presence unavailable for meeting %s", meeting_id)
        return False
    if score is None:
        return False
    return datetime.now(UTC).timestamp() * 1000 - float(score) < PRESENCE_FRESH_MS


# --- choices and rules ----------------------------------------------------------------------


class AttendanceRule(BaseModel):
    id: uuid.UUID
    choice: Choice
    title_contains: str | None = None
    template_id: uuid.UUID | None = None
    template_name: str | None = None
    recurring_only: bool = False
    include_hosted: bool = False
    position: int = 0
    created_at: datetime
    updated_at: datetime


_MEETING_FACTS = (
    "SELECT m.id, m.title, m.status, m.scheduled_start, m.scheduled_end, m.confidential, "
    "m.template_id, m.source, m.created_by, p.user_id, p.role_in_meeting, p.attendance, "
    "p.attendance_choice, p.attendance_choice_source, p.attendance_rule_id, "
    "(m.created_by = p.user_id OR p.role_in_meeting = 'host') AS hosting, "
    "(m.previous_meeting_id IS NOT NULL "
    " OR EXISTS (SELECT 1 FROM meetings f WHERE f.previous_meeting_id = m.id "
    "            AND f.deleted_at IS NULL) "
    " OR COALESCE(m.calendar_event_id ~ '_[0-9]{8}(T[0-9]{6}Z?)?$', false) "
    " OR (m.title IS NOT NULL AND m.scheduled_start IS NOT NULL AND EXISTS ("
    "     SELECT 1 FROM meetings s WHERE s.id <> m.id AND s.deleted_at IS NULL "
    "     AND lower(s.title) = lower(m.title) "
    "     AND s.created_by IS NOT DISTINCT FROM m.created_by "
    "     AND s.scheduled_start BETWEEN m.scheduled_start - interval '35 days' "
    "                              AND m.scheduled_start + interval '35 days'))) AS recurring "
    "FROM meeting_participants p JOIN meetings m ON m.id = p.meeting_id "
    "JOIN users u ON u.id = p.user_id AND u.deleted_at IS NULL "
    "WHERE m.deleted_at IS NULL"
)


async def meeting_facts(
    session: AsyncSession,
    *,
    user_id: uuid.UUID | None = None,
    meeting_id: uuid.UUID | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    open_only: bool = False,
) -> list[dict[str, Any]]:
    clauses, params = [], {}
    if user_id is not None:
        clauses.append("p.user_id = :u")
        params["u"] = str(user_id)
    if meeting_id is not None:
        clauses.append("m.id = :m")
        params["m"] = str(meeting_id)
    if start is not None:
        clauses.append("m.scheduled_start >= :s")
        params["s"] = start  # type: ignore[assignment]
    if end is not None:
        clauses.append("m.scheduled_start < :e")
        params["e"] = end  # type: ignore[assignment]
    if open_only:
        clauses.append("m.status IN ('scheduled', 'live')")
    where = "".join(f" AND {c}" for c in clauses)
    rows = await session.execute(
        text(f"{_MEETING_FACTS}{where} ORDER BY m.scheduled_start NULLS LAST, m.id"), params
    )
    return [dict(r) for r in rows.mappings().all()]


async def load_rules(session: AsyncSession, user_id: uuid.UUID) -> list[AttendanceRule]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT r.id, r.choice, r.title_contains, r.template_id, "
                    "t.name AS template_name, r.recurring_only, r.include_hosted, r.position, "
                    "r.created_at, r.updated_at FROM attendance_rules r "
                    "LEFT JOIN meeting_templates t ON t.id = r.template_id "
                    "WHERE r.user_id = :u ORDER BY r.position, r.created_at, r.id"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .all()
    )
    return [AttendanceRule(**dict(r)) for r in rows]


def rule_matches(rule: AttendanceRule, m: dict[str, Any]) -> bool:
    if m.get("hosting") and not rule.include_hosted:
        return False
    if rule.choice == "agent_listens" and m.get("confidential"):
        return False  # an agent never listens to a confidential meeting
    if rule.title_contains and rule.title_contains.lower() not in (m.get("title") or "").lower():
        return False
    if rule.template_id is not None and m.get("template_id") != rule.template_id:
        return False
    return not (rule.recurring_only and not m.get("recurring"))


def first_match(rules: list[AttendanceRule], m: dict[str, Any]) -> AttendanceRule | None:
    return next((r for r in rules if rule_matches(r, m)), None)


def effective(m: dict[str, Any]) -> tuple[Choice, Source]:
    choice = m.get("attendance_choice")
    if choice in CHOICES:
        return choice, m.get("attendance_choice_source") or "manual"
    return "attend", "default"


async def apply_rules(
    session: AsyncSession,
    facts: list[dict[str, Any]],
    rules_by_user: dict[uuid.UUID, list[AttendanceRule]] | None = None,
) -> list[uuid.UUID]:
    """Materialize each person's rules into the open meetings in `facts` they did not choose by
    hand. Returns the meetings whose listening agents changed (to tell their rooms)."""
    rules_by_user = dict(rules_by_user or {})
    changed: list[uuid.UUID] = []
    for m in facts:
        if m["status"] not in OPEN_STATUSES or m.get("attendance_choice_source") == "manual":
            continue
        user = m["user_id"]
        if user not in rules_by_user:
            rules_by_user[user] = await load_rules(session, user)
        rule = first_match(rules_by_user[user], m)
        want_choice = rule.choice if rule else None
        want_rule = rule.id if rule else None
        if m.get("attendance_choice") == want_choice and m.get("attendance_rule_id") == want_rule:
            continue
        attendance = "agent" if want_choice == "agent_listens" else "live"
        await session.execute(
            text(
                "UPDATE meeting_participants SET attendance_choice = :c, "
                "attendance_choice_source = :s, attendance_rule_id = :r, "
                "attendance_choice_at = now(), attendance = :a "
                "WHERE meeting_id = :m AND user_id = :u "
                "AND attendance_choice_source IS DISTINCT FROM 'manual'"
            ),
            {
                "c": want_choice,
                "s": "rule" if rule else None,
                "r": str(want_rule) if want_rule else None,
                "a": attendance,
                "m": str(m["id"]),
                "u": str(user),
            },
        )
        if (m.get("attendance") == "agent") != (attendance == "agent"):
            changed.append(m["id"])
        m.update(
            attendance_choice=want_choice,
            attendance_choice_source="rule" if rule else None,
            attendance_rule_id=want_rule,
            attendance=attendance,
        )
    return list(dict.fromkeys(changed))


async def apply_rules_for_user(
    session: AsyncSession, user_id: uuid.UUID, now: datetime | None = None
) -> list[uuid.UUID]:
    now = now or datetime.now(UTC)
    facts = await meeting_facts(session, user_id=user_id, open_only=True)
    facts = [
        m
        for m in facts
        if m["scheduled_start"] is None or m["scheduled_start"] < now + RULE_HORIZON
    ]
    return await apply_rules(session, facts)


async def apply_rules_for_meeting(session: AsyncSession, meeting_id: uuid.UUID) -> bool:
    facts = await meeting_facts(session, meeting_id=meeting_id, open_only=True)
    return bool(await apply_rules(session, facts))


def duration_minutes(m: dict[str, Any]) -> int:
    start, end = m.get("scheduled_start"), m.get("scheduled_end")
    if start is None or end is None or end <= start:
        return DEFAULT_MINUTES
    return max(1, int(round((end - start).total_seconds() / 60)))


async def listening_agents(session: AsyncSession, meeting_id: uuid.UUID) -> list[uuid.UUID]:
    """The people whose agent listens to this meeting (`attendance = 'agent'`)."""
    rows = await session.execute(
        text(
            "SELECT p.user_id FROM meeting_participants p JOIN users u ON u.id = p.user_id "
            "AND u.deleted_at IS NULL WHERE p.meeting_id = :m AND p.attendance = 'agent' "
            "ORDER BY p.user_id"
        ),
        {"m": str(meeting_id)},
    )
    return [r[0] for r in rows]


# --- realtime -------------------------------------------------------------------------------


def meeting_channel(org_id: uuid.UUID | str, meeting_id: uuid.UUID | str) -> str:
    """realtime-gateway's meeting fan-out channel (`libs/keys.ts` pubsubChannel)."""
    return redis_key(org_id, "pubsub", "meeting", meeting_id)


def user_channel(org_id: uuid.UUID | str, user_id: uuid.UUID | str) -> str:
    """realtime-gateway's per-person channel: `nexus:{org_id}:pubsub:user:{user_id}`."""
    return redis_key(org_id, "pubsub", "user", user_id)


async def publish(channel: str, message: dict[str, Any]) -> None:
    try:
        await get_redis().publish(channel, json.dumps(message, default=str))
    except Exception:  # noqa: BLE001 - best effort: the durable copy is in Postgres
        logger.warning("listening broadcast failed on %s", channel, exc_info=True)


async def announce_listeners(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID
) -> None:
    """Tell a live room how many agents are listening (the "N agents listening" chip)."""
    users = await listening_agents(session, meeting_id)
    await publish(
        meeting_channel(org_id, meeting_id),
        {
            "type": "listeners",
            "meetingId": str(meeting_id),
            "payload": {"count": len(users), "userIds": [str(u) for u in users]},
        },
    )


# --- copy -----------------------------------------------------------------------------------


def clock_label(when: datetime | None, zone: ZoneInfo) -> str | None:
    """`3pm`, `3:30pm`, `11am` in the person's zone."""
    if when is None:
        return None
    local = when.astimezone(zone)
    hour = local.hour % 12 or 12
    suffix = "am" if local.hour < 12 else "pm"
    return f"{hour}{suffix}" if local.minute == 0 else f"{hour}:{local.minute:02d}{suffix}"


def interruption_title(
    headline: str, meeting_title: str | None, start: datetime | None, zone: ZoneInfo
) -> str:
    """ "Pricing for your segment is being decided in the 3pm right now." """
    head = re.sub(r"[\s.!?]+$", "", " ".join(headline.split()))
    head = head[:1].upper() + head[1:]
    clock = clock_label(start, zone)
    where = f"the {clock}" if clock else f"“{records._title(meeting_title)}”"
    return f"{head} in {where} right now."[:240]


def first_name(name: str | None) -> str:
    return (name or "").split(" ")[0] or "Your colleague"


# --- the evening digest ---------------------------------------------------------------------


class DigestSegment(BaseModel):
    text: str
    href: str | None = None


class DigestMeeting(BaseModel):
    meeting_id: uuid.UUID
    title: str
    scheduled_start: datetime | None = None
    choice: Choice
    minutes: int
    confidential: bool = False
    interruptions: int = 0
    held: int = 0


class EveningDigest(BaseModel):
    id: uuid.UUID | None = None
    day: date
    status: Literal["ready", "quiet"]
    paragraph: list[DigestSegment]
    meetings: list[DigestMeeting]
    meeting_count: int
    minutes_saved: int
    created_at: datetime | None = None


def _sentence(text_: str) -> str:
    t = " ".join(str(text_).split()).strip()
    if not t:
        return ""
    t = t[:1].upper() + t[1:]
    return t if t[-1] in ".!?" else f"{t}."


def _lower_first(text_: str) -> str:
    t = " ".join(str(text_).split()).strip().rstrip(".")
    return t[:1].lower() + t[1:] if t[:2] != t[:2].upper() else t


async def build_evening(
    session: AsyncSession, user_id: uuid.UUID, day: date, zone: ZoneInfo
) -> EveningDigest:
    """One paragraph about `day` (local to `zone`): every meeting the person's agent listened
    to for them, and the ones they skipped."""
    start = datetime.combine(day, time.min, tzinfo=zone).astimezone(UTC)
    end = start + timedelta(days=1)
    facts = [
        m
        for m in await meeting_facts(session, user_id=user_id, start=start, end=end)
        if m["status"] != "cancelled"
    ]
    listened = [
        m for m in facts if m["attendance"] == "agent" or effective(m)[0] == "agent_listens"
    ]
    skipped = [m for m in facts if effective(m)[0] == "skip" and m not in listened]
    segments: list[DigestSegment] = []
    meetings: list[DigestMeeting] = []
    if not listened and not skipped:
        return EveningDigest(
            day=day,
            status="quiet",
            paragraph=[DigestSegment(text="Your agent had nothing to listen to for you today.")],
            meetings=[],
            meeting_count=0,
            minutes_saved=0,
        )
    n = len(listened)
    if n:
        segments.append(
            DigestSegment(
                text=f"Your agent sat in on {n} meeting{'s' if n != 1 else ''} for you today. "
            )
        )
    for m in listened:
        mid = m["id"]
        title = records._title(m["title"])
        clock = clock_label(m["scheduled_start"], zone)
        label = f"{title} ({clock})" if clock else title
        stats = (
            await session.execute(
                text(
                    "SELECT count(*) FILTER (WHERE status = 'delivered'), "
                    "count(*) FILTER (WHERE status = 'held'), "
                    "(array_agg(headline ORDER BY created_at) FILTER (WHERE status = 'held'))[1], "
                    "(array_agg(topic ORDER BY created_at) "
                    " FILTER (WHERE status = 'delivered' AND topic IS NOT NULL))[1] "
                    "FROM agent_interruptions WHERE user_id = :u AND meeting_id = :m"
                ),
                {"u": str(user_id), "m": str(mid)},
            )
        ).one()
        delivered, held, held_headline, topic = stats
        meetings.append(
            DigestMeeting(
                meeting_id=mid,
                title=title,
                scheduled_start=m["scheduled_start"],
                choice="agent_listens",
                minutes=duration_minutes(m),
                confidential=bool(m["confidential"]),
                interruptions=int(delivered or 0),
                held=int(held or 0),
            )
        )
        segments.append(DigestSegment(text="In "))
        segments.append(DigestSegment(text=label, href=f"/meetings/{mid}"))
        if m["confidential"]:
            segments.append(
                DigestSegment(text=", a confidential meeting, its notes stay on the meeting page. ")
            )
            continue
        if m["status"] in OPEN_STATUSES:
            segments.append(DigestSegment(text=", still going when this was written. "))
        summary = await _minutes_summary(session, mid, user_id)
        segments.append(
            DigestSegment(
                text=f", {_lower_first(summary)}. " if summary else ", nothing was settled. "
            )
        )
        decisions = (
            (
                await session.execute(
                    text(
                        "SELECT id, content, quote_start_ms FROM decisions WHERE meeting_id = :m "
                        "AND deleted_at IS NULL AND status = 'confirmed' "
                        "ORDER BY created_at LIMIT 2"
                    ),
                    {"m": str(mid)},
                )
            )
            .mappings()
            .all()
        )
        for d in decisions:
            href = (
                f"/meetings/{mid}/transcript#t={int(d['quote_start_ms']) // 1000}"
                if d["quote_start_ms"] is not None
                else f"/meetings/{mid}/decisions"
            )
            segments.append(DigestSegment(text="Decided: "))
            segments.append(DigestSegment(text=_sentence(d["content"]).rstrip("."), href=href))
            segments.append(DigestSegment(text=". "))
        if delivered:
            about = f", about {topic}" if topic else ""
            segments.append(
                DigestSegment(
                    text=(
                        f"It pulled you in {'once' if delivered == 1 else f'{delivered} times'}"
                        f"{about}. "
                    )
                )
            )
        if held:
            what = f": {_lower_first(held_headline)}" if held_headline else ""
            segments.append(
                DigestSegment(
                    text=(
                        f"It held {'one thing' if held == 1 else f'{held} things'} back while "
                        f"you were not to be disturbed{what}. "
                    )
                )
            )
    if skipped:
        names = [records._title(m["title"]) for m in skipped]
        for m in skipped:
            meetings.append(
                DigestMeeting(
                    meeting_id=m["id"],
                    title=records._title(m["title"]),
                    scheduled_start=m["scheduled_start"],
                    choice="skip",
                    minutes=duration_minutes(m),
                    confidential=bool(m["confidential"]),
                )
            )
        listed = ", ".join(names[:-1]) + (" and " if len(names) > 1 else "") + names[-1]
        segments.append(
            DigestSegment(
                text=(
                    f"You also skipped {listed}; nobody listened for you there, and the shared "
                    "minutes are on each meeting's page."
                )
            )
        )
    saved = sum(x.minutes for x in meetings)
    paragraph = [s for s in segments if s.text]
    if paragraph:
        paragraph[-1] = DigestSegment(text=paragraph[-1].text.rstrip(), href=paragraph[-1].href)
    return EveningDigest(
        day=day,
        status="ready",
        paragraph=paragraph,
        meetings=meetings,
        meeting_count=len(meetings),
        minutes_saved=saved,
    )


async def _minutes_summary(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> str | None:
    """The person's own minutes' summary, else the shared minutes'."""
    rows = (
        await session.execute(
            text(
                "SELECT user_id, content FROM meeting_minutes WHERE meeting_id = :m "
                "AND status = 'ready' AND (user_id = :u OR user_id IS NULL) "
                "ORDER BY user_id NULLS LAST"
            ),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).all()
    for _uid, content in rows:
        body = content if isinstance(content, dict) else json.loads(content)
        summary = " ".join(str(body.get("summary") or "").split())
        if summary:
            return summary[:400]
    return None
