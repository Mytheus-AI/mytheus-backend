"""The capacity check at the moment of commitment (promise ledger, Layer 2).

A per-person model of a day, built from what core-api already knows:

- what is due that day: the person's open, confirmed promises (`commitments.due_at`) and their
  open (`confirmed`) actions (`action_items.due_date`);
- the meetings they are in that day (`meetings` they take part in or created; Google Calendar
  events arrive here as meetings once the calendar integration is connected), split into meeting
  time and focus time by `meetings.calendar_event_kind` (`focusTime` is focus, `outOfOffice`
  marks the day away).

The arithmetic is deliberately plain so the warning can say why. A workday has
`WORKDAY_MINUTES`; each thing due costs a nominal `ITEM_MINUTES`; meetings take their length;
focus time is time to do the work, so it costs nothing. A day is:

- `over` when the load passes the day, or `OVER_ITEMS` or more things are due, or it is a
  weekend or an away day with anything due;
- `tight` when less than `TIGHT_FREE_MINUTES` is left, or `TIGHT_ITEMS` or more are due;
- `ok` otherwise (an empty weekend is `off`).

Private to the person: every read here is the caller's own (`user_id` predicate under RLS), and
nothing ranks or compares people. When an action is assigned to someone else, the assigner
never sees the assignee's load; the assignee's own agent warns them when it lands.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.promise_dates import day_label, ordinal

WORKDAY_MINUTES = 8 * 60
ITEM_MINUTES = 60
TIGHT_FREE_MINUTES = 120
TIGHT_ITEMS = 4
OVER_ITEMS = 6
DEFAULT_MEETING_MINUTES = 30
SUGGEST_WITHIN_DAYS = 14
MAX_RANGE_DAYS = 31

Level = Literal["ok", "tight", "over", "off"]
TargetKind = Literal["commitment", "action"]


def resolve_tz(raw: str | None) -> str:
    """An IANA zone name the browser sent, or UTC. Never trusted beyond being a valid zone."""
    if raw:
        try:
            ZoneInfo(raw)
            return raw
        except (ZoneInfoNotFoundError, ValueError):
            return "UTC"
    return "UTC"


def today_in(tz: str) -> date:
    return datetime.now(ZoneInfo(tz)).date()


@dataclass
class DayLoad:
    day: date
    promises: int = 0
    actions: int = 0
    meeting_minutes: int = 0
    focus_minutes: int = 0
    away: bool = False

    @property
    def items(self) -> int:
        return self.promises + self.actions

    @property
    def weekend(self) -> bool:
        return self.day.weekday() >= 5

    @property
    def capacity_minutes(self) -> int:
        return 0 if (self.weekend or self.away) else WORKDAY_MINUTES

    def load_minutes(self, extra_items: int = 0) -> int:
        return (self.items + extra_items) * ITEM_MINUTES + self.meeting_minutes

    def level(self, extra_items: int = 0) -> Level:
        items = self.items + extra_items
        if self.capacity_minutes == 0:
            return "over" if items else "off"
        free = self.capacity_minutes - self.load_minutes(extra_items)
        if free < 0 or items >= OVER_ITEMS:
            return "over"
        if free < TIGHT_FREE_MINUTES or items >= TIGHT_ITEMS:
            return "tight"
        return "ok"


def _excluded(exclude: tuple[TargetKind, uuid.UUID] | None, kind: TargetKind) -> str | None:
    return str(exclude[1]) if exclude and exclude[0] == kind else None


async def day_loads(
    session: AsyncSession,
    user_id: uuid.UUID,
    start: date,
    days: int,
    tz: str,
    exclude: tuple[TargetKind, uuid.UUID] | None = None,
) -> list[DayLoad]:
    """One `DayLoad` per day from `start`, in the person's zone. `exclude` leaves one promise or
    action out (the one being checked, already stored)."""
    days = max(1, min(days, MAX_RANGE_DAYS))
    end = start + timedelta(days=days)
    loads = {start + timedelta(days=i): DayLoad(start + timedelta(days=i)) for i in range(days)}
    params: dict[str, Any] = {"u": str(user_id), "s": start, "e": end}
    for kind, sql in (
        (
            "commitment",
            "SELECT due_at AS d, count(*) AS n FROM commitments WHERE user_id = :u "
            "AND status = 'open' AND review_state = 'confirmed' AND due_at >= :s AND due_at < :e "
            "AND (CAST(:x AS uuid) IS NULL OR id <> CAST(:x AS uuid)) GROUP BY due_at",
        ),
        (
            "action",
            "SELECT due_date AS d, count(*) AS n FROM action_items WHERE assignee_id = :u "
            "AND status = 'confirmed' AND deleted_at IS NULL AND due_date >= :s "
            "AND due_date < :e AND (CAST(:x AS uuid) IS NULL OR id <> CAST(:x AS uuid)) "
            "GROUP BY due_date",
        ),
    ):
        rows = await session.execute(
            text(sql), {**params, "x": _excluded(exclude, kind)}  # type: ignore[arg-type]
        )
        for d, n in rows:
            if d in loads:
                if kind == "commitment":
                    loads[d].promises = int(n)
                else:
                    loads[d].actions = int(n)
    meetings = await session.execute(
        text(
            "SELECT (m.scheduled_start AT TIME ZONE :tz)::date AS d, m.calendar_event_kind AS k, "
            "COALESCE(EXTRACT(EPOCH FROM (m.scheduled_end - m.scheduled_start)) / 60, :dm) AS mins "
            "FROM meetings m WHERE m.deleted_at IS NULL AND m.status <> 'cancelled' "
            "AND m.scheduled_start IS NOT NULL "
            "AND (m.scheduled_start AT TIME ZONE :tz)::date >= :s "
            "AND (m.scheduled_start AT TIME ZONE :tz)::date < :e "
            "AND (m.created_by = :u OR EXISTS (SELECT 1 FROM meeting_participants p "
            "  WHERE p.meeting_id = m.id AND p.user_id = :u))"
        ),
        {**params, "tz": tz, "dm": DEFAULT_MEETING_MINUTES},
    )
    for d, kind, mins in meetings:
        load = loads.get(d)
        if load is None:
            continue
        minutes = int(min(float(mins or 0), 24 * 60))
        if kind == "focusTime":
            load.focus_minutes += minutes
        elif kind == "outOfOffice":
            load.away = True
        elif kind != "workingLocation":
            load.meeting_minutes += minutes
    return [loads[d] for d in sorted(loads)]


async def calendar_synced(session: AsyncSession, user_id: uuid.UUID) -> bool:
    """True when this person's calendar reaches Mytheus: a meeting of theirs synced from Google
    Calendar in the last 30 days or ahead. Without it, only Mytheus meetings count."""
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM meetings m WHERE m.calendar_event_id IS NOT NULL "
                "AND m.deleted_at IS NULL AND m.scheduled_start >= now() - interval '30 days' "
                "AND (m.created_by = :u OR EXISTS (SELECT 1 FROM meeting_participants p "
                "  WHERE p.meeting_id = m.id AND p.user_id = :u)) LIMIT 1"
            ),
            {"u": str(user_id)},
        )
    ).first()
    return row is not None


async def counts_outside(session: AsyncSession, user_id: uuid.UUID, today: date) -> tuple[int, int]:
    """(overdue, undated) open things the person owes, promises and actions together."""
    row = (
        await session.execute(
            text(
                "SELECT "
                "(SELECT count(*) FROM commitments WHERE user_id = :u AND status = 'open' "
                " AND review_state = 'confirmed' AND due_at < :t) + "
                "(SELECT count(*) FROM action_items WHERE assignee_id = :u "
                " AND status = 'confirmed' AND deleted_at IS NULL AND due_date < :t), "
                "(SELECT count(*) FROM commitments WHERE user_id = :u AND status = 'open' "
                " AND review_state = 'confirmed' AND due_at IS NULL) + "
                "(SELECT count(*) FROM action_items WHERE assignee_id = :u "
                " AND status = 'confirmed' AND deleted_at IS NULL AND due_date IS NULL)"
            ),
            {"u": str(user_id), "t": today},
        )
    ).one()
    return int(row[0]), int(row[1])


@dataclass
class Check:
    due: date
    level: Level
    items_on_day: int  # including the one being checked
    meeting_minutes: int
    warn: bool
    message: str
    suggested: date | None
    suggested_label: str | None
    due_label: str


def _hours(minutes: int) -> str:
    hours = round(minutes / 60 * 2) / 2
    text_ = f"{hours:g}"
    return f"{text_} hour" if hours == 1 else f"{text_} hours"


def compose(load: DayLoad, today: date, suggested: date | None) -> tuple[bool, str]:
    """The agent's sentence for taking one more thing on `load.day`."""
    lvl = load.level(1)
    label = day_label(load.day, today)
    on = label if label in ("today", "tomorrow") else f"on {label}"
    due_on = label
    n = load.items + 1
    if lvl in ("ok", "off"):
        if n == 1:
            return False, f"Nothing else is due {on}."
        return False, f"That makes {n} things due {on}; your day still has room."
    cap = label[:1].upper() + label[1:]
    if load.capacity_minutes == 0:
        reason = f"You are away {on}." if load.away else f"{cap} is a weekend day."
    elif n >= TIGHT_ITEMS:
        reason = f"That's your {ordinal(n)} promise due {due_on}."
    else:
        reason = f"{cap} already has {_hours(load.meeting_minutes)} of meetings."
    if suggested is not None:
        return True, f"{reason} Offer {day_label(suggested, today)} instead?"
    return True, f"{reason} The next two weeks are full too."


async def check(
    session: AsyncSession,
    user_id: uuid.UUID,
    due: date,
    tz: str,
    exclude: tuple[TargetKind, uuid.UUID] | None = None,
) -> Check:
    """What taking one more thing due on `due` does to the person's day, and the first workday
    after it with room (within `SUGGEST_WITHIN_DAYS`)."""
    today = today_in(tz)
    loads = await day_loads(session, user_id, due, SUGGEST_WITHIN_DAYS + 1, tz, exclude)
    first = loads[0]
    suggested: date | None = None
    if first.level(1) in ("tight", "over"):
        for load in loads[1:]:
            if load.day < today or load.weekend:
                continue
            if load.level(1) == "ok":
                suggested = load.day
                break
    warn, message = compose(first, today, suggested)
    return Check(
        due=due,
        level=first.level(1),
        items_on_day=first.items + 1,
        meeting_minutes=first.meeting_minutes,
        warn=warn,
        message=message,
        suggested=suggested,
        suggested_label=day_label(suggested, today) if suggested else None,
        due_label=day_label(due, today),
    )


async def warn_on_landing(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    user_id: uuid.UUID,
    kind: TargetKind,
    item_id: uuid.UUID,
    content: str,
    due: date | None,
    tz: str = "UTC",
) -> bool:
    """A promise or action just landed on `user_id` with a due date: if it makes that day tight,
    their agent says so, privately, with one tap to offer another date (the link opens the
    renegotiate sheet prefilled). At most one warning per (item, due date): the notification
    key carries both. Returns whether a warning was queued."""
    if due is None or due < today_in(tz):
        return False
    result = await check(session, user_id, due, tz, exclude=(kind, item_id))
    if not result.warn:
        return False
    short = " ".join(content.split())
    short = short if len(short) <= 140 else short[:137].rstrip() + "..."
    source = f"capacity:{kind}:{item_id}:{due.isoformat()}"
    link = f"/work/promises?offer={kind}:{item_id}"
    if result.suggested is not None:
        link += f"&date={result.suggested.isoformat()}"
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer="core-api",
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": 2,
                "title": result.message[:200],
                "body": f"{short}\n\nOnly you see this. Your agent can offer the new date for you."[
                    :2000
                ],
                "link": link,
            },
        ),
    )
    return True
