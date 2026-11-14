"""When an Ask Myte routine next runs (ADR-0049 F5). Pure: no database, no clock of its own.

A routine runs at `hour:00` on its days, in its own IANA timezone:

- `daily`: every day;
- `weekdays`: Monday to Friday;
- `weekly`: one `weekday` (0 Monday to 6 Sunday).

`next_run(...)` is the first such moment strictly after `after`, as an aware UTC datetime. The
wall clock is what the person chose, so it follows daylight saving: "9:00 in Europe/London" is
08:00 UTC in summer and 09:00 UTC in winter. Two edge cases, both settled by `zoneinfo`'s
`fold=0` reading of a local time:

- an hour that does not exist that day (clocks jump forward over it, e.g. 02:00 on the US spring
  change) runs at the same instant the skipped wall time would have been on the old offset, which
  reads as the hour after the gap (03:00);
- an hour that happens twice (clocks fall back over it) runs once, at its first occurrence.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

Cadence = Literal["daily", "weekdays", "weekly"]

# A weekly routine recurs within 7 days; one more covers a slot earlier today that has passed.
_HORIZON_DAYS = 8


def zone(name: str) -> ZoneInfo:
    """The IANA zone `name`, or ValueError. Only real zone names: no paths, no POSIX TZ strings."""
    if not name or len(name) > 64 or name.startswith(("/", ".")) or ".." in name:
        raise ValueError("timezone must be an IANA name such as Europe/London")
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError) as exc:
        raise ValueError("timezone must be an IANA name such as Europe/London") from exc


def runs_on(cadence: Cadence, weekday: int | None, day: date) -> bool:
    if cadence == "daily":
        return True
    if cadence == "weekdays":
        return day.weekday() < 5
    return day.weekday() == weekday


def next_run(
    cadence: Cadence,
    hour: int,
    timezone: str,
    after: datetime,
    weekday: int | None = None,
) -> datetime:
    """The first `hour:00` on a day of `cadence` in `timezone` strictly after `after` (UTC)."""
    if after.tzinfo is None:
        raise ValueError("after must be timezone-aware")
    if not 0 <= hour <= 23:
        raise ValueError("hour must be 0 to 23")
    if cadence == "weekly" and (weekday is None or not 0 <= weekday <= 6):
        raise ValueError("a weekly routine needs a weekday 0 to 6")
    tz = zone(timezone)
    start = after.astimezone(tz).date()
    for offset in range(_HORIZON_DAYS):
        day = start + timedelta(days=offset)
        if not runs_on(cadence, weekday, day):
            continue
        at = datetime.combine(day, time(hour), tzinfo=tz).astimezone(UTC)
        if at > after:
            return at
    raise AssertionError("unreachable: every cadence recurs within a week")  # pragma: no cover
