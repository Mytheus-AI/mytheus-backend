"""Dates for the promise ledger: a due hint as said ("by Friday", "end of month") to a date, and
the words the capacity check uses ("Friday", "tomorrow", "seventh").

Deterministic and conservative on purpose. A hint it cannot read stays a hint (`None` here), and
the person sets the date; a wrong date would put a promise in the wrong day of the capacity bar,
which is worse than no date. The intelligence tier resolves dates for the messages it extracts
(it sees the whole sentence); this parser covers the meeting promises, whose PersonalListener
payload carries only the hint.
"""

from __future__ import annotations

import calendar
import re
from datetime import date, timedelta

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_WEEKDAY_ABBR = {name[:3]: i for i, name in enumerate(WEEKDAYS)} | {
    "tue": 1,
    "tues": 1,
    "wed": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
}
_MONTHS = {name.lower(): i for i, name in enumerate(calendar.month_name) if name} | {
    name.lower(): i for i, name in enumerate(calendar.month_abbr) if name
}
_MONTHS["sept"] = 9

_ORDINALS = (
    "first",
    "second",
    "third",
    "fourth",
    "fifth",
    "sixth",
    "seventh",
    "eighth",
    "ninth",
    "tenth",
    "eleventh",
    "twelfth",
)
_NUMBER_WORDS = {
    "a": 1,
    "an": 1,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "ten": 10,
    "couple of": 2,
    "few": 3,
}


def ordinal(n: int) -> str:
    """1 -> `first`, 7 -> `seventh`, 23 -> `23rd`."""
    if 1 <= n <= len(_ORDINALS):
        return _ORDINALS[n - 1]
    suffix = "th" if 10 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def short_date(d: date) -> str:
    """`Fri 21 Nov`."""
    return f"{d.strftime('%a')} {d.day} {d.strftime('%b')}"


def day_label(d: date, today: date) -> str:
    """How a person says the day: `today`, `tomorrow`, `Friday` within the coming week,
    `Fri 21 Nov` otherwise (and for anything in the past)."""
    delta = (d - today).days
    if delta == 0:
        return "today"
    if delta == 1:
        return "tomorrow"
    if 1 < delta < 7:
        return WEEKDAYS[d.weekday()].capitalize()
    return short_date(d)


def next_weekday(d: date) -> date:
    """`d` itself on a weekday, else the Monday after."""
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def _upcoming(ref: date, weekday: int) -> date:
    """The next `weekday` on or after `ref` (said on a Friday, "Friday" means today)."""
    return ref + timedelta(days=(weekday - ref.weekday()) % 7)


def _end_of_week(ref: date) -> date:
    friday = ref + timedelta(days=4 - ref.weekday())
    return friday if friday >= ref else ref


def _clean(hint: str) -> str:
    text = hint.lower().strip()
    text = re.sub(r"[,.!?;]", " ", text)
    text = re.sub(
        r"^(?:by|before|on|until|till|due|for|no later than|at the latest by|latest)\s+", "", text
    )
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"^(?:the)\s+", "", text)


def _month_day(text: str, ref: date) -> date | None:
    """`nov 20`, `20 november`, `november 20th`, `20th of nov`: the next such date on or after
    `ref` (this year, or next year when it has passed)."""
    m = re.fullmatch(r"([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s+(\d{4}))?", text) or None
    month_word: str | None = None
    day_s: str | None = None
    year_s: str | None = None
    if m:
        month_word, day_s, year_s = m.group(1), m.group(2), m.group(3)
    else:
        m = re.fullmatch(r"(\d{1,2})(?:st|nd|rd|th)?(?:\s+of)?\s+([a-z]+)(?:\s+(\d{4}))?", text)
        if m:
            day_s, month_word, year_s = m.group(1), m.group(2), m.group(3)
    if not month_word or not day_s or month_word not in _MONTHS:
        return None
    month, day = _MONTHS[month_word], int(day_s)
    year = int(year_s) if year_s else ref.year
    try:
        found = date(year, month, day)
    except ValueError:
        return None
    if not year_s and found < ref:
        try:
            found = date(year + 1, month, day)
        except ValueError:
            return None
    return found


def parse_due_hint(hint: str | None, ref: date) -> date | None:
    """The date a due hint names, said on `ref`; None when it names none we can be sure of."""
    if not hint:
        return None
    text = _clean(hint)
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        try:
            return date.fromisoformat(text)
        except ValueError:
            return None
    if text in (
        "today",
        "tonight",
        "end of day",
        "end of today",
        "eod",
        "close of business",
        "cob",
        "this afternoon",
        "this evening",
        "this morning",
        "later today",
    ):
        return ref
    if text in (
        "tomorrow",
        "tomorrow morning",
        "tomorrow afternoon",
        "end of tomorrow",
        "tomorrow eod",
    ):
        return ref + timedelta(days=1)
    if text in (
        "end of week",
        "end of this week",
        "end of the week",
        "this week",
        "eow",
        "the end of the week",
    ):
        return _end_of_week(ref)
    if text in ("next week", "early next week", "start of next week", "beginning of next week"):
        return _upcoming(ref + timedelta(days=1), 0)
    if text in ("end of next week", "late next week", "by the end of next week"):
        return _end_of_week(ref) + timedelta(days=7)
    if text in (
        "end of month",
        "end of the month",
        "end of this month",
        "this month",
        "eom",
        "month end",
        "month-end",
    ):
        return date(ref.year, ref.month, calendar.monthrange(ref.year, ref.month)[1])
    if text in ("next month", "end of next month"):
        year, month = (ref.year + 1, 1) if ref.month == 12 else (ref.year, ref.month + 1)
        if text == "next month":
            return date(year, month, 1)
        return date(year, month, calendar.monthrange(year, month)[1])
    m = re.fullmatch(r"(?:in\s+)?(\d+|[a-z]+(?: of)?)\s+(day|days|week|weeks)", text)
    if m:
        raw = m.group(1)
        n = int(raw) if raw.isdigit() else _NUMBER_WORDS.get(raw)
        if n is not None and 0 < n <= 60:
            return ref + timedelta(days=n * (7 if m.group(2).startswith("week") else 1))
    m = re.fullmatch(
        r"(this|next|coming)?\s*([a-z]+)(?:\s+(?:morning|afternoon|evening|eod))?", text
    )
    if m:
        word = m.group(2)
        weekday = WEEKDAYS.index(word) if word in WEEKDAYS else _WEEKDAY_ABBR.get(word)
        if weekday is not None:
            upcoming = _upcoming(ref, weekday)
            if m.group(1) == "next":
                # "next Friday": the Friday of next week (Monday-start weeks).
                monday_next = ref + timedelta(days=7 - ref.weekday())
                return monday_next + timedelta(days=weekday)
            return upcoming
    return _month_day(text, ref)
