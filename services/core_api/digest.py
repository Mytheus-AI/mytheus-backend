"""The weekly "what changed for you" digest, built from the decision graph (polish wave 3).

One digest per person per ISO week (Monday to Sunday, UTC). What it holds, all scoped to the
meetings the person took part in (the decision log's participant rule, ADR-0033 D1):

- `decisions`: decisions confirmed that week (not replacements, see below);
- `decision_changes`: decisions confirmed that week that superseded an earlier one
  (`supersedes`, the `FOLLOWS_FROM` lineage), old -> new;
- `actions_assigned`: the person's actions confirmed or created that week;
- `actions_closed`: the person's actions marked done that week;
- `promises_due`: the person's open actions due in the next seven days, and their open
  promises (ADR-0039 B6 commitments) that carry a due hint.

Listen-for topics (`users.listen_for`) filter the two decision sections: with topics set, only
decisions whose text names one of them are kept (each carries `matched_topics`). A change that
replaced a decision one of the person's own open actions hangs on is always kept, topics or not.
With no topics, nothing is filtered. Actions and promises are the person's own and are never
filtered.

`status_changed_at` (migration b4c5d6e7f8a9, set by trigger) dates confirmations and
completions; rows from before it fall back to `created_at`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

MAX_PER_SECTION = 25


class WeeklyDigestDecision(BaseModel):
    id: uuid.UUID
    content: str
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    decided_at: datetime
    matched_topics: list[str] = Field(default_factory=list)


class WeeklyDigestChange(BaseModel):
    old_decision_id: uuid.UUID
    old_content: str
    new_decision_id: uuid.UUID
    new_content: str
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    changed_at: datetime
    matched_topics: list[str] = Field(default_factory=list)


class WeeklyDigestAction(BaseModel):
    id: uuid.UUID
    content: str
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    due_date: date | None = None
    status: str
    changed_at: datetime | None = None


class WeeklyDigestPromise(BaseModel):
    kind: Literal["action", "commitment"]
    id: uuid.UUID
    content: str
    due_date: date | None = None
    due_hint: str | None = None
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None


class WeeklyDigest(BaseModel):
    id: uuid.UUID | None = None
    week_start: date
    week_end: date
    created_at: datetime | None = None
    topics: list[str] = Field(default_factory=list)
    item_count: int = 0
    decisions: list[WeeklyDigestDecision] = Field(default_factory=list)
    decision_changes: list[WeeklyDigestChange] = Field(default_factory=list)
    actions_assigned: list[WeeklyDigestAction] = Field(default_factory=list)
    actions_closed: list[WeeklyDigestAction] = Field(default_factory=list)
    promises_due: list[WeeklyDigestPromise] = Field(default_factory=list)


def week_start_of(day: date) -> date:
    return day - timedelta(days=day.weekday())


def previous_week_start(today: date) -> date:
    return week_start_of(today) - timedelta(days=7)


def next_run_at(now: datetime) -> datetime:
    """The next Monday 07:00 UTC (the `weekly-digest` schedule)."""
    monday = week_start_of(now.date())
    run = datetime.combine(monday, time(7, 0), tzinfo=UTC)
    return run if run > now else run + timedelta(days=7)


def matched(content: str, topics: list[str]) -> list[str]:
    low = content.lower()
    return [t for t in topics if t.lower() in low]


def clean_topics(raw: Any) -> list[str]:
    out: list[str] = []
    for t in raw or []:
        value = " ".join(str(t).split())
        if value and value.lower() not in {o.lower() for o in out}:
            out.append(value)
    return out[:20]


_MINE = (
    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = d.meeting_id "
    "AND p.user_id = :u)"
)
_DECIDED_AT = "COALESCE(d.status_changed_at, d.created_at)"


async def build_digest(
    session: AsyncSession, user_id: uuid.UUID, week_start: date, *, until: datetime | None = None
) -> WeeklyDigest:
    """The digest for `week_start`'s week (or up to `until`, for this week's preview). RLS is
    already set on `session`."""
    start = datetime.combine(week_start, time(0, 0), tzinfo=UTC)
    end = start + timedelta(days=7)
    if until is not None and until < end:
        end = until
    window = {"u": str(user_id), "s": start, "e": end, "lim": MAX_PER_SECTION * 4}
    topics = clean_topics(
        (
            await session.execute(
                text("SELECT listen_for FROM users WHERE id = :u"), {"u": str(user_id)}
            )
        ).scalar()
    )

    decision_rows = (
        (
            await session.execute(
                text(
                    f"SELECT d.id, d.content, d.meeting_id, m.title AS meeting_title, "
                    f"{_DECIDED_AT} AS decided_at FROM decisions d "
                    "JOIN meetings m ON m.id = d.meeting_id AND m.deleted_at IS NULL "
                    "WHERE d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL "
                    f"AND d.supersedes IS NULL AND {_DECIDED_AT} >= :s AND {_DECIDED_AT} < :e "
                    f"AND {_MINE} ORDER BY decided_at DESC, d.id LIMIT :lim"
                ),
                window,
            )
        )
        .mappings()
        .all()
    )
    decisions: list[WeeklyDigestDecision] = []
    for r in decision_rows:
        hits = matched(r["content"], topics)
        if topics and not hits:
            continue
        decisions.append(WeeklyDigestDecision(**dict(r), matched_topics=hits))

    change_rows = (
        (
            await session.execute(
                text(
                    f"SELECT o.id AS old_decision_id, o.content AS old_content, "
                    f"d.id AS new_decision_id, d.content AS new_content, d.meeting_id, "
                    f"m.title AS meeting_title, {_DECIDED_AT} AS changed_at, "
                    "EXISTS (SELECT 1 FROM action_items a WHERE a.meeting_id = o.meeting_id "
                    " AND a.assignee_id = :u AND a.status = 'confirmed' "
                    " AND a.deleted_at IS NULL) AS touches_mine "
                    "FROM decisions d JOIN decisions o ON o.id = d.supersedes "
                    "JOIN meetings m ON m.id = d.meeting_id AND m.deleted_at IS NULL "
                    "WHERE d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL "
                    f"AND {_DECIDED_AT} >= :s AND {_DECIDED_AT} < :e AND {_MINE} "
                    "ORDER BY changed_at DESC, d.id LIMIT :lim"
                ),
                window,
            )
        )
        .mappings()
        .all()
    )
    changes: list[WeeklyDigestChange] = []
    for r in change_rows:
        row = dict(r)
        touches_mine = row.pop("touches_mine")
        hits = sorted(
            set(matched(row["old_content"], topics) + matched(row["new_content"], topics))
        )
        if topics and not hits and not touches_mine:
            continue
        changes.append(WeeklyDigestChange(**row, matched_topics=hits))

    _ACTION_COLS = (
        "a.id, a.content, a.meeting_id, m.title AS meeting_title, a.due_date, a.status, "
        "COALESCE(a.status_changed_at, a.created_at) AS changed_at "
        "FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id "
    )
    assigned = (
        (
            await session.execute(
                text(
                    f"SELECT {_ACTION_COLS}"
                    "WHERE a.assignee_id = :u AND a.deleted_at IS NULL "
                    "AND a.status IN ('confirmed', 'done') AND ("
                    " (a.status = 'confirmed' AND a.status_changed_at >= :s "
                    "  AND a.status_changed_at < :e) "
                    " OR (a.created_at >= :s AND a.created_at < :e)) "
                    "ORDER BY a.created_at DESC, a.id LIMIT :lim"
                ),
                window,
            )
        )
        .mappings()
        .all()
    )
    closed = (
        (
            await session.execute(
                text(
                    f"SELECT {_ACTION_COLS}"
                    "WHERE a.assignee_id = :u AND a.deleted_at IS NULL AND a.status = 'done' "
                    "AND a.status_changed_at >= :s AND a.status_changed_at < :e "
                    "ORDER BY a.status_changed_at DESC, a.id LIMIT :lim"
                ),
                window,
            )
        )
        .mappings()
        .all()
    )
    horizon_from = end.date() if until is None else until.date()
    due_rows = (
        (
            await session.execute(
                text(
                    "SELECT a.id, a.content, a.due_date, a.meeting_id, m.title AS meeting_title "
                    "FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id "
                    "WHERE a.assignee_id = :u AND a.deleted_at IS NULL AND a.status = 'confirmed' "
                    "AND a.due_date IS NOT NULL AND a.due_date < :h "
                    "ORDER BY a.due_date, a.id LIMIT :lim"
                ),
                {"u": str(user_id), "h": horizon_from + timedelta(days=7), "lim": MAX_PER_SECTION},
            )
        )
        .mappings()
        .all()
    )
    commitment_rows = (
        (
            await session.execute(
                text(
                    "SELECT c.id, c.content, c.due_hint, c.due_at AS due_date, "
                    "c.meeting_id, m.title AS meeting_title "
                    "FROM commitments c LEFT JOIN meetings m ON m.id = c.meeting_id "
                    "WHERE c.user_id = :u AND c.status = 'open' AND c.review_state = 'confirmed' "
                    "AND (c.due_hint IS NOT NULL OR c.due_at IS NOT NULL) "
                    "AND c.created_at >= :since ORDER BY c.created_at DESC, c.id LIMIT 10"
                ),
                {"u": str(user_id), "since": start - timedelta(days=21)},
            )
        )
        .mappings()
        .all()
    )
    promises = [WeeklyDigestPromise(kind="action", **dict(r)) for r in due_rows] + [
        WeeklyDigestPromise(kind="commitment", **dict(r)) for r in commitment_rows
    ]

    digest = WeeklyDigest(
        week_start=week_start,
        week_end=week_start + timedelta(days=6),
        topics=topics,
        decisions=decisions[:MAX_PER_SECTION],
        decision_changes=changes[:MAX_PER_SECTION],
        actions_assigned=[WeeklyDigestAction(**dict(r)) for r in assigned][:MAX_PER_SECTION],
        actions_closed=[WeeklyDigestAction(**dict(r)) for r in closed][:MAX_PER_SECTION],
        promises_due=promises[:MAX_PER_SECTION],
    )
    digest.item_count = (
        len(digest.decisions)
        + len(digest.decision_changes)
        + len(digest.actions_assigned)
        + len(digest.actions_closed)
        + len(digest.promises_due)
    )
    return digest


def render_email(digest: WeeklyDigest, base_url: str) -> tuple[str, str, str]:
    """(subject, text, html) for the digest email. Plain and short; the page has the rest."""
    from html import escape

    def when(d: date) -> str:
        return f"{d.day} {d.strftime('%b')}"

    subject = f"What changed for you, week of {when(digest.week_start)}"
    lines: list[str] = [subject, ""]
    blocks: list[str] = []

    def section(title: str, items: list[str]) -> None:
        if not items:
            return
        lines.append(title)
        lines.extend(f"- {i}" for i in items)
        lines.append("")
        lis = "".join(f"<li>{escape(i)}</li>" for i in items)
        blocks.append(f'<h3 style="margin:20px 0 6px">{escape(title)}</h3><ul>{lis}</ul>')

    section("Decisions", [d.content for d in digest.decisions])
    section(
        "Decisions that changed",
        [f"{c.old_content} -> {c.new_content}" for c in digest.decision_changes],
    )
    section("Assigned to you", [a.content for a in digest.actions_assigned])
    section("Closed", [a.content for a in digest.actions_closed])
    section(
        "Promises due",
        [
            p.content
            + (f" (due {when(p.due_date)})" if p.due_date else "")
            + (f" ({p.due_hint})" if p.due_hint and not p.due_date else "")
            for p in digest.promises_due
        ],
    )
    link = f"{base_url.rstrip('/')}/digest"
    lines.append(f"Open your digest: {link}")
    if digest.topics:
        lines.append(f"Filtered by your topics: {', '.join(digest.topics)}")
    html = (
        '<div style="font-family:system-ui,sans-serif;font-size:15px;line-height:1.5;'
        'color:#1f1e1d;max-width:640px">'
        f'<h2 style="margin:0 0 4px">{escape(subject)}</h2>'
        + "".join(blocks)
        + f'<p style="margin-top:24px"><a href="{escape(link)}">Open your digest</a></p>'
        + "</div>"
    )
    return subject, "\n".join(lines), html
