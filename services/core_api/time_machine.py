"""The company time machine: the decision graph as it stood on any date, as one person may see it.

"Beyond the meeting", Layer 3. Three reads, all participant-scoped like the decision log:

- `snapshot(as_of)`: what was decided and in force on that date (with why, the words that
  decided it, what it replaced and the bet it carried), what had been replaced by then, the
  open promises and their owners, the goals, and the expectations still being waited on.
- `diff(from, to)`: what changed between two dates: new decisions, reversed ones, promises
  made, kept and broken, bets resolved, goals that moved, and the meetings that caused each.
- `timeline(topic)`: one topic's decisions in order, each step citing its meeting and quote.

How the past is reconstructed. A decision is "in force" on T when it was confirmed on or before
T and no decision replacing it (`supersedes`) had been confirmed by T. The confirm time is the
first `confirmed` row in `record_status_history` (the append-only trigger table), falling back
to `status_changed_at` and then `created_at` for rows older than the history. Actions are
read the same way: their status on T is the last history row at or before T. Promises
(`commitments`) are private: only the viewer's own appear. Goals (`org_goals`) carry no history
of their own: a goal created by T counts, with its current status when that status was set on
or before T (`updated_at`), else as active.

What the viewer sees. Only decisions, actions and bets from meetings they were in. A decision
from a meeting they were not in that REPLACED one they can see keeps its place in the story
with its content withheld ("replaced by a decision from a meeting you were not in"), the
rule `records.py` applies to lineage. Nothing here writes.
"""

from __future__ import annotations

import re
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.core_api.records import display_name

MAX_DECISIONS = 2000
MAX_ACTIONS = 3000
MAX_LIST = 200
_WORD = re.compile(r"[a-z0-9]+")
# Words that name the shape of a question rather than its subject ("our pricing strategy").
_TOPIC_STOP = frozenset(
    "the a an and or of to in on for by with from our we us how did does do what why when "
    "change changed changes strategy strategies plan plans policy approach decision decisions "
    "between about over".split()
)


def end_of(d: date) -> datetime:
    return datetime.combine(d, time.max, tzinfo=UTC)


# --- topic matching --------------------------------------------------------------------------


def topic_stems(topic: str | None) -> list[str]:
    """Crude, language-light stems: "pricing" -> "pric", so "price" and "priced" match too."""
    out: list[str] = []
    for w in _WORD.findall((topic or "").lower()):
        if len(w) < 3 or w in _TOPIC_STOP:
            continue
        stem = w[: max(4, len(w) - 3)] if len(w) > 4 else w
        if stem not in out:
            out.append(stem)
    return out[:8]


def matches(stems: list[str], *texts: str | None) -> bool:
    if not stems:
        return True
    words = _WORD.findall(" ".join(t or "" for t in texts).lower())
    return any(w.startswith(s) for s in stems for w in words)


# --- the loaded world -------------------------------------------------------------------------


@dataclass
class D:
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str
    meeting_at: datetime
    content: str
    rationale: str | None
    why: str | None
    quote: str | None
    quote_speaker: str | None
    supersedes: uuid.UUID | None
    confirmed_at: datetime
    confirmed_by: str | None
    visible: bool


@dataclass
class A:
    id: uuid.UUID
    meeting_id: uuid.UUID | None
    meeting_title: str | None
    content: str
    owner: str | None
    due_date: date | None
    created_at: datetime
    history: list[tuple[datetime, str]]  # (changed_at, to_status), oldest first

    def status_at(self, t: datetime) -> str | None:
        state = None
        for at, status in self.history:
            if at > t:
                break
            state = status
        return state

    def first(self, status: str) -> datetime | None:
        return next((at for at, s in self.history if s == status), None)

    def last(self, status: str, before: datetime) -> datetime | None:
        found = None
        for at, s in self.history:
            if at > before:
                break
            if s == status:
                found = at
        return found


@dataclass
class C:
    id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None
    content: str
    due_hint: str | None
    status: str
    created_at: datetime
    done_at: datetime | None
    updated_at: datetime


@dataclass
class G:
    id: uuid.UUID
    title: str
    period: str | None
    status: str
    owner: str | None
    created_at: datetime
    updated_at: datetime


@dataclass
class B:
    id: uuid.UUID
    decision_id: uuid.UUID
    statement: str
    check_date: date
    status: str
    outcome: str | None
    created_at: datetime
    resolved_at: datetime | None
    meeting_id: uuid.UUID


@dataclass
class World:
    viewer: uuid.UUID
    decisions: dict[uuid.UUID, D] = field(default_factory=dict)
    replaced_by: dict[uuid.UUID, list[D]] = field(default_factory=dict)
    actions: list[A] = field(default_factory=list)
    commitments: list[C] = field(default_factory=list)
    goals: list[G] = field(default_factory=list)
    bets: dict[uuid.UUID, B] = field(default_factory=dict)
    meeting_dates: dict[uuid.UUID, tuple[str, datetime]] = field(default_factory=dict)

    def replacement_at(self, decision_id: uuid.UUID, t: datetime) -> D | None:
        """The first decision confirmed by `t` that replaced this one, if any."""
        found = [r for r in self.replaced_by.get(decision_id, []) if r.confirmed_at <= t]
        return min(found, key=lambda r: r.confirmed_at) if found else None


_DECISIONS_SQL = text("""
    SELECT d.id, d.meeting_id, m.title AS meeting_title,
           COALESCE(m.scheduled_start, m.created_at) AS meeting_at,
           d.content, d.rationale, d.why, d.quote, d.supersedes,
           (SELECT u.email FROM users u WHERE u.id = d.quote_speaker_id) AS speaker_email,
           (SELECT u.email FROM users u WHERE u.id = d.confirmed_by) AS confirmer_email,
           COALESCE(
             (SELECT min(h.changed_at) FROM record_status_history h
              WHERE h.record_id = d.id AND h.record_kind = 'decision'
                AND h.to_status IN ('confirmed', 'superseded')),
             d.status_changed_at, d.created_at) AS confirmed_at,
           EXISTS (SELECT 1 FROM meeting_participants p
                   WHERE p.meeting_id = d.meeting_id AND p.user_id = :v) AS visible
    FROM decisions d JOIN meetings m ON m.id = d.meeting_id AND m.deleted_at IS NULL
    WHERE d.deleted_at IS NULL AND d.status IN ('confirmed', 'superseded')
      AND (EXISTS (SELECT 1 FROM meeting_participants p
                   WHERE p.meeting_id = d.meeting_id AND p.user_id = :v)
           OR d.supersedes IS NOT NULL)
    ORDER BY d.created_at, d.id
    LIMIT :lim
    """)


async def load(session: AsyncSession, viewer: uuid.UUID) -> World:
    world = World(viewer=viewer)
    params = {"v": str(viewer)}
    for r in (await session.execute(_DECISIONS_SQL, {**params, "lim": MAX_DECISIONS})).mappings():
        d = D(
            id=r["id"],
            meeting_id=r["meeting_id"],
            meeting_title=r["meeting_title"] or "Untitled meeting",
            meeting_at=r["meeting_at"],
            content=r["content"],
            rationale=r["rationale"],
            why=r["why"],
            quote=r["quote"],
            quote_speaker=display_name(r["speaker_email"]),
            supersedes=r["supersedes"],
            confirmed_at=r["confirmed_at"],
            confirmed_by=display_name(r["confirmer_email"]),
            visible=bool(r["visible"]),
        )
        if d.visible:
            world.decisions[d.id] = d
            world.meeting_dates[d.meeting_id] = (d.meeting_title, d.meeting_at)
        if d.supersedes is not None:
            world.replaced_by.setdefault(d.supersedes, []).append(d)

    rows = (
        (
            await session.execute(
                text(
                    "SELECT a.id, a.meeting_id, m.title AS meeting_title, "
                    "COALESCE(m.scheduled_start, m.created_at) AS meeting_at, a.content, "
                    "a.due_date, a.created_at, a.status, a.status_changed_at, "
                    "(SELECT u.email FROM users u WHERE u.id = a.assignee_id) AS owner_email "
                    "FROM action_items a JOIN meetings m ON m.id = a.meeting_id "
                    "AND m.deleted_at IS NULL WHERE a.deleted_at IS NULL "
                    "AND a.status IN ('confirmed', 'done', 'dismissed') "
                    "AND EXISTS (SELECT 1 FROM meeting_participants p "
                    "  WHERE p.meeting_id = a.meeting_id AND p.user_id = :v) "
                    "ORDER BY a.created_at, a.id LIMIT :lim"
                ),
                {**params, "lim": MAX_ACTIONS},
            )
        )
        .mappings()
        .all()
    )
    history: dict[uuid.UUID, list[tuple[datetime, str]]] = defaultdict(list)
    if rows:
        for h in (
            await session.execute(
                text(
                    "SELECT record_id, changed_at, to_status FROM record_status_history "
                    "WHERE record_kind = 'action' AND record_id = ANY(CAST(:ids AS uuid[])) "
                    "ORDER BY changed_at, id"
                ),
                {"ids": [str(r["id"]) for r in rows]},
            )
        ).all():
            history[h[0]].append((h[1], h[2]))
    for r in rows:
        hist = history.get(r["id"]) or []
        if not hist:  # older than the history table: what the row itself says
            hist = [(r["created_at"], "proposed")]
            if r["status"] != "proposed":
                hist.append((r["status_changed_at"] or r["created_at"], r["status"]))
        world.actions.append(
            A(
                id=r["id"],
                meeting_id=r["meeting_id"],
                meeting_title=r["meeting_title"],
                content=r["content"],
                owner=display_name(r["owner_email"]),
                due_date=r["due_date"],
                created_at=r["created_at"],
                history=hist,
            )
        )
        world.meeting_dates.setdefault(r["meeting_id"], (r["meeting_title"], r["meeting_at"]))

    for r in (
        await session.execute(
            text(
                "SELECT c.id, c.meeting_id, m.title AS meeting_title, c.content, c.due_hint, "
                "c.status, c.created_at, c.done_at, c.updated_at FROM commitments c "
                "JOIN meetings m ON m.id = c.meeting_id AND m.deleted_at IS NULL "
                "WHERE c.user_id = :v ORDER BY c.created_at LIMIT :lim"
            ),
            {**params, "lim": MAX_ACTIONS},
        )
    ).mappings():
        world.commitments.append(C(**dict(r)))

    for r in (
        await session.execute(
            text(
                "SELECT g.id, g.title, g.period, g.status, g.created_at, g.updated_at, "
                "(SELECT u.email FROM users u WHERE u.id = g.owner_user_id) AS owner_email "
                "FROM org_goals g ORDER BY g.position, g.created_at"
            )
        )
    ).mappings():
        world.goals.append(
            G(
                id=r["id"],
                title=r["title"],
                period=r["period"],
                status=r["status"],
                owner=display_name(r["owner_email"]),
                created_at=r["created_at"],
                updated_at=r["updated_at"],
            )
        )

    for r in (
        await session.execute(
            text(
                "SELECT b.id, b.decision_id, b.statement, b.check_date, b.status, b.outcome, "
                "b.created_at, b.resolved_at, b.meeting_id FROM decision_bets b "
                "WHERE b.deleted_at IS NULL AND b.status IN ('open', 'due', 'resolved') "
                "AND EXISTS (SELECT 1 FROM meeting_participants p "
                "  WHERE p.meeting_id = b.meeting_id AND p.user_id = :v)"
            ),
            params,
        )
    ).mappings():
        world.bets[r["decision_id"]] = B(**dict(r))
    return world


# --- shapes -------------------------------------------------------------------------------


class MeetingAtDate(BaseModel):
    id: uuid.UUID
    title: str
    date: datetime


class DecisionRef(BaseModel):
    """A decision in the story. `visible` false: from a meeting you were not in, withheld."""

    visible: bool = True
    id: uuid.UUID | None = None
    content: str | None = None
    meeting: MeetingAtDate | None = None
    decided_at: datetime | None = None


class BetAtDate(BaseModel):
    id: uuid.UUID
    statement: str
    check_date: date
    status: Literal["waiting", "resolved"]
    outcome: str | None = None


class DecisionAtDate(BaseModel):
    id: uuid.UUID
    content: str
    rationale: str | None = None
    why: str | None = None
    quote: str | None = None
    quote_speaker_name: str | None = None
    confirmed_by_name: str | None = None
    meeting: MeetingAtDate
    decided_at: datetime
    replaces: DecisionRef | None = None
    bet: BetAtDate | None = None


class ReplacedAtDate(BaseModel):
    id: uuid.UUID
    content: str
    meeting: MeetingAtDate
    decided_at: datetime
    replaced_at: datetime
    replaced_by: DecisionRef


class PromiseAtDate(BaseModel):
    id: uuid.UUID
    kind: Literal["action", "promise"]
    content: str
    owner_name: str | None = None
    due: str | None = None
    overdue: bool = False
    meeting: MeetingAtDate | None = None
    made_at: datetime


class GoalAtDate(BaseModel):
    id: uuid.UUID
    title: str
    period: str | None = None
    status: Literal["active", "achieved", "dropped"]
    owner_name: str | None = None


class SnapshotCounts(BaseModel):
    in_force: int
    replaced: int
    promises_open: int
    promises_overdue: int
    goals_active: int
    bets_waiting: int
    meetings: int


class Snapshot(BaseModel):
    as_of: date
    topic: str | None = None
    earliest: date | None = None
    counts: SnapshotCounts
    decisions: list[DecisionAtDate]
    replaced: list[ReplacedAtDate]
    promises: list[PromiseAtDate]
    goals: list[GoalAtDate]
    truncated: bool = False


class Reversal(BaseModel):
    old: DecisionRef
    new: DecisionRef
    replaced_at: datetime
    caused_by: MeetingAtDate | None = None


class PromiseChange(BaseModel):
    id: uuid.UUID
    kind: Literal["action", "promise"]
    content: str
    owner_name: str | None = None
    due_date: date | None = None
    at: datetime
    on_time: bool | None = None
    kept_later_at: datetime | None = None
    caused_by: MeetingAtDate | None = None


class BetChange(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    statement: str
    outcome: str
    resolved_at: datetime
    caused_by: MeetingAtDate | None = None


class GoalChange(BaseModel):
    id: uuid.UUID
    title: str
    change: Literal["added", "achieved", "dropped"]
    at: datetime


class MeetingImpact(BaseModel):
    meeting: MeetingAtDate
    new_decisions: int
    reversals: int
    promises_made: int


class DiffCounts(BaseModel):
    new_decisions: int
    reversed: int
    promises_made: int
    promises_kept: int
    promises_broken: int
    bets_resolved: int
    goals_changed: int


class Diff(BaseModel):
    from_date: date = Field(alias="from")
    to_date: date = Field(alias="to")
    topic: str | None = None
    counts: DiffCounts
    new_decisions: list[DecisionAtDate]
    reversed: list[Reversal]
    promises_made: list[PromiseChange]
    promises_kept: list[PromiseChange]
    promises_broken: list[PromiseChange]
    bets_resolved: list[BetChange]
    goals: list[GoalChange]
    meetings: list[MeetingImpact]

    model_config = {"populate_by_name": True}


class TimelineStep(BaseModel):
    at: datetime
    kind: Literal["decided", "replaced", "bet_resolved"]
    decision: DecisionRef
    rationale: str | None = None
    quote: str | None = None
    quote_speaker_name: str | None = None
    replaces: DecisionRef | None = None
    in_force_now: bool
    outcome: str | None = None
    bet_statement: str | None = None


class Timeline(BaseModel):
    topic: str
    from_date: date | None = Field(default=None, alias="from")
    to_date: date | None = Field(default=None, alias="to")
    steps: list[TimelineStep]
    meetings: int
    in_force_now: int

    model_config = {"populate_by_name": True}


# --- building -------------------------------------------------------------------------------


def _meeting(world: World, meeting_id: uuid.UUID | None) -> MeetingAtDate | None:
    if meeting_id is None or meeting_id not in world.meeting_dates:
        return None
    title, at = world.meeting_dates[meeting_id]
    return MeetingAtDate(id=meeting_id, title=title or "Untitled meeting", date=at)


def _ref(world: World, d: D | None, *, supersedes_id: uuid.UUID | None = None) -> DecisionRef:
    if d is None:
        return DecisionRef(visible=False)
    if not d.visible:
        return DecisionRef(visible=False, decided_at=d.confirmed_at)
    return DecisionRef(
        id=d.id,
        content=d.content,
        meeting=_meeting(world, d.meeting_id),
        decided_at=d.confirmed_at,
    )


def _bet_at(world: World, decision_id: uuid.UUID, t: datetime) -> BetAtDate | None:
    b = world.bets.get(decision_id)
    if b is None or b.created_at > t:
        return None
    resolved = b.resolved_at is not None and b.resolved_at <= t
    return BetAtDate(
        id=b.id,
        statement=b.statement,
        check_date=b.check_date,
        status="resolved" if resolved else "waiting",
        outcome=b.outcome if resolved else None,
    )


def _decision_at(world: World, d: D, t: datetime) -> DecisionAtDate:
    replaces = None
    if d.supersedes is not None:
        prior = world.decisions.get(d.supersedes)
        replaces = _ref(world, prior) if prior is not None else DecisionRef(visible=False)
    meeting = _meeting(world, d.meeting_id)
    assert meeting is not None
    return DecisionAtDate(
        id=d.id,
        content=d.content,
        rationale=d.rationale,
        why=d.why,
        quote=d.quote,
        quote_speaker_name=d.quote_speaker,
        confirmed_by_name=d.confirmed_by,
        meeting=meeting,
        decided_at=d.confirmed_at,
        replaces=replaces,
        bet=_bet_at(world, d.id, t),
    )


def _d_matches(d: D, stems: list[str]) -> bool:
    return matches(stems, d.content, d.rationale, d.quote, d.why)


def earliest(world: World) -> date | None:
    times = [d.confirmed_at for d in world.decisions.values()] + [
        a.created_at for a in world.actions
    ]
    return min(times).date() if times else None


def snapshot(world: World, as_of: date, topic: str | None = None) -> Snapshot:
    t = end_of(as_of)
    stems = topic_stems(topic)
    in_force: list[DecisionAtDate] = []
    replaced: list[ReplacedAtDate] = []
    for d in sorted(world.decisions.values(), key=lambda x: x.confirmed_at, reverse=True):
        if d.confirmed_at > t or not _d_matches(d, stems):
            continue
        by = world.replacement_at(d.id, t)
        if by is None:
            in_force.append(_decision_at(world, d, t))
        else:
            meeting = _meeting(world, d.meeting_id)
            assert meeting is not None
            replaced.append(
                ReplacedAtDate(
                    id=d.id,
                    content=d.content,
                    meeting=meeting,
                    decided_at=d.confirmed_at,
                    replaced_at=by.confirmed_at,
                    replaced_by=_ref(world, by),
                )
            )
    promises: list[PromiseAtDate] = []
    for a in world.actions:
        if a.status_at(t) != "confirmed" or not matches(stems, a.content):
            continue
        made = a.first("confirmed") or a.created_at
        promises.append(
            PromiseAtDate(
                id=a.id,
                kind="action",
                content=a.content,
                owner_name=a.owner,
                due=a.due_date.isoformat() if a.due_date else None,
                overdue=a.due_date is not None and a.due_date < as_of,
                meeting=_meeting(world, a.meeting_id),
                made_at=made,
            )
        )
    for c in world.commitments:
        if c.created_at > t or not matches(stems, c.content):
            continue
        if c.done_at is not None and c.done_at <= t:
            continue
        if c.status == "dismissed" and c.updated_at <= t:
            continue
        promises.append(
            PromiseAtDate(
                id=c.id,
                kind="promise",
                content=c.content,
                owner_name="You",
                due=c.due_hint,
                meeting=MeetingAtDate(
                    id=c.meeting_id, title=c.meeting_title or "Untitled meeting", date=c.created_at
                ),
                made_at=c.created_at,
            )
        )
    promises.sort(key=lambda p: (not p.overdue, p.due or "9999", p.made_at))
    goals: list[GoalAtDate] = []
    for g in world.goals:
        if g.created_at > t or not matches(stems, g.title):
            continue
        status = g.status if g.status != "active" and g.updated_at <= t else "active"
        goals.append(
            GoalAtDate(
                id=g.id,
                title=g.title,
                period=g.period,
                status=status,  # type: ignore[arg-type]
                owner_name=g.owner,
            )
        )
    meetings = {m for m, (_, at) in world.meeting_dates.items() if at <= t}
    counts = SnapshotCounts(
        in_force=len(in_force),
        replaced=len(replaced),
        promises_open=len(promises),
        promises_overdue=sum(p.overdue for p in promises),
        goals_active=sum(g.status == "active" for g in goals),
        bets_waiting=sum(1 for d in in_force if d.bet is not None and d.bet.status == "waiting"),
        meetings=len(meetings),
    )
    truncated = len(in_force) > MAX_LIST or len(replaced) > MAX_LIST or len(promises) > MAX_LIST
    return Snapshot(
        as_of=as_of,
        topic=topic or None,
        earliest=earliest(world),
        counts=counts,
        decisions=in_force[:MAX_LIST],
        replaced=replaced[:MAX_LIST],
        promises=promises[:MAX_LIST],
        goals=goals,
        truncated=truncated,
    )


def diff(world: World, start: date, end: date, topic: str | None = None) -> Diff:
    if end < start:
        start, end = end, start
    t0, t1 = end_of(start), end_of(end)
    stems = topic_stems(topic)
    impact: dict[uuid.UUID, list[int]] = defaultdict(lambda: [0, 0, 0])

    new = [
        d for d in world.decisions.values() if t0 < d.confirmed_at <= t1 and _d_matches(d, stems)
    ]
    new.sort(key=lambda d: d.confirmed_at)
    for d in new:
        impact[d.meeting_id][0] += 1

    reversals: list[Reversal] = []
    for old in world.decisions.values():
        by = world.replacement_at(old.id, t1)
        if by is None or not (t0 < by.confirmed_at <= t1):
            continue
        if not (_d_matches(old, stems) or (by.visible and _d_matches(by, stems))):
            continue
        caused = _meeting(world, by.meeting_id) if by.visible else None
        if by.visible:
            impact[by.meeting_id][1] += 1
        reversals.append(
            Reversal(
                old=_ref(world, old),
                new=_ref(world, by),
                replaced_at=by.confirmed_at,
                caused_by=caused,
            )
        )
    reversals.sort(key=lambda r: r.replaced_at)

    made: list[PromiseChange] = []
    kept: list[PromiseChange] = []
    broken: list[PromiseChange] = []
    for a in world.actions:
        if not matches(stems, a.content):
            continue
        confirmed = a.first("confirmed")
        if confirmed is not None and t0 < confirmed <= t1:
            made.append(
                PromiseChange(
                    id=a.id,
                    kind="action",
                    content=a.content,
                    owner_name=a.owner,
                    due_date=a.due_date,
                    at=confirmed,
                    caused_by=_meeting(world, a.meeting_id),
                )
            )
            if a.meeting_id is not None:
                impact[a.meeting_id][2] += 1
        done = a.last("done", t1)
        if done is not None and done > t0:
            kept.append(
                PromiseChange(
                    id=a.id,
                    kind="action",
                    content=a.content,
                    owner_name=a.owner,
                    due_date=a.due_date,
                    at=done,
                    on_time=(done.date() <= a.due_date) if a.due_date else None,
                    caused_by=_meeting(world, a.meeting_id),
                )
            )
        if a.due_date is not None and start < a.due_date <= end:
            at_due = a.status_at(end_of(a.due_date))
            if at_due == "confirmed":
                later = a.last("done", t1)
                broken.append(
                    PromiseChange(
                        id=a.id,
                        kind="action",
                        content=a.content,
                        owner_name=a.owner,
                        due_date=a.due_date,
                        at=end_of(a.due_date),
                        on_time=False,
                        kept_later_at=later if later and later > end_of(a.due_date) else None,
                        caused_by=_meeting(world, a.meeting_id),
                    )
                )
    for c in world.commitments:
        if not matches(stems, c.content):
            continue
        if t0 < c.created_at <= t1:
            made.append(
                PromiseChange(
                    id=c.id,
                    kind="promise",
                    content=c.content,
                    owner_name="You",
                    at=c.created_at,
                    caused_by=MeetingAtDate(
                        id=c.meeting_id,
                        title=c.meeting_title or "Untitled meeting",
                        date=c.created_at,
                    ),
                )
            )
        if c.done_at is not None and t0 < c.done_at <= t1:
            kept.append(
                PromiseChange(
                    id=c.id,
                    kind="promise",
                    content=c.content,
                    owner_name="You",
                    at=c.done_at,
                )
            )
    for lst in (made, kept, broken):
        lst.sort(key=lambda p: p.at)

    bets: list[BetChange] = []
    for b in world.bets.values():
        if b.resolved_at is None or not (t0 < b.resolved_at <= t1) or b.outcome is None:
            continue
        bd = world.decisions.get(b.decision_id)
        if stems and not matches(stems, b.statement, bd.content if bd else None):
            continue
        bets.append(
            BetChange(
                id=b.id,
                decision_id=b.decision_id,
                statement=b.statement,
                outcome=b.outcome,
                resolved_at=b.resolved_at,
                caused_by=_meeting(world, b.meeting_id),
            )
        )
    bets.sort(key=lambda b: b.resolved_at)

    goals: list[GoalChange] = []
    for g in world.goals:
        if not matches(stems, g.title):
            continue
        if t0 < g.created_at <= t1:
            goals.append(GoalChange(id=g.id, title=g.title, change="added", at=g.created_at))
        if g.status in ("achieved", "dropped") and t0 < g.updated_at <= t1:
            goals.append(
                GoalChange(id=g.id, title=g.title, change=g.status, at=g.updated_at)  # type: ignore[arg-type]
            )
    goals.sort(key=lambda g: g.at)

    meetings = [
        MeetingImpact(meeting=ref, new_decisions=n[0], reversals=n[1], promises_made=n[2])
        for mid, n in impact.items()
        if (ref := _meeting(world, mid)) is not None
    ]
    meetings.sort(
        key=lambda m: (-(m.new_decisions + m.reversals + m.promises_made), m.meeting.date)
    )
    return Diff(
        **{"from": start, "to": end},
        topic=topic or None,
        counts=DiffCounts(
            new_decisions=len(new),
            reversed=len(reversals),
            promises_made=len(made),
            promises_kept=len(kept),
            promises_broken=len(broken),
            bets_resolved=len(bets),
            goals_changed=len(goals),
        ),
        new_decisions=[_decision_at(world, d, t1) for d in new][:MAX_LIST],
        reversed=reversals[:MAX_LIST],
        promises_made=made[:MAX_LIST],
        promises_kept=kept[:MAX_LIST],
        promises_broken=broken[:MAX_LIST],
        bets_resolved=bets[:MAX_LIST],
        goals=goals,
        meetings=meetings[:50],
    )


def timeline(
    world: World, topic: str, start: date | None = None, end: date | None = None
) -> Timeline:
    """The topic's decisions in order. A decision that replaced a topic decision is a step
    too, even when its own words do not repeat the topic: it is how the topic changed."""
    stems = topic_stems(topic)
    lo = datetime.combine(start, time.min, tzinfo=UTC) if start else None
    hi = end_of(end) if end else None
    now = datetime.now(UTC)
    on_topic = {d.id for d in world.decisions.values() if _d_matches(d, stems)} if stems else set()
    chosen: dict[uuid.UUID, D] = {i: world.decisions[i] for i in on_topic}
    for i in list(on_topic):
        for r in world.replaced_by.get(i, []):
            if r.visible and r.id in world.decisions:
                chosen.setdefault(r.id, r)
    steps: list[TimelineStep] = []
    for d in sorted(chosen.values(), key=lambda x: x.confirmed_at):
        prior = world.decisions.get(d.supersedes) if d.supersedes else None
        in_force = world.replacement_at(d.id, now) is None
        if (lo is None or d.confirmed_at >= lo) and (hi is None or d.confirmed_at <= hi):
            steps.append(
                TimelineStep(
                    at=d.confirmed_at,
                    kind="replaced" if d.supersedes is not None else "decided",
                    decision=_ref(world, d),
                    rationale=d.rationale or d.why,
                    quote=d.quote,
                    quote_speaker_name=d.quote_speaker,
                    replaces=(
                        (_ref(world, prior) if prior is not None else DecisionRef(visible=False))
                        if d.supersedes is not None
                        else None
                    ),
                    in_force_now=in_force,
                )
            )
        b = world.bets.get(d.id)
        if b is not None and b.resolved_at is not None and b.outcome is not None:
            if (lo is None or b.resolved_at >= lo) and (hi is None or b.resolved_at <= hi):
                steps.append(
                    TimelineStep(
                        at=b.resolved_at,
                        kind="bet_resolved",
                        decision=_ref(world, d),
                        in_force_now=in_force,
                        outcome=b.outcome,
                        bet_statement=b.statement,
                    )
                )
    steps.sort(key=lambda s: s.at)
    in_steps = {s.decision.id for s in steps if s.decision.id is not None}
    return Timeline(
        **{"from": start, "to": end},
        topic=topic,
        steps=steps[:MAX_LIST],
        meetings=len({s.decision.meeting.id for s in steps if s.decision.meeting is not None}),
        in_force_now=sum(1 for i in in_steps if world.replacement_at(i, now) is None),
    )


def any_rows(world: World) -> bool:
    return bool(world.decisions or world.actions or world.commitments)
