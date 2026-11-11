"""Decisions as bets: what a decision expects to happen, checked on a date, resolved by a person.

"Beyond the meeting", Layer 3. One live bet per decision (`decision_bets`), its evidence
(`decision_bet_evidence`), and the company-level learning built from resolved bets.

The rules this module holds, so every surface applies them the same way:

- **Agents propose, people decide.** DecisionAgent may propose a bet (`proposed_by = agent`,
  `status = proposed`) only when the meeting said what it expected. The owner accepts, edits or
  dismisses it. A check never resolves a bet: a connected metric only fills
  `suggested_outcome`, shown to the owner for confirmation.
- **Who may change a bet.** Its owner, the meeting's host, or the person who confirmed the
  decision, and only while they are a participant of the decision's meeting. Everyone else in
  the meeting can read it; anyone else gets a 404 (participant-scoped, like the decision log).
- **Evidence is withheld per viewer.** A later meeting or decision found by the check is shown
  only to people who were in that meeting; others see that something exists, not what it says
  (the same rule `records.py` applies to lineage steps). Confidential meetings are never
  searched for evidence.
- **Aggregate only.** The company page counts bets by decision type and by team, never by
  person, and shows a team only when it has at least `MIN_TEAM_RESOLVED` resolved bets.
  Lessons and failing assumptions are what owners wrote at resolution for the workspace to
  learn from; bets from confidential meetings are counted but their text is never listed.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.core_api.records import person_name

BetStatus = Literal["proposed", "open", "due", "resolved", "dismissed"]
BetKind = Literal["metric", "event"]
Direction = Literal["increase", "decrease", "reach", "hold", "happen", "not_happen"]
Source = Literal["crm", "metrics", "meetings", "jira", "manual"]
Category = Literal[
    "pricing",
    "product",
    "growth",
    "customers",
    "hiring",
    "operations",
    "engineering",
    "finance",
    "other",
]
Outcome = Literal["hit", "partly", "missed"]
EvidenceKind = Literal["metric", "meeting", "decision", "jira", "note"]
Signal = Literal["supports", "against", "neutral"]

CATEGORIES: tuple[str, ...] = (
    "pricing",
    "product",
    "growth",
    "customers",
    "hiring",
    "operations",
    "engineering",
    "finance",
    "other",
)
METRIC_DIRECTIONS = frozenset({"increase", "decrease", "reach", "hold"})
EVENT_DIRECTIONS = frozenset({"happen", "not_happen"})
SCORE: dict[str, float] = {"hit": 1.0, "partly": 0.5, "missed": 0.0}
MIN_TEAM_RESOLVED = 3
MAX_EVIDENCE_SEARCH = 5

# The CRM metrics integration-service can read (HubSpot), with their unit. A bet on one of
# these with `source = crm` gets a metric reading on its check date when HubSpot is connected.
CRM_METRICS: dict[str, tuple[str, str]] = {
    "hubspot.deals_won": ("Deals won", "deals"),
    "hubspot.revenue_won": ("Revenue won", "$"),
    "hubspot.new_contacts": ("New contacts", "contacts"),
    "hubspot.new_companies": ("New companies", "companies"),
}

_STOP = frozenset(
    "the a an and or of to in on for by with from this that will should would could we our "
    "it is be are was were at as into than then more less per cut grow raise lower drop "
    "increase decrease reduce improve points point percent month months week weeks quarter "
    "year next end".split()
)
_WORD = re.compile(r"[a-z0-9]{3,}")


# --- shapes -------------------------------------------------------------------------------


class BetEvidence(BaseModel):
    id: uuid.UUID
    kind: EvidenceKind
    visible: bool = True
    title: str
    detail: str | None = None
    url: str | None = None
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    decision_id: uuid.UUID | None = None
    observed_value: float | None = None
    signal: Signal | None = None
    occurred_at: datetime | None = None


class Bet(BaseModel):
    id: uuid.UUID
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    status: BetStatus
    statement: str
    kind: BetKind
    metric: str
    direction: Direction
    magnitude: float | None = None
    unit: str | None = None
    baseline: float | None = None
    check_date: date
    source: Source
    metric_key: str | None = None
    category: Category
    confidence_pct: int | None = None
    assumption: str | None = None
    quote: str | None = None
    quote_speaker_name: str | None = None
    proposed_by: Literal["agent", "person"]
    agent_confidence: float | None = None
    owner_user_id: uuid.UUID | None = None
    owner_name: str | None = None
    team_id: uuid.UUID | None = None
    team_name: str | None = None
    evidence_status: Literal["pending", "gathered"]
    checked_at: datetime | None = None
    suggested_outcome: Outcome | None = None
    suggested_reason: str | None = None
    observed_value: float | None = None
    outcome: Outcome | None = None
    assumption_held: bool | None = None
    lesson: str | None = None
    resolved_at: datetime | None = None
    resolved_by_name: str | None = None
    created_at: datetime
    updated_at: datetime
    evidence: list[BetEvidence] = Field(default_factory=list)


class Lesson(BaseModel):
    """A resolved bet's lesson, offered when a similar decision is being made."""

    category: Category
    statement: str
    outcome: Outcome
    lesson: str | None = None
    failed_assumption: str | None = None
    resolved_at: datetime
    decision_id: uuid.UUID | None = None  # only when the viewer was in that meeting
    meeting_id: uuid.UUID | None = None  # likewise


class CategoryRecord(BaseModel):
    category: Category
    resolved: int
    hit: int
    partly: int
    missed: int
    hit_rate: float | None = None


class BetHints(BaseModel):
    category: Category | None = None
    record: CategoryRecord | None = None
    lessons: list[Lesson] = Field(default_factory=list)


class DecisionBetView(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    decision_status: str
    bet: Bet | None = None
    can_edit: bool
    owner_candidates: list[OwnerOption] = Field(default_factory=list)
    teams: list[TeamOption] = Field(default_factory=list)
    hints: BetHints


class OwnerOption(BaseModel):
    user_id: uuid.UUID
    name: str


class TeamOption(BaseModel):
    id: uuid.UUID
    name: str


class BetUpsert(BaseModel):
    """Create or edit the expectation. Accepting a proposed bet with edits is this too."""

    model_config = ConfigDict(extra="forbid")

    statement: str = Field(min_length=1, max_length=300)
    kind: BetKind = "metric"
    metric: str = Field(min_length=1, max_length=120)
    direction: Direction
    magnitude: float | None = Field(default=None, ge=-1e12, le=1e12)
    unit: str | None = Field(default=None, max_length=20)
    baseline: float | None = Field(default=None, ge=-1e12, le=1e12)
    check_date: date
    source: Source = "meetings"
    metric_key: str | None = Field(default=None, max_length=60)
    category: Category = "other"
    confidence_pct: int | None = Field(default=None, ge=1, le=99)
    assumption: str | None = Field(default=None, max_length=300)
    owner_user_id: uuid.UUID | None = None
    team_id: uuid.UUID | None = None

    @field_validator("statement", "metric", "unit", "assumption", "metric_key")
    @classmethod
    def _squash(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = " ".join(v.split())
        return v or None

    @model_validator(mode="after")
    def _consistent(self) -> BetUpsert:
        if not self.statement or not self.metric:
            raise ValueError("statement and metric must not be blank")
        if self.kind == "metric" and self.direction not in METRIC_DIRECTIONS:
            raise ValueError("a metric bet goes up, goes down, reaches a level or holds")
        if self.kind == "event" and self.direction not in EVENT_DIRECTIONS:
            raise ValueError("an event bet either happens or does not")
        if self.kind == "event":
            self.magnitude = self.unit = self.baseline = None
        if self.direction == "reach" and self.magnitude is None:
            raise ValueError("say the level it should reach")
        if self.metric_key is not None:
            if self.metric_key not in CRM_METRICS:
                raise ValueError("unknown CRM metric")
            if self.source != "crm":
                raise ValueError("a CRM metric needs source crm")
        return self


class BetResolve(BaseModel):
    model_config = ConfigDict(extra="forbid")

    outcome: Outcome
    assumption_held: bool | None = None
    lesson: str | None = Field(default=None, max_length=500)

    @field_validator("lesson")
    @classmethod
    def _squash(cls, v: str | None) -> str | None:
        return (" ".join(v.split()) or None) if v is not None else None


DecisionBetView.model_rebuild()


# --- reading --------------------------------------------------------------------------------

BET_COLS = (
    "b.id, b.decision_id, b.meeting_id, b.status, b.statement, b.kind, b.metric, b.direction, "
    "b.magnitude, b.unit, b.baseline, b.check_date, b.source, b.metric_key, b.category, "
    "b.confidence_pct, b.assumption, b.quote, b.quote_speaker_id, b.proposed_by, "
    "b.agent_confidence, b.owner_user_id, b.team_id, b.evidence_status, b.checked_at, "
    "b.suggested_outcome, b.suggested_reason, b.observed_value, b.outcome, b.assumption_held, "
    "b.lesson, b.resolved_at, b.resolved_by, b.created_by, b.created_at, b.updated_at, "
    "(SELECT u.email FROM users u WHERE u.id = b.owner_user_id) AS owner_email, "
    "(SELECT u.email FROM users u WHERE u.id = b.quote_speaker_id) AS speaker_email, "
    "(SELECT u.email FROM users u WHERE u.id = b.resolved_by) AS resolver_email, "
    "(SELECT u.display_name FROM users u WHERE u.id = b.owner_user_id) AS owner_display_name, "
    "(SELECT u.display_name FROM users u WHERE u.id = b.quote_speaker_id) "
    "AS speaker_display_name, "
    "(SELECT u.display_name FROM users u WHERE u.id = b.resolved_by) AS resolver_display_name, "
    "(SELECT t.name FROM teams t WHERE t.id = b.team_id AND t.deleted_at IS NULL) AS team_name"
)


def _f(v: Any) -> float | None:
    if v is None:
        return None
    return float(v) if isinstance(v, (Decimal, int, float)) else None


def bet_from_row(row: Any, evidence: list[BetEvidence] | None = None) -> Bet:
    return Bet(
        id=row["id"],
        decision_id=row["decision_id"],
        meeting_id=row["meeting_id"],
        status=row["status"],
        statement=row["statement"],
        kind=row["kind"],
        metric=row["metric"],
        direction=row["direction"],
        magnitude=_f(row["magnitude"]),
        unit=row["unit"],
        baseline=_f(row["baseline"]),
        check_date=row["check_date"],
        source=row["source"],
        metric_key=row["metric_key"],
        category=row["category"],
        confidence_pct=row["confidence_pct"],
        assumption=row["assumption"],
        quote=row["quote"],
        quote_speaker_name=person_name(row["speaker_display_name"], row["speaker_email"]),
        proposed_by=row["proposed_by"],
        agent_confidence=row["agent_confidence"],
        owner_user_id=row["owner_user_id"],
        owner_name=person_name(row["owner_display_name"], row["owner_email"]),
        team_id=row["team_id"] if row["team_name"] else None,
        team_name=row["team_name"],
        evidence_status=row["evidence_status"],
        checked_at=row["checked_at"],
        suggested_outcome=row["suggested_outcome"],
        suggested_reason=row["suggested_reason"],
        observed_value=_f(row["observed_value"]),
        outcome=row["outcome"],
        assumption_held=row["assumption_held"],
        lesson=row["lesson"],
        resolved_at=row["resolved_at"],
        resolved_by_name=person_name(row["resolver_display_name"], row["resolver_email"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        evidence=evidence or [],
    )


async def bet_row_for_decision(session: AsyncSession, decision_id: uuid.UUID) -> Any:
    return (
        (
            await session.execute(
                text(
                    f"SELECT {BET_COLS} FROM decision_bets b "  # noqa: S608 - constant columns
                    "WHERE b.decision_id = :d AND b.deleted_at IS NULL"
                ),
                {"d": str(decision_id)},
            )
        )
        .mappings()
        .first()
    )


async def evidence_for(
    session: AsyncSession, bet_id: uuid.UUID, viewer: uuid.UUID
) -> list[BetEvidence]:
    """The bet's evidence as `viewer` may see it: a meeting or decision from a meeting the
    viewer was not in keeps its place with its content withheld."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT e.id, e.kind, e.title, e.detail, e.url, e.meeting_id, e.decision_id, "
                    "e.observed_value, e.signal, e.occurred_at, m.title AS meeting_title, "
                    "(e.meeting_id IS NULL OR EXISTS (SELECT 1 FROM meeting_participants p "
                    "  WHERE p.meeting_id = e.meeting_id AND p.user_id = :v)) AS visible "
                    "FROM decision_bet_evidence e LEFT JOIN meetings m ON m.id = e.meeting_id "
                    "WHERE e.bet_id = :b "
                    "ORDER BY CASE e.kind WHEN 'metric' THEN 0 WHEN 'jira' THEN 1 "
                    "WHEN 'decision' THEN 2 WHEN 'meeting' THEN 3 ELSE 4 END, "
                    "e.occurred_at DESC NULLS LAST, e.created_at"
                ),
                {"b": str(bet_id), "v": str(viewer)},
            )
        )
        .mappings()
        .all()
    )
    out: list[BetEvidence] = []
    for r in rows:
        if r["visible"]:
            out.append(
                BetEvidence(
                    id=r["id"],
                    kind=r["kind"],
                    title=r["title"],
                    detail=r["detail"],
                    url=r["url"],
                    meeting_id=r["meeting_id"],
                    meeting_title=r["meeting_title"],
                    decision_id=r["decision_id"],
                    observed_value=_f(r["observed_value"]),
                    signal=r["signal"],
                    occurred_at=r["occurred_at"],
                )
            )
        else:
            out.append(
                BetEvidence(
                    id=r["id"],
                    kind=r["kind"],
                    visible=False,
                    title=(
                        "A later decision from a meeting you were not in"
                        if r["kind"] == "decision"
                        else "A later meeting you were not in"
                    ),
                    signal=r["signal"],
                    occurred_at=r["occurred_at"],
                )
            )
    return out


@dataclass(frozen=True)
class DecisionFacts:
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    status: str
    content: str
    created_at: datetime
    confirmed_by: uuid.UUID | None
    host_id: uuid.UUID | None
    is_participant: bool
    confidential: bool


async def decision_facts(
    session: AsyncSession, decision_id: uuid.UUID, viewer: uuid.UUID
) -> DecisionFacts | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT d.id, d.meeting_id, d.status, d.content, d.created_at, "
                    "d.confirmed_by, m.confidential, "
                    "COALESCE((SELECT p.user_id FROM meeting_participants p "
                    "  WHERE p.meeting_id = m.id AND p.role_in_meeting = 'host' "
                    "  ORDER BY p.user_id LIMIT 1), m.created_by) AS host_id, "
                    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = m.id "
                    "  AND p.user_id = :v) AS is_participant "
                    "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                    "AND m.deleted_at IS NULL "
                    "WHERE d.id = :d AND d.deleted_at IS NULL"
                ),
                {"d": str(decision_id), "v": str(viewer)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    return DecisionFacts(
        decision_id=row["id"],
        meeting_id=row["meeting_id"],
        status=row["status"],
        content=row["content"],
        created_at=row["created_at"],
        confirmed_by=row["confirmed_by"],
        host_id=row["host_id"],
        is_participant=bool(row["is_participant"]),
        confidential=bool(row["confidential"]),
    )


def can_edit(facts: DecisionFacts, owner: uuid.UUID | None, viewer: uuid.UUID) -> bool:
    if not facts.is_participant:
        return False
    return viewer in {x for x in (owner, facts.host_id, facts.confirmed_by) if x is not None}


async def first_team(session: AsyncSession, user_id: uuid.UUID | None) -> uuid.UUID | None:
    if user_id is None:
        return None
    row = (
        await session.execute(
            text(
                "SELECT tm.team_id FROM team_members tm JOIN teams t ON t.id = tm.team_id "
                "AND t.deleted_at IS NULL WHERE tm.user_id = :u ORDER BY t.name, t.id LIMIT 1"
            ),
            {"u": str(user_id)},
        )
    ).first()
    return row[0] if row else None


# --- matching text ------------------------------------------------------------------------


def keywords(*texts: str | None, limit: int = 8) -> list[str]:
    """Distinct content words (3+ letters, no stop words), first seen first."""
    out: list[str] = []
    for t in texts:
        for w in _WORD.findall((t or "").lower()):
            if w not in _STOP and w not in out:
                out.append(w)
            if len(out) >= limit:
                return out
    return out


def tsquery(words: list[str]) -> str | None:
    """An OR `to_tsquery` over sanitized words (only [a-z0-9] can reach it)."""
    safe = [w for w in words if _WORD.fullmatch(w)]
    return " | ".join(safe) if safe else None


def _tokens(value: str) -> set[str]:
    return {w for w in _WORD.findall(value.lower()) if w not in _STOP}


def similar(a: str, b: str, threshold: float = 0.5) -> bool:
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return a.strip().lower() == b.strip().lower()
    return len(ta & tb) / len(ta | tb) >= threshold


# --- what a metric says ---------------------------------------------------------------------


def suggest_outcome(
    *,
    direction: str,
    magnitude: float | None,
    unit: str | None,
    baseline: float | None,
    observed: float,
) -> tuple[str | None, str]:
    """What a connected metric says about a metric bet, and why in one line. None when the
    reading cannot answer it (no baseline for a change, an event bet). Never a resolution:
    the owner confirms or overrides it."""
    pct = (unit or "").strip() == "%"

    def fmt(x: float) -> str:
        s = f"{x:,.2f}".rstrip("0").rstrip(".")
        return f"{s}%" if pct else (f"{s} {unit}" if unit else s)

    if direction == "reach":
        if magnitude is None:
            return None, "No target level to compare against"
        if observed >= magnitude:
            return "hit", f"Reached {fmt(observed)}, target {fmt(magnitude)}"
        if magnitude and observed >= 0.8 * magnitude:
            return "partly", f"At {fmt(observed)}, short of {fmt(magnitude)}"
        return "missed", f"At {fmt(observed)}, well short of {fmt(magnitude)}"
    if baseline is None:
        return None, f"Now {fmt(observed)}; no baseline to compare against"
    change = observed - baseline
    if direction == "hold":
        tolerance = abs(baseline) * 0.05 if baseline else 0.0
        if abs(change) <= tolerance:
            return "hit", f"Held at {fmt(observed)} (was {fmt(baseline)})"
        if abs(change) <= tolerance * 2:
            return "partly", f"Moved to {fmt(observed)} from {fmt(baseline)}"
        return "missed", f"Moved to {fmt(observed)} from {fmt(baseline)}"
    signed = change if direction == "increase" else -change
    verb = "up" if change >= 0 else "down"
    moved = f"{verb} {fmt(abs(change))} to {fmt(observed)} (was {fmt(baseline)})"
    target = abs(magnitude) if magnitude is not None else None
    if signed <= 0:
        return "missed", f"Went the wrong way: {moved}"
    if target is None or signed >= target:
        return "hit", f"Moved as expected: {moved}"
    if signed >= target / 2:
        return "partly", f"Moved part of the way: {moved}, expected {fmt(target)}"
    return "missed", f"Barely moved: {moved}, expected {fmt(target)}"


# --- the company page -------------------------------------------------------------------------


class Totals(BaseModel):
    bets: int
    proposed: int
    open: int
    due: int
    resolved: int
    hit: int
    partly: int
    missed: int
    hit_rate: float | None = None
    brier: float | None = None


class GroupRecord(BaseModel):
    key: str
    label: str
    resolved: int
    hit: int
    partly: int
    missed: int
    open: int
    hit_rate: float | None = None
    stated_confidence: float | None = None


class CalibrationBucket(BaseModel):
    label: str
    low: int
    high: int
    count: int
    stated: float
    actual: float


class FailingAssumption(BaseModel):
    assumption: str
    times: int
    categories: list[Category]
    last_failed_at: datetime


class PublicLesson(BaseModel):
    category: Category
    outcome: Outcome
    lesson: str
    resolved_at: datetime


class MonthPoint(BaseModel):
    month: str
    resolved: int
    hit_rate: float | None = None


class Calibration(BaseModel):
    totals: Totals
    by_category: list[GroupRecord]
    by_team: list[GroupRecord]
    teams_below_threshold: int
    min_team_resolved: int = MIN_TEAM_RESOLVED
    buckets: list[CalibrationBucket]
    failing_assumptions: list[FailingAssumption]
    lessons: list[PublicLesson]
    months: list[MonthPoint]


CATEGORY_LABELS: dict[str, str] = {
    "pricing": "Pricing",
    "product": "Product",
    "growth": "Growth and marketing",
    "customers": "Customers",
    "hiring": "Hiring and people",
    "operations": "Operations",
    "engineering": "Engineering",
    "finance": "Finance",
    "other": "Other",
}

_BUCKETS = ((1, 49), (50, 59), (60, 69), (70, 79), (80, 89), (90, 99))


def _rate(rows: list[dict[str, Any]]) -> float | None:
    scored = [SCORE[r["outcome"]] for r in rows if r["status"] == "resolved" and r["outcome"]]
    return round(sum(scored) / len(scored), 3) if scored else None


def _group(key: str, label: str, rows: list[dict[str, Any]]) -> GroupRecord:
    resolved = [r for r in rows if r["status"] == "resolved"]
    stated = [r["confidence_pct"] for r in rows if r["confidence_pct"] is not None]
    return GroupRecord(
        key=key,
        label=label,
        resolved=len(resolved),
        hit=sum(r["outcome"] == "hit" for r in resolved),
        partly=sum(r["outcome"] == "partly" for r in resolved),
        missed=sum(r["outcome"] == "missed" for r in resolved),
        open=sum(r["status"] in ("open", "due") for r in rows),
        hit_rate=_rate(rows),
        stated_confidence=round(sum(stated) / len(stated) / 100, 3) if stated else None,
    )


async def calibration(session: AsyncSession) -> Calibration:
    """The workspace's track record, aggregated. Every live, non-dismissed bet of the tenant
    is counted (RLS); no person appears anywhere in it."""
    rows = [
        dict(r)
        for r in (
            await session.execute(
                text(
                    "SELECT b.status, b.category, b.outcome, b.confidence_pct, b.assumption, "
                    "b.assumption_held, b.lesson, b.resolved_at, b.team_id, t.name AS team_name, "
                    "m.confidential "
                    "FROM decision_bets b JOIN meetings m ON m.id = b.meeting_id "
                    "LEFT JOIN teams t ON t.id = b.team_id AND t.deleted_at IS NULL "
                    "WHERE b.deleted_at IS NULL AND b.status <> 'dismissed'"
                )
            )
        )
        .mappings()
        .all()
    ]
    resolved = [r for r in rows if r["status"] == "resolved"]
    with_p = [r for r in resolved if r["confidence_pct"] is not None]
    brier = (
        round(
            sum((r["confidence_pct"] / 100 - SCORE[r["outcome"]]) ** 2 for r in with_p)
            / len(with_p),
            3,
        )
        if with_p
        else None
    )
    totals = Totals(
        bets=len(rows),
        proposed=sum(r["status"] == "proposed" for r in rows),
        open=sum(r["status"] == "open" for r in rows),
        due=sum(r["status"] == "due" for r in rows),
        resolved=len(resolved),
        hit=sum(r["outcome"] == "hit" for r in resolved),
        partly=sum(r["outcome"] == "partly" for r in resolved),
        missed=sum(r["outcome"] == "missed" for r in resolved),
        hit_rate=_rate(rows),
        brier=brier,
    )
    counted = [r for r in rows if r["status"] != "proposed"]
    by_category = [
        _group(c, CATEGORY_LABELS[c], [r for r in counted if r["category"] == c])
        for c in CATEGORIES
        if any(r["category"] == c for r in counted)
    ]
    teams: dict[str, tuple[str, list[dict[str, Any]]]] = {}
    for r in counted:
        if r["team_id"] is not None and r["team_name"]:
            teams.setdefault(str(r["team_id"]), (r["team_name"], []))[1].append(r)
    team_groups = [_group(k, name, rs) for k, (name, rs) in teams.items()]
    shown = sorted(
        (g for g in team_groups if g.resolved >= MIN_TEAM_RESOLVED), key=lambda g: g.label
    )
    buckets: list[CalibrationBucket] = []
    for low, high in _BUCKETS:
        inside = [r for r in with_p if low <= r["confidence_pct"] <= high]
        if inside:
            buckets.append(
                CalibrationBucket(
                    label=f"{low}-{high}%" if low > 1 else "Under 50%",
                    low=low,
                    high=high,
                    count=len(inside),
                    stated=round(sum(r["confidence_pct"] for r in inside) / len(inside) / 100, 3),
                    actual=round(sum(SCORE[r["outcome"]] for r in inside) / len(inside), 3),
                )
            )
    shareable = [r for r in resolved if not r["confidential"]]
    failing: list[FailingAssumption] = []
    for r in sorted(shareable, key=lambda r: r["resolved_at"], reverse=True):
        assumption = r["assumption"]
        if not assumption or r["assumption_held"] is not False:
            continue
        for f in failing:
            if similar(f.assumption, assumption):
                f.times += 1
                if r["category"] not in f.categories:
                    f.categories.append(r["category"])
                break
        else:
            failing.append(
                FailingAssumption(
                    assumption=assumption,
                    times=1,
                    categories=[r["category"]],
                    last_failed_at=r["resolved_at"],
                )
            )
    failing.sort(key=lambda f: (-f.times, -f.last_failed_at.timestamp()))
    lessons = [
        PublicLesson(
            category=r["category"],
            outcome=r["outcome"],
            lesson=r["lesson"],
            resolved_at=r["resolved_at"],
        )
        for r in sorted(shareable, key=lambda r: r["resolved_at"], reverse=True)
        if r["lesson"]
    ][:12]
    months: dict[str, list[dict[str, Any]]] = {}
    for r in resolved:
        months.setdefault(r["resolved_at"].strftime("%Y-%m"), []).append(r)
    points = [MonthPoint(month=k, resolved=len(v), hit_rate=_rate(v)) for k, v in months.items()]
    points.sort(key=lambda p: p.month)
    return Calibration(
        totals=totals,
        by_category=by_category,
        by_team=shown,
        teams_below_threshold=len(team_groups) - len(shown),
        buckets=buckets,
        failing_assumptions=failing[:8],
        lessons=lessons,
        months=points[-12:],
    )


async def hints_for(
    session: AsyncSession,
    *,
    category: str | None,
    content: str,
    viewer: uuid.UUID,
    exclude_decision: uuid.UUID | None = None,
) -> BetHints:
    """What earlier calls like this one taught: the category's record and up to three
    lessons from resolved bets of the same type or with words in common. Lessons come from
    non-confidential meetings only; the decision link only for meetings the viewer was in."""
    words = keywords(content)
    q = tsquery(words)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT b.category, b.statement, b.outcome, b.lesson, b.assumption, "
                    "b.assumption_held, b.resolved_at, b.decision_id, b.meeting_id, "
                    "EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = "
                    "  b.meeting_id AND p.user_id = :v) AS mine, "
                    "CASE WHEN CAST(:q AS text) IS NULL THEN 0 ELSE ts_rank("
                    "  to_tsvector('english', b.statement || ' ' || coalesce(b.lesson, '') || ' ' "
                    "  || coalesce(b.assumption, '')), to_tsquery('english', CAST(:q AS text))) "
                    "END AS rank "
                    "FROM decision_bets b JOIN meetings m ON m.id = b.meeting_id "
                    "WHERE b.deleted_at IS NULL AND b.status = 'resolved' "
                    "AND NOT m.confidential "
                    "AND (CAST(:d AS uuid) IS NULL OR b.decision_id <> CAST(:d AS uuid)) "
                    "AND (b.lesson IS NOT NULL OR b.assumption_held = false) "
                    "AND (b.category = CAST(:c AS text) OR (CAST(:q AS text) IS NOT NULL AND "
                    "  to_tsvector('english', b.statement || ' ' || coalesce(b.lesson, '') || ' ' "
                    "  || coalesce(b.assumption, '')) @@ to_tsquery('english', CAST(:q AS text)))) "
                    "ORDER BY rank DESC, b.resolved_at DESC LIMIT 3"
                ),
                {
                    "v": str(viewer),
                    "q": q,
                    "c": category,
                    "d": str(exclude_decision) if exclude_decision else None,
                },
            )
        )
        .mappings()
        .all()
    )
    lessons = [
        Lesson(
            category=r["category"],
            statement=r["statement"],
            outcome=r["outcome"],
            lesson=r["lesson"],
            failed_assumption=r["assumption"] if r["assumption_held"] is False else None,
            resolved_at=r["resolved_at"],
            decision_id=r["decision_id"] if r["mine"] else None,
            meeting_id=r["meeting_id"] if r["mine"] else None,
        )
        for r in rows
    ]
    record = None
    if category is not None:
        counts = (
            (
                await session.execute(
                    text(
                        "SELECT count(*) AS resolved, "
                        "count(*) FILTER (WHERE outcome = 'hit') AS hit, "
                        "count(*) FILTER (WHERE outcome = 'partly') AS partly, "
                        "count(*) FILTER (WHERE outcome = 'missed') AS missed "
                        "FROM decision_bets WHERE deleted_at IS NULL AND status = 'resolved' "
                        "AND category = :c"
                    ),
                    {"c": category},
                )
            )
            .mappings()
            .one()
        )
        if counts["resolved"]:
            n = int(counts["resolved"])
            record = CategoryRecord(
                category=category,  # type: ignore[arg-type]
                resolved=n,
                hit=int(counts["hit"]),
                partly=int(counts["partly"]),
                missed=int(counts["missed"]),
                hit_rate=round((int(counts["hit"]) + 0.5 * int(counts["partly"])) / n, 3),
            )
    return BetHints(category=category, record=record, lessons=lessons)  # type: ignore[arg-type]
