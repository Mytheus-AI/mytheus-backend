"""Decisions as bets: the check on the date, and DecisionAgent's proposals.

The daily `DecisionBetsWorkflow` (workflow-service, schedule `decision-bets-daily`) runs per
tenant:

1. `GET /internal/bets/due?as_of=` here: open bets whose check date has come, each with what
   the check needs from outside core-api (its CRM metric key, the decision date, and the
   confirmed actions of the decision's meeting for Jira).
2. integration-service, for each bet: `POST /internal/crm/metric` (a HubSpot reading, or
   `connected: false`) and `POST /internal/jira/issue-status` (the meeting's Jira outcomes, or
   `connected: false`).
3. `POST /internal/bets/{bet_id}/check` here with those answers. core-api adds what it holds
   itself (later decisions and meetings that mention the bet, the decision that replaced it),
   stores every piece as evidence (UNIQUE (bet_id, kind, ref), so a re-run adds nothing twice),
   fills `suggested_outcome` when a connected metric gives a clear answer, moves the bet to
   `due` and asks its owner, once, to resolve it. Nothing is ever resolved here.

Service-to-service (`internal_lifecycle.workflow_caller`), caller `workflow-service`.

`persist_bet_proposals` is the agent-results side (`consumer.py`): DecisionAgent's after-meeting
`decision_candidate` record whose payload carries `expectations` instead of `candidates`.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, time
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api import bets
from services.core_api.routes.internal_lifecycle import workflow_caller

router = APIRouter(tags=["core-internal"])

PRODUCER = "core-api"
MAX_DUE = 200
MAX_ACTIONS = 50
MAX_JIRA_ROWS = 10
MAX_EXPECTATIONS = 20


def _day(d: date) -> str:
    """`Aug 11, 2026`, the way the app writes a day."""
    return f"{d:%b} {d.day}, {d.year}"


def _short(value: Any, limit: int) -> str:
    s = " ".join(str(value or "").split())
    return s if len(s) <= limit else s[: limit - 1].rstrip() + "…"


async def notify(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    user_id: uuid.UUID,
    source: str,
    title: str,
    body: str,
    link: str,
    priority: int = 2,
) -> None:
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": title[:200],
                "body": body[:2000],
                "link": link,
            },
        ),
    )


# --- 1. what is due -------------------------------------------------------------------------


class DueBet(BaseModel):
    bet_id: uuid.UUID
    decision_id: uuid.UUID
    source: bets.Source
    metric_key: str | None = None
    decided_on: date
    check_date: date
    action_ids: list[uuid.UUID]


class DueBets(BaseModel):
    items: list[DueBet]


@router.get("/internal/bets/due", response_model=DueBets)
async def due_bets(
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
    as_of: Annotated[date, Query()],
) -> DueBets:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT b.id, b.decision_id, b.source, b.metric_key, b.check_date, "
                        "d.created_at, b.meeting_id FROM decision_bets b "
                        "JOIN decisions d ON d.id = b.decision_id AND d.deleted_at IS NULL "
                        "WHERE b.deleted_at IS NULL AND b.status = 'open' "
                        "AND b.check_date <= :d AND d.status <> 'dismissed' "
                        "ORDER BY b.check_date, b.id LIMIT :lim"
                    ),
                    {"d": as_of, "lim": MAX_DUE},
                )
            )
            .mappings()
            .all()
        )
        items: list[DueBet] = []
        for r in rows:
            actions = (
                await session.execute(
                    text(
                        "SELECT id FROM action_items WHERE meeting_id = :m "
                        "AND deleted_at IS NULL AND status IN ('confirmed', 'done') "
                        "ORDER BY created_at, id LIMIT :lim"
                    ),
                    {"m": str(r["meeting_id"]), "lim": MAX_ACTIONS},
                )
            ).scalars()
            items.append(
                DueBet(
                    bet_id=r["id"],
                    decision_id=r["decision_id"],
                    source=r["source"],
                    metric_key=r["metric_key"],
                    decided_on=r["created_at"].date(),
                    check_date=r["check_date"],
                    action_ids=list(actions),
                )
            )
        return DueBets(items=items)


# --- 2. the check ---------------------------------------------------------------------------


class MetricReading(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connected: bool
    provider: str | None = Field(default=None, max_length=40)
    label: str | None = Field(default=None, max_length=120)
    unit: str | None = Field(default=None, max_length=20)
    value: float | None = None
    baseline_value: float | None = None
    window_start: date | None = None
    window_end: date | None = None
    error: str | None = Field(default=None, max_length=200)


class JiraIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_id: uuid.UUID
    issue_key: str = Field(min_length=1, max_length=100)
    issue_url: str | None = Field(default=None, max_length=500)
    open: bool


class JiraOutcome(BaseModel):
    """The tenant's tracker outcome. Named for Jira on the wire; since ADR-0044 it may be
    Linear, and `provider` says which (evidence `kind` stays `jira`, meaning the tracker)."""

    model_config = ConfigDict(extra="forbid")

    connected: bool
    provider: Literal["jira", "linear"] | None = None
    issues: list[JiraIssue] = Field(default_factory=list, max_length=MAX_ACTIONS)


class CheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    as_of: date
    metric: MetricReading | None = None
    jira: JiraOutcome | None = None


class CheckResult(BaseModel):
    status: str
    evidence_added: int
    suggested_outcome: str | None = None
    notified: bool


async def _evidence(session: AsyncSession, org_id: uuid.UUID, bet_id: Any, **row: Any) -> int:
    added = (
        await session.execute(
            text(
                "INSERT INTO decision_bet_evidence (org_id, bet_id, kind, ref, title, detail, "
                "url, meeting_id, decision_id, observed_value, signal, occurred_at) VALUES "
                "(:o, :b, :kind, :ref, :title, :detail, :url, :meeting, :decision, :value, "
                ":signal, :at) ON CONFLICT (bet_id, kind, ref) DO NOTHING RETURNING 1"
            ),
            {
                "o": str(org_id),
                "b": str(bet_id),
                "kind": row["kind"],
                "ref": row["ref"][:120],
                "title": _short(row["title"], 300),
                "detail": _short(row["detail"], 600) if row.get("detail") else None,
                "url": row.get("url"),
                "meeting": str(row["meeting"]) if row.get("meeting") else None,
                "decision": str(row["decision"]) if row.get("decision") else None,
                "value": row.get("value"),
                "signal": row.get("signal"),
                "at": row.get("at"),
            },
        )
    ).first()
    return 1 if added else 0


async def _internal_evidence(session: AsyncSession, org_id: uuid.UUID, bet: Any) -> int:
    """Later decisions and meetings that mention the bet, and the decision that replaced it.
    Confidential meetings are never searched."""
    added = 0
    since: datetime = bet["decided_at"]
    q = bets.tsquery(bets.keywords(bet["metric"], bet["statement"]))
    replaced = (
        (
            await session.execute(
                text(
                    "SELECT d.id, d.content, d.meeting_id, d.created_at FROM decisions d "
                    "JOIN meetings m ON m.id = d.meeting_id AND m.deleted_at IS NULL "
                    "AND NOT m.confidential WHERE d.supersedes = :d AND d.deleted_at IS NULL "
                    "AND d.status IN ('confirmed', 'superseded') ORDER BY d.created_at LIMIT 3"
                ),
                {"d": str(bet["decision_id"])},
            )
        )
        .mappings()
        .all()
    )
    for r in replaced:
        added += await _evidence(
            session,
            org_id,
            bet["id"],
            kind="decision",
            ref=f"replaced:{r['id']}",
            title=f"Replaced by a later decision: {r['content']}",
            meeting=r["meeting_id"],
            decision=r["id"],
            signal="against",
            at=r["created_at"],
        )
    if q is None:
        return added
    later = (
        (
            await session.execute(
                text(
                    "SELECT d.id, d.content, d.rationale, d.meeting_id, d.created_at "
                    "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                    "AND m.deleted_at IS NULL AND NOT m.confidential "
                    "WHERE d.deleted_at IS NULL AND d.status IN ('confirmed', 'superseded') "
                    "AND d.id <> :d AND d.created_at > :since "
                    "AND (d.supersedes IS NULL OR d.supersedes <> :d) "
                    "AND to_tsvector('english', d.content || ' ' || coalesce(d.rationale, '')) "
                    "  @@ to_tsquery('english', :q) "
                    "ORDER BY ts_rank(to_tsvector('english', d.content || ' ' || "
                    "  coalesce(d.rationale, '')), to_tsquery('english', :q)) DESC, "
                    "  d.created_at DESC LIMIT :lim"
                ),
                {
                    "d": str(bet["decision_id"]),
                    "since": since,
                    "q": q,
                    "lim": bets.MAX_EVIDENCE_SEARCH,
                },
            )
        )
        .mappings()
        .all()
    )
    for r in later:
        added += await _evidence(
            session,
            org_id,
            bet["id"],
            kind="decision",
            ref=f"mentions:{r['id']}",
            title=r["content"],
            detail=r["rationale"],
            meeting=r["meeting_id"],
            decision=r["id"],
            signal="neutral",
            at=r["created_at"],
        )
    meetings = (
        (
            await session.execute(
                text(
                    "SELECT mm.meeting_id, m.title, mm.content->>'summary' AS summary, "
                    "COALESCE(m.scheduled_start, m.created_at) AS at FROM meeting_minutes mm "
                    "JOIN meetings m ON m.id = mm.meeting_id AND m.deleted_at IS NULL "
                    "AND NOT m.confidential WHERE mm.user_id IS NULL AND mm.status = 'ready' "
                    "AND mm.meeting_id <> :m "
                    "AND COALESCE(m.scheduled_start, m.created_at) > :since "
                    "AND to_tsvector('english', mm.content::text) @@ to_tsquery('english', :q) "
                    "ORDER BY COALESCE(m.scheduled_start, m.created_at) DESC LIMIT :lim"
                ),
                {
                    "m": str(bet["meeting_id"]),
                    "since": since,
                    "q": q,
                    "lim": bets.MAX_EVIDENCE_SEARCH,
                },
            )
        )
        .mappings()
        .all()
    )
    for r in meetings:
        added += await _evidence(
            session,
            org_id,
            bet["id"],
            kind="meeting",
            ref=f"minutes:{r['meeting_id']}",
            title=r["title"] or "A later meeting",
            detail=r["summary"],
            meeting=r["meeting_id"],
            signal="neutral",
            at=r["at"],
        )
    return added


@router.post("/internal/bets/{bet_id}/check", response_model=CheckResult)
async def check_bet(
    bet_id: uuid.UUID,
    body: CheckRequest,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> CheckResult:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        bet = (
            (
                await session.execute(
                    text(
                        "SELECT b.*, d.created_at AS decided_at, d.content AS decision_content, "
                        "COALESCE((SELECT p.user_id FROM meeting_participants p "
                        "  WHERE p.meeting_id = b.meeting_id AND p.role_in_meeting = 'host' "
                        "  ORDER BY p.user_id LIMIT 1), m.created_by) AS host_id "
                        "FROM decision_bets b JOIN decisions d ON d.id = b.decision_id "
                        "JOIN meetings m ON m.id = b.meeting_id "
                        "WHERE b.id = :b AND b.deleted_at IS NULL FOR UPDATE OF b"
                    ),
                    {"b": str(bet_id)},
                )
            )
            .mappings()
            .first()
        )
        if bet is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        if bet["status"] not in ("open", "due"):
            return CheckResult(status=bet["status"], evidence_added=0, notified=False)
        added = 0
        suggested: str | None = None
        reason: str | None = None
        observed: float | None = None
        baseline: float | None = float(bet["baseline"]) if bet["baseline"] is not None else None
        stamp = datetime.combine(body.as_of, time(12), tzinfo=UTC)
        m = body.metric
        if bet["source"] in ("crm", "metrics"):
            if m is None or not m.connected:
                where = "HubSpot" if bet["source"] == "crm" else "A metrics source"
                added += await _evidence(
                    session,
                    org_id,
                    bet_id,
                    kind="metric",
                    ref="metric:not_connected",
                    title=f"{where} is not connected, so the number was not read",
                    detail=(
                        "Connect it in Workspace, Integrations to check this automatically. "
                        "You can still resolve the bet from what you know."
                    ),
                    signal="neutral",
                    at=stamp,
                )
            elif m.value is None:
                added += await _evidence(
                    session,
                    org_id,
                    bet_id,
                    kind="metric",
                    ref=f"metric:unavailable:{body.as_of}",
                    title=f"{m.label or bet['metric']} could not be read on {body.as_of}",
                    detail=m.error,
                    signal="neutral",
                    at=stamp,
                )
            else:
                observed = m.value
                if baseline is None:
                    baseline = m.baseline_value
                if bet["kind"] == "metric":
                    suggested, reason = bets.suggest_outcome(
                        direction=bet["direction"],
                        magnitude=float(bet["magnitude"]) if bet["magnitude"] is not None else None,
                        unit=bet["unit"] if bet["unit"] else m.unit,
                        baseline=baseline,
                        observed=m.value,
                    )
                span = (
                    f" from {_day(m.window_start)} to {_day(m.window_end)}"
                    if m.window_start and m.window_end
                    else ""
                )
                before = (
                    f"; {m.baseline_value:g} in the same span before the decision"
                    if m.baseline_value is not None
                    else ""
                )
                added += await _evidence(
                    session,
                    org_id,
                    bet_id,
                    kind="metric",
                    ref=f"metric:{m.provider or 'crm'}:{body.as_of}",
                    title=f"{m.label or bet['metric']}: {m.value:g}{span}",
                    detail=(reason or "") + before if (reason or before) else None,
                    value=m.value,
                    signal=(
                        "supports"
                        if suggested == "hit"
                        else "against" if suggested == "missed" else "neutral"
                    ),
                    at=stamp,
                )
        j = body.jira
        tracker = "Linear" if j is not None and j.provider == "linear" else "Jira"
        if j is not None and j.connected and j.issues:
            done = [i for i in j.issues if not i.open]
            added += await _evidence(
                session,
                org_id,
                bet_id,
                kind="jira",
                ref=f"jira:summary:{body.as_of}",
                title=(
                    f"{len(done)} of {len(j.issues)} {tracker} issues from this meeting are done"
                ),
                detail=f"The work the meeting agreed on, as {tracker} sees it on the check date.",
                signal="supports" if len(done) == len(j.issues) else "neutral",
                at=stamp,
            )
            for i in j.issues[:MAX_JIRA_ROWS]:
                added += await _evidence(
                    session,
                    org_id,
                    bet_id,
                    kind="jira",
                    ref=f"jira:{i.issue_key}",
                    title=f"{i.issue_key} is {'still open' if i.open else 'done'}",
                    url=i.issue_url,
                    signal="neutral",
                    at=stamp,
                )
        elif bet["source"] == "jira" and (j is None or not j.connected):
            added += await _evidence(
                session,
                org_id,
                bet_id,
                kind="jira",
                ref="jira:not_connected",
                title="No tracker is connected, so the work's status was not read",
                detail="Connect Jira or Linear in Workspace, Integrations to see it here.",
                signal="neutral",
                at=stamp,
            )
        added += await _internal_evidence(session, org_id, bet)
        await session.execute(
            text(
                "UPDATE decision_bets SET status = 'due', evidence_status = 'gathered', "
                "checked_at = now(), suggested_outcome = COALESCE(:s, suggested_outcome), "
                "suggested_reason = COALESCE(:r, suggested_reason), "
                "observed_value = COALESCE(:v, observed_value), "
                "baseline = COALESCE(baseline, :base), updated_at = now() WHERE id = :b"
            ),
            {
                "s": suggested,
                "r": _short(reason, 300) if reason else None,
                "v": observed,
                "base": baseline,
                "b": str(bet_id),
            },
        )
        owner = bet["owner_user_id"] or bet["host_id"]
        notified = False
        if owner is not None and bet["status"] == "open":
            statement = _short(bet["statement"], 120)
            hint = f" The numbers suggest {suggested}: {reason}." if suggested and reason else ""
            await notify(
                session,
                org_id,
                user_id=owner,
                source=f"decision-bet:{bet_id}:due:{bet['check_date']}",
                title=f"Time to score a bet: {statement}",
                body=(
                    f'The check date for "{_short(bet["decision_content"], 140)}" has come. '
                    f"The evidence is gathered; say whether it hit, partly hit or missed.{hint}"
                ),
                link=f"/meetings/{bet['meeting_id']}/decisions/{bet['decision_id']}#bet",
            )
            notified = True
        return CheckResult(
            status="due", evidence_added=added, suggested_outcome=suggested, notified=notified
        )


# --- DecisionAgent's proposals (agent-results) ---------------------------------------------


def _num(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    return float(raw) if abs(raw) <= 1e12 else None


def _text(raw: Any, limit: int) -> str | None:
    if not isinstance(raw, str):
        return None
    s = " ".join(raw.split())
    return s[:limit] if s else None


def parse_expectation(raw: Any) -> dict[str, Any] | None:
    """One proposal from the payload, or None when it is not a usable bet. Tolerant: bad
    optional fields are dropped, a bad required field drops the proposal."""
    if not isinstance(raw, dict):
        return None
    try:
        decision_id = uuid.UUID(str(raw.get("decision_id")))
        check = date.fromisoformat(str(raw.get("check_date")))
    except ValueError:
        return None
    statement, metric = _text(raw.get("statement"), 300), _text(raw.get("metric"), 120)
    kind = raw.get("kind") if raw.get("kind") in ("metric", "event") else "metric"
    direction = raw.get("direction")
    allowed = bets.METRIC_DIRECTIONS if kind == "metric" else bets.EVENT_DIRECTIONS
    if not statement or not metric or direction not in allowed:
        return None
    magnitude = _num(raw.get("magnitude")) if kind == "metric" else None
    if direction == "reach" and magnitude is None:
        return None
    source = (
        raw.get("source")
        if raw.get("source") in ("crm", "metrics", "meetings", "jira")
        else "meetings"
    )
    category = raw.get("category") if raw.get("category") in bets.CATEGORIES else "other"
    confidence = _num(raw.get("confidence"))
    start = raw.get("quote_start_ms")
    return {
        "decision_id": decision_id,
        "statement": statement,
        "kind": kind,
        "metric": metric,
        "direction": direction,
        "magnitude": magnitude,
        "unit": _text(raw.get("unit"), 20) if kind == "metric" else None,
        "check_date": check,
        "source": source,
        "category": category,
        "assumption": _text(raw.get("assumption"), 300),
        "quote": _text(raw.get("quote"), 300),
        "quote_start_ms": (
            start if isinstance(start, int) and not isinstance(start, bool) and start >= 0 else None
        ),
        "confidence": max(0.0, min(1.0, confidence)) if confidence is not None else None,
    }


async def persist_bet_proposals(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    payload: dict[str, Any],
) -> bool:
    """Insert each proposal as a `proposed` bet on a decision of THIS meeting that has none.

    The owner is the person who confirmed the decision, else the meeting's host; they get one
    private in-app note per proposal. A decision that already has a bet (a person's, or an
    earlier proposal) is left alone: a replayed record is a no-op."""
    prompt_version = _text(payload.get("prompt_version"), 60)
    raw = payload.get("expectations")
    written = False
    for item in (raw if isinstance(raw, list) else [])[:MAX_EXPECTATIONS]:
        e = parse_expectation(item)
        if e is None:
            continue
        decision = (
            (
                await session.execute(
                    text(
                        "SELECT d.id, d.content, d.confirmed_by, d.quote_speaker_id, "
                        "COALESCE((SELECT p.user_id FROM meeting_participants p "
                        "  WHERE p.meeting_id = d.meeting_id AND p.role_in_meeting = 'host' "
                        "  ORDER BY p.user_id LIMIT 1), m.created_by) AS host_id "
                        "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                        "WHERE d.id = :d AND d.meeting_id = :m AND d.deleted_at IS NULL "
                        "AND d.status <> 'dismissed'"
                    ),
                    {"d": str(e["decision_id"]), "m": str(meeting_id)},
                )
            )
            .mappings()
            .first()
        )
        if decision is None:
            continue
        owner = decision["confirmed_by"] or decision["host_id"]
        team = await bets.first_team(session, owner)
        bet_id = (
            await session.execute(
                text(
                    "INSERT INTO decision_bets (org_id, decision_id, meeting_id, owner_user_id, "
                    "team_id, status, statement, kind, metric, direction, magnitude, unit, "
                    "check_date, source, category, assumption, quote, quote_speaker_id, "
                    "quote_start_ms, proposed_by, prompt_version, agent_confidence) VALUES "
                    "(:o, :d, :m, :owner, :team, 'proposed', :statement, :kind, :metric, "
                    ":direction, :magnitude, :unit, :check_date, :source, :category, "
                    ":assumption, :quote, :speaker, :qstart, 'agent', :pv, :conf) "
                    "ON CONFLICT (decision_id) WHERE deleted_at IS NULL DO NOTHING RETURNING id"
                ),
                {
                    "o": str(org_id),
                    "d": str(e["decision_id"]),
                    "m": str(meeting_id),
                    "owner": str(owner) if owner else None,
                    "team": str(team) if team else None,
                    "statement": e["statement"],
                    "kind": e["kind"],
                    "metric": e["metric"],
                    "direction": e["direction"],
                    "magnitude": e["magnitude"],
                    "unit": e["unit"],
                    "check_date": e["check_date"],
                    "source": e["source"],
                    "category": e["category"],
                    "assumption": e["assumption"],
                    "quote": e["quote"],
                    "speaker": (
                        str(decision["quote_speaker_id"])
                        if e["quote"] and decision["quote_speaker_id"]
                        else None
                    ),
                    "qstart": e["quote_start_ms"],
                    "pv": prompt_version,
                    "conf": e["confidence"],
                },
            )
        ).first()
        if bet_id is None:
            continue
        written = True
        if owner is not None:
            await notify(
                session,
                org_id,
                user_id=owner,
                source=f"decision-bet:{bet_id[0]}:proposed",
                title=f"Your Myte heard an expected outcome: {_short(e['statement'], 120)}",
                body=(
                    f'For "{_short(decision["content"], 140)}". Keep it as the bet this '
                    f"decision is checked against on {e['check_date']}, edit it, or dismiss it."
                ),
                link=f"/meetings/{meeting_id}/decisions/{e['decision_id']}#bet",
                priority=3,
            )
    return written
