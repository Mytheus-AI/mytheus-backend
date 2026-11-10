"""Agents that learn from confirms and dismisses (polish program wave 2).

Three things, all core-api's, all on a session with `app.current_org` bound:

1. **Feedback records.** `record_feedback` writes one `agent_feedback` row for a person's
   confirm, dismiss, edit (with the before/after diff), ask or save of something an agent
   proposed, enqueues the `agent-feedback` event in the same transaction (outbox, key
   `feedback:{id}`), and recalibrates the agent's thresholds. The routes call it only on a real
   transition: a repeated confirm or dismiss that changes nothing records nothing.

2. **Calibration.** Per agent, for the workspace and (for the agents that run for one person:
   `USER_SCOPED_AGENTS`) per person, an adjusted surface threshold from the recent confirm and
   dismiss rates:

   - one sample per (person, item): the person's LATEST action on it (an edit then a confirm is
     one positive; a save then a dismiss is one negative), from the last `WINDOW_DAYS` days,
     weighted by recency (`HALF_LIFE_DAYS` half-life);
   - the decayed dismiss share `r = negatives / (positives + negatives)` moves the base
     threshold by `GAIN * (r - TARGET_DISMISS)`, shrunk toward zero for small samples
     (`n / (n + SHRINK_K)`), at most `MAX_LOWER` down and `MAX_RAISE` up;
   - bounded to `[SURFACE_FLOOR, SURFACE_CEIL]` (widened to include the base when an admin set
     a stricter or looser one), and quantised to `STEP` so a single new feedback rarely moves
     it: the agent caches the policy for 60 s, so a threshold that moved on every click would
     make the live card and the reload disagree more often;
   - nothing below `MIN_SAMPLES_WORKSPACE` / `MIN_SAMPLES_USER` items: the tenant's base
     applies (`agent_calibration.surface` is NULL, kept for observability).

   The decayed counts are stored per (agent, person or workspace) when feedback is recorded;
   the threshold is computed from them and the CURRENT base on every read, so an admin who
   changes `agent_confidence_overrides` moves the calibrated value with it at once.

   The base is the tenant's `agent_confidence_overrides` entry, else the doc 08 default 0.7.
   `silent` is the base's, never above the calibrated `surface`.

3. **One threshold source.** `band_for` is what every core-api reader and the agent-results
   consumer use, and `agent_policy` is what `GET /internal/agent-policy` serves to the agent:
   the same numbers, so the agent's live `surfaced` (which realtime-gateway forwards) and
   core-api's store and reload agree. Decision and action candidates are one card for the whole
   meeting, so they only ever use the workspace value; notes, suggestions and context fills are
   one person's, so they use that person's value when they have one.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.confidence import SILENT_FROM, SURFACE_ABOVE, thresholds

PRODUCER = "core-api"
TOPIC = "agent-feedback"

KINDS = frozenset({"decision", "action", "suggestion", "note", "context_fill"})
ACTIONS = frozenset({"confirm", "dismiss", "edit", "ask", "save"})
# Agents whose output is one person's (a note, a suggestion, a context fill): calibrated per
# person as well as per workspace. Decision and action candidates are one card for everyone.
USER_SCOPED_AGENTS = frozenset({"PersonalListenerAgent", "QuestionAgent", "ContextGapAgent"})

SURFACE_FLOOR = 0.6
SURFACE_CEIL = 0.85
TARGET_DISMISS = 0.35
GAIN = 0.4
MAX_LOWER = 0.10
MAX_RAISE = 0.15
SHRINK_K = 10.0
HALF_LIFE_DAYS = 14.0
WINDOW_DAYS = 90
MIN_SAMPLES_WORKSPACE = 20
MIN_SAMPLES_USER = 8
STEP = 0.025

MAX_CONTENT = 280
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PHONE = re.compile(r"(?<!\w)\+?\d[\d\s().-]{7,}\d(?!\w)")


def redact(raw: object, limit: int = MAX_CONTENT) -> str | None:
    """Whitespace-flattened, emails and phone numbers masked (the markers the agent tier's own
    redaction uses), at most `limit` characters; None for empty or non-text."""
    if not isinstance(raw, str):
        return None
    flat = " ".join(raw.split())
    if not flat:
        return None
    flat = _EMAIL.sub("[REDACTED_EMAIL]", flat)
    flat = _PHONE.sub("[REDACTED_PHONE]", flat)
    return flat if len(flat) <= limit else flat[: limit - 1].rstrip() + "…"


def edit_diff(before: Mapping[str, Any], after: Mapping[str, Any]) -> dict[str, Any]:
    """`{field: {"before": x, "after": y}}` for the fields whose value changed. Text is redacted
    and bounded; anything else (an assignee id, a date) is its string form."""
    out: dict[str, Any] = {}
    for key, new in after.items():
        old = before.get(key)
        if old == new:
            continue
        out[key] = {"before": _diff_value(old), "after": _diff_value(new)}
    return out


def _diff_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return redact(value) or ""
    return str(value)


# --- calibration ------------------------------------------------------------------------------


def _quantise(x: float) -> float:
    return round(round(x / STEP) * STEP, 4)


def calibrated_surface(
    base: float, positives: float, negatives: float, samples: int, min_samples: int
) -> float | None:
    """The adjusted surface threshold, or None when there are too few samples to move it."""
    weight = positives + negatives
    if samples < min_samples or weight <= 0:
        return None
    dismiss_rate = negatives / weight
    shrink = weight / (weight + SHRINK_K)
    delta = shrink * GAIN * (dismiss_rate - TARGET_DISMISS)
    delta = max(-MAX_LOWER, min(MAX_RAISE, delta))
    lo, hi = min(SURFACE_FLOOR, base), max(SURFACE_CEIL, base)
    return max(lo, min(hi, _quantise(base + delta)))


_COUNTS_SQL = (
    "SELECT count(*) AS samples, "
    "COALESCE(sum(w) FILTER (WHERE latest <> 'dismiss'), 0) AS positives, "
    "COALESCE(sum(w) FILTER (WHERE latest = 'dismiss'), 0) AS negatives "
    "FROM (SELECT (array_agg(action ORDER BY created_at DESC))[1] AS latest, "
    "  power(0.5, EXTRACT(EPOCH FROM (now() - max(created_at))) / 86400.0 / :hl) AS w "
    "  FROM agent_feedback WHERE agent = :a "
    "  AND created_at > now() - make_interval(days => :days) {user_filter} "
    "  GROUP BY user_id, candidate_id) s"
)


async def _tenant_overrides(session: AsyncSession) -> Any:
    return (
        await session.execute(text("SELECT agent_confidence_overrides FROM tenant_settings"))
    ).scalar()


async def _recalibrate_one(
    session: AsyncSession,
    org_id: uuid.UUID,
    agent: str,
    user_id: uuid.UUID | None,
    base: float,
) -> None:
    sql = _COUNTS_SQL.format(user_filter="AND user_id = :u" if user_id else "")
    params: dict[str, Any] = {"a": agent, "hl": HALF_LIFE_DAYS, "days": WINDOW_DAYS}
    if user_id:
        params["u"] = str(user_id)
    row = (await session.execute(text(sql), params)).one()
    samples, positives, negatives = int(row[0]), float(row[1]), float(row[2])
    minimum = MIN_SAMPLES_USER if user_id else MIN_SAMPLES_WORKSPACE
    surface = calibrated_surface(base, positives, negatives, samples, minimum)
    await session.execute(
        text(
            "INSERT INTO agent_calibration (org_id, user_id, agent, surface, positives, "
            "negatives, samples, computed_at) VALUES (:o, :u, :a, :s, :p, :n, :c, now()) "
            "ON CONFLICT (org_id, agent, user_id) DO UPDATE SET surface = EXCLUDED.surface, "
            "positives = EXCLUDED.positives, negatives = EXCLUDED.negatives, "
            "samples = EXCLUDED.samples, computed_at = now()"
        ),
        {
            "o": str(org_id),
            "u": str(user_id) if user_id else None,
            "a": agent,
            "s": surface,
            "p": round(positives, 4),
            "n": round(negatives, 4),
            "c": samples,
        },
    )


async def recalibrate(
    session: AsyncSession, org_id: uuid.UUID, agent: str, user_id: uuid.UUID
) -> None:
    """Recompute the workspace row for `agent`, and the person's row for a user-scoped agent."""
    base, _ = thresholds(await _tenant_overrides(session), agent)
    await _recalibrate_one(session, org_id, agent, None, base)
    if agent in USER_SCOPED_AGENTS:
        await _recalibrate_one(session, org_id, agent, user_id, base)


# --- the one threshold source -----------------------------------------------------------------


async def _calibrations(
    session: AsyncSession, agent: str | None = None, user_id: uuid.UUID | None = None
) -> list[Mapping[str, Any]]:
    clauses = ["samples > 0"]
    params: dict[str, Any] = {}
    if agent is not None:
        clauses.append("agent = :a")
        params["a"] = agent
    if user_id is not None:
        clauses.append("(user_id IS NULL OR user_id = :u)")
        params["u"] = str(user_id)
    return [
        dict(r)
        for r in (
            await session.execute(
                text(
                    "SELECT user_id, agent, positives, negatives, samples "  # noqa: S608
                    "FROM agent_calibration "
                    f"WHERE {' AND '.join(clauses)}"
                ),
                params,
            )
        )
        .mappings()
        .all()
    ]


def _resolve(
    overrides: Any,
    agent: str,
    rows: list[Mapping[str, Any]],
    user_id: uuid.UUID | None,
) -> tuple[float, float]:
    base_surface, base_silent = thresholds(overrides, agent)
    workspace = next((r for r in rows if r["agent"] == agent and r["user_id"] is None), None)
    person = None
    if user_id is not None and agent in USER_SCOPED_AGENTS:
        person = next((r for r in rows if r["agent"] == agent and r["user_id"] == user_id), None)
    surface = None
    if person is not None:
        surface = _surface_of(base_surface, person, MIN_SAMPLES_USER)
    if surface is None and workspace is not None:
        surface = _surface_of(base_surface, workspace, MIN_SAMPLES_WORKSPACE)
    if surface is None:
        surface = base_surface
    return surface, min(base_silent, surface)


def _surface_of(base: float, row: Mapping[str, Any], minimum: int) -> float | None:
    # From the stored counts and the CURRENT base, so an admin's new override applies at once.
    return calibrated_surface(
        base, float(row["positives"]), float(row["negatives"]), int(row["samples"]), minimum
    )


async def band_for(
    session: AsyncSession,
    agent: str,
    user_id: uuid.UUID | None = None,
    overrides: Any = None,
    *,
    have_overrides: bool = False,
) -> tuple[float, float]:
    """(surface_above, silent_from) for `agent`, as the agent applies it: the person's
    calibration for a user-scoped agent when they have one, else the workspace's, else the
    tenant's base. Pass `overrides` (with `have_overrides=True`) when already read."""
    if not have_overrides:
        overrides = await _tenant_overrides(session)
    rows = await _calibrations(session, agent, user_id)
    return _resolve(overrides, agent, rows, user_id)


async def agent_policy(
    session: AsyncSession,
) -> tuple[dict[str, tuple[float, float]], dict[str, dict[str, tuple[float, float]]], list[Any]]:
    """What `GET /internal/agent-policy` serves: the workspace band per agent (every agent with
    a tenant override or a calibration), the per-person bands that differ from it (user-scoped
    agents only), and the calibration rows (for the response's `calibration` detail)."""
    overrides = await _tenant_overrides(session)
    rows = await _calibrations(session)
    agents = {str(a) for a in (overrides if isinstance(overrides, dict) else {})}
    agents |= {str(r["agent"]) for r in rows}
    workspace = {a: _resolve(overrides, a, rows, None) for a in sorted(agents)}
    per_user: dict[str, dict[str, tuple[float, float]]] = {}
    for r in rows:
        if r["user_id"] is None or r["agent"] not in USER_SCOPED_AGENTS:
            continue
        band = _resolve(overrides, r["agent"], rows, r["user_id"])
        if band != workspace.get(r["agent"], (SURFACE_ABOVE, SILENT_FROM)):
            per_user.setdefault(str(r["user_id"]), {})[r["agent"]] = band
    return workspace, per_user, rows


# --- recording --------------------------------------------------------------------------------


async def record_feedback(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    meeting_id: uuid.UUID | None,
    agent: str,
    kind: str,
    candidate_id: uuid.UUID,
    action: str,
    confidence: float | None = None,
    content: str | None = None,
    diff: dict[str, Any] | None = None,
) -> uuid.UUID:
    """One `agent_feedback` row + its `agent-feedback` outbox row + recalibration, in the
    caller's transaction (so a rolled-back confirm records nothing)."""
    if kind not in KINDS or action not in ACTIONS:
        raise ValueError(f"unknown feedback {kind}/{action}")
    feedback_id = uuid.uuid4()
    snapshot = redact(content)
    conf = None if confidence is None else max(0.0, min(1.0, float(confidence)))
    diff_json = json.dumps(diff) if diff else None
    await session.execute(
        text(
            "INSERT INTO agent_feedback (id, org_id, user_id, meeting_id, agent, kind, "
            "candidate_id, action, confidence, content, edit_diff) VALUES (:id, :o, :u, :m, "
            ":a, :k, :c, :act, :conf, :content, CAST(:diff AS jsonb))"
        ),
        {
            "id": str(feedback_id),
            "o": str(org_id),
            "u": str(user_id),
            "m": str(meeting_id) if meeting_id else None,
            "a": agent,
            "k": kind,
            "c": str(candidate_id),
            "act": action,
            "conf": conf,
            "content": snapshot,
            "diff": diff_json,
        },
    )
    await enqueue(
        session,
        EventEnvelope(
            topic=TOPIC,
            payload={
                "feedback_id": str(feedback_id),
                "user_id": str(user_id),
                "meeting_id": str(meeting_id) if meeting_id else None,
                "agent": agent,
                "kind": kind,
                "candidate_id": str(candidate_id),
                "action": action,
                "confidence": conf,
                "content": snapshot,
                "edit_json": diff_json,
            },
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"feedback:{feedback_id}",
        ),
    )
    await recalibrate(session, org_id, agent, user_id)
    return feedback_id
