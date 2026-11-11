"""Meeting minutes: the shapes, the consumer's validation and the reads (polish program,
"Minutes for everyone").

After a meeting ends, `agent-runtime-service` writes one set of minutes per participant and one
shared, meeting-level set (`MeetingAgent`, kind `minutes` on `agent-results`; the payload is
documented in mytheus-events `schemas/agent-results/v1.avsc`). core-api is the one writer of
`meeting_minutes`:

- `persist` validates and clips the payload (never trusts it), and upserts ONE row per
  (meeting, participant) plus ONE shared row (`user_id` NULL). A later generation (the meeting
  ended again) replaces the row in place; an older generation arriving late does not overwrite a
  newer one.
- A personal row is private: the routes serve it to its own `user_id` only. The shared row
  carries no promises, ever (stripped here even if a producer sent some).
- Owners and askers are kept only when they are participants of the meeting, so a stray id from
  the model can never name someone outside it.

The shared minutes feed four other places, all read through `shared_minutes`: the next meeting's
carry-forward (`routes/meetings.py`, `routes/internal_agents.py`), the brief (`bff`), the drafted
agenda, and the public recap and follow-up email (`records.py`, `routes/followups.py`).
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.core_api.room_signals import LateContribution

MinutesScope = Literal["personal", "shared"]
MinutesStatus = Literal["ready", "empty", "unavailable"]
MinutesReason = Literal["no_transcript", "budget", "model_error"]
DecisionStatus = Literal["agreed", "tentative", "deferred", "reversed"]

MAX_ITEMS = 20
MAX_TEXT = 300
MAX_SUMMARY = 600
MAX_NAME = 120
MAX_DUE = 60
_PROMPT_RE = re.compile(r"^[a-z_]+@v\d+$")
_DECISION_STATUSES = frozenset({"agreed", "tentative", "deferred", "reversed"})
_REASONS = frozenset({"no_transcript", "budget", "model_error"})


class MinutesDecision(BaseModel):
    text: str
    status: DecisionStatus
    decision_id: uuid.UUID | None = Field(
        default=None, description="The decision record this matches, when there is one."
    )


class MinutesOpenItem(BaseModel):
    text: str
    owner_user_id: uuid.UUID | None = None
    owner_name: str | None = None
    due: str | None = Field(default=None, description="As said in the meeting, e.g. 'Friday'.")


class MinutesQuestion(BaseModel):
    text: str
    asked_by_user_id: uuid.UUID | None = None
    asked_by_name: str | None = None


class MinutesPromise(BaseModel):
    text: str
    to_name: str | None = None
    due: str | None = None


class MeetingMinutes(BaseModel):
    """One set of minutes. `promises` is always empty on the shared minutes."""

    scope: MinutesScope
    status: MinutesStatus
    reason: MinutesReason | None = None
    summary: str
    decisions: list[MinutesDecision]
    open_items: list[MinutesOpenItem]
    unanswered_questions: list[MinutesQuestion]
    promises: list[MinutesPromise]
    prompt_version: str
    created_at: datetime
    updated_at: datetime


MinutesState = Literal["ready", "pending", "unavailable", "not_ended"]


class MeetingMinutesView(BaseModel):
    """`GET /api/v1/meetings/{id}/minutes`: the caller's own minutes and the shared minutes.

    `state`: `not_ended` before the meeting is over; `pending` once it ended and before the
    shared minutes landed; `ready` when they did; `unavailable` when none could be made."""

    meeting_id: uuid.UUID
    state: MinutesState
    mine: MeetingMinutes | None = None
    shared: MeetingMinutes | None = None
    # "After the meeting" layer 1: the points people added after the meeting, oldest first.
    late_contributions: list[LateContribution] = Field(default_factory=list)


# --- validation -------------------------------------------------------------------------------


def _clip(raw: Any, limit: int) -> str | None:
    if not isinstance(raw, str):
        return None
    value = " ".join(raw.split())
    if not value:
        return None
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _uuid(raw: Any) -> uuid.UUID | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        return None


def _entries(payload: dict[str, Any], key: str) -> list[dict[str, Any]]:
    raw = payload.get(key)
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in raw if isinstance(raw, list) else []:
        if not isinstance(entry, dict):
            continue
        text_ = _clip(entry.get("text"), MAX_TEXT)
        if text_ is None or text_.lower() in seen:
            continue
        seen.add(text_.lower())
        out.append({**entry, "text": text_})
        if len(out) == MAX_ITEMS:
            break
    return out


def clean_content(
    payload: dict[str, Any],
    *,
    scope: MinutesScope,
    participants: set[uuid.UUID],
    decisions: set[uuid.UUID],
) -> dict[str, Any]:
    """The payload reduced to exactly the stored shape, every text clipped, every id checked:
    an owner or asker who is not a participant becomes a name only, a decision id that is not a
    decision of this meeting is dropped. The shared minutes never keep promises."""

    def person(raw: Any) -> str | None:
        found = _uuid(raw)
        return str(found) if found is not None and found in participants else None

    out: dict[str, Any] = {
        "summary": _clip(payload.get("summary"), MAX_SUMMARY) or "",
        "decisions": [
            {
                "text": d["text"],
                "status": d.get("status") if d.get("status") in _DECISION_STATUSES else "agreed",
                "decision_id": (
                    str(found)
                    if (found := _uuid(d.get("decision_id"))) is not None and found in decisions
                    else None
                ),
            }
            for d in _entries(payload, "decisions")
        ],
        "open_items": [
            {
                "text": o["text"],
                "owner_user_id": person(o.get("owner_user_id")),
                "owner_name": _clip(o.get("owner_name"), MAX_NAME),
                "due": _clip(o.get("due"), MAX_DUE),
            }
            for o in _entries(payload, "open_items")
        ],
        "unanswered_questions": [
            {
                "text": q["text"],
                "asked_by_user_id": person(q.get("asked_by_user_id")),
                "asked_by_name": _clip(q.get("asked_by_name"), MAX_NAME),
            }
            for q in _entries(payload, "unanswered_questions")
        ],
        "promises": (
            []
            if scope == "shared"
            else [
                {
                    "text": p["text"],
                    "to_name": _clip(p.get("to_name"), MAX_NAME),
                    "due": _clip(p.get("due"), MAX_DUE),
                }
                for p in _entries(payload, "promises")
            ]
        ),
    }
    return out


def _has_content(content: dict[str, Any]) -> bool:
    return bool(content["summary"]) or any(
        content[k] for k in ("decisions", "open_items", "unanswered_questions", "promises")
    )


def parse_payload(payload_json: Any) -> dict[str, Any] | None:
    if isinstance(payload_json, dict):
        return payload_json
    if not isinstance(payload_json, str):
        return None
    try:
        parsed = json.loads(payload_json)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


async def persist(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID | None,
    payload: dict[str, Any],
) -> bool:
    """Upsert one set of minutes. True when a row was written or replaced.

    `user_id` None is the shared minutes. A personal set for someone who is not a participant
    of the meeting is dropped (False): minutes are only ever for the people who were in it."""
    scope: MinutesScope = "shared" if user_id is None else "personal"
    if payload.get("scope") not in (None, scope):
        return False  # the scope and the user disagree: not a record we can file
    participants = set(
        (
            await session.execute(
                text("SELECT user_id FROM meeting_participants WHERE meeting_id = :m"),
                {"m": str(meeting_id)},
            )
        ).scalars()
    )
    if user_id is not None and user_id not in participants:
        return False
    decisions = set(
        (
            await session.execute(
                text("SELECT id FROM decisions WHERE meeting_id = :m AND deleted_at IS NULL"),
                {"m": str(meeting_id)},
            )
        ).scalars()
    )
    content = clean_content(payload, scope=scope, participants=participants, decisions=decisions)
    status = payload.get("status")
    reason = payload.get("reason") if payload.get("reason") in _REASONS else None
    if status == "unavailable" and scope == "shared":
        content = clean_content({}, scope=scope, participants=set(), decisions=set())
        reason = reason or "model_error"
    elif _has_content(content):
        status, reason = "ready", None
    else:
        status, reason = "empty", None
    prompt = str(payload.get("prompt_version") or "")
    prompt = prompt if _PROMPT_RE.match(prompt) else "minutes@unknown"
    generation = str(payload.get("generation") or "")[:64] or "unknown"
    row = (
        await session.execute(
            text(
                "INSERT INTO meeting_minutes (org_id, meeting_id, user_id, scope, status, reason, "
                "content, prompt_version, generation) VALUES (:o, :m, :u, :s, :st, :r, "
                "CAST(:c AS jsonb), :p, :g) "
                "ON CONFLICT ON CONSTRAINT uq_meeting_minutes_meeting_user DO UPDATE SET "
                "status = EXCLUDED.status, reason = EXCLUDED.reason, content = EXCLUDED.content, "
                "prompt_version = EXCLUDED.prompt_version, generation = EXCLUDED.generation, "
                "updated_at = now() "
                # A newer generation replaces; an unavailable shared set never replaces real
                # minutes from an earlier generation.
                "WHERE NOT (EXCLUDED.status = 'unavailable' "
                "           AND meeting_minutes.status <> 'unavailable') "
                "RETURNING 1"
            ),
            {
                "o": str(org_id),
                "m": str(meeting_id),
                "u": str(user_id) if user_id else None,
                "s": scope,
                "st": status,
                "r": reason,
                "c": json.dumps(content),
                "p": prompt[:64],
                "g": generation,
            },
        )
    ).first()
    return row is not None


# --- reads ------------------------------------------------------------------------------------

_COLS = "scope, status, reason, content, prompt_version, created_at, updated_at"


def _from_row(row: Any) -> MeetingMinutes:
    content = row["content"] if isinstance(row["content"], dict) else json.loads(row["content"])
    return MeetingMinutes(
        scope=row["scope"],
        status=row["status"],
        reason=row["reason"],
        summary=content.get("summary") or "",
        decisions=content.get("decisions") or [],
        open_items=content.get("open_items") or [],
        unanswered_questions=content.get("unanswered_questions") or [],
        promises=content.get("promises") or [] if row["scope"] == "personal" else [],
        prompt_version=row["prompt_version"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def shared_minutes(session: AsyncSession, meeting_id: uuid.UUID) -> MeetingMinutes | None:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM meeting_minutes "  # noqa: S608 - constant columns
                    "WHERE meeting_id = :m AND user_id IS NULL"
                ),
                {"m": str(meeting_id)},
            )
        )
        .mappings()
        .first()
    )
    return _from_row(row) if row else None


async def personal_minutes(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> MeetingMinutes | None:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM meeting_minutes "  # noqa: S608 - constant columns
                    "WHERE meeting_id = :m AND user_id = :u"
                ),
                {"m": str(meeting_id), "u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    return _from_row(row) if row else None


def view_state(meeting_status: str, shared: MeetingMinutes | None) -> MinutesState:
    if shared is not None:
        return "unavailable" if shared.status == "unavailable" else "ready"
    return "pending" if meeting_status == "ended" else "not_ended"
