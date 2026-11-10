"""The caller's own memory across meetings: `GET`, `PATCH` and `DELETE /api/v1/me/memory`
(polish program wave 2, "Memory across meetings"; the page that edits it is a later UI wave).

Owner only, by construction: every statement is keyed on the caller's own `user_id` from the
verified token, so there is no id in the path to probe with, and RLS keeps it inside the tenant
on top. `require_tier("member")`.

- `GET` returns what the agents learned (topics with their weight, recurring questions), what the
  person changed (pinned and muted topics, the learning switch), what they have open (confirmed
  actions assigned to them, open promises; read live, never stored) and how their agents are
  tuned (the surface threshold their notes, suggestions and context fills use, and whether it is
  their own calibration or the workspace's). A person with no memory row gets the empty memory,
  never a 404: nothing learned yet is a valid state.
- `PATCH` edits it: switch learning off or on (off = nothing new is learned AND the agents stop
  using it), pin topics they care about, forget topics (removed and muted, so they are never
  learned again), unmute, forget questions.
- `DELETE` forgets everything: the memory row goes and the text of their feedback (the
  examples their agents learn from) is scrubbed. The confirm/dismiss counts stay: they only
  tune thresholds and hold no text. 204.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import person_memory as pm
from services.core_api.learning import MIN_SAMPLES_USER, USER_SCOPED_AGENTS, band_for

router = APIRouter(prefix="/api/v1", tags=["memory"])

_Label = Annotated[str, Field(min_length=1, max_length=pm.MAX_LABEL)]
_Question = Annotated[str, Field(min_length=1, max_length=pm.MAX_QUESTION)]


class MemoryTopic(BaseModel):
    label: str
    weight: float = Field(description="Decayed weight; pinned topics are fixed")
    mentions: int = Field(description="Feedback items that named it")
    pinned: bool = Field(description="Added by the person; never fades")
    last_seen_at: datetime | None = None


class MemoryQuestion(BaseModel):
    text: str
    count: int = Field(description="How often the person asked or saved it")
    last_at: datetime | None = None


class MemoryOpenAction(BaseModel):
    id: uuid.UUID
    content: str
    due_date: date | None
    meeting_id: uuid.UUID | None


class MemoryPromise(BaseModel):
    id: uuid.UUID
    content: str
    due_hint: str | None
    meeting_id: uuid.UUID


class MemoryCalibration(BaseModel):
    agent: str
    surface: float = Field(description="Confidence above which this agent shows you something")
    personal: bool = Field(description="True when tuned from your own feedback")
    samples: int = Field(description="Your items with feedback in the window")


class PersonMemory(BaseModel):
    learning_enabled: bool
    topics: list[MemoryTopic]
    recurring_questions: list[MemoryQuestion]
    muted_topics: list[str]
    open_actions: list[MemoryOpenAction]
    open_promises: list[MemoryPromise]
    calibration: list[MemoryCalibration]
    updated_at: datetime | None


class PersonMemoryUpdate(BaseModel):
    """At least one field. Lists are applied in this order: forget, unmute, pin."""

    model_config = ConfigDict(extra="forbid")

    learning_enabled: bool | None = None
    pin_topics: list[_Label] = Field(default_factory=list, max_length=pm.MAX_PINNED)
    forget_topics: list[_Label] = Field(default_factory=list, max_length=pm.MAX_MUTED)
    unmute_topics: list[_Label] = Field(default_factory=list, max_length=pm.MAX_MUTED)
    forget_questions: list[_Question] = Field(default_factory=list, max_length=pm.MAX_QUESTIONS)

    @model_validator(mode="after")
    def _at_least_one(self) -> PersonMemoryUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        if self.learning_enabled is None and "learning_enabled" in self.model_fields_set:
            raise ValueError("learning_enabled cannot be null")
        return self


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


async def _calibration(session: AsyncSession, user_id: uuid.UUID) -> list[MemoryCalibration]:
    samples = {
        str(r[0]): int(r[1])
        for r in (
            await session.execute(
                text("SELECT agent, samples FROM agent_calibration WHERE user_id = :u"),
                {"u": str(user_id)},
            )
        ).all()
    }
    out = []
    for agent in sorted(USER_SCOPED_AGENTS):
        surface, _ = await band_for(session, agent, user_id)
        n = samples.get(agent, 0)
        out.append(
            MemoryCalibration(
                agent=agent, surface=surface, personal=n >= MIN_SAMPLES_USER, samples=n
            )
        )
    return out


def _ts(raw: Any) -> datetime | None:
    return pm.parse_ts(raw)


async def view(session: AsyncSession, user_id: uuid.UUID) -> PersonMemory:
    memory = await pm.load(session, user_id)
    stored = memory or {**pm.empty(), "updated_at": None}
    topics = pm.top_topics(stored["topics"], pm.SERVE_TOPICS)
    questions = pm.ranked_questions(stored["questions"], pm.SERVE_QUESTIONS)
    return PersonMemory(
        learning_enabled=stored["learning_enabled"],
        topics=[
            MemoryTopic(
                label=t["label"],
                weight=round(float(t.get("weight") or 0.0), 3),
                mentions=int(t.get("mentions") or 0),
                pinned=bool(t.get("pinned")),
                last_seen_at=_ts(t.get("last_seen_at")),
            )
            for t in topics
        ],
        recurring_questions=[
            MemoryQuestion(
                text=q["text"], count=int(q.get("count") or 1), last_at=_ts(q.get("last_at"))
            )
            for q in questions
        ],
        muted_topics=list(stored["muted_topics"]),
        open_actions=[
            MemoryOpenAction(**a) for a in await pm.open_actions(session, user_id, pm.SERVE_OPEN)
        ],
        open_promises=[
            MemoryPromise(**p) for p in await pm.open_promises(session, user_id, pm.SERVE_OPEN)
        ],
        calibration=await _calibration(session, user_id),
        updated_at=stored.get("updated_at"),
    )


@router.get("/me/memory", response_model=PersonMemory)
async def get_my_memory(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PersonMemory:
    """What your agents remember about you across meetings. Only ever your own."""
    return await view(session, _me(ctx))


@router.patch("/me/memory", response_model=PersonMemory)
async def update_my_memory(
    body: PersonMemoryUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PersonMemory:
    """Pin, forget or unmute topics, forget questions, switch learning off or on."""
    me = _me(ctx)
    memory = await pm.load(session, me) or pm.empty()
    now = datetime.now(UTC)
    norm = pm.norm
    if body.learning_enabled is not None:
        memory["learning_enabled"] = body.learning_enabled
    muted = list(memory["muted_topics"])
    for label in body.forget_topics:
        clean = pm.clean_label(label)
        if clean is None:
            continue
        memory["topics"] = [
            t for t in memory["topics"] if norm(str(t.get("label", ""))) != norm(clean)
        ]
        if norm(clean) not in {norm(m) for m in muted}:
            muted.append(clean)
    unmute = {norm(u) for u in body.unmute_topics}
    muted = [m for m in muted if norm(m) not in unmute]
    for label in body.pin_topics:
        clean = pm.clean_label(label)
        if clean is None:
            continue
        muted = [m for m in muted if norm(m) != norm(clean)]
        existing = next(
            (t for t in memory["topics"] if norm(str(t.get("label", ""))) == norm(clean)), None
        )
        if existing is not None:
            existing.update(pinned=True, weight=pm.PINNED_WEIGHT, last_seen_at=now.isoformat())
        else:
            memory["topics"].append(
                {
                    "label": clean,
                    "weight": pm.PINNED_WEIGHT,
                    "mentions": 0,
                    "pinned": True,
                    "last_seen_at": now.isoformat(),
                }
            )
    pinned = [t for t in memory["topics"] if t.get("pinned")]
    if len(pinned) > pm.MAX_PINNED:
        raise ProblemDetail(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            ErrorCode.VALIDATION_FAILED,
            f"At most {pm.MAX_PINNED} pinned topics",
        )
    memory["muted_topics"] = muted[-pm.MAX_MUTED :]
    gone = {norm(q) for q in body.forget_questions}
    memory["questions"] = [
        q for q in memory["questions"] if norm(str(q.get("text", ""))) not in gone
    ]
    memory["topics"] = pm.merge_topics(memory["topics"], [], 0.0, memory["muted_topics"], now)
    await pm.save(session, ctx.org_id, me, memory)
    return await view(session, me)


@router.delete("/me/memory", status_code=status.HTTP_204_NO_CONTENT)
async def forget_my_memory(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """Forget everything learned about you, and scrub the text of your feedback."""
    await pm.forget(session, _me(ctx))
    return Response(status_code=status.HTTP_204_NO_CONTENT)
