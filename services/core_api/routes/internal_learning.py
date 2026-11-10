"""Internal reads that let the agents learn (polish program wave 2), for agent-runtime-service.

Service-to-service, the `internal_meetings.py` scheme (like `GET /internal/agent-policy`):
`X-Mytheus-Service: agent-runtime-service`, its secret from `MYTHEUS_CORE_CALLER_TOKENS`, the
tenant in `X-Mytheus-Org`. Not routed by `api-gateway`. Every read runs under the tenant's RLS.

`GET /internal/agent-guidance[?user_id=]`: recent confirmed, edited and dismissed examples per
agent, for the agents' versioned guidance block. Which examples an agent may see follows who
sees its output:

- `DecisionAgent`, `ActionAgent` (one card for the whole meeting): the WORKSPACE's recent
  feedback, never from a confidential meeting (its text must not reach another meeting's
  prompt). Served with or without `user_id`.
- `QuestionAgent`, `PersonalListenerAgent` (one person's output): only THAT person's own
  feedback, because suggestions and notes are private. Served only with `user_id`.

Per agent at most `MAX_PER_ACTION` confirmed, `MAX_PER_ACTION` dismissed and `MAX_EDITS`
edited examples, newest first, one per item (its latest action), from the last `WINDOW_DAYS`
days. Texts are the redacted, bounded snapshots stored with the feedback; a person who asked to
forget their memory has none left.

`GET /internal/person-memory/{user_id}`: the compact memory the PersonalListenerAgent and
QuestionAgent prompts use: top topics, recurring questions, open actions, open promises. Empty
when the person switched learning off. 404 for a user this tenant cannot see.
"""

from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Request, Security
from pydantic import BaseModel, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api import person_memory as pm
from services.core_api.learning import WINDOW_DAYS
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

LEARNING_CALLERS = frozenset({"agent-runtime-service"})
MEETING_AGENTS = ("DecisionAgent", "ActionAgent")
PERSONAL_AGENTS = ("QuestionAgent", "PersonalListenerAgent")
MAX_PER_ACTION = 3
MAX_EDITS = 2
SCAN = 200  # newest feedback rows considered per agent

router = APIRouter(tags=["core-internal"])


async def learning_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, LEARNING_CALLERS)


class GuidanceExample(BaseModel):
    action: Literal["confirm", "dismiss", "edit"]
    text: str = Field(description="The item as the person left it (redacted, short)")
    before: str | None = Field(default=None, description="action=edit: the agent's wording")


class AgentGuidance(BaseModel):
    scope: Literal["workspace", "person"]
    examples: list[GuidanceExample]


class Guidance(BaseModel):
    agents: dict[str, AgentGuidance] = Field(default_factory=dict)


class MemoryOpen(BaseModel):
    content: str
    due: str | None = Field(default=None, description="An ISO due date or a spoken due hint")


class CompactMemory(BaseModel):
    learning_enabled: bool
    topics: list[str]
    recurring_questions: list[str]
    open_actions: list[MemoryOpen]
    open_promises: list[MemoryOpen]


_POSITIVE = {"confirm", "ask", "save"}


async def _examples(
    session: Any, agent: str, user_id: uuid.UUID | None, *, exclude_confidential: bool
) -> list[GuidanceExample]:
    clauses = [
        "f.agent = :a",
        "f.created_at > now() - make_interval(days => :days)",
        "(f.content IS NOT NULL OR f.edit_diff IS NOT NULL)",
    ]
    params: dict[str, Any] = {"a": agent, "days": WINDOW_DAYS, "scan": SCAN}
    if user_id is not None:
        clauses.append("f.user_id = :u")
        params["u"] = str(user_id)
    if exclude_confidential:
        clauses.append("COALESCE(m.confidential, false) = false")
    rows = (
        (
            await session.execute(
                text(
                    "SELECT f.candidate_id, f.action, f.content, f.edit_diff, f.created_at "
                    "FROM agent_feedback f LEFT JOIN meetings m ON m.id = f.meeting_id "
                    f"WHERE {' AND '.join(clauses)} "  # noqa: S608 - closed set of clauses
                    "ORDER BY f.created_at DESC LIMIT :scan"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    seen: set[Any] = set()
    edited: set[Any] = set()
    confirmed: list[GuidanceExample] = []
    dismissed: list[GuidanceExample] = []
    edits: list[GuidanceExample] = []
    for r in rows:  # newest first
        cid = r["candidate_id"]
        if r["action"] == "edit" and cid not in edited and len(edits) < MAX_EDITS:
            change = _content_change(r["edit_diff"])
            if change is not None:
                edited.add(cid)
                edits.append(GuidanceExample(action="edit", text=change[1], before=change[0]))
        if cid in seen or r["action"] == "edit":
            continue
        seen.add(cid)  # the item's latest non-edit action decides
        if not r["content"]:
            continue
        if r["action"] in _POSITIVE and len(confirmed) < MAX_PER_ACTION:
            confirmed.append(GuidanceExample(action="confirm", text=r["content"]))
        elif r["action"] == "dismiss" and len(dismissed) < MAX_PER_ACTION:
            dismissed.append(GuidanceExample(action="dismiss", text=r["content"]))
    return [*confirmed, *edits, *dismissed]


def _content_change(raw: Any) -> tuple[str, str] | None:
    diff = raw if isinstance(raw, dict) else None
    if isinstance(raw, str):
        try:
            diff = json.loads(raw)
        except ValueError:
            diff = None
    entry = diff.get("content") if isinstance(diff, dict) else None
    if not isinstance(entry, dict):
        return None
    before, after = entry.get("before"), entry.get("after")
    if isinstance(before, str) and isinstance(after, str) and before and after:
        return before, after
    return None


@router.get("/internal/agent-guidance", response_model=Guidance)
async def agent_guidance(
    org_id: Annotated[uuid.UUID, Depends(learning_caller)],
    user_id: Annotated[uuid.UUID | None, Query()] = None,
) -> Guidance:
    out: dict[str, AgentGuidance] = {}
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        for agent in MEETING_AGENTS:
            examples = await _examples(session, agent, None, exclude_confidential=True)
            if examples:
                out[agent] = AgentGuidance(scope="workspace", examples=examples)
        if user_id is not None:
            for agent in PERSONAL_AGENTS:
                examples = await _examples(session, agent, user_id, exclude_confidential=False)
                if examples:
                    out[agent] = AgentGuidance(scope="person", examples=examples)
    return Guidance(agents=out)


def _due(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    return value if isinstance(value, str) and value else None


@router.get("/internal/person-memory/{user_id}", response_model=CompactMemory)
async def person_memory(
    user_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(learning_caller)],
) -> CompactMemory:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        known = (
            await session.execute(
                text("SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL"),
                {"u": str(user_id)},
            )
        ).scalar_one()
        if not known:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        memory = await pm.load(session, user_id)
        if memory is not None and not memory["learning_enabled"]:
            return CompactMemory(
                learning_enabled=False,
                topics=[],
                recurring_questions=[],
                open_actions=[],
                open_promises=[],
            )
        topics = pm.top_topics(memory["topics"], pm.AGENT_TOPICS) if memory else []
        questions = pm.ranked_questions(memory["questions"], pm.AGENT_QUESTIONS) if memory else []
        actions = await pm.open_actions(session, user_id, pm.AGENT_OPEN)
        promises = await pm.open_promises(session, user_id, pm.AGENT_OPEN)
    return CompactMemory(
        learning_enabled=True,
        topics=[t["label"] for t in topics],
        recurring_questions=[q["text"] for q in questions],
        open_actions=[MemoryOpen(content=a["content"], due=_due(a["due_date"])) for a in actions],
        open_promises=[MemoryOpen(content=p["content"], due=_due(p["due_hint"])) for p in promises],
    )
