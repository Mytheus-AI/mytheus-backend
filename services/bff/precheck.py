"""`POST /bff/meetings/precheck`: "Should this be a meeting?" (polish wave 3).

While someone types a new meeting's title and agenda, the scheduling screen asks this route
(debounced) and shows a dismissible, never-blocking hint:

- `already_decided`: confirmed decisions that already cover the topic, best first (at most 3).
  Semantic first: rag-service `POST /api/rag/query` with `source: "decisions"` and
  `generate: false` (retrieval only, no LLM answer; called as `bff` on the person's behalf, the
  `/api/v1/ask` scheme, so it is participant-scoped inside rag-service). When retrieval is not
  connected, slow (> `RAG_TIMEOUT`) or failing, a keyword fallback over the decisions of the
  person's own meetings answers instead (`retrieval: "keyword"`). Either way the decision rows
  are re-read here under RLS and the participant rule (the decision log's), so nothing the
  person could not open is shown.
- `status_updates`: agenda items that read like status updates ("status of", "update on",
  "where are we with", "progress", "check-in", ...) or that restate an open action of an
  invitee; each carries the matched open action (owner, due date) when there is one, so the
  screen can offer "send status requests instead" (core-api `POST /api/v1/status-requests`).
- `recent_meetings`: the person's meetings of the last 30 days with a similar title.

No LLM runs here (CLAUDE.md: no synchronous LLM in the CRUD tier); matching is lexical plus
rag-service's retrieval. Read-only, bounded, and fast.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any, Literal

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from services.bff import ask as ask_mod
from services.core_api.records import person_name

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["meeting-precheck"])

RAG_TIMEOUT = httpx.Timeout(2.5, connect=1.0)
MIN_CHARS = 8
MAX_DECIDED = 3
MAX_RECENT = 3
RECENT_DAYS = 30
SEMANTIC_MIN_SCORE = float(os.environ.get("PRECHECK_DECIDED_MIN_SCORE", "0.55"))
KEYWORD_MIN_SCORE = 0.6
ACTION_MATCH = 0.5
STRONG_ACTION_MATCH = 0.75

_STOP = frozenset(
    """a an and are as at be by for from has have how i in into is it its of on or our over
    so that the their them then there these this to up us was we were what when where which
    who why will with you your about after again all also any before can did do does done
    get got just let lets meeting meet sync discuss discussion talk review quick weekly daily
    team call agenda item items next new plan""".split()
)
_STATUS_WORDS = frozenset(
    """status update updates progress check checkin check-in standup stand-up recap report
    reporting eta blockers blocker""".split()
)
_STATUS_RE = re.compile(
    r"\b(status( of| on| update)?|update on|updates on|progress( on| of)?|check[- ]?in( on)?|"
    r"where (are|is) we( at)?( with| on)?|how is .* going|stand[- ]?up|recap of|"
    r"report(ing)? on|eta on|blockers?( on| for)?)\b",
    re.IGNORECASE,
)
_WORD = re.compile(r"[a-z0-9][a-z0-9'-]*")


class PrecheckRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str = Field(default="", max_length=300)
    agenda: list[str] = Field(default_factory=list, max_length=30)
    participant_ids: list[uuid.UUID] = Field(default_factory=list, max_length=100)


class DecidedHint(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    meeting_title: str | None = None
    content: str
    decided_at: datetime
    score: float
    superseded_by: uuid.UUID | None = None


class StatusAction(BaseModel):
    id: uuid.UUID
    content: str
    owner_user_id: uuid.UUID
    owner_name: str | None = None
    due_date: date | None = None


class StatusHint(BaseModel):
    agenda_index: int
    text: str
    action: StatusAction | None = None


class RecentMeeting(BaseModel):
    meeting_id: uuid.UUID
    title: str
    when: datetime


class MeetingPrecheck(BaseModel):
    already_decided: list[DecidedHint] = Field(default_factory=list)
    status_updates: list[StatusHint] = Field(default_factory=list)
    recent_meetings: list[RecentMeeting] = Field(default_factory=list)
    retrieval: Literal["semantic", "keyword"] = "keyword"


def terms(value: str) -> set[str]:
    """Significant lowercase words, crudely singularised (`vendors` -> `vendor`)."""
    out: set[str] = set()
    for w in _WORD.findall(value.lower()):
        w = w.strip("'-")
        if len(w) < 3 or w in _STOP:
            continue
        if len(w) > 4 and w.endswith("s") and not w.endswith("ss"):
            w = w[:-1]
        out.add(w)
    return out


def overlap(query: set[str], candidate: set[str]) -> float:
    return len(query & candidate) / len(query) if query else 0.0


def is_status_like(item: str) -> bool:
    return bool(_STATUS_RE.search(item))


def _name(stored: str | None, email: str | None) -> str | None:
    return person_name(stored, email)


async def _semantic(ctx: RequestContext, queries: list[str]) -> dict[uuid.UUID, float] | None:
    """decision_id -> best rerank score, or None when retrieval is unavailable."""
    secret = ask_mod._own_secret()
    if secret is None or ctx.user_id is None:
        return None
    base = os.environ.get(ask_mod.URL_ENV, "http://rag-service:8201").rstrip("/")
    headers = {
        "X-Mytheus-Service": ask_mod.SERVICE_NAME,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(ctx.org_id),
        "X-Mytheus-User": str(ctx.user_id),
    }
    scores: dict[uuid.UUID, float] = {}
    try:
        async with httpx.AsyncClient(transport=ask_mod._transport, timeout=RAG_TIMEOUT) as client:
            for q in queries:
                res = await client.post(
                    f"{base}/api/rag/query",
                    json={"query": q[:1000], "source": "decisions", "generate": False},
                    headers=headers,
                )
                if res.status_code >= 400:
                    return None
                for p in res.json().get("answer_passages") or []:
                    cite = p.get("citation") or {}
                    if cite.get("kind") != "decision" or not cite.get("decision_id"):
                        continue
                    try:
                        did = uuid.UUID(str(cite["decision_id"]))
                        score = float(p.get("rerank_score") or 0.0)
                    except (TypeError, ValueError):
                        continue
                    scores[did] = max(scores.get(did, 0.0), score)
    except httpx.HTTPError:
        logger.info("precheck: rag-service unavailable, keyword fallback")
        return None
    return {d: s for d, s in scores.items() if s >= SEMANTIC_MIN_SCORE}


_DECISION_SELECT = """
SELECT d.id, d.content, d.meeting_id, m.title AS meeting_title,
       COALESCE(d.status_changed_at, d.created_at) AS decided_at,
       (SELECT n.id FROM decisions n WHERE n.supersedes = d.id AND n.deleted_at IS NULL
          AND n.status IN ('confirmed', 'superseded') ORDER BY n.created_at DESC LIMIT 1)
         AS superseded_by
FROM decisions d JOIN meetings m ON m.id = d.meeting_id AND m.deleted_at IS NULL
WHERE d.status IN ('confirmed', 'superseded') AND d.deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM meeting_participants p WHERE p.meeting_id = d.meeting_id
              AND p.user_id = :u)
"""


async def _keyword(
    session: AsyncSession, user_id: uuid.UUID, queries: list[set[str]]
) -> dict[uuid.UUID, float]:
    words = sorted(set().union(*queries)) if queries else []
    if not words:
        return {}
    tsq = " | ".join(re.sub(r"[^a-z0-9]", "", w) for w in words if re.sub(r"[^a-z0-9]", "", w))
    if not tsq:
        return {}
    rows = (
        await session.execute(
            text(
                f"{_DECISION_SELECT} AND to_tsvector('english', d.content) @@ "
                "to_tsquery('english', :q) ORDER BY ts_rank(to_tsvector('english', d.content), "
                "to_tsquery('english', :q)) DESC LIMIT 40"
            ),
            {"u": str(user_id), "q": tsq},
        )
    ).mappings()
    scores: dict[uuid.UUID, float] = {}
    for r in rows:
        content_terms = terms(r["content"])
        best = 0.0
        for q in queries:
            if len(q & content_terms) >= min(2, len(q)):
                best = max(best, overlap(q, content_terms))
        if best >= KEYWORD_MIN_SCORE:
            scores[r["id"]] = round(best, 3)
    return scores


@router.post("/meetings/precheck", response_model=MeetingPrecheck)
async def precheck(
    body: PrecheckRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingPrecheck:
    title = " ".join(body.title.split())
    agenda = [" ".join(a.split())[:300] for a in body.agenda]
    if ctx.user_id is None or len(re.sub(r"\s", "", title + "".join(agenda))) < MIN_CHARS:
        return MeetingPrecheck()
    user_id = ctx.user_id
    texts = [t for t in [title, *agenda] if t]
    term_sets = [s for s in (terms(t) for t in texts) if s]

    # --- already decided --------------------------------------------------------------------
    retrieval: Literal["semantic", "keyword"] = "keyword"
    scores = await _semantic(ctx, texts[:4])
    if scores is not None:
        retrieval = "semantic"
    else:
        scores = await _keyword(session, user_id, term_sets)
    decided: list[DecidedHint] = []
    if scores:
        rows = (
            await session.execute(
                text(f"{_DECISION_SELECT} AND d.id = ANY(CAST(:ids AS uuid[]))"),
                {"u": str(user_id), "ids": [str(i) for i in scores]},
            )
        ).mappings()
        decided = sorted(
            (
                DecidedHint(
                    decision_id=r["id"],
                    meeting_id=r["meeting_id"],
                    meeting_title=r["meeting_title"],
                    content=r["content"],
                    decided_at=r["decided_at"],
                    superseded_by=r["superseded_by"],
                    score=round(scores[r["id"]], 3),
                )
                for r in rows
            ),
            key=lambda h: (h.superseded_by is not None, -h.score),
        )[:MAX_DECIDED]

    # --- status updates ---------------------------------------------------------------------
    status: list[StatusHint] = []
    if agenda:
        people = [str(p) for p in body.participant_ids]
        actions = (
            (
                await session.execute(
                    text(
                        "SELECT a.id, a.content, a.assignee_id, a.due_date, u.email, "
                        "u.display_name "
                        "FROM action_items a JOIN users u ON u.id = a.assignee_id "
                        " AND u.deleted_at IS NULL "
                        "WHERE a.status = 'confirmed' AND a.deleted_at IS NULL AND ("
                        " a.assignee_id = ANY(CAST(:people AS uuid[])) OR a.assignee_id = :u "
                        " OR EXISTS (SELECT 1 FROM meeting_participants p "
                        "  WHERE p.meeting_id = a.meeting_id AND p.user_id = :u)) "
                        "ORDER BY a.created_at DESC LIMIT 300"
                    ),
                    {"u": str(user_id), "people": people},
                )
            )
            .mappings()
            .all()
        )
        indexed = [(a, terms(a["content"])) for a in actions]
        for i, item in enumerate(agenda):
            if not item:
                continue
            item_terms = terms(item) - _STATUS_WORDS
            best, best_score = None, 0.0
            for a, a_terms in indexed:
                score = overlap(item_terms, a_terms)
                if score > best_score and len(item_terms & a_terms) >= min(2, len(item_terms)):
                    best, best_score = a, score
            status_like = is_status_like(item)
            strong = best is not None and best_score >= STRONG_ACTION_MATCH
            if not status_like and not strong:
                continue
            action = None
            if best is not None and best_score >= ACTION_MATCH:
                action = StatusAction(
                    id=best["id"],
                    content=best["content"],
                    owner_user_id=best["assignee_id"],
                    owner_name=_name(best["display_name"], best["email"]),
                    due_date=best["due_date"],
                )
            status.append(StatusHint(agenda_index=i, text=item, action=action))

    # --- recent meetings --------------------------------------------------------------------
    recent: list[RecentMeeting] = []
    title_terms = terms(title)
    if title_terms:
        meeting_rows = (
            await session.execute(
                text(
                    "SELECT m.id, m.title, COALESCE(m.scheduled_start, m.created_at) AS at "
                    "FROM meetings m WHERE m.deleted_at IS NULL AND m.title IS NOT NULL "
                    "AND COALESCE(m.scheduled_start, m.created_at) >= :since "
                    "AND m.status <> 'cancelled' "
                    "AND EXISTS (SELECT 1 FROM meeting_participants p "
                    " WHERE p.meeting_id = m.id AND p.user_id = :u) "
                    "ORDER BY at DESC LIMIT 200"
                ),
                {"u": str(user_id), "since": datetime.now(UTC) - timedelta(days=RECENT_DAYS)},
            )
        ).all()
        scored: list[tuple[float, Any]] = []
        for mid, mtitle, at in meeting_rows:
            mterms = terms(str(mtitle))
            s = overlap(title_terms, mterms)
            if s >= 0.5 and len(title_terms & mterms) >= min(2, len(title_terms)):
                scored.append((s, (mid, mtitle, at)))
        scored.sort(key=lambda x: (-x[0], -x[1][2].timestamp()))
        recent = [
            RecentMeeting(meeting_id=m[0], title=m[1], when=m[2]) for _, m in scored[:MAX_RECENT]
        ]

    return MeetingPrecheck(
        already_decided=decided,
        status_updates=status,
        recent_meetings=recent,
        retrieval=retrieval,
    )
