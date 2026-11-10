"""`POST /api/v1/ask`: ask across your meetings, with citations (Phase 4 public API).

The answer comes from rag-service (`POST /api/rag/query`, mytheus-intelligence), the only
owner of retrieval. rag-service verifies Clerk sessions itself and does not know API tokens,
so bff calls it as a SERVICE on the person's behalf: `X-Mytheus-Service: bff`, bff's secret
from `MYTHEUS_RAG_CALLER_TOKENS`, the tenant in `X-Mytheus-Org` and the person's `users.id` in
`X-Mytheus-User`. Scope is per user inside rag-service (ADR-0033 D1), so the answer covers
exactly the meetings that person took part in, the same as asking in the app.

Not connected until both sides are configured: bff needs its `bff=<secret>` entry in
`MYTHEUS_RAG_CALLER_TOKENS` and `RAG_SERVICE_URL` (default `http://rag-service:8201`), and
rag-service needs `bff` among its allowed callers with the same secret. Until then this route
(and the MCP `ask_meetings` tool) answers 503 with `reason: ask_not_connected`, never an empty
answer that would read as "nothing was found".
"""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field

from libs.shared.authz import RequestContext, require_tier
from libs.shared.errors import ErrorCode, ProblemDetail

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["ask"])

SERVICE_NAME = "bff"
TOKENS_ENV = "MYTHEUS_RAG_CALLER_TOKENS"
URL_ENV = "RAG_SERVICE_URL"
TIMEOUT = httpx.Timeout(45.0, connect=5.0)

_transport: httpx.AsyncBaseTransport | None = None


def set_transport(transport: httpx.AsyncBaseTransport | None) -> None:
    """Tests route rag-service calls to an in-process fake."""
    global _transport
    _transport = transport


class AskRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(min_length=1, max_length=1000, description="Your question.")
    meeting_id: uuid.UUID | None = Field(
        default=None, description="Narrow to one meeting you took part in."
    )


class AskCitation(BaseModel):
    n: int = Field(description="The [n] marker the answer uses.")
    text: str
    kind: str = Field(description="transcript | document | decision | action")
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    occurred_at: datetime | None = None
    speaker: str | None = None
    decision_id: uuid.UUID | None = None
    action_id: uuid.UUID | None = None


class AskAnswer(BaseModel):
    question: str
    answer: str | None = Field(
        default=None, description="Null when no answer could be grounded (see `abstained`)."
    )
    abstained: bool = Field(description="True when the sources did not support an answer.")
    degraded: bool = Field(description="True when the workspace's AI budget limited the answer.")
    citations: list[AskCitation]


def _own_secret() -> str | None:
    for entry in os.environ.get(TOKENS_ENV, "").split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and len(secret.strip()) >= 32:
            return secret.strip()
    return None


def _not_connected() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Ask is not connected on this server",
        extra={"reason": "ask_not_connected"},
    )


def _citations(body: dict[str, Any]) -> list[AskCitation]:
    out: list[AskCitation] = []
    for p in body.get("answer_passages") or []:
        cite = p.get("citation") or {}
        out.append(
            AskCitation(
                n=int(p.get("n", len(out) + 1)),
                text=str(p.get("text", "")),
                kind=str(cite.get("kind", "transcript")),
                meeting_id=cite.get("meeting_id"),
                meeting_title=p.get("meeting_title"),
                occurred_at=p.get("occurred_at"),
                speaker=cite.get("speaker"),
                decision_id=cite.get("decision_id"),
                action_id=cite.get("action_id"),
            )
        )
    return out


async def ask(ctx: RequestContext, question: str, meeting_id: uuid.UUID | None) -> AskAnswer:
    """Ask rag-service as bff on `ctx`'s behalf. Shared by the route and the MCP tool."""
    secret = _own_secret()
    if secret is None or ctx.user_id is None:
        raise _not_connected()
    base = os.environ.get(URL_ENV, "http://rag-service:8201").rstrip("/")
    payload: dict[str, Any] = {"query": question, "generate": True}
    if meeting_id is not None:
        payload["meeting_id"] = str(meeting_id)
    try:
        async with httpx.AsyncClient(transport=_transport, timeout=TIMEOUT) as client:
            res = await client.post(
                f"{base}/api/rag/query",
                json=payload,
                headers={
                    "X-Mytheus-Service": SERVICE_NAME,
                    "X-Mytheus-Service-Token": secret,
                    "X-Mytheus-Org": str(ctx.org_id),
                    "X-Mytheus-User": str(ctx.user_id),
                },
            )
    except httpx.HTTPError:
        logger.warning("ask: rag-service unreachable")
        raise _not_connected() from None
    if res.status_code in (401, 403):
        logger.error("ask: rag-service refused bff's service credentials")
        raise _not_connected()
    if res.status_code >= 400:
        raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Ask is unavailable right now")
    body: dict[str, Any] = res.json()
    answer = body.get("answer") or {}
    return AskAnswer(
        question=question,
        answer=None if answer.get("abstained", True) else answer.get("text"),
        abstained=bool(answer.get("abstained", True)),
        degraded=bool(body.get("degraded", False)),
        citations=_citations(body),
    )


@router.post(
    "/ask",
    response_model=AskAnswer,
    summary="Ask across your meetings",
    description=(
        "A grounded answer with numbered citations, from the meetings you took part in "
        "(transcripts, decisions, actions and shared documents). Read-only: allowed for "
        "`read` API tokens. 503 `ask_not_connected` until retrieval is wired on the server."
    ),
    responses={503: {"description": "Ask is not connected or unavailable (`reason`)."}},
)
async def ask_route(
    body: AskRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> AskAnswer:
    return await ask(ctx, body.question, body.meeting_id)
