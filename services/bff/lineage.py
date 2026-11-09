"""The decision-lineage screen. Plan 7-9 section 3.8 step 3.

`GET /bff/meetings/{meeting_id}/decisions/{decision_id}/lineage` returns the `FOLLOWS_FROM`
chain around one decision, oldest first: what it superseded, the decision itself, then what
superseded it. The chain comes from `knowledge-graph-service`'s `decision_lineage` template
(`POST /graph/query/decision_lineage`), the only reader of the graph; `bff` owns no data and
writes nothing.

Tenant: `org_id` is the caller's, resolved from the verified session (`require_tier`), and is
handed to the graph service in `X-Mytheus-Org` together with `bff`'s own service secret
(`MYTHEUS_KG_SERVICE_TOKEN`). The meeting is checked visible under RLS first, so another
tenant's meeting is a 404 before the graph is ever asked; a decision that does not belong to the
named meeting is a 404 too. The graph unreachable or unconfigured is a 503, never an empty chain
that would read as "this decision has no history".
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Annotated, Any, Literal

import httpx
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/bff", tags=["bff"])

TIMEOUT_SECONDS = 3.0

LineageRole = Literal["ancestor", "subject", "descendant"]


class LineageDecision(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID | None = None
    content: str | None = None  # None for a superseded decision the graph has only as a stub
    status: str | None = None
    confidence: float | None = None
    confirmed_at: str | None = None
    role: LineageRole
    depth: int


class DecisionLineage(BaseModel):
    decision_id: uuid.UUID
    meeting_id: uuid.UUID
    chain: list[LineageDecision]


def _unavailable() -> ProblemDetail:
    return ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Decision history is unavailable")


async def fetch_lineage(org_id: uuid.UUID, decision_id: uuid.UUID) -> list[dict[str, Any]]:
    """Call the graph's `decision_lineage` template. Separate so tests can replace it."""
    base = os.environ.get("KNOWLEDGE_GRAPH_URL")
    token = os.environ.get("MYTHEUS_KG_SERVICE_TOKEN")
    if not base or not token:
        logger.error("bff lineage: KNOWLEDGE_GRAPH_URL / MYTHEUS_KG_SERVICE_TOKEN not set")
        raise _unavailable()
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS) as client:
            res = await client.post(
                f"{base.rstrip('/')}/graph/query/decision_lineage",
                json={"params": {"decision_id": str(decision_id)}},
                headers={
                    "X-Mytheus-Service": "bff",
                    "X-Mytheus-Service-Token": token,
                    "X-Mytheus-Org": str(org_id),
                },
            )
    except httpx.HTTPError:
        logger.exception("bff lineage: knowledge-graph-service unreachable")
        raise _unavailable() from None
    if res.status_code != 200:
        logger.error("bff lineage: knowledge-graph-service answered %s", res.status_code)
        raise _unavailable()
    nodes: list[dict[str, Any]] = res.json()["nodes"]
    return nodes


@router.get(
    "/meetings/{meeting_id}/decisions/{decision_id}/lineage", response_model=DecisionLineage
)
async def decision_lineage(
    meeting_id: uuid.UUID,
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DecisionLineage:
    visible = (
        await session.execute(
            text("SELECT 1 FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if visible is None:
        raise not_found()

    nodes = await fetch_lineage(ctx.org_id, decision_id)
    chain = [
        LineageDecision(
            id=n["id"],
            meeting_id=n["properties"].get("meeting_id"),
            content=n["properties"].get("content"),
            status=n["properties"].get("status"),
            confidence=n["properties"].get("confidence"),
            confirmed_at=n["properties"].get("confirmed_at"),
            role=n["role"],
            depth=n["depth"],
        )
        for n in nodes
        if n.get("label") == "Decision"
    ]
    subject = next((d for d in chain if d.role == "subject"), None)
    if subject is None or subject.meeting_id != meeting_id:
        raise not_found()
    return DecisionLineage(decision_id=decision_id, meeting_id=meeting_id, chain=chain)
