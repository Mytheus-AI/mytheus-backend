"""Internal reads that make the AI tier apply core-api's own policies (polish plan, Phase 0).

`GET /internal/agent-policy` (agent-runtime-service): the tenant's
`tenant_settings.agent_confidence_overrides`, normalized exactly as core-api applies them
(`confidence.thresholds`: a malformed entry reads as the defaults), shaped
`{"agent_confidence_overrides": {"<Agent>": {"surface": x, "silent": y}}}`; `{}` for a tenant
with no row or no overrides. The agent decides `surfaced` with them, so realtime-gateway's live
forward (which trusts the agent's `surfaced`) and core-api's store and reload (which re-derive
it with the same numbers) agree: one threshold source. The agent caches it per org (60 s).

`GET /internal/meetings/{meeting_id}/index-policy` (embedding-service, search-service):
`{"confidential": bool}` for one meeting, so the indexing consumers flag a confidential
meeting's chunks (ADR-0039 B2): participant-scoped retrieval still finds them, org-wide search
does not. An unknown, deleted or other tenant's meeting is 404 (the consumers then treat it as
confidential).

Service-to-service, the `internal_meetings.py` scheme: `X-Mytheus-Service`, its secret from
`MYTHEUS_CORE_CALLER_TOKENS`, the tenant in `X-Mytheus-Org`. Not routed by `api-gateway`.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Security
from pydantic import BaseModel, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.core_api.confidence import thresholds
from services.core_api.routes.internal_meetings import authenticate_caller, service_token_scheme

AGENT_CALLERS = frozenset({"agent-runtime-service"})
INDEX_CALLERS = frozenset({"embedding-service", "search-service"})

router = APIRouter(tags=["core-internal"])


async def agent_policy_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, AGENT_CALLERS)


async def index_policy_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    return await authenticate_caller(request, INDEX_CALLERS)


class ConfidenceBand(BaseModel):
    surface: float = Field(ge=0, le=1)
    silent: float = Field(ge=0, le=1)


class AgentPolicy(BaseModel):
    agent_confidence_overrides: dict[str, ConfidenceBand] = Field(default_factory=dict)


class IndexPolicy(BaseModel):
    confidential: bool


@router.get("/internal/agent-policy", response_model=AgentPolicy)
async def agent_policy(
    org_id: Annotated[uuid.UUID, Depends(agent_policy_caller)],
) -> AgentPolicy:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        raw = (
            await session.execute(text("SELECT agent_confidence_overrides FROM tenant_settings"))
        ).scalar()
    out: dict[str, ConfidenceBand] = {}
    for agent in raw if isinstance(raw, dict) else {}:
        surface, silent = thresholds(raw, str(agent))
        out[str(agent)] = ConfidenceBand(
            surface=min(1.0, max(0.0, surface)), silent=min(1.0, max(0.0, silent))
        )
    return AgentPolicy(agent_confidence_overrides=out)


@router.get("/internal/meetings/{meeting_id}/index-policy", response_model=IndexPolicy)
async def index_policy(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(index_policy_caller)],
) -> IndexPolicy:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        row = (
            await session.execute(
                text("SELECT confidential FROM meetings WHERE id = :m AND deleted_at IS NULL"),
                {"m": str(meeting_id)},
            )
        ).first()
    if row is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return IndexPolicy(confidential=bool(row[0]))
