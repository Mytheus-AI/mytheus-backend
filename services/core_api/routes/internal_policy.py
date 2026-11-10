"""Internal reads that make the AI tier apply core-api's own policies (polish plan, Phase 0).

`GET /internal/agent-policy` (agent-runtime-service): the thresholds core-api itself applies
(`learning.band_for`), so the agent decides `surfaced` with the same numbers. realtime-gateway's
live forward (which trusts the agent's `surfaced`) and core-api's store and reload (which
re-derive it) agree: one threshold source. The agent caches it per org (60 s) and drops the
cache on an `agent-feedback` event. Shape (every field after the first added in polish wave 2,
all optional for a reader, so the response stays BACKWARD compatible):

    {"agent_confidence_overrides": {"<Agent>": {"surface": x, "silent": y}},
     "user_overrides": {"<user_id>": {"<Agent>": {"surface": x, "silent": y}}},
     "calibration": {"<Agent>": {"samples": n, "dismiss_rate": r, "surface": x|null}}}

- `agent_confidence_overrides`: the WORKSPACE band per agent: the tenant's
  `agent_confidence_overrides` entry (a malformed one reads as the defaults), moved by the
  workspace calibration. Every agent with an override or a calibration is listed; `{}` for a
  tenant with neither. An agent built before wave 2 reads only this and still agrees with the
  reload for every meeting-wide agent.
- `user_overrides`: per person, only where it differs from the workspace band, only for the
  agents whose output is one person's (`learning.USER_SCOPED_AGENTS`).
- `calibration`: the workspace calibration detail per agent, for observability.

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
from services.core_api.learning import MIN_SAMPLES_WORKSPACE, calibrated_surface
from services.core_api.learning import agent_policy as learned_policy
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


class CalibrationDetail(BaseModel):
    samples: int = Field(ge=0, description="Items with feedback in the window")
    dismiss_rate: float = Field(ge=0, le=1, description="Decayed share of dismisses")
    surface: float | None = Field(
        default=None, description="The calibrated surface threshold; null below the minimum"
    )


class AgentPolicy(BaseModel):
    agent_confidence_overrides: dict[str, ConfidenceBand] = Field(default_factory=dict)
    user_overrides: dict[str, dict[str, ConfidenceBand]] = Field(default_factory=dict)
    calibration: dict[str, CalibrationDetail] = Field(default_factory=dict)


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
        workspace, per_user, rows = await learned_policy(session)
    calibration: dict[str, CalibrationDetail] = {}
    for r in rows:
        if r["user_id"] is not None:
            continue
        positives, negatives = float(r["positives"]), float(r["negatives"])
        weight = positives + negatives
        base, _ = thresholds(raw, str(r["agent"]))
        calibration[str(r["agent"])] = CalibrationDetail(
            samples=int(r["samples"]),
            dismiss_rate=round(negatives / weight, 4) if weight > 0 else 0.0,
            surface=calibrated_surface(
                base, positives, negatives, int(r["samples"]), MIN_SAMPLES_WORKSPACE
            ),
        )
    return AgentPolicy(
        agent_confidence_overrides={a: _band(b) for a, b in workspace.items()},
        user_overrides={
            u: {a: _band(b) for a, b in bands.items()} for u, bands in per_user.items()
        },
        calibration=calibration,
    )


def _band(band: tuple[float, float]) -> ConfidenceBand:
    surface, silent = band
    return ConfidenceBand(surface=min(1.0, max(0.0, surface)), silent=min(1.0, max(0.0, silent)))


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
