"""`POST /internal/meetings/{meeting_id}/room-state`: meeting-service reports a call's lifecycle.

`meetings.status` belongs to core-api, but only meeting-service sees the LiveKit room: when it
starts, and when LiveKit closes it (after the last person has been gone for the room's
departure timeout, five minutes). Nothing used to move a meeting off `scheduled`, so a call
long over still read "Scheduled" and offered Join. meeting-service now reports the two facts
here, and core-api applies them to its own table:

- `live`: `scheduled` -> `live`. Anything else is left alone (a re-opened room never revives
  an ended or cancelled meeting).
- `ended`: `scheduled` or `live` -> `ended`, with the same `meeting-events` `ended` outbox
  record and audit row a user ending it through `PATCH /api/v1/meetings/{id}` produces, so
  meeting-service's own consumer closes the room records exactly as it does today.

Service-to-service, the same scheme as billing's internal route: `X-Mytheus-Service`,
`X-Mytheus-Service-Token`, the tenant in `X-Mytheus-Org`, secrets in
`MYTHEUS_CORE_CALLER_TOKENS` (`meeting-service=<secret>`, 32+ characters). Unconfigured means
every caller is refused. Not routed by `api-gateway` (no `/internal` prefix in its map), so it
is reachable only inside the network.
"""

from __future__ import annotations

import hmac
import os
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict
from sqlalchemy import text

from libs.shared.audit import ACTION_MEETING_UPDATED, audit
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api.routes.meetings import expire_status_requests

ALLOWED_CALLERS = frozenset({"meeting-service"})
# Every service that may hold a secret in MYTHEUS_CORE_CALLER_TOKENS. Each route still names
# its own allowed set (`core_caller_for`): rag-service can read retrieval scope
# (`internal_retrieval.py`) but cannot report a room's state here.
# agent-runtime-service reads related attendees (`internal_agents.py`, P10); workflow-service
# reads the post-meeting digest (`internal_lifecycle.py`, P10 MeetingLifecycleWorkflow).
# integration-service applies Jira status and reads the Slack digest
# (`internal_integrations.py`, P12). embedding-service and search-service read a meeting's
# index policy (`internal_policy.py`, polish plan: confidential meetings out of org-wide search).
KNOWN_CALLERS = frozenset(
    {
        "meeting-service",
        "rag-service",
        "agent-runtime-service",
        "workflow-service",
        "integration-service",
        "embedding-service",
        "search-service",
    }
)
TOKENS_ENV = "MYTHEUS_CORE_CALLER_TOKENS"
SERVICE_HEADER = "x-mytheus-service"
SERVICE_TOKEN_HEADER = "x-mytheus-service-token"
ORG_HEADER = "x-mytheus-org"
PRODUCER = "core-api"

router = APIRouter(tags=["core-internal"])

service_token_scheme = APIKeyHeader(
    name="X-Mytheus-Service-Token",
    scheme_name="MytheusServiceToken",
    description=(
        "INTERNAL ONLY. Per-caller shared secret from MYTHEUS_CORE_CALLER_TOKENS, sent with "
        "`X-Mytheus-Service` (caller name) and `X-Mytheus-Org` (tenant)."
    ),
    auto_error=False,
)


class RoomState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    state: Literal["live", "ended"]


class RoomStateResult(BaseModel):
    meeting_id: uuid.UUID
    status: str
    changed: bool


def parse_caller_tokens(raw: str) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError(f"{TOKENS_ENV} entries must be service=secret")
        if name not in KNOWN_CALLERS:
            raise ValueError(f"{name!r} is not an allowed core-api caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


def _unauthenticated() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")


async def authenticate_caller(request: Request, allowed: frozenset[str]) -> uuid.UUID:
    """An allowed service with its secret, for an active tenant; otherwise refuse."""
    try:
        tokens = parse_caller_tokens(os.environ.get(TOKENS_ENV, ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "core-api caller config invalid"
        ) from exc
    if not tokens:
        raise _unauthenticated()
    service = request.headers.get(SERVICE_HEADER, "")
    presented = request.headers.get(SERVICE_TOKEN_HEADER, "").encode("utf-8")
    expected = tokens.get(service) if service in allowed else None
    # Compare against a dummy on a miss so timing does not reveal which services exist.
    if not hmac.compare_digest(presented, expected or b"\x00" * 32) or expected is None:
        raise _unauthenticated()
    try:
        org_id = uuid.UUID(request.headers.get(ORG_HEADER, ""))
    except ValueError as exc:
        raise _unauthenticated() from exc
    async with get_sessionmaker()() as session, session.begin():
        resolved = await resolve_org_by_id(session, org_id)
    if resolved is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return resolved


async def core_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    """meeting-service only: the room-state route."""
    return await authenticate_caller(request, ALLOWED_CALLERS)


_TRANSITION = {
    "live": ("live", ("scheduled",)),
    "ended": ("ended", ("scheduled", "live")),
}


@router.post("/internal/meetings/{meeting_id}/room-state", response_model=RoomStateResult)
async def report_room_state(
    meeting_id: uuid.UUID,
    body: RoomState,
    org_id: Annotated[uuid.UUID, Depends(core_caller)],
) -> RoomStateResult:
    target, allowed_from = _TRANSITION[body.state]
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        current = (
            await session.execute(
                text("SELECT status, source FROM meetings WHERE id = :id AND deleted_at IS NULL"),
                {"id": str(meeting_id)},
            )
        ).first()
        if current is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        # A call captured from Meet/Teams/Zoom is not ruled by a Mytheus room: an empty-room
        # timeout (or any room report) must never end it while the external call runs on.
        if current[1] != "mytheus" or current[0] not in allowed_from:
            return RoomStateResult(meeting_id=meeting_id, status=str(current[0]), changed=False)

        row = (
            (
                await session.execute(
                    text(
                        "UPDATE meetings SET status = :s, updated_at = now() "
                        "WHERE id = :id RETURNING livekit_room, updated_at"
                    ),
                    {"s": target, "id": str(meeting_id)},
                )
            )
            .mappings()
            .one()
        )
        event_type = "ended" if target == "ended" else "updated"
        if target == "ended":
            # ADR-0039 B7: unanswered status requests for a meeting that is over expire.
            await expire_status_requests(session, meeting_id)
        await enqueue(
            session,
            EventEnvelope(
                topic="meeting-events",
                payload={
                    "meeting_id": str(meeting_id),
                    "event_type": event_type,
                    "livekit_room": row["livekit_room"],
                },
                org_id=str(org_id),
                producer=PRODUCER,
                idempotency_key=(
                    f"meeting:{meeting_id}:{event_type}:{row['updated_at'].isoformat()}"
                ),
            ),
        )
        await audit(
            session,
            org_id=org_id,
            actor_type="system",
            action=ACTION_MEETING_UPDATED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={"changed": ["status"], "status": target, "by": "meeting-service"},
        )
    return RoomStateResult(meeting_id=meeting_id, status=target, changed=True)
