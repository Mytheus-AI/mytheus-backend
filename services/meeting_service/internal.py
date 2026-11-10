"""Internal room open/close for `workflow-service`'s `MeetingLifecycleWorkflow` (P10, plan 3.1).

meeting-service owns `meeting_rooms`, so the workflow's room activities call here instead of
writing that table (one writer per store):

- `POST /internal/meetings/{meeting_id}/room/open`: upsert the meeting's `meeting_rooms` row
  (`rooms.ensure_meeting_room`) and, after that commits, make sure the LiveKit room exists with
  its tenant metadata (`rooms.sync_livekit_metadata`, which never raises and leaves the row
  unsynced for the next caller to retry). A meeting that is already `ended`/`cancelled` is not
  re-opened (`opened: false`). Idempotent. The response carries the meeting's
  `scheduled_start`: `meeting-events` does not, and the workflow bounds how long it waits for
  `ended` from the scheduled start rather than from when the meeting was created.
- `POST /internal/meetings/{meeting_id}/room/close`: mark the room `ended`
  (`rooms.mark_room_ended`). Idempotent; a meeting that never had a room is `changed: false`.

This repeats what the `meeting-events` consumer in this service already does for
`created`/`ended`, on purpose: both paths are idempotent upserts on the same row, and the
workflow's copy is the one that retries with backoff and survives a worker restart.

Service-to-service, the scheme core-api's internal routes use, with this service's own secret
list: `X-Mytheus-Service: workflow-service`, `X-Mytheus-Service-Token: <secret>` from
`MYTHEUS_MEETING_CALLER_TOKENS` (`workflow-service=<32+ chars>`), the tenant in
`X-Mytheus-Org`. Unconfigured means every caller is refused. Not routed by `api-gateway`.
"""

from __future__ import annotations

import hmac
import os
import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.tenancy import set_rls
from services.meeting_service.rooms import (
    ensure_meeting_room,
    mark_room_ended,
    meeting_is_visible,
    sync_livekit_metadata,
)
from services.meeting_service.routes import get_transport
from services.meeting_service.transport import MeetingTransport

TOKENS_ENV = "MYTHEUS_MEETING_CALLER_TOKENS"
KNOWN_CALLERS = frozenset({"workflow-service"})
SERVICE_HEADER = "x-mytheus-service"
SERVICE_TOKEN_HEADER = "x-mytheus-service-token"
ORG_HEADER = "x-mytheus-org"

router = APIRouter(tags=["meeting-internal"])

service_token_scheme = APIKeyHeader(
    name="X-Mytheus-Service-Token",
    scheme_name="MytheusServiceToken",
    description=(
        "INTERNAL ONLY. Per-caller shared secret from MYTHEUS_MEETING_CALLER_TOKENS, sent with "
        "`X-Mytheus-Service` (caller name) and `X-Mytheus-Org` (tenant)."
    ),
    auto_error=False,
)


class RoomOpened(BaseModel):
    meeting_id: uuid.UUID
    room: str | None
    opened: bool
    scheduled_start: datetime | None = None


class RoomClosed(BaseModel):
    meeting_id: uuid.UUID
    changed: bool


def parse_caller_tokens(raw: str) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError(f"{TOKENS_ENV} entries must be service=secret")
        if name not in KNOWN_CALLERS:
            raise ValueError(f"{name!r} is not an allowed meeting-service caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


def _unauthenticated() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")


async def workflow_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    """workflow-service with its secret, for an active tenant; otherwise refuse."""
    try:
        tokens = parse_caller_tokens(os.environ.get(TOKENS_ENV, ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "meeting-service caller config invalid"
        ) from exc
    if not tokens:
        raise _unauthenticated()
    service = request.headers.get(SERVICE_HEADER, "")
    presented = request.headers.get(SERVICE_TOKEN_HEADER, "").encode("utf-8")
    expected = tokens.get(service)
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


@router.post("/internal/meetings/{meeting_id}/room/open", response_model=RoomOpened)
async def open_room(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
    transport: Annotated[MeetingTransport, Depends(get_transport)],
) -> RoomOpened:
    session_factory = get_sessionmaker()
    async with session_factory() as session, session.begin():
        await set_rls(session, org_id)
        # Same RLS-scoped read `rooms.meeting_is_visible` / `meeting_has_ended` make, in one query
        # so the scheduled start comes back with it.
        meeting = (
            (
                await session.execute(
                    text(
                        "SELECT status, scheduled_start, source FROM meetings "
                        "WHERE id = :id AND deleted_at IS NULL"
                    ),
                    {"id": str(meeting_id)},
                )
            )
            .mappings()
            .first()
        )
        if meeting is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        scheduled = meeting["scheduled_start"]
        # Ended meetings stay closed; a call captured from Meet/Teams/Zoom never gets a room.
        if meeting["status"] in ("ended", "cancelled") or meeting["source"] != "mytheus":
            return RoomOpened(
                meeting_id=meeting_id, room=None, opened=False, scheduled_start=scheduled
            )
        result = await ensure_meeting_room(session, org_id, meeting_id)
    if result.needs_livekit_sync:
        # After the commit, never inside it (rooms.py: no network call in an open transaction).
        await sync_livekit_metadata(session_factory, transport, org_id=org_id, room=result.room)
    return RoomOpened(
        meeting_id=meeting_id, room=result.room, opened=True, scheduled_start=scheduled
    )


@router.post("/internal/meetings/{meeting_id}/room/close", response_model=RoomClosed)
async def close_room(
    meeting_id: uuid.UUID,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> RoomClosed:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if not await meeting_is_visible(session, meeting_id):
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        changed = await mark_room_ended(session, org_id, meeting_id)
    return RoomClosed(meeting_id=meeting_id, changed=changed)
