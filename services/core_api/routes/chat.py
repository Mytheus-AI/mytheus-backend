"""Chat persistence. Plan 3-4 section 3.3, step 2.

`POST /chat` is the durable write side of `chat_messages`. `realtime-gateway` relays chat over
its socket immediately for latency, but the row here is the canonical fact - `core-api` is the
SOLE writer of `chat_messages` (`04-service-catalog.md` section 3.5). `web-app` is the plan's
other named caller, so this route is not gateway-specific: it enforces the same "am I actually
allowed to speak in this meeting" check regardless of caller.

Same three invariants as `routes/meetings.py` (see that module's docstring), plus one more
specific to this route: `user_id` is never trusted from the request body. The body accepts it
only for wire compatibility with `realtime-gateway`'s relay payload (see
`schemas.ChatMessageCreate`'s docstring) and ignores it; the row's author is always the
verified bearer token's own resolved identity.

Idempotency: `POST /chat` relies entirely on the shared `IdempotencyMiddleware` (mounted on
every service by `libs.shared.app_factory.create_app`), not on a table-level idempotency
column. `chat_messages` is monthly range-partitioned on `created_at`, and PostgreSQL requires a
unique constraint on a partitioned table to include every partition-key column - so a
`UNIQUE (org_id, idempotency_key)` constraint on this table is not directly expressible, and
`UNIQUE (org_id, idempotency_key, created_at)` would not actually dedupe two retries landing at
different microseconds, which defeats the purpose. The middleware's Redis-backed
tenant-scoped cache (proven by `tests/test_idempotency.py`) is the mechanism this codebase
already uses for HTTP-level replay of mutating requests, and `test_chat_api.py` in this task
proves the acceptance property end to end: the same `Idempotency-Key` posted twice produces
exactly one `chat_messages` row.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import ChatHistory, ChatMessage, ChatMessageCreate

router = APIRouter(prefix="/api/v1", tags=["chat"])


def _not_a_participant() -> ProblemDetail:
    return ProblemDetail(
        status.HTTP_403_FORBIDDEN,
        ErrorCode.NOT_A_PARTICIPANT,
        "You are not a participant of this meeting",
    )


async def _fetch_meeting_exists(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    """RLS-scoped existence check. No `org_id` predicate: RLS already scopes it, matching
    `routes.meetings._fetch_meeting`'s and `meeting_service.routes._fetch_meeting_or_404`'s
    precedent - a meeting in another tenant must be a 404, never a 403."""
    row = (
        await session.execute(
            text("SELECT id FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if row is None:
        raise not_found()


async def _require_participant(
    session: AsyncSession, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> None:
    """Deny-by-default: a chat write must come from an actual participant of the meeting.

    Defense in depth over `realtime-gateway`'s own join-gated `chat` handler: this route also
    serves `web-app` per the plan text, and without this check any member-tier org user could
    post into any meeting in their own org, participant or not.
    """
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM meeting_participants "
                "WHERE meeting_id = :meeting_id AND user_id = :user_id"
            ),
            {"meeting_id": str(meeting_id), "user_id": str(user_id)},
        )
    ).first()
    if row is None:
        raise _not_a_participant()


@router.post("/chat", status_code=status.HTTP_201_CREATED, response_model=ChatMessage)
async def post_chat(
    body: ChatMessageCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ChatMessage:
    if ctx.user_id is None:
        # A verified Clerk user with no local `users` mirror row cannot be a participant of
        # anything (matches routes.meetings and meeting_service.routes's identical case).
        raise _not_a_participant()

    await _fetch_meeting_exists(session, body.meeting_id)
    await _require_participant(session, body.meeting_id, ctx.user_id)

    row: Any = (
        (
            await session.execute(
                text(
                    "INSERT INTO chat_messages (org_id, meeting_id, user_id, content) "
                    "VALUES (:org, :meeting, :user, :content) "
                    "RETURNING id, meeting_id, user_id, content, created_at"
                ),
                {
                    "org": str(ctx.org_id),
                    "meeting": str(body.meeting_id),
                    "user": str(ctx.user_id),
                    "content": body.content,
                },
            )
        )
        .mappings()
        .one()
    )
    return ChatMessage(**dict(row))


# A meeting's chat is read back whole by the room on every (re)connect; 500 recent messages is
# far beyond any real meeting and keeps the read bounded.
MAX_CHAT_HISTORY = 500


@router.get("/meetings/{meeting_id}/chat", response_model=ChatHistory)
async def list_chat(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_CHAT_HISTORY)] = MAX_CHAT_HISTORY,
) -> ChatHistory:
    """The meeting's chat, newest `limit` messages, returned oldest first.

    Polish program, Phase 0: chat used to ride LiveKit's data channel only, so a late joiner saw
    nothing and agents could not read it. The room now sends through `realtime-gateway`'s socket
    chat, which persists here via `POST /chat`; this is the read the room re-hydrates from on
    every (re)connect. Participants only, same codes as the write (404 other tenant's meeting,
    403 not a participant).
    """
    if ctx.user_id is None:
        raise _not_a_participant()
    await _fetch_meeting_exists(session, meeting_id)
    await _require_participant(session, meeting_id, ctx.user_id)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, meeting_id, user_id, content, created_at FROM ("
                    "  SELECT id, meeting_id, user_id, content, created_at FROM chat_messages "
                    "  WHERE meeting_id = :m ORDER BY created_at DESC, id DESC LIMIT :limit"
                    ") recent ORDER BY created_at ASC, id ASC"
                ),
                {"m": str(meeting_id), "limit": limit},
            )
        )
        .mappings()
        .all()
    )
    return ChatHistory(items=[ChatMessage(**dict(r)) for r in rows])
