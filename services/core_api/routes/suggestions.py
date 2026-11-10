"""QuestionAgent suggestions: read, ask, dismiss, save. Plan 7-9 section 3.10; ADR-0034.

The `agent-results` consumer stores each suggestion as an `open` row for the participant it was
written for (`user_id`). This module serves and changes those rows, and only for their owner.

Access (ADR-0034 D6): `require_tier("member")`. A suggestion is its owner's private prompt, so
anyone else gets 404, including another participant of the same meeting: whether a suggestion
exists for someone else is not shown. Another tenant's row is invisible to RLS, also 404. The
read additionally requires the caller to be a participant of the meeting it is about
(404 for a meeting this tenant cannot see, 403 `NOT_A_PARTICIPANT` otherwise).

Transitions (ADR-0034 D7, 409 `VERSION_CONFLICT` otherwise):
- `open` -> `asked` | `dismissed` | `saved`;
- `saved` -> `asked` | `dismissed` (a saved question reappears in a later meeting);
- `asked` and `dismissed` are final;
- repeating the current status is 200 and writes nothing.
Each change writes one audit row in the same transaction. Nothing is ever published: a
suggestion is not a commitment.

Save-for-later is a read rule (ADR-0034 D8): the GET returns the caller's saved suggestions from
every meeting next to this meeting's open ones, so a saved question reappears in their next
meeting without copying the row.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_SUGGESTION_ASKED,
    ACTION_SUGGESTION_DISMISSED,
    ACTION_SUGGESTION_SAVED,
    audit,
)
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.confidence import thresholds
from services.core_api.meeting_ledger import mark_suggestion
from services.core_api.routes.candidates import _require_meeting_participant
from services.core_api.schemas import Suggestion, SuggestionList, SuggestionStatus

router = APIRouter(prefix="/api/v1")

_COLS = (
    "id, meeting_id, question, priority, status, confidence, rationale, citations, created_at, "
    "moment_id, why, quote, quote_speaker_id, quote_start_ms, quote_end_ms"
)
MAX_OPEN = 200  # bounded by windows x 2 per meeting
MAX_SAVED = 50

# target status -> the statuses it may be reached from (the repeat is handled separately)
_FROM: dict[str, frozenset[str]] = {
    "asked": frozenset({"open", "saved"}),
    "dismissed": frozenset({"open", "saved"}),
    "saved": frozenset({"open"}),
}
_AUDIT = {
    "asked": ACTION_SUGGESTION_ASKED,
    "dismissed": ACTION_SUGGESTION_DISMISSED,
    "saved": ACTION_SUGGESTION_SAVED,
}


def _suggestion(row: Any) -> Suggestion:
    return Suggestion(**dict(row))


@router.get("/suggestions", response_model=SuggestionList, tags=["suggestions"])
async def list_suggestions(
    meeting_id: Annotated[uuid.UUID, Query(description="The meeting the sidebar is open on")],
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> SuggestionList:
    """The caller's open, surfaced suggestions for `meeting_id`, plus their saved ones from any
    meeting. Re-hydrates the sidebar on every socket (re)connect. The 0.4-0.7 silent band is
    stored but never served; the surface threshold is the tenant's."""
    user_id = await _require_meeting_participant(session, ctx, meeting_id)
    overrides = (
        await session.execute(text("SELECT agent_confidence_overrides FROM tenant_settings"))
    ).scalar()
    surface, _ = thresholds(overrides, "QuestionAgent")
    open_rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM agent_suggestions WHERE user_id = :u "
                    "AND meeting_id = :m AND status = 'open' AND deleted_at IS NULL "
                    "AND confidence > :s ORDER BY created_at ASC, id ASC LIMIT :limit"
                ),
                {"u": str(user_id), "m": str(meeting_id), "s": surface, "limit": MAX_OPEN},
            )
        )
        .mappings()
        .all()
    )
    saved_rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM agent_suggestions WHERE user_id = :u "
                    "AND status = 'saved' AND deleted_at IS NULL "
                    "ORDER BY created_at DESC, id DESC LIMIT :limit"
                ),
                {"u": str(user_id), "limit": MAX_SAVED},
            )
        )
        .mappings()
        .all()
    )
    return SuggestionList(
        open=[_suggestion(r) for r in open_rows], saved=[_suggestion(r) for r in saved_rows]
    )


async def _transition(
    session: AsyncSession, ctx: RequestContext, suggestion_id: uuid.UUID, target: str
) -> Suggestion:
    if ctx.user_id is None:
        raise not_found()
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM agent_suggestions WHERE id = :id AND user_id = :u "
                    "AND deleted_at IS NULL FOR UPDATE"
                ),
                {"id": str(suggestion_id), "u": str(ctx.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()  # missing, another tenant's, or another user's: all the same
    current: SuggestionStatus = row["status"]
    if current == target:
        return _suggestion(row)
    if current not in _FROM[target]:
        raise ProblemDetail(
            status.HTTP_409_CONFLICT,
            ErrorCode.VERSION_CONFLICT,
            f"A suggestion that is {current} cannot be marked {target}",
        )
    updated = (
        (
            await session.execute(
                text(
                    "UPDATE agent_suggestions SET status = :t WHERE id = :id " f"RETURNING {_COLS}"
                ),
                {"t": target, "id": str(suggestion_id)},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=_AUDIT[target],
        target_type="agent_suggestion",
        target_id=suggestion_id,
        metadata={"meeting_id": str(updated["meeting_id"]), "from": current},
        ip=ctx.ip,
    )
    # memory inside the meeting: the agent does not suggest this question to this person again
    await mark_suggestion(
        ctx.org_id,
        updated["meeting_id"],
        ctx.user_id,
        suggestion_id,
        status=target,
        text=updated["question"],
    )
    return _suggestion(updated)


@router.post("/suggestions/{suggestion_id}/ask", response_model=Suggestion, tags=["suggestions"])
async def ask_suggestion(
    suggestion_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Suggestion:
    """open or saved -> asked. Owner only."""
    return await _transition(session, ctx, suggestion_id, "asked")


@router.post(
    "/suggestions/{suggestion_id}/dismiss", response_model=Suggestion, tags=["suggestions"]
)
async def dismiss_suggestion(
    suggestion_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Suggestion:
    """open or saved -> dismissed. Owner only."""
    return await _transition(session, ctx, suggestion_id, "dismissed")


@router.post("/suggestions/{suggestion_id}/save", response_model=Suggestion, tags=["suggestions"])
async def save_suggestion(
    suggestion_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Suggestion:
    """open -> saved: it reappears in the owner's next meeting. Owner only."""
    return await _transition(session, ctx, suggestion_id, "saved")
