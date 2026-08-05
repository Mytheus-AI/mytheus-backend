"""Speaker mapping. Plan 3-4 section 3.3, step 3.

`PATCH /meetings/:id/speakers/:speaker_id` is the speaker-mapping UI's target: it sets
`user_id` and `assignment = 'manual'` on a `meeting_speakers` row. `core-api` is the canon
owner of `meeting_speakers`; `transcription-service` only ever emits the diarized label, never
writes this table.

R7: manual is authoritative. This route is the ONLY write path in this codebase that sets
`assignment = 'manual'`, and it always does so via an upsert whose `SET assignment = 'manual'`
is a literal, never a caller-supplied value - so there is no code path here through which a
request body could set `assignment = 'suggested'` and downgrade a row. The database trigger
added in migration `25e7cede5672` (`meeting_speakers_manual_is_authoritative`) is the second,
independent guard: even if a future write path here or elsewhere gets this wrong, the trigger
refuses the UPDATE outright. See that migration's docstring for the full reasoning.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import MeetingSpeaker, SpeakerAssign

router = APIRouter(prefix="/api/v1", tags=["speakers"])


async def _fetch_meeting_exists(session: AsyncSession, meeting_id: uuid.UUID) -> None:
    """Same shape as `routes.chat._fetch_meeting_exists` / `routes.meetings._fetch_meeting`:
    RLS-scoped, no hand-written `org_id` predicate, cross-tenant is 404 never 403."""
    row = (
        await session.execute(
            text("SELECT id FROM meetings WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(meeting_id)},
        )
    ).first()
    if row is None:
        raise not_found()


@router.patch(
    "/meetings/{meeting_id}/speakers/{speaker_id}",
    response_model=MeetingSpeaker,
)
async def assign_speaker(
    meeting_id: uuid.UUID,
    speaker_id: str,
    body: SpeakerAssign,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MeetingSpeaker:
    await _fetch_meeting_exists(session, meeting_id)

    # Upsert rather than a bare UPDATE: a manual assignment must succeed even if diarization
    # never produced a "suggested" row for this speaker_id at all (e.g. the mapping UI is used
    # before or without transcription). `assignment = 'manual'` is always the literal on the
    # SET side, never `body`-derived, per the module docstring's R7 note.
    try:
        row: Any = (
            (
                await session.execute(
                    text(
                        "INSERT INTO meeting_speakers "
                        "(org_id, meeting_id, speaker_id, user_id, assignment) "
                        "VALUES (:org, :meeting, :speaker, :user, 'manual') "
                        "ON CONFLICT (org_id, meeting_id, speaker_id) DO UPDATE SET "
                        "  user_id = EXCLUDED.user_id, "
                        "  assignment = 'manual', "
                        "  updated_at = now() "
                        "RETURNING id, meeting_id, speaker_id, user_id, confidence, "
                        "  assignment, created_at, updated_at"
                    ),
                    {
                        "org": str(ctx.org_id),
                        "meeting": str(meeting_id),
                        "speaker": speaker_id,
                        "user": str(body.user_id),
                    },
                )
            )
            .mappings()
            .one()
        )
    except IntegrityError as exc:
        # The FK on user_id -> users(id) is the enforcement: a user id from another tenant, or
        # one that does not exist, is not insertable (same mechanism `routes.meetings`'s
        # participant-insert comment describes for meeting_participants). Translated to a
        # client error rather than the generic 500 the unhandled-exception handler would give,
        # matching the "every error is RFC 7807 with a stable code" house rule.
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "user_id does not name a user in this organization",
        ) from exc

    return MeetingSpeaker(**dict(row))
