"""The last word, the agreement check and pick up where we stopped ("After the meeting" layer 1).
The domain half is `services/core_api/room_signals.py`.

Member tier. A meeting or decision of another tenant is a 404 (RLS hides it); one the caller is
not a participant of is a 403 `NOT_A_PARTICIPANT`. A nudge or an agreement check is only ever
its own person's: anyone else, in any tenant, gets a 404.

- `GET  /api/v1/meetings/{id}/after-signals`: the after page's read: whether the analyzer has
  run, your own nudge, the late contributions, per decision its agreement strength, its late
  contribution count and its 24 h window, and the resume point saved from this meeting.
- `GET  /api/v1/me/last-word?status=open|all`, `POST /api/v1/last-word/{id}/dismiss`.
- `POST /api/v1/meetings/{id}/late-contributions` (written and/or recorded; for 7 days after the
  meeting ended), `GET` the list, `GET /api/v1/late-contributions/{id}/audio` (Range aware),
  `DELETE /api/v1/late-contributions/{id}` (the author; the nudge reopens).
- `GET  /api/v1/decisions/{id}/agreement`: the strength for every participant, the aggregate and
  the notes for the decision's owner only, your own check if you were asked.
- `GET  /api/v1/me/agreement-checks?status=asked|all`, `POST /api/v1/agreement-checks/{id}/answer`.
- `GET  /api/v1/meetings/{id}/resume-point`: `{incoming, saved}`.
- `POST /api/v1/resume-points/{id}/continue|dismiss` `{meeting_id}`: any participant of that
  meeting continues; its host (or creator) sets it aside. Only the point that meeting picks up
  can be resolved there; resolving it again in the same way is 200 and writes nothing.
"""

from __future__ import annotations

import base64
import binascii
import re
import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import records
from services.core_api import room_signals as rs

router = APIRouter(prefix="/api/v1", tags=["room-signals"])

MAX_AUDIO_BYTES = 2_621_440
MAX_AUDIO_MS = 180_000
AudioMime = Literal["audio/webm", "audio/ogg", "audio/mp4", "audio/mpeg"]

ACTION_LATE_CREATED = "late_contribution.created"
ACTION_LATE_DELETED = "late_contribution.deleted"
ACTION_NUDGE_DISMISSED = "last_word.dismissed"
ACTION_CHECK_ANSWERED = "agreement_check.answered"
ACTION_RESUME_CONTINUED = "resume_point.continued"
ACTION_RESUME_DISMISSED = "resume_point.dismissed"

_NOT_PARTICIPANT: dict[int | str, dict[str, Any]] = {
    403: {"description": "Not a participant of this meeting."}
}


def _not_participant() -> ProblemDetail:
    return ProblemDetail(
        403, ErrorCode.NOT_A_PARTICIPANT, "You are not a participant of this meeting"
    )


async def _participant(
    session: AsyncSession, meeting_id: uuid.UUID, ctx: RequestContext
) -> tuple[records.MeetingAccess, uuid.UUID]:
    access = await records.access_for(session, meeting_id, ctx.user_id or rs.NIL)
    if access is None:
        raise not_found()
    if ctx.user_id is None or not access.is_participant:
        raise _not_participant()
    return access, ctx.user_id


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


# --- the after page ---------------------------------------------------------------------------


@router.get(
    "/meetings/{meeting_id}/after-signals",
    response_model=rs.AfterSignals,
    summary="What the meeting left behind: your last word, late points, agreement, the open thread",
    responses=_NOT_PARTICIPANT,
)
async def after_signals(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.AfterSignals:
    access, me = await _participant(session, meeting_id, ctx)
    return rs.AfterSignals(
        meeting_id=meeting_id,
        analyzed=await rs.analyzed(session, meeting_id),
        my_nudge=await rs.meeting_nudge(session, meeting_id, me),
        late_contributions=await rs.late_contributions(session, meeting_id, me),
        decisions=await rs.decision_signals(session, meeting_id, me, access.host_id),
        resume_point=await rs.saved_point(session, meeting_id),
    )


# --- the last word ----------------------------------------------------------------------------


@router.get("/me/last-word", response_model=rs.LastWordList, summary="Your last-word nudges")
async def my_last_word(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_: Annotated[Literal["open", "all"], Query(alias="status")] = "open",
) -> rs.LastWordList:
    return await rs.my_nudges(session, _me(ctx), only_open=status_ == "open")


@router.post(
    "/last-word/{nudge_id}/dismiss",
    response_model=rs.LastWordNudge,
    summary="Nothing to add: close your last-word nudge",
)
async def dismiss_nudge(
    nudge_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.LastWordNudge:
    me = _me(ctx)
    changed = (
        await session.execute(
            text(
                "UPDATE last_word_nudges SET status = 'dismissed', resolved_at = now() "
                "WHERE id = :n AND user_id = :u AND status = 'open' RETURNING 1"
            ),
            {"n": str(nudge_id), "u": str(me)},
        )
    ).first()
    nudge = await rs.nudge_for(session, nudge_id, me)
    if nudge is None:
        raise not_found()
    if changed:
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=me,
            action=ACTION_NUDGE_DISMISSED,
            target_type="last_word_nudge",
            target_id=nudge_id,
            ip=ctx.ip,
        )
    return nudge


class AudioIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_base64: Annotated[str, Field(min_length=4, max_length=3_600_000)]
    mime: AudioMime
    duration_ms: Annotated[int, Field(ge=1, le=MAX_AUDIO_MS)]


class LateContributionCreate(BaseModel):
    """A point added after the meeting: written, recorded, or both."""

    model_config = ConfigDict(extra="forbid")

    body: Annotated[str, Field(max_length=2000)] | None = None
    nudge_id: uuid.UUID | None = None
    decision_id: uuid.UUID | None = None
    at_ms: Annotated[int, Field(ge=0)] | None = None
    audio: AudioIn | None = None

    @field_validator("body")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        v = (v or "").strip()
        return v or None

    @model_validator(mode="after")
    def _something(self) -> LateContributionCreate:
        if self.body is None and self.audio is None:
            raise ValueError("write your point or record it")
        return self


@router.post(
    "/meetings/{meeting_id}/late-contributions",
    response_model=rs.LateContribution,
    status_code=status.HTTP_201_CREATED,
    summary="Add a late point to a meeting",
    description="Written or recorded, tied to its moment and decision. Open for 7 days after the "
    "meeting ended (409 `reason: late_window_closed`; `reason: not_ended` before it ended). "
    "Honours `Idempotency-Key`.",
    responses={
        **_NOT_PARTICIPANT,
        409: {"description": "The meeting has not ended, or its late window closed."},
        413: {"description": "The recording is larger than 2.5 MB."},
    },
)
async def add_late_contribution(
    meeting_id: uuid.UUID,
    body: LateContributionCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.LateContribution:
    access, me = await _participant(session, meeting_id, ctx)
    ended = await rs.meeting_ended_at(session, meeting_id)
    if ended is None:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "The meeting has not ended yet",
            extra={"reason": "not_ended"},
        )
    now = (await session.execute(text("SELECT now()"))).scalar_one()
    if now - ended > rs.LATE_WINDOW:
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "Late points can be added for 7 days after a meeting",
            extra={"reason": "late_window_closed"},
        )
    audio: bytes | None = None
    if body.audio is not None:
        try:
            audio = base64.b64decode(body.audio.data_base64, validate=True)
        except (binascii.Error, ValueError):
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "The recording is not valid base64"
            ) from None
        if not audio:
            raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "The recording is empty")
        if len(audio) > MAX_AUDIO_BYTES:
            raise ProblemDetail(
                413, ErrorCode.VALIDATION_FAILED, "The recording is larger than 2.5 MB"
            )
    at_ms, decision_id = body.at_ms, body.decision_id
    if body.nudge_id is not None:
        nudge = await rs.nudge_for(session, body.nudge_id, me)
        if nudge is None or nudge.meeting_id != meeting_id:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "That nudge is not yours in this meeting"
            )
        at_ms = at_ms if at_ms is not None else nudge.at_ms
        decision_id = decision_id or nudge.decision_id
    if decision_id is not None:
        found = (
            await session.execute(
                text(
                    "SELECT 1 FROM decisions WHERE id = :d AND meeting_id = :m "
                    "AND deleted_at IS NULL AND status <> 'dismissed'"
                ),
                {"d": str(decision_id), "m": str(meeting_id)},
            )
        ).first()
        if found is None:
            raise ProblemDetail(
                422, ErrorCode.VALIDATION_FAILED, "That decision is not from this meeting"
            )
    new_id = (
        await session.execute(
            text(
                "INSERT INTO late_contributions (org_id, meeting_id, user_id, nudge_id, "
                "decision_id, at_ms, body, audio, audio_mime, audio_duration_ms) VALUES "
                "(:o, :m, :u, :n, :d, :at, :b, :a, :mime, :dur) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "m": str(meeting_id),
                "u": str(me),
                "n": str(body.nudge_id) if body.nudge_id else None,
                "d": str(decision_id) if decision_id else None,
                "at": at_ms,
                "b": body.body,
                "a": audio,
                "mime": body.audio.mime if body.audio else None,
                "dur": body.audio.duration_ms if body.audio else None,
            },
        )
    ).scalar_one()
    if body.nudge_id is not None:
        await session.execute(
            text(
                "UPDATE last_word_nudges SET status = 'contributed', "
                "resolved_at = COALESCE(resolved_at, now()) "
                "WHERE id = :n AND status <> 'contributed'"
            ),
            {"n": str(body.nudge_id)},
        )
    created = await rs.late_contribution(session, new_id, me)
    assert created is not None
    if access.host_id is not None and access.host_id != me:
        await rs._notify(
            session,
            ctx.org_id,
            source=f"latecontribution:{new_id}",
            user_id=access.host_id,
            priority=3,
            title=f"{created.author_name} added a late point to {access.title}",
            body=(
                rs._short(body.body, 300)
                if body.body
                else "A recorded point. Listen on the meeting page."
            ),
            link=f"/meetings/{meeting_id}#late",
        )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_LATE_CREATED,
        target_type="late_contribution",
        target_id=new_id,
        metadata={"meeting_id": str(meeting_id), "audio": audio is not None},
        ip=ctx.ip,
    )
    return created


@router.get(
    "/meetings/{meeting_id}/late-contributions",
    response_model=rs.LateContributionList,
    summary="The late points added to a meeting",
    responses=_NOT_PARTICIPANT,
)
async def list_late_contributions(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.LateContributionList:
    _, me = await _participant(session, meeting_id, ctx)
    return rs.LateContributionList(items=await rs.late_contributions(session, meeting_id, me))


async def _owned_meeting_of(
    session: AsyncSession, contribution_id: uuid.UUID
) -> tuple[uuid.UUID, uuid.UUID]:
    row = (
        await session.execute(
            text(
                "SELECT meeting_id, user_id FROM late_contributions "
                "WHERE id = :c AND deleted_at IS NULL"
            ),
            {"c": str(contribution_id)},
        )
    ).first()
    if row is None:
        raise not_found()
    return row[0], row[1]


_RANGE = re.compile(r"^bytes=(\d*)-(\d*)$")


@router.get(
    "/late-contributions/{contribution_id}/audio",
    summary="Play a recorded late point",
    response_class=Response,
    responses={
        200: {
            "content": {"audio/webm": {}, "audio/ogg": {}, "audio/mp4": {}, "audio/mpeg": {}},
            "description": "The recording.",
        },
        206: {"description": "A byte range of the recording."},
        **_NOT_PARTICIPANT,
    },
)
async def late_contribution_audio(
    contribution_id: uuid.UUID,
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    meeting_id, _ = await _owned_meeting_of(session, contribution_id)
    await _participant(session, meeting_id, ctx)
    row = (
        await session.execute(
            text("SELECT audio, audio_mime FROM late_contributions WHERE id = :c"),
            {"c": str(contribution_id)},
        )
    ).first()
    if row is None or row[0] is None:
        raise not_found()
    data, mime = bytes(row[0]), str(row[1])
    headers = {"Accept-Ranges": "bytes", "Cache-Control": "private, max-age=300"}
    m = _RANGE.match(request.headers.get("range", "").strip())
    if m and (m.group(1) or m.group(2)):
        size = len(data)
        if m.group(1):
            start = int(m.group(1))
            end = min(int(m.group(2)) if m.group(2) else size - 1, size - 1)
        else:
            start, end = max(0, size - int(m.group(2))), size - 1
        if start >= size or start > end:
            return Response(
                status_code=416, headers={**headers, "Content-Range": f"bytes */{size}"}
            )
        return Response(
            content=data[start : end + 1],
            status_code=206,
            media_type=mime,
            headers={**headers, "Content-Range": f"bytes {start}-{end}/{size}"},
        )
    return Response(content=data, media_type=mime, headers=headers)


@router.delete(
    "/late-contributions/{contribution_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove your late point",
    responses={403: {"description": "Only its author can remove a late point."}},
)
async def delete_late_contribution(
    contribution_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    meeting_id, author = await _owned_meeting_of(session, contribution_id)
    _, me = await _participant(session, meeting_id, ctx)
    if author != me:
        raise ProblemDetail(
            403, ErrorCode.FORBIDDEN_ROLE, "Only its author can remove a late point"
        )
    nudge = (
        await session.execute(
            text(
                "UPDATE late_contributions SET deleted_at = now() WHERE id = :c "
                "AND deleted_at IS NULL RETURNING nudge_id"
            ),
            {"c": str(contribution_id)},
        )
    ).first()
    if nudge is not None and nudge[0] is not None:
        await session.execute(
            text(
                "UPDATE last_word_nudges SET status = 'open', resolved_at = NULL WHERE id = :n "
                "AND status = 'contributed' AND NOT EXISTS (SELECT 1 FROM late_contributions c "
                "WHERE c.nudge_id = :n AND c.deleted_at IS NULL)"
            ),
            {"n": str(nudge[0])},
        )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_LATE_DELETED,
        target_type="late_contribution",
        target_id=contribution_id,
        ip=ctx.ip,
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- agreement --------------------------------------------------------------------------------


@router.get(
    "/decisions/{decision_id}/agreement",
    response_model=rs.DecisionAgreement,
    summary="How strongly a decision was agreed",
    description="Everyone in the meeting sees the strength. The decision's owner also sees the "
    "aggregate and the notes (named only when their author chose so; unsigned ones once at "
    "least three people answered). Never anyone's individual stance.",
    responses=_NOT_PARTICIPANT,
)
async def decision_agreement(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.DecisionAgreement:
    row = (
        await session.execute(
            text(
                "SELECT meeting_id, COALESCE(review_owner_id, confirmed_by) FROM decisions "
                "WHERE id = :d AND deleted_at IS NULL"
            ),
            {"d": str(decision_id)},
        )
    ).first()
    if row is None:
        raise not_found()
    access, me = await _participant(session, row[0], ctx)
    return await rs.agreement_view(session, decision_id, row[0], me, row[1] or access.host_id)


@router.get(
    "/me/agreement-checks",
    response_model=rs.AgreementCheckList,
    summary="Decisions you were asked about",
)
async def my_agreement_checks(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    status_: Annotated[Literal["asked", "all"], Query(alias="status")] = "asked",
) -> rs.AgreementCheckList:
    return await rs.my_checks(session, _me(ctx), only_asked=status_ == "asked")


class AgreementAnswerIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer: rs.Answer
    note: Annotated[str, Field(max_length=1000)] | None = None
    share_name: bool = False

    @field_validator("note")
    @classmethod
    def _strip(cls, v: str | None) -> str | None:
        v = (v or "").strip()
        return v or None


@router.post(
    "/agreement-checks/{check_id}/answer",
    response_model=rs.AgreementCheck,
    summary="Say where you really stand on a decision",
    description="Anonymous unless `share_name`. Answering again replaces your answer.",
)
async def answer_check(
    check_id: uuid.UUID,
    body: AgreementAnswerIn,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.AgreementCheck:
    me = _me(ctx)
    changed = (
        await session.execute(
            text(
                "UPDATE agreement_checks SET status = 'answered', answer = :a, note = :n, "
                "share_name = :s, answered_at = now() WHERE id = :k AND user_id = :u "
                "AND (status <> 'answered' OR answer IS DISTINCT FROM :a "
                "     OR note IS DISTINCT FROM :n "
                "     OR share_name IS DISTINCT FROM :s) RETURNING 1"
            ),
            {
                "a": body.answer,
                "n": body.note,
                "s": body.share_name,
                "k": str(check_id),
                "u": str(me),
            },
        )
    ).first()
    check = await rs.check_for(session, check_id, me)
    if check is None:
        raise not_found()
    if changed:
        # Deliberately no answer in the audit row: admins read the audit log, and the answer
        # is the person's private view.
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=me,
            action=ACTION_CHECK_ANSWERED,
            target_type="agreement_check",
            target_id=check_id,
            ip=ctx.ip,
        )
    return check


# --- resume points ----------------------------------------------------------------------------


@router.get(
    "/meetings/{meeting_id}/resume-point",
    response_model=rs.ResumePointRead,
    summary="The thread this meeting picks up, and the one it leaves for the next",
    responses=_NOT_PARTICIPANT,
)
async def resume_point(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.ResumePointRead:
    await _participant(session, meeting_id, ctx)
    return rs.ResumePointRead(
        incoming=await rs.incoming_point(session, meeting_id),
        saved=await rs.saved_point(session, meeting_id),
    )


class ResumeAction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meeting_id: uuid.UUID


async def _resolve(
    session: AsyncSession,
    ctx: RequestContext,
    point_id: uuid.UUID,
    meeting_id: uuid.UUID,
    outcome: Literal["continued", "dismissed"],
) -> rs.ResumePoint:
    access, me = await _participant(session, meeting_id, ctx)
    if outcome == "dismissed" and access.host_id != me:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Only the host can set the thread aside")
    incoming = await rs.incoming_point(session, meeting_id)
    if incoming is None or incoming.id != point_id:
        raise not_found()
    if incoming.status == outcome:
        return incoming
    if incoming.status != "open":
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"This thread was already {incoming.status}"
        )
    await session.execute(
        text(
            "UPDATE resume_points SET status = :s, resolved_at = now(), resolved_by = :u, "
            "resolved_in_meeting_id = :m, continued_in_meeting_id = CASE WHEN :s = 'continued' "
            "THEN CAST(:m AS uuid) END WHERE id = :id AND status = 'open'"
        ),
        {"s": outcome, "u": str(me), "m": str(meeting_id), "id": str(point_id)},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=me,
        action=ACTION_RESUME_CONTINUED if outcome == "continued" else ACTION_RESUME_DISMISSED,
        target_type="resume_point",
        target_id=point_id,
        metadata={"meeting_id": str(meeting_id)},
        ip=ctx.ip,
    )
    done = await rs.incoming_point(session, meeting_id)
    assert done is not None
    return done


@router.post(
    "/resume-points/{point_id}/continue",
    response_model=rs.ResumePoint,
    summary="Continue the open thread in this meeting",
    responses={**_NOT_PARTICIPANT, 409: {"description": "The thread was already set aside."}},
)
async def continue_point(
    point_id: uuid.UUID,
    body: ResumeAction,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.ResumePoint:
    return await _resolve(session, ctx, point_id, body.meeting_id, "continued")


@router.post(
    "/resume-points/{point_id}/dismiss",
    response_model=rs.ResumePoint,
    summary="Set the open thread aside (host)",
    responses={
        403: {"description": "Not the host, or not a participant."},
        409: {"description": "The thread was already continued."},
    },
)
async def dismiss_point(
    point_id: uuid.UUID,
    body: ResumeAction,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> rs.ResumePoint:
    return await _resolve(session, ctx, point_id, body.meeting_id, "dismissed")
