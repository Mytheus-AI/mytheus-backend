"""Data erasure, core-api's share: one person's, or one meeting's, data in the caller's workspace.

`POST /api/v1/erasures` (owner/admin) erases what core-api owns, in ONE transaction under the
caller's `app.current_org`, and writes one `audit_logs` row (`data_erasure.applied`) with the
row counts. It is the core-api leg of the console's erasure, which admin-ops-service drives
(`POST /admin/tenant/erasures`): admin-ops forwards the admin's own bearer token here, revokes
the person's access tokens through identity, then records `data_erasure.completed` and emits
`admin-events` `erasure_requested` for the stores this repo does not own (vectors, the graph,
`transcript_chunks`, agent memory). It is not in api-gateway's ROUTE_MAP on purpose: the console
route is the single public entry point, so every erasure carries the umbrella audit row and the
event. Being admin-only it would be harmless if it were reached directly, only incomplete.

Typed confirmation is checked HERE, against the record core-api reads, not trusted from the
client: `confirm` must equal the person's email, or the meeting's title (`Untitled meeting` when
it has none), ignoring case and outer spaces. A mismatch is a 422 that changes nothing.

What a PERSON erasure does (rows matched on their `users.id`):

- deletes their notes (`worklogs`), promises (`commitments`), agent questions
  (`agent_suggestions`), in-meeting chat (`chat_messages`) and the status requests asked of them
  (`action_status_requests`);
- deletes their speaker attribution (`meeting_speakers`), so transcript lines they spoke fall
  back to an unnamed speaker (the words themselves are the meeting's record, owned by
  transcription-service);
- keeps confirmed org records but removes the person from them: `decisions.confirmed_by`,
  `action_items.confirmed_by` and `assignee_id` (the task stays, unassigned),
  `meetings.created_by`, `meeting_agenda_items.owner_user_id`, `meeting_templates.created_by` and
  `meeting_followups.updated_by` become NULL;
- revokes the share links they created.

What a MEETING erasure does: deletes the meeting's notes, promises, agent questions, chat,
speaker map, status requests, agenda, follow-up settings (guest addresses included), and the
decisions and actions that were never confirmed (`proposed`/`dismissed`); revokes its share
links; and soft-deletes the meeting (`deleted_at`), which hides it and its transcript from every
read. Confirmed and superseded decisions and confirmed or done actions are org records and stay.
A meeting that is live is refused (409): end it first.

Idempotent by construction: every statement is a delete or a set-to-NULL keyed on the target, so
a repeat (a retry after a later step failed, or a double click) matches nothing and reports zero.
Erasing your own data from the console is refused (422): an admin's own erasure is a different
request with a different reviewer.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_DATA_ERASURE_APPLIED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(prefix="/api/v1", tags=["erasure"])

UNTITLED_MEETING = "Untitled meeting"

ErasureKind = Literal["person", "meeting"]


class ErasureRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: ErasureKind
    target_id: uuid.UUID = Field(description="The person's `users.id` or the meeting's id.")
    confirm: str = Field(
        min_length=1,
        max_length=320,
        description=(
            "Typed confirmation: the person's email, or the meeting's title "
            f"(`{UNTITLED_MEETING}` when it has none). Case and outer spaces are ignored."
        ),
    )


class ErasureApplied(BaseModel):
    kind: ErasureKind
    target_id: uuid.UUID
    label: str = Field(description="What the confirmation matched: an email or a meeting title.")
    erased: dict[str, int] = Field(description="Rows deleted, by kind of data.")
    anonymized: dict[str, int] = Field(description="Records kept with the person removed.")
    revoked_share_links: int


def _matches(typed: str, label: str) -> bool:
    return typed.strip().casefold() == label.strip().casefold()


async def _count(session: AsyncSession, sql: str, params: dict[str, Any]) -> int:
    result = await session.execute(text(sql), params)
    return int(result.rowcount or 0)  # type: ignore[attr-defined]


_PERSON_DELETES: tuple[tuple[str, str], ...] = (
    ("notes", "DELETE FROM worklogs WHERE user_id = :t"),
    ("promises", "DELETE FROM commitments WHERE user_id = :t"),
    ("agent_questions", "DELETE FROM agent_suggestions WHERE user_id = :t"),
    ("chat_messages", "DELETE FROM chat_messages WHERE user_id = :t"),
    ("status_requests", "DELETE FROM action_status_requests WHERE user_id = :t"),
    ("speaker_attributions", "DELETE FROM meeting_speakers WHERE user_id = :t"),
)

_PERSON_ANONYMIZE: tuple[tuple[str, str], ...] = (
    ("decisions", "UPDATE decisions SET confirmed_by = NULL WHERE confirmed_by = :t"),
    ("actions_confirmed", "UPDATE action_items SET confirmed_by = NULL WHERE confirmed_by = :t"),
    ("actions_unassigned", "UPDATE action_items SET assignee_id = NULL WHERE assignee_id = :t"),
    ("meetings", "UPDATE meetings SET created_by = NULL WHERE created_by = :t"),
    (
        "agenda_items",
        "UPDATE meeting_agenda_items SET owner_user_id = NULL WHERE owner_user_id = :t",
    ),
    ("meeting_templates", "UPDATE meeting_templates SET created_by = NULL WHERE created_by = :t"),
    ("follow_ups", "UPDATE meeting_followups SET updated_by = NULL WHERE updated_by = :t"),
)

# A never-confirmed decision is deleted only when no other decision points at it: `supersedes`
# is ON DELETE RESTRICT, and the decision pointing at it is an org record that must survive.
_MEETING_DELETES: tuple[tuple[str, str], ...] = (
    ("notes", "DELETE FROM worklogs WHERE meeting_id = :t"),
    ("promises", "DELETE FROM commitments WHERE meeting_id = :t"),
    ("agent_questions", "DELETE FROM agent_suggestions WHERE meeting_id = :t"),
    ("chat_messages", "DELETE FROM chat_messages WHERE meeting_id = :t"),
    ("status_requests", "DELETE FROM action_status_requests WHERE meeting_id = :t"),
    ("speaker_attributions", "DELETE FROM meeting_speakers WHERE meeting_id = :t"),
    ("agenda_items", "DELETE FROM meeting_agenda_items WHERE meeting_id = :t"),
    ("follow_ups", "DELETE FROM meeting_followups WHERE meeting_id = :t"),
    (
        "unconfirmed_decisions",
        "DELETE FROM decisions d WHERE d.meeting_id = :t AND d.status IN ('proposed', 'dismissed') "
        "AND NOT EXISTS (SELECT 1 FROM decisions s WHERE s.supersedes = d.id)",
    ),
    (
        "unconfirmed_actions",
        "DELETE FROM action_items WHERE meeting_id = :t AND status IN ('proposed', 'dismissed')",
    ),
)


async def _erase_person(
    session: AsyncSession, ctx: RequestContext, target: uuid.UUID, confirm: str
) -> ErasureApplied:
    if ctx.user_id is not None and target == ctx.user_id:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "You cannot erase your own data from the workspace console",
            extra={"reason": "self"},
        )
    row = (
        await session.execute(text("SELECT email FROM users WHERE id = :t"), {"t": str(target)})
    ).first()
    if row is None:
        raise not_found()
    label = str(row[0] or target)
    if not _matches(confirm, label):
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "Type the person's email exactly to confirm",
            extra={"reason": "confirmation_mismatch"},
        )
    params: dict[str, Any] = {"t": str(target)}
    erased = {name: await _count(session, sql, params) for name, sql in _PERSON_DELETES}
    anonymized = {name: await _count(session, sql, params) for name, sql in _PERSON_ANONYMIZE}
    revoked = await _count(
        session,
        "UPDATE share_links SET revoked_at = now(), revoked_by = :by "
        "WHERE created_by = :t AND revoked_at IS NULL",
        {**params, "by": str(ctx.user_id) if ctx.user_id else None},
    )
    return ErasureApplied(
        kind="person",
        target_id=target,
        label=label,
        erased=erased,
        anonymized=anonymized,
        revoked_share_links=revoked,
    )


async def _erase_meeting(
    session: AsyncSession, ctx: RequestContext, target: uuid.UUID, confirm: str
) -> ErasureApplied:
    row = (
        await session.execute(
            text("SELECT title, status FROM meetings WHERE id = :t FOR UPDATE"),
            {"t": str(target)},
        )
    ).first()
    if row is None:
        raise not_found()
    title, state = row
    label = (title or "").strip() or UNTITLED_MEETING
    if not _matches(confirm, label):
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "Type the meeting's title exactly to confirm",
            extra={"reason": "confirmation_mismatch"},
        )
    if state == "live":
        raise ProblemDetail(
            409,
            ErrorCode.VERSION_CONFLICT,
            "The meeting is live; end it before erasing its data",
            extra={"reason": "meeting_live"},
        )
    params: dict[str, Any] = {"t": str(target)}
    erased = {name: await _count(session, sql, params) for name, sql in _MEETING_DELETES}
    revoked = await _count(
        session,
        "UPDATE share_links SET revoked_at = now(), revoked_by = :by "
        "WHERE meeting_id = :t AND revoked_at IS NULL",
        {**params, "by": str(ctx.user_id) if ctx.user_id else None},
    )
    hidden = await _count(
        session,
        "UPDATE meetings SET deleted_at = now(), updated_at = now() "
        "WHERE id = :t AND deleted_at IS NULL",
        params,
    )
    return ErasureApplied(
        kind="meeting",
        target_id=target,
        label=label,
        erased=erased,
        anonymized={"meeting_hidden": hidden},
        revoked_share_links=revoked,
    )


@router.post(
    "/erasures",
    response_model=ErasureApplied,
    responses={
        404: {"description": "No such person or meeting in this workspace."},
        409: {"description": "The meeting is live."},
        422: {"description": "The typed confirmation does not match, or it is your own data."},
    },
)
async def apply_erasure(
    body: ErasureRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ErasureApplied:
    """authz: admin. Erase core-api's data for one person or one meeting (module docstring)."""
    if body.kind == "person":
        result = await _erase_person(session, ctx, body.target_id, body.confirm)
    else:
        result = await _erase_meeting(session, ctx, body.target_id, body.confirm)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_DATA_ERASURE_APPLIED,
        target_type=body.kind,
        target_id=body.target_id,
        metadata={
            "erased": result.erased,
            "anonymized": result.anonymized,
            "revoked_share_links": result.revoked_share_links,
        },
        ip=ctx.ip,
    )
    return result
