"""Ask another person's Myte: `/api/v1/myte/peer-questions` (ADR-0049 F1).

A person asks a teammate's Myte a question (the asker confirmed an `ask_myte` proposal). The
recipient's own Myte drafts an answer from the recipient's own data, in the recipient's session;
the recipient edits and sends it, or declines. Nothing leaves without their review, and this route
group is where the reviewed answer is stored.

Visible to its asker and its recipient only, admins included: every statement carries
`(from_user_id = caller OR to_user_id = caller)`, on top of RLS keeping it inside the workspace.
Anyone else, in this workspace or another, gets a 404.

- `POST /peer-questions` `{to_user_id, question (1..1000), thread_id?}`: 201. The recipient must be
  a current member of this workspace (else 404) and not the caller (422); `thread_id`, when given,
  must be one of the caller's own threads (else 404). At most 20 pending per asker (422).
  `Idempotency-Key` is honoured by the shared middleware. The recipient is notified in-app
  (`notifications`, through the outbox, as an `action_status_requests` ask is) with
  `source_event_id` `mytepeer:{id}`, which is how the Inbox ties the notification to the question.
- `GET /peer-questions?box=inbox|sent&status=&limit=`: `{items}` newest first. `inbox` (the
  default) is what was asked of the caller, `sent` what the caller asked.
- `GET /peer-questions/{id}`.
- `POST /peer-questions/{id}/answer` `{answer (1..8000)}`: the recipient only, once; the asker is
  notified (`mytepeer:{id}:answered`).
- `POST /peer-questions/{id}/decline` `{reason?}`: the recipient only, while pending; the asker is
  notified (`mytepeer:{id}:declined`).
- `DELETE /peer-questions/{id}`: the asker withdraws a pending question (204). Not in the ADR's
  table: without it an asker whose teammate never opens Mytheus is stuck at 20 pending for good.

Answering or declining a question that is no longer pending, or acting on the other side's
behalf, is a 409. `thread_id` is the asker's own thread, so the recipient reads it as null.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.records import person_name
from services.core_api.routes.myte import _me

router = APIRouter(prefix="/api/v1/myte", tags=["myte"])

PRODUCER = "core-api"
MAX_QUESTION = 1_000
MAX_ANSWER = 8_000
MAX_DECLINE_REASON = 500
MAX_PENDING_PER_ASKER = 20
DEFAULT_LIMIT = 50
MAX_LIMIT = 100

PeerStatus = Literal["pending", "answered", "declined"]
Box = Literal["inbox", "sent"]


# --- models -------------------------------------------------------------------------------------


def clean_text(value: str) -> str:
    """Trimmed at both ends; inner line breaks kept (a question or answer may be paragraphs)."""
    return value.strip()


class MytePerson(BaseModel):
    """A person and their Myte, as a peer question shows them."""

    id: uuid.UUID
    name: str = Field(description="Their display name (or the name guessed from their email).")
    agent_name: str = Field(description="Their Myte's name; \"{first name}'s Myte\" when unset.")
    agent_avatar_seed: str | None = Field(
        default=None, description="Their Myte's plush seed; null when they never chose one."
    )


class MytePeerQuestion(BaseModel):
    id: uuid.UUID
    from_user: MytePerson
    to_user: MytePerson
    question: str
    thread_id: uuid.UUID | None = Field(
        default=None,
        description="The asker's own thread the question came from; null for the recipient.",
    )
    status: PeerStatus
    answer: str | None = None
    decline_reason: str | None = None
    created_at: datetime
    answered_at: datetime | None = Field(
        default=None, description="When it was answered or declined."
    )


class MytePeerQuestionList(BaseModel):
    items: list[MytePeerQuestion]


class MytePeerQuestionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to_user_id: uuid.UUID
    question: str = Field(max_length=4 * MAX_QUESTION)
    thread_id: uuid.UUID | None = None

    @field_validator("question")
    @classmethod
    def _question(cls, value: str) -> str:
        value = clean_text(value)
        if not 1 <= len(value) <= MAX_QUESTION:
            raise ValueError(f"question must be 1 to {MAX_QUESTION} characters")
        return value


class MytePeerAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answer: str = Field(max_length=4 * MAX_ANSWER)

    @field_validator("answer")
    @classmethod
    def _answer(cls, value: str) -> str:
        value = clean_text(value)
        if not 1 <= len(value) <= MAX_ANSWER:
            raise ValueError(f"answer must be 1 to {MAX_ANSWER} characters")
        return value


class MytePeerDecline(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: str | None = Field(default=None, max_length=4 * MAX_DECLINE_REASON)

    @field_validator("reason")
    @classmethod
    def _reason(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = clean_text(value)
        if len(value) > MAX_DECLINE_REASON:
            raise ValueError(f"reason must be at most {MAX_DECLINE_REASON} characters")
        return value or None


# --- helpers ------------------------------------------------------------------------------------

_PERSON = (
    "{a}.id AS {a}_id, {a}.email AS {a}_email, {a}.display_name AS {a}_display_name, "
    "{a}.agent_name AS {a}_agent_name, {a}.agent_avatar_seed AS {a}_agent_avatar_seed"
)
_SELECT = (
    "SELECT q.id, q.question, q.thread_id, q.status, q.answer, q.decline_reason, q.created_at, "
    "q.answered_at, q.from_user_id, "
    + _PERSON.format(a="fu")
    + ", "
    + _PERSON.format(a="tu")
    + " FROM myte_peer_questions q JOIN users fu ON fu.id = q.from_user_id "
    "JOIN users tu ON tu.id = q.to_user_id "
)
_VISIBLE = "(q.from_user_id = :me OR q.to_user_id = :me)"


def person(row: Any, alias: str) -> MytePerson:
    """`{id, name, agent_name, agent_avatar_seed}` from the aliased user columns of `row`."""
    name = person_name(row[f"{alias}_display_name"], row[f"{alias}_email"]) or "A teammate"
    first = name.split()[0] if name.split() else name
    agent = " ".join(str(row[f"{alias}_agent_name"] or "").split())[:40]
    seed = row[f"{alias}_agent_avatar_seed"]
    return MytePerson(
        id=row[f"{alias}_id"],
        name=name,
        agent_name=agent or f"{first}'s Myte",
        agent_avatar_seed=str(seed) if seed else None,
    )


def _question(row: Any, me: uuid.UUID) -> MytePeerQuestion:
    return MytePeerQuestion(
        id=row["id"],
        from_user=person(row, "fu"),
        to_user=person(row, "tu"),
        question=row["question"],
        thread_id=row["thread_id"] if row["from_user_id"] == me else None,
        status=row["status"],
        answer=row["answer"],
        decline_reason=row["decline_reason"],
        created_at=row["created_at"],
        answered_at=row["answered_at"],
    )


async def _load(
    session: AsyncSession, question_id: uuid.UUID, me: uuid.UUID, *, lock: bool = False
) -> Any:
    row = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE q.id = :id AND {_VISIBLE}"
                    + (" FOR UPDATE OF q" if lock else "")
                ),
                {"id": str(question_id), "me": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return row


def short(value: str, limit: int) -> str:
    flat = " ".join(value.split())
    return flat if len(flat) <= limit else flat[: limit - 3].rstrip() + "..."


async def _notify(
    session: AsyncSession,
    ctx: RequestContext,
    *,
    source: str,
    user_id: uuid.UUID,
    priority: int,
    title: str,
    body: str,
    link: str,
) -> None:
    """One in-app notification through the outbox, keyed so a replay sends nothing new."""
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(ctx.org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:{source}:in_app:{user_id}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(user_id),
                "priority": priority,
                "title": title[:200],
                "body": body,
                "link": link,
            },
        ),
    )


def _already(state: str) -> ProblemDetail:
    return ProblemDetail(409, ErrorCode.VERSION_CONFLICT, f"This question was already {state}")


def asker_link(thread_id: uuid.UUID | None) -> str:
    return f"/ask?thread={thread_id}" if thread_id else "/ask"


# --- routes -------------------------------------------------------------------------------------


@router.post(
    "/peer-questions",
    response_model=MytePeerQuestion,
    status_code=status.HTTP_201_CREATED,
    summary="Ask a teammate's Myte a question",
    description=(
        "Stores the question and notifies the teammate in-app. 404 for someone who is not a "
        "member of this workspace (or a `thread_id` that is not yours); 422 for yourself or past "
        "20 pending questions. Honours `Idempotency-Key`."
    ),
)
async def ask_peer(
    body: MytePeerQuestionCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MytePeerQuestion:
    me = _me(ctx)
    if body.to_user_id == me:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Ask your own Myte directly")
    recipient = (
        await session.execute(
            text("SELECT 1 FROM users WHERE id = :u AND deleted_at IS NULL"),
            {"u": str(body.to_user_id)},
        )
    ).first()
    if recipient is None:
        raise not_found()
    if body.thread_id is not None:
        owned = (
            await session.execute(
                text("SELECT 1 FROM myte_threads WHERE id = :t AND user_id = :u"),
                {"t": str(body.thread_id), "u": str(me)},
            )
        ).first()
        if owned is None:
            raise not_found()
    # Serialise this asker's creates so two at once cannot both pass the count.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
        {"k": f"myte_peer_questions:{me}"},
    )
    pending = int(
        (
            await session.execute(
                text(
                    "SELECT count(*) FROM myte_peer_questions "
                    "WHERE from_user_id = :u AND status = 'pending'"
                ),
                {"u": str(me)},
            )
        ).scalar()
        or 0
    )
    if pending >= MAX_PENDING_PER_ASKER:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"You have {MAX_PENDING_PER_ASKER} questions waiting; wait for an answer or withdraw "
            "one",
        )
    question_id = (
        await session.execute(
            text(
                "INSERT INTO myte_peer_questions (org_id, from_user_id, to_user_id, question, "
                "thread_id) VALUES (:o, :f, :t, :q, :th) RETURNING id"
            ),
            {
                "o": str(ctx.org_id),
                "f": str(me),
                "t": str(body.to_user_id),
                "q": body.question,
                "th": str(body.thread_id) if body.thread_id else None,
            },
        )
    ).scalar_one()
    row = await _load(session, question_id, me)
    asked = _question(row, me)
    await _notify(
        session,
        ctx,
        source=f"mytepeer:{question_id}",
        user_id=body.to_user_id,
        priority=2,
        title=f"{asked.from_user.name} asked your Myte a question",
        body=short(asked.question, 2000),
        link=f"/inbox?peer_question={question_id}",
    )
    response.headers["Location"] = f"/api/v1/myte/peer-questions/{question_id}"
    return asked


@router.get(
    "/peer-questions",
    response_model=MytePeerQuestionList,
    summary="List questions for your Myte, or the ones you asked",
    description=(
        "`box=inbox` (default): questions teammates asked your Myte. `box=sent`: questions you "
        "asked theirs. Newest first; `status` filters."
    ),
)
async def list_peer_questions(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    box: Box = "inbox",
    status_: Annotated[PeerStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> MytePeerQuestionList:
    me = _me(ctx)
    where = ["q.to_user_id = :me" if box == "inbox" else "q.from_user_id = :me"]
    params: dict[str, Any] = {"me": str(me), "limit": limit}
    if status_ is not None:
        where.append("q.status = :s")
        params["s"] = status_
    rows = (
        (
            await session.execute(
                text(
                    f"{_SELECT} WHERE {' AND '.join(where)} "
                    "ORDER BY q.created_at DESC, q.id DESC LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    return MytePeerQuestionList(items=[_question(r, me) for r in rows])


@router.get(
    "/peer-questions/{question_id}",
    response_model=MytePeerQuestion,
    summary="Open a question between you and a teammate's Myte",
    description="Visible to the person who asked and the person asked; anyone else gets 404.",
)
async def get_peer_question(
    question_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MytePeerQuestion:
    me = _me(ctx)
    return _question(await _load(session, question_id, me), me)


async def _settle(session: AsyncSession, question_id: uuid.UUID, me: uuid.UUID, verb: str) -> Any:
    row = await _load(session, question_id, me, lock=True)
    if row["tu_id"] != me:
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, f"Only the person asked can {verb} this question"
        )
    if row["status"] != "pending":
        raise _already(row["status"])
    return row


@router.post(
    "/peer-questions/{question_id}/answer",
    response_model=MytePeerQuestion,
    summary="Send your reviewed answer to a teammate's question",
    description=(
        "The person asked only, once (409 after it was answered or declined). The asker is "
        "notified in-app."
    ),
)
async def answer_peer_question(
    question_id: uuid.UUID,
    body: MytePeerAnswer,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MytePeerQuestion:
    me = _me(ctx)
    before = await _settle(session, question_id, me, "answer")
    await session.execute(
        text(
            "UPDATE myte_peer_questions SET status = 'answered', answer = :a, "
            "answered_at = now() WHERE id = :id AND to_user_id = :me AND status = 'pending'"
        ),
        {"a": body.answer, "id": str(question_id), "me": str(me)},
    )
    recipient = person(before, "tu")
    await _notify(
        session,
        ctx,
        source=f"mytepeer:{question_id}:answered",
        user_id=before["fu_id"],
        priority=3,
        title=f"{recipient.agent_name} ({recipient.name}'s Myte) answered your question",
        body=short(body.answer, 2000),
        link=asker_link(before["thread_id"]),
    )
    return _question(await _load(session, question_id, me), me)


@router.post(
    "/peer-questions/{question_id}/decline",
    response_model=MytePeerQuestion,
    summary="Decline a teammate's question",
    description=(
        "The person asked only, while it is pending (409 otherwise). The asker is notified "
        "in-app, with the reason when one is given."
    ),
)
async def decline_peer_question(
    question_id: uuid.UUID,
    body: MytePeerDecline,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MytePeerQuestion:
    me = _me(ctx)
    before = await _settle(session, question_id, me, "decline")
    await session.execute(
        text(
            "UPDATE myte_peer_questions SET status = 'declined', decline_reason = :r, "
            "answered_at = now() WHERE id = :id AND to_user_id = :me AND status = 'pending'"
        ),
        {"r": body.reason, "id": str(question_id), "me": str(me)},
    )
    recipient = person(before, "tu")
    await _notify(
        session,
        ctx,
        source=f"mytepeer:{question_id}:declined",
        user_id=before["fu_id"],
        priority=3,
        title=f"{recipient.name} could not answer your question",
        body=short(body.reason or before["question"], 2000),
        link=asker_link(before["thread_id"]),
    )
    return _question(await _load(session, question_id, me), me)


@router.delete(
    "/peer-questions/{question_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Withdraw a question you asked",
    description="The asker only, while it is pending (409 otherwise). Deleted for good.",
)
async def withdraw_peer_question(
    question_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    row = await _load(session, question_id, me, lock=True)
    if row["fu_id"] != me:
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "Only the person who asked can withdraw it"
        )
    if row["status"] != "pending":
        raise _already(row["status"])
    await session.execute(
        text("DELETE FROM myte_peer_questions WHERE id = :id AND from_user_id = :me"),
        {"id": str(question_id), "me": str(me)},
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
