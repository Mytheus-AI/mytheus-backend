"""Ask Myte threads: `/api/v1/myte/*` (ADR-0047 D7).

A person's conversations with their own Myte, stored so `/ask`, the `⌘J` sheet and every entry
point read and write the same history. rag-service streams the answer (`POST /api/rag/myte`) and
never writes; the UI server persists the turn here with the person's own token once the stream
ends (ADR-0047 D8).

Private to the owner, admins included. Every statement carries `user_id = caller` (on the thread
AND on the message row), on top of RLS keeping it inside the workspace. A thread or message of
anyone else, in this workspace or another, is a 404, never a 403: whether it exists is not the
caller's business.

- `GET /threads?limit=&cursor=&q=`: pinned first, then most recent message first. Keyset cursor
  over `(pinned, last_message_at, id)`. `q` matches the title or any message's text (ILIKE, with
  `%`/`_` escaped so they match themselves).
- `POST /threads` `{title?, scope?}`: 201. An empty title becomes "New question"; whitespace is
  collapsed and the title cut to 120 characters.
- `GET /threads/{id}`: the thread and its latest 200 messages, oldest first.
- `PATCH /threads/{id}` `{title?, pinned?}`.
- `DELETE /threads/{id}`, `DELETE /threads`: hard deletes; messages cascade. 204.
- `POST /threads/{id}/messages` `{messages: [1..2]}`: 201 `{items}`. Appended in order, and the
  thread's `last_message_at` and `updated_at` move to the newest one. `Idempotency-Key` is
  honoured by the shared middleware, so a retried save stores the turn once. A payload over
  64 KB (compact UTF-8 JSON) is a 422; a thread holds at most 2000 messages.
- `PATCH /messages/{id}` `{feedback: 1|-1|0, feedback_reason?}`: on Myte's answers only; 0 clears
  the reason.

Session principals only: `/api/v1/myte` is not in the personal-access-token surface
(`libs/shared/pat.py`), so an API token is refused before this runs. `require_tier("member")`.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(prefix="/api/v1/myte", tags=["myte"])

DEFAULT_TITLE = "New question"
MAX_TITLE = 120
MAX_CONTENT = 32_000
MAX_PAYLOAD_BYTES = 65_536
MAX_FEEDBACK_REASON = 500
MAX_MESSAGES_SERVED = 200
MAX_MESSAGES_PER_THREAD = 2_000
DEFAULT_LIMIT = 30
MAX_LIMIT = 100

Role = Literal["user", "myte"]
ScopeKind = Literal["all", "meeting", "person", "as_of"]

_THREAD_COLUMNS = "id, title, pinned, scope, created_at, updated_at, last_message_at"
_MESSAGE_COLUMNS = "id, thread_id, role, content, payload, feedback, feedback_reason, created_at"


# --- models -------------------------------------------------------------------------------------


def clean_title(value: str | None) -> str:
    """Collapse whitespace, cut to 120 characters, and default an empty title."""
    value = " ".join((value or "").split())[:MAX_TITLE].strip()
    return value or DEFAULT_TITLE


class MyteScope(BaseModel):
    """What the question was asked about (ADR-0047 D8 `scope`)."""

    model_config = ConfigDict(extra="forbid")

    kind: ScopeKind = "all"
    meeting_id: uuid.UUID | None = None
    person_id: uuid.UUID | None = None
    as_of: date | None = None


class MyteThreadSummary(BaseModel):
    id: uuid.UUID
    title: str
    pinned: bool
    created_at: datetime
    last_message_at: datetime


class MyteThreadPage(BaseModel):
    items: list[MyteThreadSummary]
    next_cursor: str | None = None


class MyteThread(MyteThreadSummary):
    scope: MyteScope | None = None
    updated_at: datetime


class MyteMessage(BaseModel):
    id: uuid.UUID
    thread_id: uuid.UUID
    role: Role
    content: str
    payload: dict[str, Any] | None = Field(
        default=None,
        description=(
            "What the UI renders besides text: `steps`, `sources`, `cards`, `proposals` (with "
            "their confirmed state), `followups`, `abstained`, `degraded`. At most 64 KB."
        ),
    )
    feedback: int = Field(description="The owner's rating: 1, -1, or 0 for none.")
    feedback_reason: str | None = None
    created_at: datetime


class MyteThreadDetail(MyteThread):
    messages: list[MyteMessage] = Field(description="The latest 200 messages, oldest first.")


class MyteThreadCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, max_length=2000)
    scope: MyteScope | None = None


class MyteThreadUpdate(BaseModel):
    """At least one field."""

    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, max_length=2000)
    pinned: bool | None = None

    @model_validator(mode="after")
    def _at_least_one(self) -> MyteThreadUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        if "pinned" in self.model_fields_set and self.pinned is None:
            raise ValueError("pinned cannot be null")
        return self


def payload_size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode())


class MyteMessageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    role: Role
    content: str = Field(default="", max_length=MAX_CONTENT)
    payload: dict[str, Any] | None = None

    @field_validator("payload")
    @classmethod
    def _payload_size(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is not None and payload_size(value) > MAX_PAYLOAD_BYTES:
            raise ValueError("payload must be at most 64 KB")
        return value

    @model_validator(mode="after")
    def _question_has_text(self) -> MyteMessageIn:
        # A stopped or failed answer may have no text (its payload says why); a question cannot.
        if self.role == "user" and not self.content.strip():
            raise ValueError("a question must not be blank")
        return self


class MyteMessagesAppend(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: list[MyteMessageIn] = Field(min_length=1, max_length=2)


class MyteMessageList(BaseModel):
    items: list[MyteMessage]


class MyteFeedback(BaseModel):
    model_config = ConfigDict(extra="forbid")

    feedback: Literal[1, -1, 0]
    feedback_reason: str | None = Field(default=None, max_length=MAX_FEEDBACK_REASON)


# --- helpers ------------------------------------------------------------------------------------


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


def _encode_cursor(row: Any) -> str:
    raw = {
        "p": bool(row["pinned"]),
        "t": row["last_message_at"].isoformat(),
        "i": str(row["id"]),
    }
    return base64.urlsafe_b64encode(json.dumps(raw).encode()).decode()


def _decode_cursor(cursor: str) -> tuple[bool, datetime, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        if not isinstance(raw["p"], bool):
            raise ValueError("pinned must be a boolean")
        return raw["p"], datetime.fromisoformat(raw["t"]), uuid.UUID(raw["i"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed pagination cursor") from exc


def like_pattern(q: str) -> str:
    """`q` as an ILIKE substring pattern, its own `\\`, `%` and `_` matching themselves."""
    escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def _jsonb(value: dict[str, Any] | None) -> str | None:
    return None if value is None else json.dumps(value, ensure_ascii=False)


def _loads(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _thread(row: Any) -> dict[str, Any]:
    out = dict(row)
    out["scope"] = _loads(out.get("scope"))
    return out


def _message(row: Any) -> MyteMessage:
    out = dict(row)
    out["payload"] = _loads(out.get("payload"))
    return MyteMessage(**out)


async def _owned_thread(
    session: AsyncSession, thread_id: uuid.UUID, me: uuid.UUID, *, lock: bool = False
) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_THREAD_COLUMNS} FROM myte_threads WHERE id = :id AND user_id = :u"
                    + (" FOR UPDATE" if lock else "")
                ),
                {"id": str(thread_id), "u": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return _thread(row)


# --- routes -------------------------------------------------------------------------------------


@router.get(
    "/threads",
    response_model=MyteThreadPage,
    summary="List your Ask Myte conversations",
    description=(
        "Your own conversations with your Myte, pinned first and then the most recent. `q` "
        "matches a title or any message's text. Keyset-paginated with `cursor`."
    ),
)
async def list_threads(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
    q: Annotated[str | None, Query(max_length=200)] = None,
) -> MyteThreadPage:
    me = _me(ctx)
    where = ["t.user_id = :u"]
    params: dict[str, Any] = {"u": str(me), "limit": limit + 1}
    if q is not None and q.strip():
        params["q"] = like_pattern(q.strip())
        where.append(
            "(t.title ILIKE :q ESCAPE '\\' OR EXISTS (SELECT 1 FROM myte_messages m "
            "WHERE m.thread_id = t.id AND m.user_id = :u AND m.content ILIKE :q ESCAPE '\\'))"
        )
    if cursor:
        pinned, last_at, row_id = _decode_cursor(cursor)
        # The same tuple, in the same direction, as the ORDER BY: no row skipped or repeated.
        where.append("(t.pinned, t.last_message_at, t.id) < (:cp, :ct, :ci)")
        params.update(cp=pinned, ct=last_at, ci=str(row_id))
    rows = (
        (
            await session.execute(
                text(
                    "SELECT t.id, t.title, t.pinned, t.created_at, t.last_message_at "
                    f"FROM myte_threads t WHERE {' AND '.join(where)} "
                    "ORDER BY t.pinned DESC, t.last_message_at DESC, t.id DESC LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    page = list(rows[:limit])
    has_more = len(rows) > limit
    return MyteThreadPage(
        items=[MyteThreadSummary(**dict(r)) for r in page],
        next_cursor=_encode_cursor(page[-1]) if has_more and page else None,
    )


@router.post(
    "/threads",
    response_model=MyteThread,
    status_code=status.HTTP_201_CREATED,
    summary="Start an Ask Myte conversation",
    description=(
        'A new, empty conversation of yours. An empty title becomes "New question"; a title '
        "is cut to 120 characters. Honours `Idempotency-Key`."
    ),
)
async def create_thread(
    body: MyteThreadCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteThread:
    me = _me(ctx)
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO myte_threads (org_id, user_id, title, scope) "
                    "VALUES (:o, :u, :t, CAST(:s AS jsonb)) "
                    f"RETURNING {_THREAD_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(me),
                    "t": clean_title(body.title),
                    "s": _jsonb(body.scope.model_dump(mode="json")) if body.scope else None,
                },
            )
        )
        .mappings()
        .one()
    )
    response.headers["Location"] = f"/api/v1/myte/threads/{row['id']}"
    return MyteThread(**_thread(row))


@router.get(
    "/threads/{thread_id}",
    response_model=MyteThreadDetail,
    summary="Open an Ask Myte conversation",
    description="One of your conversations with its latest 200 messages, oldest first.",
)
async def get_thread(
    thread_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteThreadDetail:
    me = _me(ctx)
    thread = await _owned_thread(session, thread_id, me)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_MESSAGE_COLUMNS} FROM ("
                    f" SELECT {_MESSAGE_COLUMNS} FROM myte_messages "
                    " WHERE thread_id = :t AND user_id = :u "
                    " ORDER BY created_at DESC, id DESC LIMIT :n"
                    ") latest ORDER BY created_at ASC, id ASC"
                ),
                {"t": str(thread_id), "u": str(me), "n": MAX_MESSAGES_SERVED},
            )
        )
        .mappings()
        .all()
    )
    return MyteThreadDetail(**thread, messages=[_message(r) for r in rows])


@router.patch(
    "/threads/{thread_id}",
    response_model=MyteThread,
    summary="Rename or pin an Ask Myte conversation",
)
async def update_thread(
    thread_id: uuid.UUID,
    body: MyteThreadUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteThread:
    me = _me(ctx)
    sets: list[str] = []
    params: dict[str, Any] = {"id": str(thread_id), "u": str(me)}
    if "title" in body.model_fields_set:
        sets.append("title = :title")
        params["title"] = clean_title(body.title)
    if body.pinned is not None:
        sets.append("pinned = :pinned")
        params["pinned"] = body.pinned
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE myte_threads SET {', '.join(sets)}, updated_at = now() "
                    f"WHERE id = :id AND user_id = :u RETURNING {_THREAD_COLUMNS}"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return MyteThread(**_thread(row))


@router.delete(
    "/threads/{thread_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an Ask Myte conversation",
    description="Deletes the conversation and every message in it, for good.",
)
async def delete_thread(
    thread_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    gone = (
        await session.execute(
            text("DELETE FROM myte_threads WHERE id = :id AND user_id = :u RETURNING id"),
            {"id": str(thread_id), "u": str(me)},
        )
    ).first()
    if gone is None:
        raise not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.delete(
    "/threads",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete all your Ask Myte conversations",
    description="Deletes every conversation of yours and their messages, for good.",
)
async def delete_all_threads(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    await session.execute(text("DELETE FROM myte_threads WHERE user_id = :u"), {"u": str(me)})
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/threads/{thread_id}/messages",
    response_model=MyteMessageList,
    status_code=status.HTTP_201_CREATED,
    summary="Save a turn to an Ask Myte conversation",
    description=(
        "Appends one or two messages (a question and its answer) in order and moves the "
        "conversation's `last_message_at` to the newest. `payload` is at most 64 KB. Honours "
        "`Idempotency-Key`, so a retried save stores the turn once."
    ),
)
async def append_messages(
    thread_id: uuid.UUID,
    body: MyteMessagesAppend,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteMessageList:
    me = _me(ctx)
    # Locked, so two saves to one thread append one after the other and the count holds.
    await _owned_thread(session, thread_id, me, lock=True)
    count = int(
        (
            await session.execute(
                text("SELECT count(*) FROM myte_messages WHERE thread_id = :t AND user_id = :u"),
                {"t": str(thread_id), "u": str(me)},
            )
        ).scalar()
        or 0
    )
    if count + len(body.messages) > MAX_MESSAGES_PER_THREAD:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            "This conversation is full; start a new one",
        )
    saved: list[MyteMessage] = []
    for message in body.messages:
        # clock_timestamp(), not now(): two messages in one transaction keep their order.
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO myte_messages "
                        "(org_id, thread_id, user_id, role, content, payload, created_at) "
                        "VALUES (:o, :t, :u, :r, :c, CAST(:p AS jsonb), clock_timestamp()) "
                        f"RETURNING {_MESSAGE_COLUMNS}"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "t": str(thread_id),
                        "u": str(me),
                        "r": message.role,
                        "c": message.content,
                        "p": _jsonb(message.payload),
                    },
                )
            )
            .mappings()
            .one()
        )
        saved.append(_message(row))
    await session.execute(
        text(
            "UPDATE myte_threads SET last_message_at = GREATEST(last_message_at, :at), "
            "updated_at = now() WHERE id = :t AND user_id = :u"
        ),
        {"at": saved[-1].created_at, "t": str(thread_id), "u": str(me)},
    )
    return MyteMessageList(items=saved)


@router.patch(
    "/messages/{message_id}",
    response_model=MyteMessage,
    summary="Rate one of Myte's answers",
    description="1 helpful, -1 not helpful, 0 clears the rating (and its reason).",
)
async def rate_message(
    message_id: uuid.UUID,
    body: MyteFeedback,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteMessage:
    me = _me(ctx)
    found = (
        await session.execute(
            text(
                "SELECT m.role FROM myte_messages m JOIN myte_threads t ON t.id = m.thread_id "
                "WHERE m.id = :id AND m.user_id = :u AND t.user_id = :u"
            ),
            {"id": str(message_id), "u": str(me)},
        )
    ).first()
    if found is None:
        raise not_found()
    if found[0] != "myte":
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Only Myte's answers take feedback")
    reason = None
    if body.feedback != 0 and body.feedback_reason is not None:
        reason = " ".join(body.feedback_reason.split()) or None
    row = (
        (
            await session.execute(
                text(
                    "UPDATE myte_messages SET feedback = :f, feedback_reason = :fr "
                    f"WHERE id = :id AND user_id = :u RETURNING {_MESSAGE_COLUMNS}"
                ),
                {"f": body.feedback, "fr": reason, "id": str(message_id), "u": str(me)},
            )
        )
        .mappings()
        .one()
    )
    return _message(row)
