"""What your Myte remembers, and your routines: `/api/v1/myte/memories`, `/api/v1/myte/routines`
(ADR-0049 F4, F5).

Both are owner only, admins included: every statement carries `user_id = caller`, on top of RLS
keeping the row inside the workspace. Anyone else's memory or routine is a 404.

Memories (F4). A durable preference or fact the person stated, stored when they confirm Myte's
`memory` proposal or add one in Your Myte. The UI server sends up to 30 with every ask.

- `GET /memories`: `{items}` newest first (all of them; at most 100 exist).
- `POST /memories` `{text (1..300, trimmed), source_thread_id?}`: 201. `source_thread_id`, when
  given, must be one of the caller's threads (else 404). The same text again (ignoring case and
  spacing) returns the memory already stored instead of a duplicate. Past 100 memories: 422.
  Honours `Idempotency-Key`.
- `DELETE /memories/{id}`: 204.

Routines (F5). A recurring question, run by the app the next time the person opens Mytheus once
it is due: the app reads `GET /routines/due`, streams each question into a new thread and records
it with `POST /routines/{id}/ran {thread_id}`.

- `GET /routines`: `{items}`, oldest first.
- `POST /routines` `{question, scope?, cadence, weekday?, hour, timezone, enabled?}`: 201, with
  `next_run_at` computed. `weekday` (0 Monday to 6) is required for `weekly` and must be absent
  (or null) otherwise. At most 10 per person (422). Honours `Idempotency-Key`.
- `PATCH /routines/{id}`: any of those fields. `next_run_at` is recomputed when the cadence,
  weekday, hour or timezone changes, and when a disabled routine is enabled again (so it does not
  come back already due). Moving away from `weekly` drops the weekday.
- `DELETE /routines/{id}`: 204.
- `GET /routines/due`: `{items}` enabled with `next_run_at <= now()`, the most overdue first.
- `POST /routines/{id}/ran` `{thread_id}`: sets `last_run_at` (now) and `last_thread_id` (one of the
  caller's threads, else 404), and moves `next_run_at` to the next slot strictly after now. A
  routine that was missed for days runs once, not once per missed slot.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.myte_schedule import Cadence, next_run, zone
from services.core_api.routes.myte import MyteScope, _jsonb, _loads, _me

router = APIRouter(prefix="/api/v1/myte", tags=["myte"])

MAX_MEMORY = 300
MAX_MEMORIES = 100
MAX_ROUTINE_QUESTION = 1_000
MAX_ROUTINES = 10


async def _lock_owner(session: AsyncSession, kind: str, me: uuid.UUID) -> None:
    """Serialise one person's creates of `kind`, so two at once cannot both pass the limit."""
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"), {"k": f"{kind}:{me}"}
    )


async def _owned_thread_or_404(
    session: AsyncSession, thread_id: uuid.UUID | None, me: uuid.UUID
) -> None:
    if thread_id is None:
        return
    found = (
        await session.execute(
            text("SELECT 1 FROM myte_threads WHERE id = :t AND user_id = :u"),
            {"t": str(thread_id), "u": str(me)},
        )
    ).first()
    if found is None:
        raise not_found()


async def _count(session: AsyncSession, table: str, me: uuid.UUID) -> int:
    return int(
        (
            await session.execute(
                text(f"SELECT count(*) FROM {table} WHERE user_id = :u"), {"u": str(me)}
            )
        ).scalar()
        or 0
    )


# --- memories -----------------------------------------------------------------------------------


def clean_memory(value: str) -> str:
    """Trimmed, inner whitespace collapsed: a memory is one line."""
    return " ".join(value.split())


class MyteMemory(BaseModel):
    id: uuid.UUID
    text: str
    source_thread_id: uuid.UUID | None = None
    created_at: datetime


class MyteMemoryList(BaseModel):
    items: list[MyteMemory]


class MyteMemoryCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(max_length=4 * MAX_MEMORY)
    source_thread_id: uuid.UUID | None = None

    @field_validator("text")
    @classmethod
    def _text(cls, value: str) -> str:
        value = clean_memory(value)
        if not 1 <= len(value) <= MAX_MEMORY:
            raise ValueError(f"text must be 1 to {MAX_MEMORY} characters")
        return value


_MEMORY_COLUMNS = "id, text, source_thread_id, created_at"


@router.get(
    "/memories",
    response_model=MyteMemoryList,
    summary="What your Myte remembers about you",
    description="Your own memories, newest first (at most 100 exist).",
)
async def list_memories(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteMemoryList:
    me = _me(ctx)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_MEMORY_COLUMNS} FROM myte_memories WHERE user_id = :u "
                    "ORDER BY created_at DESC, id DESC LIMIT :n"
                ),
                {"u": str(me), "n": MAX_MEMORIES},
            )
        )
        .mappings()
        .all()
    )
    return MyteMemoryList(items=[MyteMemory(**dict(r)) for r in rows])


@router.post(
    "/memories",
    response_model=MyteMemory,
    status_code=status.HTTP_201_CREATED,
    summary="Ask your Myte to remember something",
    description=(
        "Text 1 to 300 characters (trimmed). The same text again returns the memory already "
        "stored. At most 100 per person (422). `source_thread_id` must be one of your threads. "
        "Honours `Idempotency-Key`."
    ),
)
async def create_memory(
    body: MyteMemoryCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteMemory:
    me = _me(ctx)
    await _owned_thread_or_404(session, body.source_thread_id, me)
    await _lock_owner(session, "myte_memories", me)
    same = (
        (
            await session.execute(
                text(
                    f"SELECT {_MEMORY_COLUMNS} FROM myte_memories WHERE user_id = :u "
                    "AND lower(text) = lower(:t) ORDER BY created_at LIMIT 1"
                ),
                {"u": str(me), "t": body.text},
            )
        )
        .mappings()
        .first()
    )
    if same is not None:
        response.headers["Location"] = f"/api/v1/myte/memories/{same['id']}"
        return MyteMemory(**dict(same))
    if await _count(session, "myte_memories", me) >= MAX_MEMORIES:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"Your Myte remembers at most {MAX_MEMORIES} things; delete one first",
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO myte_memories (org_id, user_id, text, source_thread_id) "
                    f"VALUES (:o, :u, :t, :s) RETURNING {_MEMORY_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(me),
                    "t": body.text,
                    "s": str(body.source_thread_id) if body.source_thread_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    response.headers["Location"] = f"/api/v1/myte/memories/{row['id']}"
    return MyteMemory(**dict(row))


@router.delete(
    "/memories/{memory_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Make your Myte forget something",
)
async def delete_memory(
    memory_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    gone = (
        await session.execute(
            text("DELETE FROM myte_memories WHERE id = :id AND user_id = :u RETURNING id"),
            {"id": str(memory_id), "u": str(me)},
        )
    ).first()
    if gone is None:
        raise not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- routines -----------------------------------------------------------------------------------


def _check_timezone(value: str) -> str:
    zone(value)
    return value


def _check_question(value: str) -> str:
    value = value.strip()
    if not 1 <= len(value) <= MAX_ROUTINE_QUESTION:
        raise ValueError(f"question must be 1 to {MAX_ROUTINE_QUESTION} characters")
    return value


class MyteRoutine(BaseModel):
    id: uuid.UUID
    question: str
    scope: MyteScope | None = None
    cadence: Cadence
    weekday: int | None = Field(default=None, description="0 Monday to 6 Sunday; weekly only.")
    hour: int = Field(description="0 to 23, on the hour, in `timezone`.")
    timezone: str = Field(description="An IANA timezone, e.g. Europe/London.")
    enabled: bool
    next_run_at: datetime
    last_run_at: datetime | None = None
    last_thread_id: uuid.UUID | None = None
    created_at: datetime


class MyteRoutineList(BaseModel):
    items: list[MyteRoutine]


class MyteRoutineCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: str = Field(max_length=4 * MAX_ROUTINE_QUESTION)
    scope: MyteScope | None = None
    cadence: Cadence
    weekday: int | None = Field(default=None, ge=0, le=6)
    hour: int = Field(ge=0, le=23)
    timezone: str = Field(max_length=64)
    enabled: bool = True

    @field_validator("question")
    @classmethod
    def _question(cls, value: str) -> str:
        return _check_question(value)

    @field_validator("timezone")
    @classmethod
    def _timezone(cls, value: str) -> str:
        return _check_timezone(value)

    @model_validator(mode="after")
    def _weekday_for_weekly(self) -> MyteRoutineCreate:
        if self.cadence == "weekly" and self.weekday is None:
            raise ValueError("a weekly routine needs a weekday (0 Monday to 6 Sunday)")
        if self.cadence != "weekly" and self.weekday is not None:
            raise ValueError("weekday is for weekly routines only")
        return self


class MyteRoutineUpdate(BaseModel):
    """At least one field. `weekday` may be sent with a move to `weekly`, or alone on one."""

    model_config = ConfigDict(extra="forbid")

    question: str | None = Field(default=None, max_length=4 * MAX_ROUTINE_QUESTION)
    scope: MyteScope | None = None
    cadence: Cadence | None = None
    weekday: int | None = Field(default=None, ge=0, le=6)
    hour: int | None = Field(default=None, ge=0, le=23)
    timezone: str | None = Field(default=None, max_length=64)
    enabled: bool | None = None

    @field_validator("question")
    @classmethod
    def _question(cls, value: str | None) -> str | None:
        return None if value is None else _check_question(value)

    @field_validator("timezone")
    @classmethod
    def _timezone(cls, value: str | None) -> str | None:
        return None if value is None else _check_timezone(value)

    @model_validator(mode="after")
    def _at_least_one(self) -> MyteRoutineUpdate:
        if not self.model_fields_set:
            raise ValueError("PATCH requires at least one field")
        for name in ("question", "cadence", "hour", "timezone", "enabled"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} cannot be null")
        return self


class MyteRoutineRan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    thread_id: uuid.UUID


_ROUTINE_COLUMNS = (
    "id, question, scope, cadence, weekday, hour, timezone, enabled, next_run_at, last_run_at, "
    "last_thread_id, created_at"
)


def _routine(row: Any) -> MyteRoutine:
    out = dict(row)
    out["scope"] = _loads(out.get("scope"))
    return MyteRoutine(**out)


def _now() -> datetime:
    return datetime.now(UTC)


async def _owned_routine(
    session: AsyncSession, routine_id: uuid.UUID, me: uuid.UUID, *, lock: bool = False
) -> dict[str, Any]:
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_ROUTINE_COLUMNS} FROM myte_routines WHERE id = :id AND user_id = :u"
                    + (" FOR UPDATE" if lock else "")
                ),
                {"id": str(routine_id), "u": str(me)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()
    return dict(row)


@router.get(
    "/routines",
    response_model=MyteRoutineList,
    summary="Your Ask Myte routines",
    description="Your recurring questions, oldest first (at most 10 exist).",
)
async def list_routines(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteRoutineList:
    me = _me(ctx)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_ROUTINE_COLUMNS} FROM myte_routines WHERE user_id = :u "
                    "ORDER BY created_at, id"
                ),
                {"u": str(me)},
            )
        )
        .mappings()
        .all()
    )
    return MyteRoutineList(items=[_routine(r) for r in rows])


@router.get(
    "/routines/due",
    response_model=MyteRoutineList,
    summary="Your routines that are due to run",
    description="Enabled routines whose `next_run_at` has passed, the most overdue first.",
)
async def list_due_routines(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteRoutineList:
    me = _me(ctx)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_ROUTINE_COLUMNS} FROM myte_routines WHERE user_id = :u "
                    "AND enabled AND next_run_at <= now() ORDER BY next_run_at, id"
                ),
                {"u": str(me)},
            )
        )
        .mappings()
        .all()
    )
    return MyteRoutineList(items=[_routine(r) for r in rows])


@router.post(
    "/routines",
    response_model=MyteRoutine,
    status_code=status.HTTP_201_CREATED,
    summary="Make a question a routine",
    description=(
        "Runs at `hour:00` in `timezone`, daily, on weekdays, or weekly on `weekday` (0 Monday). "
        "`next_run_at` is computed. At most 10 per person (422). Honours `Idempotency-Key`."
    ),
)
async def create_routine(
    body: MyteRoutineCreate,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteRoutine:
    me = _me(ctx)
    await _lock_owner(session, "myte_routines", me)
    if await _count(session, "myte_routines", me) >= MAX_ROUTINES:
        raise ProblemDetail(
            422,
            ErrorCode.VALIDATION_FAILED,
            f"You can keep at most {MAX_ROUTINES} routines; delete one first",
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO myte_routines (org_id, user_id, question, scope, cadence, "
                    "weekday, hour, timezone, enabled, next_run_at) VALUES (:o, :u, :q, "
                    "CAST(:s AS jsonb), :c, :w, :h, :tz, :e, :n) "
                    f"RETURNING {_ROUTINE_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(me),
                    "q": body.question,
                    "s": _jsonb(body.scope.model_dump(mode="json")) if body.scope else None,
                    "c": body.cadence,
                    "w": body.weekday,
                    "h": body.hour,
                    "tz": body.timezone,
                    "e": body.enabled,
                    "n": next_run(body.cadence, body.hour, body.timezone, _now(), body.weekday),
                },
            )
        )
        .mappings()
        .one()
    )
    response.headers["Location"] = f"/api/v1/myte/routines/{row['id']}"
    return _routine(row)


def apply_update(current: dict[str, Any], body: MyteRoutineUpdate, now: datetime) -> dict[str, Any]:
    """The routine's new columns after `body`, with `next_run_at` recomputed when it must be.

    Pure, so the rules are tested without a database. Raises ValueError for a weekly routine left
    with no weekday.
    """
    fields = body.model_fields_set
    out = {
        k: current[k]
        for k in ("question", "scope", "cadence", "weekday", "hour", "timezone", "enabled")
    }
    if "question" in fields:
        out["question"] = body.question
    if "scope" in fields:
        out["scope"] = body.scope.model_dump(mode="json") if body.scope else None
    for name in ("cadence", "hour", "timezone", "enabled"):
        if name in fields:
            out[name] = getattr(body, name)
    if "weekday" in fields:
        out["weekday"] = body.weekday
    if out["cadence"] != "weekly":
        out["weekday"] = None
    elif out["weekday"] is None:
        raise ValueError("a weekly routine needs a weekday (0 Monday to 6 Sunday)")
    timing = {"cadence", "weekday", "hour", "timezone"} & fields
    re_enabled = "enabled" in fields and body.enabled and not current["enabled"]
    if timing or re_enabled:
        out["next_run_at"] = next_run(
            out["cadence"], out["hour"], out["timezone"], now, out["weekday"]
        )
    else:
        out["next_run_at"] = current["next_run_at"]
    return out


@router.patch(
    "/routines/{routine_id}",
    response_model=MyteRoutine,
    summary="Change a routine",
    description=(
        "Any of question, scope, cadence, weekday, hour, timezone, enabled. `next_run_at` is "
        "recomputed when the timing changes or a disabled routine is enabled again."
    ),
)
async def update_routine(
    routine_id: uuid.UUID,
    body: MyteRoutineUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteRoutine:
    me = _me(ctx)
    current = await _owned_routine(session, routine_id, me, lock=True)
    current["scope"] = _loads(current.get("scope"))
    try:
        new = apply_update(current, body, _now())
    except ValueError as exc:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, str(exc)) from exc
    row = (
        (
            await session.execute(
                text(
                    "UPDATE myte_routines SET question = :q, scope = CAST(:s AS jsonb), "
                    "cadence = :c, weekday = :w, hour = :h, timezone = :tz, enabled = :e, "
                    "next_run_at = :n WHERE id = :id AND user_id = :u "
                    f"RETURNING {_ROUTINE_COLUMNS}"
                ),
                {
                    "q": new["question"],
                    "s": _jsonb(new["scope"]),
                    "c": new["cadence"],
                    "w": new["weekday"],
                    "h": new["hour"],
                    "tz": new["timezone"],
                    "e": new["enabled"],
                    "n": new["next_run_at"],
                    "id": str(routine_id),
                    "u": str(me),
                },
            )
        )
        .mappings()
        .one()
    )
    return _routine(row)


@router.delete(
    "/routines/{routine_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a routine",
)
async def delete_routine(
    routine_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    me = _me(ctx)
    gone = (
        await session.execute(
            text("DELETE FROM myte_routines WHERE id = :id AND user_id = :u RETURNING id"),
            {"id": str(routine_id), "u": str(me)},
        )
    ).first()
    if gone is None:
        raise not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/routines/{routine_id}/ran",
    response_model=MyteRoutine,
    summary="Record that a routine ran",
    description=(
        "Sets `last_run_at` to now and `last_thread_id`, and moves `next_run_at` to the next slot "
        "strictly after now. `thread_id` must be one of your threads."
    ),
)
async def routine_ran(
    routine_id: uuid.UUID,
    body: MyteRoutineRan,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MyteRoutine:
    me = _me(ctx)
    current = await _owned_routine(session, routine_id, me, lock=True)
    await _owned_thread_or_404(session, body.thread_id, me)
    now = _now()
    row = (
        (
            await session.execute(
                text(
                    "UPDATE myte_routines SET last_run_at = :now, last_thread_id = :t, "
                    "next_run_at = :n WHERE id = :id AND user_id = :u "
                    f"RETURNING {_ROUTINE_COLUMNS}"
                ),
                {
                    "now": now,
                    "t": str(body.thread_id),
                    "n": next_run(
                        current["cadence"],
                        current["hour"],
                        current["timezone"],
                        now,
                        current["weekday"],
                    ),
                    "id": str(routine_id),
                    "u": str(me),
                },
            )
        )
        .mappings()
        .one()
    )
    return _routine(row)
