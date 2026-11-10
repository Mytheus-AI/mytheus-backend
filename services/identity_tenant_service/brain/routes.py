"""The company brain's REST surface (ADR-0039 A1 to A8).

- `/api/v1/org/profile`, `/glossary`, `/topic-owners`, `/goals`, `/ground-rules`: reads are open
  to members, writes need `admin` or above (`require_tier`, so a denial is a 403 and audited).
- `/api/v1/org/readiness`: owner/admin. `/api/v1/org/welcome`: any member.
- `/api/v1/users/me/topic-watches`: self only. Another person's watch is a 404, like another
  tenant's row, so the route is not an oracle for who watches what.

The tenant comes from the verified token, never the path. Every row id is looked up under RLS,
so another tenant's id and an unknown id are the same 404.

Limits are 422 naming the field: the body limits in `schemas.py`, and the per-org row caps here
(500 terms, 200 owners, 30 active goals, 20 watches per person), each counted under a
transaction-level advisory lock keyed by (table, org) so two concurrent writes cannot both pass
the count. The same lock serialises the case-insensitive uniqueness check, so a duplicate is a
clean 409 rather than a unique-violation 500.

Every write commits, then evicts `nexus:{org_id}:org_ctx` (A9), in that order: evicting inside
the transaction would let a concurrent `GetOrgContext` re-cache the pre-commit state. The
eviction is best-effort; the 5-minute TTL bounds anything it misses. Company-wide writes write
one audit row each, in the same transaction; a personal topic watch does not.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.brain import schemas as s
from services.identity_tenant_service.brain.context import invalidate_org_context

router = APIRouter(prefix="/api/v1", tags=["company-brain"])

# Audit actions. Kept beside the routes that write them; the vocabulary is `org_brain.*`.
ACTION_PROFILE_UPDATED = "org_brain.profile.updated"
ACTION_GLOSSARY_CREATED = "org_brain.glossary.created"
ACTION_GLOSSARY_UPDATED = "org_brain.glossary.updated"
ACTION_GLOSSARY_DELETED = "org_brain.glossary.deleted"
ACTION_GLOSSARY_IMPORTED = "org_brain.glossary.imported"
ACTION_OWNER_CREATED = "org_brain.topic_owner.created"
ACTION_OWNER_UPDATED = "org_brain.topic_owner.updated"
ACTION_OWNER_DELETED = "org_brain.topic_owner.deleted"
ACTION_GOAL_CREATED = "org_brain.goal.created"
ACTION_GOAL_UPDATED = "org_brain.goal.updated"
ACTION_GOAL_DELETED = "org_brain.goal.deleted"
ACTION_GROUND_RULES_UPDATED = "org_brain.ground_rules.updated"

# A7 thresholds.
PROFILE_MIN_DESCRIPTION = 40
VOCABULARY_MIN_TERMS = 10
OWNERS_MIN = 5
WELCOME_GLOSSARY_LIMIT = 40

Member = Annotated[RequestContext, Depends(require_tier("member"))]
Admin = Annotated[RequestContext, Depends(require_tier("admin"))]

_PROFILE_COLS = "description, industry, products, customers, updated_by, updated_at"
_TERM_COLS = "id, term, kind, definition, aliases, created_at, updated_at"
_OWNER_COLS = "id, topic, owner_user_id, created_at"
_GOAL_COLS = (
    "id, title, description, period, status, owner_user_id, position, created_at, updated_at"
)
_WATCH_COLS = "id, user_id, topic, created_at"


# --- helpers -----------------------------------------------------------------------------------


@asynccontextmanager
async def _tx(ctx: RequestContext) -> AsyncIterator[AsyncSession]:
    """One transaction with `app.current_org` bound, committed when the block exits."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        yield session


def _invalid(field: str, message: str) -> ProblemDetail:
    """A 422 shaped like a body-validation failure, so a client reads one `errors[].loc`."""
    return ProblemDetail(
        422,
        ErrorCode.VALIDATION_FAILED,
        message,
        extra={"errors": [{"loc": ["body", field], "msg": message, "type": "value_error"}]},
    )


def _conflict(message: str) -> ProblemDetail:
    return ProblemDetail(409, ErrorCode.VERSION_CONFLICT, message)


async def _lock(session: AsyncSession, scope: str, org_id: uuid.UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))"),
        {"k": f"{scope}:{org_id}"},
    )


async def _count(session: AsyncSession, sql: str, params: dict[str, Any] | None = None) -> int:
    return int((await session.execute(text(sql), params or {})).scalar_one())


async def _require_member_user(session: AsyncSession, field: str, user_id: uuid.UUID) -> None:
    """The FK alone does not confine a reference to the tenant; this does, under RLS."""
    found = (
        await session.execute(
            text("SELECT 1 FROM users WHERE id = :u AND deleted_at IS NULL"), {"u": str(user_id)}
        )
    ).first()
    if found is None:
        raise _invalid(field, "must be a member of this organization")


async def _audit(
    session: AsyncSession,
    ctx: RequestContext,
    action: str,
    target_type: str,
    target_id: Any,
    metadata: dict[str, Any] | None = None,
) -> None:
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=action,
        target_type=target_type,
        target_id=target_id,
        metadata=metadata,
        ip=ctx.ip,
    )


def _profile(row: Any) -> s.OrgProfile:
    if row is None:
        return s.OrgProfile(
            description="", industry="", products=[], customers=[], updated_by=None, updated_at=None
        )
    return s.OrgProfile(**dict(row))


async def _read_profile(session: AsyncSession, org_id: uuid.UUID) -> s.OrgProfile:
    row = (
        (
            await session.execute(
                text(f"SELECT {_PROFILE_COLS} FROM org_profiles WHERE org_id = :o"),
                {"o": str(org_id)},
            )
        )
        .mappings()
        .first()
    )
    return _profile(row)


async def _terms(session: AsyncSession, limit: int | None = None) -> list[s.GlossaryTerm]:
    sql = f"SELECT {_TERM_COLS} FROM org_glossary_terms ORDER BY lower(term), id"
    params: dict[str, Any] = {}
    if limit is not None:
        sql += " LIMIT :n"
        params["n"] = limit
    rows = (await session.execute(text(sql), params)).mappings().all()
    return [s.GlossaryTerm(**dict(r)) for r in rows]


async def _owners(session: AsyncSession) -> list[s.TopicOwner]:
    rows = (
        (
            await session.execute(
                text(f"SELECT {_OWNER_COLS} FROM topic_owners ORDER BY lower(topic), id")
            )
        )
        .mappings()
        .all()
    )
    return [s.TopicOwner(**dict(r)) for r in rows]


async def _goals(session: AsyncSession, *, active_only: bool = False) -> list[s.OrgGoal]:
    where = "WHERE status = 'active' " if active_only else ""
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_GOAL_COLS} FROM org_goals {where}"
                    "ORDER BY position, created_at, id"
                )
            )
        )
        .mappings()
        .all()
    )
    return [s.OrgGoal(**dict(r)) for r in rows]


# --- A1 company profile ------------------------------------------------------------------------


@router.get("/org/profile", response_model=s.OrgProfile)
async def read_profile(ctx: Member) -> s.OrgProfile:
    """authz: member. An org that never saved a profile reads as empty, not 404."""
    async with _tx(ctx) as session:
        return await _read_profile(session, ctx.org_id)


@router.put("/org/profile", response_model=s.OrgProfile)
async def put_profile(body: s.OrgProfileUpdate, ctx: Admin) -> s.OrgProfile:
    """authz: admin. Replaces the whole profile."""
    async with _tx(ctx) as session:
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO org_profiles (org_id, description, industry, products, "
                        "customers, updated_by, updated_at) "
                        "VALUES (:o, :d, :i, :p, :c, :by, now()) "
                        "ON CONFLICT (org_id) DO UPDATE SET description = EXCLUDED.description, "
                        "industry = EXCLUDED.industry, products = EXCLUDED.products, "
                        "customers = EXCLUDED.customers, updated_by = EXCLUDED.updated_by, "
                        f"updated_at = now() RETURNING {_PROFILE_COLS}"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "d": body.description,
                        "i": body.industry,
                        "p": body.products,
                        "c": body.customers,
                        "by": str(ctx.user_id) if ctx.user_id else None,
                    },
                )
            )
            .mappings()
            .one()
        )
        await _audit(session, ctx, ACTION_PROFILE_UPDATED, "org_profile", ctx.org_id)
    await invalidate_org_context(ctx.org_id)
    return _profile(row)


# --- A2 vocabulary -----------------------------------------------------------------------------


@router.get("/org/glossary", response_model=s.GlossaryTermList)
async def list_glossary(ctx: Member) -> s.GlossaryTermList:
    """authz: member. Every term (at most 500), ordered by term, case-insensitively."""
    async with _tx(ctx) as session:
        return s.GlossaryTermList(items=await _terms(session))


async def _term_taken(session: AsyncSession, term: str, exclude: uuid.UUID | None = None) -> bool:
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM org_glossary_terms WHERE lower(term) = lower(:t) "
                "AND (CAST(:x AS uuid) IS NULL OR id <> CAST(:x AS uuid))"
            ),
            {"t": term, "x": str(exclude) if exclude else None},
        )
    ).first()
    return row is not None


@router.post("/org/glossary", status_code=status.HTTP_201_CREATED, response_model=s.GlossaryTerm)
async def create_glossary_term(body: s.GlossaryTermCreate, ctx: Admin) -> s.GlossaryTerm:
    """authz: admin. 409 when the term exists (case-insensitively); 422 at 500 terms."""
    async with _tx(ctx) as session:
        await _lock(session, "org_glossary_terms", ctx.org_id)
        if await _term_taken(session, body.term):
            raise _conflict("A term with this name already exists")
        if await _count(session, "SELECT count(*) FROM org_glossary_terms") >= (
            s.MAX_GLOSSARY_TERMS
        ):
            raise _invalid("term", f"The glossary holds at most {s.MAX_GLOSSARY_TERMS} terms")
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO org_glossary_terms (org_id, term, kind, definition, aliases) "
                        f"VALUES (:o, :t, :k, :d, :a) RETURNING {_TERM_COLS}"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "t": body.term,
                        "k": body.kind,
                        "d": body.definition,
                        "a": body.aliases,
                    },
                )
            )
            .mappings()
            .one()
        )
        await _audit(session, ctx, ACTION_GLOSSARY_CREATED, "org_glossary_term", row["id"])
    await invalidate_org_context(ctx.org_id)
    return s.GlossaryTerm(**dict(row))


@router.patch("/org/glossary/{term_id}", response_model=s.GlossaryTerm)
async def update_glossary_term(
    term_id: uuid.UUID, body: s.GlossaryTermUpdate, ctx: Admin
) -> s.GlossaryTerm:
    """authz: admin. 404 for an unknown or another tenant's id; 409 on a rename collision."""
    fields = body.model_dump(exclude_unset=True)
    async with _tx(ctx) as session:
        await _lock(session, "org_glossary_terms", ctx.org_id)
        if "term" in fields and await _term_taken(session, fields["term"], exclude=term_id):
            raise _conflict("A term with this name already exists")
        sets = ", ".join(f"{col} = :{col}" for col in fields)
        row = (
            (
                await session.execute(
                    text(
                        f"UPDATE org_glossary_terms SET {sets}, updated_at = now() "
                        f"WHERE id = :id RETURNING {_TERM_COLS}"
                    ),
                    {**fields, "id": str(term_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found()
        await _audit(
            session,
            ctx,
            ACTION_GLOSSARY_UPDATED,
            "org_glossary_term",
            term_id,
            {"fields": sorted(fields)},
        )
    await invalidate_org_context(ctx.org_id)
    return s.GlossaryTerm(**dict(row))


@router.delete("/org/glossary/{term_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_glossary_term(term_id: uuid.UUID, ctx: Admin) -> Response:
    """authz: admin. 404 for an unknown or another tenant's id."""
    async with _tx(ctx) as session:
        row = (
            await session.execute(
                text("DELETE FROM org_glossary_terms WHERE id = :id RETURNING id"),
                {"id": str(term_id)},
            )
        ).first()
        if row is None:
            raise not_found()
        await _audit(session, ctx, ACTION_GLOSSARY_DELETED, "org_glossary_term", term_id)
    await invalidate_org_context(ctx.org_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/org/glossary/import", response_model=s.GlossaryImportResult)
async def import_glossary(body: s.GlossaryImport, ctx: Admin) -> s.GlossaryImportResult:
    """authz: admin. Upsert up to 200 terms by `lower(term)`; all or nothing.

    A term already present takes the imported spelling, kind, definition and aliases. A repeat
    within the same import collapses to its last entry. 422 naming `terms` when the new terms
    would take the glossary past 500; nothing is written then.
    """
    latest: dict[str, s.GlossaryTermCreate] = {}
    for entry in body.terms:
        latest.pop(entry.term.lower(), None)  # re-insert so the last entry keeps its place
        latest[entry.term.lower()] = entry
    async with _tx(ctx) as session:
        await _lock(session, "org_glossary_terms", ctx.org_id)
        existing = set(
            (
                await session.execute(
                    text(
                        "SELECT lower(term) FROM org_glossary_terms "
                        "WHERE lower(term) = ANY(CAST(:ts AS text[]))"
                    ),
                    {"ts": [e.term.lower() for e in latest.values()]},
                )
            )
            .scalars()
            .all()
        )
        new = [e for e in latest.values() if e.term.lower() not in existing]
        total = await _count(session, "SELECT count(*) FROM org_glossary_terms")
        if total + len(new) > s.MAX_GLOSSARY_TERMS:
            raise _invalid(
                "terms",
                f"Importing {len(new)} new terms would take the glossary past "
                f"{s.MAX_GLOSSARY_TERMS} (it holds {total})",
            )
        for entry in latest.values():
            await session.execute(
                text(
                    "INSERT INTO org_glossary_terms (org_id, term, kind, definition, aliases) "
                    "VALUES (:o, :t, :k, :d, :a) "
                    "ON CONFLICT (org_id, lower(term)) DO UPDATE SET term = EXCLUDED.term, "
                    "kind = EXCLUDED.kind, definition = EXCLUDED.definition, "
                    "aliases = EXCLUDED.aliases, updated_at = now()"
                ),
                {
                    "o": str(ctx.org_id),
                    "t": entry.term,
                    "k": entry.kind,
                    "d": entry.definition,
                    "a": entry.aliases,
                },
            )
        created, updated = len(new), len(latest) - len(new)
        await _audit(
            session,
            ctx,
            ACTION_GLOSSARY_IMPORTED,
            "org_glossary_terms",
            ctx.org_id,
            {"created": created, "updated": updated},
        )
        items = await _terms(session)
    await invalidate_org_context(ctx.org_id)
    return s.GlossaryImportResult(created=created, updated=updated, items=items)


# --- A3 who owns what --------------------------------------------------------------------------


@router.get("/org/topic-owners", response_model=s.TopicOwnerList)
async def list_topic_owners(ctx: Member) -> s.TopicOwnerList:
    """authz: member."""
    async with _tx(ctx) as session:
        return s.TopicOwnerList(items=await _owners(session))


async def _topic_owned(session: AsyncSession, topic: str, exclude: uuid.UUID | None = None) -> bool:
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM topic_owners WHERE lower(topic) = lower(:t) "
                "AND (CAST(:x AS uuid) IS NULL OR id <> CAST(:x AS uuid))"
            ),
            {"t": topic, "x": str(exclude) if exclude else None},
        )
    ).first()
    return row is not None


@router.post("/org/topic-owners", status_code=status.HTTP_201_CREATED, response_model=s.TopicOwner)
async def create_topic_owner(body: s.TopicOwnerCreate, ctx: Admin) -> s.TopicOwner:
    """authz: admin. 409 when the topic already has an owner; 422 at 200 topics, or when
    `owner_user_id` is not a member of this organization."""
    async with _tx(ctx) as session:
        await _lock(session, "topic_owners", ctx.org_id)
        await _require_member_user(session, "owner_user_id", body.owner_user_id)
        if await _topic_owned(session, body.topic):
            raise _conflict("This topic already has an owner")
        if await _count(session, "SELECT count(*) FROM topic_owners") >= s.MAX_TOPIC_OWNERS:
            raise _invalid("topic", f"At most {s.MAX_TOPIC_OWNERS} topics can have an owner")
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO topic_owners (org_id, topic, owner_user_id) "
                        f"VALUES (:o, :t, :u) RETURNING {_OWNER_COLS}"
                    ),
                    {"o": str(ctx.org_id), "t": body.topic, "u": str(body.owner_user_id)},
                )
            )
            .mappings()
            .one()
        )
        await _audit(session, ctx, ACTION_OWNER_CREATED, "topic_owner", row["id"])
    await invalidate_org_context(ctx.org_id)
    return s.TopicOwner(**dict(row))


@router.patch("/org/topic-owners/{owner_id}", response_model=s.TopicOwner)
async def update_topic_owner(
    owner_id: uuid.UUID, body: s.TopicOwnerUpdate, ctx: Admin
) -> s.TopicOwner:
    """authz: admin. 404 for an unknown or another tenant's id; 409 on a topic collision."""
    fields = body.model_dump(exclude_unset=True)
    async with _tx(ctx) as session:
        await _lock(session, "topic_owners", ctx.org_id)
        if "owner_user_id" in fields:
            await _require_member_user(session, "owner_user_id", fields["owner_user_id"])
            fields["owner_user_id"] = str(fields["owner_user_id"])
        if "topic" in fields and await _topic_owned(session, fields["topic"], exclude=owner_id):
            raise _conflict("This topic already has an owner")
        sets = ", ".join(f"{col} = :{col}" for col in fields)
        row = (
            (
                await session.execute(
                    text(f"UPDATE topic_owners SET {sets} WHERE id = :id RETURNING {_OWNER_COLS}"),
                    {**fields, "id": str(owner_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found()
        await _audit(
            session, ctx, ACTION_OWNER_UPDATED, "topic_owner", owner_id, {"fields": sorted(fields)}
        )
    await invalidate_org_context(ctx.org_id)
    return s.TopicOwner(**dict(row))


@router.delete("/org/topic-owners/{owner_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_topic_owner(owner_id: uuid.UUID, ctx: Admin) -> Response:
    """authz: admin. 404 for an unknown or another tenant's id."""
    async with _tx(ctx) as session:
        row = (
            await session.execute(
                text("DELETE FROM topic_owners WHERE id = :id RETURNING id"), {"id": str(owner_id)}
            )
        ).first()
        if row is None:
            raise not_found()
        await _audit(session, ctx, ACTION_OWNER_DELETED, "topic_owner", owner_id)
    await invalidate_org_context(ctx.org_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- A4 goals ----------------------------------------------------------------------------------


@router.get("/org/goals", response_model=s.OrgGoalList)
async def list_goals(ctx: Member) -> s.OrgGoalList:
    """authz: member. Every goal, any status, in `position` order."""
    async with _tx(ctx) as session:
        return s.OrgGoalList(items=await _goals(session))


async def _active_goal_count(session: AsyncSession, exclude: uuid.UUID | None = None) -> int:
    return await _count(
        session,
        "SELECT count(*) FROM org_goals WHERE status = 'active' "
        "AND (CAST(:x AS uuid) IS NULL OR id <> CAST(:x AS uuid))",
        {"x": str(exclude) if exclude else None},
    )


@router.post("/org/goals", status_code=status.HTTP_201_CREATED, response_model=s.OrgGoal)
async def create_goal(body: s.OrgGoalCreate, ctx: Admin) -> s.OrgGoal:
    """authz: admin. 422 naming `status` when it would be the 31st active goal."""
    async with _tx(ctx) as session:
        await _lock(session, "org_goals", ctx.org_id)
        if body.owner_user_id is not None:
            await _require_member_user(session, "owner_user_id", body.owner_user_id)
        if body.status == "active" and await _active_goal_count(session) >= s.MAX_ACTIVE_GOALS:
            raise _invalid("status", f"At most {s.MAX_ACTIVE_GOALS} goals can be active")
        position = body.position
        if position is None:
            position = await _count(session, "SELECT COALESCE(max(position) + 1, 0) FROM org_goals")
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO org_goals (org_id, title, description, period, status, "
                        "owner_user_id, position) VALUES (:o, :t, :d, :p, :s, :u, :pos) "
                        f"RETURNING {_GOAL_COLS}"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "t": body.title,
                        "d": body.description,
                        "p": body.period,
                        "s": body.status,
                        "u": str(body.owner_user_id) if body.owner_user_id else None,
                        "pos": position,
                    },
                )
            )
            .mappings()
            .one()
        )
        await _audit(session, ctx, ACTION_GOAL_CREATED, "org_goal", row["id"])
    await invalidate_org_context(ctx.org_id)
    return s.OrgGoal(**dict(row))


@router.patch("/org/goals/{goal_id}", response_model=s.OrgGoal)
async def update_goal(goal_id: uuid.UUID, body: s.OrgGoalUpdate, ctx: Admin) -> s.OrgGoal:
    """authz: admin. 404 for an unknown or another tenant's id; 422 naming `status` when
    re-activating would make a 31st active goal."""
    fields = body.model_dump(exclude_unset=True)
    async with _tx(ctx) as session:
        await _lock(session, "org_goals", ctx.org_id)
        if fields.get("owner_user_id") is not None:
            await _require_member_user(session, "owner_user_id", fields["owner_user_id"])
            fields["owner_user_id"] = str(fields["owner_user_id"])
        if (
            fields.get("status") == "active"
            and await _active_goal_count(session, exclude=goal_id) >= s.MAX_ACTIVE_GOALS
        ):
            # Only a goal that exists can be over the cap; an unknown id stays a 404.
            exists = (
                await session.execute(
                    text("SELECT 1 FROM org_goals WHERE id = :id"), {"id": str(goal_id)}
                )
            ).first()
            if exists is None:
                raise not_found()
            raise _invalid("status", f"At most {s.MAX_ACTIVE_GOALS} goals can be active")
        sets = ", ".join(f"{col} = :{col}" for col in fields)
        row = (
            (
                await session.execute(
                    text(
                        f"UPDATE org_goals SET {sets}, updated_at = now() "
                        f"WHERE id = :id RETURNING {_GOAL_COLS}"
                    ),
                    {**fields, "id": str(goal_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found()
        await _audit(
            session, ctx, ACTION_GOAL_UPDATED, "org_goal", goal_id, {"fields": sorted(fields)}
        )
    await invalidate_org_context(ctx.org_id)
    return s.OrgGoal(**dict(row))


@router.delete("/org/goals/{goal_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_goal(goal_id: uuid.UUID, ctx: Admin) -> Response:
    """authz: admin. 404 for an unknown or another tenant's id."""
    async with _tx(ctx) as session:
        row = (
            await session.execute(
                text("DELETE FROM org_goals WHERE id = :id RETURNING id"), {"id": str(goal_id)}
            )
        ).first()
        if row is None:
            raise not_found()
        await _audit(session, ctx, ACTION_GOAL_DELETED, "org_goal", goal_id)
    await invalidate_org_context(ctx.org_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- A5 ground rules ---------------------------------------------------------------------------


@router.get("/org/ground-rules", response_model=s.GroundRules)
async def read_ground_rules(ctx: Member) -> s.GroundRules:
    """authz: member. No `tenant_settings` row reads as an empty list."""
    async with _tx(ctx) as session:
        topics = (
            await session.execute(
                text("SELECT never_record_topics FROM tenant_settings WHERE org_id = :o"),
                {"o": str(ctx.org_id)},
            )
        ).scalar_one_or_none()
    return s.GroundRules(never_record_topics=list(topics or []))


@router.put("/org/ground-rules", response_model=s.GroundRules)
async def put_ground_rules(body: s.GroundRulesUpdate, ctx: Admin) -> s.GroundRules:
    """authz: admin. Upsert, for the reason `routes/tenant_settings.py` gives: provisioning
    does not create a `tenant_settings` row."""
    async with _tx(ctx) as session:
        topics = (
            await session.execute(
                text(
                    "INSERT INTO tenant_settings (org_id, never_record_topics) VALUES (:o, :t) "
                    "ON CONFLICT (org_id) DO UPDATE SET "
                    "never_record_topics = EXCLUDED.never_record_topics, updated_at = now() "
                    "RETURNING never_record_topics"
                ),
                {"o": str(ctx.org_id), "t": body.never_record_topics},
            )
        ).scalar_one()
        await _audit(
            session,
            ctx,
            ACTION_GROUND_RULES_UPDATED,
            "tenant_settings",
            ctx.org_id,
            {"count": len(topics)},
        )
    await invalidate_org_context(ctx.org_id)
    return s.GroundRules(never_record_topics=list(topics))


# --- A6 topic watches --------------------------------------------------------------------------


def _me(ctx: RequestContext) -> uuid.UUID:
    # A verified caller with no local `users` row yet owns nothing, so it reads as 404, the same
    # answer `GET /users/me` gives.
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


@router.get("/users/me/topic-watches", response_model=s.TopicWatchList)
async def list_my_topic_watches(ctx: Member) -> s.TopicWatchList:
    """authz: member, self only. The caller's own watches."""
    me = _me(ctx)
    async with _tx(ctx) as session:
        rows = (
            (
                await session.execute(
                    text(
                        f"SELECT {_WATCH_COLS} FROM topic_watches WHERE user_id = :u "
                        "ORDER BY lower(topic), id"
                    ),
                    {"u": str(me)},
                )
            )
            .mappings()
            .all()
        )
    return s.TopicWatchList(items=[s.TopicWatch(**dict(r)) for r in rows])


@router.post(
    "/users/me/topic-watches", status_code=status.HTTP_201_CREATED, response_model=s.TopicWatch
)
async def create_my_topic_watch(body: s.TopicWatchCreate, ctx: Member) -> s.TopicWatch:
    """authz: member, self only. 409 when already watched (case-insensitively); 422 naming
    `topic` at 20 watches."""
    me = _me(ctx)
    async with _tx(ctx) as session:
        await _lock(session, f"topic_watches:{me}", ctx.org_id)
        params = {"u": str(me), "t": body.topic}
        taken = (
            await session.execute(
                text("SELECT 1 FROM topic_watches WHERE user_id = :u AND lower(topic) = lower(:t)"),
                params,
            )
        ).first()
        if taken is not None:
            raise _conflict("You already watch this topic")
        count = await _count(
            session, "SELECT count(*) FROM topic_watches WHERE user_id = :u", {"u": str(me)}
        )
        if count >= s.MAX_TOPIC_WATCHES:
            raise _invalid("topic", f"You can watch at most {s.MAX_TOPIC_WATCHES} topics")
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO topic_watches (org_id, user_id, topic) "
                        f"VALUES (:o, :u, :t) RETURNING {_WATCH_COLS}"
                    ),
                    {**params, "o": str(ctx.org_id)},
                )
            )
            .mappings()
            .one()
        )
    await invalidate_org_context(ctx.org_id)
    return s.TopicWatch(**dict(row))


@router.delete("/users/me/topic-watches/{watch_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_my_topic_watch(watch_id: uuid.UUID, ctx: Member) -> Response:
    """authz: member, self only. Someone else's watch is a 404, never a 403."""
    me = _me(ctx)
    async with _tx(ctx) as session:
        row = (
            await session.execute(
                text("DELETE FROM topic_watches WHERE id = :id AND user_id = :u RETURNING id"),
                {"id": str(watch_id), "u": str(me)},
            )
        ).first()
        if row is None:
            raise not_found()
    await invalidate_org_context(ctx.org_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- A7 readiness ------------------------------------------------------------------------------


def _plural(n: int, one: str, many: str) -> str:
    return f"{n} {one if n == 1 else many}"


@router.get("/org/readiness", response_model=s.Readiness)
async def readiness(ctx: Admin) -> s.Readiness:
    """authz: admin. Six checks, equal weights; `score` is the share done, 0 to 100."""
    async with _tx(ctx) as session:
        o = {"o": str(ctx.org_id)}
        description = (
            await session.execute(text("SELECT description FROM org_profiles WHERE org_id = :o"), o)
        ).scalar_one_or_none() or ""
        terms = await _count(session, "SELECT count(*) FROM org_glossary_terms")
        owners = await _count(session, "SELECT count(*) FROM topic_owners")
        active_goals = await _count(
            session, "SELECT count(*) FROM org_goals WHERE status = 'active'"
        )
        rules = (
            await session.execute(
                text("SELECT never_record_topics FROM tenant_settings WHERE org_id = :o"), o
            )
        ).scalar_one_or_none() or []
        no_role = await _count(
            session,
            "SELECT count(*) FROM users WHERE deleted_at IS NULL "
            "AND (role IS NULL OR btrim(role) = '')",
        )

    length = len(description.strip())
    checks = [
        s.ReadinessCheck(
            key="profile",
            label="Company profile",
            done=length >= PROFILE_MIN_DESCRIPTION,
            detail=(
                "Description written"
                if length >= PROFILE_MIN_DESCRIPTION
                else f"Description has {length} of {PROFILE_MIN_DESCRIPTION} characters"
            ),
        ),
        s.ReadinessCheck(
            key="vocabulary",
            label="Vocabulary",
            done=terms >= VOCABULARY_MIN_TERMS,
            detail=f"{_plural(terms, 'term', 'terms')} of {VOCABULARY_MIN_TERMS}",
        ),
        s.ReadinessCheck(
            key="owners",
            label="Who owns what",
            done=owners >= OWNERS_MIN,
            detail=f"{_plural(owners, 'owned topic', 'owned topics')} of {OWNERS_MIN}",
        ),
        s.ReadinessCheck(
            key="goals",
            label="Goals",
            done=active_goals >= 1,
            detail=_plural(active_goals, "active goal", "active goals"),
        ),
        s.ReadinessCheck(
            key="ground_rules",
            label="Ground rules",
            done=len(rules) >= 1,
            detail=_plural(len(rules), "never-record topic", "never-record topics"),
        ),
        s.ReadinessCheck(
            key="roles",
            label="Roles",
            done=no_role == 0,
            detail=(
                "Every member has a role"
                if no_role == 0
                else f"{_plural(no_role, 'member has', 'members have')} no role"
            ),
        ),
    ]
    score = round(100 * sum(c.done for c in checks) / len(checks))
    return s.Readiness(score=score, checks=checks)


# --- A8 welcome --------------------------------------------------------------------------------


@router.get("/org/welcome", response_model=s.Welcome)
async def welcome(ctx: Member) -> s.Welcome:
    """authz: member. The new joiner brief: profile, active goals, owners, the first 40 terms,
    and the caller's team mates (people sharing a team with them, themselves excluded).

    `teams` / `team_members` are core-api's; this only reads them, the same established pattern
    `context/service.py` uses for a user's teams.
    """
    async with _tx(ctx) as session:
        profile = await _read_profile(session, ctx.org_id)
        goals = await _goals(session, active_only=True)
        owners = await _owners(session)
        glossary = await _terms(session, limit=WELCOME_GLOSSARY_LIMIT)
        mates: list[Any] = []
        if ctx.user_id is not None:
            mates = list(
                (
                    await session.execute(
                        text(
                            "SELECT DISTINCT other.user_id FROM team_members mine "
                            "JOIN teams t ON t.id = mine.team_id AND t.deleted_at IS NULL "
                            "JOIN team_members other ON other.team_id = mine.team_id "
                            "JOIN users u ON u.id = other.user_id AND u.deleted_at IS NULL "
                            "WHERE mine.user_id = :me AND other.user_id <> :me "
                            "ORDER BY other.user_id"
                        ),
                        {"me": str(ctx.user_id)},
                    )
                )
                .scalars()
                .all()
            )
    return s.Welcome(
        profile=profile,
        goals=goals,
        owners=[s.WelcomeOwner(topic=o.topic, owner_user_id=o.owner_user_id) for o in owners],
        glossary=glossary,
        team=[s.TeamMate(user_id=m) for m in mates],
    )
