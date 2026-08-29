"""Teams and departments. Plan section 3.6 step 2.

`core-api` owns the CRUD for these even though they are organizational, because
`identity-tenant-service` owns the identity-and-tenancy surface and nothing else. One writer
per store: identity never writes these tables, and this service never writes `users`.

Creating either requires `admin`. Team and department structure is who-reports-to-whom, which
downstream bands use for agent scoping and worklog rollups, so it is not an ordinary member's
edit.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_TEAM_MEMBER_ADDED, ACTION_TEAM_MEMBER_REMOVED, audit
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import (
    Department,
    DepartmentCreate,
    Team,
    TeamCreate,
    TeamMember,
    TeamMemberUpsert,
)
from services.identity_tenant_service.context.service import invalidate_user_context

router = APIRouter(prefix="/api/v1", tags=["org-hierarchy"])


@router.get("/teams", response_model=list[Team])
async def list_teams(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[Team]:
    """Deliberately `/teams`, not the plan's `/orgs/{id}/teams`.

    The plan's own form puts a tenant identifier in the path, which violates the
    `org_id`-never-in-path invariant the same plan states two sections earlier. The tenant is
    already known from the token, so the path parameter could only ever agree with it or be
    an attempted cross-tenant read.
    """
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, name, department_id, created_at FROM teams "
                    "WHERE deleted_at IS NULL ORDER BY name"
                )
            )
        )
        .mappings()
        .all()
    )
    return [Team(**dict(r)) for r in rows]


@router.post("/teams", status_code=status.HTTP_201_CREATED, response_model=Team)
async def create_team(
    body: TeamCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Team:
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO teams (org_id, name, department_id) "
                    "VALUES (:org, :name, :dept) "
                    "RETURNING id, name, department_id, created_at"
                ),
                {
                    "org": str(ctx.org_id),
                    "name": body.name,
                    "dept": str(body.department_id) if body.department_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    return Team(**dict(row))


@router.get("/departments", response_model=list[Department])
async def list_departments(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[Department]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, name, parent_id, created_at FROM departments "
                    "WHERE deleted_at IS NULL ORDER BY name"
                )
            )
        )
        .mappings()
        .all()
    )
    return [Department(**dict(r)) for r in rows]


@router.post("/departments", status_code=status.HTTP_201_CREATED, response_model=Department)
async def create_department(
    body: DepartmentCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Department:
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO departments (org_id, name, parent_id) "
                    "VALUES (:org, :name, :parent) "
                    "RETURNING id, name, parent_id, created_at"
                ),
                {
                    "org": str(ctx.org_id),
                    "name": body.name,
                    # A parent in another tenant is invisible to RLS, so the FK cannot resolve
                    # and the insert fails. Cross-tenant nesting is unrepresentable.
                    "parent": str(body.parent_id) if body.parent_id else None,
                },
            )
        )
        .mappings()
        .one()
    )
    return Department(**dict(row))


# --- Team membership ------------------------------------------------------------------------
#
# `team_members` had no write route anywhere in the program until this point: the Plan 5-6
# seeder was its only writer, which is why that write was ruled an acceptable exception rather
# than a one-writer violation. The routes live here because `core-api` owns the table;
# `identity-tenant-service` reads it through `get_user_context` and must not become a second
# writer of it.
#
# No event is emitted. `_canon/03-event-taxonomy.md` defines no topic carrying team-membership
# changes, and the two candidates are both wrong: `tenant-events` is produced by
# `identity-tenant-service` alone, and inventing a topic here would put a name in the code that
# canon does not define. If a consumer ever needs these facts, the topic gets added to canon
# first and this comment is what should be revisited.


async def _visible_team_id(session: AsyncSession, team_id: uuid.UUID) -> None:
    """404 unless this tenant can see the team.

    RLS already makes another tenant's row invisible, so this SELECT returning nothing covers
    "no such team" and "not your team" identically - which is the point. Distinguishing them
    would confirm the existence of another tenant's team to anyone who can guess a UUID.
    """
    row = (
        await session.execute(
            text("SELECT 1 FROM teams WHERE id = :id AND deleted_at IS NULL"),
            {"id": str(team_id)},
        )
    ).first()
    if row is None:
        raise ProblemDetail(status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such team")


async def _visible_user_id(session: AsyncSession, user_id: uuid.UUID) -> None:
    """404 unless this tenant can see the user.

    THIS CHECK IS LOAD-BEARING FOR TENANT ISOLATION. It is not a nicety that turns a foreign
    key violation into a tidier status code, and it must not be "simplified" away on the
    grounds that `fk_team_members_user_id_users` already covers it. It does not.

    PostgreSQL performs referential-integrity checks as the referenced table's owner and with
    row-level security BYPASSED, so the foreign key happily resolves another tenant's `users`
    row even though this transaction cannot SELECT it. `team_members`' own WITH CHECK clause
    does not catch it either, because the `org_id` being written is this caller's own; the
    column that points across the boundary is `user_id`, which no policy constrains.

    Verified by deleting this call and re-running `test_a_user_from_another_tenant_cannot_be_added`:
    the insert returned 200 and wrote a row joining this tenant's team to another tenant's
    user. Removing it is a cross-tenant write, not a worse error message.
    """
    row = (
        await session.execute(text("SELECT 1 FROM users WHERE id = :id"), {"id": str(user_id)})
    ).first()
    if row is None:
        raise ProblemDetail(status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such user")


@router.get("/teams/{team_id}/members", response_model=list[TeamMember])
async def list_team_members(
    team_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[TeamMember]:
    """The roster. Readable by any member: who is on a team is not privileged inside a tenant."""
    await _visible_team_id(session, team_id)
    rows = (
        (
            await session.execute(
                text(
                    "SELECT team_id, user_id, role FROM team_members "
                    "WHERE team_id = :team ORDER BY user_id"
                ),
                {"team": str(team_id)},
            )
        )
        .mappings()
        .all()
    )
    return [TeamMember(**dict(r)) for r in rows]


@router.put("/teams/{team_id}/members/{user_id}", response_model=TeamMember)
async def add_team_member(
    team_id: uuid.UUID,
    user_id: uuid.UUID,
    body: TeamMemberUpsert,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> TeamMember:
    """PUT, not POST, because `(team_id, user_id)` is the primary key.

    Adding somebody already on the team is not an error, it is the same end state, and a POST
    would force a caller correcting a role to DELETE first. The upsert makes the request
    naturally idempotent, which is also why it needs no `Idempotency-Key` interaction.
    """
    await _visible_team_id(session, team_id)
    await _visible_user_id(session, user_id)

    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO team_members (org_id, team_id, user_id, role) "
                    "VALUES (:org, :team, :user, :role) "
                    "ON CONFLICT (team_id, user_id) DO UPDATE SET role = EXCLUDED.role "
                    "RETURNING team_id, user_id, role"
                ),
                {
                    "org": str(ctx.org_id),
                    "team": str(team_id),
                    "user": str(user_id),
                    "role": body.role,
                },
            )
        )
        .mappings()
        .one()
    )

    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_TEAM_MEMBER_ADDED,
        target_type="team",
        target_id=team_id,
        metadata={"user_id": str(user_id), "role": body.role},
        ip=ctx.ip,
    )

    await _invalidate(ctx, user_id)
    return TeamMember(**dict(row))


@router.delete("/teams/{team_id}/members/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_team_member(
    team_id: uuid.UUID,
    user_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """404 when the row is not there, rather than a silent 204.

    A no-op reported as success tells an admin the removal happened when the id was simply
    wrong, and membership is exactly the kind of change someone verifies by reading the
    response rather than re-reading the roster.
    """
    await _visible_team_id(session, team_id)

    deleted = (
        await session.execute(
            text(
                "DELETE FROM team_members WHERE team_id = :team AND user_id = :user "
                "RETURNING user_id"
            ),
            {"team": str(team_id), "user": str(user_id)},
        )
    ).first()
    if deleted is None:
        raise ProblemDetail(
            status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "That user is not on this team"
        )

    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_TEAM_MEMBER_REMOVED,
        target_type="team",
        target_id=team_id,
        metadata={"user_id": str(user_id)},
        ip=ctx.ip,
    )

    await _invalidate(ctx, user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _invalidate(ctx: RequestContext, user_id: uuid.UUID) -> None:
    """Evict the affected user's cached context. Plan 5-6 section 3.3.

    Cross-service on purpose, and the direction matters: `identity-tenant-service` owns the
    `nexus:{org_id}:user_ctx:{user_id}` entry and exports this function precisely so the writer
    of the underlying table can evict without either service writing the other's store. The
    same import shape already exists in the other direction (`bff` imports `get_user_context`).

    Note this evicts the MEMBER's key, never the acting admin's: it is the member's context
    whose `team` array just changed.

    Best-effort, per the function's own docstring: an eviction failure must not fail the write
    it is attached to, because the entry expires on its own at the TTL either way.
    """
    await invalidate_user_context(ctx.org_id, user_id)
