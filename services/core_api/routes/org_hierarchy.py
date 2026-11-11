"""Teams and departments. Plan section 3.6 step 2.

`core-api` owns the CRUD for these even though they are organizational, because
`identity-tenant-service` owns the identity-and-tenancy surface and nothing else. One writer
per store: identity never writes these tables, and this service never writes `users`.

Creating either requires `admin`. Team and department structure is who-reports-to-whom, which
downstream bands use for agent scoping and worklog rollups, so it is not an ordinary member's
edit.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterable
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_DEPARTMENT_CREATED,
    ACTION_DEPARTMENT_DELETED,
    ACTION_DEPARTMENT_UPDATED,
    ACTION_TEAM_CREATED,
    ACTION_TEAM_DELETED,
    ACTION_TEAM_MEMBER_ADDED,
    ACTION_TEAM_MEMBER_MOVED,
    ACTION_TEAM_MEMBER_REMOVED,
    ACTION_TEAM_UPDATED,
    audit,
)
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.schemas import (
    Department,
    DepartmentCreate,
    DepartmentUpdate,
    Team,
    TeamCreate,
    TeamMember,
    TeamMemberMove,
    TeamMemberUpsert,
    TeamUpdate,
)
from services.core_api.visibility import is_visible
from services.identity_tenant_service.brain.context import invalidate_org_context
from services.identity_tenant_service.context.service import invalidate_user_context

router = APIRouter(prefix="/api/v1", tags=["org-hierarchy"])

_TEAM_COLS = "id, name, department_id, created_at, description"
_DEPT_COLS = "id, name, parent_id, created_at, description"


def _description(raw: str | None) -> str | None:
    """Trimmed; blank is "no description" (NULL), so a cleared field reads as unset."""
    if raw is None:
        return None
    cleaned = raw.strip()
    return cleaned or None


# --- cache eviction, after commit -------------------------------------------------------------
#
# The structure feeds two identity-owned caches: each member's `user_ctx` (their team names and
# descriptions) and the org's `org_ctx` (the company brain's departments and teams, which every
# agent reads). Both are evicted twice: once inside the transaction, as the membership routes
# always did, and again AFTER the commit, so a concurrent reader that re-cached the pre-commit
# state between the two is corrected (`brain/routes.py` gives the same reason for evicting after
# its own commits). The second pass hangs off SQLAlchemy's `after_commit` event because the
# request transaction belongs to `tenant_session` and commits after this module returns;
# FastAPI's BackgroundTasks run before that commit (see `routes/clips.py`).

_EVICTIONS: set[asyncio.Task[None]] = set()
_PENDING = "org_structure_evict"


async def _evict(org_id: uuid.UUID, user_ids: Iterable[uuid.UUID]) -> None:
    await invalidate_org_context(org_id)
    for uid in set(user_ids):
        await invalidate_user_context(org_id, uid)


def _evict_after_commit(
    session: AsyncSession, org_id: uuid.UUID, user_ids: Iterable[uuid.UUID]
) -> None:
    pending: set[uuid.UUID] | None = session.info.get(_PENDING)
    if pending is not None:
        pending.update(user_ids)
        return
    session.info[_PENDING] = set(user_ids)

    def _run(_sync_session: Any) -> None:
        users = session.info.pop(_PENDING, set())
        try:
            task = asyncio.get_running_loop().create_task(_evict(org_id, users))
        except RuntimeError:  # no loop (should not happen on the request path): TTL heals it
            return
        _EVICTIONS.add(task)
        task.add_done_callback(_EVICTIONS.discard)

    event.listen(session.sync_session, "after_commit", _run, once=True)


async def _evict_now_and_after_commit(
    session: AsyncSession, org_id: uuid.UUID, user_ids: Iterable[uuid.UUID]
) -> None:
    users = list(user_ids)
    await _evict(org_id, users)
    _evict_after_commit(session, org_id, users)


async def _members_of_teams(session: AsyncSession, team_ids: list[str]) -> list[uuid.UUID]:
    if not team_ids:
        return []
    rows = await session.execute(
        text("SELECT DISTINCT user_id FROM team_members WHERE team_id = ANY(CAST(:ids AS uuid[]))"),
        {"ids": team_ids},
    )
    return [uuid.UUID(str(r[0])) for r in rows]


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
                    "SELECT id, name, department_id, created_at, description FROM teams "
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
    # Gap 31: the FK to departments resolves another tenant's row (RI bypasses RLS), so
    # without this a team could be filed under another tenant's department.
    if body.department_id is not None and not await is_visible(
        session, "departments", body.department_id
    ):
        raise _no_such_department()
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO teams (org_id, name, department_id, description) "
                    "VALUES (:org, :name, :dept, :description) "
                    f"RETURNING {_TEAM_COLS}"
                ),
                {
                    "org": str(ctx.org_id),
                    "name": body.name.strip() or body.name,
                    "dept": str(body.department_id) if body.department_id else None,
                    "description": _description(body.description),
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
        action=ACTION_TEAM_CREATED,
        target_type="team",
        target_id=row["id"],
        metadata={"department_id": str(body.department_id) if body.department_id else None},
        ip=ctx.ip,
    )
    await _evict_now_and_after_commit(session, ctx.org_id, [])
    return Team(**dict(row))


def _no_such_department() -> ProblemDetail:
    # 404, not 422, matching the participant and team-member checks: whether an id exists at
    # all is tenant-confidential.
    return ProblemDetail(status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such department")


@router.get("/departments", response_model=list[Department])
async def list_departments(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> list[Department]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, name, parent_id, created_at, description FROM departments "
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
    # Gap 31. The comment that used to sit on `parent` below claimed "a parent in another
    # tenant is invisible to RLS, so the FK cannot resolve". False: RI checks bypass RLS, so
    # the FK resolves it and the insert succeeded, nesting this tenant's department under
    # another tenant's. The explicit visible-parent check is the enforcement.
    if body.parent_id is not None and not await is_visible(session, "departments", body.parent_id):
        raise _no_such_department()
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO departments (org_id, name, parent_id, description) "
                    "VALUES (:org, :name, :parent, :description) "
                    f"RETURNING {_DEPT_COLS}"
                ),
                {
                    "org": str(ctx.org_id),
                    "name": body.name.strip() or body.name,
                    "parent": str(body.parent_id) if body.parent_id else None,
                    "description": _description(body.description),
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
        action=ACTION_DEPARTMENT_CREATED,
        target_type="department",
        target_id=row["id"],
        ip=ctx.ip,
    )
    await _evict_now_and_after_commit(session, ctx.org_id, [])
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
    if not await is_visible(session, "teams", team_id):
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
    if not await is_visible(session, "users", user_id):
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

    await _invalidate(session, ctx, user_id)
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

    await _invalidate(session, ctx, user_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


async def _invalidate(session: AsyncSession, ctx: RequestContext, user_id: uuid.UUID) -> None:
    """Evict the affected user's cached context, and the org's company brain. Plan 5-6 section
    3.3, People structure.

    Cross-service on purpose, and the direction matters: `identity-tenant-service` owns the
    `nexus:{org_id}:user_ctx:{user_id}` and `nexus:{org_id}:org_ctx` entries and exports the
    eviction functions precisely so the writer of the underlying table can evict without either
    service writing the other's store.

    Note this evicts the MEMBER's key, never the acting admin's: it is the member's context
    whose `team` array just changed. The org context lists every team's members, so it goes too.

    Best-effort: an eviction failure must not fail the write it is attached to, because the
    entry expires on its own at the TTL either way.
    """
    await _evict_now_and_after_commit(session, ctx.org_id, [user_id])


# --- Editing the structure (People) ---------------------------------------------------------
#
# Admin-only like the creates. Reads stay member-level. Every id is checked for visibility
# first: RLS hides another tenant's rows, so "not yours" and "does not exist" are the same 404.


async def _visible_department_id(session: AsyncSession, department_id: uuid.UUID) -> None:
    if not await is_visible(session, "departments", department_id):
        raise _no_such_department()


@router.patch("/departments/{department_id}", response_model=Department)
async def update_department(
    department_id: uuid.UUID,
    body: DepartmentUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Department:
    """Rename, describe or re-parent a department. Only the fields sent change.

    A parent must be a live department of this tenant and must not be the department itself or
    one of its descendants (that would detach a loop of departments from every root)."""
    await _visible_department_id(session, department_id)
    sent = body.model_fields_set
    sets: list[str] = []
    params: dict[str, Any] = {"id": str(department_id)}
    if "name" in sent:
        if body.name is None or not body.name.strip():
            raise _invalid("name", "A department needs a name.")
        sets.append("name = :name")
        params["name"] = body.name.strip()
    if "description" in sent:
        sets.append("description = :description")
        params["description"] = _description(body.description)
    if "parent_id" in sent:
        if body.parent_id is not None:
            await _visible_department_id(session, body.parent_id)
            if await _is_self_or_descendant(session, department_id, body.parent_id):
                raise _invalid("parent_id", "A department cannot sit under itself.")
        sets.append("parent_id = :parent")
        params["parent"] = str(body.parent_id) if body.parent_id else None
    if sets:
        sets.append("updated_at = now()")
        await session.execute(
            text(f"UPDATE departments SET {', '.join(sets)} WHERE id = :id"), params
        )
    row = (
        (
            await session.execute(
                text(f"SELECT {_DEPT_COLS} FROM departments WHERE id = :id"), {"id": params["id"]}
            )
        )
        .mappings()
        .one()
    )
    if sets:
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_DEPARTMENT_UPDATED,
            target_type="department",
            target_id=department_id,
            metadata={"fields": sorted(sent)},
            ip=ctx.ip,
        )
        members = await _members_of_department(session, department_id)
        await _evict_now_and_after_commit(session, ctx.org_id, members)
    return Department(**dict(row))


@router.delete("/departments/{department_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_department(
    department_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """Delete a department. Its teams are NEVER deleted: they move to "No department"
    (`department_id = NULL`), people and all. Departments nested under it move up one level, to
    its own parent. The row is soft-deleted (`deleted_at`), like every org row."""
    await _visible_department_id(session, department_id)
    members = await _members_of_department(session, department_id)
    parent = (
        await session.execute(
            text(
                "UPDATE departments SET deleted_at = now(), updated_at = now() "
                "WHERE id = :id AND deleted_at IS NULL RETURNING parent_id"
            ),
            {"id": str(department_id)},
        )
    ).first()
    if parent is None:
        raise _no_such_department()
    moved = (
        await session.execute(
            text(
                "UPDATE teams SET department_id = NULL, updated_at = now() "
                "WHERE department_id = :id AND deleted_at IS NULL RETURNING id"
            ),
            {"id": str(department_id)},
        )
    ).all()
    await session.execute(
        text(
            "UPDATE departments SET parent_id = :parent, updated_at = now() "
            "WHERE parent_id = :id AND deleted_at IS NULL"
        ),
        {"id": str(department_id), "parent": str(parent[0]) if parent[0] else None},
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_DEPARTMENT_DELETED,
        target_type="department",
        target_id=department_id,
        metadata={"teams_moved_to_no_department": [str(r[0]) for r in moved]},
        ip=ctx.ip,
    )
    await _evict_now_and_after_commit(session, ctx.org_id, members)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.patch("/teams/{team_id}", response_model=Team)
async def update_team(
    team_id: uuid.UUID,
    body: TeamUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Team:
    """Rename, describe or move a team to another department (`department_id`, or null for "No
    department"). Only the fields sent change."""
    await _visible_team_id(session, team_id)
    sent = body.model_fields_set
    sets: list[str] = []
    params: dict[str, Any] = {"id": str(team_id)}
    if "name" in sent:
        if body.name is None or not body.name.strip():
            raise _invalid("name", "A team needs a name.")
        sets.append("name = :name")
        params["name"] = body.name.strip()
    if "description" in sent:
        sets.append("description = :description")
        params["description"] = _description(body.description)
    if "department_id" in sent:
        if body.department_id is not None:
            await _visible_department_id(session, body.department_id)
        sets.append("department_id = :dept")
        params["dept"] = str(body.department_id) if body.department_id else None
    if sets:
        sets.append("updated_at = now()")
        await session.execute(text(f"UPDATE teams SET {', '.join(sets)} WHERE id = :id"), params)
    row = (
        (
            await session.execute(
                text(f"SELECT {_TEAM_COLS} FROM teams WHERE id = :id"), {"id": params["id"]}
            )
        )
        .mappings()
        .one()
    )
    if sets:
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_TEAM_UPDATED,
            target_type="team",
            target_id=team_id,
            metadata={"fields": sorted(sent)},
            ip=ctx.ip,
        )
        members = await _members_of_teams(session, [str(team_id)])
        await _evict_now_and_after_commit(session, ctx.org_id, members)
    return Team(**dict(row))


@router.delete("/teams/{team_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_team(
    team_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """Delete a team: the team is soft-deleted and its memberships are removed (the people stay
    in the workspace, on whatever other teams they are on). The UI confirms first and says how
    many people are on it; the API itself is a plain DELETE so a script can do the same."""
    await _visible_team_id(session, team_id)
    members = await _members_of_teams(session, [str(team_id)])
    deleted = (
        await session.execute(
            text(
                "UPDATE teams SET deleted_at = now(), updated_at = now() "
                "WHERE id = :id AND deleted_at IS NULL RETURNING id"
            ),
            {"id": str(team_id)},
        )
    ).first()
    if deleted is None:
        raise ProblemDetail(status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "No such team")
    await session.execute(
        text("DELETE FROM team_members WHERE team_id = :id"), {"id": str(team_id)}
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_TEAM_DELETED,
        target_type="team",
        target_id=team_id,
        metadata={"members_removed": [str(u) for u in members]},
        ip=ctx.ip,
    )
    await _evict_now_and_after_commit(session, ctx.org_id, members)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/teams/{team_id}/members/{user_id}/move", response_model=TeamMember)
async def move_team_member(
    team_id: uuid.UUID,
    user_id: uuid.UUID,
    body: TeamMemberMove,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> TeamMember:
    """Move a person from this team to `to_team_id` in one transaction: never on both, never on
    neither. Their label (`role`) comes along unless the body sets a new one. Moving onto a team
    they are already on just updates that membership and leaves this one."""
    await _visible_team_id(session, team_id)
    await _visible_team_id(session, body.to_team_id)
    if body.to_team_id == team_id:
        raise _invalid("to_team_id", "Pick a different team to move them to.")
    old = (
        await session.execute(
            text(
                "DELETE FROM team_members WHERE team_id = :team AND user_id = :user "
                "RETURNING role"
            ),
            {"team": str(team_id), "user": str(user_id)},
        )
    ).first()
    if old is None:
        raise ProblemDetail(
            status.HTTP_404_NOT_FOUND, ErrorCode.NOT_FOUND, "That user is not on this team"
        )
    role = body.role if body.role is not None else old[0]
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
                    "team": str(body.to_team_id),
                    "user": str(user_id),
                    "role": role,
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
        action=ACTION_TEAM_MEMBER_MOVED,
        target_type="team",
        target_id=body.to_team_id,
        metadata={"user_id": str(user_id), "from_team_id": str(team_id), "role": role},
        ip=ctx.ip,
    )
    await _invalidate(session, ctx, user_id)
    return TeamMember(**dict(row))


def _invalid(field: str, message: str) -> ProblemDetail:
    """A 422 shaped like a body-validation failure, so a client reads one `errors[].loc`."""
    return ProblemDetail(
        422,
        ErrorCode.VALIDATION_FAILED,
        message,
        extra={"errors": [{"loc": ["body", field], "msg": message, "type": "value_error"}]},
    )


async def _is_self_or_descendant(
    session: AsyncSession, department_id: uuid.UUID, candidate: uuid.UUID
) -> bool:
    """True when `candidate` is `department_id` or sits anywhere below it."""
    row = (
        await session.execute(
            text(
                "WITH RECURSIVE below(id) AS ("
                "  SELECT CAST(:root AS uuid) "
                "  UNION SELECT d.id FROM departments d JOIN below b ON d.parent_id = b.id "
                "  WHERE d.deleted_at IS NULL"
                ") SELECT 1 FROM below WHERE id = :candidate"
            ),
            {"root": str(department_id), "candidate": str(candidate)},
        )
    ).first()
    return row is not None


async def _members_of_department(
    session: AsyncSession, department_id: uuid.UUID
) -> list[uuid.UUID]:
    teams = await session.execute(
        text("SELECT id FROM teams WHERE department_id = :id AND deleted_at IS NULL"),
        {"id": str(department_id)},
    )
    return await _members_of_teams(session, [str(r[0]) for r in teams])
