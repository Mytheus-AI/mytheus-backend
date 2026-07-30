"""Teams and departments. Plan section 3.6 step 2.

`core-api` owns the CRUD for these even though they are organizational, because
`identity-tenant-service` owns the identity-and-tenancy surface and nothing else. One writer
per store: identity never writes these tables, and this service never writes `users`.

Creating either requires `admin`. Team and department structure is who-reports-to-whom, which
downstream bands use for agent scoping and worklog rollups, so it is not an ordinary member's
edit.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from services.core_api.schemas import Department, DepartmentCreate, Team, TeamCreate

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
