"""`/api/v1/agent-authz-scopes`: the A2A grant store's admin surface (ADR-0013; P10 contract 3).

Owner/admin only (`require_tier("admin")`, so a member is 403 like every other admin route in
this service, and the denial is audited). The tenant comes from the verified token, never the
path. A fresh tenant has no rows, so agent-to-agent collaboration is off until an admin grants
it; the UI's "Off" is "no grants", reached by revoking.

- `GET`: the live (non-deleted) grants.
- `POST`: upsert one grant. A soft-deleted row for the same tuple is re-activated with the new
  effect; an `allow` can be tightened to `deny` and back.
- `DELETE /{id}`: soft delete. Another tenant's id, an unknown id, and an already-revoked id are
  all 404.
- `POST /template`: adopt the platform default at a scope: `allow` for every role x every query
  class at that scope and every narrower one (self <= same_team <= department <= org). Rows that
  already exist are left exactly as they are, so an explicit `deny` an admin set survives the
  template (explicit deny wins here too); revoked rows in range are re-activated as `allow`.

Every change writes one `a2a.grant.create` / `a2a.grant.revoke` audit row per row changed, in the
same transaction, and then, after the commit, deletes the PEP's cached decisions
(`nexus:{org_id}:a2a:grant:*`). The commit comes first on purpose: invalidating before it would
let a PEP miss re-read the old grant and cache it again. A Redis failure after the commit is
logged, not raised: the grant is already in effect for every uncached decision and the 60 s TTL
bounds a stale cached one.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_A2A_GRANT_CREATE, ACTION_A2A_GRANT_REVOKE, audit
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.cache import get_redis
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.a2a.pdp import (
    GRANTEE_ROLES,
    QUERY_CLASSES,
    SCOPE_ORDER,
    invalidate_decision_cache,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["agent-authz"])

GranteeRole = Literal["owner", "admin", "member"]
QueryClass = Literal["meeting_context", "decision_lineage", "action_status"]
TargetScope = Literal["self", "same_team", "department", "org"]
Effect = Literal["allow", "deny"]

_COLS = "id, grantee_role, query_class, target_scope, effect, created_by, created_at"


class AgentAuthzScope(BaseModel):
    id: uuid.UUID
    grantee_role: GranteeRole
    query_class: QueryClass
    target_scope: TargetScope
    effect: Effect
    created_by: uuid.UUID
    created_at: datetime


class AgentAuthzScopeList(BaseModel):
    items: list[AgentAuthzScope]


class AgentAuthzScopeCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grantee_role: GranteeRole
    query_class: QueryClass
    target_scope: TargetScope
    effect: Effect = "allow"


class AgentAuthzTemplate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_scope: Literal["same_team", "department", "org"]


class AgentAuthzTemplateResult(BaseModel):
    created: int
    items: list[AgentAuthzScope]


async def _invalidate(org_id: uuid.UUID) -> None:
    try:
        await invalidate_decision_cache(get_redis(), org_id)
    except (RedisError, OSError):
        logger.warning(
            "a2a grant changed for org %s but the decision cache could not be cleared; "
            "cached decisions expire within 60 s",
            org_id,
            exc_info=True,
        )


def _actor(ctx: RequestContext) -> uuid.UUID:
    # `require_tier` passing means the caller has a users row (the tier is joined from it), so
    # this is defensive: a grant must name who made it (`created_by NOT NULL`).
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this caller")
    return ctx.user_id


async def _live_rows(session: AsyncSession) -> list[AgentAuthzScope]:
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLS} FROM agent_authz_scopes WHERE deleted_at IS NULL "
                    "ORDER BY target_scope, grantee_role, query_class"
                )
            )
        )
        .mappings()
        .all()
    )
    return [AgentAuthzScope(**dict(r)) for r in rows]


async def _audit_change(
    session: AsyncSession, ctx: RequestContext, action: str, row: Any, **extra: Any
) -> None:
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=action,
        target_type="agent_authz_scope",
        target_id=row["id"],
        metadata={
            "grantee_role": row["grantee_role"],
            "query_class": row["query_class"],
            "target_scope": row["target_scope"],
            "effect": row["effect"],
            **extra,
        },
        ip=ctx.ip,
    )


@router.get("/agent-authz-scopes", response_model=AgentAuthzScopeList)
async def list_scopes(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> AgentAuthzScopeList:
    """The caller's tenant's live grants. Empty means agent-to-agent collaboration is off."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        return AgentAuthzScopeList(items=await _live_rows(session))


@router.post("/agent-authz-scopes", response_model=AgentAuthzScope)
async def upsert_scope(
    body: AgentAuthzScopeCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> AgentAuthzScope:
    """Create or change one grant. Idempotent in the value written; audits only a change."""
    actor = _actor(ctx)
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        before = (
            (
                await session.execute(
                    text(
                        "SELECT effect, deleted_at FROM agent_authz_scopes WHERE "
                        "grantee_role = :r AND query_class = :q AND target_scope = :s FOR UPDATE"
                    ),
                    {"r": body.grantee_role, "q": body.query_class, "s": body.target_scope},
                )
            )
            .mappings()
            .first()
        )
        row = (
            (
                await session.execute(
                    text(
                        "INSERT INTO agent_authz_scopes "
                        "(org_id, grantee_role, query_class, target_scope, effect, created_by) "
                        "VALUES (:o, :r, :q, :s, :e, :by) "
                        "ON CONFLICT (org_id, grantee_role, query_class, target_scope) "
                        "DO UPDATE SET effect = EXCLUDED.effect, deleted_at = NULL, "
                        "created_by = CASE WHEN agent_authz_scopes.deleted_at IS NULL "
                        "AND agent_authz_scopes.effect = EXCLUDED.effect "
                        "THEN agent_authz_scopes.created_by ELSE EXCLUDED.created_by END, "
                        "created_at = CASE WHEN agent_authz_scopes.deleted_at IS NULL "
                        "AND agent_authz_scopes.effect = EXCLUDED.effect "
                        "THEN agent_authz_scopes.created_at ELSE now() END "
                        f"RETURNING {_COLS}"
                    ),
                    {
                        "o": str(ctx.org_id),
                        "r": body.grantee_role,
                        "q": body.query_class,
                        "s": body.target_scope,
                        "e": body.effect,
                        "by": str(actor),
                    },
                )
            )
            .mappings()
            .one()
        )
        changed = before is None or before["deleted_at"] is not None
        changed = changed or (before is not None and before["effect"] != body.effect)
        if changed:
            await _audit_change(
                session,
                ctx,
                ACTION_A2A_GRANT_CREATE,
                row,
                previous=(
                    None if before is None or before["deleted_at"] is not None else before["effect"]
                ),
            )
    if changed:
        await _invalidate(ctx.org_id)
    return AgentAuthzScope(**dict(row))


@router.delete("/agent-authz-scopes/{scope_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_scope(
    scope_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> Response:
    """Soft-delete one grant. 404 when it is not a live grant of the caller's tenant."""
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        row = (
            (
                await session.execute(
                    text(
                        "UPDATE agent_authz_scopes SET deleted_at = now() "
                        "WHERE id = :id AND deleted_at IS NULL "
                        "RETURNING id, grantee_role, query_class, target_scope, effect"
                    ),
                    {"id": str(scope_id)},
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise not_found()
        await _audit_change(session, ctx, ACTION_A2A_GRANT_REVOKE, row)
    await _invalidate(ctx.org_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/agent-authz-scopes/template", response_model=AgentAuthzTemplateResult)
async def adopt_template(
    body: AgentAuthzTemplate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
) -> AgentAuthzTemplateResult:
    """Adopt the platform default grant set at `target_scope` (and every narrower scope)."""
    actor = _actor(ctx)
    scopes = SCOPE_ORDER[: SCOPE_ORDER.index(body.target_scope) + 1]
    created = 0
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        for scope in scopes:
            for role in GRANTEE_ROLES:
                for qclass in QUERY_CLASSES:
                    # New tuple -> insert; revoked tuple -> re-activate as allow; live tuple
                    # (allow OR deny) -> untouched, so the WHERE makes the DO UPDATE a no-op
                    # and RETURNING yields nothing.
                    row = (
                        (
                            await session.execute(
                                text(
                                    "INSERT INTO agent_authz_scopes (org_id, grantee_role, "
                                    "query_class, target_scope, effect, created_by) "
                                    "VALUES (:o, :r, :q, :s, 'allow', :by) "
                                    "ON CONFLICT (org_id, grantee_role, query_class, "
                                    "target_scope) DO UPDATE SET effect = 'allow', "
                                    "deleted_at = NULL, created_by = EXCLUDED.created_by, "
                                    "created_at = now() "
                                    "WHERE agent_authz_scopes.deleted_at IS NOT NULL "
                                    f"RETURNING {_COLS}"
                                ),
                                {
                                    "o": str(ctx.org_id),
                                    "r": role,
                                    "q": qclass,
                                    "s": scope,
                                    "by": str(actor),
                                },
                            )
                        )
                        .mappings()
                        .first()
                    )
                    if row is not None:
                        created += 1
                        await _audit_change(
                            session,
                            ctx,
                            ACTION_A2A_GRANT_CREATE,
                            row,
                            template=body.target_scope,
                        )
        items = await _live_rows(session)
    if created:
        await _invalidate(ctx.org_id)
    return AgentAuthzTemplateResult(created=created, items=items)
