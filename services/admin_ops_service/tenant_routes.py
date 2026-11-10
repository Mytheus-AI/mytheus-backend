"""`/admin/tenant/*`: the tenant-admin console API (P12, plan 3.13). `owner`/`admin` only.

Every route is `require_tier("admin")` (which admits `owner`); a lower tier is a 403 that
`require_tier` audits. Every write lands in `audit_logs`:

- members (tier change, remove) and settings are forwarded to identity-tenant-service with the
  caller's token (`common.forward_to_identity`), which applies its own RBAC rules and writes its
  own audit rows (`rbac.granted` / `rbac.revoked` / `tenant_settings.updated`). "Remove" revokes
  the member's grant: they stay a user of the org with no tier (Clerk membership is Clerk's).
- support grants and data-export requests are this service's own writes, audited here in the
  same transaction.

Data export: `POST /admin/tenant/data-export` records the request as an `audit_logs` row
(`data_export.requested`) and answers 202 `pending`. It emits NO `admin-events` record:
`AdminEventType` has no export symbol, and its `erasure_requested` is what the RTBF saga treats
as "delete this tenant", so reusing it for an export would be dangerous. The export workflow
does not exist yet, so nothing completes the request (no manifest in `mytheus-exports`);
`GET /admin/tenant/data-export` lists the recorded requests from `audit_logs`.

Data erasure: `POST /admin/tenant/erasures` erases one person's or one meeting's data. This
service owns no row of it, so it orchestrates the owners, each as the CALLER (their bearer
token), in order:

1. core-api `POST /api/v1/erasures` erases core-api's tables in one transaction and checks the
   typed confirmation (the person's email or the meeting's title) against its own records;
2. for a person, identity revokes each of their active personal access tokens
   (`GET /api/v1/api-tokens?owner=all`, then `DELETE /api/v1/api-tokens/{id}`; identity audits
   each revoke);
3. here, in one transaction: an `audit_logs` row `data_erasure.completed` (counts, never the
   erased content and never the person's email) and an `admin-events` `erasure_requested`
   outbox row with `target_type` `person` or `meeting` (never a tenant), so the stores this repo
   does not own (vectors, the graph, `transcript_chunks`, agent memory) can purge their share.

Every leg is idempotent, so a failure part-way (a 503 from an upstream) is fixed by asking
again: what was already erased matches nothing the second time and nothing is recorded as
completed until every leg has run. `GET /admin/tenant/erasures` lists the completed requests.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, Request, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_DATA_ERASURE_COMPLETED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.admin_ops_service.common import (
    CoreTransport,
    IdentityTransport,
    emit_admin_event,
    forward_to_core,
    forward_to_identity,
    staff_user_ids,
)
from services.admin_ops_service.schemas import (
    DataExport,
    DataExportList,
    Erasure,
    ErasureCreate,
    ErasureList,
    Member,
    MemberList,
    MemberTierUpdate,
    SupportGrant,
    SupportGrantCreate,
    SupportGrantList,
)
from services.identity_tenant_service.routes.tenant_settings import (
    TenantSettings,
    TenantSettingsUpdate,
)

router = APIRouter(prefix="/admin/tenant", tags=["admin-tenant"])

ACTION_SUPPORT_GRANT_CREATED = "support_grant.created"
ACTION_SUPPORT_GRANT_REVOKED = "support_grant.revoked"
ACTION_DATA_EXPORT_REQUESTED = "data_export.requested"
MAX_SUPPORT_WINDOW = timedelta(days=30)
_MEMBER_LIMIT = 500

_MEMBERS = text(
    "SELECT u.id AS user_id, u.email, g.tier, u.created_at FROM users u "
    "LEFT JOIN rbac_grants g ON g.user_id = u.id "
    "WHERE u.deleted_at IS NULL {extra} ORDER BY lower(u.email), u.id LIMIT :n"
)
_GRANT_COLUMNS = "id, staff_user_id, scope, granted_by, expires_at, revoked_at, created_at"


async def _members(session: AsyncSession, user_id: uuid.UUID | None = None) -> list[Member]:
    sql = _MEMBERS.text.format(extra="AND u.id = :u" if user_id else "")
    params: dict[str, Any] = {"n": _MEMBER_LIMIT}
    if user_id:
        params["u"] = str(user_id)
    rows = (await session.execute(text(sql), params)).mappings().all()
    return [Member(**dict(r)) for r in rows]


@router.get("/members", response_model=MemberList)
async def list_members(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> MemberList:
    """authz: admin. Everyone in the caller's tenant with their tier (RLS scopes both tables)."""
    return MemberList(items=await _members(session))


@router.patch("/members/{user_id}", response_model=Member)
async def set_member_tier(
    user_id: uuid.UUID,
    body: MemberTierUpdate,
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    transport: IdentityTransport,
) -> Member:
    """authz: admin. Identity's `PUT /rbac/grants/{user_id}` as the caller: identity's rules and
    audit row apply; its 403/404/409 come back unchanged."""
    await forward_to_identity(
        request, "PUT", f"/rbac/grants/{user_id}", transport, {"tier": body.tier}
    )
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        found = await _members(session, user_id)
    if not found:
        raise not_found()
    return found[0]


@router.delete(
    "/members/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def remove_member(
    user_id: uuid.UUID,
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    transport: IdentityTransport,
) -> Response:
    """authz: admin. Revokes the member's grant through identity (`DELETE /rbac/grants/...`)."""
    await forward_to_identity(request, "DELETE", f"/rbac/grants/{user_id}", transport)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/settings", response_model=TenantSettings)
async def get_settings(
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    transport: IdentityTransport,
) -> TenantSettings:
    """authz: admin. Identity's `GET /tenant-settings`."""
    response = await forward_to_identity(request, "GET", "/tenant-settings", transport)
    return TenantSettings.model_validate(response.json())


@router.patch("/settings", response_model=TenantSettings)
async def update_settings(
    body: TenantSettingsUpdate,
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    transport: IdentityTransport,
) -> TenantSettings:
    """authz: admin. Identity's `PATCH /tenant-settings` (the single writer; it audits)."""
    response = await forward_to_identity(
        request,
        "PATCH",
        "/tenant-settings",
        transport,
        body.model_dump(mode="json", exclude_unset=True),
    )
    return TenantSettings.model_validate(response.json())


@router.get(
    "/support-grants",
    response_model=SupportGrantList,
)
async def list_support_grants(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> SupportGrantList:
    """authz: admin. The tenant's grants, newest first (revoked and expired included)."""
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_GRANT_COLUMNS} FROM support_grants "
                    "ORDER BY created_at DESC, id LIMIT 200"
                )
            )
        )
        .mappings()
        .all()
    )
    return SupportGrantList(items=[SupportGrant(**dict(r)) for r in rows])


@router.post(
    "/support-grants",
    status_code=status.HTTP_201_CREATED,
    response_model=SupportGrant,
)
async def create_support_grant(
    body: SupportGrantCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> SupportGrant:
    """authz: admin. A time-boxed grant to one Mytheus staff user (at most 30 days)."""
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this caller")
    if body.staff_user_id not in staff_user_ids():
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Not a Mytheus staff user")
    expires = body.expires_at if body.expires_at.tzinfo else body.expires_at.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    if expires <= now or expires - now > MAX_SUPPORT_WINDOW:
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "expires_at must be within the next 30 days"
        )
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO support_grants (org_id, granted_by, staff_user_id, scope, "
                    "expires_at) VALUES (:o, :by, :s, :scope, :exp) "
                    f"RETURNING {_GRANT_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "by": str(ctx.user_id),
                    "s": str(body.staff_user_id),
                    "scope": body.scope,
                    "exp": expires,
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
        action=ACTION_SUPPORT_GRANT_CREATED,
        target_type="support_grant",
        target_id=row["id"],
        metadata={
            "staff_user_id": str(body.staff_user_id),
            "scope": body.scope,
            "expires_at": expires.isoformat(),
        },
        ip=ctx.ip,
    )
    return SupportGrant(**dict(row))


@router.delete(
    "/support-grants/{grant_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def revoke_support_grant(
    grant_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """authz: admin. Revoke now. Unknown (or another tenant's) id is 404; an already revoked
    grant is a 204 no-op with no second audit row."""
    row = (
        await session.execute(
            text("SELECT revoked_at FROM support_grants WHERE id = :g FOR UPDATE"),
            {"g": str(grant_id)},
        )
    ).first()
    if row is None:
        raise not_found()
    if row[0] is None:
        await session.execute(
            text("UPDATE support_grants SET revoked_at = now() WHERE id = :g"),
            {"g": str(grant_id)},
        )
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_SUPPORT_GRANT_REVOKED,
            target_type="support_grant",
            target_id=grant_id,
            ip=ctx.ip,
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/data-export",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=DataExport,
)
async def request_data_export(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DataExport:
    """authz: admin. Records the request in `audit_logs` and answers 202 `pending`; no
    `admin-events` record (see the module docstring)."""
    export_id = uuid.uuid4()
    requested_at = datetime.now(UTC)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_DATA_EXPORT_REQUESTED,
        target_type="data_export",
        target_id=export_id,
        metadata={"kind": "export", "status": "pending"},
        ip=ctx.ip,
    )
    return DataExport(id=export_id, status="pending", requested_at=requested_at)


@router.get("/data-export", response_model=DataExportList)
async def list_data_exports(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> DataExportList:
    """authz: admin. The tenant's export requests, newest first."""
    rows = (
        await session.execute(
            text(
                "SELECT target_id, occurred_at, metadata FROM audit_logs "
                "WHERE action = :a ORDER BY occurred_at DESC LIMIT 50"
            ),
            {"a": ACTION_DATA_EXPORT_REQUESTED},
        )
    ).all()
    items = []
    for target_id, occurred_at, metadata in rows:
        meta = metadata if isinstance(metadata, dict) else json.loads(metadata or "{}")
        items.append(
            DataExport(
                id=uuid.UUID(str(target_id)),
                status="completed" if meta.get("status") == "completed" else "pending",
                requested_at=occurred_at,
                manifest_url=meta.get("manifest_url"),
            )
        )
    return DataExportList(items=items)


async def _revoke_tokens(
    request: Request, user_id: uuid.UUID, transport: httpx.AsyncBaseTransport | None
) -> int:
    """Revoke every active token `user_id` holds, through identity (the tokens' one writer)."""
    listed = await forward_to_identity(request, "GET", "/api/v1/api-tokens?owner=all", transport)
    revoked = 0
    for item in listed.json().get("items", []):
        if item.get("user_id") != str(user_id) or item.get("state") != "active":
            continue
        await forward_to_identity(request, "DELETE", f"/api/v1/api-tokens/{item['id']}", transport)
        revoked += 1
    return revoked


@router.post("/erasures", response_model=Erasure)
async def create_erasure(
    body: ErasureCreate,
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    core: CoreTransport,
    identity: IdentityTransport,
) -> Erasure:
    """authz: admin. Erase one person's or one meeting's data (see the module docstring).
    core-api's 404 / 409 (meeting live) / 422 (confirmation mismatch, own data) pass through."""
    applied = (
        await forward_to_core(
            request, "POST", "/api/v1/erasures", core, body.model_dump(mode="json")
        )
    ).json()
    revoked_tokens = 0
    if body.kind == "person":
        revoked_tokens = await _revoke_tokens(request, body.target_id, identity)
    erasure_id = uuid.uuid4()
    requested_at = datetime.now(UTC)
    metadata: dict[str, Any] = {
        "kind": body.kind,
        "status": "completed",
        "target_id": str(body.target_id),
        "erased": applied.get("erased", {}),
        "anonymized": applied.get("anonymized", {}),
        "revoked_share_links": applied.get("revoked_share_links", 0),
        "revoked_tokens": revoked_tokens,
    }
    if body.kind == "meeting":
        # A meeting's title is the console's name for the request; a person's email is not
        # kept here (the record of an erasure must not hold what it erased).
        metadata["label"] = applied.get("label")
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        await audit(
            session,
            org_id=ctx.org_id,
            actor_id=ctx.user_id,
            action=ACTION_DATA_ERASURE_COMPLETED,
            target_type="data_erasure",
            target_id=erasure_id,
            metadata=metadata,
            ip=ctx.ip,
        )
        await emit_admin_event(
            session,
            org_id=ctx.org_id,
            event_type="erasure_requested",
            actor_id=ctx.user_id,
            target_type=body.kind,
            target_id=str(body.target_id),
            detail={"erasure_id": str(erasure_id), "scope": body.kind},
        )
    return Erasure(
        id=erasure_id,
        kind=body.kind,
        target_id=body.target_id,
        label=applied.get("label"),
        requested_by=ctx.user_id,
        requested_at=requested_at,
        erased=metadata["erased"],
        anonymized=metadata["anonymized"],
        revoked_share_links=int(metadata["revoked_share_links"]),
        revoked_tokens=revoked_tokens,
    )


@router.get("/erasures", response_model=ErasureList)
async def list_erasures(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> ErasureList:
    """authz: admin. Completed erasures, newest first. A person's label is their email read now
    from `users` (RLS-scoped); null when they are gone."""
    rows = (
        await session.execute(
            text(
                "SELECT a.target_id, a.actor_id, a.occurred_at, a.metadata, u.email "
                "FROM audit_logs a LEFT JOIN users u "
                "ON a.metadata->>'kind' = 'person' AND u.id::text = a.metadata->>'target_id' "
                "WHERE a.action = :a ORDER BY a.occurred_at DESC LIMIT 100"
            ),
            {"a": ACTION_DATA_ERASURE_COMPLETED},
        )
    ).all()
    items = []
    for target_id, actor_id, occurred_at, metadata, email in rows:
        meta = metadata if isinstance(metadata, dict) else json.loads(metadata or "{}")
        kind = meta.get("kind")
        if kind not in ("person", "meeting"):
            continue
        items.append(
            Erasure(
                id=uuid.UUID(str(target_id)),
                kind=kind,
                target_id=uuid.UUID(str(meta.get("target_id"))),
                label=email if kind == "person" else meta.get("label"),
                requested_by=actor_id,
                requested_at=occurred_at,
                erased=meta.get("erased") or {},
                anonymized=meta.get("anonymized") or {},
                revoked_share_links=int(meta.get("revoked_share_links") or 0),
                revoked_tokens=int(meta.get("revoked_tokens") or 0),
            )
        )
    return ErasureList(items=items)
