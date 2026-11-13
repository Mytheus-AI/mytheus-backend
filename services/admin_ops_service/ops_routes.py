"""`/admin/ops/*`: the internal-ops console API (P12, plan 3.13). Mytheus staff only.

Deny by default (`common.require_staff`: a workspace-less Clerk account listed in
`MYTHEUS_STAFF_CLERK_USER_IDS`, ADR-0046 D1), and every call is written to `ops_audit_log`:
reads included, inside the same transaction as the work (so a call whose log cannot be written
returns nothing), and denials in their own transaction. `whoami` answers any signed-in session
(and is logged too).

No standing cross-tenant row access. The directory and the cost view are aggregates over
control-plane tables, read one tenant at a time with `app.current_org` bound to that tenant
(RLS still applies to every read): `organizations` (id, name, status, created_at), a `count(*)`
of `users`, the plan code from `subscriptions`/`plans`, and for costs only the aggregate tables
`usage_aggregates` and `agent_budgets` (`COST_TABLES`). Tenant content (meetings, transcripts,
decisions, actions) is never selected. The audit viewer reads one tenant's `audit_logs` per call,
named in the body (never a path or query parameter), and only with that tenant's consent: an
active (unrevoked, unexpired) `support_grants` row for this staff user and that tenant, of any
scope. Without one it is a 403 (logged with `outcome: no_support_grant`); with one the rows are
returned as stored, the grant being the consent. Every read is logged with that tenant.

Suspend/reinstate: identity-tenant-service flips `organizations.status` (it is the sole
`tenant-events` producer); this service then logs the call and, for a suspension that changed
something, emits `admin-events` `tenant_suspended`. `AdminEventType` has no reinstated symbol
and this band may not change a schema, so a reinstatement emits no `admin-events` record (it is
still in `ops_audit_log`, and identity writes the tenant's `audit_logs` row). Staff accounts
belong to no workspace, so suspending one can never lock the console out.

Feature flags: CRUD with `app.ops_console` set transaction-locally (the `ops_console` policy);
a change to a workspace's flag emits `admin-events` `flag_changed` keyed to that workspace; a
global flag's change is in `ops_audit_log` only (the topic is partitioned by `org_id`, which
neither a global flag nor a staff account has).

DLQ counts and consumer lag are not readable from this service (they live in Redpanda and
Prometheus, neither of which it can reach), so the cost report returns `null` for both.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query, Request, Response, status
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.admin_ops_service.common import (
    IdentityTransport,
    StaffPrincipal,
    emit_admin_event,
    enable_ops_console,
    ops_caller,
    ops_log,
    ops_log_now,
    ops_session,
    require_staff,
    set_tenant_status,
    staff_refusal,
)
from services.admin_ops_service.schemas import (
    AuditEntry,
    AuditPage,
    AuditSearch,
    CostReport,
    Flag,
    FlagCreate,
    FlagList,
    FlagUpdate,
    ReinstateRequest,
    SuspendRequest,
    TenantCost,
    TenantList,
    TenantStatus,
    TenantSummary,
    Whoami,
)

router = APIRouter(prefix="/admin/ops", tags=["admin-ops"])

DEFAULT_PAGE = 100
MAX_PAGE = 200

# Newest first, keyset-paged on (created_at, id): the directory and the cost view walk tenants
# one page at a time, so a page costs a bounded number of per-tenant aggregate reads.
_ORGS = text(
    "SELECT id, name, status, created_at FROM organizations "
    "WHERE deleted_at IS NULL {after} ORDER BY created_at DESC, id DESC LIMIT :n"
)
_MEMBER_COUNT = text("SELECT count(*) FROM users WHERE deleted_at IS NULL")
_PLAN = text("SELECT p.code FROM subscriptions s JOIN plans p ON p.id = s.plan_id LIMIT 1")

# The cost view's statements. Their tables (`COST_TABLES`) are aggregate tables only; a test
# asserts nothing else is touched while the route runs.
_TOKENS = text(
    "SELECT COALESCE(sum(quantity), 0) FROM usage_aggregates "
    "WHERE dimension = 'agent_token' AND billing_period = :p"
)
_QUOTA = text("SELECT token_quota FROM agent_budgets WHERE period = :p LIMIT 1")
COST_TABLES = frozenset({"organizations", "usage_aggregates", "agent_budgets"})

# Bound to the target tenant (RLS), so only that tenant's grants are visible.
_ACTIVE_GRANT = text(
    "SELECT id FROM support_grants WHERE staff_user_id = :s AND revoked_at IS NULL "
    "AND expires_at > now() ORDER BY expires_at DESC LIMIT 1"
)

_FLAG_COLUMNS = "id, key, org_id, enabled, rules, updated_by, updated_at"


def _encode_org_cursor(created_at: datetime, org_id: uuid.UUID) -> str:
    raw = f"{created_at.isoformat()}|{org_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


async def _org_page(
    session: AsyncSession, ctx: StaffPrincipal, action: str, cursor: str | None, limit: int
) -> tuple[list[Any], str | None]:
    params: dict[str, Any] = {"n": limit + 1}
    after = ""
    if cursor:
        try:
            at, org_id = _decode_cursor(cursor)
        except ProblemDetail:
            await ops_log_now(
                staff_user_id=ctx.user_id, action=action, details={"outcome": "bad_cursor"}
            )
            raise
        after = "AND (created_at, id) < (:at, :oid)"
        params.update(at=at, oid=str(org_id))
    rows = list(
        (await session.execute(text(_ORGS.text.format(after=after)), params)).mappings().all()
    )
    page = rows[:limit]
    more = len(rows) > limit and page
    return page, _encode_org_cursor(page[-1]["created_at"], page[-1]["id"]) if more else None


@router.get("/whoami", response_model=Whoami, response_model_exclude_none=True)
async def whoami(
    caller: Annotated[StaffPrincipal, Depends(ops_caller)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> Whoami:
    """authz: any signed-in session, with or without a workspace; answers whether it is a
    Mytheus staff account (ADR-0046 D1), and its Staff ID when it is. Logged, with the reason
    when it is not."""
    reason = await staff_refusal(caller)
    await ops_log(
        session,
        staff_user_id=caller.user_id,
        action="ops.whoami",
        details={
            "staff": reason is None,
            "reason": reason,
            "clerk_user_id": caller.clerk_user_id,
        },
    )
    if reason is None:
        return Whoami(staff=True, staff_id=caller.user_id)
    return Whoami(staff=False)


@router.get("/tenants", response_model=TenantList)
async def list_tenants(
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
    cursor: str | None = None,
) -> TenantList:
    """authz: staff. Tenant directory, newest first, keyset-paged: aggregates only."""
    items = []
    orgs, next_cursor = await _org_page(session, ctx, "tenants.list", cursor, limit)
    for org in orgs:
        await set_rls(session, org["id"])
        members = (await session.execute(_MEMBER_COUNT)).scalar_one()
        plan = (await session.execute(_PLAN)).scalar()
        items.append(
            TenantSummary(
                org_id=org["id"],
                name=org["name"],
                plan=plan,
                status=org["status"],
                member_count=int(members),
                created_at=org["created_at"],
            )
        )
    await ops_log(
        session, staff_user_id=ctx.user_id, action="tenants.list", details={"count": len(items)}
    )
    return TenantList(items=items, next_cursor=next_cursor)


async def _change_status(
    ctx: StaffPrincipal,
    session: AsyncSession,
    transport: Any,
    *,
    org_id: uuid.UUID,
    target: str,
    reason: str | None,
) -> TenantStatus:
    action = "tenant.suspend" if target == "suspended" else "tenant.reinstate"
    try:
        result = await set_tenant_status(
            org_id, target, reason=reason, staff_user_id=ctx.user_id, transport=transport
        )
    except ProblemDetail as exc:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=org_id,
            details={"reason": reason, "outcome": "failed", "status": exc.status},
        )
        raise
    changed = bool(result.get("changed"))
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action=action,
        target_org_id=org_id,
        details={"reason": reason, "outcome": "changed" if changed else "unchanged"},
    )
    if changed and target == "suspended":
        await emit_admin_event(
            session,
            org_id=org_id,
            event_type="tenant_suspended",
            actor_id=ctx.user_id,
            target_type="organization",
            target_id=str(org_id),
            detail={"reason": reason, "status": "suspended"},
        )
    return TenantStatus(org_id=org_id, status=result["status"])


@router.post("/tenants/suspend", response_model=TenantStatus)
async def suspend_tenant(
    body: SuspendRequest,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    transport: IdentityTransport,
) -> TenantStatus:
    """authz: staff. Suspend through identity; `admin-events` `tenant_suspended`."""
    return await _change_status(
        ctx, session, transport, org_id=body.org_id, target="suspended", reason=body.reason
    )


@router.post("/tenants/reinstate", response_model=TenantStatus)
async def reinstate_tenant(
    body: ReinstateRequest,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    transport: IdentityTransport,
) -> TenantStatus:
    """authz: staff. Reinstate through identity (no `admin-events` symbol exists for it)."""
    return await _change_status(
        ctx, session, transport, org_id=body.org_id, target="active", reason=body.reason
    )


def _flag(row: Any) -> Flag:
    return Flag(**dict(row))


async def _flag_event(session: AsyncSession, ctx: StaffPrincipal, flag: Flag, op: str) -> None:
    # `admin-events` is partitioned by `org_id`, and a global flag has none; nor does a staff
    # account (ADR-0046 D1). A global flag's change is in `ops_audit_log` only.
    if flag.org_id is None:
        return
    await emit_admin_event(
        session,
        org_id=flag.org_id,
        event_type="flag_changed",
        actor_id=ctx.user_id,
        target_type="feature_flag",
        target_id=str(flag.id),
        detail={
            "op": op,
            "key": flag.key,
            "org_id": str(flag.org_id) if flag.org_id else None,
            "enabled": flag.enabled,
            "rules": flag.rules,
        },
    )


@router.get("/flags", response_model=FlagList)
async def list_flags(
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> FlagList:
    """authz: staff. Every flag, global first."""
    await enable_ops_console(session)
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_FLAG_COLUMNS} FROM feature_flags "
                    "ORDER BY org_id NULLS FIRST, key LIMIT 1000"
                )
            )
        )
        .mappings()
        .all()
    )
    await ops_log(
        session, staff_user_id=ctx.user_id, action="flags.list", details={"count": len(rows)}
    )
    return FlagList(items=[_flag(r) for r in rows])


@router.post(
    "/flags",
    status_code=status.HTTP_201_CREATED,
    response_model=Flag,
)
async def create_flag(
    body: FlagCreate,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> Flag:
    """authz: staff. 409 if the (key, org) pair exists; 404 for an unknown org."""
    await enable_ops_console(session)
    try:
        async with session.begin_nested():
            row = (
                (
                    await session.execute(
                        text(
                            "INSERT INTO feature_flags (key, org_id, enabled, rules, updated_by) "
                            "VALUES (:k, :o, :e, CAST(:r AS jsonb), :by) "
                            f"RETURNING {_FLAG_COLUMNS}"
                        ),
                        {
                            "k": body.key,
                            "o": str(body.org_id) if body.org_id else None,
                            "e": body.enabled,
                            "r": json.dumps(body.rules),
                            "by": str(ctx.user_id),
                        },
                    )
                )
                .mappings()
                .one()
            )
    except IntegrityError as exc:
        sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
        if sqlstate == "23503":
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found") from exc
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "That flag already exists for this scope"
        ) from exc
    flag = _flag(row)
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="flag.create",
        target_org_id=flag.org_id,
        details={"flag_id": str(flag.id), "key": flag.key, "enabled": flag.enabled},
    )
    await _flag_event(session, ctx, flag, "created")
    return flag


@router.patch("/flags/{flag_id}", response_model=Flag)
async def update_flag(
    flag_id: uuid.UUID,
    body: FlagUpdate,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> Flag:
    """authz: staff."""
    fields = body.model_dump(exclude_unset=True, exclude_none=True)
    if not fields:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Nothing to change")
    await enable_ops_console(session)
    sets = ["updated_by = :by", "updated_at = now()"]
    params: dict[str, Any] = {"id": str(flag_id), "by": str(ctx.user_id)}
    if "enabled" in fields:
        sets.append("enabled = :e")
        params["e"] = fields["enabled"]
    if "rules" in fields:
        sets.append("rules = CAST(:r AS jsonb)")
        params["r"] = json.dumps(fields["rules"])
    row = (
        (
            await session.execute(
                text(
                    f"UPDATE feature_flags SET {', '.join(sets)} WHERE id = :id "
                    f"RETURNING {_FLAG_COLUMNS}"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="flag.update",
            details={"flag_id": str(flag_id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    flag = _flag(row)
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="flag.update",
        target_org_id=flag.org_id,
        details={"flag_id": str(flag.id), "key": flag.key, "changed": sorted(fields)},
    )
    await _flag_event(session, ctx, flag, "updated")
    return flag


@router.delete(
    "/flags/{flag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_flag(
    flag_id: uuid.UUID,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> Response:
    """authz: staff."""
    await enable_ops_console(session)
    row = (
        (
            await session.execute(
                text(f"DELETE FROM feature_flags WHERE id = :id RETURNING {_FLAG_COLUMNS}"),
                {"id": str(flag_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="flag.delete",
            details={"flag_id": str(flag_id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    flag = _flag(row)
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="flag.delete",
        target_org_id=flag.org_id,
        details={"flag_id": str(flag.id), "key": flag.key},
    )
    await _flag_event(session, ctx, flag, "deleted")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


def current_period(now: datetime | None = None) -> str:
    return (now or datetime.now(UTC)).strftime("%Y-%m")


@router.get("/costs", response_model=CostReport)
async def cost_report(
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE)] = DEFAULT_PAGE,
    cursor: str | None = None,
) -> CostReport:
    """authz: staff. Per-tenant token usage this billing period, from aggregate tables only.
    Same tenant order and paging as the directory."""
    period = current_period()
    tenants = []
    orgs, next_cursor = await _org_page(session, ctx, "costs.read", cursor, limit)
    for org in orgs:
        await set_rls(session, org["id"])
        tokens = (await session.execute(_TOKENS, {"p": period})).scalar_one()
        quota = (await session.execute(_QUOTA, {"p": period})).scalar()
        tenants.append(
            TenantCost(
                org_id=org["id"],
                name=org["name"],
                tokens_consumed=int(tokens),
                token_quota=int(quota) if quota is not None else None,
            )
        )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="costs.read",
        details={"period": period, "tenants": len(tenants)},
    )
    return CostReport(period=period, tenants=tenants, next_cursor=next_cursor)


def _encode_cursor(occurred_at: datetime, row_id: uuid.UUID) -> str:
    raw = f"{occurred_at.isoformat()}|{row_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        at, _, row_id = base64.urlsafe_b64decode(padded.encode()).decode().partition("|")
        return datetime.fromisoformat(at), uuid.UUID(row_id)
    except (ValueError, binascii.Error, UnicodeDecodeError):
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed cursor") from None


@router.post("/audit/search", response_model=AuditPage)
async def search_audit(
    body: AuditSearch,
    request: Request,
    ctx: Annotated[StaffPrincipal, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> AuditPage:
    """authz: staff with an active support grant from that tenant. Read-only page of one
    tenant's `audit_logs`, newest first; the read is itself logged with that tenant as the
    target. A POST so the tenant id rides in the body."""
    params: dict[str, Any] = {"n": body.limit + 1}
    where = ""
    if body.cursor:
        try:
            at, row_id = _decode_cursor(body.cursor)
        except ProblemDetail:
            await ops_log_now(
                staff_user_id=ctx.user_id,
                action="audit.read",
                target_org_id=body.org_id,
                details={"outcome": "bad_cursor"},
            )
            raise
        where = "WHERE (occurred_at, id) < (:at, :rid)"
        params.update(at=at, rid=str(row_id))
    await set_rls(session, body.org_id)
    grant = (await session.execute(_ACTIVE_GRANT, {"s": str(ctx.user_id)})).scalar()
    if grant is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="audit.read",
            target_org_id=body.org_id,
            details={"outcome": "no_support_grant"},
        )
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "Reading a tenant's audit log needs an active support grant from that tenant",
        )
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, org_id, actor_id, actor_type, action, target_type, target_id, "
                    f"metadata, occurred_at FROM audit_logs {where} "
                    "ORDER BY occurred_at DESC, id DESC LIMIT :n"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    page = rows[: body.limit]
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="audit.read",
        target_org_id=body.org_id,
        details={
            "returned": len(page),
            "cursor": bool(body.cursor),
            "support_grant_id": str(grant),
        },
    )
    next_cursor = (
        _encode_cursor(page[-1]["occurred_at"], page[-1]["id"])
        if len(rows) > body.limit and page
        else None
    )
    return AuditPage(items=[AuditEntry(**dict(r)) for r in page], next_cursor=next_cursor)
