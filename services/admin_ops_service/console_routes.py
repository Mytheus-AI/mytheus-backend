"""`/admin/ops/*`, the rest of the superadmin console (ADR-0046). Mytheus staff only.

Same rules as `ops_routes.py`: deny by default (`require_staff`), every call logged in
`ops_audit_log` inside its own transaction (denials and failures in their own), aggregates
rather than tenant content, a target workspace named in the BODY, and RLS on every read.

- `GET  /overview`               platform totals and weekly series, from `ops_tenant_rollup`
                                 (a SECURITY INVOKER function that binds each workspace in turn,
                                 so RLS still checks every row it counts).
- `POST /tenants/detail`         one workspace: placement, usage, flags, grants, support
                                 sessions, move requests and the recent ops actions on it.
- `GET  /health`                 each backend service's `/healthz`, probed from here, and the
                                 unpublished outbox backlog. DLQ and consumer lag stay `null`:
                                 they live in Redpanda and Prometheus, which this service cannot
                                 reach.
- `GET  /staff`                  the allowlist with each person's email (resolved through their
                                 own `ops.whoami` rows) and activity.
- `POST /tenants/migrate`        record a request to move a workspace (one open per workspace);
  `POST /tenants/migrations/cancel`. The move is not automated (ADR-0046 D5).
- `POST /support/members`        a workspace's members, under an active `impersonate` grant.
- `POST /support/sessions`       start a support session (Clerk actor token), under that grant.
- `POST /support/sessions/end`   end it.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import clerk_admin
from libs.shared.audit import audit
from libs.shared.authz import RequestContext
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.admin_ops_service.common import (
    enable_ops_console,
    ops_log,
    ops_log_now,
    ops_session,
    require_staff,
    staff_user_ids,
)
from services.admin_ops_service.ops_routes import current_period
from services.admin_ops_service.schemas import (
    DayCount,
    Flag,
    GrantView,
    Health,
    MigrateRequest,
    MigrationCancel,
    MigrationView,
    OpsAction,
    OutboxTenant,
    Overview,
    OverviewTotals,
    PlanCount,
    Residency,
    ServiceProbe,
    StaffMember,
    StaffReport,
    SupportMember,
    SupportMemberList,
    SupportSessionEnd,
    SupportSessionStart,
    SupportSessionStarted,
    SupportSessionView,
    TenantDetail,
    TenantTarget,
    TokenLeader,
    WeekCount,
)

router = APIRouter(prefix="/admin/ops", tags=["admin-ops"])

WEEKS = 12
DAYS = 14
ACTION_SUPPORT_SESSION_STARTED = "support_session.started"
ACTION_SUPPORT_SESSION_ENDED = "support_session.ended"

# The backend services with an HTTP `/healthz`, by their canonical names. Reached the way the
# gateway reaches them: `UPSTREAM_<NAME>` if set, else the compose service name on port 8000.
PROBED_SERVICES = (
    "api-gateway",
    "bff",
    "identity-tenant-service",
    "core-api",
    "meeting-service",
    "integration-service",
    "billing-metering-service",
    "notification-service",
)
SELF = "admin-ops-service"
PROBE_TIMEOUT = 2.0


# --- shared reads ------------------------------------------------------------------------------


def _week_starts(now: datetime, weeks: int = WEEKS) -> list[date]:
    monday = (now - timedelta(days=now.weekday())).date()
    return [monday - timedelta(weeks=weeks - 1 - i) for i in range(weeks)]


def _day_starts(now: datetime, days: int = DAYS) -> list[date]:
    today = now.date()
    return [today - timedelta(days=days - 1 - i) for i in range(days)]


async def _rollup(session: AsyncSession, period: str) -> list[Any]:
    """Per-workspace numbers for every live workspace, newest first (`ops_tenant_rollup`)."""
    result = await session.execute(
        text("SELECT * FROM ops_tenant_rollup(:w, :p)"), {"w": WEEKS, "p": period}
    )
    return list(result.mappings().all())


async def _activity_by_day(session: AsyncSession, now: datetime) -> list[DayCount]:
    rows = (
        await session.execute(
            text(
                "SELECT (created_at AT TIME ZONE 'UTC')::date AS d, count(*) AS n "
                "FROM ops_audit_log WHERE created_at >= :since GROUP BY 1"
            ),
            {"since": datetime.combine(_day_starts(now)[0], datetime.min.time(), tzinfo=UTC)},
        )
    ).all()
    counts = {r[0]: int(r[1]) for r in rows}
    return [DayCount(day=d.isoformat(), count=counts.get(d, 0)) for d in _day_starts(now)]


def _ops_action(row: Any) -> OpsAction:
    return OpsAction(**dict(row))


_OPS_ACTION_COLUMNS = "id, staff_user_id, action, target_org_id, details, created_at"


# --- overview ----------------------------------------------------------------------------------


@router.get("/overview", response_model=Overview)
async def overview(
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> Overview:
    """authz: staff. Platform totals and series. Aggregates only."""
    now = datetime.now(UTC)
    period = current_period(now)
    rows = await _rollup(session, period)
    weeks = _week_starts(now)

    signups: Counter[date] = Counter()
    meetings = [0] * WEEKS
    for r in rows:
        created = r["created_at"].astimezone(UTC)
        signups[(created - timedelta(days=created.weekday())).date()] += 1
        for i, n in enumerate((r["meetings_weekly"] or [])[:WEEKS]):
            meetings[i] += int(n)

    plans = Counter((r["plan"] or "none") for r in rows)
    leaders = sorted(
        (r for r in rows if int(r["tokens"]) > 0),
        key=lambda r: (
            -(int(r["tokens"]) / r["token_quota"] if r["token_quota"] else -1.0),
            -int(r["tokens"]),
        ),
    )[:10]

    result = Overview(
        period=period,
        totals=OverviewTotals(
            tenants=len(rows),
            active=sum(1 for r in rows if r["status"] == "active"),
            suspended=sum(1 for r in rows if r["status"] == "suspended"),
            members=sum(int(r["members"]) for r in rows),
            meetings_7d=sum(int(r["meetings_7d"]) for r in rows),
            meetings_total=sum(int(r["meetings_total"]) for r in rows),
            tokens_period=sum(int(r["tokens"]) for r in rows),
        ),
        signups_by_week=[WeekCount(week=w.isoformat(), count=signups.get(w, 0)) for w in weeks],
        meetings_by_week=[
            WeekCount(week=w.isoformat(), count=meetings[i]) for i, w in enumerate(weeks)
        ],
        plans=[PlanCount(plan=p, count=n) for p, n in plans.most_common()],
        token_leaders=[
            TokenLeader(
                org_id=r["org_id"],
                name=r["name"],
                tokens=int(r["tokens"]),
                token_quota=int(r["token_quota"]) if r["token_quota"] is not None else None,
            )
            for r in leaders
        ],
        ops_activity_by_day=await _activity_by_day(session, now),
    )
    await enable_ops_console(session)
    result.totals.feedback_new = int(
        (
            await session.execute(
                text("SELECT count(*) FROM feedback_reports WHERE status = 'new'")
            )
        ).scalar_one()
    )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="overview.read",
        details={"tenants": len(rows), "period": period},
    )
    return result


# --- workspace detail --------------------------------------------------------------------------


async def _live_org(
    session: AsyncSession, ctx: RequestContext, org_id: uuid.UUID, action: str
) -> Any:
    org = (
        (
            await session.execute(
                text(
                    "SELECT id, name, status, created_at FROM organizations "
                    "WHERE id = :o AND deleted_at IS NULL"
                ),
                {"o": str(org_id)},
            )
        )
        .mappings()
        .first()
    )
    if org is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=org_id,
            details={"outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return org


_SESSION_COLUMNS = (
    "s.id, s.staff_user_id, s.user_id, u.email AS user_email, s.reason, s.started_at, "
    "s.expires_at, s.ended_at, s.end_reason, "
    "(s.ended_at IS NULL AND s.expires_at > now()) AS open"
)
_MIGRATION_COLUMNS = (
    "id, from_region, from_cell, to_region, to_cell, status, reason, requested_by, "
    "created_at, updated_at"
)


@router.post("/tenants/detail", response_model=TenantDetail)
async def tenant_detail(
    body: TenantTarget,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> TenantDetail:
    """authz: staff. One workspace's control-plane picture. No meeting, transcript, decision
    or action content: meetings are counted by week, nothing else is read from them."""
    org = await _live_org(session, ctx, body.org_id, "tenant.detail")
    now = datetime.now(UTC)
    period = current_period(now)
    weeks = _week_starts(now)
    await set_rls(session, body.org_id)

    async def scalar(sql: str, **params: Any) -> Any:
        return (await session.execute(text(sql), params)).scalar()

    plan = await scalar(
        "SELECT p.code FROM subscriptions s JOIN plans p ON p.id = s.plan_id LIMIT 1"
    )
    res = (
        (
            await session.execute(
                text(
                    "SELECT region, db_cell, qdrant_region, object_bucket_region, locked_at "
                    "FROM data_residency LIMIT 1"
                )
            )
        )
        .mappings()
        .first()
    )
    members = await scalar("SELECT count(*) FROM users WHERE deleted_at IS NULL")
    meetings_total = await scalar("SELECT count(*) FROM meetings")
    since = datetime.combine(weeks[0], datetime.min.time(), tzinfo=UTC)
    weekly = {
        r[0]: int(r[1])
        for r in (
            await session.execute(
                text(
                    "SELECT (date_trunc('week', created_at AT TIME ZONE 'UTC'))::date, count(*) "
                    "FROM meetings WHERE created_at >= :since GROUP BY 1"
                ),
                {"since": since},
            )
        ).all()
    }
    tokens = await scalar(
        "SELECT COALESCE(sum(quantity), 0) FROM usage_aggregates "
        "WHERE dimension = 'agent_token' AND billing_period = :p",
        p=period,
    )
    quota = await scalar(
        "SELECT token_quota FROM agent_budgets WHERE period = :p LIMIT 1", p=period
    )
    grants = (
        (
            await session.execute(
                text(
                    "SELECT id, staff_user_id, scope, expires_at, revoked_at, created_at, "
                    "(revoked_at IS NULL AND expires_at > now()) AS active FROM support_grants "
                    "ORDER BY created_at DESC LIMIT 20"
                )
            )
        )
        .mappings()
        .all()
    )
    sessions = (
        (
            await session.execute(
                text(
                    f"SELECT {_SESSION_COLUMNS} FROM support_sessions s "
                    "LEFT JOIN users u ON u.id = s.user_id ORDER BY s.started_at DESC LIMIT 20"
                )
            )
        )
        .mappings()
        .all()
    )
    migrations = (
        (
            await session.execute(
                text(
                    f"SELECT {_MIGRATION_COLUMNS} FROM tenant_migrations "
                    "ORDER BY created_at DESC LIMIT 20"
                )
            )
        )
        .mappings()
        .all()
    )
    await enable_ops_console(session)
    flags = (
        (
            await session.execute(
                text(
                    "SELECT id, key, org_id, enabled, rules, updated_by, updated_at "
                    "FROM feature_flags WHERE org_id = :o OR org_id IS NULL "
                    "ORDER BY org_id NULLS FIRST, key LIMIT 500"
                ),
                {"o": str(body.org_id)},
            )
        )
        .mappings()
        .all()
    )
    recent = (
        (
            await session.execute(
                text(
                    f"SELECT {_OPS_ACTION_COLUMNS} FROM ops_audit_log WHERE target_org_id = :o "
                    "ORDER BY created_at DESC LIMIT 20"
                ),
                {"o": str(body.org_id)},
            )
        )
        .mappings()
        .all()
    )
    result = TenantDetail(
        org_id=org["id"],
        name=org["name"],
        status=org["status"],
        plan=plan,
        created_at=org["created_at"],
        residency=Residency(**dict(res)) if res else None,
        members=int(members or 0),
        meetings_total=int(meetings_total or 0),
        meetings_by_week=[WeekCount(week=w.isoformat(), count=weekly.get(w, 0)) for w in weeks],
        tokens_period=int(tokens or 0),
        token_quota=int(quota) if quota is not None else None,
        period=period,
        flags=[Flag(**dict(f)) for f in flags],
        grants=[GrantView(**dict(g)) for g in grants],
        sessions=[SupportSessionView(**dict(s)) for s in sessions],
        migrations=[MigrationView(**dict(m)) for m in migrations],
        recent_ops=[_ops_action(r) for r in recent],
    )
    await ops_log(
        session, staff_user_id=ctx.user_id, action="tenant.detail", target_org_id=body.org_id
    )
    return result


# --- health ------------------------------------------------------------------------------------


def _upstream(service: str) -> str:
    override = os.environ.get("UPSTREAM_" + service.upper().replace("-", "_"))
    return (override or f"http://{service}:8000").rstrip("/")


async def _probe(client: httpx.AsyncClient, service: str) -> ServiceProbe:
    started = time.perf_counter()
    try:
        response = await client.get(f"{_upstream(service)}/healthz")
    except httpx.HTTPError as exc:
        return ServiceProbe(
            service=service, status="down", latency_ms=None, detail=type(exc).__name__
        )
    latency = int((time.perf_counter() - started) * 1000)
    if response.status_code != 200:
        return ServiceProbe(
            service=service,
            status="down",
            latency_ms=latency,
            detail=f"HTTP {response.status_code}",
        )
    return ServiceProbe(service=service, status="up", latency_ms=latency)


def probe_transport() -> httpx.AsyncBaseTransport | None:
    """FastAPI dependency: the transport health probes use. None = real network."""
    return None


@router.get("/health", response_model=Health)
async def health(
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    transport: Annotated[httpx.AsyncBaseTransport | None, Depends(probe_transport)],
) -> Health:
    """authz: staff. Service probes and the outbox backlog (counts per workspace only)."""
    async with httpx.AsyncClient(transport=transport, timeout=PROBE_TIMEOUT) as client:
        probes = await asyncio.gather(*(_probe(client, s) for s in PROBED_SERVICES))
    services = [ServiceProbe(service=SELF, status="up", latency_ms=0), *probes]

    rows = await _rollup(session, current_period())
    backlog = sorted(
        (r for r in rows if int(r["outbox_pending"]) > 0),
        key=lambda r: (-int(r["outbox_pending"]), r["outbox_oldest"]),
    )
    oldest = min((r["outbox_oldest"] for r in backlog), default=None)
    result = Health(
        checked_at=datetime.now(UTC),
        services=services,
        outbox_pending=sum(int(r["outbox_pending"]) for r in backlog),
        outbox_oldest_pending_at=oldest,
        outbox_max_attempts=max((int(r["outbox_attempts"]) for r in backlog), default=0),
        outbox_tenants=[
            OutboxTenant(
                org_id=r["org_id"],
                name=r["name"],
                pending=int(r["outbox_pending"]),
                oldest_pending_at=r["outbox_oldest"],
                max_attempts=int(r["outbox_attempts"]),
            )
            for r in backlog[:10]
        ],
    )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="health.read",
        details={"down": [p.service for p in services if p.status == "down"]},
    )
    return result


# --- staff -------------------------------------------------------------------------------------


@router.get("/staff", response_model=StaffReport)
async def staff_report(
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> StaffReport:
    """authz: staff. The allowlist and its activity. A staff member's email is read from their
    own workspace, found through their own `ops.whoami` rows; someone who has never opened the
    console has none yet."""
    ids = sorted(staff_user_ids(), key=str)
    now = datetime.now(UTC)
    id_list = [str(i) for i in ids]
    seen = {
        r["staff_user_id"]: r
        for r in (
            await session.execute(
                text(
                    "SELECT DISTINCT ON (staff_user_id) staff_user_id, "
                    "details->>'caller_org_id' AS org_id FROM ops_audit_log "
                    "WHERE staff_user_id = ANY(CAST(:ids AS uuid[])) AND action = 'ops.whoami' "
                    "ORDER BY staff_user_id, created_at DESC"
                ),
                {"ids": id_list},
            )
        )
        .mappings()
        .all()
    }
    activity = {
        r["staff_user_id"]: r
        for r in (
            await session.execute(
                text(
                    "SELECT staff_user_id, max(created_at) AS last_seen_at, "
                    "count(*) FILTER (WHERE created_at >= now() - interval '30 days') AS n "
                    "FROM ops_audit_log WHERE staff_user_id = ANY(CAST(:ids AS uuid[])) "
                    "GROUP BY staff_user_id"
                ),
                {"ids": id_list},
            )
        )
        .mappings()
        .all()
    }
    members: list[StaffMember] = []
    for staff_id in ids:
        org_id: uuid.UUID | None = None
        email = org_name = name = None
        raw = seen[staff_id]["org_id"] if staff_id in seen else None
        try:
            org_id = uuid.UUID(raw) if raw else None
        except ValueError:
            org_id = None
        if org_id is not None:
            org_name = (
                await session.execute(
                    text("SELECT name FROM organizations WHERE id = :o"), {"o": str(org_id)}
                )
            ).scalar()
            await set_rls(session, org_id)
            who = (
                await session.execute(
                    text("SELECT email, display_name FROM users WHERE id = :u"),
                    {"u": str(staff_id)},
                )
            ).first()
            email, name = (who[0], who[1]) if who else (None, None)
        act = activity.get(staff_id)
        members.append(
            StaffMember(
                user_id=staff_id,
                name=name,
                email=email,
                org_id=org_id,
                org_name=org_name,
                last_seen_at=act["last_seen_at"] if act else None,
                actions_30d=int(act["n"]) if act else 0,
            )
        )
    recent = (
        (
            await session.execute(
                text(
                    f"SELECT {_OPS_ACTION_COLUMNS} FROM ops_audit_log "
                    "ORDER BY created_at DESC LIMIT 50"
                )
            )
        )
        .mappings()
        .all()
    )
    result = StaffReport(
        staff=members,
        activity_by_day=await _activity_by_day(session, now),
        recent=[_ops_action(r) for r in recent],
    )
    await ops_log(
        session, staff_user_id=ctx.user_id, action="staff.read", details={"staff": len(ids)}
    )
    return result


# --- tenant move requests ----------------------------------------------------------------------


@router.post("/tenants/migrate", response_model=MigrationView, status_code=201)
async def request_migration(
    body: MigrateRequest,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> MigrationView:
    """authz: staff. Record a move request (ADR-0046 D5). One open request per workspace."""
    await _live_org(session, ctx, body.org_id, "tenant.migrate")
    await set_rls(session, body.org_id)
    res = (
        (await session.execute(text("SELECT region, db_cell FROM data_residency LIMIT 1")))
        .mappings()
        .first()
    )
    from_region = res["region"] if res else "us"
    from_cell = res["db_cell"] if res else "pool-us-1"
    to_cell = body.to_cell or f"pool-{body.to_region}-1"
    if (body.to_region, to_cell) == (from_region, from_cell):
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="tenant.migrate",
            target_org_id=body.org_id,
            details={"outcome": "already_there", "to_region": body.to_region, "to_cell": to_cell},
        )
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The workspace is already there")
    try:
        async with session.begin_nested():
            row = (
                (
                    await session.execute(
                        text(
                            "INSERT INTO tenant_migrations (org_id, from_region, from_cell, "
                            "to_region, to_cell, reason, requested_by) "
                            "VALUES (:o, :fr, :fc, :tr, :tc, :reason, :by) "
                            f"RETURNING {_MIGRATION_COLUMNS}"
                        ),
                        {
                            "o": str(body.org_id),
                            "fr": from_region,
                            "fc": from_cell,
                            "tr": body.to_region,
                            "tc": to_cell,
                            "reason": body.reason,
                            "by": str(ctx.user_id),
                        },
                    )
                )
                .mappings()
                .one()
            )
    except IntegrityError as exc:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="tenant.migrate",
            target_org_id=body.org_id,
            details={"outcome": "open_request_exists"},
        )
        raise ProblemDetail(
            409, ErrorCode.VERSION_CONFLICT, "This workspace already has an open move request"
        ) from exc
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="tenant.migrate",
        target_org_id=body.org_id,
        details={
            "migration_id": str(row["id"]),
            "from": f"{from_region}/{from_cell}",
            "to": f"{body.to_region}/{to_cell}",
            "reason": body.reason,
        },
    )
    return MigrationView(**dict(row))


@router.post("/tenants/migrations/cancel", response_model=MigrationView)
async def cancel_migration(
    body: MigrationCancel,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> MigrationView:
    """authz: staff. Cancel an open move request."""
    await set_rls(session, body.org_id)
    row = (
        (
            await session.execute(
                text(
                    "UPDATE tenant_migrations SET status = 'cancelled', updated_at = now() "
                    "WHERE id = :m AND status = 'requested' "
                    f"RETURNING {_MIGRATION_COLUMNS}"
                ),
                {"m": str(body.migration_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="tenant.migrate.cancel",
            target_org_id=body.org_id,
            details={"migration_id": str(body.migration_id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="tenant.migrate.cancel",
        target_org_id=body.org_id,
        details={"migration_id": str(body.migration_id)},
    )
    return MigrationView(**dict(row))


# --- support sessions --------------------------------------------------------------------------

# Bound to the target workspace (RLS), so only its grants are visible.
_IMPERSONATE_GRANT = text(
    "SELECT id, expires_at FROM support_grants WHERE staff_user_id = :s AND scope = 'impersonate' "
    "AND revoked_at IS NULL AND expires_at > now() ORDER BY expires_at DESC LIMIT 1"
)


async def _impersonate_grant(
    session: AsyncSession, ctx: RequestContext, org_id: uuid.UUID, action: str
) -> Any:
    await set_rls(session, org_id)
    grant = (await session.execute(_IMPERSONATE_GRANT, {"s": str(ctx.user_id)})).mappings().first()
    if grant is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=org_id,
            details={"outcome": "no_impersonate_grant"},
        )
        raise ProblemDetail(
            403,
            ErrorCode.FORBIDDEN_ROLE,
            "Acting as a member needs an active impersonate grant from that workspace",
        )
    return grant


@router.post("/support/members", response_model=SupportMemberList)
async def support_members(
    body: TenantTarget,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> SupportMemberList:
    """authz: staff with an active `impersonate` grant from that workspace."""
    grant = await _impersonate_grant(session, ctx, body.org_id, "support.members")
    staff = staff_user_ids()
    rows = (
        (
            await session.execute(
                text(
                    "SELECT u.id, u.email, u.display_name, g.tier FROM users u "
                    "LEFT JOIN rbac_grants g ON g.user_id = u.id AND g.org_id = u.org_id "
                    "WHERE u.deleted_at IS NULL ORDER BY u.email LIMIT 500"
                )
            )
        )
        .mappings()
        .all()
    )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="support.members",
        target_org_id=body.org_id,
        details={"returned": len(rows), "support_grant_id": str(grant["id"])},
    )
    return SupportMemberList(
        items=[
            SupportMember(
                user_id=r["id"],
                name=r["display_name"],
                email=r["email"],
                tier=r["tier"],
                is_staff=r["id"] in staff,
            )
            for r in rows
        ],
        grant_expires_at=grant["expires_at"],
    )


@router.post("/support/sessions", response_model=SupportSessionStarted, status_code=201)
async def start_support_session(
    body: SupportSessionStart,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> SupportSessionStarted:
    """authz: staff with an active `impersonate` grant. Opens a time-boxed session as one
    member through a Clerk actor token. The workspace's own audit log records it."""
    action = "support.session.start"
    grant = await _impersonate_grant(session, ctx, body.org_id, action)
    target = (
        (
            await session.execute(
                text(
                    "SELECT id, clerk_user_id, email FROM users "
                    "WHERE id = :u AND deleted_at IS NULL"
                ),
                {"u": str(body.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if target is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=body.org_id,
            details={"user_id": str(body.user_id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    if body.user_id in staff_user_ids():
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=body.org_id,
            details={"user_id": str(body.user_id), "outcome": "target_is_staff"},
        )
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "Staff accounts cannot be acted as")

    now = datetime.now(UTC)
    expires = min(now + timedelta(minutes=body.minutes), grant["expires_at"])
    seconds = int((expires - now).total_seconds())
    if seconds < 60:
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "The support grant is about to expire")
    try:
        token = await clerk_admin.create_actor_token(
            target["clerk_user_id"], ctx.clerk_user_id, expires_in_seconds=seconds
        )
    except clerk_admin.ClerkUnavailable as exc:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=body.org_id,
            details={"user_id": str(body.user_id), "outcome": "clerk_unavailable"},
        )
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "Clerk could not open the session"
        ) from exc

    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO support_sessions (org_id, grant_id, staff_user_id, "
                    "staff_clerk_user_id, user_id, reason, clerk_actor_token_id, expires_at) "
                    "VALUES (:o, :g, :s, :sc, :u, :reason, :tok, :exp) RETURNING id"
                ),
                {
                    "o": str(body.org_id),
                    "g": str(grant["id"]),
                    "s": str(ctx.user_id),
                    "sc": ctx.clerk_user_id,
                    "u": str(body.user_id),
                    "reason": body.reason,
                    "tok": token.id,
                    "exp": expires,
                },
            )
        )
        .mappings()
        .one()
    )
    view = (
        (
            await session.execute(
                text(
                    f"SELECT {_SESSION_COLUMNS} FROM support_sessions s "
                    "LEFT JOIN users u ON u.id = s.user_id WHERE s.id = :i"
                ),
                {"i": str(row["id"])},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=body.org_id,
        actor_id=ctx.user_id,
        actor_type="system",
        action=ACTION_SUPPORT_SESSION_STARTED,
        target_type="user",
        target_id=body.user_id,
        metadata={
            "support_session_id": str(row["id"]),
            "staff_user_id": str(ctx.user_id),
            "support_grant_id": str(grant["id"]),
            "reason": body.reason,
            "expires_at": expires.isoformat(),
        },
    )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action=action,
        target_org_id=body.org_id,
        details={
            "support_session_id": str(row["id"]),
            "user_id": str(body.user_id),
            "reason": body.reason,
            "expires_at": expires.isoformat(),
        },
    )
    return SupportSessionStarted(session=SupportSessionView(**dict(view)), sign_in_url=token.url)


@router.post("/support/sessions/end", response_model=SupportSessionView)
async def end_support_session(
    body: SupportSessionEnd,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> SupportSessionView:
    """authz: staff, their own session. Access ends on the member's next request."""
    action = "support.session.end"
    await set_rls(session, body.org_id)
    ended = (
        (
            await session.execute(
                text(
                    "UPDATE support_sessions SET ended_at = now(), end_reason = 'staff' "
                    "WHERE id = :i AND staff_user_id = :s AND ended_at IS NULL "
                    "RETURNING id, user_id, clerk_actor_token_id"
                ),
                {"i": str(body.session_id), "s": str(ctx.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if ended is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action=action,
            target_org_id=body.org_id,
            details={"support_session_id": str(body.session_id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    if ended["clerk_actor_token_id"]:
        await clerk_admin.revoke_actor_token(ended["clerk_actor_token_id"])
    await audit(
        session,
        org_id=body.org_id,
        actor_id=ctx.user_id,
        actor_type="system",
        action=ACTION_SUPPORT_SESSION_ENDED,
        target_type="user",
        target_id=ended["user_id"],
        metadata={"support_session_id": str(body.session_id), "staff_user_id": str(ctx.user_id)},
    )
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action=action,
        target_org_id=body.org_id,
        details={"support_session_id": str(body.session_id)},
    )
    view = (
        (
            await session.execute(
                text(
                    f"SELECT {_SESSION_COLUMNS} FROM support_sessions s "
                    "LEFT JOIN users u ON u.id = s.user_id WHERE s.id = :i"
                ),
                {"i": str(body.session_id)},
            )
        )
        .mappings()
        .one()
    )
    return SupportSessionView(**dict(view))
