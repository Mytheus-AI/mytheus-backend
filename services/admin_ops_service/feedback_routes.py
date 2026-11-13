"""Bug reports and feedback from members to Mytheus staff (ADR-0046 D6).

Two sides, one table (`feedback_reports`):

- `POST /admin/tenant/feedback`, `GET /admin/tenant/feedback` (any member): send a report, and
  list your own with the state staff gave it. Written in the caller's tenant transaction, so
  the `tenant_insert` policy holds the row to the caller's workspace. The reporter's email is
  copied onto the row so staff can answer without reading the workspace's `users`. At most
  `DAILY_LIMIT` per person per 24 hours.
- `GET /admin/ops/feedback`, `POST /admin/ops/feedback/status` (staff): the inbox across every
  workspace, through the `ops_console` policy (set transaction-locally), each read and change
  logged in `ops_audit_log`. This is the one place `/ops` reads text a member wrote, and it is
  text they wrote to Mytheus.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.admin_ops_service.common import (
    enable_ops_console,
    ops_log,
    ops_log_now,
    ops_session,
    require_staff,
)
from services.admin_ops_service.schemas import (
    FeedbackCreate,
    FeedbackList,
    FeedbackReport,
    FeedbackStatusUpdate,
    OpsFeedback,
    OpsFeedbackList,
)

tenant_router = APIRouter(prefix="/admin/tenant", tags=["admin-tenant"])
ops_router = APIRouter(prefix="/admin/ops", tags=["admin-ops"])

DAILY_LIMIT = 20
ACTION_FEEDBACK_SUBMITTED = "feedback.submitted"
_COLUMNS = "id, kind, message, page, status, created_at, updated_at"


@tenant_router.post("/feedback", response_model=FeedbackReport, status_code=status.HTTP_201_CREATED)
async def submit_feedback(
    body: FeedbackCreate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> FeedbackReport:
    """authz: member. Send Mytheus a bug report or feedback."""
    if ctx.user_id is None:
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "No local user for this caller")
    message = body.message.strip()
    if not message:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Write a few words first")
    recent = (
        await session.execute(
            text(
                "SELECT count(*) FROM feedback_reports WHERE user_id = :u "
                "AND created_at > now() - interval '1 day'"
            ),
            {"u": str(ctx.user_id)},
        )
    ).scalar_one()
    if recent >= DAILY_LIMIT:
        raise ProblemDetail(
            429, ErrorCode.RATE_LIMITED, "That is a lot of reports for one day. Try again tomorrow."
        )
    email = (
        await session.execute(
            text("SELECT email FROM users WHERE id = :u"), {"u": str(ctx.user_id)}
        )
    ).scalar() or ""
    page = body.page.strip() if body.page and body.page.strip() else None
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO feedback_reports "
                    "(org_id, user_id, reporter_email, kind, message, page) "
                    "VALUES (:o, :u, :e, :k, :m, :p) "
                    f"RETURNING {_COLUMNS}"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(ctx.user_id),
                    "e": email,
                    "k": body.kind,
                    "m": message,
                    "p": page,
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
        action=ACTION_FEEDBACK_SUBMITTED,
        target_type="feedback_report",
        target_id=row["id"],
        metadata={"kind": body.kind},
        ip=ctx.ip,
    )
    return FeedbackReport(**dict(row))


@tenant_router.get("/feedback", response_model=FeedbackList)
async def my_feedback(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> FeedbackList:
    """authz: member. The caller's own reports, newest first."""
    if ctx.user_id is None:
        return FeedbackList(items=[])
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLUMNS} FROM feedback_reports WHERE user_id = :u "
                    "ORDER BY created_at DESC LIMIT 50"
                ),
                {"u": str(ctx.user_id)},
            )
        )
        .mappings()
        .all()
    )
    return FeedbackList(items=[FeedbackReport(**dict(r)) for r in rows])


@ops_router.get("/feedback", response_model=OpsFeedbackList)
async def feedback_inbox(
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
    state: Annotated[Literal["new", "seen", "resolved", "all"], Query()] = "all",
) -> OpsFeedbackList:
    """authz: staff. Every report, newest first (at most 200), with counts by state."""
    await enable_ops_console(session)
    where = "" if state == "all" else "WHERE f.status = :s"
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT f.{_COLUMNS.replace(', ', ', f.')}, f.org_id, o.name AS org_name, "
                    "f.user_id, f.reporter_email, f.handled_by FROM feedback_reports f "
                    f"LEFT JOIN organizations o ON o.id = f.org_id {where} "
                    "ORDER BY f.created_at DESC LIMIT 200"
                ),
                {"s": state},
            )
        )
        .mappings()
        .all()
    )
    counts = {
        r[0]: int(r[1])
        for r in (
            await session.execute(
                text("SELECT status, count(*) FROM feedback_reports GROUP BY status")
            )
        ).all()
    }
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="feedback.list",
        details={"state": state, "returned": len(rows)},
    )
    return OpsFeedbackList(
        items=[OpsFeedback(**dict(r)) for r in rows],
        counts={k: counts.get(k, 0) for k in ("new", "seen", "resolved")},
    )


@ops_router.post("/feedback/status", response_model=OpsFeedback)
async def set_feedback_status(
    body: FeedbackStatusUpdate,
    ctx: Annotated[RequestContext, Depends(require_staff)],
    session: Annotated[AsyncSession, Depends(ops_session)],
) -> OpsFeedback:
    """authz: staff. Mark a report new, seen or resolved."""
    await enable_ops_console(session)
    row = (
        (
            await session.execute(
                text(
                    "UPDATE feedback_reports f SET status = :s, updated_at = now(), "
                    "handled_by = :by WHERE f.id = :i "
                    f"RETURNING f.{_COLUMNS.replace(', ', ', f.')}, f.org_id, "
                    "(SELECT name FROM organizations WHERE id = f.org_id) AS org_name, "
                    "f.user_id, f.reporter_email, f.handled_by"
                ),
                {"s": body.status, "by": str(ctx.user_id), "i": str(body.id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        await ops_log_now(
            staff_user_id=ctx.user_id,
            action="feedback.status",
            details={"feedback_id": str(body.id), "outcome": "not_found"},
        )
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    await ops_log(
        session,
        staff_user_id=ctx.user_id,
        action="feedback.status",
        target_org_id=row["org_id"],
        details={"feedback_id": str(body.id), "status": body.status},
    )
    return OpsFeedback(**dict(row))
