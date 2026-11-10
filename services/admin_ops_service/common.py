"""Shared plumbing for admin-ops-service: identity calls, `admin-events`, staff, `ops_audit_log`.

Identity calls. The tenant-admin member and settings routes do not write `rbac_grants` or
`tenant_settings` (identity-tenant-service owns both). They forward the caller's own bearer
token to identity's `/rbac/grants/{user_id}` and `/tenant-settings`, so identity re-verifies the
JWT and runs its own rules (`check_grant_change`: no escalation, no peer edits, last owner kept)
and its own audit rows; nothing is duplicated here. Suspend/reinstate calls identity's internal
`POST /internal/tenant-status` with this service's caller token
(`admin-ops-service=<secret>` in `MYTHEUS_IDENTITY_CALLER_TOKENS`). Base URL:
`UPSTREAM_IDENTITY_TENANT_SERVICE` (default `http://identity-tenant-service:8000`, the gateway's
convention).

Staff. `MYTHEUS_STAFF_USER_IDS` is a comma-separated list of internal `users.id` values, read on
every request. Unset or empty means nobody is staff (deny by default). A staff member signs in
with their own Clerk session in their own org; the token is verified and resolved exactly like
any tenant request (`tenant_context`), then the resolved `users.id` is checked against the list.
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Annotated, Any

import httpx
from fastapi import Depends, Request
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, tenant_context
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls

SERVICE_NAME = "admin-ops-service"
STAFF_ENV = "MYTHEUS_STAFF_USER_IDS"
IDENTITY_TOKENS_ENV = "MYTHEUS_IDENTITY_CALLER_TOKENS"
IDENTITY_URL_ENV = "UPSTREAM_IDENTITY_TENANT_SERVICE"
CORE_URL_ENV = "CORE_API_URL"


# --- identity --------------------------------------------------------------------------------


def identity_transport() -> httpx.AsyncBaseTransport | None:
    """FastAPI dependency: the transport identity calls use. None = real network. Tests
    override it with an in-process transport to the real identity app."""
    return None


IdentityTransport = Annotated[httpx.AsyncBaseTransport | None, Depends(identity_transport)]


def core_transport() -> httpx.AsyncBaseTransport | None:
    """FastAPI dependency: the transport core-api calls use (the erasure leg). None = real
    network; tests override it with an in-process transport to the real core-api routers."""
    return None


CoreTransport = Annotated[httpx.AsyncBaseTransport | None, Depends(core_transport)]


def _core_base() -> str:
    return os.environ.get(CORE_URL_ENV, "http://core-api:8000").rstrip("/")


def _identity_base() -> str:
    return os.environ.get(IDENTITY_URL_ENV, "http://identity-tenant-service:8000").rstrip("/")


def own_identity_secret() -> str | None:
    for entry in os.environ.get(IDENTITY_TOKENS_ENV, "").split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


def _passthrough(response: httpx.Response) -> ProblemDetail:
    """The upstream's RFC 7807 answer, re-raised with the same status, code, detail and
    `reason` extension (identity's and core-api's share the one error model)."""
    try:
        body = response.json()
    except ValueError:
        body = {}
    code = body.get("code") if isinstance(body, dict) else None
    try:
        error_code = ErrorCode(code) if code else None
    except ValueError:
        error_code = None
    if response.status_code >= 500 or error_code is None:
        return ProblemDetail(
            503 if response.status_code >= 500 else response.status_code,
            error_code or ErrorCode.DEPENDENCY_UNAVAILABLE,
            "identity-tenant-service refused or failed the request",
        )
    detail = body.get("detail") if isinstance(body.get("detail"), str) else error_code.value
    reason = body.get("reason")
    extra = {"reason": reason} if isinstance(reason, str) else None
    return ProblemDetail(response.status_code, error_code, detail, extra=extra)


async def forward_to_identity(
    request: Request,
    method: str,
    path: str,
    transport: httpx.AsyncBaseTransport | None,
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    """Call identity as the CALLER (their bearer token), raising identity's own error."""
    headers = {}
    if auth := request.headers.get("authorization"):
        headers["Authorization"] = auth
    if fwd := request.headers.get("x-forwarded-for"):
        headers["X-Forwarded-For"] = fwd
    try:
        async with httpx.AsyncClient(transport=transport, timeout=15.0) as client:
            response = await client.request(
                method, f"{_identity_base()}{path}", headers=headers, json=json_body
            )
    except httpx.HTTPError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "identity-tenant-service unreachable"
        ) from exc
    if response.status_code >= 300:
        raise _passthrough(response)
    return response


async def forward_to_core(
    request: Request,
    method: str,
    path: str,
    transport: httpx.AsyncBaseTransport | None,
    json_body: dict[str, Any] | None = None,
) -> httpx.Response:
    """Call core-api as the CALLER (their bearer token), like `forward_to_identity`: core-api
    re-verifies the token and applies its own tier check; its 4xx comes back unchanged."""
    headers = {}
    if auth := request.headers.get("authorization"):
        headers["Authorization"] = auth
    if fwd := request.headers.get("x-forwarded-for"):
        headers["X-Forwarded-For"] = fwd
    try:
        async with httpx.AsyncClient(transport=transport, timeout=30.0) as client:
            response = await client.request(
                method, f"{_core_base()}{path}", headers=headers, json=json_body
            )
    except httpx.HTTPError as exc:
        raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "core-api unreachable") from exc
    if response.status_code >= 300:
        raise _passthrough(response)
    return response


async def set_tenant_status(
    org_id: uuid.UUID,
    status: str,
    *,
    reason: str | None,
    staff_user_id: uuid.UUID | None,
    transport: httpx.AsyncBaseTransport | None,
) -> dict[str, Any]:
    """identity `POST /internal/tenant-status` for `org_id`; returns its result body."""
    secret = own_identity_secret()
    if not secret:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            f"{IDENTITY_TOKENS_ENV} has no {SERVICE_NAME} entry",
        )
    headers = {
        "X-Mytheus-Service": SERVICE_NAME,
        "X-Mytheus-Service-Token": secret,
        "X-Mytheus-Org": str(org_id),
    }
    body = {
        "status": status,
        "reason": reason,
        "staff_user_id": str(staff_user_id) if staff_user_id else None,
    }
    try:
        async with httpx.AsyncClient(transport=transport, timeout=15.0) as client:
            response = await client.post(
                f"{_identity_base()}/internal/tenant-status", headers=headers, json=body
            )
    except httpx.HTTPError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "identity-tenant-service unreachable"
        ) from exc
    if response.status_code == 404:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    if response.status_code >= 300:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "identity-tenant-service refused the change"
        )
    result: dict[str, Any] = response.json()
    return result


# --- admin-events ----------------------------------------------------------------------------


async def emit_admin_event(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    event_type: str,
    actor_id: uuid.UUID | None,
    target_type: str,
    target_id: str,
    detail: dict[str, Any],
) -> int:
    """One `admin-events` outbox row (the frozen `AdminEvent` shape). Binds `app.current_org`
    to `org_id` first: the outbox is tenant-policied."""
    await set_rls(session, org_id)
    return await enqueue(
        session,
        EventEnvelope(
            topic="admin-events",
            org_id=str(org_id),
            producer=SERVICE_NAME,
            idempotency_key=f"admin:{event_type}:{target_type}:{target_id}:{uuid.uuid4()}",
            payload={
                "event_type": event_type,
                "actor_id": str(actor_id) if actor_id else "",
                "target_type": target_type,
                "target_id": target_id,
                "detail_json": json.dumps(detail, sort_keys=True, default=str),
            },
        ),
    )


# --- staff and ops_audit_log -----------------------------------------------------------------


def staff_user_ids() -> frozenset[uuid.UUID]:
    out: set[uuid.UUID] = set()
    for raw in os.environ.get(STAFF_ENV, "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            out.add(uuid.UUID(raw))
        except ValueError:
            continue  # a malformed entry grants nothing
    return frozenset(out)


def is_staff(ctx: RequestContext) -> bool:
    return ctx.user_id is not None and ctx.user_id in staff_user_ids()


_OPS_LOG = text(
    "INSERT INTO ops_audit_log (staff_user_id, action, target_org_id, details) "
    "VALUES (:s, :a, :t, CAST(:d AS jsonb))"
)


async def ops_log(
    session: AsyncSession,
    *,
    staff_user_id: uuid.UUID | None,
    action: str,
    target_org_id: uuid.UUID | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Append one `ops_audit_log` row inside the caller's transaction (fail closed: the route's
    work and its log commit together or not at all)."""
    await session.execute(
        _OPS_LOG,
        {
            "s": str(staff_user_id) if staff_user_id else None,
            "a": action,
            "t": str(target_org_id) if target_org_id else None,
            "d": json.dumps(details or {}, sort_keys=True, default=str),
        },
    )


async def ops_log_now(**kwargs: Any) -> None:
    """`ops_log` in its own committed transaction: for denials and failed calls, whose request
    transaction is about to be discarded."""
    async with get_sessionmaker()() as session, session.begin():
        await ops_log(session, **kwargs)


async def require_staff(
    request: Request, ctx: Annotated[RequestContext, Depends(tenant_context)]
) -> RequestContext:
    """Deny by default: 403 unless the caller's `users.id` is in `MYTHEUS_STAFF_USER_IDS`. The
    denial itself is logged in `ops_audit_log`."""
    if is_staff(ctx):
        return ctx
    await ops_log_now(
        staff_user_id=ctx.user_id,
        action="ops.denied",
        details={
            "method": request.method,
            "path": request.url.path,
            "caller_org_id": str(ctx.org_id),
            "clerk_user_id": ctx.clerk_user_id,
        },
    )
    raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Mytheus staff only")


StaffContext = Annotated[RequestContext, Depends(require_staff)]


async def ops_session() -> Any:
    """A session in one transaction with `app.ops_console` NOT set and no tenant bound; routes
    bind a tenant (`set_rls`) or the console GUC explicitly, per statement group."""
    async with get_sessionmaker()() as session, session.begin():
        yield session


async def enable_ops_console(session: AsyncSession) -> None:
    """Transaction-local, like `app.current_org`: unlocks the `ops_console` policy on
    `feature_flags` for this transaction only."""
    await session.execute(text("SELECT set_config('app.ops_console', 'on', true)"))
