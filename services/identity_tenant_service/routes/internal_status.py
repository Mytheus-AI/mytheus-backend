"""`POST /internal/tenant-status`: suspend or reinstate a tenant (P12, plan 3.13).

The ops console's suspend/reinstate lands here. identity-tenant-service owns `organizations`
and is the sole `tenant-events` producer, so the status flip and its event happen here and
nowhere else; admin-ops-service records the staff action (`ops_audit_log`, `admin-events`) and
calls this route.

- The tenant is the `X-Mytheus-Org` header, never a path or body field, the same as every other
  internal hop. Unlike the other internal routes it is NOT resolved through
  `resolve_org_by_id`, which refuses a suspended tenant: reinstating one has to reach it.
- The flip goes through `set_organization_status()` (SECURITY DEFINER, migration e2b6c4a8f913),
  because `mytheus_app` holds no UPDATE on any `organizations` column by design.
- On an actual change: an `audit_logs` row in the target tenant (`actor_type=system`, the
  staff user in metadata), and for a suspension a `tenant-events` `suspended` outbox row, all in
  the same transaction. `TenantEventType` has no reinstated symbol and this band may not change
  a schema, so a reinstatement emits no `tenant-events` record (recorded deviation).
- Repeating a call is a no-op (`changed: false`, nothing audited or emitted).

Auth: `X-Mytheus-Service` + `X-Mytheus-Service-Token` against `MYTHEUS_IDENTITY_CALLER_TOKENS`
(`admin-ops-service=<secret>`, 32+ characters). Unconfigured means every caller is refused. Not
routed by api-gateway (no `/internal` prefix in its map).
"""

from __future__ import annotations

import hmac
import os
import uuid
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.audit import audit
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls

TOKENS_ENV = "MYTHEUS_IDENTITY_CALLER_TOKENS"
ALLOWED_CALLERS = frozenset({"admin-ops-service"})
PRODUCER = "identity-tenant-service"
ACTION_TENANT_SUSPENDED = "tenant.suspended"
ACTION_TENANT_REINSTATED = "tenant.reinstated"

router = APIRouter(tags=["identity-internal"])

service_token_scheme = APIKeyHeader(
    name="X-Mytheus-Service-Token",
    scheme_name="MytheusServiceToken",
    description=(
        "INTERNAL ONLY. Per-caller shared secret from MYTHEUS_IDENTITY_CALLER_TOKENS, sent with "
        "`X-Mytheus-Service` (caller name) and `X-Mytheus-Org` (target tenant)."
    ),
    auto_error=False,
)


class TenantStatusChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["active", "suspended"]
    reason: str | None = Field(default=None, max_length=500)
    staff_user_id: uuid.UUID | None = None


class TenantStatusResult(BaseModel):
    org_id: uuid.UUID
    status: Literal["active", "suspended"]
    previous: Literal["active", "suspended"]
    changed: bool


def _parse_tokens(raw: str) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError(f"{TOKENS_ENV} entries must be service=secret")
        if name not in ALLOWED_CALLERS:
            raise ValueError(f"{name!r} is not an allowed identity caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


async def identity_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    """An allowed service with its secret; returns the target tenant from `X-Mytheus-Org`."""
    try:
        tokens = _parse_tokens(os.environ.get(TOKENS_ENV, ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "identity caller config invalid"
        ) from exc
    unauthenticated = ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")
    if not tokens:
        raise unauthenticated
    service = request.headers.get("x-mytheus-service", "")
    presented = request.headers.get("x-mytheus-service-token", "").encode("utf-8")
    expected = tokens.get(service)
    if not hmac.compare_digest(presented, expected or b"\x00" * 32) or expected is None:
        raise unauthenticated
    try:
        return uuid.UUID(request.headers.get("x-mytheus-org", ""))
    except ValueError as exc:
        raise unauthenticated from exc


_PLAN_CODE = text(
    "SELECT p.code FROM subscriptions s JOIN plans p ON p.id = s.plan_id "
    "WHERE s.org_id = :o LIMIT 1"
)


@router.post("/internal/tenant-status", response_model=TenantStatusResult)
async def change_tenant_status(
    body: TenantStatusChange,
    org_id: Annotated[uuid.UUID, Depends(identity_caller)],
) -> TenantStatusResult:
    async with get_sessionmaker()() as session, session.begin():
        previous = (
            await session.execute(
                text("SELECT set_organization_status(:o, :s)"),
                {"o": str(org_id), "s": body.status},
            )
        ).scalar_one()
        if previous is None:
            raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
        changed = previous != body.status
        if changed:
            await set_rls(session, org_id)
            suspended = body.status == "suspended"
            await audit(
                session,
                org_id=org_id,
                actor_type="system",
                action=ACTION_TENANT_SUSPENDED if suspended else ACTION_TENANT_REINSTATED,
                target_type="organization",
                target_id=org_id,
                metadata={
                    "reason": body.reason,
                    "staff_user_id": str(body.staff_user_id) if body.staff_user_id else None,
                    "via": "admin-ops-service",
                },
            )
            if suspended:
                slug = (
                    await session.execute(
                        text("SELECT slug FROM organizations WHERE id = :o"), {"o": str(org_id)}
                    )
                ).scalar_one()
                plan = (await session.execute(_PLAN_CODE, {"o": str(org_id)})).scalar()
                await enqueue(
                    session,
                    EventEnvelope(
                        topic="tenant-events",
                        org_id=str(org_id),
                        producer=PRODUCER,
                        idempotency_key=f"tenant:{org_id}:suspended:{uuid.uuid4()}",
                        payload={
                            "event_type": "suspended",
                            "org_slug": slug,
                            "plan_code": plan or "free",
                        },
                    ),
                )
    return TenantStatusResult(org_id=org_id, status=body.status, previous=previous, changed=changed)
