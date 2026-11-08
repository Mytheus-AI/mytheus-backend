"""`POST /billing/internal/budget`: provision and publish a tenant's budget for the agent tier.

The agent tier gates every LLM call on the Redis mirror `nexus:{org_id}:budget:{period}`
(Plan 7-9 section 3.2 step 9). The mirror is missing in two ordinary cases: a tenant's first
metered call ever, and the first call of every month (the key expires at period end). The plan
says a miss "reads through to Postgres (fail-closed)", but `agent-runtime-service` writes no
Postgres and cannot create the period's `agent_budgets` row, so a pure read would leave a new
tenant with no budget and therefore blocked forever. This route is the read-through: it creates
the row if needed (the same lazy provisioning `GET /usage` does), writes the mirror, and
returns the snapshot. `billing-metering-service` stays the only writer of its tables.

Service-to-service, not user, authentication, using the same header scheme as the pii vault
(`services/identity_tenant_service/pii_vault/internal_auth.py`): `X-Mytheus-Service`,
`X-Mytheus-Service-Token`, and the tenant in `X-Mytheus-Org`. Secrets come from
`MYTHEUS_BILLING_CALLER_TOKENS` (`service=secret,...`, at least 32 characters each); the only
allowed callers are the two `usage-events` producers. Every negative case refuses: an
unconfigured service never answers, so the agent tier's fallback is "block", never "allow".
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from libs.shared.cache import get_redis
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.tenancy import set_rls
from services.billing_metering_service.budgets import (
    current_budget,
    enforcement_mode,
    period_of,
    write_mirror,
)
from services.identity_tenant_service.pii_vault.internal_auth import (
    ORG_HEADER,
    authenticate_service,
)

ALLOWED_CALLERS = frozenset({"agent-runtime-service", "rag-service"})

router = APIRouter(tags=["billing-internal"])

service_token_scheme = APIKeyHeader(
    name="X-Mytheus-Service-Token",
    scheme_name="MytheusServiceToken",
    description=(
        "INTERNAL ONLY. Per-caller shared secret from MYTHEUS_BILLING_CALLER_TOKENS, sent with "
        "`X-Mytheus-Service` (caller name) and `X-Mytheus-Org` (tenant)."
    ),
    auto_error=False,
)


def parse_caller_tokens(raw: str) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, sep, secret = entry.partition("=")
        name, secret = name.strip(), secret.strip()
        if not sep or not name or not secret:
            raise ValueError("MYTHEUS_BILLING_CALLER_TOKENS entries must be service=secret")
        if name not in ALLOWED_CALLERS:
            raise ValueError(f"{name!r} is not an allowed billing caller")
        if len(secret) < 32:
            raise ValueError(f"the secret for {name!r} must be at least 32 characters")
        out[name] = secret.encode("utf-8")
    return out


class BudgetMirror(BaseModel):
    period: str
    token_quota: int
    tokens_consumed: int
    hard_cap: int
    state: str
    enforcement: str


def _unauthenticated() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Service authentication required")


async def billing_caller(
    request: Request,
    _token: Annotated[str | None, Security(service_token_scheme)] = None,
) -> uuid.UUID:
    try:
        tokens = parse_caller_tokens(os.environ.get("MYTHEUS_BILLING_CALLER_TOKENS", ""))
    except ValueError as exc:
        raise ProblemDetail(
            503, ErrorCode.DEPENDENCY_UNAVAILABLE, "billing caller config invalid"
        ) from exc
    if not tokens:
        raise _unauthenticated()
    authenticate_service(request.headers, tokens)
    try:
        org_id = uuid.UUID(request.headers.get(ORG_HEADER, ""))
    except ValueError as exc:
        raise _unauthenticated() from exc
    async with get_sessionmaker()() as session, session.begin():
        resolved = await resolve_org_by_id(session, org_id)
    if resolved is None:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    return resolved


@router.post("/billing/internal/budget", response_model=BudgetMirror)
async def refresh_budget(org_id: Annotated[uuid.UUID, Depends(billing_caller)]) -> BudgetMirror:
    period = period_of(datetime.now(UTC))
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        snap = await current_budget(session, org_id, period)
    await write_mirror(get_redis(), snap)
    return BudgetMirror(
        period=snap.period,
        token_quota=snap.token_quota,
        tokens_consumed=snap.tokens_consumed,
        hard_cap=snap.hard_cap,
        state=snap.state,
        enforcement=enforcement_mode(),
    )
