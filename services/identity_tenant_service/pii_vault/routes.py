"""The vault's REST surface. Two routes, two different trust models.

`POST /pii-tokens/mint` - INTERNAL. Called by the redaction boundaries (ADR-0020 D2) with a
batch of detected spans; returns one token per span, in order. Authenticated as a service
(`internal_auth.py`), tenant from `X-Mytheus-Org`. Must never be routed by `api-gateway`.
Mint is idempotent by construction (deterministic token, UNIQUE on the blind index), so a
retried call returns the same tokens and needs no `Idempotency-Key`.

`POST /pii-tokens/resolve` - USER-FACING, RBAC-gated and audited. Reversal is "in-region,
in-tenant, for authorized members" (ADR-0020); the caller's Clerk JWT goes through the full
`libs/shared/authz` chain, and every token requested writes an `audit_logs` row in the same
transaction as the read. `RESOLVE_MIN_TIER` is `admin`: DECIDED by the lead (2026-11-06),
members may not resolve (ADR-0024). POST rather than GET so tokens never land in access logs as
query strings; `Cache-Control: no-store` on the answer.

Mounted in `services/identity_tenant_service/main.py`; the committed OpenAPI document is
regenerated from it (ADR-0022). `api-gateway`'s ROUTE_MAP has no `/pii-tokens` entry, and
`tests/test_gateway_edge.py` asserts it never will.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from functools import lru_cache
from typing import Annotated

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.crypto import BlindIndex, EnvelopeCipher, key_provider_from_env
from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.identity_tenant_service.pii_vault.internal_auth import (
    InternalCaller,
    internal_caller,
)
from services.identity_tenant_service.pii_vault.normalize import MAX_VALUE_CHARS, EntityType
from services.identity_tenant_service.pii_vault.vault import PiiVault

router = APIRouter(tags=["pii-vault"])

RESOLVE_MIN_TIER = "admin"
MAX_MINT_SPANS = 256
MAX_RESOLVE_TOKENS = 100


@lru_cache(maxsize=1)
def get_vault() -> PiiVault:
    """Built once per process. Raises `KeyConfigurationError` if custody is not configured,
    so a misconfigured deployment fails on the first vault call, never silently."""
    provider = key_provider_from_env()
    return PiiVault(EnvelopeCipher(provider), BlindIndex(provider))


async def internal_tenant_session(
    caller: Annotated[InternalCaller, Depends(internal_caller)],
) -> AsyncIterator[AsyncSession]:
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, caller.org_id)
        yield session


class Span(BaseModel):
    entity_type: EntityType
    value: str = Field(min_length=1, max_length=MAX_VALUE_CHARS)


class MintRequest(BaseModel):
    spans: list[Span] = Field(min_length=1, max_length=MAX_MINT_SPANS)


class MintedSpan(BaseModel):
    entity_type: EntityType
    token: str
    placeholder: str


class MintResponse(BaseModel):
    tokens: list[MintedSpan]


class ResolveRequest(BaseModel):
    tokens: list[str] = Field(min_length=1, max_length=MAX_RESOLVE_TOKENS)
    reason: str | None = Field(default=None, max_length=200)


class ResolvedValue(BaseModel):
    token: str
    entity_type: EntityType
    value: str


class ResolveResponse(BaseModel):
    values: list[ResolvedValue]
    unresolved: list[str]


@router.post("/pii-tokens/mint", response_model=MintResponse)
async def mint_tokens(
    body: MintRequest,
    caller: Annotated[InternalCaller, Depends(internal_caller)],
    session: Annotated[AsyncSession, Depends(internal_tenant_session)],
    vault: Annotated[PiiVault, Depends(get_vault)],
) -> MintResponse:
    minted = await vault.tokenize_many(
        session, caller.org_id, [(s.entity_type, s.value) for s in body.spans]
    )
    return MintResponse(
        tokens=[
            MintedSpan(entity_type=m.entity_type, token=m.token, placeholder=m.placeholder)
            for m in minted
        ]
    )


@router.post("/pii-tokens/resolve", response_model=ResolveResponse)
async def resolve_tokens(
    body: ResolveRequest,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier(RESOLVE_MIN_TIER))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    vault: Annotated[PiiVault, Depends(get_vault)],
) -> ResolveResponse:
    response.headers["Cache-Control"] = "no-store"
    resolved, unresolved = await vault.detokenize(
        session, ctx.org_id, body.tokens, actor_id=ctx.user_id, ip=ctx.ip, reason=body.reason
    )
    return ResolveResponse(
        values=[
            ResolvedValue(token=r.token, entity_type=r.entity_type, value=r.value) for r in resolved
        ],
        unresolved=unresolved,
    )
