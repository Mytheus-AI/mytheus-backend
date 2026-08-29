"""The vault's mint/resolve API - ADR-0020 D2's "callers, never writers" made concrete.

Two routes, both internal-only and neither exposed through `api-gateway`:

    POST /internal/pii-tokens/mint      values  -> opaque tokens   (idempotent)
    POST /internal/pii-tokens/resolve   tokens  -> raw values      (audited disclosure)

They are batch-shaped because their callers are: a redaction pass holds every span it detected
in one transcript final and wants one round trip, not one per entity. A per-entity endpoint on
the path that persists a final would put an HTTP call inside a loop inside a p95 < 2 s caption
SLO.

THE ASYMMETRY BETWEEN THE TWO ROUTES IS THE WHOLE SECURITY MODEL

`mint` takes PII in and gives opaque labels back. It reveals nothing: a caller who already
holds the value learns only a token for it. It is safe to call often, from a machine, with no
user present.

`resolve` takes labels in and gives PII back. Every successful call is a disclosure of personal
data, so it is audited unconditionally, in the same transaction, before the values are read -
per ADR-0020's consequence list and `libs/shared/audit.py`'s fail-closed rule. There is no
best-effort mode and no `?skip_audit`. If the audit write fails, the disclosure does not happen.

WHY THE AUDIT ROW IS WRITTEN BEFORE THE DECRYPT AND NOT AFTER

Both orders commit together inside `tenant_session`, so at first glance it makes no difference.
It does under failure: writing first means a decrypt that throws still leaves the *attempt*
recorded, and an attempted disclosure is precisely the thing an auditor asks about. The row
records the tokens requested, which is exactly the right granularity - it identifies what was
asked for without copying the raw values it returned into a second table that then also needs
protecting.

MINTING IS REFUSED UNDER A DEVELOPMENT KEY PROVIDER

ADR-0024 § The production condition. `LocalKeyProvider` keeps the root key in this process's
environment, on the same host as the ciphertext. ADR-0020's objection is that a vault with
ad-hoc key management is *strictly worse* than the irreversible redaction shipping today, and
that objection is correct, so minting refuses unless `PII_VAULT_ALLOW_LOCAL_KEK` explicitly
acknowledges the deployment. The refusal is a `503`, which the redaction boundaries already
treat as "vault unavailable, use the irreversible placeholder" - so the degraded path is the
status quo and never anything weaker than it.

`resolve` is NOT gated the same way. A tenant whose values were minted under an acknowledged
local provider must still be able to read them back, and refusing would strand data rather
than protect it.
"""

from __future__ import annotations

import os
from uuid import UUID

from fastapi import Depends, FastAPI
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import tenant_session_for_org
from libs.shared.crypto import KeyProvider, RootKeyUnavailable, get_key_provider
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.internal_auth import require_internal_service
from services.identity_tenant_service.vault import (
    MintRequest,
    VaultKeysUnavailable,
    mint,
    resolve,
)

ACTION_PII_TOKENS_RESOLVED = "pii_tokens.resolved"

ALLOW_LOCAL_KEK_ENV = "PII_VAULT_ALLOW_LOCAL_KEK"

# ADR-0020 D3's typed vocabulary, mirrored by `ck_pii_tokens_entity_type_valid`. Validated here
# too so a bad type is a 422 naming the field rather than a 500 from a CHECK violation.
ENTITY_TYPES = frozenset({"EMAIL", "PHONE", "IBAN", "CARD", "GOVID", "PERSON", "ORG", "LOC"})

# One transcript final holds a bounded number of entities; a request far past this is a caller
# batching a whole meeting, which would hold the request transaction open across thousands of
# encrypt calls and block the tenant's other mints behind it.
_MAX_BATCH = 256


class MintItem(BaseModel):
    entity_type: str = Field(description="One of ADR-0020 D3's typed entity classes.")
    value: str = Field(min_length=1, description="The raw detected value. Never logged.")


class MintBody(BaseModel):
    org_id: UUID = Field(
        description="The tenant to mint in. Supplied by the calling service, which holds no "
        "user session on this path - see libs/shared/internal_auth.py on what that does and "
        "does not authorize."
    )
    items: list[MintItem] = Field(min_length=1, max_length=_MAX_BATCH)


class MintedItem(BaseModel):
    entity_type: str
    token: str


class MintResponse(BaseModel):
    tokens: list[MintedItem] = Field(
        description="One per request item, IN THE SAME ORDER. Callers splice these back into "
        "the character spans they detected, so order is load-bearing."
    )


class ResolveBody(BaseModel):
    org_id: UUID
    tokens: list[str] = Field(min_length=1, max_length=_MAX_BATCH)
    actor_id: UUID | None = Field(
        default=None,
        description="The user on whose behalf this disclosure happens, when there is one. "
        "Recorded in audit_logs. Null for a system-initiated resolve.",
    )


class ResolveResponse(BaseModel):
    values: dict[str, str] = Field(
        description="token -> raw value. Unknown tokens are OMITTED rather than erroring, so "
        "this endpoint is not an oracle for which tokens exist in a tenant."
    )


def _provider() -> KeyProvider:
    try:
        return get_key_provider()
    except RootKeyUnavailable as exc:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "The PII vault has no key custody configured in this deployment",
        ) from exc


def create_pii_tokens_app() -> FastAPI:
    """Mounted at `/internal`, matching the AI tier's convention for internal-only surfaces.

    A sub-application rather than a prefixed router so the internal surface is absent from the
    parent app's OpenAPI document entirely: ADR-0022 emits that document and `spectral lint`
    gates it, and an internal route appearing in the public contract is how a client SDK
    eventually generates a method for it.
    """
    app = FastAPI(title="identity-tenant-service-pii-tokens")

    @app.post(
        "/pii-tokens/mint",
        response_model=MintResponse,
        dependencies=[Depends(require_internal_service)],
    )
    async def mint_tokens(body: MintBody) -> MintResponse:
        provider = _provider()
        if not provider.is_production_grade and not os.environ.get(ALLOW_LOCAL_KEK_ENV):
            # See the module docstring. Not a 403: from the caller's side this is exactly the
            # "vault unavailable" condition it already knows how to degrade from, and giving it
            # a different code would make it treat a deployment posture as a bug in itself.
            raise ProblemDetail(
                503,
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "The PII vault is configured with a development-only key provider; minting is "
                f"refused until {ALLOW_LOCAL_KEK_ENV} acknowledges it (ADR-0024)",
            )

        bad = sorted({i.entity_type for i in body.items} - ENTITY_TYPES)
        if bad:
            raise ProblemDetail(
                422,
                ErrorCode.VALIDATION_FAILED,
                f"unknown entity_type(s): {', '.join(bad)}",
            )

        async with tenant_session_for_org(body.org_id) as session:
            try:
                minted = await mint(
                    session,
                    body.org_id,
                    [MintRequest(entity_type=i.entity_type, value=i.value) for i in body.items],
                    provider,
                )
            except VaultKeysUnavailable as exc:
                raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, str(exc)) from exc

        return MintResponse(
            tokens=[MintedItem(entity_type=m.entity_type, token=m.token) for m in minted]
        )

    @app.post(
        "/pii-tokens/resolve",
        response_model=ResolveResponse,
        dependencies=[Depends(require_internal_service)],
    )
    async def resolve_tokens(body: ResolveBody) -> ResolveResponse:
        provider = _provider()
        async with tenant_session_for_org(body.org_id) as session:
            await _audit_disclosure(session, body)
            values = await resolve(session, body.org_id, body.tokens, provider)
        return ResolveResponse(values=values)

    return app


async def _audit_disclosure(session: AsyncSession, body: ResolveBody) -> None:
    """The disclosure record, written before anything is decrypted.

    `actor_type` is `user` when the caller named one and `system` otherwise, rather than always
    `system`: an audit trail that cannot distinguish "a person read this customer's email" from
    "a batch job did" cannot answer the question an auditor actually asks, which is the exact
    reason `libs/shared/audit.py` carries the three-value vocabulary at all.

    The requested tokens go in `metadata`; the resolved VALUES never do. Copying decrypted PII
    into `audit_logs` would defeat the vault by putting plaintext in a second table - one that
    is deliberately append-only and retained for a year.
    """
    await audit(
        session,
        org_id=body.org_id,
        action=ACTION_PII_TOKENS_RESOLVED,
        actor_id=body.actor_id,
        actor_type="user" if body.actor_id else "system",
        target_type="pii_tokens",
        metadata={"tokens": sorted(set(body.tokens)), "count": len(set(body.tokens))},
    )
