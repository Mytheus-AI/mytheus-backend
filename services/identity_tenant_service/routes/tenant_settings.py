"""Tenant compliance settings: the write path for `tenant_settings.pii_redaction_enabled`.

Closes the gap recorded in Plan 5-6 (`work-done-till-now/04-...` open gap 8): the AI tier's
redaction gate reads a real Redis key and defaults to `False`, honestly, because **nothing in
the program ever wrote that key**. `ingestion-service` shipped a working redaction layer that
no tenant could ever turn on.

Two things about this route are load-bearing rather than incidental.

**It writes Postgres AND the Redis key the reader actually reads.** `ingestion-service`
(`services/ingestion_service/pii.py`) does not query `tenant_settings`; it cannot, because
`mytheus-intelligence` is forbidden from joining into this repo's tables (see
[ADR-0019](../../../../Mytheus-docs/reprise-docs/adr/0019-ai-tier-verifies-jwt-and-reads-resolved-org-claim.md)
D3 for the same boundary on the sync path). It reads
`nexus:{org_id}:tenant_settings:pii_redaction_enabled` and treats `b"1"`/`b"true"` as on.
A write that updated only the column would leave the gate exactly as dead as it was.

**The two stores do not fail the same way, so the ordering is not arbitrary.** Redis stale-ON
while Postgres says OFF over-redacts: text is replaced that need not have been, which is
recoverable and visible. Redis stale-OFF while Postgres says ON *under*-redacts: PII reaches
the vector store and the bus in the clear, which is neither. So the dangerous direction is
precisely "the operator turned redaction on and the mirror did not get it", and this route
refuses to report success in that case. The mirror write is the LAST statement inside the same
transaction as the column write and its audit row, so a mirror failure raises, the column
write rolls back with it, and the caller gets a `503` naming the state rather than a `200`
that would be a lie. Either both stores move or neither does. The retry is safe because the
whole operation is idempotent in the value written.

**Upsert, not update.** `provision_organization()` creates an `organizations` row and a
`tenant-events` outbox row; it does **not** create a `tenant_settings` row. An `UPDATE` would
therefore silently affect zero rows for every organization provisioned to date. The upsert
fills the row from the column defaults on first write, so this route works for an org whether
or not one exists. That provisioning omission is a separate gap and is recorded as one.

Scope (P12 widened it for the tenant-admin console, which forwards here rather than writing
`tenant_settings` itself, so this stays the single writer): `pii_redaction_enabled`, the two
retention windows, and `agent_confidence_overrides` (shape `{"<Agent>": {"surface": x,
"silent": y}}`, ADR-0030 D3, both in 0..1 and `silent <= surface`). Every field is optional in
a PATCH; at least one must be present. The Redis mirror is written only when
`pii_redaction_enabled` is part of the change. `consent_required` is deliberately NOT editable
here -
[ADR-0018](../../../../Mytheus-docs/reprise-docs/adr/0018-consent-entity-behind-flag.md) makes
flipping it a legal decision with a token-mint `428` consequence, so it wants its own route and
its own audit action rather than riding along in a general settings PATCH.
"""

from __future__ import annotations

import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.exceptions import RedisError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, require_tier, tenant_session
from libs.shared.cache import get_redis, redis_key
from libs.shared.errors import ErrorCode, ProblemDetail

router = APIRouter(tags=["tenant-settings"])

ACTION_TENANT_SETTINGS_UPDATED = "tenant_settings.updated"

# The exact key `ingestion-service` reads. Built through `redis_key` so it carries the frozen
# `nexus:{org_id}:` prefix and so an absent org_id raises rather than writing a tenant-less key.
_PII_KEY_PARTS = ("tenant_settings", "pii_redaction_enabled")

# `pii.py` accepts b"1"/b"true". "1"/"0" is written here rather than "true"/"false" because the
# reader's false branch is "anything not in that set", so an explicit "0" and a missing key are
# equivalent to it, and "0" additionally distinguishes "explicitly off" from "never set" for a
# human reading Redis during an incident.
_ON, _OFF = "1", "0"


class ConfidenceThresholds(BaseModel):
    model_config = ConfigDict(extra="forbid")

    surface: float = Field(ge=0, le=1)
    silent: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def _silent_not_above_surface(self) -> ConfidenceThresholds:
        if self.silent > self.surface:
            raise ValueError("silent must not exceed surface")
        return self


class TenantSettings(BaseModel):
    pii_redaction_enabled: bool
    retention_days_transcripts: int = 365
    retention_days_worklogs: int = 365
    agent_confidence_overrides: dict[str, ConfidenceThresholds] = Field(default_factory=dict)


class TenantSettingsUpdate(BaseModel):
    pii_redaction_enabled: bool | None = Field(
        default=None,
        description="When true, ingestion redacts detected PII before the chunk reaches "
        "Postgres, the object store, or the bus.",
    )
    retention_days_transcripts: int | None = Field(default=None, ge=1, le=3650)
    retention_days_worklogs: int | None = Field(default=None, ge=1, le=3650)
    agent_confidence_overrides: dict[str, ConfidenceThresholds] | None = Field(
        default=None, description="Replaces the whole overrides object."
    )


_SETTINGS_COLUMNS = (
    "pii_redaction_enabled, retention_days_transcripts, retention_days_worklogs, "
    "agent_confidence_overrides"
)


def _row_to_settings(row: Any) -> TenantSettings:
    return TenantSettings(
        pii_redaction_enabled=row["pii_redaction_enabled"],
        retention_days_transcripts=row["retention_days_transcripts"],
        retention_days_worklogs=row["retention_days_worklogs"],
        agent_confidence_overrides=_valid_overrides(row["agent_confidence_overrides"]),
    )


def _valid_overrides(raw: Any) -> dict[str, ConfidenceThresholds]:
    """Stored overrides that parse; anything malformed is left out of the read (core-api's
    `confidence.thresholds` falls back to the defaults for those the same way)."""
    out: dict[str, ConfidenceThresholds] = {}
    if isinstance(raw, dict):
        for agent, entry in raw.items():
            try:
                out[str(agent)] = ConfidenceThresholds.model_validate(entry)
            except ValueError:
                continue
    return out


async def _publish_pii_mirror(org_id: UUID, enabled: bool) -> None:
    """Publish the committed value to the key the AI tier reads. Raises on failure.

    Deliberately NOT best-effort. See the module docstring: a silent failure here is the
    under-redaction direction, and an operator who saw `200` would reasonably believe redaction
    was on for every subsequent ingest.
    """
    key = redis_key(org_id, *_PII_KEY_PARTS)
    try:
        await get_redis().set(key, _ON if enabled else _OFF)
    except RedisError as exc:
        raise ProblemDetail(
            503,
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "Setting was saved but could not be propagated to the redaction cache. It is NOT "
            "yet in effect for ingestion. Retry this request; it is idempotent.",
        ) from exc


@router.get("/tenant-settings", response_model=TenantSettings)
async def read_tenant_settings(
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> TenantSettings:
    """The caller's own tenant's settings. `org_id` comes from the verified token, never a path
    parameter, so there is no shape in which this route can be asked about another tenant."""
    row = (
        (
            await session.execute(
                text(f"SELECT {_SETTINGS_COLUMNS} FROM tenant_settings WHERE org_id = :o"),
                {"o": str(ctx.org_id)},
            )
        )
        .mappings()
        .first()
    )
    # No row is not an error: it means this org predates any settings write. Report the column
    # defaults rather than 404, so a reader sees what the database would apply.
    if row is None:
        return TenantSettings(pii_redaction_enabled=True)
    return _row_to_settings(row)


@router.patch("/tenant-settings", response_model=TenantSettings)
async def update_tenant_settings(
    body: TenantSettingsUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("admin"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> TenantSettings:
    fields = body.model_dump(exclude_unset=True, exclude_none=True)
    if not fields:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "No setting to change")
    params: dict[str, Any] = {"o": str(ctx.org_id)}
    columns: list[str] = []
    values: list[str] = []
    for column, value in fields.items():
        columns.append(column)
        if column == "agent_confidence_overrides":
            values.append(f"CAST(:{column} AS jsonb)")
            params[column] = json.dumps(value)
        else:
            values.append(f":{column}")
            params[column] = value

    # Upsert: see the module docstring on why UPDATE would be a no-op for every org that
    # exists today. Every other column takes its declared default on insert.
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns)
    row = (
        (
            await session.execute(
                text(
                    f"INSERT INTO tenant_settings (org_id, {', '.join(columns)}) "
                    f"VALUES (:o, {', '.join(values)}) "
                    f"ON CONFLICT (org_id) DO UPDATE SET {updates}, updated_at = now() "
                    f"RETURNING {_SETTINGS_COLUMNS}"
                ),
                params,
            )
        )
        .mappings()
        .one()
    )

    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_TENANT_SETTINGS_UPDATED,
        target_type="tenant_settings",
        target_id=ctx.org_id,
        metadata=fields,
        ip=ctx.ip,
    )

    # LAST statement inside the transaction, on purpose. `tenant_session` yields inside
    # `session.begin()`, so this route body runs in one transaction that commits when it
    # returns. Raising here therefore rolls the column write back, and the two stores stay
    # consistent: either both move or neither does. Publishing after the commit instead would
    # need a hand-managed session, and would trade this for a window where the column has moved
    # and the mirror has not - the under-redaction direction.
    #
    # One residual window remains and is disclosed rather than papered over: if the mirror
    # write succeeds and the COMMIT then fails (a connection loss at exactly that point), the
    # mirror is ahead of the column. On enable that is Redis-ON/column-OFF, which over-redacts
    # and is the harmless direction. On disable it is Redis-OFF/column-ON, which under-redacts
    # until the next successful write. Closing that last window needs the value to travel as a
    # `tenant-events` message with a consumer in `mytheus-intelligence`, which does not exist
    # yet; when it does, this mirror becomes an optimization rather than the delivery path.
    if "pii_redaction_enabled" in fields:
        await _publish_pii_mirror(ctx.org_id, row["pii_redaction_enabled"])

    return _row_to_settings(row)
