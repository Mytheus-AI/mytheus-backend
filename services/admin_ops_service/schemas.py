"""Request and response models for `/admin/tenant/*` and `/admin/ops/*` (P12, plan 3.13)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

Tier = Literal["owner", "admin", "member"]

# --- tenant admin ------------------------------------------------------------------------------


class Member(BaseModel):
    user_id: uuid.UUID
    email: str
    tier: Tier | None
    created_at: datetime


class MemberList(BaseModel):
    items: list[Member]


class MemberTierUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tier: Tier


class SupportGrant(BaseModel):
    id: uuid.UUID
    staff_user_id: uuid.UUID
    scope: Literal["read_only", "impersonate"]
    granted_by: uuid.UUID
    expires_at: datetime
    revoked_at: datetime | None
    created_at: datetime


class SupportGrantList(BaseModel):
    items: list[SupportGrant]


class SupportGrantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    staff_user_id: uuid.UUID
    scope: Literal["read_only", "impersonate"] = "read_only"
    expires_at: datetime = Field(description="In the future and at most 30 days out.")


class DataExport(BaseModel):
    id: uuid.UUID
    status: Literal["pending", "completed"]
    requested_at: datetime
    manifest_url: str | None = None


class DataExportList(BaseModel):
    items: list[DataExport]


# --- internal ops ------------------------------------------------------------------------------


class Whoami(BaseModel):
    staff: bool


class TenantSummary(BaseModel):
    org_id: uuid.UUID
    name: str
    plan: str | None
    status: Literal["active", "suspended"]
    member_count: int
    created_at: datetime


class TenantList(BaseModel):
    items: list[TenantSummary]
    next_cursor: str | None = None


class SuspendRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    reason: str = Field(min_length=1, max_length=500)


class ReinstateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    reason: str | None = Field(default=None, max_length=500)


class TenantStatus(BaseModel):
    org_id: uuid.UUID
    status: Literal["active", "suspended"]


class Flag(BaseModel):
    id: uuid.UUID
    key: str
    org_id: uuid.UUID | None
    enabled: bool
    rules: dict[str, Any]
    updated_by: uuid.UUID | None
    updated_at: datetime


class FlagList(BaseModel):
    items: list[Flag]


class FlagCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    key: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,99}$")
    org_id: uuid.UUID | None = Field(default=None, description="null = a global flag")
    enabled: bool = False
    rules: dict[str, Any] = Field(default_factory=dict)


class FlagUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool | None = None
    rules: dict[str, Any] | None = None


class TenantCost(BaseModel):
    org_id: uuid.UUID
    name: str
    tokens_consumed: int
    token_quota: int | None


class DlqCount(BaseModel):
    topic: str
    count: int


class ConsumerLag(BaseModel):
    group: str
    topic: str
    lag: int


class CostReport(BaseModel):
    period: str
    tenants: list[TenantCost]
    dlq: list[DlqCount] | None = None
    consumer_lag: list[ConsumerLag] | None = None
    next_cursor: str | None = None


class AuditSearch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=200)


class AuditEntry(BaseModel):
    id: uuid.UUID
    org_id: uuid.UUID
    actor_id: uuid.UUID | None
    actor_type: str
    action: str
    target_type: str | None
    target_id: str | None
    metadata: dict[str, Any]
    occurred_at: datetime


class AuditPage(BaseModel):
    items: list[AuditEntry]
    next_cursor: str | None
