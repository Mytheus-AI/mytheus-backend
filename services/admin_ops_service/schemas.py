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


ErasureKind = Literal["person", "meeting"]


class ErasureCreate(BaseModel):
    """Erase one person's, or one meeting's, data in this workspace."""

    model_config = ConfigDict(extra="forbid")

    kind: ErasureKind
    target_id: uuid.UUID = Field(description="The person's `users.id`, or the meeting's id.")
    confirm: str = Field(
        min_length=1,
        max_length=320,
        description=(
            "Typed confirmation: the person's email, or the meeting's title (`Untitled meeting` "
            "when it has none). Case and outer spaces are ignored."
        ),
    )


ErasureStoreName = Literal[
    "core_api", "transcript_chunks", "vectors", "search_index", "knowledge_graph"
]


class ErasureStore(BaseModel):
    """One store's share of an erasure. `core_api` finishes inside the request; the other four
    report back on `admin-events` `erasure_completed` (one `data_erasure.store_completed` audit
    row each), so they are `pending` until their service has run."""

    store: ErasureStoreName
    status: Literal["pending", "completed"]
    completed_at: datetime | None = None
    counts: dict[str, int] = Field(default_factory=dict)


class Erasure(BaseModel):
    """One erasure, as recorded in `audit_logs` (`data_erasure.completed` is the request; each
    store's finish is a `data_erasure.store_completed` row)."""

    id: uuid.UUID
    kind: ErasureKind
    target_id: uuid.UUID
    label: str | None = Field(
        default=None,
        description="The person's email or the meeting's title; null when no longer readable.",
    )
    status: Literal["in_progress", "completed"] = Field(
        default="completed",
        description="`completed` once every store in `stores` has finished.",
    )
    stores: list[ErasureStore] = Field(
        default_factory=list,
        description=(
            "Per-store progress: `core_api` (finished with the request), then "
            "`transcript_chunks`, `vectors`, `search_index` and `knowledge_graph`."
        ),
    )
    requested_by: uuid.UUID | None = None
    requested_at: datetime
    erased: dict[str, int] = Field(default_factory=dict)
    anonymized: dict[str, int] = Field(default_factory=dict)
    revoked_share_links: int = 0
    revoked_tokens: int = 0


class ErasureList(BaseModel):
    items: list[Erasure]


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
