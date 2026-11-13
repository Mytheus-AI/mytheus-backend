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
    status: Literal["completed"] = Field(
        default="completed",
        description="core-api applied the request (always `completed`; see `progress`).",
    )
    progress: Literal["in_progress", "completed"] = Field(
        default="completed",
        description="The erasure across every store: `completed` once each of `stores` has run.",
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


# --- superadmin console (ADR-0046) -------------------------------------------------------------


class WeekCount(BaseModel):
    week: str  # ISO date of the week's Monday
    count: int


class DayCount(BaseModel):
    day: str
    count: int


class PlanCount(BaseModel):
    plan: str
    count: int


class TokenLeader(BaseModel):
    org_id: uuid.UUID
    name: str
    tokens: int
    token_quota: int | None


class OverviewTotals(BaseModel):
    tenants: int
    active: int
    suspended: int
    members: int
    meetings_7d: int
    meetings_total: int
    tokens_period: int


class Overview(BaseModel):
    period: str
    totals: OverviewTotals
    signups_by_week: list[WeekCount]
    meetings_by_week: list[WeekCount]
    plans: list[PlanCount]
    token_leaders: list[TokenLeader]
    ops_activity_by_day: list[DayCount]


class TenantTarget(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID


class Residency(BaseModel):
    region: str
    db_cell: str
    qdrant_region: str
    object_bucket_region: str
    locked_at: datetime | None


class GrantView(BaseModel):
    id: uuid.UUID
    staff_user_id: uuid.UUID
    scope: str
    expires_at: datetime
    revoked_at: datetime | None
    created_at: datetime
    active: bool


class SupportSessionView(BaseModel):
    id: uuid.UUID
    staff_user_id: uuid.UUID
    user_id: uuid.UUID
    user_email: str | None = None
    reason: str
    started_at: datetime
    expires_at: datetime
    ended_at: datetime | None
    end_reason: str | None
    open: bool


class SupportSessionList(BaseModel):
    items: list[SupportSessionView]


class MigrationView(BaseModel):
    id: uuid.UUID
    from_region: str
    from_cell: str
    to_region: str
    to_cell: str
    status: str
    reason: str
    requested_by: uuid.UUID
    created_at: datetime
    updated_at: datetime


class OpsAction(BaseModel):
    id: uuid.UUID
    staff_user_id: uuid.UUID | None
    action: str
    target_org_id: uuid.UUID | None
    details: dict[str, Any]
    created_at: datetime


class TenantDetail(BaseModel):
    org_id: uuid.UUID
    name: str
    status: str
    plan: str | None
    created_at: datetime
    residency: Residency | None
    members: int
    meetings_total: int
    meetings_by_week: list[WeekCount]
    tokens_period: int
    token_quota: int | None
    period: str
    flags: list[Flag]
    grants: list[GrantView]
    sessions: list[SupportSessionView]
    migrations: list[MigrationView]
    recent_ops: list[OpsAction]


class ServiceProbe(BaseModel):
    service: str
    status: Literal["up", "down"]
    latency_ms: int | None
    detail: str | None = None


class OutboxTenant(BaseModel):
    org_id: uuid.UUID
    name: str
    pending: int
    oldest_pending_at: datetime | None
    max_attempts: int


class Health(BaseModel):
    checked_at: datetime
    services: list[ServiceProbe]
    outbox_pending: int
    outbox_oldest_pending_at: datetime | None
    outbox_max_attempts: int
    outbox_tenants: list[OutboxTenant]
    dlq: list[DlqCount] | None = None
    consumer_lag: list[ConsumerLag] | None = None


class StaffMember(BaseModel):
    user_id: uuid.UUID
    name: str | None = None
    email: str | None
    org_id: uuid.UUID | None
    org_name: str | None
    last_seen_at: datetime | None
    actions_30d: int


class StaffReport(BaseModel):
    staff: list[StaffMember]
    activity_by_day: list[DayCount]
    recent: list[OpsAction]


class MigrateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    to_region: Literal["us", "eu", "ap"]
    to_cell: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{1,40}$")
    reason: str = Field(min_length=5, max_length=500)


class MigrationCancel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    migration_id: uuid.UUID


class SupportMember(BaseModel):
    user_id: uuid.UUID
    name: str | None = None
    email: str
    tier: str | None
    is_staff: bool


class SupportMemberList(BaseModel):
    items: list[SupportMember]
    grant_expires_at: datetime


class SupportSessionStart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    user_id: uuid.UUID
    reason: str = Field(min_length=5, max_length=500)
    minutes: int = Field(default=30, ge=15, le=60)


class SupportSessionStarted(BaseModel):
    session: SupportSessionView
    sign_in_url: str


class SupportSessionEnd(BaseModel):
    model_config = ConfigDict(extra="forbid")

    org_id: uuid.UUID
    session_id: uuid.UUID
