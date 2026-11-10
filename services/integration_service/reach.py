"""integration-service Phase 4 "reach" routes: the CRM meeting note and decision record exports.

Internal (caller token, `internal_auth.py`; not routed by api-gateway):

- `POST /internal/crm/meeting-note` {meeting_id} (workflow-service, when a meeting ends) ->
  {status, companies, contacts, note_id}. `status` is one of:
  `not_connected` (HubSpot not configured on the server or not connected by the tenant),
  `skipped_confidential` (a confidential meeting never reaches a CRM), `no_match` (no company or
  contact in HubSpot matches anyone external), `created` or `updated` (the one note per
  meeting; a replay updates it). See `connectors/hubspot.py`.

Public (Clerk session, member tier, through the gateway's `/integrations` prefix):

- `GET /integrations/exports/destinations?provider=notion|confluence|slack`: where a decision
  record can go for this tenant (Notion pages shared with the integration, Confluence spaces,
  Slack direct message), plus whether the provider is configured and connected, so the UI can
  say "not connected" instead of failing.
- `POST /integrations/exports/decisions/{decision_id}` {provider, parent_page_id | space_key |
  channel}: export the decision record as the caller may see it. 404 when they cannot see or
  share it (a confidential meeting they do not host included); 409 `PROVIDER_NOT_CONFIGURED`
  with `reason` `not_configured` or `not_connected`; 409 `VALIDATION_FAILED` with a `slack_*`
  reason the person can fix (invite the app to the channel, no Slack user with their email);
  502 `DEPENDENCY_UNAVAILABLE` (`export_failed`) when the provider fails. Audited as
  `decision.exported`. Idempotency-Key honoured (the app factory's middleware).
- `GET /integrations/exports/decisions/{decision_id}`: where this decision has been exported.

Exporting is a person's explicit action on a record they can see, so it is a member-tier route;
connecting the tool stays owner/admin (`routes.py`). The Slack message is posted by this
service with the tenant's bot token because it is an export the person asked for, not a
notification (notification-service stays the sole fan-out for `notifications`).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import text

from libs.shared.audit import ACTION_DECISION_EXPORTED, audit
from libs.shared.authz import RequestContext, not_found, require_tier
from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.clients import ProviderError, get_clients
from services.integration_service.connectors import decision_export, hubspot
from services.integration_service.core_client import CoreApiError, get_core
from services.integration_service.internal_auth import workflow_caller
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

internal_router = APIRouter(tags=["integration-internal"])
router = APIRouter(prefix="/integrations/exports", tags=["integration-exports"])

ExportProvider = Literal["notion", "confluence", "slack"]


# --- CRM meeting note (internal) ---------------------------------------------------------------


class MeetingRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    meeting_id: uuid.UUID


class CrmNoteResult(BaseModel):
    status: Literal["not_connected", "skipped_confidential", "no_match", "created", "updated"]
    companies: int = 0
    contacts: int = 0
    note_id: str | None = None


@internal_router.post("/internal/crm/meeting-note", response_model=CrmNoteResult)
async def crm_meeting_note(
    body: MeetingRef,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> CrmNoteResult:
    if not config.is_configured(config.HUBSPOT):
        return CrmNoteResult(status="not_connected")
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        if await store.get_connection(session, config.HUBSPOT, active_only=True) is None:
            return CrmNoteResult(status="not_connected")
    try:
        digest = await get_core().crm_digest(org_id, body.meeting_id)
    except CoreApiError as exc:
        if exc.status == 404 and exc.problem_reason == "confidential":
            return CrmNoteResult(status="skipped_confidential")
        if exc.status == 404:
            raise not_found() from None
        raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "core-api unavailable") from exc
    clients = get_clients()
    try:
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, org_id)
            conn = await store.get_connection(session, config.HUBSPOT, active_only=True)
            if conn is None:
                return CrmNoteResult(status="not_connected")
            result = await hubspot.push_meeting_note(session, org_id, conn, digest, clients)
    except store.NotConnected:
        return CrmNoteResult(status="not_connected")
    except ProviderError as exc:
        logger.warning("hubspot note for %s failed: %s", body.meeting_id, exc)
        raise ProblemDetail(
            502, ErrorCode.DEPENDENCY_UNAVAILABLE, "HubSpot did not accept the note"
        ) from None
    return CrmNoteResult(
        status=result.status,
        companies=result.companies,
        contacts=result.contacts,
        note_id=result.note_id,
    )


# --- decision record exports (public) ----------------------------------------------------------


class ExportDestination(BaseModel):
    id: str = Field(
        description="Pass back as `parent_page_id` (Notion) or `space_key` (Confluence); "
        "`dm` for Slack means a direct message to you."
    )
    name: str
    kind: Literal["page", "space", "dm"]
    url: str | None = None


class ExportDestinations(BaseModel):
    provider: ExportProvider
    configured: bool = Field(description="The server has app credentials for this provider.")
    connected: bool = Field(description="The workspace has a live connection to it.")
    destinations: list[ExportDestination]
    note: str | None = Field(
        default=None, description="Guidance for the person, e.g. how to share a Notion page."
    )


class DecisionExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: ExportProvider
    parent_page_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
        pattern=r"^[0-9a-fA-F-]+$",
        description="Notion: the page the export is created under (required for Notion).",
    )
    space_key: str | None = Field(
        default=None,
        min_length=1,
        max_length=255,
        pattern=r"^[A-Za-z0-9~_\-]+$",
        description="Confluence: the space the page is created in (required for Confluence).",
    )
    channel: str | None = Field(
        default=None,
        pattern=r"^[CG][A-Z0-9]{2,30}$",
        description="Slack: a channel id the Mytheus app is in. Omit for a direct message.",
    )

    @model_validator(mode="after")
    def _destination(self) -> DecisionExportRequest:
        if self.provider == "notion" and not self.parent_page_id:
            raise ValueError("parent_page_id is required for a Notion export")
        if self.provider == "confluence" and not self.space_key:
            raise ValueError("space_key is required for a Confluence export")
        return self


class DecisionExport(BaseModel):
    decision_id: uuid.UUID
    provider: ExportProvider
    external_id: str = Field(description="The page id, or `{channel}:{ts}` for Slack.")
    external_url: str | None = Field(default=None, description="Open the export in the tool.")
    destination: str = Field(description="The parent page, space key, channel id or `dm`.")
    replaced: bool = Field(description="An earlier export of this decision was replaced.")
    exported_at: datetime


class DecisionExportSummary(BaseModel):
    provider: ExportProvider
    external_url: str | None = None
    exported_at: datetime


class DecisionExportList(BaseModel):
    items: list[DecisionExportSummary]


def _not_configured() -> ProblemDetail:
    return ProblemDetail(
        409,
        ErrorCode.PROVIDER_NOT_CONFIGURED,
        "This tool is not configured on this server",
        extra={"reason": "not_configured"},
    )


def _not_connected() -> ProblemDetail:
    return ProblemDetail(
        409,
        ErrorCode.PROVIDER_NOT_CONFIGURED,
        "This tool is not connected to your workspace. An owner or admin can connect it.",
        extra={"reason": "not_connected"},
    )


def _failed() -> ProblemDetail:
    return ProblemDetail(
        502,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "The tool did not accept the export",
        extra={"reason": "export_failed"},
    )


async def _record(ctx: RequestContext, decision_id: uuid.UUID) -> dict[str, object]:
    if ctx.user_id is None:
        raise not_found()
    try:
        return await get_core().decision_record(ctx.org_id, decision_id, ctx.user_id)
    except CoreApiError as exc:
        if exc.status == 404:
            raise not_found() from None
        raise ProblemDetail(503, ErrorCode.DEPENDENCY_UNAVAILABLE, "core-api unavailable") from exc


@router.get("/destinations", response_model=ExportDestinations)
async def export_destinations(
    provider: Annotated[ExportProvider, Query(description="Where the record would go.")],
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> ExportDestinations:
    configured = config.is_configured(provider)
    empty = ExportDestinations(
        provider=provider, configured=configured, connected=False, destinations=[]
    )
    if not configured:
        return empty
    clients = get_clients()
    try:
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, ctx.org_id)
            conn = await store.get_connection(session, provider, active_only=True)
            if conn is None:
                return empty
            tokens = await store.access_token(session, conn, clients.oauth[provider])
            if provider == "notion":
                pages = await clients.notion.search_pages(tokens.access_token)
                return ExportDestinations(
                    provider=provider,
                    configured=True,
                    connected=True,
                    destinations=[
                        ExportDestination(id=p["id"], name=p["title"], kind="page", url=p["url"])
                        for p in pages
                    ],
                    note="Only pages shared with the Mytheus integration appear here. In Notion, "
                    "open a page, choose Connections, and add Mytheus.",
                )
            if provider == "confluence":
                spaces = await clients.confluence.spaces(
                    tokens.access_token, str(conn.external_account_id)
                )
                return ExportDestinations(
                    provider=provider,
                    configured=True,
                    connected=True,
                    destinations=[
                        ExportDestination(id=s["key"], name=s["name"], kind="space") for s in spaces
                    ],
                )
    except store.NotConnected:
        return empty
    except ProviderError as exc:
        logger.warning("export destinations for %s failed: %s", provider, exc)
        raise _failed() from None
    return ExportDestinations(
        provider=provider,
        configured=True,
        connected=True,
        destinations=[ExportDestination(id="dm", name="Direct message to you", kind="dm")],
        note="To post in a channel instead, invite the Mytheus app to it and give its channel "
        "id (it starts with C).",
    )


@router.post("/decisions/{decision_id}", response_model=DecisionExport)
async def export_decision(
    decision_id: uuid.UUID,
    body: DecisionExportRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> DecisionExport:
    provider = body.provider
    if not config.is_configured(provider):
        raise _not_configured()
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        if await store.get_connection(session, provider, active_only=True) is None:
            raise _not_connected()
    record = await _record(ctx, decision_id)
    clients = get_clients()
    try:
        async with get_sessionmaker()() as session, session.begin():
            await set_rls(session, ctx.org_id)
            conn = await store.get_connection(session, provider, active_only=True)
            if conn is None:
                raise _not_connected()
            if provider == "notion":
                exported = await decision_export.export_notion(
                    session,
                    ctx.org_id,
                    conn,
                    record,
                    clients,
                    parent_page_id=str(body.parent_page_id),
                )
            elif provider == "confluence":
                exported = await decision_export.export_confluence(
                    session, ctx.org_id, conn, record, clients, space_key=str(body.space_key)
                )
            else:
                email = (
                    await session.execute(
                        text("SELECT email FROM users WHERE id = :u AND deleted_at IS NULL"),
                        {"u": str(ctx.user_id)},
                    )
                ).scalar()
                exported = await decision_export.export_slack(
                    session,
                    ctx.org_id,
                    conn,
                    record,
                    clients,
                    channel=body.channel,
                    caller_email=str(email) if email else None,
                )
            await audit(
                session,
                org_id=ctx.org_id,
                actor_id=ctx.user_id,
                action=ACTION_DECISION_EXPORTED,
                target_type="decision",
                target_id=decision_id,
                metadata={
                    "provider": provider,
                    "destination": exported.destination,
                    "replaced": exported.replaced,
                },
                ip=ctx.ip,
            )
    except store.NotConnected:
        raise _not_connected() from None
    except decision_export.ExportRefused as exc:
        raise ProblemDetail(
            409, ErrorCode.VALIDATION_FAILED, exc.detail, extra={"reason": exc.reason}
        ) from None
    except ProviderError as exc:
        logger.warning("decision export to %s failed: %s", provider, exc)
        raise _failed() from None
    return DecisionExport(
        decision_id=decision_id,
        provider=provider,
        external_id=exported.external_id,
        external_url=exported.external_url,
        destination=exported.destination,
        replaced=exported.replaced,
        exported_at=datetime.now(UTC),
    )


@router.get("/decisions/{decision_id}", response_model=DecisionExportList)
async def list_decision_exports(
    decision_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> DecisionExportList:
    await _record(ctx, decision_id)  # 404 unless the caller may see and share it
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, ctx.org_id)
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT provider, external_url, updated_at FROM integration_id_map "
                        "WHERE nexus_kind = 'decision' AND nexus_id = :d "
                        "AND provider = ANY(:p) ORDER BY updated_at DESC"
                    ),
                    {"d": str(decision_id), "p": list(config.EXPORT_PROVIDERS)},
                )
            )
            .mappings()
            .all()
        )
    return DecisionExportList(
        items=[
            DecisionExportSummary(
                provider=r["provider"], external_url=r["external_url"], exported_at=r["updated_at"]
            )
            for r in rows
        ]
    )
