"""`POST /internal/jira/issue-status` (workflow-service): are these actions' tracker issues open?

The path keeps its name; since ADR-0044 it answers for the tenant's tracker, Jira or Linear,
and says which in `provider`.

Polish wave 3, promise drift. The daily `PromiseDriftWorkflow` hands over the ids of overdue
open actions; this answers, for each one linked to a Jira issue this tenant created (the
`integration_id_map`, provider `jira`, kind `action`), whether the issue is still open. The
status is re-read from Jira with the tenant's own token (the webhook watermark can lag or miss
a delivery); if Jira cannot be reached for an issue, the stored `last_synced_status` answers
instead. `statusCategory` `done` is closed, anything else open (the connector's `map_status`).

`{connected: false, issues: []}` when the tenant has no active Jira connection (the "not
connected" state: the workflow then nudges nobody). Never a 5xx for a provider problem.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.integration_service import store, tracker
from services.integration_service.clients import Clients, get_clients
from services.integration_service.internal_auth import workflow_caller
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["integration-internal"])

MAX_ACTIONS = 200


class IssueStatusRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action_ids: list[uuid.UUID] = Field(max_length=MAX_ACTIONS)


class IssueStatus(BaseModel):
    action_id: uuid.UUID
    issue_key: str
    issue_url: str | None = None
    open: bool
    source: str  # "jira" | "linear" (re-read now) | "watermark" (last synced status)


class IssueStatusResult(BaseModel):
    connected: bool
    # Which tracker answered (ADR-0044): None when the tenant has none connected.
    provider: Literal["jira", "linear"] | None = None
    issues: list[IssueStatus]


async def issue_statuses(
    org_id: uuid.UUID, action_ids: list[uuid.UUID], clients: Clients | None = None
) -> IssueStatusResult:
    clients = clients or get_clients()
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        found = await tracker.active(session)
        if found is None:
            return IssueStatusResult(connected=False, issues=[])
        provider, conn = found
        if not action_ids:
            return IssueStatusResult(connected=True, provider=provider, issues=[])
        links = (
            await session.execute(
                text(
                    "SELECT nexus_id, external_id, external_url, last_synced_status "
                    "FROM integration_id_map WHERE provider = :p AND nexus_kind = 'action' "
                    "AND nexus_id = ANY(CAST(:ids AS uuid[]))"
                ),
                {"p": provider, "ids": [str(a) for a in action_ids]},
            )
        ).all()
        token: str | None = None
        ctx: dict[str, Any] = {}
        if links:
            try:
                tokens = await store.access_token(session, conn, clients.oauth[provider])
                token = tokens.access_token
                ctx = await tracker.context(session, provider, conn)
            except Exception:  # noqa: BLE001 - no usable token: the watermark answers
                logger.warning("%s token unavailable; answering from the watermark", provider)
    out: list[IssueStatus] = []
    for nexus_id, key, url, watermark in links:
        open_, source = watermark != "done", "watermark"
        if token:
            try:
                fetched = await tracker.read_issue(provider, clients, token, ctx, str(key))
                if fetched is not None:
                    open_, source = tracker.map_status(provider, fetched) != "done", provider
            except Exception:  # noqa: BLE001 - a provider hiccup falls back to the watermark
                logger.warning("%s re-read failed for one issue; using the watermark", provider)
        out.append(
            IssueStatus(
                action_id=nexus_id, issue_key=str(key), issue_url=url, open=open_, source=source
            )
        )
    return IssueStatusResult(connected=True, provider=provider, issues=out)


@router.post("/internal/jira/issue-status", response_model=IssueStatusResult)
async def issue_status(
    body: IssueStatusRequest,
    org_id: Annotated[uuid.UUID, Depends(workflow_caller)],
) -> IssueStatusResult:
    return await issue_statuses(org_id, list(dict.fromkeys(body.action_ids)))
