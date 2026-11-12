"""Linear connector: bidirectional `action_items` sync (ADR-0044 D1 to D3; 21 section 5).

Install: the OAuth exchange names the Linear organization (`external_account_id`); post-install
picks its first team and keeps it in the `linear_team` cursor. There is no per-tenant webhook to
register: the webhook belongs to the Linear OAuth application (resource Issue), so there is
nothing to renew or delete either.

Outbound (`actions` consumer, `confirmed` only): if the tenant's tracker is Linear,
1. the id map already has the action -> nothing to do (a redelivered `actions` event);
2. else look for an issue carrying an attachment whose URL is the action's Mytheus link
   (`action_url`) -> link it (the crash window between creating an issue and committing the
   map row);
3. else create the issue: title = content, a markdown description with the back-link, due date,
   assignee by email when Linear knows the email; then attach `action_url`. Store the issue
   identifier and URL with `last_synced_status = confirmed`.

Inbound (webhook inbox row, `Issue` `update`): id map by identifier only -> re-read the issue
from Linear with the tenant's token and take the state from that read, not from the webhook
body -> `map_status` (state type `completed` -> done, anything else -> confirmed, `canceled`
included) -> if it equals `last_synced_status`, drop it (loop suppression) -> else core-api
`POST /internal/actions/{id}/external-status` with `source: linear`, then the new watermark. An
issue in no id map of this tenant is ignored.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError
from services.integration_service.connectors._tracker_common import due_iso, source_url
from services.integration_service.core_client import CoreApiClient
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

PROVIDER = config.LINEAR
TEAM_KIND = "linear_team"
MAX_TITLE = 250


def map_status(issue: dict[str, Any]) -> str:
    state_type = (issue.get("state") or {}).get("type")
    return "done" if state_type == "completed" else "confirmed"


def action_url(action_id: uuid.UUID) -> str:
    """The attachment URL that ties an issue to its action (the crash-window lookup key)."""
    return f"{config.app_base_url()}/work/tasks?action={action_id}"


async def team(session: AsyncSession, conn: store.Connection) -> dict[str, Any]:
    cursor = await store.get_cursor(session, conn.id, TEAM_KIND)
    return dict(cursor[0]) if cursor else {}


async def post_install(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    first = await clients.linear.first_team(tokens.access_token)
    if not first:
        raise ProviderError(PROVIDER, "post_install", "no_team")
    await store.put_cursor(
        session,
        conn.org_id,
        conn.id,
        TEAM_KIND,
        {"team_id": first["id"], "team_key": first.get("key")},
        None,
    )


def _description(content: str, back_link: str) -> str:
    return f"{content}\n\nConfirmed in Mytheus. [Open the meeting]({back_link})"


async def push_action(
    session: AsyncSession, org_id: uuid.UUID, action: dict[str, Any], clients: Clients
) -> str:
    """Outbound for one confirmed action. Returns created|linked|exists|not_connected."""
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if conn is None:
        return "not_connected"
    action_id = uuid.UUID(str(action["action_id"]))
    if await store.link_for_nexus(session, PROVIDER, "action", action_id) is not None:
        return "exists"
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    token = tokens.access_token
    link_url = action_url(action_id)

    found = await clients.linear.issues_for_url(token, link_url)
    if found:
        issue = found[0]
        await store.put_link(
            session,
            org_id,
            PROVIDER,
            "action",
            action_id,
            str(issue["identifier"]),
            issue.get("url"),
            map_status(issue),
        )
        return "linked"

    team_id = (await team(session, conn)).get("team_id")
    if not team_id:
        raise ProviderError(PROVIDER, "push_action", "no_team")
    content = str(action.get("content") or "").strip() or "Action item"
    assignee_id: str | None = None
    assignee = action.get("assignee_id")
    if assignee:
        email = (
            await session.execute(
                text("SELECT email FROM users WHERE id = :u AND deleted_at IS NULL"),
                {"u": str(assignee)},
            )
        ).scalar()
        if email:
            assignee_id = await clients.linear.user_id_for_email(token, str(email))
    created = await clients.linear.create_issue(
        token,
        team_id=str(team_id),
        title=content[:MAX_TITLE],
        description=_description(content, source_url(action.get("meeting_id"))),
        due_date=due_iso(action.get("due_date")),
        assignee_id=assignee_id,
    )
    await clients.linear.attach_url(
        token, issue_id=str(created["id"]), url=link_url, title="Mytheus action"
    )
    await store.put_link(
        session,
        org_id,
        PROVIDER,
        "action",
        action_id,
        str(created["identifier"]),
        created.get("url"),
        "confirmed",
    )
    await store.touch_synced(session, conn.id)
    return "created"


async def process_webhook(
    session: AsyncSession,
    org_id: uuid.UUID,
    payload: dict[str, Any],
    core: CoreApiClient,
    clients: Clients,
) -> str:
    """Inbound for one Linear `Issue` `update` delivery. Returns processed|skipped."""
    if payload.get("type") != "Issue" or payload.get("action") != "update":
        return "skipped"
    data = payload.get("data")
    if not isinstance(data, dict) or not data.get("identifier"):
        return "skipped"
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if conn is None:
        return "skipped"
    identifier = str(data["identifier"])
    link = await store.link_for_external(session, PROVIDER, "action", identifier)
    if link is None:
        return "skipped"  # not an issue this tenant created
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    fetched = await clients.linear.get_issue(tokens.access_token, identifier)
    if fetched is None or str(fetched.get("identifier")) != link.external_id:
        return "skipped"  # gone, moved to another team (new identifier), or not ours
    status = map_status(fetched)
    if status == link.last_synced_status:
        return "skipped"  # loop suppression: Mytheus already has this status
    await core.external_status(org_id, link.nexus_id, status, identifier, source=PROVIDER)
    await store.put_link(
        session,
        org_id,
        PROVIDER,
        "action",
        link.nexus_id,
        identifier,
        fetched.get("url") or link.external_url,
        status,
    )
    await store.touch_synced(session, conn.id)
    return "processed"
