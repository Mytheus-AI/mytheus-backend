"""Jira connector: bidirectional `action_items` sync (plan 3.11 step 3; 21 section 5).

Install: pick the Jira site (cloud id = `external_account_id`) and its first project, register a
dynamic webhook for `jira:issue_updated` in that project pointing at
`{OAUTH_REDIRECT_BASE}/webhooks/jira?cloud_id={cloud id}`, and keep the site, project and webhook
ids in the `jira_site` cursor (`expires_at` = the 30-day webhook lifetime, for the renewal job).

Outbound (`actions` consumer, `confirmed` only): if the tenant has an active Jira connection,
1. the id map already has the action -> nothing to do (a redelivered `actions` event);
2. else search Jira for label `nexus:{action_id}` -> link the existing issue (the crash window
   between creating an issue and committing the map row);
3. else create the issue: summary = content, ADF description with a back-link to
   `{APP_BASE_URL}/meetings/{meeting_id}`, duedate, assignee by email -> accountId when Jira knows
   the email, label `nexus:{action_id}`. Store key/url with `last_synced_status = confirmed`.

Inbound (webhook inbox row, `jira:issue_updated`): id map by issue key only (a `nexus:` label
is never trusted inbound: any Jira user can put one on any issue) -> re-read the issue from Jira
with the tenant's token and take the status from that read, not from the webhook body -> status
map (statusCategory `done` -> done, anything else -> confirmed) -> if it equals
`last_synced_status` drop it (loop suppression: the echo of a change NEXUS already knows about),
else call core-api `POST /internal/actions/{id}/external-status` and only then store the new
watermark. An issue that is in no id map of this tenant is ignored, whatever its labels say.
"""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError
from services.integration_service.core_client import CoreApiClient
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

PROVIDER = config.JIRA
SITE_KIND = "jira_site"
WEBHOOK_LIFETIME = timedelta(days=30)
LABEL_PREFIX = "nexus:"
_EPOCH = date(1970, 1, 1)


def label_for(action_id: uuid.UUID) -> str:
    return f"{LABEL_PREFIX}{action_id}"


def map_status(issue: dict[str, Any]) -> str:
    fields = issue.get("fields") or {}
    category = ((fields.get("status") or {}).get("statusCategory") or {}).get("key")
    return "done" if category == "done" else "confirmed"


def _site(cursor: tuple[dict[str, Any], Any] | None, conn: store.Connection) -> dict[str, Any]:
    data = dict(cursor[0]) if cursor else {}
    data.setdefault("cloud_id", conn.external_account_id)
    return data


async def post_install(
    session: AsyncSession,
    conn: store.Connection,
    clients: Clients,
    extra: dict[str, Any],
) -> None:
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    cloud_id = str(conn.external_account_id)
    project_key = await clients.jira.first_project_key(tokens.access_token, cloud_id)
    if not project_key:
        raise ProviderError(PROVIDER, "post_install", "no_project")
    ids = await clients.jira.register_webhook(
        tokens.access_token,
        cloud_id,
        url=f"{config.webhook_url(PROVIDER)}?cloud_id={cloud_id}",
        jql=f'project = "{project_key}"',
    )
    await store.put_cursor(
        session,
        conn.org_id,
        conn.id,
        SITE_KIND,
        {
            "cloud_id": cloud_id,
            "site_url": extra.get("site_url"),
            "project_key": project_key,
            "webhook_ids": ids,
        },
        datetime.now(UTC) + WEBHOOK_LIFETIME,
    )


async def renew(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    cursor = await store.get_cursor(session, conn.id, SITE_KIND)
    site = _site(cursor, conn)
    ids = [int(i) for i in site.get("webhook_ids") or []]
    if not ids:
        return
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    await clients.jira.refresh_webhooks(tokens.access_token, str(site["cloud_id"]), ids)
    await store.put_cursor(
        session, conn.org_id, conn.id, SITE_KIND, site, datetime.now(UTC) + WEBHOOK_LIFETIME
    )


async def remove_webhooks(
    session: AsyncSession, conn: store.Connection, clients: Clients, token: str
) -> None:
    cursor = await store.get_cursor(session, conn.id, SITE_KIND)
    site = _site(cursor, conn)
    ids = [int(i) for i in site.get("webhook_ids") or []]
    if ids:
        await clients.jira.delete_webhooks(token, str(site["cloud_id"]), ids)


def _due(raw: Any) -> str | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    if isinstance(raw, int):
        return (_EPOCH + timedelta(days=raw)).isoformat()
    try:
        return date.fromisoformat(str(raw)).isoformat()
    except ValueError:
        return None


def _description(content: str, meeting_url: str) -> dict[str, Any]:
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {"type": "paragraph", "content": [{"type": "text", "text": content}]},
            {
                "type": "paragraph",
                "content": [
                    {"type": "text", "text": "Confirmed in Mytheus. "},
                    {
                        "type": "text",
                        "text": "Open the meeting",
                        "marks": [{"type": "link", "attrs": {"href": meeting_url}}],
                    },
                ],
            },
        ],
    }


def _browse_url(site: dict[str, Any], key: str) -> str | None:
    base = site.get("site_url")
    return f"{str(base).rstrip('/')}/browse/{key}" if base else None


# The `actions` event's `meeting_id` for a task created directly (`POST /api/v1/actions`): the
# schema requires a uuid, and core-api sends the nil uuid when there is no meeting.
_NO_MEETING = "00000000-0000-0000-0000-000000000000"


def _source_url(meeting_id: Any) -> str:
    if not meeting_id or str(meeting_id) == _NO_MEETING:
        return f"{config.app_base_url()}/tasks"
    return f"{config.app_base_url()}/meetings/{meeting_id}"


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
    site = _site(await store.get_cursor(session, conn.id, SITE_KIND), conn)
    cloud_id = str(site["cloud_id"])
    label = label_for(action_id)

    found = await clients.jira.search_by_label(tokens.access_token, cloud_id, label)
    if found:
        issue = found[0]
        key = str(issue["key"])
        await store.put_link(
            session,
            org_id,
            PROVIDER,
            "action",
            action_id,
            key,
            _browse_url(site, key),
            map_status(issue),
        )
        return "linked"

    content = str(action.get("content") or "").strip() or "Action item"
    fields: dict[str, Any] = {
        "project": {"key": site.get("project_key")},
        "issuetype": {"name": "Task"},
        "summary": content[:250],
        "description": _description(content, _source_url(action.get("meeting_id"))),
        "labels": [label],
    }
    due = _due(action.get("due_date"))
    if due:
        fields["duedate"] = due
    assignee = action.get("assignee_id")
    if assignee:
        email = (
            await session.execute(
                text("SELECT email FROM users WHERE id = :u AND deleted_at IS NULL"),
                {"u": str(assignee)},
            )
        ).scalar()
        if email:
            account = await clients.jira.account_id_for_email(
                tokens.access_token, cloud_id, str(email)
            )
            if account:
                fields["assignee"] = {"accountId": account}
    created = await clients.jira.create_issue(tokens.access_token, cloud_id, fields)
    key = str(created["key"])
    await store.put_link(
        session, org_id, PROVIDER, "action", action_id, key, _browse_url(site, key), "confirmed"
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
    """Inbound for one `jira:issue_updated` delivery. Returns processed|skipped.

    The payload only says WHICH issue changed. The issue is looked up in this tenant's id map by
    its key (never by a `nexus:` label, which any Jira user can add to any issue), then re-read
    from Jira with the tenant's own token, and the status comes from that read."""
    if payload.get("webhookEvent") != "jira:issue_updated":
        return "skipped"
    issue = payload.get("issue")
    if not isinstance(issue, dict) or not issue.get("key"):
        return "skipped"
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if conn is None:
        return "skipped"
    key = str(issue["key"])
    link = await store.link_for_external(session, PROVIDER, "action", key)
    if link is None:
        return "skipped"  # not an issue this tenant created
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    site = _site(await store.get_cursor(session, conn.id, SITE_KIND), conn)
    fetched = await clients.jira.get_issue(tokens.access_token, str(site["cloud_id"]), key)
    if fetched is None or str(fetched.get("key")) != link.external_id:
        return "skipped"  # gone, moved, or no longer the issue we created
    status = map_status(fetched)
    if status == link.last_synced_status:
        return "skipped"  # loop suppression: NEXUS already has this status
    await core.external_status(org_id, link.nexus_id, status, key)
    await store.put_link(
        session, org_id, PROVIDER, "action", link.nexus_id, key, _browse_url(site, key), status
    )
    await store.touch_synced(session, conn.id)
    return "processed"
