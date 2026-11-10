"""Decision record exports: one confirmed decision, with its lineage, to Notion, Confluence or
Slack (Phase 4 "shareable decision records").

The record is core-api's (`GET /internal/decisions/{id}/record?viewer=`, built by
`services/core_api/records.py`) and is exactly what the exporting person may share: confirmed
facts only, a lineage step from a meeting they were not in kept as a placeholder with its
content withheld, people by display name. This module only renders it for each tool and
writes it there with the tenant's connection.

Each export links back into Mytheus (`{APP_BASE_URL}/decisions/{id}` for the decision and for
every visible lineage step), so the exported page is a pointer into the decision graph, not a
fork of it.

Re-export. `integration_id_map` (kind `decision`) keeps one external copy per provider:

- Notion: the previous page is archived (best effort) and a fresh page is created under the
  requested parent, because Notion has no "replace a page's content" call.
- Confluence: the same page gets a new version when the requested space is the one it lives
  in; another space gets a new page and the map moves to it. The map's `external_id` is
  `{space_key}:{page_id}` so the space is known without a provider read.
- Slack: a new message (a message is a moment, not a document); the map points at the latest.

Install proofs: Notion reads the bot user (`users/me`); Confluence lists spaces on the chosen
site and keeps the site URL in the `confluence_site` cursor to build page links.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from html import escape as esc
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError

NEXUS_KIND = "decision"
CONFLUENCE_SITE_KIND = "confluence_site"
MAX_TEXT = 1900


class ExportRefused(Exception):
    """A request the provider refused for a reason the caller can fix (409)."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Exported:
    external_id: str
    external_url: str | None
    destination: str
    replaced: bool


# --- install proofs ---------------------------------------------------------------------------


async def notion_post_install(
    session: AsyncSession, conn: store.Connection, clients: Clients
) -> None:
    tokens = await store.access_token(session, conn, clients.oauth[config.NOTION])
    await clients.notion.me(tokens.access_token)


async def confluence_post_install(
    session: AsyncSession, conn: store.Connection, clients: Clients, extra: dict[str, Any]
) -> None:
    tokens = await store.access_token(session, conn, clients.oauth[config.CONFLUENCE])
    await clients.confluence.spaces(tokens.access_token, str(conn.external_account_id))
    await store.put_cursor(
        session,
        conn.org_id,
        conn.id,
        CONFLUENCE_SITE_KIND,
        {"cloud_id": conn.external_account_id, "site_url": extra.get("site_url")},
    )


# --- rendering --------------------------------------------------------------------------------


def decision_url(decision_id: Any) -> str:
    return f"{config.app_base_url()}/decisions/{decision_id}"


def _clip(text: str, limit: int = MAX_TEXT) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _date(raw: Any) -> str:
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).strftime("%d %b %Y")
    except ValueError:
        return str(raw)


def title_for(record: dict[str, Any]) -> str:
    return _clip(f"Decision: {record.get('content') or ''}".strip(), 180)


def _status_line(record: dict[str, Any]) -> str:
    status = "Superseded" if record.get("status") == "superseded" else "Confirmed"
    bits = [status, f"decided {_date(record.get('decided_at'))}".strip()]
    bits.append(f"in {record.get('meeting_title') or 'a meeting'}")
    if record.get("confirmed_by_name"):
        bits.append(f"confirmed by {record['confirmed_by_name']}")
    return " · ".join(b for b in bits if b)


def lineage_lines(record: dict[str, Any]) -> list[tuple[str, str | None]]:
    """(text, link) per lineage step other than the subject, oldest first."""
    out: list[tuple[str, str | None]] = []
    for step in record.get("lineage") or []:
        relation = step.get("relation")
        if relation == "subject":
            continue
        label = "Earlier" if relation == "earlier" else "Later"
        if not step.get("visible"):
            where = "from a meeting you were not in"
            out.append((f"{label}: a decision {where} (content withheld)", None))
            continue
        meta = ", ".join(b for b in (step.get("meeting_title"), _date(step.get("decided_at"))) if b)
        text = f"{label}: {step.get('content') or ''}" + (f" ({meta})" if meta else "")
        out.append((_clip(text, 500), decision_url(step.get("decision_id"))))
    return out


def action_lines(record: dict[str, Any]) -> list[str]:
    lines = []
    for a in (record.get("meeting_actions") or [])[:25]:
        bits = [str(a.get("content") or "")]
        if a.get("owner_name"):
            bits.append(f"owner {a['owner_name']}")
        if a.get("due_date"):
            bits.append(f"due {a['due_date']}")
        if a.get("status") == "done":
            bits.append("done")
        head, *rest = bits
        lines.append(_clip(head + (f" ({', '.join(rest)})" if rest else ""), 500))
    return lines


def _rich(content: str, link: str | None = None) -> list[dict[str, Any]]:
    text: dict[str, Any] = {"content": _clip(content)}
    if link:
        text["link"] = {"url": link}
    return [{"type": "text", "text": text}]


def notion_blocks(record: dict[str, Any]) -> list[dict[str, Any]]:
    def block(kind: str, content: str, link: str | None = None) -> dict[str, Any]:
        return {"object": "block", "type": kind, kind: {"rich_text": _rich(content, link)}}

    blocks = [
        block("heading_2", "Decision"),
        block("paragraph", str(record.get("content") or "")),
        block("paragraph", _status_line(record)),
    ]
    if record.get("rationale"):
        blocks.append(block("paragraph", f"Rationale: {record['rationale']}"))
    lineage = lineage_lines(record)
    if lineage:
        blocks.append(block("heading_3", "Lineage"))
        blocks.extend(block("bulleted_list_item", t, link) for t, link in lineage)
    actions = action_lines(record)
    if actions:
        blocks.append(block("heading_3", "Actions from the same meeting"))
        blocks.extend(block("bulleted_list_item", a) for a in actions)
    blocks.append(
        block("paragraph", "Open this decision in Mytheus", decision_url(record["decision_id"]))
    )
    return blocks


def confluence_storage(record: dict[str, Any]) -> str:
    parts = [
        f"<h2>{esc(str(record.get('content') or ''))}</h2>",
        f"<p><em>{esc(_status_line(record))}</em></p>",
    ]
    if record.get("rationale"):
        parts.append(f"<p><strong>Rationale:</strong> {esc(str(record['rationale']))}</p>")
    lineage = lineage_lines(record)
    if lineage:
        parts.append("<h3>Lineage</h3><ul>")
        for text, link in lineage:
            item = f'<a href="{esc(link, quote=True)}">{esc(text)}</a>' if link else esc(text)
            parts.append(f"<li>{item}</li>")
        parts.append("</ul>")
    actions = action_lines(record)
    if actions:
        parts.append("<h3>Actions from the same meeting</h3><ul>")
        parts.extend(f"<li>{esc(a)}</li>" for a in actions)
        parts.append("</ul>")
    url = esc(decision_url(record["decision_id"]), quote=True)
    parts.append(f'<p><a href="{url}">Open this decision in Mytheus</a></p>')
    return "".join(parts)


def _mrkdwn(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def slack_message(record: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    content = str(record.get("content") or "")
    blocks: list[dict[str, Any]] = [
        {"type": "header", "text": {"type": "plain_text", "text": _clip("Decision", 150)}},
        {"type": "section", "text": {"type": "mrkdwn", "text": _clip(_mrkdwn(content), 2900)}},
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": _clip(_mrkdwn(_status_line(record)), 290)}],
        },
    ]
    if record.get("rationale"):
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _clip(f"*Rationale:* {_mrkdwn(str(record['rationale']))}", 2900),
                },
            }
        )
    lineage = lineage_lines(record)
    if lineage:
        lines = [
            f"• <{link}|{_mrkdwn(text)}>" if link else f"• {_mrkdwn(text)}"
            for text, link in lineage
        ]
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": _clip("*Lineage*\n" + "\n".join(lines), 2900)},
            }
        )
    blocks.append(
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Open in Mytheus"},
                    "url": decision_url(record["decision_id"]),
                }
            ],
        }
    )
    return _clip(f"Decision: {content}", 300), blocks


# --- the three writers ------------------------------------------------------------------------


async def export_notion(
    session: AsyncSession,
    org_id: uuid.UUID,
    conn: store.Connection,
    record: dict[str, Any],
    clients: Clients,
    *,
    parent_page_id: str,
) -> Exported:
    decision_id = uuid.UUID(str(record["decision_id"]))
    tokens = await store.access_token(session, conn, clients.oauth[config.NOTION])
    previous = await store.link_for_nexus(session, config.NOTION, NEXUS_KIND, decision_id)
    page = await clients.notion.create_page(
        tokens.access_token,
        parent_page_id=parent_page_id,
        title=title_for(record),
        blocks=notion_blocks(record),
    )
    if previous is not None and previous.external_id != page["id"]:
        try:
            await clients.notion.archive_page(tokens.access_token, previous.external_id)
        except ProviderError:
            pass  # the new page is the export; an old copy left behind is not a failure
    await store.put_link(
        session, org_id, config.NOTION, NEXUS_KIND, decision_id, page["id"], page["url"], None
    )
    return Exported(page["id"], page["url"], parent_page_id, previous is not None)


async def export_confluence(
    session: AsyncSession,
    org_id: uuid.UUID,
    conn: store.Connection,
    record: dict[str, Any],
    clients: Clients,
    *,
    space_key: str,
) -> Exported:
    decision_id = uuid.UUID(str(record["decision_id"]))
    tokens = await store.access_token(session, conn, clients.oauth[config.CONFLUENCE])
    cloud_id = str(conn.external_account_id)
    cursor = await store.get_cursor(session, conn.id, CONFLUENCE_SITE_KIND)
    site_url = str((cursor[0] if cursor else {}).get("site_url") or "").rstrip("/")
    previous = await store.link_for_nexus(session, config.CONFLUENCE, NEXUS_KIND, decision_id)
    title = f"{title_for(record)} ({str(decision_id)[:8]})"
    storage = confluence_storage(record)
    page: dict[str, Any] | None = None
    if previous is not None and previous.external_id.startswith(f"{space_key}:"):
        page_id = previous.external_id.split(":", 1)[1]
        page = await clients.confluence.update_page(
            tokens.access_token, cloud_id, page_id, title=title, storage=storage
        )
    if page is None:
        page = await clients.confluence.create_page(
            tokens.access_token, cloud_id, space_key=space_key, title=title, storage=storage
        )
    webui = page.get("webui")
    url = f"{site_url}/wiki{webui}" if site_url and webui else None
    external_id = f"{space_key}:{page['id']}"
    await store.put_link(
        session, org_id, config.CONFLUENCE, NEXUS_KIND, decision_id, external_id, url, None
    )
    return Exported(external_id, url, space_key, previous is not None)


async def export_slack(
    session: AsyncSession,
    org_id: uuid.UUID,
    conn: store.Connection,
    record: dict[str, Any],
    clients: Clients,
    *,
    channel: str | None,
    caller_email: str | None,
) -> Exported:
    decision_id = uuid.UUID(str(record["decision_id"]))
    tokens = await store.access_token(session, conn, clients.oauth[config.SLACK])
    token = tokens.access_token
    destination = channel or "dm"
    if channel is None:
        if not caller_email:
            raise ExportRefused("slack_user_not_found", "Your Slack account could not be found")
        slack_user = await clients.slack.user_id_for_email(token, caller_email)
        if slack_user is None:
            raise ExportRefused(
                "slack_user_not_found",
                "No Slack user in the connected workspace has your email address",
            )
        channel = await clients.slack.open_dm(token, slack_user)
    previous = await store.link_for_nexus(session, config.SLACK, NEXUS_KIND, decision_id)
    text, blocks = slack_message(record)
    try:
        posted = await clients.slack.post_message(token, channel=channel, text=text, blocks=blocks)
    except ProviderError as exc:
        if exc.reason in ("not_in_channel", "channel_not_found", "is_archived"):
            raise ExportRefused(
                f"slack_{exc.reason}",
                "Mytheus cannot post in that channel. Invite the Mytheus app to it first.",
            ) from None
        raise
    external_id = f"{posted['channel']}:{posted['ts']}"
    url = await clients.slack.permalink(token, channel=posted["channel"], ts=posted["ts"])
    await store.put_link(
        session, org_id, config.SLACK, NEXUS_KIND, decision_id, external_id, url, None
    )
    return Exported(external_id, url, destination, previous is not None)
