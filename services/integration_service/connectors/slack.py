"""Slack connector (plan 3.11 step 2; 21 section 4).

Build here, deliver in notification-service. integration-service owns the bot install and the
message content; notification-service is the sole external fan-out (it fetches the bot token
from `GET /internal/slack/token` at send time, resolves the recipient's Slack user by email,
opens the DM and posts).

`Notification` (mytheus-events) has no Block Kit field and P12 changes no schema, so the brief and
the summary travel as `title` + a Slack-mrkdwn `body` + an app `link`; notification-service
renders those into Block Kit (header, section, link button) when it posts.

Inbound events: only `app_uninstalled` and `tokens_revoked` act (the connection becomes
`disconnected` and its tokens are deleted); everything else is recorded and skipped.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError

PROVIDER = config.SLACK
PRODUCER = "integration-service"
MAX_BODY = 1900
MAX_ITEMS = 8


async def post_install(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    """Prove the bot token works for the workspace it says it is for."""
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    identity = await clients.slack.auth_test(tokens.access_token)
    team = identity.get("team_id")
    if team and conn.external_account_id and team != conn.external_account_id:
        raise ProviderError(PROVIDER, "post_install", "team_mismatch")


async def process(session: AsyncSession, payload: dict[str, Any]) -> str:
    event = payload.get("event") or {}
    if event.get("type") not in ("app_uninstalled", "tokens_revoked"):
        return "skipped"
    conn = await store.get_connection(session, PROVIDER, for_update=True)
    if conn is None or conn.status == "disconnected":
        return "skipped"
    await store.delete_tokens(session, conn.id)
    await store.delete_cursors(session, conn.id)
    await store.set_status(session, conn.id, "disconnected")
    return "processed"


def _clip(text: str, limit: int = MAX_BODY) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _when(raw: Any) -> str:
    if not raw:
        return "time not set"
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return str(raw)
    return parsed.strftime("%a %d %b, %H:%M UTC")


def prep_brief(digest: dict[str, Any], participant: dict[str, Any]) -> tuple[str, str]:
    title = str(digest.get("title") or "Meeting")
    others = [
        str(p.get("email"))
        for p in digest.get("participants", [])
        if p.get("user_id") != participant.get("user_id") and p.get("email")
    ]
    open_actions = int(participant.get("open_actions") or 0)
    lines = [
        f"*{title}* starts {_when(digest.get('scheduled_start'))}.",
        f"With: {', '.join(others[:MAX_ITEMS]) or 'just you so far'}"
        + (f" and {len(others) - MAX_ITEMS} more" if len(others) > MAX_ITEMS else ""),
    ]
    if open_actions:
        noun = "action" if open_actions == 1 else "actions"
        lines.append(f"You have {open_actions} open {noun} in Mytheus.")
    decisions = [str(d.get("content")) for d in digest.get("recent_decisions", [])][:3]
    if decisions:
        lines.append("Recent decisions:")
        lines.extend(f"• {d}" for d in decisions)
    return f"Prep brief: {title}"[:200], _clip("\n".join(lines))


def meeting_summary(digest: dict[str, Any], participant: dict[str, Any]) -> tuple[str, str]:
    """Confirmed decisions and actions only: proposed candidates never appear (no silent
    commits)."""
    title = str(digest.get("title") or "Meeting")
    decisions = [str(d.get("content")) for d in digest.get("decisions", [])]
    actions = digest.get("actions", [])
    mine = [a for a in actions if a.get("assignee_id") == participant.get("user_id")]
    lines = [f"*{title}* has ended."]
    if decisions:
        lines.append(f"Decisions ({len(decisions)}):")
        lines.extend(f"• {d}" for d in decisions[:MAX_ITEMS])
    if actions:
        lines.append(f"Actions ({len(actions)}):")
        for a in actions[:MAX_ITEMS]:
            due = f" (due {a['due_date']})" if a.get("due_date") else ""
            lines.append(f"• {a.get('content')}{due}")
    if mine:
        noun = "action is" if len(mine) == 1 else "actions are"
        lines.append(f"{len(mine)} of these {noun} yours.")
    if not decisions and not actions:
        lines.append("No decisions or actions were confirmed.")
    return f"Summary: {title}"[:200], _clip("\n".join(lines))


async def emit(
    session: AsyncSession,
    org_id: uuid.UUID,
    kind: str,
    digest: dict[str, Any],
) -> int:
    """One `notifications` event per participant, channel slack, through the outbox."""
    meeting_id = str(digest["meeting_id"])
    source_event_id = f"slack-{kind}:{meeting_id}"
    builder = prep_brief if kind == "prep-brief" else meeting_summary
    sent = 0
    for participant in digest.get("participants", []):
        user_id = participant.get("user_id")
        if not user_id:
            continue
        title, body = builder(digest, participant)
        await enqueue(
            session,
            EventEnvelope(
                topic="notifications",
                org_id=str(org_id),
                producer=PRODUCER,
                idempotency_key=f"notif:{source_event_id}:slack:{user_id}",
                payload={
                    "source_event_id": source_event_id,
                    "channel": "slack",
                    "user_id": str(user_id),
                    "priority": 2,
                    "title": title,
                    "body": body,
                    "link": f"/meetings/{meeting_id}",
                },
            ),
        )
        sent += 1
    return sent
