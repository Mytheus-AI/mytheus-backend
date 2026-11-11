"""Slack connector (plan 3.11 step 2; 21 section 4).

Build here, deliver in notification-service. integration-service owns the bot install and the
message content; notification-service is the sole external fan-out (it fetches the bot token
from `GET /internal/slack/token` at send time, resolves the recipient's Slack user by email,
opens the DM and posts).

`Notification` (mytheus-events) has no Block Kit field and P12 changes no schema, so the brief and
the summary travel as `title` + a Slack-mrkdwn `body` + an app `link`; notification-service
renders those into Block Kit (header, section, link button) when it posts.

Inbound events: `app_uninstalled` and `tokens_revoked` act (the connection becomes
`disconnected` and its tokens are deleted).

`message` (promise ledger): a person's own message can carry a promise ("I'll send the deck by
Friday"). In order, and stopping at the first "no":
1. only a person's plain message (no subtype, no bot, has text) that says something like a
   promise in the first person (`PROMISE_CUE`, a free deterministic pre-gate: most messages
   never reach a model);
2. the extractor is connected (`promise_client.get_extractor()`), else nothing leaves the
   tenant;
3. the author is a live user of this workspace whose agent may look at their Slack messages
   (core-api `capture-check`, by the email Slack holds for them);
4. the person's agent extracts their own promises (intelligence `promise_extract`), with the
   people the message mentions as candidate recipients;
5. core-api stores them `pending` with the message's permalink and the exact quote; the person
   confirms or dismisses each one in the ledger.
Everything else is recorded and skipped.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError
from services.integration_service.core_client import CoreApiClient
from services.integration_service.promise_client import get_extractor

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


# First-person future commitments, the cheap pre-gate before any model call. Wide on purpose
# (the model decides); a message with none of these never leaves the tenant.
PROMISE_CUE = re.compile(
    r"\b(i\s*'?ll|i will|i'm going to|i am going to|i can|let me|will do|on it|i promise|"
    r"i'?ll have|count on me|leave it with me|i'?m on|i'?ll get|we'?ll|i'?d be happy to|"
    r"i commit|by (?:mon|tue|wed|thu|fri|sat|sun|tomorrow|today|eod|end of))",
    re.IGNORECASE,
)
MAX_MESSAGE_CHARS = 4000
MAX_MENTIONS = 5
_MENTION = re.compile(r"<@([UW][A-Z0-9]+)(?:\|[^>]*)?>")
_LINK = re.compile(r"<(https?://[^|>]+)(?:\|([^>]+))?>")
_SPECIAL = re.compile(r"<!(here|channel|everyone)[^>]*>")


def is_promise_candidate(event: dict[str, Any]) -> bool:
    """A person's own plain message that may carry a promise."""
    if event.get("type") != "message" or event.get("subtype") or event.get("bot_id"):
        return False
    text = event.get("text")
    if not isinstance(text, str) or not text.strip() or not event.get("user"):
        return False
    if not event.get("channel") or not event.get("ts"):
        return False
    return PROMISE_CUE.search(text) is not None


def _plain(text: str, names: dict[str, str]) -> str:
    text = _MENTION.sub(lambda m: f"@{names.get(m.group(1), 'someone')}", text)
    text = _LINK.sub(lambda m: m.group(2) or m.group(1), text)
    text = _SPECIAL.sub(lambda m: f"@{m.group(1)}", text)
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    return text[:MAX_MESSAGE_CHARS]


def _sent_at(ts: Any) -> datetime:
    try:
        return datetime.fromtimestamp(float(ts), tz=UTC)
    except (TypeError, ValueError):
        return datetime.now(UTC)


async def capture_promises(
    session: AsyncSession,
    org_id: uuid.UUID,
    payload: dict[str, Any],
    clients: Clients,
    core: CoreApiClient,
) -> str:
    """Steps 1-5 of the module docstring for one `message` event."""
    event = payload.get("event") or {}
    if not is_promise_candidate(event):
        return "skipped"
    extractor = get_extractor()
    if extractor is None:
        return "skipped"  # not connected: no model call without the intelligence secret
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if conn is None:
        return "skipped"
    token = (await store.access_token(session, conn, clients.oauth[PROVIDER])).access_token
    author = await clients.slack.user_info(token, str(event["user"]))
    if not author or not author.get("email"):
        return "skipped"
    check = await core.promise_capture_check(org_id, "slack", str(author["email"]))
    if not check.get("user_id") or not check.get("enabled"):
        return "skipped"
    names: dict[str, str] = {}
    people: list[dict[str, str | None]] = []
    for slack_id in list(dict.fromkeys(_MENTION.findall(str(event["text"]))))[:MAX_MENTIONS]:
        if slack_id == event["user"]:
            continue
        info = await clients.slack.user_info(token, slack_id)
        if info and info.get("name"):
            names[slack_id] = str(info["name"])
            people.append({"label": str(info["name"]), "email": info.get("email")})
    sent = _sent_at(event.get("ts"))
    found = await extractor.extract(
        org_id,
        channel="slack",
        text=_plain(str(event["text"]), names),
        author_name=author.get("name"),
        occurred_at=sent.isoformat(),
        reference_date=sent.date().isoformat(),
        people=people,
    )
    if found.status != "ok" or not found.promises:
        return "processed"
    channel_id, ts = str(event["channel"]), str(event["ts"])
    link = await clients.slack.permalink(token, channel=channel_id, ts=ts)
    team = str(payload.get("team_id") or conn.external_account_id or "")
    await core.promises_captured(
        org_id,
        {
            "channel": "slack",
            "author_email": str(author["email"]),
            "source_ref": f"slack:{team}:{channel_id}:{ts}"[:300],
            "source_link": link,
            "occurred_at": sent.isoformat(),
            "prompt_version": found.prompt,
            "promises": found.promises[:20],
        },
    )
    return "processed"


async def process(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    org_id: uuid.UUID | None = None,
    clients: Clients | None = None,
    core: CoreApiClient | None = None,
) -> str:
    event = payload.get("event") or {}
    if event.get("type") == "message":
        if org_id is None or clients is None or core is None:
            return "skipped"
        return await capture_promises(session, org_id, payload, clients, core)
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
