"""Slack connector (plan 3.11 step 2; 21 section 4).

Build here, deliver in notification-service. integration-service owns the bot install and the
message content; notification-service is the sole external fan-out (it fetches the bot token
from `GET /internal/slack/token` at send time, resolves the recipient's Slack user by email,
opens the DM and posts).

`Notification` (mytheus-events) has no Block Kit field and P12 changes no schema, so the brief and
the summary travel as `title` + a Slack-mrkdwn `body` + an app `link`; notification-service
renders those into Block Kit (header, section, link button) when it posts.

Inbound events: `app_uninstalled` and `tokens_revoked` disconnect (the connection becomes
`disconnected` and its tokens are deleted); channel `message` events and the interactivity
payloads drive "Decisions that follow you" (below); everything else is recorded and skipped.
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Literal

from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError
from services.integration_service.core_client import CoreApiClient, CoreApiError

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


async def process(
    session: AsyncSession,
    payload: dict[str, Any],
    *,
    org_id: uuid.UUID | None = None,
    clients: Clients | None = None,
    core: CoreApiClient | None = None,
) -> str:
    if "interaction" in payload:
        if org_id is None or clients is None or core is None:
            return "skipped"
        return await process_interaction(session, org_id, payload["interaction"], clients, core)
    event = payload.get("event") or {}
    if event.get("type") == "message":
        if org_id is None or clients is None or core is None:
            return "skipped"
        return await process_message(session, org_id, event, clients, core)
    if event.get("type") not in ("app_uninstalled", "tokens_revoked"):
        return "skipped"
    conn = await store.get_connection(session, PROVIDER, for_update=True)
    if conn is None or conn.status == "disconnected":
        return "skipped"
    await store.delete_tokens(session, conn.id)
    await store.delete_cursors(session, conn.id)
    await store.set_status(session, conn.id, "disconnected")
    return "processed"


# --- Decisions that follow you (A1) -----------------------------------------------------------
#
# A message posted in a channel the Mytheus app is in is checked against the org's confirmed
# decisions AS ITS AUTHOR (core-api `POST /internal/decisions/check`; retrieval is scoped to the
# meetings that person took part in). Each conflict is recorded by core-api as a challenge (the
# decision's owner sees it in their Inbox) and answered here with an ephemeral note in the
# message's thread, visible only to the author first: "This was decided otherwise on 3 Oct
# (Pricing review)." with Open the decision / Reopen it / Dismiss. The buttons come back through
# `POST /webhooks/slack/interactions` and only the author's own clicks act.
#
# Not connected until the workspace granted the history scopes (an install from before them
# reports `decision_checks: needs_reconnect` on the integrations card) and the Slack app has the
# `message.channels` / `message.groups` event subscriptions and the interactivity URL set.

DECISION_CHECK_SCOPES = frozenset({"channels:history", "groups:history"})
MIN_MESSAGE_CHARS = 20
MAX_MESSAGE_CHARS = 4000
OPEN, REOPEN, DISMISS = "decision_open", "decision_reopen", "decision_dismiss"
SHORTCUT = "check_against_decisions"
_MENTION = re.compile(r"<[@#!][^>|]*(?:\|([^>]*))?>")
_LINK = re.compile(r"<(https?://[^>|]+)(?:\|([^>]*))?>")


def decision_checks_state(
    conn: store.Connection | None,
) -> Literal["on", "needs_reconnect"] | None:
    """`on` when the install can read channel messages, `needs_reconnect` when it predates the
    scopes, None when Slack is not connected."""
    if conn is None or conn.status != "active":
        return None
    return "on" if DECISION_CHECK_SCOPES & set(conn.scopes) else "needs_reconnect"


def plain_text(raw: str) -> str:
    """Slack mrkdwn to the words a person wrote: links keep their label, mentions their name."""
    out = _LINK.sub(lambda m: m.group(2) or m.group(1), raw)
    out = _MENTION.sub(lambda m: m.group(1) or "", out)
    out = out.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    return " ".join(out.split())


def day_month(raw: Any) -> str | None:
    try:
        when = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return f"{when.day} {when.strftime('%b')}"


def _mrkdwn(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def note_blocks(conflict: dict[str, Any], app_base: str) -> tuple[str, list[dict[str, Any]]]:
    """(fallback text, Block Kit) for one conflict."""
    when = day_month(conflict.get("decided_at"))
    where = conflict.get("meeting_title")
    headline = "This was decided otherwise"
    if when:
        headline += f" on {when}"
    if where:
        headline += f" ({_clip(str(where), 80)})"
    headline += "."
    decision = _clip(str(conflict.get("decision_content") or ""), 400)
    why = _clip(str(conflict.get("why") or ""), 300)
    owner = (conflict.get("owner") or {}).get("name")
    details = [f"> {_mrkdwn(decision)}"]
    if why:
        details.append(f"_{_mrkdwn(why)}_")
    context = [
        "Only you can see this. You wrote: "
        f"\u201c{_mrkdwn(_clip(str(conflict.get('quote') or ''), 200))}\u201d"
    ]
    if owner:
        context.append(f"Owner: {_mrkdwn(str(owner))}")
    challenge = str(conflict.get("challenge_id") or "")
    buttons: list[dict[str, Any]] = [
        {
            "type": "button",
            "action_id": OPEN,
            "text": {"type": "plain_text", "text": "Open the decision"},
            "url": f"{app_base}{conflict.get('link') or '/decisions'}",
        }
    ]
    if challenge:
        buttons += [
            {
                "type": "button",
                "action_id": REOPEN,
                "text": {"type": "plain_text", "text": "Reopen it"},
                "value": challenge,
            },
            {
                "type": "button",
                "action_id": DISMISS,
                "text": {"type": "plain_text", "text": "Dismiss"},
                "value": challenge,
            },
        ]
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{_mrkdwn(headline)}*\n" + "\n".join(details)},
        },
        {"type": "context", "elements": [{"type": "mrkdwn", "text": " · ".join(context)}]},
        {"type": "actions", "elements": buttons},
    ]
    return f"{headline} {decision}", blocks


async def _token(session: AsyncSession, clients: Clients) -> tuple[store.Connection, str] | None:
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if decision_checks_state(conn) != "on" or conn is None:
        return None
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    return conn, tokens.access_token


async def _check(
    org_id: uuid.UUID,
    core: CoreApiClient,
    *,
    email: str,
    message: str,
    place: str,
    link: str | None,
    external_ref: str,
) -> list[dict[str, Any]]:
    try:
        result = await core.decision_check(
            org_id,
            {
                "author_email": email,
                "text": message[:MAX_MESSAGE_CHARS],
                "place_label": place,
                "link": link,
                "external_ref": external_ref[:200],
            },
        )
    except CoreApiError as exc:
        if exc.status == 404:
            return []  # the author has no Mytheus account here: nobody to check as
        if exc.status == 503:
            return []  # checks not connected on this server: stay quiet, do not retry
        raise
    return [c for c in result.get("conflicts") or [] if isinstance(c, dict)]


async def process_message(
    session: AsyncSession,
    org_id: uuid.UUID,
    event: dict[str, Any],
    clients: Clients,
    core: CoreApiClient,
) -> str:
    if event.get("subtype") not in (None, "thread_broadcast") or event.get("bot_id"):
        return "skipped"  # edits, joins, bots (including our own notes): never checked
    if event.get("channel_type") not in ("channel", "group"):
        return "skipped"  # direct messages are private conversations: never read
    user, channel, ts = event.get("user"), event.get("channel"), event.get("ts")
    message = plain_text(str(event.get("text") or ""))
    if not (user and channel and ts) or len(message) < MIN_MESSAGE_CHARS:
        return "skipped"
    found = await _token(session, clients)
    if found is None:
        return "skipped"
    conn, token = found
    email = await clients.slack.user_email(token, str(user))
    if not email:
        return "skipped"
    name = await clients.slack.channel_name(token, str(channel))
    conflicts = await _check(
        org_id,
        core,
        email=email,
        message=message,
        place=f"#{name}" if name else "Slack",
        link=await clients.slack.permalink(token, channel=str(channel), ts=str(ts)),
        external_ref=f"{conn.external_account_id}:{channel}:{ts}",
    )
    for conflict in conflicts:
        text_, blocks = note_blocks(conflict, config.app_base_url())
        await clients.slack.post_ephemeral(
            token,
            channel=str(channel),
            user=str(user),
            text=text_,
            blocks=blocks,
            thread_ts=str(event.get("thread_ts") or ts),
        )
    return "processed"


async def process_interaction(
    session: AsyncSession,
    org_id: uuid.UUID,
    data: dict[str, Any],
    clients: Clients,
    core: CoreApiClient,
) -> str:
    found = await _token(session, clients)
    response_url = str(data.get("response_url") or "")
    user = (data.get("user") or {}).get("id")
    if found is None or not user:
        return "skipped"
    conn, token = found
    email = await clients.slack.user_email(token, str(user))
    if not email:
        return "skipped"
    if data.get("type") == "message_action":
        if data.get("callback_id") != SHORTCUT:
            return "skipped"
        message = data.get("message") or {}
        channel = (data.get("channel") or {}).get("id")
        words = plain_text(str(message.get("text") or ""))
        if len(words) < MIN_MESSAGE_CHARS or not channel:
            await _respond(clients, response_url, "That message is too short to check.")
            return "processed"
        name = (data.get("channel") or {}).get("name")
        conflicts = await _check(
            org_id,
            core,
            email=email,
            message=words,
            place=f"#{name}" if name else "Slack",
            link=await clients.slack.permalink(
                token, channel=str(channel), ts=str(message.get("ts") or "")
            ),
            external_ref=f"{conn.external_account_id}:{channel}:{message.get('ts')}",
        )
        if not conflicts:
            await _respond(
                clients,
                response_url,
                "No conflict with a decision from meetings you took part in.",
            )
        for conflict in conflicts:
            text_, blocks = note_blocks(conflict, config.app_base_url())
            await _respond(clients, response_url, text_, blocks)
        return "processed"
    action: dict[str, Any] = next(iter(data.get("actions") or []), {})
    verb = {REOPEN: "reopen", DISMISS: "dismiss"}.get(str(action.get("action_id")))
    if verb is None:
        return "skipped"  # "Open the decision" is a link; Slack still reports the click
    try:
        challenge_id = uuid.UUID(str(action.get("value")))
    except ValueError:
        return "skipped"
    try:
        await core.challenge_act(org_id, challenge_id, verb, email)
    except CoreApiError as exc:
        if exc.status == 404:
            await _respond(
                clients, response_url, "Only the person who wrote the message can do that."
            )
            return "processed"
        if exc.status == 409:
            await _respond(
                clients, response_url, "This decision was already replaced; nothing to reopen."
            )
            return "processed"
        raise
    await _respond(
        clients,
        response_url,
        (
            "Reopen request sent to the decision's owner. You will hear back in Mytheus."
            if verb == "reopen"
            else "Dismissed. Mytheus will not flag this again."
        ),
    )
    return "processed"


async def _respond(
    clients: Clients, response_url: str, text_: str, blocks: list[dict[str, Any]] | None = None
) -> None:
    if not response_url:
        return
    payload: dict[str, Any] = {
        "response_type": "ephemeral",
        "replace_original": blocks is None,
        "text": text_,
    }
    if blocks is not None:
        payload["blocks"] = blocks
    await clients.slack.respond(response_url, payload)


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
