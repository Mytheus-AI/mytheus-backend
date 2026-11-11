"""Decisions that follow you: the surfaces around core-api's check.

- Slack (integration-service): a channel message is checked as its author and answered with an
  ephemeral note in its thread (Open the decision / Reopen it / Dismiss); direct messages, bots,
  edits and short messages are never read; an install without the history scopes does nothing;
  the buttons and the message shortcut act through core-api and answer on `response_url`; the
  interactivity webhook verifies Slack's signature and inboxes once per click.
- MCP (bff): `check_against_decisions` is listed read-only and renders the conflicts.
- Gateway and API tokens: the new prefixes route to core-api, and the check is a read.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import uuid
from datetime import datetime
from types import SimpleNamespace
from typing import Any

import pytest

from libs.shared import pat
from services.api_gateway import edge
from services.bff import mcp as mcp_mod
from services.integration_service import store
from services.integration_service.connectors import slack
from services.integration_service.core_client import CoreApiError
from tests.test_chat_api import seed_tenant
from tests.test_integrations_api import (  # noqa: F401 - fixtures
    _connect_row,
    _inbox,
    _slack_headers,
    client,
    fakes,
)

ORG = uuid.uuid4()
CHALLENGE = uuid.uuid4()


# --- Slack: pure parts ---------------------------------------------------------------------------


def _conn(scopes: list[str], status: str = "active") -> store.Connection:
    return store.Connection(
        id=uuid.uuid4(),
        org_id=ORG,
        provider="slack",
        external_account_id="T1",
        status=status,
        scopes=scopes,
        installed_by=uuid.uuid4(),
        created_at=datetime.now(),
        updated_at=datetime.now(),
        last_synced_at=None,
    )


def test_decision_checks_need_the_history_scopes():
    assert slack.decision_checks_state(None) is None
    assert slack.decision_checks_state(_conn(["chat:write"])) == "needs_reconnect"
    assert slack.decision_checks_state(_conn(["chat:write", "channels:history"])) == "on"
    assert slack.decision_checks_state(_conn(["channels:history"], "disconnected")) is None


def test_plain_text_keeps_what_a_person_wrote():
    raw = "<@U123> can we do <https://x.io/p|monthly pricing> for Acme &amp; Co? <!here>"
    assert slack.plain_text(raw) == "can we do monthly pricing for Acme & Co?"


def conflict(**extra: Any) -> dict[str, Any]:
    return {
        "decision_id": str(uuid.uuid4()),
        "decision_content": "Annual plans only for new customers",
        "meeting_title": "Pricing review",
        "decided_at": "2026-10-03",
        "owner": {"id": str(uuid.uuid4()), "name": "Dana Owner"},
        "quote": "offer Acme a monthly plan",
        "relation": "contradicts",
        "confidence": 0.9,
        "why": "The decision rules out monthly plans.",
        "link": "/meetings/m/decisions/d",
        "challenge_id": str(CHALLENGE),
        **extra,
    }


def test_the_note_reads_like_the_brief():
    text, blocks = slack.note_blocks(conflict(), "https://app.example")
    assert text.startswith("This was decided otherwise on 3 Oct (Pricing review).")
    assert blocks[0]["text"]["text"].startswith(
        "*This was decided otherwise on 3 Oct (Pricing review).*"
    )
    context = blocks[1]["elements"][0]["text"]
    assert context.startswith("Only you can see this.") and "Dana Owner" in context
    buttons = blocks[2]["elements"]
    assert [b["text"]["text"] for b in buttons] == ["Open the decision", "Reopen it", "Dismiss"]
    assert buttons[0]["url"] == "https://app.example/meetings/m/decisions/d"
    assert buttons[1]["value"] == buttons[2]["value"] == str(CHALLENGE)
    # Without a recorded challenge there is nothing to reopen or dismiss: only the link.
    _, bare = slack.note_blocks(conflict(challenge_id=None), "https://app.example")
    assert [b["action_id"] for b in bare[2]["elements"]] == [slack.OPEN]
    # Text from people cannot inject mrkdwn links.
    _, safe = slack.note_blocks(conflict(why="<https://evil|click>"), "https://a")
    assert "<https://evil" not in safe[0]["text"]["text"]


# --- Slack: the worker path with fakes -----------------------------------------------------------


class FakeSlackApi:
    def __init__(self) -> None:
        self.ephemeral: list[dict[str, Any]] = []
        self.responses: list[tuple[str, dict[str, Any]]] = []
        self.emails = {"UAUTHOR": "sam@example.com"}

    async def user_email(self, token: str, user_id: str) -> str | None:
        assert token == "xoxb-token"
        return self.emails.get(user_id)

    async def channel_name(self, token: str, channel: str) -> str | None:
        return "sales"

    async def permalink(self, token: str, *, channel: str, ts: str) -> str | None:
        return f"https://acme.slack.com/archives/{channel}/p{ts.replace('.', '')}"

    async def post_ephemeral(self, token: str, **kw: Any) -> None:
        self.ephemeral.append(kw)

    async def respond(self, response_url: str, payload: dict[str, Any]) -> None:
        self.responses.append((response_url, payload))


class FakeCore:
    def __init__(self, conflicts: list[dict[str, Any]] | None = None) -> None:
        self.checks: list[dict[str, Any]] = []
        self.acts: list[tuple[uuid.UUID, str, str]] = []
        self.conflicts = conflicts if conflicts is not None else [conflict()]
        self.act_error: CoreApiError | None = None

    async def decision_check(self, org_id: uuid.UUID, body: dict[str, Any]) -> dict[str, Any]:
        assert org_id == ORG
        self.checks.append(body)
        return {"status": "ok", "conflicts": self.conflicts}

    async def challenge_act(
        self, org_id: uuid.UUID, challenge_id: uuid.UUID, verb: str, acting_email: str
    ) -> dict[str, Any]:
        if self.act_error:
            raise self.act_error
        self.acts.append((challenge_id, verb, acting_email))
        return {}


@pytest.fixture
def world(monkeypatch):
    api = FakeSlackApi()
    clients = SimpleNamespace(slack=api, oauth={})
    state = {"conn": _conn(["chat:write", "channels:history", "groups:history"])}

    async def token(session: Any, _clients: Any):
        if slack.decision_checks_state(state["conn"]) != "on":
            return None
        return state["conn"], "xoxb-token"

    monkeypatch.setattr(slack, "_token", token)
    monkeypatch.setenv("APP_BASE_URL", "https://app.example")
    return SimpleNamespace(api=api, clients=clients, state=state)


def message(**extra: Any) -> dict[str, Any]:
    return {
        "type": "message",
        "channel": "C1",
        "channel_type": "channel",
        "user": "UAUTHOR",
        "ts": "1700000000.000100",
        "text": "Let's just offer Acme a monthly plan to close the deal this quarter.",
        **extra,
    }


async def test_a_channel_message_gets_an_ephemeral_note_in_its_thread(world):
    core = FakeCore()
    out = await slack.process(
        None, {"event": message()}, org_id=ORG, clients=world.clients, core=core
    )
    assert out == "processed"
    assert core.checks == [
        {
            "author_email": "sam@example.com",
            "text": "Let's just offer Acme a monthly plan to close the deal this quarter.",
            "place_label": "#sales",
            "link": "https://acme.slack.com/archives/C1/p1700000000000100",
            "external_ref": "T1:C1:1700000000.000100",
        }
    ]
    [note] = world.api.ephemeral
    assert note["user"] == "UAUTHOR" and note["channel"] == "C1"
    assert note["thread_ts"] == "1700000000.000100"
    assert note["text"].startswith("This was decided otherwise on 3 Oct")


@pytest.mark.parametrize(
    "event",
    [
        message(channel_type="im"),  # a direct message is private
        message(channel_type="mpim"),
        message(bot_id="B1"),  # bots, including Mytheus itself
        message(subtype="message_changed"),
        message(text="ok thanks"),  # too short to mean anything
        message(user="UNOEMAIL"),  # no email, no Mytheus person to check as
    ],
)
async def test_messages_that_are_never_checked(world, event):
    core = FakeCore()
    out = await slack.process(None, {"event": event}, org_id=ORG, clients=world.clients, core=core)
    assert out == "skipped" and core.checks == [] and world.api.ephemeral == []


async def test_an_install_without_history_scopes_does_nothing(world):
    world.state["conn"] = _conn(["chat:write"])
    core = FakeCore()
    out = await slack.process(
        None, {"event": message()}, org_id=ORG, clients=world.clients, core=core
    )
    assert out == "skipped" and core.checks == []


async def test_no_conflict_means_no_note_and_core_errors(world):
    core = FakeCore(conflicts=[])
    await slack.process(None, {"event": message()}, org_id=ORG, clients=world.clients, core=core)
    assert world.api.ephemeral == []

    class Down(FakeCore):
        async def decision_check(self, org_id: uuid.UUID, body: dict[str, Any]) -> dict[str, Any]:
            raise CoreApiError(503, "HTTP 503")

    out = await slack.process(
        None, {"event": message()}, org_id=ORG, clients=world.clients, core=Down()
    )
    assert out == "processed" and world.api.ephemeral == []  # quiet, not retried forever


def click(action_id: str, user: str = "UAUTHOR") -> dict[str, Any]:
    return {
        "interaction": {
            "type": "block_actions",
            "user": {"id": user},
            "response_url": "https://hooks.slack.com/actions/T1/1/abc",
            "actions": [{"action_id": action_id, "value": str(CHALLENGE)}],
        }
    }


async def test_buttons_reopen_and_dismiss_as_the_clicker(world):
    core = FakeCore()
    for action, verb, said in (
        (slack.REOPEN, "reopen", "Reopen request sent"),
        (slack.DISMISS, "dismiss", "Dismissed."),
    ):
        out = await slack.process(None, click(action), org_id=ORG, clients=world.clients, core=core)
        assert out == "processed"
        assert core.acts[-1] == (CHALLENGE, verb, "sam@example.com")
        url, payload = world.api.responses[-1]
        assert url.startswith("https://hooks.slack.com/")
        assert payload["text"].startswith(said) and payload["replace_original"] is True
    # "Open the decision" is a link: nothing to do.
    assert (
        await slack.process(None, click(slack.OPEN), org_id=ORG, clients=world.clients, core=core)
        == "skipped"
    )
    # Someone else's click: core-api answers 404 and the clicker is told plainly.
    core.act_error = CoreApiError(404, "HTTP 404")
    world.api.emails["UOTHER"] = "olu@example.com"
    await slack.process(
        None, click(slack.DISMISS, "UOTHER"), org_id=ORG, clients=world.clients, core=core
    )
    assert world.api.responses[-1][1]["text"].startswith("Only the person who wrote the message")


async def test_the_message_shortcut_checks_as_the_person_who_asked(world):
    core = FakeCore()
    shortcut = {
        "interaction": {
            "type": "message_action",
            "callback_id": slack.SHORTCUT,
            "user": {"id": "UAUTHOR"},
            "channel": {"id": "C9", "name": "pricing"},
            "message": {"ts": "1.2", "text": "We agreed monthly plans are fine for SMB customers."},
            "response_url": "https://hooks.slack.com/actions/T1/2/def",
        }
    }
    assert (
        await slack.process(None, shortcut, org_id=ORG, clients=world.clients, core=core)
        == "processed"
    )
    assert core.checks[-1]["place_label"] == "#pricing"
    payload = world.api.responses[-1][1]
    assert payload["blocks"][2]["elements"][1]["text"]["text"] == "Reopen it"
    core.conflicts = []
    await slack.process(None, shortcut, org_id=ORG, clients=world.clients, core=core)
    assert world.api.responses[-1][1]["text"].startswith("No conflict")


async def test_respond_only_follows_slacks_own_hooks_host():
    from services.integration_service.clients import ProviderError, SlackApi

    with pytest.raises(ProviderError):
        await SlackApi().respond("https://evil.example/hook", {"text": "x"})


# --- Slack: the interactivity webhook ------------------------------------------------------------


def _form(payload: dict[str, Any]) -> bytes:
    return urllib.parse.urlencode({"payload": json.dumps(payload)}).encode()


def test_interactions_are_verified_and_inboxed_once_per_click(client):  # noqa: F811
    t = seed_tenant()
    team = f"T{uuid.uuid4().hex[:8].upper()}"
    _connect_row(t, "slack", team)
    payload = {
        "type": "block_actions",
        "team": {"id": team},
        "trigger_id": "trig-1",
        "user": {"id": "U1"},
        "actions": [{"action_id": slack.DISMISS, "value": str(CHALLENGE)}],
    }
    body = _form(payload)
    headers = {**_slack_headers(body), "Content-Type": "application/x-www-form-urlencoded"}
    r = client.post("/webhooks/slack/interactions", content=body, headers=headers)
    assert r.status_code == 200 and r.json() == {"status": "accepted"}
    again = client.post("/webhooks/slack/interactions", content=body, headers=headers)
    assert again.json() == {"status": "duplicate"}
    assert _inbox(t.org_id, "slack") == [
        {
            "delivery_id": "interaction:trig-1",
            "event_type": "interaction:block_actions",
            "status": "pending",
        }
    ]
    bad = {
        **_slack_headers(body, secret="nope"),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert client.post("/webhooks/slack/interactions", content=body, headers=bad).status_code == 401
    stale = {
        **_slack_headers(body, ts=int(time.time()) - 900),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    assert (
        client.post("/webhooks/slack/interactions", content=body, headers=stale).status_code == 401
    )
    other = _form({**payload, "type": "view_submission"})
    h = {**_slack_headers(other), "Content-Type": "application/x-www-form-urlencoded"}
    assert client.post("/webhooks/slack/interactions", content=other, headers=h).json() == {
        "status": "ignored"
    }


# --- MCP -----------------------------------------------------------------------------------------


class FakeCoreClient:
    def __init__(self, body: dict[str, Any]) -> None:
        self.body = body
        self.calls: list[tuple[str, str, Any]] = []

    async def call(
        self, method: str, path: str, *, params: Any = None, json_body: Any = None
    ) -> Any:
        self.calls.append((method, path, json_body))
        return self.body


def ctx() -> Any:
    return SimpleNamespace(org_id=ORG, user_id=uuid.uuid4(), can_write=False)


def test_the_tool_is_listed_read_only():
    tool = next(t for t in mcp_mod.TOOLS if t["name"] == "check_against_decisions")
    assert tool["annotations"]["readOnlyHint"] is True
    assert tool["inputSchema"]["required"] == ["text"]


async def test_the_tool_checks_through_core_and_renders_conflicts():
    body = {
        "status": "ok",
        "reason": None,
        "conflicts": [
            {
                **conflict(),
                "decision_id": "11111111-1111-1111-1111-111111111111",
            }
        ],
    }
    core = FakeCoreClient(body)
    text, structured = await mcp_mod.call_tool(
        "check_against_decisions",
        {"text": "We'll offer Acme monthly.", "topic": "Acme renewal", "place": "email to Acme"},
        ctx(),
        core,
    )
    assert core.calls == [
        (
            "POST",
            "/api/v1/decisions/check",
            {
                "text": "We'll offer Acme monthly.",
                "source": "mcp",
                "context": {"topic": "Acme renewal", "place_label": "email to Acme"},
            },
        )
    ]
    assert structured == body
    assert "Decided on 2026-10-03 (Pricing review), owner Dana Owner" in text
    assert 'The draft contradicts it: "offer Acme a monthly plan"' in text
    for answer, expected in (
        ({"status": "ok", "conflicts": []}, "No conflict"),
        ({"status": "skipped", "reason": "never_record", "conflicts": []}, "never records"),
        ({"status": "degraded", "reason": "budget", "conflicts": []}, "budget"),
    ):
        said, _ = await mcp_mod.call_tool(
            "check_against_decisions", {"text": "x y z w"}, ctx(), FakeCoreClient(answer)
        )
        assert expected in said
    with pytest.raises(mcp_mod.ToolError):
        await mcp_mod.call_tool("check_against_decisions", {"text": " "}, ctx(), core)


# --- gateway and API tokens ----------------------------------------------------------------------


def test_the_new_prefixes_route_to_core_api():
    for path in (
        "/api/v1/decisions/check",
        "/api/v1/decision-challenges/abc/dismiss",
        "/api/v1/decision-reopen-requests/abc/accept",
    ):
        assert edge.route_for(path) == "core-api", path


def test_the_check_is_a_read_for_api_tokens():
    assert pat.allows("POST", "/api/v1/decisions/check", "read") == "ok"
    assert (
        pat.allows("POST", f"/api/v1/decisions/{uuid.uuid4()}/reopen-requests", "read") == "scope"
    )
    assert pat.allows("GET", "/api/v1/decision-reopen-requests", "read") == "ok"
    assert pat.allows("POST", "/api/v1/decision-challenges/x/dismiss", "read_write") == "ok"
