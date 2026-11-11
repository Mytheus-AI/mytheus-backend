"""Promise capture from Slack (promise ledger): the integration-service side, with fakes.

The pre-gate (a person's own plain message that says something like a promise), "not connected"
without the intelligence secret, the author check before any model call, mentions offered as
recipients, the permalink and the hand-off to core-api. No DB and no network: the connection
store, Slack, the extractor and core-api are fakes. The rag-service client is checked against
an in-process transport.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from services.integration_service import promise_client, store
from services.integration_service.clients import ProviderError
from services.integration_service.connectors import slack
from services.integration_service.core_client import CoreApiError
from services.integration_service.promise_client import Extraction, PromiseExtractor

ORG = uuid.uuid4()


def event(text: str, **extra: Any) -> dict[str, Any]:
    return {
        "team_id": "T1",
        "event": {
            "type": "message",
            "user": "U0AUTHOR",
            "text": text,
            "channel": "C1",
            "ts": "1731315600.000200",
            **extra,
        },
    }


@pytest.mark.parametrize(
    ("text", "extra", "expected"),
    [
        ("I'll send the deck by Friday", {}, True),
        ("Sure, I can take that", {}, True),
        ("will do!", {}, True),
        ("Can you send the deck?", {}, False),
        ("The deck is attached", {}, False),
        ("I'll send it", {"subtype": "message_changed"}, False),
        ("I'll send it", {"bot_id": "B1"}, False),
        ("", {}, False),
    ],
)
def test_only_a_persons_plain_promise_like_message_passes(
    text: str, extra: dict[str, Any], expected: bool
) -> None:
    assert slack.is_promise_candidate(event(text, **extra)["event"]) is expected


@dataclass
class FakeSlackApi:
    users: dict[str, dict[str, str | None]]
    calls: list[str] = field(default_factory=list)

    async def user_info(self, token: str, user_id: str) -> dict[str, str | None] | None:
        self.calls.append(f"user_info:{user_id}")
        return self.users.get(user_id)

    async def permalink(self, token: str, *, channel: str, ts: str) -> str | None:
        return f"https://acme.slack.com/archives/{channel}/p{ts.replace('.', '')}"


@dataclass
class FakeClients:
    slack: FakeSlackApi
    oauth: dict[str, Any] = field(default_factory=lambda: {"slack": object()})


@dataclass
class FakeCore:
    enabled: bool = True
    user: str | None = str(uuid.uuid4())
    captured: list[dict[str, Any]] = field(default_factory=list)
    checks: list[str] = field(default_factory=list)

    async def promise_capture_check(
        self, org_id: uuid.UUID, channel: str, author_email: str
    ) -> dict[str, Any]:
        self.checks.append(author_email)
        return {"user_id": self.user, "enabled": self.enabled}

    async def promises_captured(self, org_id: uuid.UUID, body: dict[str, Any]) -> dict[str, Any]:
        self.captured.append(body)
        return {"user_id": self.user, "written": len(body["promises"])}


@dataclass
class FakeExtractor:
    answer: Extraction
    asked: list[dict[str, Any]] = field(default_factory=list)

    async def extract(self, org_id: uuid.UUID, **kw: Any) -> Extraction:
        self.asked.append(kw)
        return self.answer


@pytest.fixture
def connected(monkeypatch):
    class Conn:
        external_account_id = "T1"
        status = "active"
        scopes = ("channels:history", "groups:history", "im:history")

    class Tokens:
        access_token = "xoxb-test"

    async def get_connection(_s: Any, _p: str, **_k: Any) -> Conn:
        return Conn()

    async def access_token(_s: Any, _c: Any, _o: Any) -> Tokens:
        return Tokens()

    monkeypatch.setattr(store, "get_connection", get_connection)
    monkeypatch.setattr(store, "access_token", access_token)
    yield
    promise_client.set_extractor(None)


def _clients() -> FakeClients:
    return FakeClients(
        FakeSlackApi(
            {
                "U0AUTHOR": {"email": "maya@example.com", "name": "Maya"},
                "U0SAM": {"email": "sam@example.com", "name": "Sam"},
            }
        )
    )


PROMISE = {
    "content": "Send Sam the revised deck",
    "due_hint": "Friday",
    "due_date": "2026-11-13",
    "recipient_label": "Sam",
    "recipient_email": "sam@example.com",
    "quote": "I'll send you the revised deck by Friday",
    "confidence": 0.9,
}


async def test_a_promise_in_slack_reaches_core_api_pending_with_its_source(connected) -> None:
    extractor = FakeExtractor(Extraction("ok", "promise_extract@v1", [PROMISE]))
    promise_client.set_extractor(extractor)  # type: ignore[arg-type]
    core, clients = FakeCore(), _clients()
    payload = event("<@U0SAM> I'll send you the revised deck by Friday <https://x.io|the doc>")
    out = await slack.process(None, payload, org_id=ORG, clients=clients, core=core)  # type: ignore[arg-type]
    assert out == "processed"
    [asked] = extractor.asked
    assert asked["text"] == "@Sam I'll send you the revised deck by Friday the doc"
    assert asked["people"] == [{"label": "Sam", "email": "sam@example.com"}]
    assert asked["reference_date"] == "2024-11-11" and asked["author_name"] == "Maya"
    [body] = core.captured
    assert body["source_ref"] == "slack:T1:C1:1731315600.000200"
    assert body["source_link"] == "https://acme.slack.com/archives/C1/p1731315600000200"
    assert body["author_email"] == "maya@example.com" and body["promises"] == [PROMISE]


async def test_nothing_leaves_without_the_secret_or_the_authors_consent(connected) -> None:
    core, clients = FakeCore(), _clients()
    promise_client.set_extractor(None)  # not connected
    payload = event("I'll do it tomorrow")
    assert await slack.process(None, payload, org_id=ORG, clients=clients, core=core) == "skipped"  # type: ignore[arg-type]
    assert clients.slack.calls == [] and core.checks == []

    extractor = FakeExtractor(Extraction("ok", "promise_extract@v1", [PROMISE]))
    promise_client.set_extractor(extractor)  # type: ignore[arg-type]
    off = FakeCore(enabled=False)
    assert await slack.process(None, payload, org_id=ORG, clients=clients, core=off) == "skipped"  # type: ignore[arg-type]
    assert off.checks == ["maya@example.com"] and extractor.asked == []

    stranger = FakeCore(user=None, enabled=False)
    out = await slack.process(None, payload, org_id=ORG, clients=clients, core=stranger)  # type: ignore[arg-type]
    assert out == "skipped"
    assert extractor.asked == []


async def test_no_promise_or_a_budget_stop_writes_nothing(connected) -> None:
    for answer in (Extraction("ok", "p", []), Extraction("budget_denied", None, [PROMISE])):
        promise_client.set_extractor(FakeExtractor(answer))  # type: ignore[arg-type]
        core = FakeCore()
        msg = event("I'll look")
        out = await slack.process(None, msg, org_id=ORG, clients=_clients(), core=core)  # type: ignore[arg-type]
        assert out == "processed" and core.captured == []


async def test_uninstall_still_works_and_other_events_skip() -> None:
    assert await slack.process(None, {"event": {"type": "reaction_added"}}) == "skipped"  # type: ignore[arg-type]
    # Without the worker's context a message is skipped, never a crash.
    assert await slack.process(None, event("I'll do it")) == "skipped"  # type: ignore[arg-type]


async def test_the_extractor_client_speaks_the_rag_service_contract() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["headers"] = dict(request.headers)
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200, json={"status": "ok", "prompt": "promise_extract@v1", "promises": [PROMISE, 3]}
        )

    client = PromiseExtractor("http://rag.test/", "s" * 40, transport=httpx.MockTransport(handler))
    out = await client.extract(
        ORG,
        channel="slack",
        text="I'll do it",
        author_name="Maya",
        occurred_at=None,
        reference_date="2026-11-11",
        people=[],
    )
    assert seen["url"] == "http://rag.test/internal/promises/extract"
    assert seen["headers"]["x-mytheus-service"] == "integration-service"
    assert seen["headers"]["x-mytheus-org"] == str(ORG)
    assert seen["body"]["reference_date"] == "2026-11-11"
    assert out == Extraction("ok", "promise_extract@v1", [PROMISE])

    down = PromiseExtractor(
        "http://rag.test", "s" * 40, transport=httpx.MockTransport(lambda r: httpx.Response(503))
    )
    with pytest.raises(ProviderError):
        await down.extract(
            ORG,
            channel="slack",
            text="x",
            author_name=None,
            occurred_at=None,
            reference_date="2026-11-11",
            people=[],
        )


def test_the_extractor_is_not_connected_without_its_secret(monkeypatch) -> None:
    monkeypatch.delenv("MYTHEUS_RAG_CALLER_TOKENS", raising=False)
    promise_client._configured = False
    promise_client._extractor = None
    assert promise_client.get_extractor() is None
    monkeypatch.setenv(
        "MYTHEUS_RAG_CALLER_TOKENS", f"bff={'b' * 40},integration-service={'i' * 40}"
    )
    promise_client._configured = False
    assert promise_client.get_extractor() is not None
    promise_client.set_extractor(None)


# --- one message, two passes (decision check + promise capture) ------------------------------


@dataclass
class TwoPassSlack(FakeSlackApi):
    ephemeral: list[str] = field(default_factory=list)

    async def user_email(self, token: str, user_id: str) -> str | None:
        return "maya@example.com"

    async def channel_name(self, token: str, channel: str) -> str | None:
        return "pricing"

    async def post_ephemeral(self, token: str, **kw: Any) -> dict[str, Any]:
        self.ephemeral.append(str(kw.get("text")))
        return {}


@dataclass
class TwoPassCore(FakeCore):
    fail_check: bool = False
    checked: list[str] = field(default_factory=list)

    async def decision_check(self, org_id: uuid.UUID, body: dict[str, Any]) -> dict[str, Any]:
        if self.fail_check:
            raise CoreApiError(500, "HTTP 500")
        self.checked.append(body["text"])
        return {
            "conflicts": [
                {
                    "decision_content": "Annual billing only",
                    "quote": "monthly",
                    "challenge_id": str(uuid.uuid4()),
                }
            ]
        }


def channel_event(text: str) -> dict[str, Any]:
    payload = event(text)
    payload["event"]["channel_type"] = "channel"
    return payload


async def test_a_channel_message_gets_both_passes(connected) -> None:
    promise_client.set_extractor(FakeExtractor(Extraction("ok", "p", [PROMISE])))  # type: ignore[arg-type]
    core = TwoPassCore()
    clients = FakeClients(TwoPassSlack(_clients().slack.users))
    text = "I'll move Acme to monthly billing and send the new quote by Friday"
    out = await slack.process(None, channel_event(text), org_id=ORG, clients=clients, core=core)  # type: ignore[arg-type]
    assert out == "processed"
    assert len(core.checked) == 1 and len(clients.slack.ephemeral) == 1  # decision check
    assert len(core.captured) == 1  # promise capture


async def test_one_failing_pass_never_blocks_the_other(connected) -> None:
    promise_client.set_extractor(FakeExtractor(Extraction("ok", "p", [PROMISE])))  # type: ignore[arg-type]
    core = TwoPassCore(fail_check=True)
    clients = FakeClients(TwoPassSlack(_clients().slack.users))
    text = "I'll move Acme to monthly billing and send the new quote by Friday"
    out = await slack.process(None, channel_event(text), org_id=ORG, clients=clients, core=core)  # type: ignore[arg-type]
    assert out == "processed" and len(core.captured) == 1

    class Down:
        async def extract(self, *_a: Any, **_k: Any) -> Extraction:
            raise ProviderError("intelligence", "extract_promises", "http_503", 503)

    promise_client.set_extractor(Down())  # type: ignore[arg-type]
    ok_core = TwoPassCore()
    out = await slack.process(None, channel_event(text), org_id=ORG, clients=clients, core=ok_core)  # type: ignore[arg-type]
    assert out == "processed" and len(ok_core.checked) == 1  # the check still ran and answered

    # Nothing acted and something failed: the row is retried.
    with pytest.raises(CoreApiError):
        await slack.process(
            None,
            channel_event(text),
            org_id=ORG,
            clients=clients,
            core=TwoPassCore(fail_check=True),
        )  # type: ignore[arg-type]


async def test_a_dm_promise_is_captured_but_never_decision_checked(connected) -> None:
    promise_client.set_extractor(FakeExtractor(Extraction("ok", "p", [PROMISE])))  # type: ignore[arg-type]
    core = TwoPassCore()
    payload = event("I'll send you the revised deck by Friday, promise")
    payload["event"]["channel_type"] = "im"
    clients = FakeClients(TwoPassSlack(_clients().slack.users))
    out = await slack.process(None, payload, org_id=ORG, clients=clients, core=core)  # type: ignore[arg-type]
    assert out == "processed" and core.checked == [] and len(core.captured) == 1
