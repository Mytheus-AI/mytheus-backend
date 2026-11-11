"""The MCP server (bff `POST /mcp`), driven by the official MCP Python client (Phase 4).

End to end in one process: the MCP client (`mcp.client.streamable_http`) talks Streamable HTTP
to the real bff app (factory middleware, personal access token, no auth override); the tools
call the real core-api app over HTTP as the caller; rag-service is a fake that checks bff's
service headers. Real Postgres.

1. initialize negotiates a supported version; tools and resource templates are listed.
2. search, meeting outcome, decision record, open actions and promises return the caller's
   own data, and a meeting the caller was not in is a tool error, not data.
3. create_action with a read_write token creates a CONFIRMED task assigned to the caller (and
   its `actions` outbox row); with a read token it is a tool error and nothing is written.
4. ask_meetings answers through rag-service with bff's service credentials and the caller's
   user id; with no credentials configured it says Ask is not connected.
5. resources/list lists the caller's meetings; resources/read returns the recap as Markdown.
6. Protocol edges: no/invalid token is 401; GET is 405; a foreign Origin is 403; an unknown
   protocol version header is 400; an unknown method is a JSON-RPC error.
"""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager

import httpx
import httpx2
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client

from libs.shared import db as db_mod
from libs.shared.app_factory import create_app
from services.bff import ask as ask_mod
from services.bff import mcp as mcp_mod
from services.core_api.routes import action_create, commitments, my_tasks, promises, public_api
from tests.conftest import APP_URL
from tests.test_api_tokens import mint
from tests.test_chat_api import Tenant, _sql, seed_tenant
from tests.test_share_links import action, decision, meeting_with

RAG_SECRET = "r" * 40


@pytest.fixture
def apps(migrated_db, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", APP_URL)
    monkeypatch.setenv("CORE_API_URL", "http://core.test")
    monkeypatch.setenv("APP_BASE_URL", "https://app.example")
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()
    core = create_app("core-api-mcp-test")
    for r in (
        public_api.router,
        my_tasks.router,
        commitments.router,
        action_create.router,
        promises.router,
    ):
        core.include_router(r)
    bff = create_app("bff-mcp-test")
    bff.include_router(mcp_mod.router)
    bff.include_router(ask_mod.router)
    mcp_mod.set_core_transport(httpx.ASGITransport(app=core))
    yield bff
    mcp_mod.set_core_transport(None)
    ask_mod.set_transport(None)
    db_mod.get_engine.cache_clear()
    db_mod.get_sessionmaker.cache_clear()


@asynccontextmanager
async def session_for(bff, token: str):
    client = httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=bff),
        base_url="http://bff.test",
        headers={"Authorization": f"Bearer {token}"},
    )
    async with client, streamable_http_client("http://bff.test/mcp", http_client=client) as streams:
        read, write = streams[0], streams[1]
        async with ClientSession(read, write) as session:
            await session.initialize()
            yield session


def _seed() -> tuple[Tenant, uuid.UUID, uuid.UUID, uuid.UUID]:
    me = seed_tenant()
    m = meeting_with(me)
    d = decision(me, m, "Adopt the Atlas pricing")
    action(me, m, "Draft the Atlas FAQ")
    _sql(
        "INSERT INTO commitments (org_id, user_id, meeting_id, content, source_window_id) "
        "VALUES (%(o)s, %(u)s, %(m)s, 'I will call Acme', 'w1')",
        {"o": str(me.org_id), "u": str(me.user_id), "m": str(m)},
    )
    other = seed_tenant()
    foreign = meeting_with(other)
    return me, m, d, foreign


def _text(result) -> str:
    return "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")


async def test_tools_end_to_end_as_the_token_owner(apps):
    me, m, d, foreign = _seed()
    async with session_for(apps, mint(me, scope="read")) as s:
        tools = {t.name for t in (await s.list_tools()).tools}
        assert tools == {
            "search_meetings",
            "ask_meetings",
            "get_meeting_outcome",
            "get_decision_record",
            "list_my_open_actions",
            "list_my_promises",
            "check_my_capacity",
            "create_action",
            "add_promise",
        }
        found = await s.call_tool("search_meetings", {"query": "atlas"})
        assert not found.is_error
        kinds = {h["kind"] for h in found.structured_content["items"]}
        assert kinds == {"decision", "action"}
        outcome = await s.call_tool("get_meeting_outcome", {"meeting_id": str(m)})
        assert "Adopt the Atlas pricing" in _text(outcome) and "Draft the Atlas FAQ" in _text(
            outcome
        )
        record = await s.call_tool("get_decision_record", {"decision_id": str(d)})
        assert record.structured_content["decision_id"] == str(d)
        mine = await s.call_tool("list_my_open_actions", {})
        assert "Draft the Atlas FAQ" in _text(mine)
        promises = await s.call_tool("list_my_promises", {})
        assert "I will call Acme" in _text(promises) and "[Pricing review]" in _text(promises)
        week = await s.call_tool("check_my_capacity", {"timezone": "UTC"})
        assert not week.is_error and len(week.structured_content["days"]) == 7
        day = await s.call_tool("check_my_capacity", {"due_date": "2026-11-13"})
        assert not day.is_error and "things due" in _text(day)
        theirs = await s.call_tool("get_meeting_outcome", {"meeting_id": str(foreign)})
        assert theirs.is_error and "not from a meeting you took part in" in _text(theirs)


async def test_create_action_needs_a_read_write_token_and_creates_a_confirmed_task(apps):
    me, m, _, _ = _seed()
    async with session_for(apps, mint(me, scope="read")) as s:
        refused = await s.call_tool("create_action", {"content": "Send the contract"})
        assert refused.is_error and "read-only" in _text(refused)
    assert (
        _sql(
            "SELECT 1 FROM action_items WHERE org_id = %(o)s AND content = 'Send the contract'",
            {"o": str(me.org_id)},
        )
        == []
    )
    async with session_for(apps, mint(me, scope="read_write")) as s:
        made = await s.call_tool(
            "create_action",
            {"content": "Send the contract", "due_date": "2026-12-01", "meeting_id": str(m)},
        )
        assert not made.is_error, _text(made)
    [row] = _sql(
        "SELECT id, status, assignee_id, confirmed_by, meeting_id FROM action_items "
        "WHERE org_id = %(o)s AND content = 'Send the contract'",
        {"o": str(me.org_id)},
    )
    assert row["status"] == "confirmed" and str(row["assignee_id"]) == str(me.user_id)
    assert str(row["confirmed_by"]) == str(me.user_id) and str(row["meeting_id"]) == str(m)
    outbox = _sql(
        "SELECT payload FROM outbox WHERE idempotency_key = %(k)s",
        {"k": f"action:{row['id']}:confirmed"},
    )
    assert len(outbox) == 1
    audit = _sql(
        "SELECT metadata FROM audit_logs WHERE action = 'action.created' AND target_id = %(t)s",
        {"t": str(row["id"])},
    )
    assert audit[0]["metadata"]["via"] == "api_token"


async def test_ask_goes_to_rag_service_as_bff_for_the_caller(apps, monkeypatch):
    me, m, d, _ = _seed()
    seen: dict = {}

    def rag(request: httpx.Request) -> httpx.Response:
        seen.update(headers=dict(request.headers), body=json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "answer_passages": [
                    {
                        "n": 1,
                        "text": "We adopted the Atlas pricing.",
                        "rerank_score": 0.9,
                        "citation": {
                            "kind": "decision",
                            "meeting_id": str(m),
                            "decision_id": str(d),
                        },
                        "meeting_title": "Pricing review",
                    }
                ],
                "answer": {
                    "text": "Atlas pricing was adopted [1].",
                    "abstained": False,
                    "cited": [1],
                },
            },
        )

    async with session_for(apps, mint(me)) as s:
        missing = await s.call_tool("ask_meetings", {"question": "What pricing?"})
        assert missing.is_error and "not connected" in _text(missing)
        monkeypatch.setenv(ask_mod.TOKENS_ENV, f"bff={RAG_SECRET}")
        ask_mod.set_transport(httpx.MockTransport(rag))
        answered = await s.call_tool("ask_meetings", {"question": "What pricing?"})
    assert not answered.is_error
    assert "Atlas pricing was adopted" in _text(answered) and "[1]" in _text(answered)
    assert seen["headers"]["x-mytheus-service"] == "bff"
    assert seen["headers"]["x-mytheus-service-token"] == RAG_SECRET
    assert seen["headers"]["x-mytheus-org"] == str(me.org_id)
    assert seen["headers"]["x-mytheus-user"] == str(me.user_id)
    assert seen["body"] == {"query": "What pricing?", "generate": True}


async def test_resources_are_meeting_summaries(apps):
    me, m, d, _ = _seed()
    async with session_for(apps, mint(me)) as s:
        templates = await s.list_resource_templates()
        assert {t.uri_template for t in templates.resource_templates} == {
            "mytheus://meetings/{meeting_id}/recap",
            "mytheus://decisions/{decision_id}/record",
        }
        listed = await s.list_resources()
        assert [str(r.uri) for r in listed.resources] == [f"mytheus://meetings/{m}/recap"]
        read = await s.read_resource(f"mytheus://meetings/{m}/recap")
        text = read.contents[0].text
        assert text.startswith("# Pricing review") and "Adopt the Atlas pricing" in text
        record = await s.read_resource(f"mytheus://decisions/{d}/record")
        assert "Adopt the Atlas pricing" in record.contents[0].text


async def test_protocol_edges(apps):
    me, *_ = _seed()
    token = mint(me)
    init = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "t", "version": "1"},
        },
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=apps), base_url="http://bff.test"
    ) as c:
        auth = {"Authorization": f"Bearer {token}", "Accept": "application/json, text/event-stream"}
        assert (await c.post("/mcp", json=init)).status_code == 401
        bad = {"Authorization": "Bearer mytheus_pat_nope"}
        assert (await c.post("/mcp", json=init, headers=bad)).status_code == 401
        ok = await c.post("/mcp", json=init, headers=auth)
        assert ok.status_code == 200 and ok.json()["result"]["protocolVersion"] == "2025-06-18"
        assert "mcp-session-id" not in ok.headers
        assert (await c.get("/mcp", headers=auth)).status_code == 405
        foreign = {**auth, "Origin": "https://evil.example"}
        assert (await c.post("/mcp", json=init, headers=foreign)).status_code == 403
        same = {**auth, "Origin": "https://app.example"}
        assert (await c.post("/mcp", json=init, headers=same)).status_code == 200
        old = {**auth, "MCP-Protocol-Version": "1999-01-01"}
        assert (await c.post("/mcp", json=init, headers=old)).status_code == 400
        note = {"jsonrpc": "2.0", "method": "notifications/initialized"}
        assert (await c.post("/mcp", json=note, headers=auth)).status_code == 202
        unknown = await c.post(
            "/mcp", json={"jsonrpc": "2.0", "id": 2, "method": "nope"}, headers=auth
        )
        assert unknown.json()["error"]["code"] == -32601
        garbage = await c.post("/mcp", content=b"{not json", headers=auth)
        assert garbage.status_code == 400 and garbage.json()["error"]["code"] == -32700


def test_env_is_not_leaking_into_other_tests():
    assert os.environ.get(ask_mod.TOKENS_ENV) in (None, "")


async def test_add_promise_needs_a_read_write_token_and_warns_about_a_full_day(apps):
    me, _, _, _ = _seed()
    for i in range(6):
        _sql(
            "INSERT INTO commitments (org_id, user_id, content, due_at, channel, "
            "source_window_id) VALUES (%(o)s, %(u)s, %(c)s, '2030-01-04', 'manual', %(w)s)",
            {"o": str(me.org_id), "u": str(me.user_id), "c": f"Busy {i}", "w": f"m{i}"},
        )
    async with session_for(apps, mint(me, scope="read")) as s:
        refused = await s.call_tool("add_promise", {"content": "Send the deck"})
        assert refused.is_error and "read-only" in _text(refused)
    async with session_for(apps, mint(me, scope="read_write")) as s:
        added = await s.call_tool(
            "add_promise",
            {"content": "Send the deck", "due_date": "2030-01-04", "recipient": "Acme"},
        )
        assert not added.is_error, _text(added)
        assert "Added to your promises: Send the deck, due 2030-01-04" in _text(added)
        assert "That's your seventh promise due" in _text(added)
        assert added.structured_content["promise"]["recipient_name"] == "Acme"
