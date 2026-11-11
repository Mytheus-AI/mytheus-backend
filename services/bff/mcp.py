"""The Mytheus MCP server: ask Mytheus from Claude, IDEs and other MCP clients (Phase 4).

Transport: MCP Streamable HTTP at `POST /mcp`, stateless, JSON responses (the server never
opens an SSE stream, which the transport allows: a server may answer a request with a single
`application/json` body). No `Mcp-Session-Id` is issued, so every POST stands alone and any
bff replica can serve it. `GET` and `DELETE` are 405. Protocol versions 2024-11-05,
2025-03-26, 2025-06-18 and 2025-11-25 are negotiated; an unknown `MCP-Protocol-Version`
header is a 400.

Auth: `Authorization: Bearer mytheus_pat_...` (a personal access token, or an OAuth access
token in the same format that identity-tenant-service's authorization server minted for a
Claude custom connector) or a Clerk session, resolved by the same chain as every route
(`require_tier("member")`). Discovery for OAuth clients is `oauth_metadata.py`; the gateway
answers an unauthenticated `/mcp` with `WWW-Authenticate: Bearer resource_metadata=...`. A
browser `Origin` other than the web app's is refused (403), the transport's DNS-rebinding guard.

Why a route group in bff and not a new service: MCP here is a client-shaped aggregation over
the public API, exactly bff's job, and it needs no deployable, secret or platform change of
its own. The tools call the public API AS THE CALLER: core-api over HTTP with the caller's own
bearer (`CORE_API_URL`, default `http://core-api:8000`), and ask through `bff.ask` (rag-service
on the caller's behalf). So a tool can never see or do more than the same token can through
REST, and bff stays a non-writer: `create_action` is core-api's `POST /api/v1/actions`.

Tools: `search_meetings`, `ask_meetings`, `get_meeting_outcome` (decisions, actions, agenda:
the minutes), `get_decision_record`, `company_as_of` (the time machine: what was decided,
believed and promised on a date, optionally for one topic), `list_my_open_actions`,
`list_my_promises` (the promise ledger, every channel), `check_my_capacity` (the capacity
check before a deadline is accepted), `check_against_decisions` (a draft checked against the
confirmed decisions of the caller's meetings: core-api's `POST /api/v1/decisions/check`,
read-only),
`create_action` (a confirmed task for the caller, the caller's own explicit request; needs a
`read_write` token) and `add_promise` (a manual promise in the caller's ledger; `read_write`).
Resources: `mytheus://meetings/{meeting_id}/recap` (a meeting summary as
Markdown, listed for the caller's recent meetings) and
`mytheus://decisions/{decision_id}/record`.
"""

from __future__ import annotations

import json
import logging
import os
import re
import uuid
from typing import Annotated, Any

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, Response

from libs.shared.authz import RequestContext, require_tier
from libs.shared.errors import ErrorCode, ProblemDetail
from services.bff import ask as ask_mod

logger = logging.getLogger(__name__)

router = APIRouter(tags=["mcp"])

SERVER_NAME = "mytheus"
SERVER_VERSION = "1.0.0"
SUPPORTED_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
LATEST_VERSION = SUPPORTED_VERSIONS[0]
TIMEOUT = httpx.Timeout(30.0, connect=5.0)

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
RESOURCE_NOT_FOUND = -32002

INSTRUCTIONS = (
    "Mytheus is your team's meeting memory: decisions with their history, actions with owners, "
    "promises, and meeting outcomes. Search or ask first, then open a meeting's outcome or a "
    "decision record by id. Everything is limited to meetings you took part in. "
    "Before agreeing to a deadline for the user, check_my_capacity says whether that day is "
    "already full. create_action and add_promise write immediately; only call them when the "
    "user asked."
)

_READ_ONLY = (
    "This access is read-only. Use a read_write API token, or reconnect the app and allow it "
    "to read and act."
)

_core_transport: httpx.AsyncBaseTransport | None = None


def set_core_transport(transport: httpx.AsyncBaseTransport | None) -> None:
    """Tests route the tools' core-api calls to an in-process app."""
    global _core_transport
    _core_transport = transport


# --- tool and resource catalogue -----------------------------------------------------------------

_UUID = {"type": "string", "format": "uuid"}

TOOLS: list[dict[str, Any]] = [
    {
        "name": "search_meetings",
        "title": "Search meetings",
        "description": (
            "Find meetings (by title or agenda item), confirmed decisions and actions that "
            "mention some text, newest first. Omit `query` to list your recent meetings."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "minLength": 2, "maxLength": 200},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50, "default": 10},
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "ask_meetings",
        "title": "Ask across meetings",
        "description": (
            "Ask a question and get an answer grounded in your meetings (transcripts, "
            "decisions, actions, shared documents) with numbered citations."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "minLength": 1, "maxLength": 1000},
                "meeting_id": {**_UUID, "description": "Optional: only this meeting."},
            },
            "required": ["question"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "get_meeting_outcome",
        "title": "Get a meeting's decisions, actions and minutes",
        "description": (
            "A meeting's confirmed outcome: agenda items with status, decisions, and actions "
            "with owners and due dates."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"meeting_id": _UUID},
            "required": ["meeting_id"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "get_decision_record",
        "title": "Get a decision with its history",
        "description": "A confirmed decision, what it replaced and what replaced it.",
        "inputSchema": {
            "type": "object",
            "properties": {"decision_id": _UUID},
            "required": ["decision_id"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "company_as_of",
        "title": "See the company as of a date",
        "description": (
            "The company's decision graph as it stood at the end of a date: decisions in force "
            "with why and the words that decided them, what had been replaced by then, open "
            'promises with owners, and goals. Optionally only one topic ("pricing"). Limited '
            "to meetings you took part in."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "format": "date"},
                "topic": {"type": "string", "maxLength": 120},
            },
            "required": ["date"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "list_my_open_actions",
        "title": "List my open actions",
        "description": "Your confirmed, not yet done actions, soonest due first.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "list_my_promises",
        "title": "List my promises",
        "description": (
            "Things you promised, from every channel: meetings, phone recordings, Slack, email "
            "and ones you added (private to you), with their due dates and where they were made."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {
                    "type": "string",
                    "enum": ["open", "done", "dismissed"],
                    "default": "open",
                }
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "check_against_decisions",
        "title": "Check a draft against decisions",
        "description": (
            "Before you send or publish a draft (a message, a doc, a plan, a reply), check whether "
            "it contradicts or reopens a decision your team already confirmed in a meeting you "
            "took part in. Returns each conflicting decision with the conflicting quote, the "
            "meeting, the date, the owner, a confidence and why. Read-only; the draft is not "
            "stored. An empty result means no conflict was found, not that none exists."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "minLength": 1, "maxLength": 4000},
                "topic": {
                    "type": "string",
                    "maxLength": 200,
                    "description": "Optional: what the draft is about.",
                },
                "place": {
                    "type": "string",
                    "maxLength": 120,
                    "description": "Optional: where it will go, e.g. 'email to Acme'.",
                },
            },
            "required": ["text"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "check_my_capacity",
        "title": "Check my capacity",
        "description": (
            "Before saying yes to a deadline: how full a day is (promises and actions due, "
            "meeting and focus time) and, when it is tight, the first day with room. Without "
            "`due_date`, your week day by day. Private to you."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "due_date": {
                    "type": "string",
                    "format": "date",
                    "description": "The date someone asked for (YYYY-MM-DD).",
                },
                "days": {"type": "integer", "minimum": 1, "maximum": 14, "default": 7},
                "timezone": {
                    "type": "string",
                    "maxLength": 64,
                    "description": "Your IANA time zone, e.g. Europe/London. Default UTC.",
                },
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "add_promise",
        "title": "Add a promise",
        "description": (
            "Write down a promise you made (to a colleague, a customer, anyone) in your private "
            "promise ledger, with an optional due date; the answer says whether that day is "
            "already full. Only call this when the user asked."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "minLength": 1, "maxLength": 500},
                "due_date": {"type": "string", "format": "date"},
                "recipient_id": {**_UUID, "description": "A colleague it was promised to."},
                "recipient": {
                    "type": "string",
                    "maxLength": 200,
                    "description": "Or who, in words (a customer, a team).",
                },
                "timezone": {"type": "string", "maxLength": 64},
            },
            "required": ["content"],
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    },
    {
        "name": "create_action",
        "title": "Create a task",
        "description": (
            "Record a confirmed task (for you unless assignee_id is given), optionally tied to "
            "a meeting you were in. It is created immediately and appears in My Tasks, "
            "reminders and Jira. Only call this when the user explicitly asks."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "minLength": 1, "maxLength": 500},
                "due_date": {"type": "string", "format": "date"},
                "assignee_id": _UUID,
                "meeting_id": _UUID,
            },
            "required": ["content"],
            "additionalProperties": False,
        },
        "annotations": {
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    },
]
_TOOL_NAMES = {t["name"] for t in TOOLS}

RESOURCE_TEMPLATES: list[dict[str, Any]] = [
    {
        "uriTemplate": "mytheus://meetings/{meeting_id}/recap",
        "name": "meeting-recap",
        "title": "Meeting summary",
        "description": "A meeting's confirmed decisions, actions and agenda, as Markdown.",
        "mimeType": "text/markdown",
    },
    {
        "uriTemplate": "mytheus://decisions/{decision_id}/record",
        "name": "decision-record",
        "title": "Decision record",
        "description": "A decision with its lineage, as Markdown.",
        "mimeType": "text/markdown",
    },
]
_RECAP_URI = re.compile(r"^mytheus://meetings/([0-9a-fA-F-]{36})/recap$")
_RECORD_URI = re.compile(r"^mytheus://decisions/([0-9a-fA-F-]{36})/record$")


# --- the public API, as the caller ---------------------------------------------------------------


class ToolError(Exception):
    """A tool failed in a way the model should see (MCP `isError: true`), not a protocol error."""


class CoreClient:
    def __init__(self, authorization: str) -> None:
        self._auth = authorization
        self._base = os.environ.get("CORE_API_URL", "http://core-api:8000").rstrip("/")

    async def call(
        self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None
    ) -> Any:
        try:
            async with httpx.AsyncClient(transport=_core_transport, timeout=TIMEOUT) as client:
                res = await client.request(
                    method,
                    f"{self._base}{path}",
                    params=params,
                    json=json_body,
                    headers={"Authorization": self._auth},
                )
        except httpx.HTTPError:
            raise ToolError("Mytheus is unavailable right now; try again shortly.") from None
        if res.status_code in (403, 404):
            reason = _problem(res).get("reason")
            if reason == "read_only_token":
                raise ToolError(_READ_ONLY)
            raise ToolError("Not found, or not from a meeting you took part in.")
        if res.status_code == 422:
            raise ToolError(f"Invalid input: {_problem(res).get('detail', 'validation failed')}")
        if res.status_code >= 400:
            raise ToolError(_problem(res).get("detail") or "Mytheus could not do that.")
        return res.json()


def _problem(res: httpx.Response) -> dict[str, Any]:
    try:
        body = res.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


# --- rendering -----------------------------------------------------------------------------------


def _line_action(a: dict[str, Any]) -> str:
    owner = f" ({a['owner_name']})" if a.get("owner_name") else ""
    due = f", due {a['due_date']}" if a.get("due_date") else ""
    done = " [done]" if a.get("status") == "done" else ""
    return f"- {a['content']}{owner}{due}{done} `{a['id']}`"


def _line_promise(c: dict[str, Any]) -> str:
    due = (
        f", due {c['due_at']}"
        if c.get("due_at")
        else (f" ({c['due_hint']})" if c.get("due_hint") else "")
    )
    to = f" to {c['recipient_name']}" if c.get("recipient_name") else ""
    source = c.get("source") or {}
    where = f" [{source.get('label')}]" if source.get("label") else ""
    review = (
        " (found by your agent, not confirmed yet)" if c.get("review_state") == "pending" else ""
    )
    return f"- {c['content']}{to}{due}{where}{review} `{c['id']}`"


def _capacity_check_text(body: dict[str, Any]) -> str:
    lines = [str(body.get("message") or "")]
    lines.append(
        f"{body.get('items_on_day')} things due {body.get('due_label')} counting this one; "
        f"{round(int(body.get('meeting_minutes') or 0) / 60, 1)} h of meetings. "
        f"Level: {body.get('level')}."
    )
    if body.get("suggested_date"):
        lines.append(f"First day with room: {body['suggested_date']} ({body['suggested_label']}).")
    return "\n".join(lines)


def _capacity_text(body: dict[str, Any]) -> str:
    lines = []
    for d in body.get("days", []):
        meetings = round(int(d.get("meeting_minutes") or 0) / 60, 1)
        focus = round(int(d.get("focus_minutes") or 0) / 60, 1)
        extra = f", {focus} h focus" if focus else ""
        away = ", away" if d.get("away") else ""
        lines.append(
            f"- {d['date']} ({d['label']}): {d['items']} due, {meetings} h meetings{extra}{away} "
            f"-> {d['level']}"
        )
    if body.get("overdue"):
        lines.append(f"Overdue: {body['overdue']}.")
    if not body.get("calendar_synced"):
        lines.append("Only Mytheus meetings are counted: Google Calendar is not synced for you.")
    return "\n".join(lines) or "Nothing scheduled."


def recap_markdown(r: dict[str, Any]) -> str:
    lines = [f"# {r['title']}", ""]
    when = r.get("scheduled_start")
    meta = [f"Status: {r.get('status')}"]
    if when:
        meta.append(f"Start: {when}")
    if r.get("host_name"):
        meta.append(f"Host: {r['host_name']}")
    lines += [" · ".join(meta), ""]
    if r.get("agenda"):
        lines.append("## Agenda")
        lines += [f"- {i['title']} ({i['status']})" for i in r["agenda"]]
        lines.append("")
    lines.append("## Decisions")
    lines += [f"- {d['content']} `{d['id']}`" for d in r.get("decisions", [])] or ["- None yet"]
    lines += ["", "## Actions"]
    lines += [_line_action(a) for a in r.get("actions", [])] or ["- None yet"]
    return "\n".join(lines)


def record_markdown(d: dict[str, Any]) -> str:
    lines = [f"# Decision: {d['content']}", "", f"Status: {d['status']} · {d['meeting_title']}"]
    if d.get("rationale"):
        lines += ["", f"Why: {d['rationale']}"]
    lines += ["", "## History (oldest first)"]
    for s in d.get("lineage", []):
        if not s.get("visible"):
            lines.append("- (a decision from a meeting you were not in)")
            continue
        marker = " <- this decision" if s["relation"] == "subject" else ""
        lines.append(f"- {s['content']} ({s.get('status')}){marker}")
    if d.get("meeting_actions"):
        lines += ["", "## Actions from that meeting"]
        lines += [_line_action(a) for a in d["meeting_actions"]]
    return "\n".join(lines)


def check_markdown(body: dict[str, Any]) -> str:
    conflicts = body.get("conflicts") or []
    if body.get("status") == "skipped" and body.get("reason") == "never_record":
        return "Not checked: the draft names a topic your workspace never records."
    if body.get("status") == "skipped":
        return "Not checked: the draft is too short to compare with decisions."
    if not conflicts:
        degraded = body.get("status") == "degraded"
        return (
            "Not checked fully: the workspace's AI budget is used up for now."
            if degraded
            else "No conflict with a confirmed decision from meetings you took part in."
        )
    lines = [f"{len(conflicts)} confirmed decision(s) conflict with this draft:", ""]
    for c in conflicts:
        when = f" on {c['decided_at']}" if c.get("decided_at") else ""
        where = f" ({c['meeting_title']})" if c.get("meeting_title") else ""
        owner = f", owner {c['owner']['name']}" if (c.get("owner") or {}).get("name") else ""
        verb = "contradicts" if c.get("relation") == "contradicts" else "reopens"
        lines += [
            f"- Decided{when}{where}{owner}: {c['decision_content']} `{c['decision_id']}`",
            f'  The draft {verb} it: "{c["quote"]}"',
            f"  Why: {c.get('why') or 'it goes against the decision'} "
            f"(confidence {float(c.get('confidence', 0)):.2f})",
        ]
    return "\n".join(lines)


def as_of_markdown(s: dict[str, Any]) -> str:
    """The time machine's snapshot as Markdown, for `company_as_of`."""
    head = f"# The company as of {s['as_of']}" + (
        f" (topic: {s['topic']})" if s.get("topic") else ""
    )
    c = s.get("counts") or {}
    lines = [
        head,
        "",
        f"{c.get('in_force', 0)} decisions in force, {c.get('replaced', 0)} replaced by then, "
        f"{c.get('promises_open', 0)} open promises ({c.get('promises_overdue', 0)} overdue), "
        f"{c.get('goals_active', 0)} active goals. Only meetings you were in are included.",
        "",
        "## Decided and in force",
    ]
    for d in (s.get("decisions") or [])[:40]:
        line = f"- {d['content']} ({d['meeting']['title']}, {str(d['decided_at'])[:10]})"
        if d.get("rationale"):
            line += f". Why: {d['rationale']}"
        if d.get("quote"):
            who = f" {d['quote_speaker_name']}:" if d.get("quote_speaker_name") else ""
            line += f'. Said:{who} "{d['quote']}"'
        if d.get("bet"):
            line += f". Expected: {d['bet']['statement']}"
        lines.append(line + f" `{d['id']}`")
    if not s.get("decisions"):
        lines.append("- None")
    if s.get("replaced"):
        lines += ["", "## Replaced by then"]
        for r in s["replaced"][:20]:
            by = r["replaced_by"]
            what = (
                by.get("content")
                if by.get("visible")
                else "a decision from a meeting you were not in"
            )
            lines.append(f"- {r['content']}: replaced on {str(r['replaced_at'])[:10]} by {what}")
    lines += ["", "## Open promises"]
    for p in (s.get("promises") or [])[:30]:
        due = f", due {p['due']}" if p.get("due") else ""
        late = " (overdue)" if p.get("overdue") else ""
        lines.append(f"- {p['content']} ({p.get('owner_name') or 'Unassigned'}{due}){late}")
    if not s.get("promises"):
        lines.append("- None")
    if s.get("goals"):
        lines += ["", "## Goals"]
        lines += [
            f"- {g['title']}"
            + (f" ({g['period']})" if g.get("period") else "")
            + f": {g['status']}"
            for g in s["goals"]
        ]
    return "\n".join(lines)


def _search_markdown(body: dict[str, Any]) -> str:
    items = body.get("items", [])
    if not items:
        return "No matches in meetings you took part in."
    out = []
    for h in items:
        where = (
            f" (in {h['meeting_title']})"
            if h.get("meeting_title") and h["kind"] != "meeting"
            else ""
        )
        out.append(f"- [{h['kind']}] {h['text']}{where} `{h['id']}` {h['occurred_at'][:10]}")
    return "\n".join(out)


# --- tools ---------------------------------------------------------------------------------------


def _uuid_arg(args: dict[str, Any], key: str, *, required: bool = True) -> uuid.UUID | None:
    raw = args.get(key)
    if raw is None and not required:
        return None
    try:
        return uuid.UUID(str(raw))
    except ValueError:
        raise ToolError(f"`{key}` must be a UUID.") from None


async def call_tool(
    name: str, args: dict[str, Any], ctx: RequestContext, core: CoreClient
) -> tuple[str, Any]:
    """Run one tool -> (Markdown text, structured result)."""
    if name == "search_meetings":
        params: dict[str, Any] = {"limit": int(args.get("limit") or 10)}
        if args.get("query"):
            params["q"] = str(args["query"])
        body = await core.call("GET", "/api/v1/search", params=params)
        return _search_markdown(body), body
    if name == "ask_meetings":
        question = str(args.get("question") or "").strip()
        if not question:
            raise ToolError("`question` is required.")
        try:
            answer = await ask_mod.ask(ctx, question, _uuid_arg(args, "meeting_id", required=False))
        except ProblemDetail as exc:
            raise ToolError(
                "Ask is not connected on this server yet."
                if exc.extra.get("reason") == "ask_not_connected"
                else exc.detail
            ) from None
        text = answer.answer or "No grounded answer: the meetings you took part in do not say."
        cites = [f"[{c.n}] {c.meeting_title or c.kind}: {c.text[:200]}" for c in answer.citations]
        return "\n\n".join([text, *cites]) if cites else text, answer.model_dump(mode="json")
    if name == "get_meeting_outcome":
        meeting = _uuid_arg(args, "meeting_id")
        body = await core.call("GET", f"/api/v1/meetings/{meeting}/recap")
        return recap_markdown(body), body
    if name == "get_decision_record":
        decision = _uuid_arg(args, "decision_id")
        body = await core.call("GET", f"/api/v1/decisions/{decision}/record")
        return record_markdown(body), body
    if name == "company_as_of":
        day = str(args.get("date") or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day):
            raise ToolError("`date` must be a date like 2026-06-30.")
        params = {"as_of": day}
        if args.get("topic"):
            params["topic"] = str(args["topic"])[:120]
        body = await core.call("GET", "/api/v1/time-machine/snapshot", params=params)
        return as_of_markdown(body), body
    if name == "list_my_open_actions":
        body = await core.call("GET", "/api/v1/actions/mine", params={"status": "confirmed"})
        items = body.get("items", [])
        text = "\n".join(
            f"- {a['content']}"
            + (f", due {a['due_date']}" if a.get("due_date") else "")
            + (f" (from {a['meeting_title']})" if a.get("meeting_title") else "")
            + f" `{a['id']}`"
            for a in items
        )
        return text or "You have no open actions.", body
    if name == "list_my_promises":
        status = str(args.get("status") or "open")
        if status not in ("open", "done", "dismissed"):
            raise ToolError("`status` must be open, done or dismissed.")
        body = await core.call("GET", "/api/v1/promises", params={"status": status})
        items = body.get("items", [])
        text = "\n".join(_line_promise(c) for c in items)
        return text or f"No {status} promises.", body
    if name == "check_against_decisions":
        draft = str(args.get("text") or "")
        if not draft.strip():
            raise ToolError("`text` is required.")
        context = {
            k: str(args[v])[:limit]
            for k, v, limit in (("topic", "topic", 200), ("place_label", "place", 120))
            if args.get(v)
        }
        body = await core.call(
            "POST",
            "/api/v1/decisions/check",
            json_body={"text": draft, "source": "mcp", "context": context or None},
        )
        return check_markdown(body), body
    if name == "check_my_capacity":
        tz = str(args["timezone"]) if args.get("timezone") else None
        if args.get("due_date"):
            params = {"due_date": str(args["due_date"])} | ({"tz": tz} if tz else {})
            body = await core.call("GET", "/api/v1/promises/capacity/check", params=params)
            return _capacity_check_text(body), body
        days = max(1, min(int(args.get("days") or 7), 14))
        params = {"days": days} | ({"tz": tz} if tz else {})
        body = await core.call("GET", "/api/v1/promises/capacity", params=params)
        return _capacity_text(body), body
    if name == "add_promise":
        if not ctx.can_write:
            raise ToolError(_READ_ONLY)
        content = str(args.get("content") or "").strip()
        if not content:
            raise ToolError("`content` is required.")
        payload: dict[str, Any] = {"content": content}
        if args.get("due_date"):
            payload["due_at"] = str(args["due_date"])
        if args.get("recipient_id"):
            payload["recipient_user_id"] = str(_uuid_arg(args, "recipient_id"))
        elif args.get("recipient"):
            payload["recipient_label"] = str(args["recipient"])
        tz_params = {"tz": str(args["timezone"])} if args.get("timezone") else None
        body = await core.call("POST", "/api/v1/promises", params=tz_params, json_body=payload)
        promise = body.get("promise") or {}
        text = f"Added to your promises: {promise.get('content', content)}"
        if promise.get("due_at"):
            text += f", due {promise['due_at']}"
        check = body.get("capacity") or {}
        if check.get("warn"):
            text += f"\n\nHeads up: {check.get('message')}"
        return text + f" `{promise.get('id', '')}`", body
    if name == "create_action":
        if not ctx.can_write:
            raise ToolError(_READ_ONLY)
        payload = {
            k: args[k] for k in ("content", "due_date", "assignee_id", "meeting_id") if args.get(k)
        }
        if not payload.get("content"):
            raise ToolError("`content` is required.")
        body = await core.call("POST", "/api/v1/actions", json_body=payload)
        due = f", due {body['due_date']}" if body.get("due_date") else ""
        return f"Created task: {body['content']}{due} `{body['id']}`", body
    raise ToolError(f"Unknown tool {name!r}.")


# --- resources -----------------------------------------------------------------------------------


async def list_resources(core: CoreClient) -> list[dict[str, Any]]:
    body = await core.call("GET", "/api/v1/search", params={"limit": 20})
    return [
        {
            "uri": f"mytheus://meetings/{h['id']}/recap",
            "name": f"meeting-{h['id']}",
            "title": h["text"],
            "description": f"Summary of {h['text']} ({h['occurred_at'][:10]})",
            "mimeType": "text/markdown",
        }
        for h in body.get("items", [])
        if h.get("kind") == "meeting"
    ]


async def read_resource(uri: str, core: CoreClient) -> dict[str, Any]:
    if match := _RECAP_URI.match(uri):
        body = await core.call("GET", f"/api/v1/meetings/{match.group(1)}/recap")
        text = recap_markdown(body)
    elif match := _RECORD_URI.match(uri):
        body = await core.call("GET", f"/api/v1/decisions/{match.group(1)}/record")
        text = record_markdown(body)
    else:
        raise ToolError("Unknown resource")
    return {"contents": [{"uri": uri, "mimeType": "text/markdown", "text": text}]}


# --- JSON-RPC over Streamable HTTP ---------------------------------------------------------------


def _result(req_id: Any, result: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _error(req_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


def _origin_allowed(request: Request) -> bool:
    origin = request.headers.get("origin")
    if not origin:
        return True  # not a browser: MCP clients do not send Origin
    app = os.environ.get("APP_BASE_URL", "http://localhost:3000").rstrip("/")
    return origin.rstrip("/") == app


async def handle(message: dict[str, Any], ctx: RequestContext, core: CoreClient) -> dict[str, Any]:
    req_id = message.get("id")
    method = message.get("method")
    params = message.get("params") or {}
    if not isinstance(params, dict):
        return _error(req_id, INVALID_PARAMS, "params must be an object")
    if method == "initialize":
        asked = params.get("protocolVersion")
        version = asked if asked in SUPPORTED_VERSIONS else LATEST_VERSION
        return _result(
            req_id,
            {
                "protocolVersion": version,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False},
                },
                "serverInfo": {"name": SERVER_NAME, "title": "Mytheus", "version": SERVER_VERSION},
                "instructions": INSTRUCTIONS,
            },
        )
    if method == "ping":
        return _result(req_id, {})
    if method == "tools/list":
        return _result(req_id, {"tools": TOOLS})
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        if name not in _TOOL_NAMES or not isinstance(args, dict):
            return _error(req_id, INVALID_PARAMS, f"Unknown tool {name!r}")
        try:
            text, structured = await call_tool(str(name), args, ctx, core)
        except ToolError as exc:
            return _result(
                req_id, {"content": [{"type": "text", "text": str(exc)}], "isError": True}
            )
        result: dict[str, Any] = {"content": [{"type": "text", "text": text}], "isError": False}
        if isinstance(structured, dict):
            result["structuredContent"] = structured
        return _result(req_id, result)
    if method == "resources/list":
        try:
            return _result(req_id, {"resources": await list_resources(core)})
        except ToolError as exc:
            return _error(req_id, INVALID_REQUEST, str(exc))
    if method == "resources/templates/list":
        return _result(req_id, {"resourceTemplates": RESOURCE_TEMPLATES})
    if method == "resources/read":
        try:
            return _result(req_id, await read_resource(str(params.get("uri", "")), core))
        except ToolError as exc:
            return _error(req_id, RESOURCE_NOT_FOUND, str(exc))
    return _error(req_id, METHOD_NOT_FOUND, f"Method not found: {method}")


_OPENAPI_BODY = {
    "requestBody": {
        "required": True,
        "description": "One JSON-RPC 2.0 request or notification (Model Context Protocol).",
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["jsonrpc", "method"],
                    "properties": {
                        "jsonrpc": {"type": "string", "enum": ["2.0"]},
                        "id": {"type": ["string", "integer"]},
                        "method": {"type": "string"},
                        "params": {"type": "object"},
                    },
                }
            }
        },
    },
    "responses": {
        "200": {
            "description": "The JSON-RPC response.",
            "content": {"application/json": {"schema": {"type": "object"}}},
        },
        "202": {"description": "A notification was accepted (no body)."},
    },
}


@router.post(
    "/mcp",
    summary="MCP server (Streamable HTTP)",
    description=(
        "Model Context Protocol endpoint. Point an MCP client at `{API}/mcp` with "
        "`Authorization: Bearer mytheus_pat_...`, or add it as a Claude custom connector "
        "(OAuth: discovery at `/.well-known/oauth-protected-resource`). Tools: "
        "search_meetings, ask_meetings, get_meeting_outcome, get_decision_record, "
        "list_my_open_actions, list_my_promises, check_against_decisions, check_my_capacity, "
        "create_action and add_promise (read_write tokens). Resources: meeting summaries and "
        "decision records."
    ),
    openapi_extra=_OPENAPI_BODY,
    response_class=JSONResponse,
)
async def mcp_post(
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Response:
    if not _origin_allowed(request):
        raise ProblemDetail(403, ErrorCode.FORBIDDEN_ROLE, "Origin not allowed")
    version = request.headers.get("mcp-protocol-version")
    if version and version not in SUPPORTED_VERSIONS:
        raise ProblemDetail(
            400, ErrorCode.VALIDATION_FAILED, f"Unsupported MCP-Protocol-Version {version}"
        )
    try:
        message = json.loads(await request.body())
    except ValueError:
        return JSONResponse(_error(None, PARSE_ERROR, "Parse error"), status_code=400)
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return JSONResponse(_error(None, INVALID_REQUEST, "Invalid Request"), status_code=400)
    if "method" not in message:
        return Response(status_code=202)  # a client's response to us: we never ask any
    if "id" not in message:
        return Response(status_code=202)  # a notification, e.g. notifications/initialized
    core = CoreClient(request.headers.get("authorization", ""))
    return JSONResponse(await handle(message, ctx, core))


@router.get("/mcp", include_in_schema=False)
@router.delete("/mcp", include_in_schema=False)
async def mcp_other(
    _ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> Response:
    """No server-initiated stream and no sessions to end: 405, as the transport allows."""
    return Response(status_code=405, headers={"Allow": "POST"})
