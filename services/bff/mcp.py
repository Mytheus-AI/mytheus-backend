"""The Mytheus MCP server: ask Mytheus from Claude, IDEs and other MCP clients (Phase 4).

Transport: MCP Streamable HTTP at `POST /mcp`, stateless, JSON responses (the server never
opens an SSE stream, which the transport allows: a server may answer a request with a single
`application/json` body). No `Mcp-Session-Id` is issued, so every POST stands alone and any
bff replica can serve it. `GET` and `DELETE` are 405. Protocol versions 2024-11-05,
2025-03-26, 2025-06-18 and 2025-11-25 are negotiated; an unknown `MCP-Protocol-Version`
header is a 400.

Auth: `Authorization: Bearer mytheus_pat_...` (a personal access token) or a Clerk session,
resolved by the same chain as every route (`require_tier("member")`). A browser `Origin` other
than the web app's is refused (403), the transport's DNS-rebinding guard.

Why a route group in bff and not a new service: MCP here is a client-shaped aggregation over
the public API, exactly bff's job, and it needs no deployable, secret or platform change of
its own. The tools call the public API AS THE CALLER: core-api over HTTP with the caller's own
bearer (`CORE_API_URL`, default `http://core-api:8000`), and ask through `bff.ask` (rag-service
on the caller's behalf). So a tool can never see or do more than the same token can through
REST, and bff stays a non-writer: `create_action` is core-api's `POST /api/v1/actions`.

Tools: `search_meetings`, `ask_meetings`, `get_meeting_outcome` (decisions, actions, agenda:
the minutes), `get_decision_record`, `list_my_open_actions`, `list_my_promises`,
`create_action` (a confirmed task for the caller, the caller's own explicit request; needs a
`read_write` token). Resources: `mytheus://meetings/{meeting_id}/recap` (a meeting summary as
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
    "create_action records a confirmed task immediately; only call it when the user asked."
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
        "name": "list_my_open_actions",
        "title": "List my open actions",
        "description": "Your confirmed, not yet done actions, soonest due first.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "list_my_promises",
        "title": "List my promises",
        "description": "Things you said you would do in meetings (private to you).",
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
                raise ToolError("This API token is read-only; create a read_write token to write.")
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
        body = await core.call("GET", "/api/v1/commitments", params={"status": status})
        items = body.get("items", [])
        text = "\n".join(
            f"- {c['content']}"
            + (f" ({c['due_hint']})" if c.get("due_hint") else "")
            + (f", said in {c['meeting_title']}" if c.get("meeting_title") else "")
            for c in items
        )
        return text or f"No {status} promises.", body
    if name == "create_action":
        if not ctx.can_write:
            raise ToolError("This API token is read-only; create a read_write token to write.")
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
        "`Authorization: Bearer mytheus_pat_...`. Tools: search_meetings, ask_meetings, "
        "get_meeting_outcome, get_decision_record, list_my_open_actions, list_my_promises, "
        "create_action (read_write tokens). Resources: meeting summaries and decision records."
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
