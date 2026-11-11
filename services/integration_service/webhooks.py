"""`POST /webhooks/{provider}`: the single inbound door for provider callbacks (plan 3.10 step 4;
21 section 2.4).

Order, every time: (1) verify the provider's signature over the RAW body, before parsing it;
(2) resolve the tenant from what the verified request names; (3) write ONE inbox row keyed by the
provider's delivery id (`UNIQUE (org_id, provider, delivery_id)` is the replay guard: a replay is
a no-op that still answers 200); (4) answer 200 at once. The worker
(`python -m services.integration_service.worker`) drains the inbox, so the receiver does no
provider call and no business logic and stays inside Slack's 3 s / Google's ~10 s deadline.

Per provider:

- Slack: `X-Slack-Signature` = `v0=` HMAC-SHA256(`SLACK_SIGNING_SECRET`, `v0:{ts}:{body}`) and
  `X-Slack-Request-Timestamp` within 5 minutes. `url_verification` answers the challenge.
  Tenant = the connection whose `external_account_id` is the event's `team_id`. Delivery id =
  `event_id`.
- Google Calendar: `X-Goog-Channel-Token` is our own signed {org, connection, channel} token
  (minted at `events.watch`), and must name the `X-Goog-Channel-ID` it arrived with. Delivery
  id = `{channel id}:{X-Goog-Message-Number}`.
- Jira: the dynamic webhook carries an `Authorization` JWT signed HS256 with the app's client
  secret (`JIRA_CLIENT_SECRET`), with `exp` required and a claim naming the same cloud as the
  `cloud_id` query parameter we registered the webhook URL with. Tenant = that cloud's
  connection. The payload is still not trusted for status: the worker re-reads the issue from
  Jira with the tenant's own token and only acts on issue keys in that tenant's id map.
  Delivery id = `X-Atlassian-Webhook-Identifier` (or the body hash).

Slack interactivity (`POST /webhooks/slack/interactions`, the app's Interactivity Request URL):
the buttons on a "decided otherwise" note (Reopen it, Dismiss) and the "Check against decisions"
message shortcut. Same v0 signature over the raw form body; `payload` is the JSON. Tenant = the
connection for `team.id`. Delivery id = `interaction:{trigger_id}` (one per click). Inboxed like
an event, so the worker answers through the payload's `response_url` (valid 30 minutes).

A verified request for a tenant we no longer have (disconnected, unknown workspace) gets 200 and
is dropped, so the provider stops retrying; a request that fails verification gets 401.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
import uuid
from typing import Any
from urllib.parse import parse_qs

import jwt
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from libs.shared.db import get_sessionmaker
from libs.shared.errors import ErrorCode, ProblemDetail
from libs.shared.org_resolver import resolve_org_by_id
from libs.shared.tenancy import set_rls
from services.integration_service import config, store
from services.integration_service.connectors.calendar import CHANNEL_PURPOSE
from services.integration_service.oauth_state import verify_token
from services.integration_service.redaction import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["webhooks"])

SLACK_MAX_SKEW = 300


def _unauthorized() -> ProblemDetail:
    return ProblemDetail(401, ErrorCode.AUTH_REQUIRED, "Webhook signature invalid")


def verify_slack(headers: Any, body: bytes, *, now: float | None = None) -> None:
    secret = config.slack_signing_secret()
    if secret is None:
        raise _unauthorized()
    ts = headers.get("x-slack-request-timestamp", "")
    signature = headers.get("x-slack-signature", "")
    try:
        skew = abs((now if now is not None else time.time()) - int(ts))
    except ValueError:
        raise _unauthorized() from None
    if skew > SLACK_MAX_SKEW:
        raise _unauthorized()
    base = b"v0:" + ts.encode() + b":" + body
    expected = "v0=" + hmac.new(secret, base, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise _unauthorized()


# Where a cloud (site) id can appear in an Atlassian webhook JWT. The token must name the same
# cloud as the `cloud_id` the webhook URL was registered with, so a token minted for one site
# cannot be replayed against another tenant's route.
_CLOUD_CLAIMS = ("cloudId", "cloud_id", "aud", "iss", "sub")


def _claimed_clouds(claims: dict[str, Any]) -> set[str]:
    found: set[str] = set()
    for key in _CLOUD_CLAIMS:
        value = claims.get(key)
        if isinstance(value, str):
            found.add(value)
        elif isinstance(value, list):
            found.update(str(v) for v in value)
    context = claims.get("context")
    if isinstance(context, dict) and context.get("cloudId"):
        found.add(str(context["cloudId"]))
    return found


def verify_jira(headers: Any, cloud_id: str) -> None:
    """HS256 under the app's client secret, `exp` required, and the claims must name
    `cloud_id`."""
    creds = config.credentials(config.JIRA)
    if creds is None or not cloud_id:
        raise _unauthorized()
    raw = headers.get("authorization", "")
    scheme, _, token = raw.partition(" ")
    if scheme.lower() not in ("bearer", "jwt") or not token:
        raise _unauthorized()
    try:
        claims = jwt.decode(
            token,
            creds.client_secret,
            algorithms=["HS256"],
            options={"verify_aud": False, "require": ["exp"]},
        )
    except jwt.InvalidTokenError:
        raise _unauthorized() from None
    if cloud_id not in _claimed_clouds(claims):
        raise _unauthorized()


def _json(body: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(body or b"{}")
    except ValueError:
        raise ProblemDetail(400, ErrorCode.MALFORMED_BODY, "Body is not JSON") from None
    if not isinstance(parsed, dict):
        raise ProblemDetail(400, ErrorCode.MALFORMED_BODY, "Body is not a JSON object")
    return parsed


def _ack(outcome: str) -> JSONResponse:
    return JSONResponse({"status": outcome}, status_code=200)


async def _accept(
    org_id: uuid.UUID,
    provider: str,
    delivery_id: str,
    event_type: str | None,
    payload: dict[str, Any],
) -> JSONResponse:
    async with get_sessionmaker()() as session, session.begin():
        if await resolve_org_by_id(session, org_id) is None:
            return _ack("ignored")
        await set_rls(session, org_id)
        fresh = await store.inbox_insert(
            session, org_id, provider, delivery_id, event_type, payload
        )
    return _ack("accepted" if fresh else "duplicate")


async def _route(provider: str, external_account_id: str) -> uuid.UUID | None:
    async with get_sessionmaker()() as session, session.begin():
        conn = await store.route_connection(session, provider, external_account_id)
    return conn.org_id if conn else None


INTERACTIONS = ("block_actions", "message_action")


# authz: public - verified by Slack's v0 signature over the raw body, not a JWT.
@router.post("/webhooks/slack/interactions")
async def slack_interactions(request: Request) -> JSONResponse:
    body = await request.body()
    verify_slack(request.headers, body)
    try:
        raw = parse_qs(body.decode("utf-8")).get("payload", [""])[0]
        data = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        raise ProblemDetail(400, ErrorCode.MALFORMED_BODY, "No interaction payload") from None
    if not isinstance(data, dict) or data.get("type") not in INTERACTIONS:
        return _ack("ignored")
    team_id = (data.get("team") or {}).get("id")
    trigger = data.get("trigger_id") or hashlib.sha256(body).hexdigest()
    if not isinstance(team_id, str):
        return _ack("ignored")
    org_id = await _route(config.SLACK, team_id)
    if org_id is None:
        return _ack("ignored")
    return await _accept(
        org_id,
        config.SLACK,
        f"interaction:{trigger}",
        f"interaction:{data['type']}",
        {"interaction": data},
    )


# authz: public - verified by the provider's signature over the raw body, not a JWT.
@router.post("/webhooks/{provider}")
async def receive(provider: str, request: Request) -> JSONResponse:
    if provider not in config.R8_PROVIDERS:
        raise ProblemDetail(404, ErrorCode.NOT_FOUND, "Not found")
    body = await request.body()
    headers = request.headers

    if provider == config.SLACK:
        verify_slack(headers, body)
        data = _json(body)
        if data.get("type") == "url_verification":
            return JSONResponse({"challenge": str(data.get("challenge", ""))})
        team_id = data.get("team_id")
        event_id = data.get("event_id")
        if not isinstance(team_id, str) or not isinstance(event_id, str):
            return _ack("ignored")
        org_id = await _route(provider, team_id)
        if org_id is None:
            return _ack("ignored")
        event_type = (data.get("event") or {}).get("type")
        return await _accept(org_id, provider, event_id, event_type, data)

    if provider == config.GOOGLE_CALENDAR:
        token = headers.get("x-goog-channel-token", "")
        channel_id = headers.get("x-goog-channel-id", "")
        claims = verify_token(config.state_secret(), CHANNEL_PURPOSE, token) if token else None
        if not claims or claims.get("ch") != channel_id:
            raise _unauthorized()
        try:
            org_id = uuid.UUID(str(claims.get("o")))
        except ValueError:
            raise _unauthorized() from None
        number = headers.get("x-goog-message-number", "")
        state = headers.get("x-goog-resource-state", "")
        return await _accept(
            org_id,
            provider,
            f"{channel_id}:{number}",
            state or None,
            {"channel_id": channel_id, "resource_state": state},
        )

    # Jira
    cloud_id = request.query_params.get("cloud_id", "")
    verify_jira(headers, cloud_id)
    data = _json(body)
    org_id = await _route(provider, cloud_id)
    if org_id is None:
        return _ack("ignored")
    delivery = headers.get("x-atlassian-webhook-identifier") or hashlib.sha256(body).hexdigest()
    return await _accept(org_id, provider, delivery, str(data.get("webhookEvent") or ""), data)
