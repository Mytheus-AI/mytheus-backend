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
  secret (`JIRA_CLIENT_SECRET`). Tenant = the connection for the `cloud_id` query parameter we
  registered the webhook URL with; the worker additionally only acts on issues in that tenant's
  own id map. Delivery id = `X-Atlassian-Webhook-Identifier` (or the body hash).

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


def verify_jira(headers: Any) -> None:
    creds = config.credentials(config.JIRA)
    if creds is None:
        raise _unauthorized()
    raw = headers.get("authorization", "")
    scheme, _, token = raw.partition(" ")
    if scheme.lower() not in ("bearer", "jwt") or not token:
        raise _unauthorized()
    try:
        jwt.decode(
            token,
            creds.client_secret,
            algorithms=["HS256"],
            options={"verify_aud": False, "require": []},
        )
    except jwt.InvalidTokenError:
        raise _unauthorized() from None


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
    verify_jira(headers)
    cloud_id = request.query_params.get("cloud_id", "")
    data = _json(body)
    if not cloud_id:
        return _ack("ignored")
    org_id = await _route(provider, cloud_id)
    if org_id is None:
        return _ack("ignored")
    delivery = headers.get("x-atlassian-webhook-identifier") or hashlib.sha256(body).hexdigest()
    return await _accept(org_id, provider, delivery, str(data.get("webhookEvent") or ""), data)
