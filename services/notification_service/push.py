"""Phone push: the caller's Expo push tokens (`push_devices`) and the Expo Push API sender.

Routes (the caller's own rows; `org_id` from the verified token, RLS on every query):

- `GET /notifications/push-devices`: the caller's registered phones, plus `push_configured`
  (whether this server sends pushes at all) so the app can say "not connected".
- `POST /notifications/push-devices` `{token, platform}`: register (or refresh) this phone for
  the caller. Idempotent: the same token again only bumps `last_seen_at`. A token already held
  by someone else in this tenant moves to the caller (one person per phone at a time: the last
  one to sign in on it).
- `DELETE /notifications/push-devices/{id}`: unregister (the app calls it on sign-out). Owner
  only; 404 for anyone else.

Delivery. `PUSH_PROVIDER=expo` turns it on (default off = "not connected": tokens are still
registered, nothing is sent). The consumer (`consumer.py`) mirrors each NEW in-app notification
row to the recipient's phones AFTER its transaction commits, like email: a redelivery writes no
new row and so never pushes twice. The push carries the title, the body and the relative `link`
(for the app's deep link) and the notification id; never meeting content beyond what the inbox
row already says. The Expo Push API needs no credential for basic sends; `EXPO_ACCESS_TOKEN`
is sent as a bearer when set (required only if the Expo project enforces push security). A
ticket `DeviceNotRegistered` deletes that token. Nothing is logged but counts and error codes.
"""

from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, Protocol

import httpx
from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.tenancy import set_rls

router = APIRouter(tags=["notifications"])

PROVIDER_ENV = "PUSH_PROVIDER"
EXPO_TOKEN_ENV = "EXPO_ACCESS_TOKEN"
EXPO_URL_ENV = "EXPO_PUSH_URL"
EXPO_PUSH_URL = "https://exp.host/--/api/v2/push/send"
EXPO_BATCH = 100  # the Expo Push API's per-request limit
TIMEOUT = httpx.Timeout(10.0, connect=5.0)
MAX_DEVICES_PER_USER = 10
TOKEN_RE = re.compile(r"^(ExponentPushToken|ExpoPushToken)\[[A-Za-z0-9_\-]{8,200}\]$")

Platform = Literal["ios", "android"]


def push_provider() -> str | None:
    """The configured provider name, or None when push is not connected."""
    return "expo" if os.environ.get(PROVIDER_ENV, "").strip().lower() == "expo" else None


# --- routes ----------------------------------------------------------------------------------


class PushDeviceRegister(BaseModel):
    model_config = ConfigDict(extra="forbid")

    token: str = Field(min_length=1, max_length=255, description="An Expo push token.")
    platform: Platform

    @field_validator("token")
    @classmethod
    def _expo_token(cls, value: str) -> str:
        value = value.strip()
        if not TOKEN_RE.match(value):
            raise ValueError("token must be an Expo push token (ExponentPushToken[...])")
        return value


class PushDevice(BaseModel):
    id: uuid.UUID
    platform: Platform
    created_at: datetime
    last_seen_at: datetime
    push_configured: bool = Field(
        description="False when this server sends no pushes (`PUSH_PROVIDER` unset)."
    )


class PushDeviceList(BaseModel):
    items: list[PushDevice]
    push_configured: bool


def _device(row: Any) -> PushDevice:
    return PushDevice(
        id=row["id"],
        platform=row["platform"],
        created_at=row["created_at"],
        last_seen_at=row["last_seen_at"],
        push_configured=push_provider() is not None,
    )


@router.get("/notifications/push-devices", response_model=PushDeviceList)
async def list_push_devices(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PushDeviceList:
    """authz: member. The caller's registered phones (never the tokens themselves)."""
    if ctx.user_id is None:
        raise not_found()
    rows = (
        (
            await session.execute(
                text(
                    "SELECT id, platform, created_at, last_seen_at FROM push_devices "
                    "WHERE user_id = :u ORDER BY last_seen_at DESC"
                ),
                {"u": str(ctx.user_id)},
            )
        )
        .mappings()
        .all()
    )
    return PushDeviceList(
        items=[_device(r) for r in rows], push_configured=push_provider() is not None
    )


@router.post(
    "/notifications/push-devices", response_model=PushDevice, status_code=status.HTTP_200_OK
)
async def register_push_device(
    body: PushDeviceRegister,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PushDevice:
    """authz: member. Register this phone's Expo push token for the caller (idempotent)."""
    if ctx.user_id is None:
        raise not_found()
    row = (
        (
            await session.execute(
                text(
                    "INSERT INTO push_devices (org_id, user_id, token, platform) "
                    "VALUES (:o, :u, :t, :p) ON CONFLICT (org_id, token) DO UPDATE SET "
                    "user_id = EXCLUDED.user_id, platform = EXCLUDED.platform, "
                    "last_seen_at = now() "
                    "RETURNING id, platform, created_at, last_seen_at"
                ),
                {
                    "o": str(ctx.org_id),
                    "u": str(ctx.user_id),
                    "t": body.token,
                    "p": body.platform,
                },
            )
        )
        .mappings()
        .one()
    )
    # A phone list that only grows (reinstalls mint new tokens) would fan every push out to
    # dead installs: keep the most recently seen few.
    await session.execute(
        text(
            "DELETE FROM push_devices WHERE user_id = :u AND id NOT IN ("
            "SELECT id FROM push_devices WHERE user_id = :u "
            "ORDER BY last_seen_at DESC LIMIT :n)"
        ),
        {"u": str(ctx.user_id), "n": MAX_DEVICES_PER_USER},
    )
    return _device(row)


@router.delete(
    "/notifications/push-devices/{device_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def unregister_push_device(
    device_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> Response:
    """authz: member. Remove one of the caller's phones. 404 when it is not theirs."""
    if ctx.user_id is None:
        raise not_found()
    gone = (
        await session.execute(
            text("DELETE FROM push_devices WHERE id = :id AND user_id = :u RETURNING 1"),
            {"id": str(device_id), "u": str(ctx.user_id)},
        )
    ).first()
    if gone is None:
        raise not_found()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


# --- delivery --------------------------------------------------------------------------------


@dataclass(frozen=True)
class PushMessage:
    token: str
    title: str
    body: str
    data: dict[str, Any]
    urgent: bool


class PushSender(Protocol):
    async def send(self, messages: list[PushMessage]) -> list[str | None]:
        """One result per message, in order: None when accepted, else the error code
        (`DeviceNotRegistered`, `MessageTooBig`, ...)."""
        ...


class ExpoPushSender:
    provider = "expo"

    def __init__(
        self,
        url: str = EXPO_PUSH_URL,
        access_token: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._url = url
        self._access_token = access_token
        self._transport = transport

    async def send(self, messages: list[PushMessage]) -> list[str | None]:
        results: list[str | None] = []
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"
        async with httpx.AsyncClient(transport=self._transport, timeout=TIMEOUT) as client:
            for start in range(0, len(messages), EXPO_BATCH):
                batch = messages[start : start + EXPO_BATCH]
                payload = [
                    {
                        "to": m.token,
                        "title": m.title,
                        "body": m.body,
                        "data": m.data,
                        "sound": "default",
                        "priority": "high" if m.urgent else "default",
                    }
                    for m in batch
                ]
                try:
                    res = await client.post(self._url, json=payload, headers=headers)
                    tickets = res.json().get("data") if res.status_code < 300 else None
                except (httpx.HTTPError, ValueError):
                    tickets = None
                if not isinstance(tickets, list) or len(tickets) != len(batch):
                    results.extend(["ExpoUnavailable"] * len(batch))
                    continue
                for ticket in tickets:
                    if isinstance(ticket, dict) and ticket.get("status") == "ok":
                        results.append(None)
                    else:
                        details = ticket.get("details") if isinstance(ticket, dict) else None
                        code = details.get("error") if isinstance(details, dict) else None
                        results.append(str(code or "Error")[:64])
        return results


def push_sender_from_env() -> PushSender | None:
    if push_provider() != "expo":
        return None
    return ExpoPushSender(
        url=os.environ.get(EXPO_URL_ENV, EXPO_PUSH_URL),
        access_token=os.environ.get(EXPO_TOKEN_ENV) or None,
    )


@dataclass(frozen=True)
class PendingPush:
    """A new in-app row to mirror to the recipient's phones after the commit."""

    org_id: uuid.UUID
    user_id: uuid.UUID
    notification_id: uuid.UUID
    title: str
    body: str
    link: str | None
    priority: int


MAX_PUSH_TITLE = 120
MAX_PUSH_BODY = 240


async def deliver_push(session_factory: Any, item: PendingPush, sender: PushSender) -> int:
    """Send one notification to the recipient's phones; drop tokens Expo no longer knows.
    Returns the number of phones that accepted it. Never raises for a provider problem."""
    async with session_factory() as session, session.begin():
        await set_rls(session, item.org_id)
        devices = (
            await session.execute(
                text("SELECT id, token FROM push_devices WHERE user_id = :u"),
                {"u": str(item.user_id)},
            )
        ).all()
    if not devices:
        return 0
    data: dict[str, Any] = {"notification_id": str(item.notification_id)}
    if item.link:
        data["link"] = item.link
    messages = [
        PushMessage(
            token=str(token),
            title=item.title[:MAX_PUSH_TITLE],
            body=item.body[:MAX_PUSH_BODY],
            data=data,
            urgent=item.priority == 1,
        )
        for _id, token in devices
    ]
    try:
        results = await sender.send(messages)
    except Exception:  # a provider problem is never a consumer failure
        return 0
    dead = [
        str(device_id)
        for (device_id, _token), error in zip(devices, results, strict=False)
        if error == "DeviceNotRegistered"
    ]
    if dead:
        async with session_factory() as session, session.begin():
            await set_rls(session, item.org_id)
            await session.execute(
                text("DELETE FROM push_devices WHERE id = ANY(CAST(:ids AS uuid[]))"),
                {"ids": dead},
            )
    return sum(1 for error in results if error is None)
