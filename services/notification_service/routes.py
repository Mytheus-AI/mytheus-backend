"""notification-service REST surface (P11 contract section 3). Every route is the CALLER'S own.

- `GET /notifications?meeting_id=&status=&limit=&cursor=`: the caller's rows. Without `status`,
  dismissed rows are left out. Newest first; with `meeting_id`, priority first (1 = most
  urgent) and then newest, the order the meeting sidebar shows alerts in. Keyset cursor.
- `PATCH /notifications/{id}` `{status: read|dismissed}`: owner only, 404 for anyone else in
  any tenant. `unread -> read`, `unread|read -> dismissed`; dismissed is terminal. Repeating the
  current status (or asking a dismissed row to be read) is 200 and writes nothing. Each real
  change writes one `notification.read` / `notification.dismissed` audit row.
- `GET /notifications/preferences`: the caller's three channels, defaults filled in.
- `PUT /notifications/preferences` `{items: [...]}`: upserts the given channels; `in_app` is
  forced on (see `prefs.py`). Returns the same shape as GET.

`org_id` comes from the verified token only; RLS (`tenant_session`) scopes every query and
`user_id = caller` scopes it to the person.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.notification_service.prefs import CHANNELS, IN_APP_FORCED, Channel, load_prefs

router = APIRouter(tags=["notifications"])

ACTION_NOTIFICATION_READ = "notification.read"
ACTION_NOTIFICATION_DISMISSED = "notification.dismissed"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

Status = Literal["unread", "read", "dismissed"]

_COLUMNS = (
    "id, meeting_id, source, alert_type, priority, title, body, link, payload, status, "
    "created_at, read_at, dismissed_at"
)


class NotificationRow(BaseModel):
    id: uuid.UUID
    meeting_id: uuid.UUID | None
    source: Literal["agent_alert", "notification"]
    alert_type: str | None
    priority: int
    title: str
    body: str
    link: str | None
    payload: dict[str, Any]
    status: Status
    created_at: datetime
    read_at: datetime | None
    dismissed_at: datetime | None


class NotificationPage(BaseModel):
    items: list[NotificationRow]
    next_cursor: str | None = None


class NotificationStatusChange(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["read", "dismissed"]


class ChannelPreference(BaseModel):
    model_config = ConfigDict(extra="forbid")

    channel: Channel
    enabled: bool
    max_priority: int = Field(ge=1, le=3)


class PreferenceList(BaseModel):
    items: list[ChannelPreference]


class PreferenceUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ChannelPreference] = Field(max_length=len(CHANNELS))

    @field_validator("items")
    @classmethod
    def _unique_channels(cls, items: list[ChannelPreference]) -> list[ChannelPreference]:
        channels = [i.channel for i in items]
        if len(set(channels)) != len(channels):
            raise ValueError("each channel may appear once")
        return items


def _encode_cursor(by_priority: bool, row: Any) -> str:
    raw = {"p": row["priority"] if by_priority else None, "c": row["created_at"].isoformat()}
    raw["i"] = str(row["id"])
    return base64.urlsafe_b64encode(json.dumps(raw).encode()).decode()


def _decode_cursor(cursor: str, by_priority: bool) -> tuple[int | None, datetime, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        priority = raw["p"]
        if by_priority != isinstance(priority, int):
            raise ValueError("cursor from a different ordering")
        return priority, datetime.fromisoformat(raw["c"]), uuid.UUID(raw["i"])
    except (ValueError, KeyError, TypeError, binascii.Error) as exc:
        raise ProblemDetail(400, ErrorCode.INVALID_CURSOR, "Malformed pagination cursor") from exc


def _row(r: Any) -> NotificationRow:
    return NotificationRow(**{k: r[k] for k in NotificationRow.model_fields})


@router.get("/notifications", response_model=NotificationPage)
async def list_notifications(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
    meeting_id: uuid.UUID | None = None,
    status: Status | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> NotificationPage:
    """The caller's notifications. Dismissed ones only when asked for by `status`."""
    if ctx.user_id is None:
        return NotificationPage(items=[])
    by_priority = meeting_id is not None
    where = ["user_id = :u"]
    params: dict[str, Any] = {"u": str(ctx.user_id), "limit": limit + 1}
    if status is None:
        where.append("status <> 'dismissed'")
    else:
        where.append("status = :s")
        params["s"] = status
    if meeting_id is not None:
        where.append("meeting_id = :m")
        params["m"] = str(meeting_id)
    if cursor:
        priority, created_at, row_id = _decode_cursor(cursor, by_priority)
        params.update(cc=created_at, ci=str(row_id))
        if by_priority:
            where.append("(priority > :cp OR (priority = :cp AND (created_at, id) < (:cc, :ci)))")
            params["cp"] = priority
        else:
            where.append("(created_at, id) < (:cc, :ci)")
    order = ("priority ASC, " if by_priority else "") + "created_at DESC, id DESC"
    rows = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLUMNS} FROM notification_log WHERE {' AND '.join(where)} "
                    f"ORDER BY {order} LIMIT :limit"
                ),
                params,
            )
        )
        .mappings()
        .all()
    )
    page = list(rows[:limit])
    has_more = len(rows) > limit
    return NotificationPage(
        items=[_row(r) for r in page],
        next_cursor=_encode_cursor(by_priority, page[-1]) if has_more and page else None,
    )


@router.get("/notifications/preferences", response_model=PreferenceList)
async def get_preferences(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PreferenceList:
    """The caller's channel preferences, with the defaults for channels never set."""
    if ctx.user_id is None:
        raise not_found()
    return await _preferences(session, ctx.user_id)


@router.put("/notifications/preferences", response_model=PreferenceList)
async def put_preferences(
    body: PreferenceUpdate,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> PreferenceList:
    """Set the caller's channel preferences. Channels left out keep their current value."""
    if ctx.user_id is None:
        raise not_found()
    for item in body.items:
        pref = IN_APP_FORCED if item.channel == "in_app" else item
        await session.execute(
            text(
                "INSERT INTO notification_preferences (org_id, user_id, channel, enabled, "
                "max_priority) VALUES (:o, :u, :c, :e, :p) "
                "ON CONFLICT (org_id, user_id, channel) DO UPDATE SET enabled = EXCLUDED.enabled, "
                "max_priority = EXCLUDED.max_priority, updated_at = now()"
            ),
            {
                "o": str(ctx.org_id),
                "u": str(ctx.user_id),
                "c": item.channel,
                "e": pref.enabled,
                "p": pref.max_priority,
            },
        )
    return await _preferences(session, ctx.user_id)


async def _preferences(session: AsyncSession, user_id: uuid.UUID) -> PreferenceList:
    prefs = await load_prefs(session, user_id)
    return PreferenceList(
        items=[
            ChannelPreference(
                channel=c, enabled=prefs[c].enabled, max_priority=prefs[c].max_priority
            )
            for c in CHANNELS
        ]
    )


@router.patch("/notifications/{notification_id}", response_model=NotificationRow)
async def change_status(
    notification_id: uuid.UUID,
    body: NotificationStatusChange,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> NotificationRow:
    """Mark one of the caller's notifications read or dismissed. Idempotent; owner only."""
    if ctx.user_id is None:
        raise not_found()
    row = (
        (
            await session.execute(
                text(
                    f"SELECT {_COLUMNS} FROM notification_log WHERE id = :id AND user_id = :u "
                    "FOR UPDATE"
                ),
                {"id": str(notification_id), "u": str(ctx.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        raise not_found()  # missing, someone else's, or another tenant's
    current = row["status"]
    if current == body.status or current == "dismissed":
        return _row(row)
    if body.status == "read":
        sql = "status = 'read', read_at = now()"
        action = ACTION_NOTIFICATION_READ
    else:
        sql = "status = 'dismissed', dismissed_at = now()"
        action = ACTION_NOTIFICATION_DISMISSED
    updated = (
        (
            await session.execute(
                text(f"UPDATE notification_log SET {sql} WHERE id = :id RETURNING {_COLUMNS}"),
                {"id": str(notification_id)},
            )
        )
        .mappings()
        .one()
    )
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=action,
        target_type="notification",
        target_id=notification_id,
        metadata={"from": current, "source": row["source"], "alert_type": row["alert_type"]},
        ip=ctx.ip,
    )
    return _row(updated)
