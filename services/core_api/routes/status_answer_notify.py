"""Tell the person who asked for a status (polish wave 3) what the owner answered.

Only requests created through `POST /api/v1/status-requests` carry `requested_by`; the
workflow's pre-meeting requests do not, so they notify nobody here. One notification per
answer text: the key hashes the text, so re-sending the same answer sends nothing new and a
changed answer is news.
"""

from __future__ import annotations

import hashlib
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.core_api.records import display_name


def _short(value: str, limit: int) -> str:
    text_ = " ".join(value.split())
    return text_ if len(text_) <= limit else text_[: limit - 3].rstrip() + "..."


async def notify_asker(
    session: AsyncSession,
    org_id: uuid.UUID,
    request_id: uuid.UUID,
    owner_id: uuid.UUID,
    status_text: str,
) -> bool:
    row = (
        await session.execute(
            text(
                "SELECT r.requested_by, a.content, a.meeting_id, u.email "
                "FROM action_status_requests r JOIN action_items a ON a.id = r.action_id "
                "LEFT JOIN users u ON u.id = r.user_id "
                "WHERE r.id = :id"
            ),
            {"id": str(request_id)},
        )
    ).first()
    if row is None or row[0] is None or row[0] == owner_id:
        return False
    asker, content, meeting_id, owner_email = row
    digest = hashlib.sha256(status_text.encode("utf-8")).hexdigest()[:16]
    source = f"statusanswer:{request_id}:{digest}"
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer="core-api",
            idempotency_key=f"notif:{source}:in_app:{asker}",
            payload={
                "source_event_id": source,
                "channel": "in_app",
                "user_id": str(asker),
                "priority": 3,
                "title": f"{display_name(owner_email) or 'The owner'} answered: "
                f"{_short(str(content or 'your status request'), 100)}"[:200],
                "body": _short(status_text, 2000),
                "link": f"/meetings/{meeting_id}" if meeting_id else "/work/tasks",
            },
        ),
    )
    return True
