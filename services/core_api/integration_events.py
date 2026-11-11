"""core-api's `integration-events` handler: calendar events become meetings (P12; plan 3.11
step 1; 21 section 3.2).

Only `(google_calendar, calendar_event_upserted)` acts; every other integration event (the
`attachment_to_ingest` ones are ingestion-service's) is consumed and recorded with no effect.

Upsert, keyed on `meetings (org_id, calendar_event_id)` (migration 7c3e9a1f5b20):

- the organizer's email must match a user of this tenant, or the event is consumed with no
  effect: an outsider's invite never creates a meeting or gets `created_by`/host;
- no meeting for the event: INSERT with the event's deterministic `meeting_id` (uuid5 from
  integration-service, so the attachments it already ingested point at this meeting),
  `created_by` = that organizer, plus a
  `meeting-events` `created` outbox row (the MeetingLifecycleWorkflow starts from it) and a
  `meeting.created` audit row (actor_type `system`);
- a meeting already exists: patch title and schedule while it is still `scheduled` (a live or
  ended meeting keeps the times it actually had), with a `meeting-events` `updated` row when
  anything changed.

Participants: attendee emails (and the organizer's) are matched to users of THIS tenant here,
case-insensitively, under RLS; the payload's `user_id` hints are not trusted. Matched users are
added with `ON CONFLICT DO NOTHING` (organizer as `host`), so a redelivery adds nobody twice;
unmatched attendees stay external and get no participant row.

Dedupe follows the consumer recipe (Redis fast path + `inbox` row, group `core-api`), and the
upsert is idempotent on its own regardless.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_MEETING_CREATED, ACTION_MEETING_UPDATED, audit
from libs.shared.events import EventEnvelope
from libs.shared.inbox import is_duplicate
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls

TOPIC = "integration-events"
CONSUMER_GROUP = "core-api"
PRODUCER = "core-api"
MAX_TITLE = 500


class IntegrationEventPoison(Exception):
    """Cannot apply: routed to `integration-events.dlq` by the harness."""


def _enum(raw: Any) -> str:
    return str(getattr(raw, "name", raw))


def _ts(raw: Any, what: str) -> datetime | None:
    if raw in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError as exc:
        raise IntegrationEventPoison(f"{what} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise IntegrationEventPoison(f"{what} has no timezone")
    return parsed


async def handle_integration_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """True when a meeting was created or changed."""
    header = value.get("header") or {}
    raw_org, key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not key:
        raise IntegrationEventPoison("integration-events message missing header fields")
    try:
        org_id = uuid.UUID(str(raw_org))
    except ValueError as exc:
        raise IntegrationEventPoison("header.org_id is not a UUID") from exc
    if (_enum(value.get("provider")), value.get("event_type")) != (
        "google_calendar",
        "calendar_event_upserted",
    ):
        return False
    if await is_duplicate(redis, str(org_id), TOPIC, str(key)):
        return False
    try:
        await set_rls(session, org_id)
        try:
            claimed = (
                await session.execute(
                    text(
                        "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                        "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, "
                        "idempotency_key) DO NOTHING RETURNING 1"
                    ),
                    {"g": CONSUMER_GROUP, "o": str(org_id), "k": f"{TOPIC}:{key}"},
                )
            ).first()
        except IntegrityError as exc:
            raise IntegrationEventPoison(f"unknown tenant {org_id}") from exc
        if claimed is None:
            return False
        return await _upsert(session, org_id, value)
    except BaseException:
        await redis.delete(f"nexus:{org_id}:dedupe:{TOPIC}:{key}")
        raise


async def _upsert(session: AsyncSession, org_id: uuid.UUID, value: dict[str, Any]) -> bool:
    event_id = value.get("external_id")
    if not isinstance(event_id, str) or not event_id:
        raise IntegrationEventPoison("calendar_event_upserted has no external_id")
    try:
        payload = json.loads(str(value.get("payload_json") or ""))
        meeting_id = uuid.UUID(str(payload["meeting_id"]))
    except (ValueError, KeyError, TypeError) as exc:
        raise IntegrationEventPoison("payload_json is not a calendar event") from exc
    title = str(payload.get("title") or "Untitled meeting")[:MAX_TITLE]
    starts = _ts(payload.get("starts_at"), "starts_at")
    ends = _ts(payload.get("ends_at"), "ends_at")
    if starts is not None and ends is not None and ends <= starts:
        ends = None  # the schedule_ordered CHECK; a zero-length event keeps only its start

    emails = {
        str(a.get("email")).lower()
        for a in payload.get("attendees") or []
        if isinstance(a, dict) and a.get("email")
    }
    organizer_email = str(payload.get("organizer_email") or "").lower() or None
    lookup = sorted(emails | ({organizer_email} if organizer_email else set()))
    users = (
        {
            str(r[0]): r[1]
            for r in await session.execute(
                text(
                    "SELECT lower(email), id FROM users "
                    "WHERE lower(email) = ANY(:e) AND deleted_at IS NULL"
                ),
                {"e": lookup},
            )
        }
        if lookup
        else {}
    )
    organizer_id = users.get(organizer_email) if organizer_email else None
    if organizer_id is None:
        # Only an event organized by a user of this tenant becomes a meeting (integration-service
        # already filters; checked again here because core-api owns `meetings`).
        return False

    existing = (
        (
            await session.execute(
                text(
                    "SELECT id, title, status, scheduled_start, scheduled_end, livekit_room "
                    "FROM meetings WHERE calendar_event_id = :e FOR UPDATE"
                ),
                {"e": event_id},
            )
        )
        .mappings()
        .first()
    )

    changed = False
    if existing is None:
        inserted = (
            (
                await session.execute(
                    text(
                        "INSERT INTO meetings (id, org_id, title, scheduled_start, scheduled_end, "
                        "created_by, calendar_event_id) VALUES (:id, :o, :t, :s, :e, :c, :ce) "
                        "ON CONFLICT DO NOTHING RETURNING id, livekit_room"
                    ),
                    {
                        "id": str(meeting_id),
                        "o": str(org_id),
                        "t": title,
                        "s": starts,
                        "e": ends,
                        "c": str(organizer_id) if organizer_id else None,
                        "ce": event_id,
                    },
                )
            )
            .mappings()
            .first()
        )
        if inserted is None:
            raise IntegrationEventPoison(f"meeting id {meeting_id} is taken by another meeting")
        meeting_id, livekit_room, event_type = inserted["id"], inserted["livekit_room"], "created"
        changed = True
        await audit(
            session,
            org_id=org_id,
            actor_type="system",
            action=ACTION_MEETING_CREATED,
            target_type="meeting",
            target_id=meeting_id,
            metadata={"title": title, "source": "google_calendar", "calendar_event_id": event_id},
        )
    else:
        meeting_id, livekit_room, event_type = existing["id"], existing["livekit_room"], "updated"
        if existing["status"] == "scheduled" and (
            existing["title"] != title
            or existing["scheduled_start"] != starts
            or existing["scheduled_end"] != ends
        ):
            await session.execute(
                text(
                    "UPDATE meetings SET title = :t, scheduled_start = :s, scheduled_end = :e, "
                    "updated_at = now() WHERE id = :id"
                ),
                {"t": title, "s": starts, "e": ends, "id": str(meeting_id)},
            )
            changed = True
            await audit(
                session,
                org_id=org_id,
                actor_type="system",
                action=ACTION_MEETING_UPDATED,
                target_type="meeting",
                target_id=meeting_id,
                metadata={"source": "google_calendar", "calendar_event_id": event_id},
            )

    # Promise ledger capacity: the Google event type (`focusTime`, `outOfOffice`, ...) so focus
    # time is not counted as meeting time. Absent from older events; never part of `changed`.
    kind = payload.get("event_kind")
    if isinstance(kind, str) and 0 < len(kind) <= 40:
        await session.execute(
            text(
                "UPDATE meetings SET calendar_event_kind = :k WHERE id = :id "
                "AND calendar_event_kind IS DISTINCT FROM :k"
            ),
            {"k": kind, "id": str(meeting_id)},
        )

    for email, user_id in users.items():
        if email not in emails and email != organizer_email:
            continue
        added = (
            await session.execute(
                text(
                    "INSERT INTO meeting_participants (meeting_id, user_id, org_id, "
                    "role_in_meeting) VALUES (:m, :u, :o, :r) ON CONFLICT DO NOTHING RETURNING 1"
                ),
                {
                    "m": str(meeting_id),
                    "u": str(user_id),
                    "o": str(org_id),
                    "r": "host" if email == organizer_email else "attendee",
                },
            )
        ).first()
        changed = changed or added is not None

    if changed:
        await enqueue(
            session,
            EventEnvelope(
                topic="meeting-events",
                payload={
                    "meeting_id": str(meeting_id),
                    "event_type": event_type,
                    "livekit_room": livekit_room,
                },
                org_id=str(org_id),
                producer=PRODUCER,
                idempotency_key=(
                    f"meeting:{meeting_id}:created"
                    if event_type == "created"
                    else f"meeting:{meeting_id}:calendar:{value['header']['idempotency_key']}"
                ),
            ),
        )
    return changed
