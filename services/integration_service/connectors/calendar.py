"""Google Calendar connector (plan 3.11 step 1; 21 section 3).

Install: one `events.watch` push channel on the installing user's primary calendar. The channel
`token` is a signed `{org, connection, channel}` triple (`oauth_state.sign_token`), which is how
`POST /webhooks/google_calendar` authenticates a push and finds its tenant. The channel and its
`resourceId` go in the `gcal_channel` cursor (with `expires_at`, for renewal); an initial sync is
queued as an inbox row so the OAuth callback returns at once.

Sync (one inbox row): `events.list(syncToken)` -> for each changed event, resolve attendee
emails to users of THIS tenant (RLS-bound read, case-insensitive), then emit, through the
outbox:

- `integration-events` `calendar_event_upserted`, external_id = event id, payload
  {meeting_id, title, starts_at, ends_at, attendees [{email, user_id|null}], organizer_email,
  attachments [{file_id, title, mime_type, object_key|null}]}. core-api upserts `meetings` +
  `meeting_participants` from it. `meeting_id` is uuid5(org, provider, event id): deterministic,
  so the attachment events below can name the meeting before core-api has created it, and the
  same event always maps to the same meeting.
- one `attachment_to_ingest` per Drive attachment, after writing the bytes to
  `mytheus-attachments/{org_id}/{meeting_id}/` - exactly the {meeting_id, object_key, filename}
  ingestion-service's consumer reads.

Trust: an event becomes a meeting only when its organizer's email belongs to a user of THIS
tenant, and an attachment is downloaded only when one of its Drive owners does. Anyone can send
the connected account an invite, so neither the invite nor its files are trusted by default.

Idempotency keys: `gcal:{event_id}:{updated}` for the upsert (one emit per event version) and
`gcal:{event_id}:attachment:{file_id}` for an attachment (downloaded and ingested once). The
`syncToken` advances only after the whole delta has been enqueued, in the same transaction.
Cancelled events are skipped (meeting cancellation from a calendar is not in P12 scope).
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from services.integration_service import config, store
from services.integration_service.clients import Clients, ProviderError
from services.integration_service.oauth_state import sign_token
from services.integration_service.redaction import get_logger
from services.integration_service.storage import AttachmentStore, object_key

logger = get_logger(__name__)

PROVIDER = config.GOOGLE_CALENDAR
TOPIC = "integration-events"
PRODUCER = "integration-service"
CHANNEL_KIND = "gcal_channel"
SYNC_KIND = "gcal_sync"
CHANNEL_PURPOSE = "gcal-channel"
MEETING_NS = uuid.uuid5(uuid.NAMESPACE_URL, "https://mytheus.ai/ns/integration-service/meeting")
MAX_TITLE = 500


def meeting_id_for(org_id: uuid.UUID, event_id: str) -> uuid.UUID:
    return uuid.uuid5(MEETING_NS, f"{org_id}:{PROVIDER}:{event_id}")


def _expiry_from_ms(raw: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(raw) / 1000, tz=UTC)
    except (TypeError, ValueError):
        return None


async def watch(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    """Register (or replace) the push channel and store it in the cursor."""
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    channel_id = str(uuid.uuid4())
    channel_token = sign_token(
        config.state_secret(),
        CHANNEL_PURPOSE,
        {"o": str(conn.org_id), "c": str(conn.id), "ch": channel_id},
    )
    result = await clients.calendar.watch(
        tokens.access_token,
        channel_id=channel_id,
        address=config.webhook_url(PROVIDER),
        channel_token=channel_token,
    )
    previous = await store.get_cursor(session, conn.id, CHANNEL_KIND)
    await store.put_cursor(
        session,
        conn.org_id,
        conn.id,
        CHANNEL_KIND,
        {"channel_id": channel_id, "resource_id": result.get("resource_id")},
        _expiry_from_ms(result.get("expiration_ms")) or datetime.now(UTC) + timedelta(days=7),
    )
    if previous and previous[0].get("channel_id") and previous[0].get("resource_id"):
        try:
            await clients.calendar.stop(
                tokens.access_token,
                channel_id=str(previous[0]["channel_id"]),
                resource_id=str(previous[0]["resource_id"]),
            )
        except ProviderError as exc:
            logger.warning("stopping the previous calendar channel failed: %s", exc)


async def post_install(session: AsyncSession, conn: store.Connection, clients: Clients) -> None:
    await watch(session, conn, clients)
    await store.inbox_insert(
        session, conn.org_id, PROVIDER, f"install:{conn.id}:{uuid.uuid4()}", "initial_sync", {}
    )


async def stop(session: AsyncSession, conn: store.Connection, clients: Clients, token: str) -> None:
    cursor = await store.get_cursor(session, conn.id, CHANNEL_KIND)
    if cursor and cursor[0].get("channel_id") and cursor[0].get("resource_id"):
        await clients.calendar.stop(
            token,
            channel_id=str(cursor[0]["channel_id"]),
            resource_id=str(cursor[0]["resource_id"]),
        )


async def process(
    session: AsyncSession,
    org_id: uuid.UUID,
    payload: dict[str, Any],
    event_type: str | None,
    clients: Clients,
    attachments: AttachmentStore | None,
) -> str:
    """One inbox row: a push notification or the initial sync. Returns processed|skipped."""
    if event_type == "sync":
        return "skipped"  # Google's handshake on a new channel carries no change
    conn = await store.get_connection(session, PROVIDER, active_only=True)
    if conn is None:
        return "skipped"
    if event_type != "initial_sync":
        cursor = await store.get_cursor(session, conn.id, CHANNEL_KIND)
        if cursor is None or cursor[0].get("channel_id") != payload.get("channel_id"):
            return "skipped"  # a stale channel from before a renewal or reconnect
    await sync(session, conn, clients, attachments)
    return "processed"


async def sync(
    session: AsyncSession,
    conn: store.Connection,
    clients: Clients,
    attachments: AttachmentStore | None,
) -> int:
    tokens = await store.access_token(session, conn, clients.oauth[PROVIDER])
    cursor = await store.get_cursor(session, conn.id, SYNC_KIND)
    sync_token = (
        str(cursor[0].get("sync_token")) if cursor and cursor[0].get("sync_token") else None
    )
    try:
        events, next_token = await clients.calendar.list_events(
            tokens.access_token, sync_token=sync_token
        )
    except ProviderError as exc:
        if exc.reason != "sync_gone":
            raise
        events, next_token = await clients.calendar.list_events(
            tokens.access_token, sync_token=None
        )
    emitted = 0
    for event in events:
        if await _handle_event(session, conn, event, tokens.access_token, clients, attachments):
            emitted += 1
    await store.put_cursor(session, conn.org_id, conn.id, SYNC_KIND, {"sync_token": next_token})
    await store.touch_synced(session, conn.id)
    return emitted


def _when(part: Any) -> str | None:
    if not isinstance(part, dict):
        return None
    if part.get("dateTime"):
        return str(part["dateTime"])
    if part.get("date"):
        return f"{part['date']}T00:00:00Z"  # all-day: midnight UTC
    return None


async def _resolve_attendees(session: AsyncSession, emails: list[str]) -> dict[str, uuid.UUID]:
    if not emails:
        return {}
    rows = await session.execute(
        text(
            "SELECT lower(email), id FROM users "
            "WHERE lower(email) = ANY(:emails) AND deleted_at IS NULL"
        ),
        {"emails": emails},
    )
    return {str(r[0]): r[1] for r in rows}


async def _owned_by_tenant(
    session: AsyncSession, clients: Clients, access_token: str, file_id: str
) -> bool:
    """True when a Drive owner of the file is a user of this tenant. Files owned outside the
    tenant (a guest's attachment, a shared drive with no user owner) are not ingested."""
    try:
        owners = await clients.calendar.file_owners(access_token, file_id=file_id)
    except ProviderError as exc:
        logger.warning("calendar attachment owner lookup failed: %s", exc)
        return False
    return bool(owners) and bool(await _resolve_attendees(session, sorted(owners)))


async def _already_enqueued(session: AsyncSession, key: str) -> bool:
    row = (
        await session.execute(
            text("SELECT 1 FROM outbox WHERE topic = :t AND idempotency_key = :k"),
            {"t": TOPIC, "k": key},
        )
    ).first()
    return row is not None


async def _handle_event(
    session: AsyncSession,
    conn: store.Connection,
    event: dict[str, Any],
    access_token: str,
    clients: Clients,
    attachments: AttachmentStore | None,
) -> bool:
    event_id = event.get("id")
    if not isinstance(event_id, str) or not event_id or event.get("status") == "cancelled":
        return False
    starts_at, ends_at = _when(event.get("start")), _when(event.get("end"))
    if starts_at is None:
        return False
    org_id = conn.org_id
    meeting_id = meeting_id_for(org_id, event_id)
    raw_attendees = [a for a in event.get("attendees") or [] if isinstance(a, dict)]
    emails = sorted({str(a["email"]).lower() for a in raw_attendees if a.get("email")})
    organizer = event.get("organizer") or {}
    organizer_email = str(organizer.get("email", "")).lower() or None
    matched = await _resolve_attendees(
        session, sorted(set(emails) | ({organizer_email} if organizer_email else set()))
    )
    if organizer_email is None or organizer_email not in matched:
        # Anyone can put the connected account on an invite. Only events organized by a user
        # of this tenant become meetings; an outsider's invite never creates one, never makes
        # the outsider an organizer, and never pulls their files into the tenant's index.
        return False

    attachment_out: list[dict[str, Any]] = []
    for att in event.get("attachments") or []:
        if not isinstance(att, dict) or not att.get("fileId"):
            continue
        file_id = str(att["fileId"])
        key = object_key(org_id, meeting_id, file_id, att.get("title"))
        stored: str | None = None
        idem = f"gcal:{event_id}:attachment:{file_id}"
        if attachments is not None:
            if await _already_enqueued(session, idem):
                stored = key
            elif not await _owned_by_tenant(session, clients, access_token, file_id):
                pass  # an external file: listed on the meeting, never downloaded or ingested
            else:
                try:
                    data = await clients.calendar.download(
                        access_token, file_id=file_id, mime_type=att.get("mimeType")
                    )
                    await attachments.put(key, data, att.get("mimeType"))
                    stored = key
                    await enqueue(
                        session,
                        EventEnvelope(
                            topic=TOPIC,
                            org_id=str(org_id),
                            producer=PRODUCER,
                            idempotency_key=idem,
                            payload={
                                "provider": PROVIDER,
                                "event_type": "attachment_to_ingest",
                                "external_id": file_id,
                                "payload_json": json.dumps(
                                    {
                                        "meeting_id": str(meeting_id),
                                        "object_key": key,
                                        "filename": str(att.get("title") or "attachment"),
                                    }
                                ),
                            },
                        ),
                    )
                except ProviderError as exc:
                    logger.warning("calendar attachment skipped: %s", exc)
        attachment_out.append(
            {
                "file_id": file_id,
                "title": att.get("title"),
                "mime_type": att.get("mimeType"),
                "object_key": stored,
            }
        )

    payload = {
        "meeting_id": str(meeting_id),
        "title": str(event.get("summary") or "Untitled meeting")[:MAX_TITLE],
        "starts_at": starts_at,
        "ends_at": ends_at,
        "attendees": [
            {"email": e, "user_id": str(matched[e]) if e in matched else None} for e in emails
        ],
        "organizer_email": organizer_email,
        "attachments": attachment_out,
    }
    version = str(event.get("updated") or event.get("etag") or "0")
    await enqueue(
        session,
        EventEnvelope(
            topic=TOPIC,
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"gcal:{event_id}:{version}",
            payload={
                "provider": PROVIDER,
                "event_type": "calendar_event_upserted",
                "external_id": event_id,
                "payload_json": json.dumps(payload),
            },
        ),
    )
    await store.put_link(
        session, org_id, PROVIDER, "meeting", meeting_id, event_id, event.get("htmlLink"), None
    )
    return True
