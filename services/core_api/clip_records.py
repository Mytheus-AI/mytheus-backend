"""Clips (polish wave 3, Recording playback): the clip model, the off-the-record rule, and the
public clip page's content. Kept apart from the routes so `routes/share_links.py` (the public
read) and `routes/clips.py` (CRUD) can both use it without importing each other.

A clip is a named span of a meeting on the meeting's own clock (`meeting_rooms.started_at` is
zero, the transcript's origin), at most `MAX_CLIP_MS` (10 minutes). core-api owns
`meeting_clips` because a clip is meeting content built on the transcript core-api already
serves, and its share rides core-api's `share_links`; the video, when a share includes it, is
meeting-service's (it owns the bucket), asked for through `recording_client.clip_video`.

THE OFF-THE-RECORD RULE. Nothing said off the record was kept, so a clip can never span an
off-the-record stretch (`meeting_off_record_segments`, a running one included): creating one
that overlaps is refused (409, reason `off_record_overlap`).

THE PUBLIC PAGE. A `meeting_clip` share always shows the transcript lines of the span (speakers
by name, never by email; times relative to the clip's start) and the video only when the sharer
chose `include_video`. It is built at read time as the creator sees it: a creator who is no
longer a participant (or a confidential meeting they do not host) takes the page down.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.core_api import records

MAX_CLIP_MS = 600_000
MAX_LINES = 400

ClipVideoStatus = Literal["ready", "rendering", "unavailable", "not_included"]


class ClipLine(BaseModel):
    speaker_name: str | None = Field(default=None, description="By name; null when unknown.")
    offset_ms: int = Field(description="When the line starts, from the clip's start.")
    end_offset_ms: int
    text: str


class ClipVideoView(BaseModel):
    """The clip's video on its public page. `not_included`: the sharer shared the words only.
    `rendering`: being cut from the recording, ask again shortly. `unavailable`: there is no
    recording to cut it from, or recording is not connected on this server (`reason`)."""

    status: ClipVideoStatus
    reason: str | None = None
    url: str | None = Field(
        default=None, description="Short-lived presigned GET of the clip alone, when ready."
    )
    expires_at: datetime | None = None
    media_type: Literal["video/mp4", "audio/ogg"] | None = None


class ClipShare(BaseModel):
    """What a public clip page renders."""

    clip_id: uuid.UUID
    title: str
    meeting_title: str
    meeting_start: datetime | None = None
    start_ms: int
    end_ms: int
    duration_ms: int
    lines: list[ClipLine]
    video: ClipVideoView


async def off_record_overlap(
    session: AsyncSession, meeting_id: uuid.UUID, start_ms: int, end_ms: int
) -> bool:
    row = (
        await session.execute(
            text(
                "SELECT 1 FROM meeting_off_record_segments WHERE meeting_id = :m "
                "AND start_ms IS NOT NULL AND start_ms < :e "
                "AND (end_ms IS NULL OR end_ms > :s) LIMIT 1"
            ),
            {"m": str(meeting_id), "s": start_ms, "e": end_ms},
        )
    ).first()
    return row is not None


async def clip_row(session: AsyncSession, clip_id: uuid.UUID) -> dict[str, Any] | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT c.id, c.meeting_id, c.created_by, c.title, c.start_ms, c.end_ms, "
                    "c.created_at FROM meeting_clips c JOIN meetings m ON m.id = c.meeting_id "
                    "WHERE c.id = :c AND m.deleted_at IS NULL"
                ),
                {"c": str(clip_id)},
            )
        )
        .mappings()
        .first()
    )
    return dict(row) if row else None


async def speaker_names(
    session: AsyncSession, meeting_id: uuid.UUID, speaker_ids: set[str]
) -> dict[str, str | None]:
    """A transcript speaker id is the LiveKit identity (a user id) for a Mytheus call, or a
    diarized label mapped in `meeting_speakers`. Named from the email, never the email."""
    if not speaker_ids:
        return {}
    names: dict[str, str | None] = {}
    mapped = await session.execute(
        text(
            "SELECT s.speaker_id, u.email FROM meeting_speakers s JOIN users u ON u.id = s.user_id "
            "WHERE s.meeting_id = :m AND s.speaker_id = ANY(CAST(:ids AS text[]))"
        ),
        {"m": str(meeting_id), "ids": list(speaker_ids)},
    )
    for sid, email in mapped:
        names[str(sid)] = records.display_name(email)
    as_users = []
    for sid in speaker_ids - set(names):
        try:
            as_users.append(str(uuid.UUID(sid)))
        except ValueError:
            continue
    if as_users:
        rows = await session.execute(
            text("SELECT id, email FROM users WHERE id = ANY(CAST(:ids AS uuid[]))"),
            {"ids": as_users},
        )
        for uid, email in rows:
            names[str(uid)] = records.display_name(email)
    return names


async def clip_lines(
    session: AsyncSession, meeting_id: uuid.UUID, start_ms: int, end_ms: int
) -> list[ClipLine]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT speaker_id, start_ms, end_ms, text FROM transcript_chunks "
                    "WHERE meeting_id = :m AND is_final = true AND start_ms < :e AND end_ms > :s "
                    "ORDER BY start_ms ASC, id ASC LIMIT :n"
                ),
                {"m": str(meeting_id), "s": start_ms, "e": end_ms, "n": MAX_LINES},
            )
        )
        .mappings()
        .all()
    )
    rows = [r for r in rows if str(r["text"] or "").strip()]
    names = await speaker_names(session, meeting_id, {str(r["speaker_id"]) for r in rows})
    return [
        ClipLine(
            speaker_name=names.get(str(r["speaker_id"])),
            offset_ms=max(0, int(r["start_ms"]) - start_ms),
            end_offset_ms=max(0, min(int(r["end_ms"]), end_ms) - start_ms),
            text=str(r["text"]).strip(),
        )
        for r in rows
    ]


async def build_clip_share(
    session: AsyncSession, clip_id: uuid.UUID, creator: uuid.UUID
) -> tuple[ClipShare, dict[str, Any]] | None:
    """The clip page as the link's creator may share it, with video `not_included` (the caller
    fills it in after its transaction). None when the clip is gone or the creator lost access."""
    clip = await clip_row(session, clip_id)
    if clip is None:
        return None
    access = await records.access_for(session, clip["meeting_id"], creator)
    if access is None or not access.shareable_by(creator):
        return None
    start = (
        await session.execute(
            text("SELECT scheduled_start FROM meetings WHERE id = :m"),
            {"m": str(clip["meeting_id"])},
        )
    ).scalar()
    share = ClipShare(
        clip_id=clip["id"],
        title=clip["title"],
        meeting_title=access.title,
        meeting_start=start,
        start_ms=clip["start_ms"],
        end_ms=clip["end_ms"],
        duration_ms=clip["end_ms"] - clip["start_ms"],
        lines=await clip_lines(session, clip["meeting_id"], clip["start_ms"], clip["end_ms"]),
        video=ClipVideoView(status="not_included"),
    )
    return share, clip


def video_view(raw: dict[str, Any] | None) -> ClipVideoView:
    """meeting-service's `ClipVideo` as the page shows it; no answer is `unavailable`."""
    if raw is None:
        return ClipVideoView(status="unavailable", reason="not_connected")
    status = raw.get("status")
    if status == "ready" and raw.get("url"):
        return ClipVideoView(
            status="ready",
            url=str(raw["url"]),
            expires_at=raw.get("expires_at"),
            media_type=raw.get("media_type"),
        )
    if status == "rendering":
        return ClipVideoView(status="rendering", media_type=raw.get("media_type"))
    return ClipVideoView(status="unavailable", reason=raw.get("reason"))


__all__ = [
    "MAX_CLIP_MS",
    "ClipLine",
    "ClipShare",
    "ClipVideoView",
    "build_clip_share",
    "clip_lines",
    "clip_row",
    "off_record_overlap",
    "video_view",
]
