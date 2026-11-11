"""Cutting one clip out of a meeting's recording, for a public clip share.

A public clip page must never be handed the whole recording: a presigned URL to the full
segment would let anyone holding the share link watch the rest of the meeting. So the video of a
shared clip is its OWN object, cut from the segment that contains it and written next to it:
`{org_id}/{meeting_id}/clips/{clip_id}.{mp4|ogg}`. Under the meeting's prefix on purpose, so
the bucket's retention rule and a meeting's erasure cover it with no extra bookkeeping.

The cut is `ffmpeg -ss <start> -i <presigned source> -t <length> -c copy`: a stream copy, so it
is fast and cheap (no re-encode); it starts on the nearest keyframe at or before the clip's
start, which can add up to a couple of seconds at the front. ffmpeg reads the source over HTTP
range requests, so it never downloads the whole recording.

NOT CONNECTED until ffmpeg is installed where meeting-service runs (`FFMPEG_BIN`, default
`ffmpeg` on PATH) and recording storage is configured: without it the clip page shows the
transcript lines only and says the video is not available. One render per clip at a time
(`nexus:{org_id}:clip-render:{clip_id}`, 15 minutes); a failed render is retried by the next
read once that lock expires.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import tempfile
from uuid import UUID

from libs.shared.cache import get_redis, redis_key
from services.meeting_service.recording_objects import RecordingObjects
from services.meeting_service.storage import recording_prefix

logger = logging.getLogger(__name__)

RENDER_LOCK_SECONDS = 900
_RENDER_TIMEOUT_SECONDS = 600
SOURCE_URL_TTL_SECONDS = 1800


def ffmpeg_path() -> str | None:
    return shutil.which(os.environ.get("FFMPEG_BIN", "ffmpeg") or "ffmpeg")


def clip_key(*, org_id: UUID, meeting_id: UUID, clip_id: UUID, media_type: str) -> str:
    ext = "ogg" if media_type == "audio/ogg" else "mp4"
    return f"{recording_prefix(org_id=org_id, meeting_id=meeting_id)}clips/{clip_id}.{ext}"


def render_lock_key(org_id: UUID, clip_id: UUID) -> str:
    return redis_key(org_id, "clip-render", clip_id)


async def claim_render(org_id: UUID, clip_id: UUID) -> bool:
    """True when this caller should render the clip now (nobody else is). A Redis outage
    answers True: a duplicate render writes the same object, which is harmless."""
    try:
        return bool(
            await get_redis().set(
                render_lock_key(org_id, clip_id), b"1", nx=True, ex=RENDER_LOCK_SECONDS
            )
        )
    except Exception:  # noqa: BLE001
        logger.warning("clip render lock unavailable for clip %s; rendering anyway", clip_id)
        return True


async def render_clip(
    objects: RecordingObjects,
    *,
    source_url: str,
    start_s: float,
    duration_s: float,
    key: str,
    media_type: str,
) -> bool:
    """Cut and upload one clip. Never raises; returns whether the object was written."""
    binary = ffmpeg_path()
    if binary is None:
        return False
    suffix = ".ogg" if media_type == "audio/ogg" else ".mp4"
    with tempfile.TemporaryDirectory(prefix="mytheus-clip-") as tmp:
        out = os.path.join(tmp, f"clip{suffix}")
        args = [
            binary,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-ss",
            f"{max(0.0, start_s):.3f}",
            "-i",
            source_url,
            "-t",
            f"{max(0.1, duration_s):.3f}",
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
        ]
        if suffix == ".mp4":
            args += ["-movflags", "+faststart"]
        args += ["-y", out]
        try:
            proc = await asyncio.create_subprocess_exec(
                *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await asyncio.wait_for(proc.communicate(), _RENDER_TIMEOUT_SECONDS)
        except (TimeoutError, OSError):
            logger.warning("clip render did not finish for %s", key, exc_info=True)
            return False
        if proc.returncode != 0 or not os.path.exists(out) or os.path.getsize(out) == 0:
            # stderr can carry the presigned source URL: log its length, not its text.
            logger.warning(
                "clip render failed for %s (exit %s, %d bytes of stderr)",
                key,
                proc.returncode,
                len(stderr or b""),
            )
            return False
        try:
            await objects.upload_file(out, key, content_type=media_type)
        except Exception:  # noqa: BLE001
            logger.warning("clip upload failed for %s", key, exc_info=True)
            return False
    return True


__all__ = [
    "SOURCE_URL_TTL_SECONDS",
    "claim_render",
    "clip_key",
    "ffmpeg_path",
    "render_clip",
    "render_lock_key",
]
