"""core-api's calls to meeting-service's recording routes (polish wave 3, Recording playback).

meeting-service owns the `mytheus-recordings` bucket and the `meeting_recordings` rows, so
core-api never touches either; it asks:

- `pause` / `resume`: the meeting went off the record / came back (`routes/off_record.py`),
  so the recording keeps nothing from the stretch that was off the record;
- `erase`: a meeting's data erasure (`routes/erasure.py`) deletes its recording objects;
- `clip_video`: the video of a publicly shared clip (`routes/clips.py`), the clip's own cut,
  never the whole recording.

Settings:
- `MEETING_SERVICE_URL` (default `http://meeting-service:8000`, the compose alias);
- `MYTHEUS_MEETING_CALLER_TOKENS`: the same `service=secret,...` list meeting-service reads;
  core-api presents its own `core-api` entry. Unset means nothing is called: `pause`/`resume`
  do nothing (recording is off anyway without it in any real deployment), `erase` reports
  `not_connected`, and a clip share shows its transcript lines without the video.

Every call is bounded (5 s, 15 s for erase) and never raises; the result says what happened.
"""

from __future__ import annotations

import logging
import os
import uuid
from typing import Any, Literal

import httpx

logger = logging.getLogger(__name__)

SERVICE_NAME = "core-api"
TOKENS_ENV = "MYTHEUS_MEETING_CALLER_TOKENS"


def _own_secret() -> str | None:
    for entry in os.environ.get(TOKENS_ENV, "").split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


def configured() -> bool:
    return _own_secret() is not None


async def _post(
    org_id: uuid.UUID, path: str, body: dict[str, Any] | None = None, *, timeout: float = 5.0
) -> dict[str, Any] | None:
    secret = _own_secret()
    if secret is None:
        return None
    base = os.environ.get("MEETING_SERVICE_URL", "http://meeting-service:8000").rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
            response = await client.post(
                f"{base}{path}",
                json=body,
                headers={
                    "X-Mytheus-Service": SERVICE_NAME,
                    "X-Mytheus-Service-Token": secret,
                    "X-Mytheus-Org": str(org_id),
                },
            )
    except httpx.HTTPError:
        logger.warning("meeting-service %s failed", path.rsplit("/", 1)[-1], exc_info=True)
        return None
    if response.status_code >= 400:
        logger.warning("meeting-service refused %s: HTTP %s", path, response.status_code)
        return None
    try:
        data = response.json()
    except ValueError:
        return None
    return data if isinstance(data, dict) else None


async def pause(org_id: uuid.UUID, meeting_id: uuid.UUID) -> None:
    await _post(org_id, f"/internal/meetings/{meeting_id}/recording/pause")


async def resume(org_id: uuid.UUID, meeting_id: uuid.UUID) -> None:
    await _post(org_id, f"/internal/meetings/{meeting_id}/recording/resume")


EraseStatus = Literal["erased", "not_connected", "failed"]


async def erase(org_id: uuid.UUID, meeting_id: uuid.UUID) -> tuple[EraseStatus, int]:
    """(`erased` | `not_connected` | `failed`, objects deleted)."""
    if not configured():
        return "not_connected", 0
    data = await _post(org_id, f"/internal/meetings/{meeting_id}/recording/erase", timeout=15.0)
    if data is None:
        return "failed", 0
    status = data.get("status")
    return (
        ("erased" if status == "erased" else "not_connected"),
        int(data.get("objects_deleted") or 0),
    )


async def clip_video(
    org_id: uuid.UUID, meeting_id: uuid.UUID, clip_id: uuid.UUID, start_ms: int, end_ms: int
) -> dict[str, Any] | None:
    """meeting-service's `ClipVideo` for the clip, or None when it cannot be asked."""
    return await _post(
        org_id,
        f"/internal/meetings/{meeting_id}/clips/{clip_id}/video",
        {"start_ms": start_ms, "end_ms": end_ms},
    )


__all__ = ["clip_video", "configured", "erase", "pause", "resume"]
