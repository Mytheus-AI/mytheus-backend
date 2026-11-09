"""Report a LiveKit room's lifecycle to core-api, which owns `meetings.status`.

meeting-service sees the room start and LiveKit close it (after the departure timeout, five
minutes with nobody in it); core-api owns the meeting row. This posts the fact to core-api's
internal route (`services/core_api/routes/internal_meetings.py`). Called from a background task
after the webhook's own transaction, and never raises: a status that does not reach core-api is
logged and leaves the meeting as it was, which is never worse than before this existed.

Settings:
- `CORE_API_URL` (default `http://core-api:8000`, the compose alias).
- `MYTHEUS_CORE_CALLER_TOKENS`: the same `service=secret,...` list core-api reads; this service
  presents its own `meeting-service` entry. Unset means nothing is reported.
"""

from __future__ import annotations

import logging
import os
from typing import Literal
from uuid import UUID

import httpx

logger = logging.getLogger(__name__)

SERVICE_NAME = "meeting-service"
_TIMEOUT = httpx.Timeout(5.0)


def _own_secret() -> str | None:
    for entry in os.environ.get("MYTHEUS_CORE_CALLER_TOKENS", "").split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


async def report_room_state(
    *, org_id: UUID, meeting_id: UUID, state: Literal["live", "ended"]
) -> bool:
    """Tell core-api the meeting's call is live or has ended. Returns whether it was accepted."""
    secret = _own_secret()
    if secret is None:
        logger.warning(
            "MYTHEUS_CORE_CALLER_TOKENS has no meeting-service entry; meeting %s stays as it is",
            meeting_id,
        )
        return False
    base = os.environ.get("CORE_API_URL", "http://core-api:8000").rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            response = await client.post(
                f"{base}/internal/meetings/{meeting_id}/room-state",
                json={"state": state},
                headers={
                    "X-Mytheus-Service": SERVICE_NAME,
                    "X-Mytheus-Service-Token": secret,
                    "X-Mytheus-Org": str(org_id),
                },
            )
    except httpx.HTTPError:
        logger.exception("reporting meeting %s as %s failed", meeting_id, state)
        return False
    if response.status_code >= 400:
        logger.error(
            "core-api refused meeting %s -> %s: HTTP %s", meeting_id, state, response.status_code
        )
        return False
    return True
