"""What the Jira and Linear connectors share: the due date and the link back to Mytheus."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from services.integration_service import config

_EPOCH = date(1970, 1, 1)

# The `actions` event's `meeting_id` for a task created directly (`POST /api/v1/actions`): the
# schema requires a uuid, and core-api sends the nil uuid when there is no meeting.
NO_MEETING = "00000000-0000-0000-0000-000000000000"


def due_iso(raw: Any) -> str | None:
    """An `actions` due date (Avro days since epoch, a date, a datetime or an ISO string) as
    `YYYY-MM-DD`, or None."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    if isinstance(raw, int):
        return (_EPOCH + timedelta(days=raw)).isoformat()
    try:
        return date.fromisoformat(str(raw)).isoformat()
    except ValueError:
        return None


def source_url(meeting_id: Any) -> str:
    """The Mytheus page an issue links back to: the meeting, or the task list for a task made
    without one."""
    if not meeting_id or str(meeting_id) == NO_MEETING:
        return f"{config.app_base_url()}/tasks"
    return f"{config.app_base_url()}/meetings/{meeting_id}"
