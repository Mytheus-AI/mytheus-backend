"""The `nexus-recordings` key convention, and the `recording_ref` builder.

Deviation (Task 4 brief): no S3 bucket and no LiveKit account exist for this build, and this
task does not create one. What it must get right, for real, is the KEY CONVENTION -
`{org_id}/{meeting_id}/{egress_id}` - so `meeting_rooms.recording_ref` is deterministic and
consistent before a real bucket exists behind it.

No S3 API call happens anywhere in this module, and that is not a stub: this service never
uploads or reads recording bytes itself. LiveKit's own egress process writes the object
directly to S3, using AWS credentials and an output-path template supplied when a future
`start_egress` caller starts the recording. This module's only job is to be the ONE place that
key is computed, so the webhook path (which persists the reference once egress ends) and the
`start_egress` caller (which will one day supply the same key as the output path) can never
disagree on what "the same recording" is named. That is also why there is only one concrete
implementation and no separate fake: it does no I/O, so it is already deterministic and
side-effect-free in tests.

The lifecycle policy (cold storage after 30 days, RTBF purge - Plan 3-4 section 3.1 step 7) is
`mytheus-platform` infrastructure. Deferred, not built here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

# Canon (CLAUDE.md service map): meeting-service owns the `nexus-recordings` bucket.
RECORDINGS_BUCKET = "nexus-recordings"


class RecordingStorage(Protocol):
    def recording_key(self, *, org_id: UUID, meeting_id: UUID, egress_id: str) -> str: ...


@dataclass(frozen=True)
class KeyConventionRecordingStorage:
    """The real key convention. `bucket` is carried for callers that need the full location
    (e.g. a future presigned-URL reader) without baking `nexus-recordings` into every call site."""

    bucket: str = RECORDINGS_BUCKET

    def recording_key(self, *, org_id: UUID, meeting_id: UUID, egress_id: str) -> str:
        return f"{org_id}/{meeting_id}/{egress_id}"


DEFAULT_STORAGE: RecordingStorage = KeyConventionRecordingStorage()

__all__ = [
    "RECORDINGS_BUCKET",
    "RecordingStorage",
    "KeyConventionRecordingStorage",
    "DEFAULT_STORAGE",
]
