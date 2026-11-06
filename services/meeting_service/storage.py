"""The `mytheus-recordings` key convention, and the `recording_ref` builder.

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

EGRESS FILEPATH vs `recording_key` (Plan 3-4 Task 2 of the recording follow-up). LiveKit's
egress filepath template cannot contain the egress id - it is assigned by LiveKit only once
`start_egress` returns - so the object LiveKit actually writes is named by
`egress_filepath` (`{org_id}/{meeting_id}/{time}.<ext>`, LiveKit expands `{time}`). The
tenant prefix, which is what retention, residency and RTBF act on (canon §6:
`{org_id}/{meeting_id}/`), is identical either way. The webhook's `egress_ended` path stores
the REAL uploaded filename from the egress result when it sits under this meeting's prefix,
and only falls back to `recording_key` when the result carries no filename.

The bucket lifecycle policy (cold tier after 30 days, expiry at the tenant's
`retention_days_transcripts`) is built and applied by `services.meeting_service.
bucket_lifecycle`. RTBF purge is the workflow-service saga's job, not this module's.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

# Canon (CLAUDE.md service map): meeting-service owns the `mytheus-recordings` bucket.
RECORDINGS_BUCKET = "mytheus-recordings"


class RecordingStorage(Protocol):
    def recording_key(self, *, org_id: UUID, meeting_id: UUID, egress_id: str) -> str: ...


@dataclass(frozen=True)
class KeyConventionRecordingStorage:
    """The real key convention. `bucket` is carried for callers that need the full location
    (e.g. a future presigned-URL reader) without baking `mytheus-recordings` into every call
    site."""

    bucket: str = RECORDINGS_BUCKET

    def recording_key(self, *, org_id: UUID, meeting_id: UUID, egress_id: str) -> str:
        return f"{org_id}/{meeting_id}/{egress_id}"


def recording_prefix(*, org_id: UUID, meeting_id: UUID) -> str:
    """Canon §6 key prefix for one meeting's recordings: `{org_id}/{meeting_id}/`."""
    return f"{org_id}/{meeting_id}/"


def egress_filepath(*, org_id: UUID, meeting_id: UUID, audio_only: bool) -> str:
    """The LiveKit egress output template. `{time}` is expanded by LiveKit, not here."""
    ext = "ogg" if audio_only else "mp4"
    return f"{recording_prefix(org_id=org_id, meeting_id=meeting_id)}{{time}}.{ext}"


def _flag(value: str | None, default: bool) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RecordingConfig:
    """Recording/egress settings, from env. No tenant-level recording setting exists in
    `tenant_settings` today, so `RECORDING_ENABLED` (default false) is the gate; consent is
    checked separately per meeting (`room_lifecycle.ConsentGate`).

    The S3 credentials are handed to LiveKit's egress worker in the egress request (LiveKit
    writes the object, not this service). `repr=False` keeps them out of logs and tracebacks.
    """

    enabled: bool = False
    bucket: str = RECORDINGS_BUCKET
    endpoint_url: str | None = None
    region: str = "auto"
    access_key_id: str | None = field(default=None, repr=False)
    secret_access_key: str | None = field(default=None, repr=False)
    force_path_style: bool = False
    audio_only: bool = False

    @property
    def s3_configured(self) -> bool:
        return bool(self.access_key_id and self.secret_access_key and self.bucket)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> RecordingConfig:
        source = os.environ if env is None else env
        return cls(
            enabled=_flag(source.get("RECORDING_ENABLED"), False),
            bucket=source.get("RECORDING_BUCKET") or RECORDINGS_BUCKET,
            endpoint_url=source.get("S3_ENDPOINT_URL") or None,
            region=source.get("S3_REGION") or "auto",
            access_key_id=source.get("S3_ACCESS_KEY_ID") or None,
            secret_access_key=source.get("S3_SECRET_ACCESS_KEY") or None,
            force_path_style=_flag(source.get("S3_FORCE_PATH_STYLE"), False),
            audio_only=_flag(source.get("RECORDING_AUDIO_ONLY"), False),
        )


DEFAULT_STORAGE: RecordingStorage = KeyConventionRecordingStorage()

__all__ = [
    "RECORDINGS_BUCKET",
    "RecordingStorage",
    "KeyConventionRecordingStorage",
    "DEFAULT_STORAGE",
    "RecordingConfig",
    "egress_filepath",
    "recording_prefix",
]
