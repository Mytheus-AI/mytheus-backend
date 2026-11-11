"""Pydantic v2 wire models for meeting-service.

`org_id` appears in no request model, same rule as `core-api`'s (`services/core_api/schemas.
py`). It is derived from the verified token via `RequestContext`, never accepted from a
caller. The token-mint route takes an OPTIONAL body: the recording/transcription consent
acknowledgment the pre-join sends with its first token request (ADR-0018). `meeting_id` comes
from the path and everything else from the tenant context.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ConsentAck(BaseModel):
    """The participant read the consent notice and agreed to join with it in force.

    `version` names the exact notice text the client showed (the UI owns the copy, so it owns the
    version label). It is stored verbatim in `meeting_participants.consent_version`.
    """

    model_config = ConfigDict(extra="forbid")

    version: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._-]+$")


class TokenRequest(BaseModel):
    """Optional body of `POST /api/meetings/{meeting_id}/token`. An empty or absent body is the
    original contract: no acknowledgment is recorded."""

    model_config = ConfigDict(extra="forbid")

    consent: ConsentAck | None = None


class TokenResponse(BaseModel):
    token: str
    room: str
    livekit_url: str
    # Additive (polish program, "Consent that is real"): what the server now holds for this
    # participant, so the client never has to guess whether it must ask again.
    consented_at: datetime | None = None
    consent_version: str | None = None
    # When the meeting's call first opened (the first successful join, server-side), so every
    # client shows the same meeting time instead of the time since it joined.
    started_at: datetime | None = None


class RoomSession(BaseModel):
    """`GET /api/meetings/{meeting_id}/session`: the caller's standing in this meeting's call.

    Read by the pre-join (ask for consent only when it has not been given for this meeting) and
    by the room (the meeting clock).
    """

    consent_required: bool
    consented_at: datetime | None
    consent_version: str | None
    # Whether audio/video is recorded (egress), not only transcribed. Server-wide today
    # (`RECORDING_ENABLED`), so the consent copy can say exactly what is kept.
    recording: bool
    started_at: datetime | None
    ended: bool


class ConsentClause(BaseModel):
    key: str = Field(description="Stable clause id, e.g. `transcript`, `audio_video`.")
    title: str
    body: str


class ConsentReceipt(BaseModel):
    """`GET /api/meetings/{meeting_id}/consent-receipt`: the caller's own record of what they
    agreed to for this meeting (polish wave 3, Off the record). Only ever the caller's own row."""

    meeting_id: UUID
    meeting_title: str
    consent_required: bool = Field(
        description="Whether this workspace requires agreement before joining."
    )
    consented: bool
    consented_at: datetime | None = Field(default=None, description="When you agreed.")
    consent_version: str | None = Field(
        default=None, description="The version of the notice you agreed to."
    )
    current_version: str = Field(description="The notice's version today.")
    recording: bool = Field(description="Whether audio and video are recorded.")
    clauses: list[ConsentClause] | None = Field(
        default=None,
        description="What the notice you agreed to said, clause by clause; null when the stored "
        "version is not one this server knows.",
    )


# --- Recording playback (polish wave 3) ----------------------------------------------------------

RecordingStatus = Literal["not_enabled", "recording", "processing", "ready", "failed", "none"]
RecordingReason = Literal[
    "storage_not_configured", "consent_missing", "not_recorded", "not_started"
]
SegmentStatus = Literal["recording", "processing", "ready", "failed"]


class RecordingSegment(BaseModel):
    """One continuous stretch of the recording (one LiveKit egress). A meeting taken off the
    record has one segment per stretch that was recorded, with the gaps between them."""

    id: UUID
    status: SegmentStatus
    media_type: Literal["video/mp4", "audio/ogg"]
    offset_ms: int | None = Field(
        default=None,
        description="Where the segment starts on the meeting's clock (the transcript's time "
        "origin). Null when the room clock is unknown.",
    )
    duration_ms: int | None = Field(default=None, description="Known once the segment is ready.")
    started_at: datetime
    ended_at: datetime | None = None
    url: str | None = Field(
        default=None,
        description="A short-lived presigned GET for a ready segment; never stored, never "
        "public. Ask again for a fresh one when it expires.",
    )


class RecordingState(BaseModel):
    """`GET /api/meetings/{meeting_id}/recording`: whether this meeting has a recording a
    participant can play, and the playable segments.

    - `not_enabled`: recording is off on this server (`reason` `storage_not_configured` when it
      is on but the bucket credentials are missing);
    - `none`: nothing was recorded (`reason` `consent_missing` when the consent gate refused
      because someone had not agreed, `not_started` before the call, `not_recorded` otherwise);
    - `recording`: the call is being recorded now;
    - `processing`: the call ended and LiveKit is finishing the upload;
    - `ready`: at least one segment can be played (`segments[].url`);
    - `failed`: every segment failed to record or upload.
    """

    meeting_id: UUID
    status: RecordingStatus
    reason: RecordingReason | None = None
    segments: list[RecordingSegment] = Field(default_factory=list)
    duration_ms: int = Field(default=0, description="Total length of the ready segments.")
    url_expires_at: datetime | None = Field(
        default=None, description="When the presigned segment URLs stop working."
    )


class RecordingControlResult(BaseModel):
    """Internal: what pausing or resuming the recording for off the record did."""

    meeting_id: UUID
    changed: bool
    detail: str


class RecordingErased(BaseModel):
    """Internal: a meeting's recording erased (data erasure, core-api's caller)."""

    meeting_id: UUID
    status: Literal["erased", "not_connected"] = Field(
        description="`not_connected`: no bucket credentials on this server, so only the "
        "recording rows were removed (there can be no objects LiveKit wrote with them)."
    )
    objects_deleted: int
    segments_deleted: int


class ClipVideoRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)


class ClipVideo(BaseModel):
    """Internal: the video for one clip, for a public clip share. Only ever the clip's own
    rendered object (cut from the recording), never the whole recording."""

    status: Literal["ready", "rendering", "unavailable"]
    reason: (
        Literal[
            "not_connected",
            "no_recording",
            "not_in_recording",
            "renderer_not_installed",
            "render_failed",
        ]
        | None
    ) = None
    url: str | None = None
    expires_at: datetime | None = None
    media_type: Literal["video/mp4", "audio/ogg"] | None = None
