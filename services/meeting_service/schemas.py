"""Pydantic v2 wire models for meeting-service.

`org_id` appears in no request model, same rule as `core-api`'s (`services/core_api/schemas.
py`). It is derived from the verified token via `RequestContext`, never accepted from a
caller. The token-mint route takes an OPTIONAL body: the recording/transcription consent
acknowledgment the pre-join sends with its first token request (ADR-0018). `meeting_id` comes
from the path and everything else from the tenant context.
"""

from __future__ import annotations

from datetime import datetime

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
