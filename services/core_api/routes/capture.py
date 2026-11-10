"""In-person capture: the phone records a meeting held in a room, through `/capture/audio`.

The browser extension's capture (mytheus-ui `src/app/api/capture/session/*`) is minted by the
UI's server from the user's Clerk cookie. The phone has no such server: it calls these routes
with its own Clerk bearer through api-gateway, and core-api, which owns `meetings`, does the
same three things the UI route does, in one transaction:

1. creates the meeting with `source` `in_person` (no Mytheus LiveKit room is opened for it,
   exactly like a captured call) and the caller as its host, and marks it `live`;
2. records the caller's consent (`capture.started`, with `consent_at`: their confirmation that
   everyone in the room knows it is being recorded); without `consent: true` nothing is
   created (422 `CONSENT_REQUIRED`);
3. signs a 15-minute capture token for transcription-service's `/capture/audio`, the exact
   format of `mytheus-ui`'s `signCaptureToken` (`capture_auth.py` in mytheus-realtime): compact
   JWS, HS256, `CAPTURE_TOKEN_SECRET`, claims `typ` capture, `iss` mytheus-ui, `aud`
   transcription-service, `sub` users.id, `org`, `mid`, `src` in_person, `consent_at`, `iat`,
   `exp`. transcription-service writes the consent to `audit_logs` again before any audio is
   accepted, as it does for the extension.

`POST /api/v1/capture/sessions/{meeting_id}/refresh` mints a fresh token for a running capture
(the socket reconnects after the 15 minutes); `.../end` ends the meeting (core-api's ordinary
`ended` event starts the post-meeting processing) with its real start and end times.

Not connected: without `CAPTURE_TOKEN_SECRET` (32+ characters) and `CAPTURE_INGEST_URL`
(`wss://.../capture/audio`) every route answers 503 `DEPENDENCY_UNAVAILABLE` with
`reason: capture_not_configured`, and `GET /api/v1/capture/status` says `configured: false` so
the app can show it before asking for the microphone.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Final, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_CAPTURE_STARTED, audit
from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api.routes.meetings import create_meeting, update_meeting
from services.core_api.schemas import MeetingCreate, MeetingUpdate

router = APIRouter(prefix="/api/v1/capture", tags=["capture"])

SECRET_ENV = "CAPTURE_TOKEN_SECRET"
INGEST_ENV = "CAPTURE_INGEST_URL"
MIN_SECRET_LENGTH = 32
TOKEN_TTL_SECONDS = 15 * 60
ISSUER = "mytheus-ui"  # the issuer transcription-service accepts (see the module docstring)
AUDIENCE = "transcription-service"
SOURCE: Final = "in_person"
DEFAULT_TITLE = "In-person meeting"
MAX_TITLE = 120


def _config() -> tuple[str, str] | None:
    secret = os.environ.get(SECRET_ENV) or ""
    ingest = (os.environ.get(INGEST_ENV) or "").strip()
    if len(secret) < MIN_SECRET_LENGTH or not ingest:
        return None
    try:
        url = urlparse(ingest)
    except ValueError:
        return None
    local = url.hostname in ("localhost", "127.0.0.1")
    if not (url.scheme == "wss" or (url.scheme == "ws" and local)) or not url.hostname:
        return None
    return secret, ingest


def _not_configured() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "In-person recording is not connected on this server",
        extra={"reason": "capture_not_configured"},
    )


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def sign_capture_token(
    *,
    user_id: uuid.UUID,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    consent_at: str,
    secret: str,
    now: int | None = None,
) -> tuple[str, int]:
    """(token, exp): byte-for-byte the shape mytheus-ui's `signCaptureToken` writes."""
    iat = int(time.time()) if now is None else now
    exp = iat + TOKEN_TTL_SECONDS
    claims = {
        "typ": "capture",
        "iss": ISSUER,
        "aud": AUDIENCE,
        "sub": str(user_id),
        "org": str(org_id),
        "mid": str(meeting_id),
        "src": SOURCE,
        "consent_at": consent_at,
        "iat": iat,
        "exp": exp,
    }
    header = _b64url(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    body = _b64url(json.dumps(claims, separators=(",", ":")).encode())
    sig = hmac.new(secret.encode(), f"{header}.{body}".encode("ascii"), hashlib.sha256).digest()
    return f"{header}.{body}.{_b64url(sig)}", exp


def _consent_at(raw: datetime | None) -> str | None:
    """The confirmation time: not in the future, not older than a day. ISO with `Z`."""
    now = datetime.now(UTC)
    at = raw or now
    if at.tzinfo is None:
        at = at.replace(tzinfo=UTC)
    if at > now + timedelta(seconds=60) or at < now - timedelta(days=1):
        return None
    return at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class CaptureStatus(BaseModel):
    configured: bool = Field(description="False until the server has a capture secret and URL.")


class CaptureStart(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, max_length=500)
    consent: bool = Field(
        description="The caller confirms everyone in the room knows it is being recorded."
    )
    consent_at: datetime | None = Field(
        default=None, description="When they confirmed; now when omitted. At most a day old."
    )


class CaptureRefresh(BaseModel):
    model_config = ConfigDict(extra="forbid")

    consent_at: datetime


class CaptureSession(BaseModel):
    meeting_id: uuid.UUID
    title: str
    source: Literal["in_person"] = "in_person"
    capture_token: str
    expires_at: int = Field(description="Unix seconds; mint a fresh token after this.")
    ingest_url: str = Field(description="transcription-service's `/capture/audio` WebSocket.")
    consent_at: str
    sample_rate: int = 16000
    encoding: Literal["pcm_s16le"] = "pcm_s16le"


class CaptureEnded(BaseModel):
    meeting_id: uuid.UUID
    status: Literal["ended"] = "ended"


def _clean_title(raw: str | None) -> str:
    if not raw:
        return DEFAULT_TITLE
    visible = "".join(" " if (ord(ch) < 0x20 or 0x7F <= ord(ch) <= 0x9F) else ch for ch in raw)
    cleaned = " ".join(visible.split())[:MAX_TITLE].strip()
    return cleaned or DEFAULT_TITLE


async def _own_capture(
    session: AsyncSession, ctx: RequestContext, meeting_id: uuid.UUID
) -> dict[str, Any]:
    """The caller's in-person meeting, or 404 (missing, another tenant's, not in person, or
    not one they are in)."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT m.id, m.title, m.status, m.source, m.created_at, m.scheduled_start "
                    "FROM meetings m JOIN meeting_participants p ON p.meeting_id = m.id "
                    "WHERE m.id = :m AND p.user_id = :u AND m.deleted_at IS NULL"
                ),
                {"m": str(meeting_id), "u": str(ctx.user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None or row["source"] != SOURCE:
        raise not_found()
    return dict(row)


@router.get("/status", response_model=CaptureStatus)
async def capture_status(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
) -> CaptureStatus:
    """authz: member. Whether in-person recording is connected on this server."""
    return CaptureStatus(configured=_config() is not None)


@router.post("/sessions", response_model=CaptureSession, status_code=status.HTTP_201_CREATED)
async def start_capture(
    body: CaptureStart,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CaptureSession:
    """authz: member. Start recording an in-person meeting on the caller's phone."""
    config = _config()
    if config is None:
        raise _not_configured()
    if ctx.user_id is None:
        raise not_found()
    if body.consent is not True:
        raise ProblemDetail(
            422,
            ErrorCode.CONSENT_REQUIRED,
            "Confirm that everyone in the room knows it is being recorded",
        )
    consent_at = _consent_at(body.consent_at)
    if consent_at is None:
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "consent_at must be within the last day"
        )
    title = _clean_title(body.title)
    created = await create_meeting(
        MeetingCreate(title=title, source=SOURCE), response, ctx, session
    )
    await update_meeting(created.id, MeetingUpdate(status="live"), ctx, session)
    await audit(
        session,
        org_id=ctx.org_id,
        actor_id=ctx.user_id,
        action=ACTION_CAPTURE_STARTED,
        target_type="meeting",
        target_id=created.id,
        metadata={"source": SOURCE, "consent_at": consent_at},
        ip=ctx.ip,
    )
    token, exp = sign_capture_token(
        user_id=ctx.user_id,
        org_id=ctx.org_id,
        meeting_id=created.id,
        consent_at=consent_at,
        secret=config[0],
    )
    response.headers["Cache-Control"] = "no-store"
    return CaptureSession(
        meeting_id=created.id,
        title=title,
        capture_token=token,
        expires_at=exp,
        ingest_url=config[1],
        consent_at=consent_at,
    )


@router.post("/sessions/{meeting_id}/refresh", response_model=CaptureSession)
async def refresh_capture(
    meeting_id: uuid.UUID,
    body: CaptureRefresh,
    response: Response,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CaptureSession:
    """authz: member (a participant of the meeting). A fresh token for a running capture."""
    config = _config()
    if config is None:
        raise _not_configured()
    if ctx.user_id is None:
        raise not_found()
    row = await _own_capture(session, ctx, meeting_id)
    if row["status"] in ("ended", "cancelled"):
        raise ProblemDetail(409, ErrorCode.VERSION_CONFLICT, "This recording has ended")
    consent_at = _consent_at(body.consent_at)
    if consent_at is None:
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "consent_at must be within the last day"
        )
    token, exp = sign_capture_token(
        user_id=ctx.user_id,
        org_id=ctx.org_id,
        meeting_id=meeting_id,
        consent_at=consent_at,
        secret=config[0],
    )
    response.headers["Cache-Control"] = "no-store"
    return CaptureSession(
        meeting_id=meeting_id,
        title=row["title"] or DEFAULT_TITLE,
        capture_token=token,
        expires_at=exp,
        ingest_url=config[1],
        consent_at=consent_at,
    )


@router.post("/sessions/{meeting_id}/end", response_model=CaptureEnded)
async def end_capture(
    meeting_id: uuid.UUID,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> CaptureEnded:
    """authz: member (a participant of the meeting). End the recording and the meeting, with
    its real start and end. Idempotent: an ended meeting answers 200 unchanged."""
    if ctx.user_id is None:
        raise not_found()
    row = await _own_capture(session, ctx, meeting_id)
    if row["status"] not in ("ended", "cancelled"):
        start = row["scheduled_start"] or row["created_at"]
        end = max(datetime.now(UTC), start + timedelta(seconds=1))
        await update_meeting(
            meeting_id,
            MeetingUpdate(status="ended", scheduled_start=start, scheduled_end=end),
            ctx,
            session,
        )
    return CaptureEnded(meeting_id=meeting_id)
