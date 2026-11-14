"""Every Myte's voice: `/api/v1/voice/*` (core-api).

Mytheus speaks with Kokoro-82M, self-hosted on the VPS (`briefing_audio`, `TTS_PROVIDER`,
default `kokoro`). These routes are what the web app and the phone use whenever a Myte talks:
Ask Myte's read aloud and voice mode, and the briefing player.

- `GET /api/v1/voice/voices`: whether server voice is on, the voices a person can choose
  (curated Kokoro voices with a short description, accent and gender), the one they chose and
  the one their Myte speaks with now (their choice, else their Myte's personality default).
- `PUT /api/v1/voice/preference` `{voice}`: choose the Myte's voice (`null` returns to the
  personality default). Stored as `briefing_preferences.server_voice`, so briefings use it too.
- `POST /api/v1/voice/speech` `{text, voice?, speed?, language?}`: streams MP3 (`audio/mpeg`).
  Text is 1 to 1500 characters (the apps send a sentence or two at a time, so the first sound
  arrives quickly). 30 requests per minute per person (`VOICE_SPEECH_PER_MINUTE`), counted in
  Redis at `nexus:{org_id}:ratelimit:voice:{user_id}:{minute}`. Server voice off or Kokoro
  unreachable: 503 `voice_not_connected` before any audio is sent.
- `POST /api/v1/voice/transcribe`: the raw recording (`audio/webm`, `audio/ogg`, `audio/mp4`,
  `audio/mpeg` or `audio/wav`, at most 10 MB) -> `{text}`. Talk mode's ears, so it works in
  every browser. Self-hosted faster-whisper (`whisper-stt`, speaches, `WHISPER_URL`, default
  `http://whisper-stt:8000`, model `WHISPER_MODEL`, default
  `Systran/faster-distil-whisper-small.en`, downloaded on first use). 30 per minute per person;
  503 `listening_not_connected` when the service is off.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from collections.abc import AsyncIterator
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.authz import RequestContext, not_found, require_tier, tenant_session
from libs.shared.cache import get_redis, redis_key
from libs.shared.errors import ErrorCode, ProblemDetail
from services.core_api import briefing_audio, briefings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["voice"])

PER_MINUTE_ENV = "VOICE_SPEECH_PER_MINUTE"
DEFAULT_PER_MINUTE = 30


class VoiceOption(BaseModel):
    id: str
    name: str
    description: str
    accent: str
    gender: str


class VoicesView(BaseModel):
    connected: bool = Field(description="Whether Mytheus speaks with its own server voice.")
    voices: list[VoiceOption]
    chosen: str | None = Field(description="The voice the person picked, if any.")
    selected: str | None = Field(
        description="The voice their Myte speaks with now: their choice, else its personality."
    )


class VoicePreference(BaseModel):
    voice: str | None = Field(default=None, max_length=100)


class SpeechRequest(BaseModel):
    text: str = Field(min_length=1, max_length=1500)
    voice: str | None = Field(default=None, max_length=100)
    speed: float = Field(default=1.0, ge=0.7, le=1.4)
    language: str = Field(default="en", min_length=2, max_length=8)


def _me(ctx: RequestContext) -> uuid.UUID:
    if ctx.user_id is None:
        raise not_found()
    return ctx.user_id


async def _personality(session: AsyncSession, user_id: uuid.UUID) -> str:
    row = (
        await session.execute(
            text("SELECT agent_personality FROM users WHERE id = :u"), {"u": str(user_id)}
        )
    ).first()
    return str(row[0]) if row and row[0] else "balanced"


def _options() -> list[VoiceOption]:
    return [
        VoiceOption(
            id=v.id, name=v.name, description=v.description, accent=v.accent, gender=v.gender
        )
        for v in briefing_audio.server_voices()
    ]


async def _view(session: AsyncSession, user_id: uuid.UUID) -> VoicesView:
    prefs = await briefings.preferences(session, user_id)
    connected = briefing_audio.speaker() is not None
    selected = (
        briefing_audio.voice_for(prefs.server_voice, await _personality(session, user_id))
        if briefing_audio.server_voices()
        else None
    )
    return VoicesView(
        connected=connected, voices=_options(), chosen=prefs.server_voice, selected=selected
    )


@router.get("/api/v1/voice/voices", response_model=VoicesView)
async def list_voices(
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> VoicesView:
    return await _view(session, _me(ctx))


@router.put("/api/v1/voice/preference", response_model=VoicesView)
async def choose_voice(
    body: VoicePreference,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> VoicesView:
    me = _me(ctx)
    if body.voice is not None and not any(v.id == body.voice for v in _options()):
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Unknown voice")
    prefs = await briefings.preferences(session, me)
    await briefings.save_preferences(
        session, ctx.org_id, me, prefs.model_copy(update={"server_voice": body.voice})
    )
    return await _view(session, me)


def _per_minute() -> int:
    try:
        return max(1, int(os.environ.get(PER_MINUTE_ENV, DEFAULT_PER_MINUTE)))
    except ValueError:
        return DEFAULT_PER_MINUTE


async def _take(org_id: uuid.UUID, user_id: uuid.UUID, kind: str = "voice") -> bool:
    """A fixed one-minute window per person. Redis down: allowed (the voice is self-hosted, so
    an outage of the counter costs CPU time, never money) and logged."""
    key = redis_key(org_id, "ratelimit", kind, user_id, int(time.time() // 60))
    try:
        redis = get_redis()
        used = int(await redis.incr(key))
        if used == 1:
            await redis.expire(key, 120)
    except Exception:  # noqa: BLE001
        logger.warning("voice: rate-limit store unavailable; allowing", exc_info=True)
        return True
    return used <= _per_minute()


def _not_connected() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Your Myte's voice is not available right now",
        extra={"reason": "voice_not_connected"},
    )


@router.post(
    "/api/v1/voice/speech",
    response_class=StreamingResponse,
    responses={200: {"content": {"audio/mpeg": {}}, "description": "MP3, streamed."}},
)
async def speak(
    body: SpeechRequest,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    session: Annotated[AsyncSession, Depends(tenant_session)],
) -> StreamingResponse:
    me = _me(ctx)
    text_in = " ".join(body.text.split())
    if not text_in:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "Nothing to say")
    speaker = briefing_audio.speaker()
    if speaker is None:
        raise _not_connected()
    if not await _take(ctx.org_id, me):
        raise ProblemDetail(429, ErrorCode.RATE_LIMITED, "Too much speech at once; slow down")
    if body.voice is not None and any(v.id == body.voice for v in _options()):
        voice = body.voice
    else:
        prefs = await briefings.preferences(session, me)
        voice = briefing_audio.voice_for(
            prefs.server_voice, await _personality(session, me), body.language
        )
    if body.language.split("-")[0].lower() != "en":
        voice = briefing_audio.voice_for(None, None, body.language)
    try:
        stream = speaker.stream(text_in, voice=voice, speed=body.speed)
        first = await stream.__anext__()
    except (briefing_audio.TtsError, StopAsyncIteration):
        logger.warning("voice: speech failed", exc_info=True)
        raise _not_connected() from None

    async def body_iter() -> AsyncIterator[bytes]:
        yield first
        try:
            async for chunk in stream:
                yield chunk
        except briefing_audio.TtsError:
            logger.warning("voice: speech stopped mid-stream", exc_info=True)

    return StreamingResponse(
        body_iter(),
        media_type=briefing_audio.MIME,
        headers={"Cache-Control": "no-store", "X-Myte-Voice": voice},
    )


# --- ears: speech to text for Talk mode ------------------------------------------------------

WHISPER_URL_ENV = "WHISPER_URL"
WHISPER_URL = "http://whisper-stt:8000"
WHISPER_MODEL_ENV = "WHISPER_MODEL"
WHISPER_MODEL = "Systran/faster-distil-whisper-small.en"
MAX_AUDIO_BYTES = 10 * 1024 * 1024
AUDIO_TYPES = ("audio/webm", "audio/ogg", "audio/mp4", "audio/mpeg", "audio/wav", "audio/x-wav")
_model_ready: set[str] = set()


class Transcript(BaseModel):
    text: str


def _whisper() -> tuple[str, str]:
    return (
        (os.environ.get(WHISPER_URL_ENV) or WHISPER_URL).rstrip("/"),
        os.environ.get(WHISPER_MODEL_ENV) or WHISPER_MODEL,
    )


def _not_listening() -> ProblemDetail:
    return ProblemDetail(
        503,
        ErrorCode.DEPENDENCY_UNAVAILABLE,
        "Your Myte can't hear right now",
        extra={"reason": "listening_not_connected"},
    )


async def _ensure_model(client: httpx.AsyncClient, base: str, model: str) -> None:
    """speaches serves only downloaded models; the first call downloads it into its volume."""
    if model in _model_ready:
        return
    res = await client.post(f"{base}/v1/models/{model}", timeout=httpx.Timeout(600.0, connect=5.0))
    if res.status_code not in (200, 201):
        raise httpx.HTTPError(f"model download answered {res.status_code}")
    _model_ready.add(model)


async def transcribe_audio(data: bytes, content_type: str, language: str | None) -> str:
    base, model = _whisper()
    ext = {"audio/webm": "webm", "audio/ogg": "ogg", "audio/mp4": "m4a", "audio/mpeg": "mp3"}.get(
        content_type, "wav"
    )
    form = {"model": model, "response_format": "json"}
    if language:
        form["language"] = language
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
        await _ensure_model(client, base, model)
        res = await client.post(
            f"{base}/v1/audio/transcriptions",
            data=form,
            files={"file": (f"speech.{ext}", data, content_type)},
        )
        if res.status_code == 404:
            _model_ready.discard(model)
            await _ensure_model(client, base, model)
            res = await client.post(
                f"{base}/v1/audio/transcriptions",
                data=form,
                files={"file": (f"speech.{ext}", data, content_type)},
            )
    if res.status_code != 200:
        raise httpx.HTTPError(f"transcription answered {res.status_code}")
    return str(res.json().get("text") or "").strip()


@router.post("/api/v1/voice/transcribe", response_model=Transcript)
async def transcribe(
    request: Request,
    ctx: Annotated[RequestContext, Depends(require_tier("member"))],
    language: str | None = None,
) -> Transcript:
    me = _me(ctx)
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if content_type not in AUDIO_TYPES:
        raise ProblemDetail(
            422, ErrorCode.VALIDATION_FAILED, "Send audio (webm, ogg, mp4, mp3, wav)"
        )
    data = await request.body()
    if not data:
        raise ProblemDetail(422, ErrorCode.VALIDATION_FAILED, "The recording is empty")
    if len(data) > MAX_AUDIO_BYTES:
        raise ProblemDetail(413, ErrorCode.VALIDATION_FAILED, "The recording is too long")
    if not await _take(ctx.org_id, me, "voice-stt"):
        raise ProblemDetail(429, ErrorCode.RATE_LIMITED, "Too many recordings at once; slow down")
    lang = (language or "").split("-")[0].lower() or None
    try:
        text_out = await transcribe_audio(
            data, content_type, lang if lang and len(lang) == 2 else None
        )
    except httpx.HTTPError:
        logger.warning("voice: transcription failed", exc_info=True)
        raise _not_listening() from None
    return Transcript(text=text_out)
