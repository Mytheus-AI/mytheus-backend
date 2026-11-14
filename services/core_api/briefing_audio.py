"""The server-side voice of every Myte: a text-to-speech provider and the briefing audio's store.

The provider is Kokoro by default (`TTS_PROVIDER`, default `kokoro`): Kokoro-82M, open-weight,
self-hosted on the VPS as the `kokoro-tts` service (Kokoro-FastAPI, OpenAI-style
`POST /v1/audio/speech`, `KOKORO_URL`, default `http://kokoro-tts:8880`). Nothing leaves the
server. `TTS_PROVIDER=elevenlabs` with `ELEVENLABS_API_KEY` keeps the hosted option;
`TTS_PROVIDER=none` turns server voice off (the apps then show text only).

Two uses:
- Live speech (`speak`, streamed MP3): Ask Myte's read aloud and voice mode, and the briefing
  player, through core-api `POST /api/v1/voice/speech`. Needs only the provider.
- Briefing files (`backend()`): rendered right after the script lands and stored privately in
  object storage, the same `S3_*` variables meeting-service and billing use, bucket
  `BRIEFING_AUDIO_BUCKET` (default `mytheus-briefings`), keys tenant first:
  `{org_id}/briefings/{user_id}/{briefing_id}.mp3`; served by a short-lived presigned GET.

The person's voice (`briefing_preferences.server_voice`) is their Myte's voice everywhere; with
none chosen the Myte's personality picks one. `set_backend` / `set_speaker` swap in fakes for
tests; no test calls a real provider.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import httpx

logger = logging.getLogger(__name__)

PROVIDER_ENV = "TTS_PROVIDER"
DEFAULT_PROVIDER = "kokoro"
KOKORO_URL_ENV = "KOKORO_URL"
KOKORO_URL = "http://kokoro-tts:8880"
ELEVENLABS_KEY_ENV = "ELEVENLABS_API_KEY"
ELEVENLABS_MODEL_ENV = "ELEVENLABS_MODEL_ID"
ELEVENLABS_URL_ENV = "ELEVENLABS_API_URL"
BUCKET_ENV = "BRIEFING_AUDIO_BUCKET"
DEFAULT_BUCKET = "mytheus-briefings"
DEFAULT_MODEL = "eleven_multilingual_v2"
ELEVENLABS_URL = "https://api.elevenlabs.io"
MIME = "audio/mpeg"
URL_TTL_S = 600
MAX_CHARS = 5000  # a standard briefing is ~1,400 characters; the provider's per-call cap is 5k


@dataclass(frozen=True)
class ServerVoice:
    id: str
    name: str
    description: str
    accent: str = ""
    gender: str = ""


# Kokoro-82M's best-graded English voices (its own quality grades, A to B-), curated so every
# Myte sounds natural. The personality picks the default; anyone can choose another.
KOKORO_VOICES: tuple[ServerVoice, ...] = (
    ServerVoice("af_heart", "Heart", "Warm and natural", "American", "female"),
    ServerVoice("af_bella", "Bella", "Bright and expressive", "American", "female"),
    ServerVoice("af_kore", "Kore", "Clear and composed", "American", "female"),
    ServerVoice("af_aoede", "Aoede", "Calm and gentle", "American", "female"),
    ServerVoice("am_michael", "Michael", "Steady and confident", "American", "male"),
    ServerVoice("am_fenrir", "Fenrir", "Deep and grounded", "American", "male"),
    ServerVoice("am_puck", "Puck", "Lively and playful", "American", "male"),
    ServerVoice("bf_emma", "Emma", "Polished and articulate", "British", "female"),
    ServerVoice("bm_george", "George", "Warm and measured", "British", "male"),
)
_KOKORO_BY_PERSONALITY = {
    "balanced": "af_heart",
    "friendly": "af_bella",
    "concise": "af_kore",
    "direct": "am_michael",
    "detailed": "bf_emma",
}
# Kokoro voices are per language; a briefing in another language uses that language's voice.
_KOKORO_BY_LANGUAGE = {
    "es": "ef_dora",
    "fr": "ff_siwis",
    "hi": "hf_alpha",
    "it": "if_sara",
    "ja": "jf_alpha",
    "pt": "pf_dora",
    "zh": "zf_xiaobei",
}

# ElevenLabs premade voices (available on every account), when TTS_PROVIDER=elevenlabs.
ELEVENLABS_VOICES: tuple[ServerVoice, ...] = (
    ServerVoice("21m00Tcm4TlvDq8ikWAM", "Rachel", "Calm and clear"),
    ServerVoice("pNInz6obpgDQGcFmaJgB", "Adam", "Deep and steady"),
    ServerVoice("EXAVITQu4vr4xnSDxMfL", "Bella", "Soft and warm"),
    ServerVoice("ErXwobaYiN019PkySvjV", "Antoni", "Friendly and round"),
    ServerVoice("TxGEqnHWrfWFTfGW9XjX", "Josh", "Brisk and direct"),
    ServerVoice("MF3mGyEqCkNgVwKbdHrq", "Elli", "Bright and quick"),
)
_ELEVENLABS_BY_PERSONALITY = {
    "balanced": "21m00Tcm4TlvDq8ikWAM",
    "concise": "TxGEqnHWrfWFTfGW9XjX",
    "detailed": "pNInz6obpgDQGcFmaJgB",
    "direct": "TxGEqnHWrfWFTfGW9XjX",
    "friendly": "ErXwobaYiN019PkySvjV",
}


def provider(env: dict[str, str] | None = None) -> str:
    """`kokoro` (default), `elevenlabs`, or `none`."""
    source = env if env is not None else os.environ
    value = (source.get(PROVIDER_ENV) or DEFAULT_PROVIDER).strip().lower()
    return value if value in ("kokoro", "elevenlabs") else "none"


def server_voices() -> tuple[ServerVoice, ...]:
    """The voices a person can choose, for the active provider."""
    p = provider()
    if p == "kokoro":
        return KOKORO_VOICES
    if p == "elevenlabs":
        return ELEVENLABS_VOICES
    return ()


def voice_for(preferred: str | None, personality: str | None, language: str = "en") -> str:
    """The person's chosen voice when it is one we know, else the persona's default. Kokoro
    speaks another language only with that language's own voice."""
    lang = (language or "en").split("-")[0].lower()
    if provider() == "kokoro":
        if lang != "en" and lang in _KOKORO_BY_LANGUAGE:
            return _KOKORO_BY_LANGUAGE[lang]
        if preferred and any(v.id == preferred for v in KOKORO_VOICES):
            return preferred
        return _KOKORO_BY_PERSONALITY.get(personality or "balanced", KOKORO_VOICES[0].id)
    if preferred and any(v.id == preferred for v in ELEVENLABS_VOICES):
        return preferred
    return _ELEVENLABS_BY_PERSONALITY.get(personality or "balanced", ELEVENLABS_VOICES[0].id)


def audio_key(org_id: uuid.UUID, user_id: uuid.UUID, briefing_id: uuid.UUID) -> str:
    return f"{org_id}/briefings/{user_id}/{briefing_id}.mp3"


def user_prefix(org_id: uuid.UUID, user_id: uuid.UUID) -> str:
    return f"{org_id}/briefings/{user_id}/"


class AudioBackend(Protocol):
    async def synthesize(self, text: str, *, voice: str, language: str) -> bytes: ...

    async def put(self, key: str, data: bytes, *, content_type: str) -> None: ...

    def presign(self, key: str, *, ttl_s: int) -> str: ...

    async def delete(self, key: str) -> None: ...

    async def delete_prefix(self, prefix: str) -> int: ...


class TtsError(Exception):
    """The provider refused or could not be reached; the briefing falls back to device speech."""


class ElevenLabsS3:
    """ElevenLabs for the voice, S3-compatible object storage for the file."""

    def __init__(self, env: dict[str, str]) -> None:
        import boto3
        from botocore.config import Config

        self._key = env.get(ELEVENLABS_KEY_ENV, "")
        self._model = env.get(ELEVENLABS_MODEL_ENV) or DEFAULT_MODEL
        self._url = (env.get(ELEVENLABS_URL_ENV) or ELEVENLABS_URL).rstrip("/")
        self._bucket = env.get(BUCKET_ENV) or DEFAULT_BUCKET
        path_style = (env.get("S3_FORCE_PATH_STYLE") or "").lower() in ("1", "true", "yes")
        self._s3: Any = boto3.client(
            "s3",
            endpoint_url=env.get("S3_ENDPOINT_URL") or None,
            region_name=env.get("S3_REGION") or "auto",
            aws_access_key_id=env["S3_ACCESS_KEY_ID"],
            aws_secret_access_key=env["S3_SECRET_ACCESS_KEY"],
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path" if path_style else "auto"},
                connect_timeout=5,
                read_timeout=30,
                retries={"max_attempts": 3},
            ),
        )

    async def synthesize(self, text: str, *, voice: str, language: str) -> bytes:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
                res = await client.post(
                    f"{self._url}/v1/text-to-speech/{voice}",
                    params={"output_format": "mp3_44100_128"},
                    headers={"xi-api-key": self._key, "accept": MIME},
                    json={
                        "text": text[:MAX_CHARS],
                        "model_id": self._model,
                        "language_code": language,
                    },
                )
        except httpx.HTTPError as exc:
            raise TtsError(f"tts unreachable: {type(exc).__name__}") from None
        if res.status_code != 200 or not res.content:
            raise TtsError(f"tts answered {res.status_code}")
        return res.content

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        await asyncio.to_thread(
            self._s3.put_object, Bucket=self._bucket, Key=key, Body=data, ContentType=content_type
        )

    def presign(self, key: str, *, ttl_s: int) -> str:
        return str(
            self._s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": self._bucket, "Key": key, "ResponseContentType": MIME},
                ExpiresIn=ttl_s,
            )
        )

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._s3.delete_object, Bucket=self._bucket, Key=key)

    async def delete_prefix(self, prefix: str) -> int:
        def run() -> int:
            removed = 0
            token: str | None = None
            while True:
                kw: dict[str, Any] = {"Bucket": self._bucket, "Prefix": prefix}
                if token:
                    kw["ContinuationToken"] = token
                page = self._s3.list_objects_v2(**kw)
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    self._s3.delete_objects(Bucket=self._bucket, Delete={"Objects": keys})
                    removed += len(keys)
                if not page.get("IsTruncated"):
                    return removed
                token = page.get("NextContinuationToken")

        return await asyncio.to_thread(run)


class KokoroS3(ElevenLabsS3):
    """Kokoro (self-hosted) for the voice, the same object storage for the file."""

    def __init__(self, env: dict[str, str]) -> None:
        super().__init__(env)
        self._kokoro = (env.get(KOKORO_URL_ENV) or KOKORO_URL).rstrip("/")

    async def synthesize(self, text: str, *, voice: str, language: str) -> bytes:
        """A whole briefing: short pieces rendered three at a time and joined (MP3 frames
        concatenate cleanly). One long streamed request was cut off by Kokoro mid-body
        (`RemoteProtocolError`), and one piece at a time took as long as the briefing itself."""
        pieces = split_for_speech(speakable(text[:MAX_CHARS]), PIECE_CHARS)
        if not pieces:
            raise TtsError("nothing to say")
        gate = asyncio.Semaphore(RENDER_CONCURRENCY)

        async def one(piece: str) -> bytes:
            async with gate:
                return await kokoro_speech(self._kokoro, piece, voice=voice)

        parts = await asyncio.gather(*(one(p) for p in pieces))
        data = b"".join(parts)
        if not data:
            raise TtsError("tts answered no audio")
        return data


_MONTHS = {
    "Jan": "January",
    "Feb": "February",
    "Mar": "March",
    "Apr": "April",
    "Jun": "June",
    "Jul": "July",
    "Aug": "August",
    "Sep": "September",
    "Sept": "September",
    "Oct": "October",
    "Nov": "November",
    "Dec": "December",
}
_DAYS = {
    "Mon": "Monday",
    "Tue": "Tuesday",
    "Tues": "Tuesday",
    "Wed": "Wednesday",
    "Thu": "Thursday",
    "Thur": "Thursday",
    "Thurs": "Thursday",
    "Fri": "Friday",
    "Sat": "Saturday",
    "Sun": "Sunday",
}
_MONTH_RE = re.compile(r"\b(" + "|".join(sorted(_MONTHS, key=len, reverse=True)) + r")\.?(?=\s+\d)")
_DAY_RE = re.compile(
    r"\b("
    + "|".join(sorted(_DAYS, key=len, reverse=True))
    + r")\.?(?=,?\s+(?:\d|"
    + "|".join(list(_MONTHS.values()) + list(_MONTHS))
    + r"))"
)
_AMPM = r"([AaPp])(?:\.\s?[Mm]\.|[Mm]\b)"
_ON_THE_HOUR = re.compile(r"\b(\d{1,2}):00\s*" + _AMPM)
_CLOCK_AMPM = re.compile(r"\b(\d{1,2}(?::\d{2})?)\s*" + _AMPM)
_CITATION = re.compile(r"\s*\[\d+\](?:\[\d+\])*")
_MARKDOWN = re.compile(r"[*_`#>]+")


def speakable(text: str) -> str:
    """Text as it should be read aloud: abbreviated months and weekdays next to a date are
    spelled out ("Nov 13" -> "November 13", "Fri, Nov" -> "Friday, November"), times lose
    ":00" and say "PM", citations and markdown go, and a few written shorthands are expanded.
    Kokoro reads what it is given, so "Nov" was spoken as "Nov"."""
    out = _CITATION.sub("", text)
    out = _MARKDOWN.sub("", out)
    out = _MONTH_RE.sub(lambda m: _MONTHS[m.group(1)], out)
    out = _DAY_RE.sub(lambda m: _DAYS[m.group(1)], out)
    out = _ON_THE_HOUR.sub(lambda m: f"{m.group(1)} {m.group(2).upper()}M", out)
    out = _CLOCK_AMPM.sub(lambda m: f"{m.group(1)} {m.group(2).upper()}M", out)
    for written, said in (
        ("e.g.", "for example"),
        ("i.e.", "that is"),
        (" vs. ", " versus "),
        (" vs ", " versus "),
        (" & ", " and "),
        ("UTC", "U T C"),
    ):
        out = out.replace(written, said)
    return " ".join(out.split())


RENDER_CONCURRENCY = 3
PIECE_CHARS = 320
_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9\"'(])")


def split_for_speech(text: str, limit: int | None = None) -> list[str]:
    """Sentences grouped into pieces of at most `limit` characters (a sentence longer than the
    limit is split at commas, then at spaces)."""
    limit = limit or PIECE_CHARS
    sentences = [s.strip() for s in _SENTENCE.split(" ".join(text.split())) if s.strip()]
    out: list[str] = []
    current = ""
    for sentence in sentences:
        while len(sentence) > limit:
            cut = sentence.rfind(", ", 0, limit)
            cut = cut + 1 if cut > limit // 3 else sentence.rfind(" ", 0, limit)
            cut = cut if cut > 0 else limit
            head, sentence = sentence[:cut].strip(), sentence[cut:].strip()
            if current:
                out.append(current)
                current = ""
            out.append(head)
        if current and len(current) + 1 + len(sentence) > limit:
            out.append(current)
            current = sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        out.append(current)
    return out


async def kokoro_speech(base_url: str, text: str, *, voice: str, speed: float = 1.0) -> bytes:
    """One short piece of MP3 from Kokoro-FastAPI (`POST /v1/audio/speech`, not streamed)."""
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=5.0)) as client:
            res = await client.post(
                f"{base_url}/v1/audio/speech",
                json={
                    "model": "kokoro",
                    "input": text,
                    "voice": voice,
                    "response_format": "mp3",
                    "speed": speed,
                    "stream": False,
                },
            )
    except httpx.HTTPError as exc:
        raise TtsError(f"tts unreachable: {type(exc).__name__}") from None
    if res.status_code != 200 or not res.content:
        raise TtsError(f"tts answered {res.status_code}")
    return res.content


async def kokoro_stream(
    base_url: str, text: str, *, voice: str, speed: float = 1.0
) -> AsyncIterator[bytes]:
    """Live speech: the apps send a sentence or two at a time, so each piece is fetched whole
    (Kokoro's chunked replies were cut off mid-body) and yielded as one chunk."""
    for piece in split_for_speech(speakable(text)):
        yield await kokoro_speech(base_url, piece, voice=voice, speed=speed)


class Speaker(Protocol):
    def stream(self, text: str, *, voice: str, speed: float) -> AsyncIterator[bytes]: ...


class KokoroSpeaker:
    """Live speech (read aloud, voice mode, the briefing player)."""

    def __init__(self, base_url: str) -> None:
        self._url = base_url.rstrip("/")

    def stream(self, text: str, *, voice: str, speed: float) -> AsyncIterator[bytes]:
        return kokoro_stream(self._url, text, voice=voice, speed=speed)


@dataclass
class FakeSpeaker:
    spoken: list[tuple[str, str, float]] = field(default_factory=list)
    fail: bool = False

    async def _gen(self, text: str) -> AsyncIterator[bytes]:
        yield b"ID3fake-mp3:"
        yield text[:20].encode()

    def stream(self, text: str, *, voice: str, speed: float) -> AsyncIterator[bytes]:
        if self.fail:
            raise TtsError("fake failure")
        self.spoken.append((text, voice, speed))
        return self._gen(text)


_speaker_override: list[Speaker | None] = []


def set_speaker(speaker: Speaker | None) -> None:
    _speaker_override.clear()
    _speaker_override.append(speaker)


def reset_speaker() -> None:
    _speaker_override.clear()


def speaker() -> Speaker | None:
    """Live speech, or None when server voice is off. Only Kokoro streams live; ElevenLabs is
    kept for briefing files."""
    if _speaker_override:
        return _speaker_override[0]
    if provider() != "kokoro":
        return None
    return KokoroSpeaker(os.environ.get(KOKORO_URL_ENV) or KOKORO_URL)


@dataclass
class FakeAudio:
    """In-memory stand-in for tests: records what was spoken and stored."""

    objects: dict[str, bytes] = field(default_factory=dict)
    spoken: list[tuple[str, str, str]] = field(default_factory=list)
    fail: bool = False

    async def synthesize(self, text: str, *, voice: str, language: str) -> bytes:
        if self.fail:
            raise TtsError("fake failure")
        self.spoken.append((text, voice, language))
        return b"ID3fake-mp3:" + text[:20].encode()

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        self.objects[key] = data

    def presign(self, key: str, *, ttl_s: int) -> str:
        return f"https://storage.test/{key}?ttl={ttl_s}"

    async def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    async def delete_prefix(self, prefix: str) -> int:
        doomed = [k for k in self.objects if k.startswith(prefix)]
        for k in doomed:
            del self.objects[k]
        return len(doomed)


_override: list[AudioBackend | None] = []


def set_backend(backend: AudioBackend | None) -> None:
    """Tests: force a backend (or `None`, "not connected"). `reset_backend` returns to env."""
    _override.clear()
    _override.append(backend)


def reset_backend() -> None:
    _override.clear()


def configured_from_env(env: dict[str, str] | None = None) -> bool:
    """Briefing files need a provider and object storage."""
    source = env if env is not None else dict(os.environ)
    p = provider(source)
    if p == "none":
        return False
    if p == "elevenlabs" and not source.get(ELEVENLABS_KEY_ENV, "").strip():
        return False
    return bool(source.get("S3_ACCESS_KEY_ID", "").strip()) and bool(
        source.get("S3_SECRET_ACCESS_KEY", "").strip()
    )


_cached: list[AudioBackend] = []


def backend() -> AudioBackend | None:
    """The configured backend, or None ("not connected": the apps play live speech instead)."""
    if _override:
        return _override[0]
    if not configured_from_env():
        return None
    if not _cached:
        env = dict(os.environ)
        _cached.append(KokoroS3(env) if provider(env) == "kokoro" else ElevenLabsS3(env))
    return _cached[0]


def connected() -> bool:
    return backend() is not None


def presigned(key: str) -> tuple[str, datetime] | None:
    found = backend()
    if found is None:
        return None
    return found.presign(key, ttl_s=URL_TTL_S), datetime.now(UTC) + timedelta(seconds=URL_TTL_S)
