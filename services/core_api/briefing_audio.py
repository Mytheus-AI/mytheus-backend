"""The server-side voice of every Myte: a text-to-speech provider and the briefing audio's store.

The provider is Deepgram Aura-2 by default (`TTS_PROVIDER`, default `deepgram`, key
`DEEPGRAM_API_KEY`, base `DEEPGRAM_API_URL`, default `https://api.deepgram.com`): REST
`POST /v1/speak`, MP3 streamed back as it renders (first audio in about 0.35 s, where the
self-hosted Kokoro on CPU took seconds). Aura-2 speaks English, Spanish, French, German,
Italian, Dutch and Japanese; another language has no server voice and is shown as text. At most
2,000 characters per request, so longer text goes in pieces. `speed` works on the English and
Spanish voices only (the others answer 400), so it is sent for those alone.

Without the key there is no server voice (the apps show text only). `TTS_PROVIDER=kokoro` keeps the
self-hosted Kokoro-82M (`KOKORO_URL`, default `http://kokoro-tts:8880`), which needs its
`kokoro-tts` service running again; `TTS_PROVIDER=elevenlabs` with `ELEVENLABS_API_KEY` is the
other hosted option for briefing files; `TTS_PROVIDER=none` turns server voice off.

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
import json
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
DEFAULT_PROVIDER = "deepgram"
PROVIDERS = ("deepgram", "kokoro", "elevenlabs")
DEEPGRAM_KEY_ENV = "DEEPGRAM_API_KEY"
DEEPGRAM_URL_ENV = "DEEPGRAM_API_URL"
DEEPGRAM_URL = "https://api.deepgram.com"
DEEPGRAM_PIECE_CHARS = 1900  # Aura-2 refuses more than 2,000 characters in one request (413)
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


# Deepgram Aura-2 English voices, curated so every Myte sounds natural; one for each Kokoro voice
# Mytheus offered before, with the same spread of accents, genders and tone. The personality
# picks the default; anyone can choose another.
DEEPGRAM_VOICES: tuple[ServerVoice, ...] = (
    ServerVoice("aura-2-thalia-en", "Thalia", "Clear and confident", "American", "female"),
    ServerVoice("aura-2-helena-en", "Helena", "Warm and friendly", "American", "female"),
    ServerVoice("aura-2-callista-en", "Callista", "Crisp and professional", "American", "female"),
    ServerVoice("aura-2-harmonia-en", "Harmonia", "Calm and gentle", "American", "female"),
    ServerVoice("aura-2-orpheus-en", "Orpheus", "Steady and confident", "American", "male"),
    ServerVoice("aura-2-zeus-en", "Zeus", "Deep and grounded", "American", "male"),
    ServerVoice("aura-2-apollo-en", "Apollo", "Relaxed and easygoing", "American", "male"),
    ServerVoice("aura-2-pandora-en", "Pandora", "Smooth and articulate", "British", "female"),
    ServerVoice("aura-2-draco-en", "Draco", "Warm and measured", "British", "male"),
)
_DEEPGRAM_BY_PERSONALITY = {
    "balanced": "aura-2-thalia-en",
    "friendly": "aura-2-helena-en",
    "concise": "aura-2-callista-en",
    "direct": "aura-2-orpheus-en",
    "detailed": "aura-2-pandora-en",
}
# Aura-2 voices are per language; a briefing in another language uses that language's voice.
_DEEPGRAM_BY_LANGUAGE = {
    "es": "aura-2-selena-es",
    "fr": "aura-2-agathe-fr",
    "de": "aura-2-elara-de",
    "it": "aura-2-livia-it",
    "nl": "aura-2-daphne-nl",
    "ja": "aura-2-izanami-ja",
}
# A voice someone chose while Mytheus spoke with Kokoro (still stored as the Kokoro id in
# `briefing_preferences.server_voice`) carries over to its Deepgram counterpart.
_DEEPGRAM_FOR_KOKORO = {
    "af_heart": "aura-2-thalia-en",
    "af_bella": "aura-2-helena-en",
    "af_kore": "aura-2-callista-en",
    "af_aoede": "aura-2-harmonia-en",
    "am_michael": "aura-2-orpheus-en",
    "am_fenrir": "aura-2-zeus-en",
    "am_puck": "aura-2-apollo-en",
    "bf_emma": "aura-2-pandora-en",
    "bm_george": "aura-2-draco-en",
}

# Kokoro-82M's best-graded English voices (its own quality grades, A to B-), when
# TTS_PROVIDER=kokoro.
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
    """`deepgram` (default), `kokoro`, `elevenlabs`, or `none`."""
    source = env if env is not None else os.environ
    value = (source.get(PROVIDER_ENV) or DEFAULT_PROVIDER).strip().lower()
    return value if value in PROVIDERS else "none"


def server_voices() -> tuple[ServerVoice, ...]:
    """The voices a person can choose, for the active provider."""
    p = provider()
    if p == "deepgram":
        return DEEPGRAM_VOICES
    if p == "kokoro":
        return KOKORO_VOICES
    if p == "elevenlabs":
        return ELEVENLABS_VOICES
    return ()


def _lang(language: str | None) -> str:
    return (language or "en").split("-")[0].lower()


def canonical_voice(voice: str | None) -> str | None:
    """A stored or sent voice id as the active provider names it: a Kokoro id (chosen before the
    move to Deepgram) becomes its Deepgram counterpart; anything else is returned unchanged."""
    if voice and provider() == "deepgram":
        return _DEEPGRAM_FOR_KOKORO.get(voice, voice)
    return voice


def known_voice(voice: str | None) -> str | None:
    """`canonical_voice` when it is one of the voices a person can choose now, else None."""
    found = canonical_voice(voice)
    return found if found and any(v.id == found for v in server_voices()) else None


def speaks(language: str | None) -> bool:
    """Whether the active provider has a voice for this language. Aura-2 has none for Hindi,
    Portuguese, Korean, Chinese or Arabic; those are shown as text."""
    if provider() != "deepgram":
        return True
    lang = _lang(language)
    return lang == "en" or lang in _DEEPGRAM_BY_LANGUAGE


def voice_for(preferred: str | None, personality: str | None, language: str = "en") -> str:
    """The person's chosen voice when it is one we know, else the persona's default. Deepgram
    and Kokoro speak another language only with that language's own voice."""
    lang = _lang(language)
    p = provider()
    if p == "deepgram":
        if lang != "en" and lang in _DEEPGRAM_BY_LANGUAGE:
            return _DEEPGRAM_BY_LANGUAGE[lang]
        return known_voice(preferred) or _DEEPGRAM_BY_PERSONALITY.get(
            personality or "balanced", DEEPGRAM_VOICES[0].id
        )
    if p == "kokoro":
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


class DeepgramS3(ElevenLabsS3):
    """Deepgram Aura-2 for the voice, the same object storage for the file."""

    def __init__(self, env: dict[str, str]) -> None:
        super().__init__(env)
        self._dg_key = (env.get(DEEPGRAM_KEY_ENV) or "").strip()
        self._dg_url = (env.get(DEEPGRAM_URL_ENV) or DEEPGRAM_URL).rstrip("/")

    async def synthesize(self, text: str, *, voice: str, language: str) -> bytes:
        """A whole briefing: pieces of at most 1,900 characters rendered three at a time and
        joined in order (MP3 frames concatenate cleanly)."""
        if not speaks(language):
            raise TtsError(f"no server voice for {_lang(language)}")
        pieces = split_for_speech(speakable(text[:MAX_CHARS]), DEEPGRAM_PIECE_CHARS)
        if not pieces:
            raise TtsError("nothing to say")
        gate = asyncio.Semaphore(RENDER_CONCURRENCY)

        async def one(piece: str) -> bytes:
            async with gate:
                stream = deepgram_stream(self._dg_url, self._dg_key, piece, voice=voice)
                return b"".join([chunk async for chunk in stream])

        data = b"".join(await asyncio.gather(*(one(p) for p in pieces)))
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


def _deepgram_error(body: bytes) -> str:
    """Deepgram's `err_code` from an error body (`{"err_code", "err_msg", "request_id"}`), for
    the log. Never the request, which carries the key."""
    try:
        parsed = json.loads(body)
    except ValueError:
        return "unreadable error body"
    if isinstance(parsed, dict):
        return str(parsed.get("err_code") or parsed.get("category") or "error")[:80]
    return "error"


async def deepgram_stream(
    base_url: str,
    key: str,
    text: str,
    *,
    voice: str,
    speed: float = 1.0,
    transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[bytes]:
    """One piece (at most 2,000 characters) as MP3 from Deepgram's `POST /v1/speak`, yielded as
    it arrives. Only the English and Spanish voices take `speed`; the others answer 400 to it."""
    params = {"model": voice, "encoding": "mp3"}
    if abs(speed - 1.0) > 1e-3 and voice.endswith(("-en", "-es")):
        params["speed"] = f"{min(max(speed, 0.7), 1.5):g}"
    got = False
    try:
        async with (
            httpx.AsyncClient(
                timeout=httpx.Timeout(60.0, connect=5.0), transport=transport
            ) as client,
            client.stream(
                "POST",
                f"{base_url.rstrip('/')}/v1/speak",
                params=params,
                headers={"Authorization": f"Token {key}", "Content-Type": "application/json"},
                json={"text": text},
            ) as res,
        ):
            if res.status_code != 200:
                raise TtsError(
                    f"tts answered {res.status_code} ({_deepgram_error(await res.aread())})"
                )
            async for chunk in res.aiter_bytes():
                if chunk:
                    got = True
                    yield chunk
    except httpx.HTTPError as exc:
        raise TtsError(f"tts unreachable: {type(exc).__name__}") from None
    if not got:
        raise TtsError("tts answered no audio")


async def deepgram_speak(
    base_url: str, key: str, text: str, *, voice: str, speed: float = 1.0
) -> AsyncIterator[bytes]:
    """Live speech: the apps send a sentence or two at a time (at most 1,500 characters), so this
    is nearly always one request, streamed straight through."""
    for piece in split_for_speech(speakable(text), DEEPGRAM_PIECE_CHARS):
        async for chunk in deepgram_stream(base_url, key, piece, voice=voice, speed=speed):
            yield chunk


class Speaker(Protocol):
    def stream(self, text: str, *, voice: str, speed: float) -> AsyncIterator[bytes]: ...


class DeepgramSpeaker:
    """Live speech (read aloud, voice mode, the briefing player) from Deepgram Aura-2."""

    def __init__(self, base_url: str, key: str) -> None:
        self._url = base_url.rstrip("/")
        self._key = key

    def stream(self, text: str, *, voice: str, speed: float) -> AsyncIterator[bytes]:
        return deepgram_speak(self._url, self._key, text, voice=voice, speed=speed)


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
    """Live speech, or None when server voice is off (or Deepgram has no key). ElevenLabs is
    kept for briefing files only."""
    if _speaker_override:
        return _speaker_override[0]
    p = provider()
    if p == "deepgram":
        key = (os.environ.get(DEEPGRAM_KEY_ENV) or "").strip()
        if not key:
            return None
        return DeepgramSpeaker(os.environ.get(DEEPGRAM_URL_ENV) or DEEPGRAM_URL, key)
    if p == "kokoro":
        return KokoroSpeaker(os.environ.get(KOKORO_URL_ENV) or KOKORO_URL)
    return None


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
    if p == "deepgram" and not source.get(DEEPGRAM_KEY_ENV, "").strip():
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
        kind = {"deepgram": DeepgramS3, "kokoro": KokoroS3}.get(provider(env), ElevenLabsS3)
        _cached.append(kind(env))
    return _cached[0]


def connected() -> bool:
    return backend() is not None


def presigned(key: str) -> tuple[str, datetime] | None:
    found = backend()
    if found is None:
        return None
    return found.presign(key, ttl_s=URL_TTL_S), datetime.now(UTC) + timedelta(seconds=URL_TTL_S)
