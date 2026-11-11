"""The server-side voice of a briefing: a text-to-speech provider and the audio's object storage.

NOT CONNECTED by default. The apps speak a briefing on the device (the browser's speechSynthesis,
expo-speech on the phone) unless BOTH of these are configured:

- `TTS_PROVIDER=elevenlabs` with `ELEVENLABS_API_KEY` (optional `ELEVENLABS_MODEL_ID`, default
  `eleven_multilingual_v2`, which speaks every briefing language). Any other value, or no key,
  is "not connected".
- Object storage, the same `S3_*` variables meeting-service and billing use
  (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL` for R2, `S3_REGION`,
  `S3_FORCE_PATH_STYLE`), and the bucket `BRIEFING_AUDIO_BUCKET` (default `mytheus-briefings`,
  core-api's own). Keys follow the program's convention, tenant first:
  `{org_id}/briefings/{user_id}/{briefing_id}.mp3`, so retention, residency and erasure act on
  a prefix.

When connected, core-api renders the audio right after the script lands (`render`), stores it
privately and serves a short-lived presigned GET (`presign`, 10 minutes, never stored, never
public). The phone and the browser then play the file instead of speaking the script.

`set_backend` swaps the provider and the store for tests (`FakeAudio`); no test calls a real
provider.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

import httpx

logger = logging.getLogger(__name__)

PROVIDER_ENV = "TTS_PROVIDER"
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


# ElevenLabs premade voices (available on every account). The persona's personality picks the
# default; the person can choose any of them in their preferences.
SERVER_VOICES: tuple[ServerVoice, ...] = (
    ServerVoice("21m00Tcm4TlvDq8ikWAM", "Rachel", "Calm and clear"),
    ServerVoice("pNInz6obpgDQGcFmaJgB", "Adam", "Deep and steady"),
    ServerVoice("EXAVITQu4vr4xnSDxMfL", "Bella", "Soft and warm"),
    ServerVoice("ErXwobaYiN019PkySvjV", "Antoni", "Friendly and round"),
    ServerVoice("TxGEqnHWrfWFTfGW9XjX", "Josh", "Brisk and direct"),
    ServerVoice("MF3mGyEqCkNgVwKbdHrq", "Elli", "Bright and quick"),
)
_BY_PERSONALITY = {
    "balanced": "21m00Tcm4TlvDq8ikWAM",
    "concise": "TxGEqnHWrfWFTfGW9XjX",
    "detailed": "pNInz6obpgDQGcFmaJgB",
    "direct": "TxGEqnHWrfWFTfGW9XjX",
    "friendly": "ErXwobaYiN019PkySvjV",
}


def voice_for(preferred: str | None, personality: str | None) -> str:
    """The person's chosen server voice when it is one we know, else the persona's default."""
    if preferred and any(v.id == preferred for v in SERVER_VOICES):
        return preferred
    return _BY_PERSONALITY.get(personality or "balanced", SERVER_VOICES[0].id)


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

        self._key = env[ELEVENLABS_KEY_ENV]
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
    source = env if env is not None else dict(os.environ)
    return (
        source.get(PROVIDER_ENV, "").strip().lower() == "elevenlabs"
        and bool(source.get(ELEVENLABS_KEY_ENV, "").strip())
        and bool(source.get("S3_ACCESS_KEY_ID", "").strip())
        and bool(source.get("S3_SECRET_ACCESS_KEY", "").strip())
    )


_cached: list[AudioBackend] = []


def backend() -> AudioBackend | None:
    """The configured backend, or None ("not connected": the apps speak on the device)."""
    if _override:
        return _override[0]
    if not configured_from_env():
        return None
    if not _cached:
        _cached.append(ElevenLabsS3(dict(os.environ)))
    return _cached[0]


def connected() -> bool:
    return backend() is not None


def presigned(key: str) -> tuple[str, datetime] | None:
    found = backend()
    if found is None:
        return None
    return found.presign(key, ttl_s=URL_TTL_S), datetime.now(UTC) + timedelta(seconds=URL_TTL_S)
