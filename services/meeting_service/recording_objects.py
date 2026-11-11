"""Reading, writing and deleting objects in the `mytheus-recordings` bucket (R2, S3-compatible).

meeting-service owns the bucket (CLAUDE.md service map, ADR-0023), so this is the only module
that touches its objects. LiveKit's egress worker still WRITES the recordings themselves; what
happens here is:

- a short-lived presigned GET for playback (`presign_get`). Never a public URL, never stored:
  a fresh one is minted for each authorised read, and the bucket stays private;
- `exists` / `upload_file`: a rendered clip (`clips.py`) is written next to its recording,
  under the same `{org_id}/{meeting_id}/` prefix, so retention and erasure cover it;
- `delete_prefix`: a meeting's data erasure deletes every object under its prefix.

`S3RecordingObjects` uses boto3 (a dependency already) behind `asyncio.to_thread`, so no
blocking call runs on the event loop. Presigning is local computation (no network call).
`FakeRecordingObjects` is an in-memory stand-in for tests.

NOT CONNECTED until recording is configured: `objects_for(config)` is None unless the S3
credentials (`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, plus `S3_ENDPOINT_URL` for R2) are set.
Every caller treats None as "recording storage is not connected" and says so.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Protocol

from services.meeting_service.storage import RecordingConfig

# Playback URLs live minutes, not hours: long enough to watch a segment (a player re-asks for a
# fresh one when it expires), short enough that a copied URL is useless soon after.
DEFAULT_URL_TTL_SECONDS = 600
MIN_URL_TTL_SECONDS = 60
MAX_URL_TTL_SECONDS = 3600


def url_ttl_seconds() -> int:
    """`RECORDING_URL_TTL_SECONDS`, clamped to 60..3600 (default 600)."""
    raw = os.environ.get("RECORDING_URL_TTL_SECONDS", "").strip()
    try:
        value = int(raw) if raw else DEFAULT_URL_TTL_SECONDS
    except ValueError:
        value = DEFAULT_URL_TTL_SECONDS
    return max(MIN_URL_TTL_SECONDS, min(MAX_URL_TTL_SECONDS, value))


class RecordingObjects(Protocol):
    def presign_get(self, key: str, *, ttl_seconds: int, content_type: str) -> str: ...

    async def exists(self, key: str) -> bool: ...

    async def upload_file(self, path: str, key: str, *, content_type: str) -> None: ...

    async def delete_prefix(self, prefix: str) -> int: ...


class S3RecordingObjects:
    def __init__(self, config: RecordingConfig) -> None:
        import boto3
        from botocore.config import Config

        self._bucket = config.bucket
        self._client: Any = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path" if config.force_path_style else "auto"},
                connect_timeout=5,
                read_timeout=30,
                retries={"max_attempts": 3},
            ),
        )

    def presign_get(self, key: str, *, ttl_seconds: int, content_type: str) -> str:
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self._bucket,
                    "Key": key,
                    "ResponseContentType": content_type,
                    "ResponseContentDisposition": "inline",
                    "ResponseCacheControl": "private, no-store",
                },
                ExpiresIn=ttl_seconds,
            )
        )

    async def exists(self, key: str) -> bool:
        def _head() -> bool:
            try:
                self._client.head_object(Bucket=self._bucket, Key=key)
            except Exception as exc:  # noqa: BLE001 - botocore ClientError 404 and friends
                code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return False
                raise
            return True

        return await asyncio.to_thread(_head)

    async def upload_file(self, path: str, key: str, *, content_type: str) -> None:
        await asyncio.to_thread(
            self._client.upload_file,
            path,
            self._bucket,
            key,
            ExtraArgs={"ContentType": content_type},
        )

    async def delete_prefix(self, prefix: str) -> int:
        if not prefix or prefix.count("/") < 2:
            # Never wider than one meeting: `{org_id}/{meeting_id}/` at least.
            raise ValueError("delete_prefix needs a meeting-level prefix")

        def _delete() -> int:
            deleted = 0
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                for start in range(0, len(keys), 1000):
                    batch = keys[start : start + 1000]
                    self._client.delete_objects(
                        Bucket=self._bucket, Delete={"Objects": batch, "Quiet": True}
                    )
                    deleted += len(batch)
            return deleted

        return await asyncio.to_thread(_delete)


@dataclass
class FakeRecordingObjects:
    """In-memory bucket for tests. Presigned URLs are deterministic and carry the TTL."""

    objects: dict[str, bytes] = field(default_factory=dict)
    presigned: list[str] = field(default_factory=list)

    def presign_get(self, key: str, *, ttl_seconds: int, content_type: str) -> str:
        url = f"https://recordings.test/{key}?X-Amz-Expires={ttl_seconds}&ct={content_type}"
        self.presigned.append(key)
        return url

    async def exists(self, key: str) -> bool:
        return key in self.objects

    async def upload_file(self, path: str, key: str, *, content_type: str) -> None:
        with open(path, "rb") as fh:
            self.objects[key] = fh.read()

    async def delete_prefix(self, prefix: str) -> int:
        doomed = [k for k in self.objects if k.startswith(prefix)]
        for k in doomed:
            del self.objects[k]
        return len(doomed)


def objects_for(config: RecordingConfig) -> RecordingObjects | None:
    """The bucket client, or None when recording storage is not connected."""
    if not config.s3_configured:
        return None
    return S3RecordingObjects(config)


def get_recording_objects() -> RecordingObjects | None:
    """FastAPI dependency seam (tests override it with a `FakeRecordingObjects`)."""
    return objects_for(RecordingConfig.from_env())


__all__ = [
    "DEFAULT_URL_TTL_SECONDS",
    "FakeRecordingObjects",
    "RecordingObjects",
    "S3RecordingObjects",
    "get_recording_objects",
    "objects_for",
    "url_ttl_seconds",
]
