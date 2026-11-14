"""The `mytheus-branding` bucket: workspace logos (ADR-0047 D2).

identity-tenant-service is the bucket's one writer, as it is the one writer of
`tenant_settings`, whose `logo_*` columns point here. Keys are tenant-first like every key in
the program (`_canon/02-data-model.md` section 6):

    {org_id}/branding/logo-{version}.{png|jpg|webp}

`version` is `tenant_settings.logo_version`, bumped on every upload, so each upload is a new
key: a cached copy of the old logo can never be served under the new name, and the previous
object is deleted once the new one is committed.

Why its own bucket rather than a `branding/` prefix in an existing one: `mytheus-recordings`
expires everything under `{org_id}/` at the tenant's transcript retention
(`meeting_service.bucket_lifecycle`), which would silently delete a logo after a year;
`mytheus-documents` belongs to ingestion-service and is swept into `storage_gb`; neither has
identity-tenant-service as its writer. `mytheus-branding` has NO lifecycle rule, so a logo is
never auto-expired. It is private: nothing is ever public, and reads go through
`GET /api/v1/org/logo/image`, which checks the caller's tenant.

The client is the shared one (`libs.shared.object_storage`), same `S3_*` variables as every
other bucket; `S3_BUCKET_BRANDING` names the bucket (default `mytheus-branding`). Not configured
-> `get_store()` is None and uploads answer 503 saying storage is not connected.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Protocol

from libs.shared import object_storage

BUCKET_ENV = "S3_BUCKET_BRANDING"
DEFAULT_BUCKET = "mytheus-branding"


def logo_key(org_id: uuid.UUID, version: int, extension: str) -> str:
    return f"{org_id}/branding/logo-{version}.{extension}"


def branding_prefix(org_id: uuid.UUID) -> str:
    return f"{org_id}/branding/"


class LogoStore(Protocol):
    async def put(self, key: str, data: bytes, *, content_type: str) -> None: ...

    async def get(self, key: str) -> bytes | None: ...

    async def delete(self, key: str) -> None: ...


class S3LogoStore:
    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            CacheControl="private, max-age=31536000, immutable",
        )

    async def get(self, key: str) -> bytes | None:
        def _get() -> bytes | None:
            try:
                obj = self._client.get_object(Bucket=self._bucket, Key=key)
            except Exception as exc:  # noqa: BLE001 - botocore ClientError and friends
                code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return None
                raise
            return bytes(obj["Body"].read())

        return await asyncio.to_thread(_get)

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._client.delete_object, Bucket=self._bucket, Key=key)


@dataclass
class FakeLogoStore:
    """In-memory bucket for tests."""

    objects: dict[str, tuple[bytes, str]] = field(default_factory=dict)
    fail_put: bool = False

    async def put(self, key: str, data: bytes, *, content_type: str) -> None:
        if self.fail_put:
            raise RuntimeError("storage unavailable")
        self.objects[key] = (data, content_type)

    async def get(self, key: str) -> bytes | None:
        hit = self.objects.get(key)
        return hit[0] if hit else None

    async def delete(self, key: str) -> None:
        self.objects.pop(key, None)


_override: list[LogoStore | None] = []


def set_store(store: LogoStore | None) -> None:
    """Tests swap the store here (None = storage not connected). `reset_store()` undoes it."""
    _override[:] = [store]


def reset_store() -> None:
    _override.clear()


@lru_cache(maxsize=1)
def _from_env() -> LogoStore | None:
    client = object_storage.make_s3_client()
    if client is None:
        return None
    return S3LogoStore(client, os.environ.get(BUCKET_ENV) or DEFAULT_BUCKET)


def get_store() -> LogoStore | None:
    if _override:
        return _override[0]
    return _from_env()
