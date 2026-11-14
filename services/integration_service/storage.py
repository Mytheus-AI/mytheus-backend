"""Calendar attachments into `mytheus-attachments` (plan 3.11 step 1; 21 section 3.4).

Object keys are `{org_id}/{meeting_id}/{file_id}-{name}`: every key in the program starts with
`{org_id}/` (`_canon/02-data-model.md` section 6), and ingestion-service refuses an
`attachment_to_ingest` whose key does not start with the event's own `org_id`.

Same `S3_*` variables as meeting-service and billing (`S3_ENDPOINT_URL`, `S3_REGION`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_FORCE_PATH_STYLE`). Unconfigured -> None, and
the calendar sync records the attachments on the meeting without ingesting them.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping
from typing import Any, Protocol

from libs.shared import object_storage

BUCKET_ATTACHMENTS = "mytheus-attachments"
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")


class AttachmentStore(Protocol):
    async def put(self, key: str, data: bytes, content_type: str | None) -> None: ...


def object_key(org_id: object, meeting_id: object, file_id: str, title: str | None) -> str:
    name = _UNSAFE.sub("_", title or "attachment").strip("._")[:120] or "attachment"
    return f"{org_id}/{meeting_id}/{_UNSAFE.sub('_', file_id)[:80]}-{name}"


class S3AttachmentStore:
    def __init__(self, client: Any, bucket: str = BUCKET_ATTACHMENTS) -> None:
        self._client = client
        self._bucket = bucket

    async def put(self, key: str, data: bytes, content_type: str | None) -> None:
        extra = {"ContentType": content_type} if content_type else {}
        await asyncio.to_thread(
            self._client.put_object, Bucket=self._bucket, Key=key, Body=data, **extra
        )


def store_from_env(env: Mapping[str, str] | None = None) -> AttachmentStore | None:
    client = object_storage.make_s3_client(env)
    return S3AttachmentStore(client) if client is not None else None
