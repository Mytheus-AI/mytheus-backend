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
import os
import re
from collections.abc import Mapping
from typing import Any, Protocol

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
    source = env if env is not None else os.environ
    key, secret = source.get("S3_ACCESS_KEY_ID"), source.get("S3_SECRET_ACCESS_KEY")
    if not key or not secret:
        return None
    import boto3
    from botocore.config import Config

    path_style = (source.get("S3_FORCE_PATH_STYLE") or "").lower() in ("1", "true", "yes")
    client = boto3.client(
        "s3",
        endpoint_url=source.get("S3_ENDPOINT_URL") or None,
        region_name=source.get("S3_REGION") or "auto",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(
            s3={"addressing_style": "path" if path_style else "auto"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )
    return S3AttachmentStore(client)
