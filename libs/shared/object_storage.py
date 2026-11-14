"""The one boto3 S3 client factory for object storage (Cloudflare R2 in production, ADR-0023).

Every service that touches a bucket reads the same `S3_*` variables: `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `S3_ENDPOINT_URL` (the R2 account endpoint), `S3_REGION` (`auto` on R2)
and `S3_FORCE_PATH_STYLE` (MinIO). The client construction below was copied into
billing-metering-service (`storage.py`) and integration-service (`storage.py`) line for line;
both now call this, and identity-tenant-service's workspace logo store (ADR-0048) does too, so
there is one place that decides how Mytheus talks to R2.

Returns None when the credentials are not set: object storage is "not connected", and each
caller says so in its own terms (a skipped sweep, attachments recorded but not ingested, a 503
on a logo upload) rather than failing at import.

Buckets are private, always. Nothing here creates a bucket or sets a public ACL; reads go
through a presigned GET or through the owning service.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any


def _flag(value: str | None) -> bool:
    return (value or "").strip().lower() in ("1", "true", "yes", "on")


def s3_configured(env: Mapping[str, str] | None = None) -> bool:
    source = env if env is not None else os.environ
    return bool(source.get("S3_ACCESS_KEY_ID") and source.get("S3_SECRET_ACCESS_KEY"))


def make_s3_client(env: Mapping[str, str] | None = None) -> Any | None:
    """A boto3 S3 client from the `S3_*` variables, or None when they are not set."""
    source = env if env is not None else os.environ
    key, secret = source.get("S3_ACCESS_KEY_ID"), source.get("S3_SECRET_ACCESS_KEY")
    if not key or not secret:
        return None
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=source.get("S3_ENDPOINT_URL") or None,
        region_name=source.get("S3_REGION") or "auto",
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        config=Config(
            s3={"addressing_style": "path" if _flag(source.get("S3_FORCE_PATH_STYLE")) else "auto"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            connect_timeout=5,
            read_timeout=30,
            retries={"max_attempts": 3},
        ),
    )
