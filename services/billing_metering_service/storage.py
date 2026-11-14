"""The `storage_gb` sweep: how much object storage each tenant holds, once a day.

mytheus-docs ADR-0029 D3: `billing-metering-service` derives `storage_gb` itself, from a nightly
sweep of the tenant buckets. Every object key in the program starts with `{org_id}/`
(`_canon/02-data-model.md` section 6), so a tenant's footprint in a bucket is one prefix
listing. The buckets swept are the ones holding customer content:

- `mytheus-recordings` (`meeting-service`)
- `mytheus-documents`, `mytheus-attachments` (`ingestion-service`)

Not swept: `mytheus-invoices`, `mytheus-exports` and `mytheus-audit-archive` hold artifacts the
platform produces about a tenant, not storage the tenant chose to use.

This is read-only: `ListObjectsV2` only, never a GET of an object body. Cost: one list call per
1,000 objects per bucket per tenant per night. On Cloudflare R2 list calls are Class A
operations, with 1M free a month, so a small deployment stays inside the free tier.

The row is a daily snapshot: `quantity` = GB held at sweep time, `occurred_at` = the sweep
day at 00:00 UTC, key `usage:{org_id}:storage_gb:{day}:{org_id}`, so re-running the sweep on
the same day inserts nothing. The rollup reports the latest snapshot of the period.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import object_storage

logger = logging.getLogger(__name__)

TENANT_BUCKETS = ("mytheus-recordings", "mytheus-documents", "mytheus-attachments")
_BYTES_PER_GB = 1_000_000_000


def make_s3_client(env: Mapping[str, str] | None = None) -> Any | None:
    """A boto3 S3 client from the same `S3_*` variables `meeting-service` uses, or None when
    object storage is not configured (the sweep is then skipped, not failed). The client itself
    comes from the shared factory (`libs.shared.object_storage`)."""
    return object_storage.make_s3_client(env)


def tenant_bytes(s3: Any, org_id: uuid.UUID, buckets: tuple[str, ...] = TENANT_BUCKETS) -> int:
    """Total bytes under `{org_id}/` across `buckets`. A bucket that does not exist counts 0."""
    total = 0
    paginator = s3.get_paginator("list_objects_v2")
    for bucket in buckets:
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{org_id}/"):
                total += sum(int(obj["Size"]) for obj in page.get("Contents", []))
        except s3.exceptions.NoSuchBucket:
            continue
    return total


async def sweep_storage(session: AsyncSession, org_id: uuid.UUID, s3: Any, day: date) -> bool:
    """Write today's `storage_gb` snapshot for one tenant. Returns True if a row was inserted.
    The caller has set `app.current_org`. The listing runs in a thread: boto3 is blocking."""
    size = await asyncio.to_thread(tenant_bytes, s3, org_id)
    at = datetime(day.year, day.month, day.day, tzinfo=UTC)
    result = await session.execute(
        text(
            "INSERT INTO usage_events (org_id, event_type, quantity, idempotency_key, occurred_at) "
            "VALUES (:org, 'storage_gb', :qty, :key, :at) "
            "ON CONFLICT (org_id, idempotency_key, occurred_at) DO NOTHING RETURNING id"
        ),
        {
            "org": str(org_id),
            "qty": round(size / _BYTES_PER_GB, 6),
            "key": f"usage:{org_id}:storage_gb:{day.isoformat()}:{org_id}",
            "at": at,
        },
    )
    return result.first() is not None
