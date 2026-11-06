"""Lifecycle (retention) configuration for the `mytheus-recordings` bucket.

Canon `_canon/02-data-model.md` §6: `mytheus-recordings`, key prefix `{org_id}/{meeting_id}/`,
"Per-tenant `retention_days_transcripts`; transition to cold tier after 30d; purge on RTBF".
Built as plain S3 lifecycle rules so the same config applies to Cloudflare R2 (ADR-0023),
MinIO, and AWS S3 through boto3:

- one bucket-wide rule moves every object to the cold storage class after 30 days and aborts
  stale multipart uploads (egress uploads in parts);
- one rule PER TENANT (`Prefix: "{org_id}/"`) expires objects at that tenant's
  `retention_days_transcripts`. There is deliberately no bucket-wide expiration: S3 applies
  the SHORTEST matching expiration, so a catch-all would silently cap every tenant's
  retention.

RTBF purge is not a lifecycle rule (it is immediate, per tenant/user) and belongs to the
workflow-service RTBF saga. S3 caps a bucket at 1000 lifecycle rules; past that, tenants need
a scheduled per-prefix sweep instead (raised here as an error, not silently truncated).

Storage class: R2 and S3 use `STANDARD_IA`; MinIO needs the NAME of a configured remote tier
(`mc admin tier add ...`), passed via `--cold-storage-class` / `RECORDING_COLD_STORAGE_CLASS`;
`none` disables the transition (MinIO with no tier configured).

CLI (does nothing destructive; `--dry-run` only prints):

    uv run python -m services.meeting_service.bucket_lifecycle \\
        --tenant-retention tenants.json --dry-run

`tenants.json` maps `org_id` to days (`null` = `--default-retention-days`, which defaults to
365, the `tenant_settings.retention_days_transcripts` server default). Exporting that map from
`tenant_settings` needs a cross-tenant read, which no request-path role may do, so it is an
operator input here rather than a query.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from services.meeting_service.storage import RecordingConfig

COLD_AFTER_DAYS = 30
DEFAULT_RETENTION_DAYS = 365
DEFAULT_COLD_STORAGE_CLASS = "STANDARD_IA"
ABORT_MULTIPART_DAYS = 7
S3_MAX_LIFECYCLE_RULES = 1000


def build_lifecycle_rules(
    *,
    tenant_retention_days: Mapping[UUID, int],
    cold_after_days: int = COLD_AFTER_DAYS,
    cold_storage_class: str | None = DEFAULT_COLD_STORAGE_CLASS,
) -> list[dict[str, Any]]:
    if cold_after_days < 1:
        raise ValueError("cold_after_days must be >= 1")
    base: dict[str, Any] = {
        "ID": "mytheus-recordings-cold-tier",
        "Filter": {"Prefix": ""},
        "Status": "Enabled",
        "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": ABORT_MULTIPART_DAYS},
    }
    if cold_storage_class:
        base["Transitions"] = [{"Days": cold_after_days, "StorageClass": cold_storage_class}]
    rules: list[dict[str, Any]] = [base]
    for org_id in sorted(tenant_retention_days, key=str):
        days = tenant_retention_days[org_id]
        if days < 1:
            raise ValueError(f"retention for org {org_id} must be >= 1 day, got {days}")
        rules.append(
            {
                "ID": f"retention-{org_id}",
                "Filter": {"Prefix": f"{org_id}/"},
                "Status": "Enabled",
                "Expiration": {"Days": days},
            }
        )
    if len(rules) > S3_MAX_LIFECYCLE_RULES:
        raise ValueError(
            f"{len(rules)} lifecycle rules exceed the S3 limit of {S3_MAX_LIFECYCLE_RULES}"
        )
    return rules


def parse_tenant_retention(
    raw: Mapping[str, int | None], *, default_days: int = DEFAULT_RETENTION_DAYS
) -> dict[UUID, int]:
    return {UUID(k): (default_days if v is None else int(v)) for k, v in raw.items()}


def make_s3_client(cfg: RecordingConfig) -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint_url,
        region_name=cfg.region,
        aws_access_key_id=cfg.access_key_id,
        aws_secret_access_key=cfg.secret_access_key,
        config=Config(
            s3={"addressing_style": "path" if cfg.force_path_style else "auto"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def apply_lifecycle(s3_client: Any, *, bucket: str, rules: Sequence[Mapping[str, Any]]) -> None:
    """Replaces the bucket's whole lifecycle configuration (S3 semantics: PUT is not a merge)."""
    s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket, LifecycleConfiguration={"Rules": [dict(r) for r in rules]}
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Apply mytheus-recordings lifecycle rules")
    parser.add_argument("--tenant-retention", required=True, help="JSON file: {org_id: days}")
    parser.add_argument("--default-retention-days", type=int, default=DEFAULT_RETENTION_DAYS)
    parser.add_argument("--cold-after-days", type=int, default=COLD_AFTER_DAYS)
    parser.add_argument(
        "--cold-storage-class",
        default=os.environ.get("RECORDING_COLD_STORAGE_CLASS", DEFAULT_COLD_STORAGE_CLASS),
        help="'none' disables the cold-tier transition",
    )
    parser.add_argument("--dry-run", action="store_true", help="print rules, do not apply")
    args = parser.parse_args(argv)

    with open(args.tenant_retention, encoding="utf-8") as fh:
        raw = json.load(fh)
    storage_class = None if args.cold_storage_class.lower() == "none" else args.cold_storage_class
    rules = build_lifecycle_rules(
        tenant_retention_days=parse_tenant_retention(raw, default_days=args.default_retention_days),
        cold_after_days=args.cold_after_days,
        cold_storage_class=storage_class,
    )
    cfg = RecordingConfig.from_env()
    if args.dry_run:
        json.dump({"Bucket": cfg.bucket, "Rules": rules}, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0
    if not cfg.s3_configured:
        sys.stderr.write("S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY are not set\n")
        return 2
    apply_lifecycle(make_s3_client(cfg), bucket=cfg.bucket, rules=rules)
    sys.stdout.write(f"applied {len(rules)} lifecycle rules to {cfg.bucket}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
