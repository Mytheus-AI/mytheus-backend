"""Per-store progress of a data erasure, derived from `audit_logs` (append-only) at read time.

A request (`POST /admin/tenant/erasures`) writes one `data_erasure.completed` row whose metadata
names the stores it waits for (`expected_stores`) and core-api's share, which finished inside
the request. Every other store reports back on `admin-events` `erasure_completed`; the
`admin-ops-service.erasure-status` consumer (`erasure_consumer.py`) records one
`data_erasure.store_completed` row per store, `target_id` = the erasure id. An erasure is
`completed` once every expected store has a row.

Rows written before wave 3 carry no `expected_stores`: nothing reported back for them, so they
read as `completed` with only `core_api` listed (what was true when they were recorded).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import ACTION_DATA_ERASURE_STORE_COMPLETED
from services.admin_ops_service.schemas import ErasureStore

CORE_STORE = "core_api"
# The stores outside this repo that finish an erasure, in the order the console lists them
# (mytheus-events admin-events `erasure_completed`: one per store).
REMOTE_STORES: tuple[str, ...] = (
    "transcript_chunks",
    "vectors",
    "search_index",
    "knowledge_graph",
)
EXPECTED_STORES: tuple[str, ...] = (CORE_STORE, *REMOTE_STORES)


def _meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        out = json.loads(raw or "{}")
    except ValueError:
        return {}
    return out if isinstance(out, dict) else {}


def _counts(raw: Any) -> dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in raw.items():
        if isinstance(value, bool) or not isinstance(value, int | float):
            continue
        out[str(key)[:64]] = int(value)
    return out


def request_metadata_stores() -> list[str]:
    """What a new request records as the stores it waits for."""
    return list(EXPECTED_STORES)


def build_status(
    request_meta: Mapping[str, Any],
    requested_at: datetime,
    completed: Mapping[str, tuple[datetime, dict[str, int]]],
) -> tuple[str, list[ErasureStore]]:
    """(`progress`: `in_progress`|`completed`, the per-store checklist) for one request."""
    expected = request_meta.get("expected_stores")
    if not isinstance(expected, list):
        # Recorded before stores reported back: core-api's share is all that ever ran here.
        return "completed", [
            ErasureStore(
                store="core_api",
                status="completed",
                completed_at=requested_at,
                counts=_counts(request_meta.get("erased")),
            )
        ]
    stores: list[ErasureStore] = []
    for name in EXPECTED_STORES:
        if name not in expected:
            continue
        if name == CORE_STORE:
            stores.append(
                ErasureStore(
                    store="core_api",
                    status="completed",
                    completed_at=requested_at,
                    counts=_counts(request_meta.get("erased")),
                )
            )
            continue
        done = completed.get(name)
        stores.append(
            ErasureStore(
                store=name,  # type: ignore[arg-type]
                status="completed" if done else "pending",
                completed_at=done[0] if done else None,
                counts=done[1] if done else {},
            )
        )
    status = "completed" if all(s.status == "completed" for s in stores) else "in_progress"
    return status, stores


async def load_store_completions(
    session: AsyncSession, erasure_ids: Iterable[str]
) -> dict[str, dict[str, tuple[datetime, dict[str, int]]]]:
    """erasure id -> store -> (first completion time, counts), under the caller's RLS."""
    ids = list(dict.fromkeys(erasure_ids))
    if not ids:
        return {}
    rows = (
        await session.execute(
            text(
                "SELECT target_id, occurred_at, metadata FROM audit_logs "
                "WHERE action = :a AND target_type = 'data_erasure' "
                "AND target_id = ANY(CAST(:ids AS text[])) ORDER BY occurred_at"
            ),
            {"a": ACTION_DATA_ERASURE_STORE_COMPLETED, "ids": ids},
        )
    ).all()
    out: dict[str, dict[str, tuple[datetime, dict[str, int]]]] = {}
    for target_id, occurred_at, metadata in rows:
        meta = _meta(metadata)
        store = meta.get("store")
        if not isinstance(store, str) or store not in REMOTE_STORES:
            continue
        out.setdefault(str(target_id), {}).setdefault(
            store, (occurred_at, _counts(meta.get("counts")))
        )
    return out
