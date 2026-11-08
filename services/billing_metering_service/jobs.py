"""The nightly metering job: derive meeting usage, then roll up every tenant's month.

Run it directly (`uv run python -m services.billing_metering_service.jobs`) or from the
`workflow-service` Temporal schedule, which calls `run_nightly` as an activity (Plan 7-9
section 3.1 step 7).

It runs as `mytheus_app` like the request path: one transaction per tenant with
`app.current_org` set, so RLS applies exactly as it does for a request and one tenant's
failure rolls back only that tenant. The tenant list comes from `organizations`, which is the
tenant root and has no RLS policy (the application role may read its `id` column).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from libs.shared.db import get_sessionmaker
from libs.shared.tenancy import set_rls
from services.billing_metering_service.budgets import period_of
from services.billing_metering_service.metering import derive_meeting_usage, rollup_period

logger = logging.getLogger(__name__)

LOOKBACK = timedelta(days=3)  # re-scan a few days so a late `ended` status is still metered


async def run_nightly(now: datetime | None = None) -> dict[str, int]:
    now = now or datetime.now(UTC)
    periods = sorted({period_of(now), period_of(now - LOOKBACK)})
    sessionmaker = get_sessionmaker()
    async with sessionmaker() as session:
        org_ids = (
            (await session.execute(text("SELECT id FROM organizations WHERE deleted_at IS NULL")))
            .scalars()
            .all()
        )

    summary = {"tenants": 0, "derived_rows": 0, "failed": 0}
    for org_id in org_ids:
        try:
            async with sessionmaker() as session, session.begin():
                await set_rls(session, uuid.UUID(str(org_id)))
                summary["derived_rows"] += await derive_meeting_usage(
                    session, org_id, now - LOOKBACK
                )
                for period in periods:
                    await rollup_period(session, org_id, period)
            summary["tenants"] += 1
        except Exception:
            summary["failed"] += 1
            logger.exception("nightly metering failed for org=%s", org_id)
    return summary


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    print(asyncio.run(run_nightly()))
