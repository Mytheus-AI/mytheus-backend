"""Derived usage dimensions and the nightly rollup.

Plan 7-9 section 3.1 steps 5 and 7, with the producer ownership fixed by mytheus-docs ADR-0029
D3: `billing-metering-service` derives `meeting_minute` and `transcription_second` itself and
writes them as `usage_events` rows directly. They never travel the `usage-events` topic.

Both are derived from `transcript_chunks` finals (owned by `transcription-service`, read-only
here, under RLS) for meetings whose `status` is `ended`, so a meeting is metered once, after
its totals stop moving:

- `meeting_minute` = whole minutes from the first final's `start_ms` to the last final's
  `end_ms`, rounded up. This is the span people were talking, which is what this program can
  measure today: `meetings` carries scheduled times, not actual ones.
- `transcription_second` = the sum of every final's `end_ms - start_ms`. There is one
  transcription stream per participant, so overlapping speech counts once per speaker, which
  is how the provider bills it.

Each row's idempotency key is `usage:{org_id}:{event_type}:{occurred_at}:{meeting_id}` with
`occurred_at` = the meeting's last final, so re-running the job inserts nothing new.

`storage_gb` is NOT derived yet. ADR-0029 D3 assigns it to a nightly object-storage sweep; that
needs the R2 listing credentials in this service and is recorded as open in the band file.

The rollup collapses a period's `usage_events` into `usage_aggregates` (upsert on
`(org_id, dimension, billing_period)`, so it is idempotent) with the haiku/sonnet/opus split
for `agent_token`, and refreshes `quotas` for `meeting_minute` against the plan's per-tenant
`included_meeting_minutes`. Meeting minutes are recorded, not enforced: nothing in this band
stops a meeting for quota, because durable facts are never shed.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from services.billing_metering_service.budgets import period_end, plan_for_tenant

_ENDED_MEETING_TOTALS = text("""
    SELECT c.meeting_id,
           max(c.created_at)                                      AS occurred_at,
           ceil((max(c.end_ms) - min(c.start_ms)) / 60000.0)      AS minutes,
           round(sum(c.end_ms - c.start_ms) / 1000.0, 3)          AS speech_seconds
    FROM transcript_chunks c
    JOIN meetings m ON m.id = c.meeting_id
    WHERE m.status = 'ended' AND m.deleted_at IS NULL
      AND c.is_final AND c.deleted_at IS NULL
      AND c.created_at >= :since
    GROUP BY c.meeting_id
    """)
_INSERT_DERIVED = text(
    "INSERT INTO usage_events (org_id, event_type, quantity, idempotency_key, occurred_at) "
    "VALUES (:org, :type, :qty, :key, :at) "
    "ON CONFLICT (org_id, idempotency_key, occurred_at) DO NOTHING RETURNING id"
)


async def derive_meeting_usage(session: AsyncSession, org_id: uuid.UUID, since: datetime) -> int:
    """Write `meeting_minute` + `transcription_second` rows for ended meetings whose finals
    landed on or after `since`. Returns the number of rows newly inserted. Caller has set RLS.
    """
    inserted = 0
    rows = (await session.execute(_ENDED_MEETING_TOTALS, {"since": since})).mappings().all()
    for row in rows:
        at = row["occurred_at"]
        stamp = at.isoformat()
        for event_type, qty in (
            ("meeting_minute", row["minutes"]),
            ("transcription_second", row["speech_seconds"]),
        ):
            result = await session.execute(
                _INSERT_DERIVED,
                {
                    "org": str(org_id),
                    "type": event_type,
                    "qty": qty,
                    "key": f"usage:{org_id}:{event_type}:{stamp}:{row['meeting_id']}",
                    "at": at,
                },
            )
            inserted += result.first() is not None
    return inserted


_ROLLUP = text("""
    WITH tiers AS (
      SELECT event_type, model_tier, sum(quantity) AS qty
      FROM usage_events
      WHERE occurred_at >= :start AND occurred_at < :end
      GROUP BY event_type, model_tier
    )
    INSERT INTO usage_aggregates (org_id, dimension, billing_period, quantity, tier_breakdown)
    SELECT :org, event_type, :period, sum(qty),
           COALESCE(jsonb_object_agg(model_tier, qty) FILTER (WHERE model_tier IS NOT NULL),
                    '{}'::jsonb)
    FROM tiers
    GROUP BY event_type
    ON CONFLICT (org_id, dimension, billing_period) DO UPDATE
      SET quantity = EXCLUDED.quantity,
          tier_breakdown = EXCLUDED.tier_breakdown,
          updated_at = now()
    """)


async def rollup_period(session: AsyncSession, org_id: uuid.UUID, period: str) -> None:
    """Recompute `usage_aggregates` and the `meeting_minute` quota for one tenant-month."""
    year, month = (int(p) for p in period.split("-"))
    start = datetime(year, month, 1, tzinfo=period_end(period).tzinfo)
    end = period_end(period)
    await session.execute(
        _ROLLUP, {"org": str(org_id), "period": period, "start": start, "end": end}
    )

    plan = await plan_for_tenant(session, org_id)
    used = (
        await session.execute(
            text(
                "SELECT COALESCE(sum(quantity), 0) FROM usage_aggregates "
                "WHERE dimension = 'meeting_minute' AND billing_period = :p"
            ),
            {"p": period},
        )
    ).scalar_one()
    quota = int(plan["included_meeting_minutes"])
    state = "throttled" if quota > 0 and used >= quota else "ok"
    await session.execute(
        text(
            "INSERT INTO quotas (org_id, dimension, period, quota, consumed, state) "
            "VALUES (:org, 'meeting_minute', :p, :quota, :used, :state) "
            "ON CONFLICT (org_id, dimension, period) DO UPDATE "
            "SET quota = EXCLUDED.quota, consumed = EXCLUDED.consumed, state = EXCLUDED.state"
        ),
        {"org": str(org_id), "p": period, "quota": quota, "used": used, "state": state},
    )
