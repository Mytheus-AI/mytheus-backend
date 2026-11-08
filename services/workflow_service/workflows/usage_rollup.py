"""UsageRollupWorkflow: the nightly metering run, on a Temporal schedule.

Plan 7-9 section 3.1 step 7. The schedule `usage-rollup-nightly` starts this workflow at 02:00
UTC every day. The workflow does no work itself: it hands one activity, `billing.run_nightly`,
to the `billing-metering-service` task queue, where `billing-metering-service`'s own worker
(`services/billing_metering_service/temporal_worker.py`) runs it. That keeps the one-writer
rule intact: `workflow-service` owns the schedule and the retry policy, and only
`billing-metering-service` code, in a `billing-metering-service` process, writes its tables.

The activity is idempotent (every row it writes has a stable idempotency key, and the rollup
is an upsert), so a retry or a manual trigger on the same day is harmless.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

SCHEDULE_ID = "usage-rollup-nightly"
SCHEDULE_CRON = "0 2 * * *"  # 02:00 UTC daily
BILLING_TASK_QUEUE = "billing-metering-service"
ACTIVITY_NAME = "billing.run_nightly"


@workflow.defn
class UsageRollupWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        result: dict[str, int] = await workflow.execute_activity(
            ACTIVITY_NAME,
            task_queue=BILLING_TASK_QUEUE,
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(minutes=1),
                backoff_coefficient=2.0,
                maximum_attempts=4,
            ),
        )
        return result


async def ensure_schedule(client: Any, *, task_queue: str) -> bool:
    """Create the nightly schedule if it does not exist. Returns True if it was created.

    Idempotent across worker restarts: an existing schedule is left exactly as it is, so an
    operator who paused it or changed its time keeps that change.
    """
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleAlreadyRunningError,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
    )

    try:
        await client.create_schedule(
            SCHEDULE_ID,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    UsageRollupWorkflow.run,
                    id=f"{SCHEDULE_ID}-run",
                    task_queue=task_queue,
                ),
                spec=ScheduleSpec(cron_expressions=[SCHEDULE_CRON]),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
    except ScheduleAlreadyRunningError:
        return False
    return True
