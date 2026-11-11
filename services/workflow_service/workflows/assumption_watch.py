"""The `assumption-watch` schedule (Beyond, Layer 1: decisions that know why).

`AssumptionWatchWorkflow` (cron `15 */6 * * *`, every six hours): one activity,
`assumption_watch.run(as_of = today)` (`assumption_activities.py`): re-read the Jira issues and
HubSpot deals decisions' reasons are watched against, and break what no longer holds. The date
comes from `workflow.now()` (deterministic under replay); overlap SKIP; idempotent, so a retry
is harmless. `ensure_schedule` creates it once and leaves an existing schedule untouched, like
`only_mytheus.ensure_schedules`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from services.workflow_service.assumption_activities import ASSUMPTION_WATCH_ACTIVITY

ASSUMPTION_WATCH_SCHEDULE = "assumption-watch"
ASSUMPTION_WATCH_CRON = "15 */6 * * *"

_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1), backoff_coefficient=2.0, maximum_attempts=4
)


@workflow.defn
class AssumptionWatchWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        today = workflow.now().date()
        result: dict[str, int] = await workflow.execute_activity(
            ASSUMPTION_WATCH_ACTIVITY,
            today.isoformat(),
            start_to_close_timeout=timedelta(minutes=45),
            retry_policy=_RETRY,
        )
        return result


async def ensure_schedule(client: Any, *, task_queue: str) -> bool:
    """Create the schedule if absent. True when it was created."""
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
            ASSUMPTION_WATCH_SCHEDULE,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    AssumptionWatchWorkflow.run,
                    id=f"{ASSUMPTION_WATCH_SCHEDULE}-run",
                    task_queue=task_queue,
                ),
                spec=ScheduleSpec(cron_expressions=[ASSUMPTION_WATCH_CRON]),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
    except ScheduleAlreadyRunningError:
        return False
    return True
