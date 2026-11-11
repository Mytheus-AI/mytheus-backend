"""The polish wave 3 schedules: `promise-drift-daily` and `weekly-digest`.

- `PromiseDriftWorkflow` (cron `0 9 * * *`, 09:00 UTC daily): one activity,
  `promise_drift.run(as_of = today)`, nudging owners of overdue Jira-linked promises first and
  their meeting hosts two days later (`scheduled_activities.py`, core-api
  `internal_promise_drift.py`).
- `WeeklyDigestWorkflow` (cron `0 7 * * 1`, Mondays 07:00 UTC): one activity,
  `weekly_digest.run(week_start = last Monday)`, the week that just ended.

The date comes from `workflow.now()` (deterministic under replay). Both activities are
idempotent per day / per week, so the schedules use overlap SKIP and a retry is harmless.
`ensure_schedules` creates them once and leaves an existing schedule untouched (an operator's
pause or time change survives a worker restart), like `usage_rollup.ensure_schedule`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from services.workflow_service.scheduled_activities import (
        PROMISE_DRIFT_ACTIVITY,
        WEEKLY_DIGEST_ACTIVITY,
    )

PROMISE_DRIFT_SCHEDULE = "promise-drift-daily"
PROMISE_DRIFT_CRON = "0 9 * * *"
WEEKLY_DIGEST_SCHEDULE = "weekly-digest"
WEEKLY_DIGEST_CRON = "0 7 * * 1"

_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1), backoff_coefficient=2.0, maximum_attempts=4
)


@workflow.defn
class PromiseDriftWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        today = workflow.now().date()
        result: dict[str, int] = await workflow.execute_activity(
            PROMISE_DRIFT_ACTIVITY,
            today.isoformat(),
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=_RETRY,
        )
        return result


@workflow.defn
class WeeklyDigestWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        today = workflow.now().date()
        week_start = today - timedelta(days=today.weekday() + 7)
        result: dict[str, int] = await workflow.execute_activity(
            WEEKLY_DIGEST_ACTIVITY,
            week_start.isoformat(),
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=_RETRY,
        )
        return result


async def ensure_schedules(client: Any, *, task_queue: str) -> list[str]:
    """Create the two schedules if absent. Returns the ids that were created."""
    from temporalio.client import (
        Schedule,
        ScheduleActionStartWorkflow,
        ScheduleAlreadyRunningError,
        ScheduleOverlapPolicy,
        SchedulePolicy,
        ScheduleSpec,
    )

    created: list[str] = []
    for schedule_id, cron, wf in (
        (PROMISE_DRIFT_SCHEDULE, PROMISE_DRIFT_CRON, PromiseDriftWorkflow.run),
        (WEEKLY_DIGEST_SCHEDULE, WEEKLY_DIGEST_CRON, WeeklyDigestWorkflow.run),
    ):
        try:
            await client.create_schedule(
                schedule_id,
                Schedule(
                    action=ScheduleActionStartWorkflow(
                        wf, id=f"{schedule_id}-run", task_queue=task_queue
                    ),
                    spec=ScheduleSpec(cron_expressions=[cron]),
                    policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
                ),
            )
        except ScheduleAlreadyRunningError:
            continue
        created.append(schedule_id)
    return created
