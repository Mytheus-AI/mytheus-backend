"""In every meeting at once: the hourly `listening-evening` schedule.

`EveningListeningWorkflow` (cron `5 * * * *`, five past every hour, UTC): one activity,
`listening_evening.run(now)`, per tenant core-api `POST /internal/listening/evening`. Hourly
because each person's evening is their own (their digest hour in their time zone); core-api
builds a person's digest once per local day (`UNIQUE (user_id, day)`), so a run that reaches
someone again, a retry or a missed hour caught up later is harmless. Each run also re-applies
everyone's attendance rules to the next three weeks.

The time comes from `workflow.now()` (deterministic under replay). Overlap SKIP;
`ensure_listening_schedule` creates the schedule once and leaves an existing one untouched.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from services.workflow_service.scheduled_activities import LISTENING_EVENING_ACTIVITY

LISTENING_EVENING_SCHEDULE = "listening-evening"
LISTENING_EVENING_CRON = "5 * * * *"

_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1), backoff_coefficient=2.0, maximum_attempts=3
)


@workflow.defn
class EveningListeningWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        result: dict[str, int] = await workflow.execute_activity(
            LISTENING_EVENING_ACTIVITY,
            workflow.now().isoformat(),
            start_to_close_timeout=timedelta(minutes=45),
            retry_policy=_RETRY,
        )
        return result


async def ensure_listening_schedule(client: Any, *, task_queue: str) -> bool:
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
            LISTENING_EVENING_SCHEDULE,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    EveningListeningWorkflow.run,
                    id=f"{LISTENING_EVENING_SCHEDULE}-run",
                    task_queue=task_queue,
                ),
                spec=ScheduleSpec(cron_expressions=[LISTENING_EVENING_CRON]),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
    except ScheduleAlreadyRunningError:
        return False
    return True
