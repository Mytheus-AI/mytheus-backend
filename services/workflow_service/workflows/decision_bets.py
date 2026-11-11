"""`DecisionBetsWorkflow` (cron `0 8 * * *`, 08:00 UTC daily): check the bets whose date came.

One activity, `decision_bets.run(as_of = today)` (`bet_activities.py`): evidence gathered, an
outcome suggested where a connected metric is clear, the owner asked to resolve. The date
comes from `workflow.now()` (deterministic under replay); the activity is idempotent per bet,
so the schedule uses overlap SKIP and a retry is harmless. `ensure_schedule` creates the
schedule once and leaves an existing one untouched, like `only_mytheus.ensure_schedules`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from services.workflow_service.bet_activities import DECISION_BETS_ACTIVITY

DECISION_BETS_SCHEDULE = "decision-bets-daily"
DECISION_BETS_CRON = "0 8 * * *"

_RETRY = RetryPolicy(
    initial_interval=timedelta(minutes=1), backoff_coefficient=2.0, maximum_attempts=4
)


@workflow.defn
class DecisionBetsWorkflow:
    @workflow.run
    async def run(self) -> dict[str, int]:
        today = workflow.now().date()
        result: dict[str, int] = await workflow.execute_activity(
            DECISION_BETS_ACTIVITY,
            today.isoformat(),
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=_RETRY,
        )
        return result


async def ensure_schedule(client: Any, *, task_queue: str) -> bool:
    """Create the schedule if absent. True when it was created now."""
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
            DECISION_BETS_SCHEDULE,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    DecisionBetsWorkflow.run,
                    id=f"{DECISION_BETS_SCHEDULE}-run",
                    task_queue=task_queue,
                ),
                spec=ScheduleSpec(cron_expressions=[DECISION_BETS_CRON]),
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
    except ScheduleAlreadyRunningError:
        return False
    return True
