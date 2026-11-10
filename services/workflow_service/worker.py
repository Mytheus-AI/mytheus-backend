"""Temporal worker entry point.

Run with: uv run python -m services.workflow_service.worker

Workers are stateless — Temporal owns workflow state. Scale on task-queue depth.

The consumer that starts and signals MeetingLifecycleWorkflow (`meeting-events`) and starts
ActionFollowUpWorkflow (`actions`) is a separate process: `python -m
services.workflow_service.consumer`.
"""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from services.workflow_service.activities import LifecycleActivities
from services.workflow_service.followup_activities import FollowUpActivities
from services.workflow_service.reminder_activities import ReminderActivities
from services.workflow_service.workflows.action_followup import ActionFollowUpWorkflow
from services.workflow_service.workflows.dunning import DunningSagaWorkflow
from services.workflow_service.workflows.meeting_lifecycle import MeetingLifecycleWorkflow
from services.workflow_service.workflows.meeting_reminders import MeetingRemindersWorkflow
from services.workflow_service.workflows.rtbf import RTBFWorkflow
from services.workflow_service.workflows.usage_rollup import UsageRollupWorkflow, ensure_schedule

_TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
_TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
_TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "mytheus-backend")


async def main() -> None:
    client = await Client.connect(_TEMPORAL_ADDRESS, namespace=_TEMPORAL_NAMESPACE)
    # MeetingLifecycleWorkflow's REST activities (P10, Slack touches P12) and
    # ActionFollowUpWorkflow's (P12; these also need DATABASE_URL for the outbox). RTBF and
    # dunning are still stubs.
    lifecycle = LifecycleActivities.from_env()
    followup = FollowUpActivities.from_env()
    # ADR-0039 B9: MeetingRemindersWorkflow's REST + outbox activities.
    reminders = ReminderActivities.from_env()
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[
            MeetingLifecycleWorkflow,
            MeetingRemindersWorkflow,
            ActionFollowUpWorkflow,
            RTBFWorkflow,
            DunningSagaWorkflow,
            UsageRollupWorkflow,
        ],
        activities=[*lifecycle.all(), *followup.all(), *reminders.all()],
    )
    if await ensure_schedule(client, task_queue=_TASK_QUEUE):
        print("Created Temporal schedule usage-rollup-nightly (02:00 UTC)")
    print(f"Worker started on task queue: {_TASK_QUEUE}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
