"""Temporal worker entry point.

Run with: uv run python -m services.workflow_service.worker

Workers are stateless — Temporal owns workflow state. Scale on task-queue depth.
"""

import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from services.workflow_service.workflows.action_followup import ActionFollowUpWorkflow
from services.workflow_service.workflows.dunning import DunningSagaWorkflow
from services.workflow_service.workflows.meeting_lifecycle import MeetingLifecycleWorkflow
from services.workflow_service.workflows.rtbf import RTBFWorkflow

_TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
_TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")
_TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "mytheus-backend")


async def main() -> None:
    client = await Client.connect(_TEMPORAL_ADDRESS, namespace=_TEMPORAL_NAMESPACE)
    worker = Worker(
        client,
        task_queue=_TASK_QUEUE,
        workflows=[
            MeetingLifecycleWorkflow,
            ActionFollowUpWorkflow,
            RTBFWorkflow,
            DunningSagaWorkflow,
        ],
        activities=[],  # TODO: register activity functions
    )
    print(f"Worker started on task queue: {_TASK_QUEUE}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
