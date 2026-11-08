"""billing-metering-service's Temporal worker: runs the nightly metering activity.

Run with: uv run python -m services.billing_metering_service.temporal_worker

It polls the `billing-metering-service` task queue for one activity, `billing.run_nightly`,
which `workflow-service`'s `UsageRollupWorkflow` schedules every night. It runs as the request
role (`DATABASE_URL` = `mytheus_app`), so RLS applies to the job exactly as to a request.
"""

from __future__ import annotations

import asyncio
import logging
import os

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from services.billing_metering_service.jobs import run_nightly

TASK_QUEUE = "billing-metering-service"


@activity.defn(name="billing.run_nightly")
async def run_nightly_activity() -> dict[str, int]:
    summary = await run_nightly()
    activity.logger.info("nightly metering: %s", summary)
    return summary


async def main() -> None:  # pragma: no cover - process entry point
    logging.basicConfig(level=logging.INFO)
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    worker = Worker(client, task_queue=TASK_QUEUE, activities=[run_nightly_activity])
    print(f"billing-metering worker started on task queue: {TASK_QUEUE}")
    await worker.run()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
