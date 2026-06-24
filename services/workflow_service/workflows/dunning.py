"""DunningSagaWorkflow — driven by billing-events (quota-exceeded, payment failed).

Staged: grace period → throttle → downgrade → suspend.
"""
from temporalio import workflow


@workflow.defn
class DunningSagaWorkflow:
    @workflow.run
    async def run(self, org_id: str, reason: str) -> None:
        # TODO: staged retry / downgrade / suspend activities
        pass
