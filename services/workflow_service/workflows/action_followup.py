"""ActionFollowUpWorkflow — driven by confirmed actions.

Durable timers (not cron): reminder at day 1, 3, 7.
Manager escalation via Slack DM at day 10.
"""
from datetime import timedelta
from temporalio import workflow


@workflow.defn
class ActionFollowUpWorkflow:
    @workflow.run
    async def run(self, action_id: str, org_id: str, assignee_id: str) -> None:
        for days in (1, 3, 7):
            await workflow.sleep(timedelta(days=days))
            # TODO: call notification activity to send reminder

        await workflow.sleep(timedelta(days=3))  # day 10 total
        # TODO: call Slack DM escalation activity to manager
