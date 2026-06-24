"""MeetingLifecycleWorkflow — driven by meeting-events."""
from temporalio import workflow


@workflow.defn
class MeetingLifecycleWorkflow:
    @workflow.run
    async def run(self, meeting_id: str, org_id: str) -> None:
        # TODO: coordinate room open/close, post-meeting processing, cleanup
        pass
