"""MeetingLifecycleWorkflow: one durable execution per meeting (P10, plan 3.1).

Started by workflow-service's `meeting-events` consumer on `created` with the deterministic id
`meetinglifecycle-{meeting_id}` and `WorkflowIDReusePolicy.REJECT_DUPLICATE`, so a redelivered
`created` (or one replayed after the run finished) never starts a second execution. Signalled
`meeting_ended` on `ended` (signal-with-start, so an `ended` whose `created` was missed still
gets its post-processing).

Steps, each an activity over REST (`services/workflow_service/activities.py`) with
`RETRY_POLICY`:

1. `open_meeting_room` (skipped when the start carried the ended signal: there is no room to
   open for a meeting that is already over). A failure here is recorded, not fatal: the token
   mint and meeting-service's own consumer open the room too, and the meeting still needs its
   post-processing and close.
2. Wait for `meeting_ended`, at most `MAX_OPEN`. `meeting-events` has no `cancelled` type (a
   cancel is an `updated`), so without the bound a cancelled meeting would leave an execution
   open forever; after it the room is closed and post-processing is skipped.
3. `run_post_processing` (only when ended). A failure is recorded and the room is still closed.
4. `close_meeting_room`. A failure here fails the run: it is the one step nothing else retries.

All workflow state is in Temporal history, so the run survives a worker restart: the next worker
replays history and resumes waiting at step 2.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

ENDED_SIGNAL = "meeting_ended"
MAX_OPEN = timedelta(days=7)
RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
    maximum_attempts=5,
)
ROOM_TIMEOUT = timedelta(seconds=30)
POST_PROCESSING_TIMEOUT = timedelta(minutes=10)


def workflow_id_for(meeting_id: str) -> str:
    return f"meetinglifecycle-{meeting_id}"


@workflow.defn(name="MeetingLifecycleWorkflow")
class MeetingLifecycleWorkflow:
    def __init__(self) -> None:
        self._ended = False

    @workflow.signal(name=ENDED_SIGNAL)
    def meeting_ended(self) -> None:
        self._ended = True

    @workflow.query(name="ended")
    def ended(self) -> bool:
        return self._ended

    @workflow.run
    async def run(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        out: dict[str, Any] = {"meeting_id": meeting_id, "ended": False}
        args = [meeting_id, org_id]

        if not self._ended:
            try:
                out["room_open"] = await workflow.execute_activity(
                    "open_meeting_room",
                    args=args,
                    start_to_close_timeout=ROOM_TIMEOUT,
                    retry_policy=RETRY_POLICY,
                )
            except ActivityError as exc:
                workflow.logger.warning("open_meeting_room failed for %s: %s", meeting_id, exc)
                out["room_open_error"] = str(exc.cause or exc)

        try:
            await workflow.wait_condition(lambda: self._ended, timeout=MAX_OPEN)
            out["ended"] = True
        except TimeoutError:
            workflow.logger.info("meeting %s never ended within %s; closing", meeting_id, MAX_OPEN)

        if out["ended"]:
            try:
                out["post_meeting"] = await workflow.execute_activity(
                    "run_post_processing",
                    args=args,
                    start_to_close_timeout=POST_PROCESSING_TIMEOUT,
                    retry_policy=RETRY_POLICY,
                )
            except ActivityError as exc:
                workflow.logger.warning("run_post_processing failed for %s: %s", meeting_id, exc)
                out["post_meeting_error"] = str(exc.cause or exc)

        out["room_close"] = await workflow.execute_activity(
            "close_meeting_room",
            args=args,
            start_to_close_timeout=ROOM_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )
        return out
