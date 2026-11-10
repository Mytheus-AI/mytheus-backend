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
2. Wait for `meeting_ended`, at most `MAX_OPEN` past the meeting's scheduled start (from the
   open response; `meeting-events` does not carry it), or past the run's start when there is no
   scheduled start or it is already past. `meeting-events` has no `cancelled` type (a cancel is
   an `updated`), so without a bound a cancelled meeting would leave an execution open forever.
   Counting from the scheduled start is what keeps a meeting booked weeks ahead from being
   closed before it happens.
3. `run_post_processing` (only when ended). A failure is recorded and the room is still closed.
4. `close_meeting_room`. A failure here fails the run: it is the one step nothing else retries.
5. When the bound expired instead (step 2), the room is closed and the run then FAILS with
   `MeetingNeverEnded`. Failing is the point: the consumer signal-with-starts `ended` with
   `ALLOW_DUPLICATE_FAILED_ONLY`, so a meeting that does end later (rescheduled, or simply
   longer than anyone expected) gets a fresh run that does its post-processing, while a run
   that completed normally can never be started again by a replayed `ended`.

All workflow state is in Temporal history, so the run survives a worker restart: the next worker
replays history and resumes waiting at step 2.

P12 adds two Slack touches (integration-service builds the Block Kit and emits the
`notifications`; it answers `{sent: 0}` when the tenant has no Slack, so both are no-ops then):

- Prep brief at `PREP_BRIEF_LEAD` (15 minutes) before the scheduled start: step 2's wait first
  runs until that instant, then `send_prep_brief` fires once, then the wait continues to the
  same `MAX_OPEN` bound. A meeting created inside the window gets its brief at once; one with no
  scheduled start, or whose start has passed, or that ends first, gets none.
- Meeting summary on `ended`, after post-processing: `send_meeting_summary`.

Neither may fail the run (a failure is recorded in the output, like post-processing). Both are
behind `workflow.patched` markers, so executions started before this change replay unchanged.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

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
SLACK_TIMEOUT = timedelta(seconds=30)
PREP_BRIEF_LEAD = timedelta(minutes=15)
PREP_BRIEF_PATCH = "p12-prep-brief"
SUMMARY_PATCH = "p12-meeting-summary"


NEVER_ENDED = "MeetingNeverEnded"


def workflow_id_for(meeting_id: str) -> str:
    return f"meetinglifecycle-{meeting_id}"


def parse_start(scheduled_start: object) -> datetime | None:
    """The scheduled start as an aware datetime (naive = UTC), or None."""
    if not isinstance(scheduled_start, str):
        return None
    try:
        parsed = datetime.fromisoformat(scheduled_start)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def prep_brief_delay(now: datetime, scheduled_start: object) -> timedelta | None:
    """How long to wait before the prep brief: None when there is no future start, zero when
    the start is less than `PREP_BRIEF_LEAD` away. Pure, so the workflow stays deterministic."""
    start = parse_start(scheduled_start)
    if start is None or start <= now:
        return None
    return max(start - PREP_BRIEF_LEAD - now, timedelta(0))


def wait_bound(now: datetime, scheduled_start: object) -> timedelta:
    """How long to wait for `ended`: `MAX_OPEN` past the scheduled start, or past `now` when
    there is none (or it has already passed). Pure, so the workflow stays deterministic."""
    start = parse_start(scheduled_start)
    anchor = max(now, start) if start is not None else now
    return anchor + MAX_OPEN - now


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

    async def _slack(self, activity: str, args: list[str], out: dict[str, Any], key: str) -> None:
        """One Slack touch; a failure is recorded under `{key}_error` and never fails the run."""
        try:
            out[key] = await workflow.execute_activity(
                activity,
                args=args,
                start_to_close_timeout=SLACK_TIMEOUT,
                retry_policy=RETRY_POLICY,
            )
        except ActivityError as exc:
            workflow.logger.warning("%s failed for %s: %s", activity, args[0], exc)
            out[f"{key}_error"] = str(exc.cause or exc)

    @workflow.run
    async def run(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        out: dict[str, Any] = {"meeting_id": meeting_id, "ended": False}
        args = [meeting_id, org_id]

        scheduled_start: object = None
        if not self._ended:
            try:
                out["room_open"] = await workflow.execute_activity(
                    "open_meeting_room",
                    args=args,
                    start_to_close_timeout=ROOM_TIMEOUT,
                    retry_policy=RETRY_POLICY,
                )
                scheduled_start = out["room_open"].get("scheduled_start")
            except ActivityError as exc:
                workflow.logger.warning("open_meeting_room failed for %s: %s", meeting_id, exc)
                out["room_open_error"] = str(exc.cause or exc)

        prep_delay = prep_brief_delay(workflow.now(), scheduled_start)
        if prep_delay is not None and not self._ended and workflow.patched(PREP_BRIEF_PATCH):
            if prep_delay > timedelta(0):
                try:
                    await workflow.wait_condition(lambda: self._ended, timeout=prep_delay)
                except TimeoutError:
                    pass
            if not self._ended:
                await self._slack("send_prep_brief", args, out, "prep_brief")

        bound = wait_bound(workflow.now(), scheduled_start)
        try:
            await workflow.wait_condition(lambda: self._ended, timeout=bound)
            out["ended"] = True
        except TimeoutError:
            workflow.logger.info("meeting %s never ended within %s; closing", meeting_id, bound)

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
            if workflow.patched(SUMMARY_PATCH):
                await self._slack("send_meeting_summary", args, out, "meeting_summary")

        out["room_close"] = await workflow.execute_activity(
            "close_meeting_room",
            args=args,
            start_to_close_timeout=ROOM_TIMEOUT,
            retry_policy=RETRY_POLICY,
        )
        if not out["ended"]:
            raise ApplicationError(
                f"meeting {meeting_id} did not end within {bound}; room closed, no "
                "post-processing. A later meeting-events ended starts a fresh run.",
                out,
                type=NEVER_ENDED,
                non_retryable=True,
            )
        return out
