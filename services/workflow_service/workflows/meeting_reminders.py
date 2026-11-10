"""MeetingRemindersWorkflow: the meeting timers of ADR-0039 B9, on durable timers, not cron.

A sibling of MeetingLifecycleWorkflow with the deterministic id `meetingreminders-{meeting_id}`,
started (`REJECT_DUPLICATE`) by workflow-service's `meeting-events` consumer on `created`, next to
the lifecycle run, and signalled by the same consumer: `meeting_updated` on every `updated`
(a reschedule or a cancel is an `updated`) and `meeting_ended` on `ended`.

Timers, each relative to the scheduled start the workflow last read from core-api
(`read_meeting_reminders`, `GET /internal/meetings/{id}/reminders`):

- start minus 60 minutes, only with a `previous_meeting_id`: `send_status_requests` (core-api
  creates one request per open action of the previous meeting; one `in_app` notification each,
  "Quick status on {action}?", link `/status-requests`);
- start minus 10 minutes: `send_briefs` (one per live participant, "Your brief for {title}",
  link `/meetings/{id}#brief`);
- 5 minutes after `ended`: `send_catchups` (one per participant whose attendance was `agent`,
  "Your agent's catch-up from {title}", link `/meetings/{id}/catch-up`).

Rescheduled or cancelled before a timer fires: no notification for it. The wait for each timer is
interrupted by `meeting_updated`, after which the schedule is read again and the timers re-planned
from the new start (a cancelled meeting ends the run, nothing sent). The schedule is also read
again when a timer fires, so a change whose `updated` was lost still cancels the old timer: if the
start moved, the old timer sends nothing and the run re-plans. A timer already due when it is
planned (a meeting created 30 minutes before its start) fires at once while the start is still
ahead; once the start has passed it is skipped. Each timer fires at most once per scheduled start.

Bound: a meeting that never ends stops being waited on `MAX_OPEN` (7 days) past its scheduled
start (or past now without one), like MeetingLifecycleWorkflow, and the run completes.

All state is Temporal history: a worker restart replays and resumes on the same timer. The
activities are idempotent (outbox keys, see `reminder_activities.py`), so a re-run activity sends
nothing twice.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

from services.workflow_service.workflows.meeting_lifecycle import MAX_OPEN, parse_start

UPDATED_SIGNAL = "meeting_updated"
ENDED_SIGNAL = "meeting_ended"
STATUS_REQUEST_LEAD = timedelta(minutes=60)
BRIEF_LEAD = timedelta(minutes=10)
CATCH_UP_DELAY = timedelta(minutes=5)
STEP_TIMEOUT = timedelta(seconds=30)
STEP_WINDOW = timedelta(minutes=10)
RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=1),
)


def workflow_id_for(meeting_id: str) -> str:
    return f"meetingreminders-{meeting_id}"


def next_timer(
    state: dict[str, Any], now: datetime, done: set[str]
) -> tuple[str, datetime, str] | None:
    """The next timer to wait for as `(kind, fire_at, start_iso)`, or None. Pure, so the
    workflow stays deterministic. A timer whose start has already passed is never next."""
    start = parse_start(state.get("scheduled_start"))
    if start is None or start <= now:
        return None
    start_iso = start.isoformat()
    plans: list[tuple[str, datetime, str]] = []
    if state.get("previous_meeting_id") and f"status_requests@{start_iso}" not in done:
        plans.append(("status_requests", start - STATUS_REQUEST_LEAD, start_iso))
    if f"brief@{start_iso}" not in done:
        plans.append(("brief", start - BRIEF_LEAD, start_iso))
    return min(plans, key=lambda p: p[1]) if plans else None


@workflow.defn(name="MeetingRemindersWorkflow")
class MeetingRemindersWorkflow:
    def __init__(self) -> None:
        self._updated = False
        self._ended = False

    @workflow.signal(name=UPDATED_SIGNAL)
    def meeting_updated(self) -> None:
        self._updated = True

    @workflow.signal(name=ENDED_SIGNAL)
    def meeting_ended(self) -> None:
        self._ended = True

    async def _read(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        # No schedule-to-close bound: while core-api is down the read keeps retrying (backing off
        # to a minute), so an outage delays a reminder instead of losing the run.
        state: dict[str, Any] = await workflow.execute_activity(
            "read_meeting_reminders",
            args=[meeting_id, org_id],
            start_to_close_timeout=STEP_TIMEOUT,
            retry_policy=RETRY,
        )
        return state

    async def _send(self, activity: str, args: list[str]) -> dict[str, Any]:
        """One notification step; a failure is recorded, never fails the run."""
        try:
            result: dict[str, Any] = await workflow.execute_activity(
                activity,
                args=args,
                start_to_close_timeout=STEP_TIMEOUT,
                schedule_to_close_timeout=STEP_WINDOW,
                retry_policy=RETRY,
            )
        except ActivityError as exc:
            workflow.logger.warning("%s failed for %s: %s", activity, args[0], exc)
            return {"error": str(exc.cause or exc)}
        return result

    async def _wait(self, timeout: timedelta) -> bool:
        """Sleep on a durable timer; True if it fired, False if a signal woke it first."""
        if timeout <= timedelta(0):
            return not (self._updated or self._ended)
        try:
            await workflow.wait_condition(lambda: self._updated or self._ended, timeout=timeout)
        except TimeoutError:
            return True
        return False

    @workflow.run
    async def run(self, meeting_id: str, org_id: str) -> dict[str, Any]:
        out: dict[str, Any] = {"meeting_id": meeting_id, "sent": [], "skipped": []}
        done: set[str] = set()
        while not self._ended:
            self._updated = False
            state = await self._read(meeting_id, org_id)
            if state.get("gone") or state.get("status") == "cancelled":
                out["completed"] = "gone" if state.get("gone") else "cancelled"
                return out
            if state.get("status") == "ended":
                break
            now = workflow.now()
            plan = next_timer(state, now, done)
            if plan is None:
                start = parse_start(state.get("scheduled_start"))
                anchor = max(now, start) if start is not None else now
                if await self._wait(anchor + MAX_OPEN - now):
                    out["completed"] = "never_ended"
                    return out
                continue
            kind, fire_at, start_iso = plan
            if not await self._wait(fire_at - now):
                continue  # rescheduled, cancelled or ended: read again before anything fires
            fresh = await self._read(meeting_id, org_id)
            if fresh.get("gone") or fresh.get("status") == "cancelled":
                out["completed"] = "gone" if fresh.get("gone") else "cancelled"
                return out
            if fresh.get("status") == "ended":
                break
            start = parse_start(fresh.get("scheduled_start"))
            if start is None or start.isoformat() != start_iso:
                out["skipped"].append({"kind": kind, "scheduled_start": start_iso})
                continue  # moved: the old timer sends nothing; re-plan from the new start
            done.add(f"{kind}@{start_iso}")
            activity = "send_status_requests" if kind == "status_requests" else "send_briefs"
            result = await self._send(activity, [meeting_id, org_id, start_iso])
            out["sent"].append({"kind": kind, "scheduled_start": start_iso, **result})

        await workflow.sleep(CATCH_UP_DELAY)
        result = await self._send("send_catchups", [meeting_id, org_id])
        out["sent"].append({"kind": "catch_up", **result})
        out["completed"] = "ended"
        return out
