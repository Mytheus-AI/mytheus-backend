"""ActionFollowUpWorkflow: chase a confirmed action until it is done (P12, plan 3.12).

Started by workflow-service's `actions` consumer on every `actions` record (which carries only
`confirmed`) with the id `actionfollowup-{action_id}` and `WorkflowIDReusePolicy.REJECT_DUPLICATE`,
so a redelivered or replayed `actions` record never starts a second chase.

Durable timers, not cron: it sleeps 1, 2, 4 and 3 days (cumulative day 1, 3, 7, 10). After each
sleep `read_action_status` asks core-api (`GET /internal/actions/{id}/status`) for the action's
current status and assignee; `done` or `dismissed` (from the UI or from the Jira sync) completes
the run early, and a 404 (the action is gone) completes it too. Days 1, 3 and 7 call
`remind_assignee`; day 10 calls `escalate_to_manager`.

All state is Temporal history, so a worker restart mid-timer replays history and resumes at the
same timer: nothing is dropped. Nothing is duplicated either: an activity is recorded as
completed in history once it returns, and if a worker dies after the activity's side effect but
before its completion reached Temporal (the activity then re-runs), the `notifications` outbox
row is keyed `notif:actionfollowup:{action_id}:day{n}:{channel}:{user_id}` and the outbox's
`(topic, idempotency_key)` unique index turns the re-run into a no-op.

A step whose status read keeps failing (core-api down past `STATUS_RETRY`'s window) is skipped
rather than failing the run, so a long outage costs at most that one reminder.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

# (sleep before the step in days, step kind, cumulative day)
SCHEDULE: tuple[tuple[int, str, int], ...] = (
    (1, "remind", 1),
    (2, "remind", 3),
    (4, "remind", 7),
    (3, "escalate", 10),
)
TERMINAL_STATUSES = frozenset({"done", "dismissed"})
STEP_TIMEOUT = timedelta(seconds=30)
STEP_WINDOW = timedelta(hours=1)
STATUS_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=5),
)


def workflow_id_for(action_id: str) -> str:
    return f"actionfollowup-{action_id}"


def _not_found(exc: ActivityError) -> bool:
    cause = exc.cause
    return isinstance(cause, ApplicationError) and cause.type == "HTTP404"


@workflow.defn(name="ActionFollowUpWorkflow")
class ActionFollowUpWorkflow:
    @workflow.run
    async def run(self, action_id: str, org_id: str, content: str = "") -> dict[str, Any]:
        out: dict[str, Any] = {"action_id": action_id, "steps": []}
        for gap_days, kind, day in SCHEDULE:
            await workflow.sleep(timedelta(days=gap_days))
            try:
                status: dict[str, Any] = await workflow.execute_activity(
                    "read_action_status",
                    args=[action_id, org_id],
                    start_to_close_timeout=STEP_TIMEOUT,
                    schedule_to_close_timeout=STEP_WINDOW,
                    retry_policy=STATUS_RETRY,
                )
            except ActivityError as exc:
                if _not_found(exc):
                    out["completed"] = "action_gone"
                    out["closed_on_day"] = day
                    return out
                workflow.logger.warning("status read failed for %s on day %d", action_id, day)
                out["steps"].append({"day": day, "kind": kind, "skipped": "status_unavailable"})
                continue
            if status.get("status") in TERMINAL_STATUSES:
                out["completed"] = status["status"]
                out["closed_on_day"] = day
                return out
            activity = "remind_assignee" if kind == "remind" else "escalate_to_manager"
            try:
                result: dict[str, Any] = await workflow.execute_activity(
                    activity,
                    args=[action_id, org_id, day, status, content],
                    start_to_close_timeout=STEP_TIMEOUT,
                    schedule_to_close_timeout=STEP_WINDOW,
                    retry_policy=STATUS_RETRY,
                )
            except ActivityError as exc:
                workflow.logger.warning("%s failed for %s: %s", activity, action_id, exc)
                result = {"error": str(exc.cause or exc)}
            out["steps"].append({"day": day, "kind": kind, **result})
        out["completed"] = "escalated"
        return out
