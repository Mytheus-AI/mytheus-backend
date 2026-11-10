"""workflow-service's `actions` handler: start ActionFollowUpWorkflow (P12, plan 3.12).

`actions` carries only user-confirmed facts (`ActionStatus` is `{confirmed}`; core-api's outbox
emits it on confirm, idempotency key `action:{action_id}:confirmed`). Each record starts
`actionfollowup-{action_id}` with `REJECT_DUPLICATE`: a redelivered or replayed record raises
`WorkflowAlreadyStartedError`, which is the dedupe (Temporal owns the state; this service owns
no table). A run that already finished is never started again either.

A record that can never apply (no header org, a non-UUID id, a status other than `confirmed`)
is poison and is dead-lettered to `actions.dlq` by the consumer harness in `consumer.py`, which
runs this handler alongside the `meeting-events` one in the same process and consumer group.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Literal

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from services.workflow_service.workflows.action_followup import (
    ActionFollowUpWorkflow,
    workflow_id_for,
)

logger = logging.getLogger(__name__)

TOPIC = "actions"


class PoisonAction(Exception):
    """The `actions` record can never be applied; route it to `actions.dlq`."""


def _enum(value: Any) -> str:
    # The Avro deserializer returns enum symbols as plain strings; tolerate a wrapped form.
    return str(value.get("string") if isinstance(value, dict) else value)


def _parse(value: dict[str, Any]) -> tuple[str, str, str]:
    header = value.get("header") or {}
    try:
        org_id = str(uuid.UUID(str(header.get("org_id"))))
        action_id = str(uuid.UUID(str(value.get("action_id"))))
    except ValueError as exc:
        raise PoisonAction(f"actions org_id/action_id is not a UUID: {value!r}") from exc
    status = _enum(value.get("status"))
    if status != "confirmed":
        raise PoisonAction(f"actions status is not confirmed ({status!r})")
    content = value.get("content")
    return org_id, action_id, content if isinstance(content, str) else ""


async def handle_action_event(
    client: Client, value: dict[str, Any], *, task_queue: str
) -> Literal["started", "duplicate"]:
    org_id, action_id, content = _parse(value)
    try:
        await client.start_workflow(
            ActionFollowUpWorkflow.run,
            args=[action_id, org_id, content],
            id=workflow_id_for(action_id),
            task_queue=task_queue,
            id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
        )
    except WorkflowAlreadyStartedError:
        logger.info("actions: %s already has a follow-up run", action_id)
        return "duplicate"
    logger.info("actions: started %s", workflow_id_for(action_id))
    return "started"
