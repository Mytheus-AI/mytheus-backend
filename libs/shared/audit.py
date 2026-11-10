"""The one way every service writes to `audit_logs`.

Shared so the audit trail has one shape across nine services rather than nine dialects of
`action` strings, and so the fail-closed rule below is implemented once.

Fail-closed rule (`04-service-catalog.md` section 3.9). An operation that REQUIRES audit must
fail if the audit write fails. That is why `audit` takes the caller's session and writes
inside the caller's transaction rather than opening its own: the audit row and the change it
describes commit together or not at all. A helper that opened its own connection could leave
a committed action with no audit record, which is the one outcome the table exists to prevent.

Do not add a `best_effort=True` escape hatch. The next person to hit a failing audit write
under deadline will use it, and the trail will have a hole precisely where something went
wrong.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Frozen vocabulary (canon `_canon/02-data-model.md`). `agent` is here from the start even
# though no agent writes yet: the AI tier acts on a user's behalf in a later band, and an
# audit trail that cannot distinguish "the user did this" from "an agent did this on their
# behalf" cannot answer the question an auditor actually asks.
ACTOR_TYPES = ("user", "agent", "system")

_INSERT = text(
    "INSERT INTO audit_logs "
    "(org_id, actor_id, actor_type, action, target_type, target_id, metadata, ip) "
    "VALUES (:org_id, :actor_id, :actor_type, :action, :target_type, :target_id, "
    "CAST(:metadata AS jsonb), CAST(:ip AS inet))"
)


async def audit(
    session: AsyncSession,
    *,
    org_id: uuid.UUID,
    action: str,
    actor_id: uuid.UUID | None = None,
    actor_type: str = "user",
    target_type: str | None = None,
    target_id: Any = None,
    metadata: dict[str, Any] | None = None,
    ip: str | None = None,
) -> None:
    """Append one audit row inside the CALLER'S transaction.

    `session` must already have `app.current_org` set for `org_id`: the table is
    RLS-policied, so a mismatch is refused by the WITH CHECK clause rather than silently
    written to the wrong tenant.
    """
    if actor_type not in ACTOR_TYPES:
        raise ValueError(f"actor_type must be one of {ACTOR_TYPES}, got {actor_type!r}")
    if not action:
        raise ValueError("action is required: an audit row with no action records nothing")

    import json

    await session.execute(
        _INSERT,
        {
            "org_id": str(org_id),
            "actor_id": str(actor_id) if actor_id else None,
            "actor_type": actor_type,
            "action": action,
            "target_type": target_type,
            "target_id": str(target_id) if target_id is not None else None,
            "metadata": json.dumps(metadata or {}),
            "ip": ip,
        },
    )


# --- The action vocabulary -------------------------------------------------------------
#
# Constants rather than free strings, because these are queried by compliance reports and a
# typo produces a silently empty report rather than an error. Extend deliberately.

ACTION_AUTHZ_DENY = "authz.deny"
ACTION_ORG_PROVISIONED = "org.provisioned"
ACTION_USER_SYNCED = "user.synced"
ACTION_USER_PROFILE_UPDATED = "user.profile_updated"
ACTION_MEETING_CREATED = "meeting.created"
ACTION_MEETING_UPDATED = "meeting.updated"
ACTION_MEETING_PARTICIPANTS_ADDED = "meeting.participants_added"
# Membership decides what `get_user_context` reports for a user, which downstream bands use to
# scope agents and worklog rollups, so who changed it is a compliance fact rather than a log line.
ACTION_TEAM_MEMBER_ADDED = "team.member_added"
ACTION_TEAM_MEMBER_REMOVED = "team.member_removed"
# An RBAC tier change is a privilege change: `PUT/DELETE /rbac/grants/{user_id}`.
ACTION_RBAC_GRANTED = "rbac.granted"
ACTION_RBAC_REVOKED = "rbac.revoked"
# Phase 8 Part A: a participant confirming or dismissing an agent candidate. Confirm is what
# puts a decision/action into the knowledge graph, so who pressed it is a compliance fact.
ACTION_DECISION_CONFIRMED = "decision.confirmed"
ACTION_DECISION_DISMISSED = "decision.dismissed"
ACTION_ACTION_CONFIRMED = "action.confirmed"
ACTION_ACTION_DISMISSED = "action.dismissed"
# My Tasks: an assignee (or an admin) closing a confirmed action item (ADR-0031 D3).
ACTION_ACTION_COMPLETED = "action.completed"
# P9: the owner acting on a QuestionAgent suggestion (ADR-0034 D7). No event is published; the
# audit row is the only durable trace of who asked, saved or dismissed what.
ACTION_SUGGESTION_ASKED = "suggestion.asked"
ACTION_SUGGESTION_DISMISSED = "suggestion.dismissed"
ACTION_SUGGESTION_SAVED = "suggestion.saved"
# P10 (ADR-0013): an admin granting or revoking an A2A grant in `agent_authz_scopes`
# (actor_type user), and core-api logging each agent-to-agent request (actor_type agent):
# allowed (answered / no_answer / deadline_miss / rag_*), denied by the PDP (authz_denied), or
# blocked as a tenant-boundary violation (org_spoof / foreign_org). `context_fill.dismissed` is
# the requester dismissing a ContextGapAgent fill card.
ACTION_A2A_GRANT_CREATE = "a2a.grant.create"
ACTION_A2A_GRANT_REVOKE = "a2a.grant.revoke"
ACTION_A2A_REQUEST_ALLOWED = "a2a.request.allowed"
ACTION_A2A_REQUEST_DENIED = "a2a.request.denied"
ACTION_A2A_REQUEST_BLOCKED = "a2a.request.blocked"
ACTION_CONTEXT_FILL_DISMISSED = "context_fill.dismissed"

# P12: integration-service connection lifecycle, and core-api applying a Jira status change to
# `action_items.status` (`POST /internal/actions/{id}/external-status`).
ACTION_INTEGRATION_CONNECTED = "integration.connected"
ACTION_INTEGRATION_DISCONNECTED = "integration.disconnected"
ACTION_ACTION_STATUS_SYNCED = "action.status_synced"

# Phase 4 reach: a person exporting a decision record to Notion, Confluence or Slack
# (integration-service `POST /integrations/exports/decisions/{id}`).
ACTION_DECISION_EXPORTED = "decision.exported"

# ADR-0039 B1: an owner/admin changing a meeting type, which is company-wide data.
ACTION_MEETING_TEMPLATE_CREATED = "meeting_template.created"
ACTION_MEETING_TEMPLATE_UPDATED = "meeting_template.updated"
ACTION_MEETING_TEMPLATE_DELETED = "meeting_template.deleted"

# Phase 4: a meeting follow-up dispatched (the recap to participants in-app, by email to those
# who opted in, and to the host's listed external guests). Who triggered it and who was mailed
# is a compliance fact: it sends meeting content outside the product.
ACTION_MEETING_FOLLOWUP_SENT = "meeting.followup_sent"

# Phase 4: personal access tokens (identity-tenant-service /api/v1/api-tokens), the actions
# a person takes through one (core-api `POST /api/v1/actions`), and share links (core-api).
ACTION_API_TOKEN_CREATED = "api_token.created"
ACTION_API_TOKEN_REVOKED = "api_token.revoked"
ACTION_ACTION_CREATED = "action.created"
ACTION_SHARE_LINK_CREATED = "share_link.created"
ACTION_SHARE_LINK_REVOKED = "share_link.revoked"
