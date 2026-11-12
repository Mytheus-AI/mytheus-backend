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
# People structure: the org chart agents read (GetOrgContext) changing shape. An admin-only,
# company-wide edit, so who made it is a compliance fact like a membership change.
ACTION_TEAM_MEMBER_MOVED = "team.member_moved"
ACTION_TEAM_CREATED = "team.created"
ACTION_TEAM_UPDATED = "team.updated"
ACTION_TEAM_DELETED = "team.deleted"
ACTION_DEPARTMENT_CREATED = "department.created"
ACTION_DEPARTMENT_UPDATED = "department.updated"
ACTION_DEPARTMENT_DELETED = "department.deleted"
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

# Polish program, "Close-out ritual": the host opening or cancelling the 60-second wrap-up, and
# closing the meeting through it (who closed it, and who now owns the cards nobody confirmed).
ACTION_MEETING_WRAP_UP_STARTED = "meeting.wrap_up_started"
ACTION_MEETING_WRAP_UP_CANCELLED = "meeting.wrap_up_cancelled"
ACTION_MEETING_CLOSED = "meeting.closed"
# Polish program, "Agents that stand in properly": a participant asking the agent of someone who
# sent it instead of coming. Allowed or denied by the A2A PDP; either way it is a compliance fact.
ACTION_STAND_IN_ASKED = "stand_in.asked"
# Data erasure (Workspace > Data and privacy). core-api records its own share
# (`POST /api/v1/erasures`, row counts); admin-ops-service records the whole request once every
# leg has run (`POST /admin/tenant/erasures`), and that row is what the console lists.
ACTION_DATA_ERASURE_APPLIED = "data_erasure.applied"
ACTION_DATA_ERASURE_COMPLETED = "data_erasure.completed"
# Wave 3: one store outside core-api finished its share (admin-events `erasure_completed`,
# recorded by admin-ops-service's `admin-ops-service.erasure-status` consumer). `target_id` is
# the erasure's id, so the console lists a per-store checklist under each request.
ACTION_DATA_ERASURE_STORE_COMPLETED = "data_erasure.store_completed"
# Wave 3: an assignee (or the person who confirmed it, or an admin) reopening a done task.
ACTION_ACTION_REOPENED = "action.reopened"
# Wave 3: someone starting a phone recording of an in-person meeting, after confirming that
# everyone in the room knows (core-api `POST /api/v1/capture/sessions`).
ACTION_CAPTURE_STARTED = "capture.started"
# Polish wave 3, Off the record: a meeting paused every agent and the transcript for a stretch.
ACTION_MEETING_OFF_RECORD_STARTED = "meeting.off_record_started"
ACTION_MEETING_OFF_RECORD_ENDED = "meeting.off_record_ended"
# Polish wave 3, Recording playback: meeting-service minting a short-lived playback URL for a
# participant (who, which meeting, how many segments, for how long), a clip's video issued to a
# public clip share, and a meeting's recording objects deleted by a data erasure; core-api
# recording who made or removed a clip.
ACTION_RECORDING_PLAYBACK_ISSUED = "recording.playback_issued"
ACTION_RECORDING_CLIP_VIDEO_ISSUED = "recording.clip_video_issued"
ACTION_RECORDING_ERASED = "recording.erased"
ACTION_CLIP_CREATED = "clip.created"
ACTION_CLIP_DELETED = "clip.deleted"
# OAuth for the MCP server (Claude custom connectors; identity-tenant-service is the
# authorization server). A person approving or denying an app on the consent page; the token
# endpoint issuing (code redeemed), refreshing (rotation) and revoking (RFC 7009, settings, or a
# family killed because a used refresh token or code came back). Client REGISTRATION has no
# tenant, so it is recorded on the `oauth_clients` row itself (`registered_ip`, `created_at`)
# and in the service log, not here: `audit_logs` is tenant-scoped.
ACTION_OAUTH_AUTHORIZATION_GRANTED = "oauth.authorization_granted"
ACTION_OAUTH_AUTHORIZATION_DENIED = "oauth.authorization_denied"
ACTION_OAUTH_TOKEN_ISSUED = "oauth.token_issued"
ACTION_OAUTH_TOKEN_REFRESHED = "oauth.token_refreshed"
ACTION_OAUTH_TOKEN_REVOKED = "oauth.token_revoked"
ACTION_OAUTH_GRANT_REVOKED = "oauth.grant_revoked"
# Beyond, Layer 1 (decisions that know why, blast radius): the decision's owner adding, editing,
# removing or re-confirming a reason behind it, reopening the decision because a reason broke,
# booking a revisit; the two owners of clashing decisions settling it (a message, sharing their
# wording, keeping both, one replacing the other, accepting or declining that, booking a meeting).
ACTION_ASSUMPTION_ADDED = "assumption.added"
ACTION_ASSUMPTION_EDITED = "assumption.edited"
ACTION_ASSUMPTION_REMOVED = "assumption.removed"
ACTION_ASSUMPTION_RECONFIRMED = "assumption.reconfirmed"
ACTION_DECISION_REOPENED = "decision.reopened"
ACTION_REVISIT_BOOKED = "decision.revisit_booked"
ACTION_SETTLEMENT_MESSAGE = "settlement.message"
ACTION_SETTLEMENT_SHARED = "settlement.shared"
ACTION_SETTLEMENT_KEPT_BOTH = "settlement.kept_both"
ACTION_SETTLEMENT_PROPOSED = "settlement.proposed"
ACTION_SETTLEMENT_SUPERSEDED = "settlement.superseded"
ACTION_SETTLEMENT_DECLINED = "settlement.declined"
ACTION_SETTLEMENT_MEETING_BOOKED = "settlement.meeting_booked"

# ADR-0043: meeting invitations sent by email (an invitation, an update after a reschedule, or a
# cancellation). Who sent it and to whom is a compliance fact: it sends meeting details outside
# the product.
ACTION_MEETING_INVITATIONS_SENT = "meeting.invitations_sent"
