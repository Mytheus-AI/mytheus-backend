"""core-api - the relational heart and domain CRUD service.

Owns: meetings, participants, speakers, worklogs, decisions, action_items, agent_suggestions,
agent_collaborations, chat_messages, teams, team_members, departments.

Confirmation rule: decisions/actions events are ONLY emitted by this service's transactional
outbox, ONLY on POST /decisions/:id/confirm or /actions/:id/confirm (`routes/candidates.py`,
Plan 7-9 section 3.6). The agent-results consumer stores candidates as `proposed` rows and
never produces either topic.
"""

from libs.shared.app_factory import create_app
from services.core_api.routes import (
    action_create,
    candidates,
    chat,
    collaborations,
    commitments,
    decision_log,
    followups,
    internal_agents,
    internal_integrations,
    internal_lifecycle,
    internal_meetings,
    internal_policy,
    internal_reach,
    internal_retrieval,
    meeting_templates,
    meetings,
    my_tasks,
    org_hierarchy,
    public_api,
    room_closeout,
    share_links,
    speakers,
    stand_ins,
    status_requests,
    suggestions,
    transcript,
    worklogs,
)

app = create_app("core-api")
app.include_router(meetings.router)
app.include_router(org_hierarchy.router)
app.include_router(chat.router)
app.include_router(speakers.router)
app.include_router(transcript.router)
app.include_router(worklogs.router)
app.include_router(decision_log.router)
app.include_router(candidates.router)
app.include_router(internal_meetings.router)
app.include_router(internal_retrieval.router)
app.include_router(my_tasks.router)
app.include_router(suggestions.router)
app.include_router(collaborations.router)
app.include_router(internal_agents.router)
app.include_router(internal_policy.router)
app.include_router(internal_lifecycle.router)
app.include_router(internal_integrations.router)
app.include_router(meeting_templates.router)
app.include_router(commitments.router)
app.include_router(status_requests.router)
# Phase 4 reach: tasks created directly, share links (and their public read), the meeting
# follow-up, and the internal reads integration-service builds CRM notes and exports from.
app.include_router(action_create.router)
app.include_router(public_api.router)
app.include_router(share_links.router)
app.include_router(share_links.public_router)
app.include_router(followups.router)
app.include_router(internal_reach.router)
# Polish program, Phase 1: the room's close-out ritual and late-joiner line (room_closeout),
# and asking a stand-in agent (stand_ins).
app.include_router(room_closeout.router)
app.include_router(stand_ins.router)

# Confirm/edit/dismiss for /decisions and /actions (the only producers of those topics) are in
# candidates; My Tasks (GET /api/v1/actions?assignee_id=me, POST /api/v1/actions/{id}/done) is in
# my_tasks. /suggestions (QuestionAgent, P9, owner-only, ADR-0034) is in suggestions.
# /meetings/{id}/collaborations, /meetings/{id}/context-fills and /context-fills/{id}/dismiss
# (ContextGapAgent, P10) are in collaborations; the internal related-attendees read
# (agent-runtime-service) is in internal_agents; the post-meeting digest (workflow-service) is in
# internal_lifecycle.
# ADR-0039 Part B: meeting types (meeting_templates), promises (commitments), status requests
# (status_requests; the workflow-service side is in internal_lifecycle); the drafted agenda and
# attendance are in meetings.
