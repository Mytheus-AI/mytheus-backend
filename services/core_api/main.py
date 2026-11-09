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
    candidates,
    chat,
    internal_meetings,
    meetings,
    org_hierarchy,
    speakers,
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
app.include_router(candidates.router)
app.include_router(internal_meetings.router)

# TODO: /suggestions lands with P9; GET /actions (My Tasks) with P8 Part B.
