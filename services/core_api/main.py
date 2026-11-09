"""core-api - the relational heart and domain CRUD service.

Owns: meetings, participants, speakers, worklogs, decisions, action_items, agent_suggestions,
agent_collaborations, chat_messages, teams, team_members, departments.

Confirmation rule: decisions/actions events are ONLY emitted by this service's transactional
outbox, ONLY on POST /decisions/:id/confirm or /actions/:id/confirm. Neither route exists yet,
and neither topic is wired, which is deliberate: there is no confirmation flow to produce them
and a speculative producer here would put unconfirmed edges into the knowledge graph.
"""

from libs.shared.app_factory import create_app
from services.core_api.routes import (
    chat,
    internal_meetings,
    meetings,
    my_tasks,
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
app.include_router(internal_meetings.router)
app.include_router(my_tasks.router)

# My Tasks (GET /api/v1/actions?assignee_id=me, POST /api/v1/actions/{id}/done) is in my_tasks.
# The confirm/edit/dismiss routes for /decisions and /actions (the only producers of those
# topics) and /suggestions land with the Plan 8 confirmation work, in their own module.
