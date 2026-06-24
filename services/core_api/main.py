"""Core API — the relational heart and domain CRUD service.

Owns: meetings, participants, speakers, worklogs, decisions, action_items,
      agent_suggestions, agent_collaborations, chat_messages, teams, team_members, departments.

Confirmation rule: decisions/actions events are ONLY emitted by this service's
transactional outbox, ONLY on POST /decisions/:id/confirm or /actions/:id/confirm.
Agents surface candidates via agent-results; they never write here directly.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.core_api.routes import (
    meetings,
    decisions,
    actions,
    worklogs,
    chat,
    teams,
    departments,
    suggestions,
)

app = FastAPI(title="core-api", version="1.0.0")
setup_telemetry(app, service_name="core-api")
add_exception_handlers(app)

app.include_router(meetings.router, prefix="/meetings")
app.include_router(decisions.router, prefix="/decisions")
app.include_router(actions.router, prefix="/actions")
app.include_router(worklogs.router, prefix="/worklogs")
app.include_router(chat.router, prefix="/chat")
app.include_router(teams.router, prefix="/teams")
app.include_router(departments.router, prefix="/departments")
app.include_router(suggestions.router, prefix="/suggestions")
