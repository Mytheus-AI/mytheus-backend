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

app = FastAPI(title="core-api", version="1.0.0")
setup_telemetry(app, service_name="core-api")
add_exception_handlers(app)

# TODO: add routers for /meetings /decisions /actions /worklogs /chat /teams /departments /suggestions
