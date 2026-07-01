"""core-api — the relational heart and domain CRUD service.

Owns: meetings, participants, speakers, worklogs, decisions, action_items, agent_suggestions,
agent_collaborations, chat_messages, teams, team_members, departments.

Confirmation rule: decisions/actions events are ONLY emitted by this service's transactional
outbox, ONLY on POST /decisions/:id/confirm or /actions/:id/confirm.
"""

from libs.shared.app_factory import create_app

app = create_app("core-api")

# TODO: add routers for /meetings /decisions /actions /worklogs
#       /chat /teams /departments /suggestions
