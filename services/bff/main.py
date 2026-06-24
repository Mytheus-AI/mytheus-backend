"""Backend-for-frontend service.

Screen-shaped read aggregation. No domain ownership; no data written here.
The only caller of /bff/* is reprise-ui.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.bff.routes import meetings, decisions, actions, notifications

app = FastAPI(title="bff", version="1.0.0")
setup_telemetry(app, service_name="bff")
add_exception_handlers(app)

app.include_router(meetings.router, prefix="/bff")
app.include_router(decisions.router, prefix="/bff")
app.include_router(actions.router, prefix="/bff")
app.include_router(notifications.router, prefix="/bff")
