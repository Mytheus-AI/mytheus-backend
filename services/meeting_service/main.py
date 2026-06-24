"""Meeting service — LiveKit transport adapter.

Owns: meeting_rooms, nexus-recordings object bucket.
SDK abstracted behind a transport adapter so the provider is swappable.
"""
from fastapi import FastAPI

from libs.shared.errors import add_exception_handlers
from libs.shared.telemetry import setup_telemetry
from services.meeting_service.routes import rooms, livekit_webhook

app = FastAPI(title="meeting-service", version="1.0.0")
setup_telemetry(app, service_name="meeting-service")
add_exception_handlers(app)

app.include_router(rooms.router, prefix="/api/meetings")
app.include_router(livekit_webhook.router)
