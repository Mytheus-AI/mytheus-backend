"""meeting-service — LiveKit transport adapter: room-token mint, webhooks, server-side audio hook.

Owns: meeting_rooms, nexus-recordings object bucket. The SDK is behind a transport adapter so
the provider is swappable.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from libs.shared.app_factory import create_app
from services.meeting_service.routes import router as token_router
from services.meeting_service.webhook import router as webhook_router

app = create_app("meeting-service")
app.include_router(token_router)  # POST /api/meetings/:id/token
app.include_router(webhook_router)  # POST /livekit/webhook

# The `meeting-events` consumer (services/meeting_service/consumer.py) runs as its own worker
# process (`python -m services.meeting_service.consumer`), not inside this FastAPI app - same
# convention as `services/core_api/consumer.py`.


_inner_lifespan = app.router.lifespan_context


@asynccontextmanager
async def _lifespan(application: Any) -> AsyncIterator[Any]:
    # On shutdown, leave every room this process was tapping instead of waiting out LiveKit's
    # participant timeout. See room_lifecycle.py's STATE note.
    from services.meeting_service.room_lifecycle import TAPS

    async with _inner_lifespan(application) as state:
        try:
            yield state
        finally:
            await TAPS.stop_all()


app.router.lifespan_context = _lifespan
