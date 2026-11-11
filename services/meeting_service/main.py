"""meeting-service — LiveKit transport adapter: room-token mint, webhooks, server-side audio hook.

Owns: meeting_rooms, mytheus-recordings object bucket. The SDK is behind a transport adapter so
the provider is swappable.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from libs.shared.app_factory import create_app
from services.meeting_service.audio_tap_worker import audio_tap_mode
from services.meeting_service.internal import router as internal_router
from services.meeting_service.recordings import internal_router as recording_internal_router
from services.meeting_service.recordings import router as recording_router
from services.meeting_service.routes import router as token_router
from services.meeting_service.webhook import router as webhook_router

# Fail at boot, not on the first webhook, if AUDIO_TAP_MODE is misspelled.
audio_tap_mode()

app = create_app("meeting-service")
app.include_router(token_router)  # POST /api/meetings/:id/token
app.include_router(webhook_router)  # POST /livekit/webhook
# POST /internal/meetings/{id}/room/open|close (workflow-service only, never routed publicly)
app.include_router(internal_router)
# GET /api/meetings/{id}/recording (participants: playback status + presigned segment URLs)
app.include_router(recording_router)
# POST /internal/meetings/{id}/recording/pause|resume|erase, .../clips/{clip_id}/video (core-api)
app.include_router(recording_internal_router)

# The `meeting-events` consumer (services/meeting_service/consumer.py) runs as its own worker
# process (`python -m services.meeting_service.consumer`), not inside this FastAPI app - same
# convention as `services/core_api/consumer.py`. Likewise the audio tap worker
# (`python -m services.meeting_service.audio_tap_worker`) when AUDIO_TAP_MODE=worker.


_inner_lifespan = app.router.lifespan_context


@asynccontextmanager
async def _lifespan(application: Any) -> AsyncIterator[Any]:
    # On shutdown, leave every room this process was tapping instead of waiting out LiveKit's
    # participant timeout (in-process mode only; empty otherwise). See room_lifecycle.py.
    from services.meeting_service.room_lifecycle import TAPS

    async with _inner_lifespan(application) as state:
        try:
            yield state
        finally:
            await TAPS.stop_all()


app.router.lifespan_context = _lifespan
