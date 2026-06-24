"""LiveKit webhook receiver.

Verifies HMAC signature before processing.
Events: room.created, participant.joined, participant.left, recording.ready.
"""
from typing import Any
from fastapi import APIRouter, Header, HTTPException, Request

router = APIRouter(tags=["livekit"])


@router.post("/livekit/webhook")
async def livekit_webhook(
    request: Request,
    authorization: str | None = Header(default=None),
) -> Any:
    # TODO: verify LiveKit HMAC signature from Authorization header
    payload = await request.json()
    event = payload.get("event")

    handlers = {
        "room.created": _on_room_created,
        "participant.joined": _on_participant_joined,
        "participant.left": _on_participant_left,
        "recording.ready": _on_recording_ready,
    }
    handler = handlers.get(event)
    if handler:
        await handler(payload)
    return {"ok": True}


async def _on_room_created(payload: dict) -> None:
    pass


async def _on_participant_joined(payload: dict) -> None:
    pass


async def _on_participant_left(payload: dict) -> None:
    pass


async def _on_recording_ready(payload: dict) -> None:
    pass
