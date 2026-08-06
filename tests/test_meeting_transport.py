"""`FakeTransport` behavior (Task 2, Deviation 1).

No LiveKit account exists for this project, so `LiveKitTransport` cannot be exercised
end-to-end here — these tests cover `FakeTransport` only, and none of them are a substitute
for a real-LiveKit test. Protocol conformance for both classes is asserted statically by
`services/meeting_service/transport.py`'s `_assert_conforms` calls (checked by `mypy`), which
is what the acceptance check "both classes satisfy MeetingTransport" actually rests on, not
anything here.
"""

from __future__ import annotations

import json
import uuid

from services.meeting_service.transport import FakeTransport


async def test_fake_transport_mints_a_structurally_valid_unsigned_token():
    fake = FakeTransport()
    org_id, meeting_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    token = await fake.mint_join_token(
        org_id=org_id,
        meeting_id=meeting_id,
        user_id=user_id,
        room="room-1",
        role_in_meeting="host",
    )

    # Structurally a JWT: three dot-separated segments, unsigned by construction.
    parts = token.split(".")
    assert len(parts) == 3
    assert parts[0] == "fake"
    assert parts[2] == "unsigned"


async def test_fake_transport_records_calls_for_assertion():
    fake = FakeTransport()
    org_id, meeting_id, user_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

    await fake.mint_join_token(
        org_id=org_id,
        meeting_id=meeting_id,
        user_id=user_id,
        room="room-1",
        role_in_meeting="participant",
        ttl_seconds=120,
    )

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call.method == "mint_join_token"
    assert call.kwargs == {
        "org_id": org_id,
        "meeting_id": meeting_id,
        "user_id": user_id,
        "room": "room-1",
        "role_in_meeting": "participant",
        "ttl_seconds": 120,
    }


async def test_fake_transport_verify_webhook_returns_configured_result():
    sentinel = object()
    fake = FakeTransport(verify_webhook_result=sentinel)

    result = fake.verify_webhook(body=b"{}", auth_header="sig")

    assert result is sentinel
    assert fake.calls[-1].method == "verify_webhook"


async def test_fake_transport_verify_webhook_raises_configured_error():
    fake = FakeTransport(verify_webhook_error=ValueError("bad signature"))

    try:
        fake.verify_webhook(body=b"{}", auth_header="sig")
        raised = False
    except ValueError:
        raised = True

    assert raised, "a configured verify_webhook_error must propagate"


async def test_fake_transport_ensure_room_records_the_internal_org_id_as_room_metadata():
    """Room-metadata follow-up: `ensure_room` must write the INTERNAL org_id (never a Clerk
    id - a related bug elsewhere in this band came from exactly that confusion), in the same
    JSON shape a real LiveKit webhook's `evt.room.metadata` would carry it back."""
    fake = FakeTransport()
    org_id = uuid.uuid4()

    await fake.ensure_room(org_id=org_id, room="meeting-abc")

    assert fake.calls[-1].method == "ensure_room"
    assert fake.calls[-1].kwargs == {"org_id": org_id, "room": "meeting-abc"}
    assert fake.room_metadata["meeting-abc"] == json.dumps({"org_id": str(org_id)})
