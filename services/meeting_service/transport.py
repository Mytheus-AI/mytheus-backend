"""The `MeetingTransport` adapter — the ONLY place the LiveKit SDK is imported.

ADR-0003 section "Decision 2", verbatim. `core-api`, `transcription-service`, and the agent
tier never import `livekit`: they call this interface (once Tasks 3/4 wire routes on top of
it) or consume `meeting-room-events`. That boundary is what makes the eventual self-host
swap (Tech Decision 2.7) a one-interface reimplementation instead of a codebase-wide
migration, and it is enforced by `tests/test_livekit_import_boundary.py`, not left as a
review convention.

`FakeTransport` is not in the plan text. There is no LiveKit account for this project as of
this build (Task 2, Deviation 1): `LIVEKIT_URL`/`LIVEKIT_API_KEY`/`LIVEKIT_API_SECRET` are
unavailable and will not become available during this band. `LiveKitTransport` is still
written for real, exactly as the plan specifies — it is simply not exercisable end-to-end
today, and no test in this build claims otherwise. `FakeTransport` is deterministic and
dependency-free so the routes Tasks 3/4 add on top of `MeetingTransport` (and their tests)
have something real to run against in the meantime. `select_transport` defaults to the fake
when `LIVEKIT_API_KEY` is unset, so a misconfigured environment fails toward "obviously not
talking to LiveKit" rather than toward a silent no-op against a real provider.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from livekit import api  # the ONLY import of the LiveKit SDK in the codebase


class MeetingTransport(Protocol):
    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str: ...

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str: ...

    async def subscribe_room_audio(
        self, *, org_id: UUID, room: str
    ) -> object: ...  # handle for transcription tap

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object: ...


class LiveKitTransport:
    """Concrete LiveKit binding. Swappable per Tech Decisions 2.7.

    Real, not stubbed: this is the binding Tasks 3/4 route against once LiveKit credentials
    exist. It is not exercised end-to-end by this build's test suite (Task 2, Deviation 1) —
    only `FakeTransport` is, and no test here claims coverage of the real SDK calls below.
    """

    def __init__(self, url: str, key: str, secret: str):
        self._url, self._key, self._secret = url, key, secret

    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str:
        grant = api.VideoGrants(room=room, room_join=True, can_publish=True, can_subscribe=True)
        tok = (
            api.AccessToken(self._key, self._secret)
            .with_identity(str(user_id))  # one identity == user_id
            .with_grants(grant)
            # `livekit-api` 1.x's `with_ttl` takes a `timedelta`, not raw seconds; the plan
            # text's `.with_ttl(ttl_seconds)` matches the >=0.7 pin's minimum, not the 1.1.1
            # actually resolved. 600 s default; reconnect re-mints.
            .with_ttl(timedelta(seconds=ttl_seconds))
            .with_metadata(f'{{"org_id":"{org_id}","role":"{role_in_meeting}"}}')
        )
        return tok.to_jwt()

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str:
        # Wired by Task 3/4 against `nexus-recordings` (plan step 7); out of scope here.
        raise NotImplementedError("start_egress lands with the nexus-recordings bucket (Task 4)")

    async def subscribe_room_audio(self, *, org_id: UUID, room: str) -> object:
        # Wired by the transcription-service integration (out of scope for this task).
        raise NotImplementedError("subscribe_room_audio lands with the transcription tap")

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object:
        # `livekit-api` 1.x's `WebhookReceiver` takes a `TokenVerifier`, not the raw
        # (key, secret) pair the plan text passes directly; same >=0.7-vs-1.1.1 gap as
        # `with_ttl` above.
        verifier = api.TokenVerifier(self._key, self._secret)
        return api.WebhookReceiver(verifier).receive(body.decode(), auth_header)


@dataclass
class _RecordedCall:
    method: str
    kwargs: dict[str, object]


@dataclass
class FakeTransport:
    """Deterministic, dependency-free stand-in for `LiveKitTransport`.

    Not in the plan text — required because no LiveKit account exists for this project
    (Task 2, Deviation 1). Tokens are structurally valid (three dot-separated base64url-ish
    segments, like a real unsigned JWT) but carry no real signature: nothing downstream may
    treat a `FakeTransport` token as authenticating against an actual LiveKit deployment.
    Every call is recorded on `self.calls` so tests can assert against what was invoked and
    with what arguments, without a network call or a real credential.
    """

    calls: list[_RecordedCall] = field(default_factory=list)
    verify_webhook_result: object = None
    verify_webhook_error: Exception | None = None

    async def mint_join_token(
        self,
        *,
        org_id: UUID,
        meeting_id: UUID,
        user_id: UUID,
        room: str,
        role_in_meeting: str,
        ttl_seconds: int = 600,
    ) -> str:
        self.calls.append(
            _RecordedCall(
                "mint_join_token",
                {
                    "org_id": org_id,
                    "meeting_id": meeting_id,
                    "user_id": user_id,
                    "room": room,
                    "role_in_meeting": role_in_meeting,
                    "ttl_seconds": ttl_seconds,
                },
            )
        )
        # Structurally a JWT (header.payload.signature) so callers that only ever inspect
        # shape, not cryptographic validity, cannot tell the fake from the real thing by
        # accident. The "signature" segment makes clear it is not one: real verification of
        # a fake token must fail, not silently pass.
        payload = _b64url(
            json.dumps(
                {
                    "sub": str(user_id),
                    "room": room,
                    "org_id": str(org_id),
                    "role": role_in_meeting,
                    "ttl": ttl_seconds,
                },
                sort_keys=True,
            ).encode()
        )
        return f"fake.{payload}.unsigned"

    async def start_egress(self, *, org_id: UUID, meeting_id: UUID, room: str) -> str:
        self.calls.append(
            _RecordedCall(
                "start_egress", {"org_id": org_id, "meeting_id": meeting_id, "room": room}
            )
        )
        return f"fake-egress-{org_id}-{meeting_id}"

    async def subscribe_room_audio(self, *, org_id: UUID, room: str) -> object:
        self.calls.append(_RecordedCall("subscribe_room_audio", {"org_id": org_id, "room": room}))
        return None

    def verify_webhook(self, *, body: bytes, auth_header: str) -> object:
        self.calls.append(
            _RecordedCall("verify_webhook", {"body": body, "auth_header": auth_header})
        )
        if self.verify_webhook_error is not None:
            raise self.verify_webhook_error
        return self.verify_webhook_result


def _b64url(data: bytes) -> str:
    import base64

    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def select_transport() -> MeetingTransport:
    """Config-selected binding: `FakeTransport` unless `LIVEKIT_API_KEY` is set.

    Not called by anything in this task (no route exists yet to call it from); provided so
    Tasks 3/4 have one sanctioned selection point rather than each re-implementing the same
    `if key is set` check. Defaulting to the fake means an environment that simply forgot to
    configure LiveKit fails toward "obviously fake tokens downstream" rather than toward
    `LiveKitTransport` raising deep inside a request on missing credentials.
    """
    key = os.environ.get("LIVEKIT_API_KEY")
    if not key:
        return FakeTransport()
    url = os.environ["LIVEKIT_URL"]
    secret = os.environ["LIVEKIT_API_SECRET"]
    return LiveKitTransport(url, key, secret)


# --- Static protocol-conformance proof ---------------------------------------------------
#
# Acceptance check 5: assert conformance via the type checker, not only at runtime. A
# Protocol is structural, so a class can silently stop conforming (a renamed method, a
# dropped parameter) with no runtime error until something finally calls the missing method.
# These two lines make `mypy` (via `make typecheck` / `uv run mypy .`) the thing that catches
# that, at the point the signature drifts rather than the point something calls it.
def _assert_conforms(_cls: type[MeetingTransport]) -> None:
    """No-op at runtime; exists so mypy checks the argument against the Protocol."""


_assert_conforms(LiveKitTransport)
_assert_conforms(FakeTransport)

__all__ = ["MeetingTransport", "LiveKitTransport", "FakeTransport", "select_transport"]
