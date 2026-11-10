"""OAuth `state` and PKCE (plan 3.10 step 3; 21 section 2.3).

`state` is `base64url(json) . base64url(hmac_sha256(OAUTH_STATE_SECRET, json))`, binding the
tenant, the installing user, the provider, a random nonce and a 10-minute expiry. It defeats
CSRF on the callback and is what authenticates `GET /oauth/{provider}` in place of a JWT: the
callback is a browser redirect from the provider and carries no bearer token.

The PKCE verifier never reaches the browser. It lives in Redis under
`nexus:{org_id}:oauth:{state}` for the same 10 minutes and is popped (GETDEL) on the callback, so
a state is single-use: a replayed callback finds no verifier.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
import uuid
from dataclasses import dataclass

STATE_TTL_SECONDS = 600


class StateError(Exception):
    """`reason` is the `?error=` value the callback redirects with."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class OAuthState:
    org_id: uuid.UUID
    user_id: uuid.UUID
    provider: str
    nonce: str
    expires_at: int


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def mint_state(
    secret: bytes,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    provider: str,
    *,
    now: float | None = None,
) -> str:
    issued = int(now if now is not None else time.time())
    body = json.dumps(
        {
            "o": str(org_id),
            "u": str(user_id),
            "p": provider,
            "n": secrets.token_urlsafe(16),
            "e": issued + STATE_TTL_SECONDS,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    sig = hmac.new(secret, body, hashlib.sha256).digest()
    return f"{_b64(body)}.{_b64(sig)}"


def verify_state(secret: bytes, state: str, *, now: float | None = None) -> OAuthState:
    """Signature first, then expiry. Any malformation is `invalid_state`."""
    try:
        body_b64, sig_b64 = state.split(".", 1)
        body, sig = _unb64(body_b64), _unb64(sig_b64)
    except (ValueError, TypeError) as exc:
        raise StateError("invalid_state") from exc
    expected = hmac.new(secret, body, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise StateError("invalid_state")
    try:
        data = json.loads(body)
        parsed = OAuthState(
            org_id=uuid.UUID(data["o"]),
            user_id=uuid.UUID(data["u"]),
            provider=str(data["p"]),
            nonce=str(data["n"]),
            expires_at=int(data["e"]),
        )
    except (ValueError, KeyError, TypeError) as exc:
        raise StateError("invalid_state") from exc
    if (now if now is not None else time.time()) > parsed.expires_at:
        raise StateError("state_expired")
    return parsed


def state_key(org_id: uuid.UUID, state: str) -> str:
    return f"nexus:{org_id}:oauth:{state}"


def new_verifier() -> str:
    """RFC 7636: 43-128 characters from the unreserved set. 64 random bytes -> 86 chars."""
    return secrets.token_urlsafe(64)


def challenge_for(verifier: str) -> str:
    return _b64(hashlib.sha256(verifier.encode("ascii")).digest())


# --- signed tokens for provider callbacks that cannot carry our state ---------------------------


def sign_token(secret: bytes, purpose: str, payload: dict[str, str]) -> str:
    """A compact signed token (Google push-channel `token`, max 256 chars)."""
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    mac = hmac.new(secret, purpose.encode() + b"\x00" + body, hashlib.sha256).digest()
    return f"{_b64(body)}.{_b64(mac[:24])}"


def verify_token(secret: bytes, purpose: str, token: str) -> dict[str, str] | None:
    try:
        body_b64, mac_b64 = token.split(".", 1)
        body, mac = _unb64(body_b64), _unb64(mac_b64)
    except (ValueError, TypeError):
        return None
    expected = hmac.new(secret, purpose.encode() + b"\x00" + body, hashlib.sha256).digest()[:24]
    if not hmac.compare_digest(mac, expected):
        return None
    try:
        data = json.loads(body)
    except ValueError:
        return None
    return data if isinstance(data, dict) else None
