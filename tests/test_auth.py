"""Clerk RS256 JWKS verification, the opaque tenant claim, and Svix webhook rejection.

Gap 8. The Plan 0 verifier treated Clerk's organization claim as a UUID, hardcoded the claim
name, did not require `nbf`, and turned a JWKS outage into a 500. Each of those is a test
below, because each of them is reachable by an ordinary, correctly-issued Clerk token.
"""

from __future__ import annotations

import time
import uuid

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.security import HTTPAuthorizationCredentials
from jwt import PyJWKClientError

from libs.shared import auth as auth_mod
from libs.shared.errors import ErrorCode, ProblemDetail

_ISSUER = "https://test.clerk.accounts.dev"
# What Clerk actually puts in the claim: an opaque string, not a UUID. This value is the
# whole point of gap 8 - `UUID(...)` on it raises, and Plan 0 called `UUID(...)` on it.
_CLERK_ORG = "org_2abcXYZdefGHI"


def _keypair():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    priv = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    pub = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return priv, pub


class _FakeJWKS:
    def __init__(self, pub):
        self._pub = pub

    def get_signing_key_from_jwt(self, token):  # noqa: ARG002
        return type("K", (), {"key": self._pub})()


class _DeadJWKS:
    """A JWKS endpoint that cannot be reached, which is a dependency outage, not a 500."""

    def get_signing_key_from_jwt(self, token):  # noqa: ARG002
        raise PyJWKClientError("Failed to fetch data from the url")


class _ParsingJWKS:
    """Behaves like the REAL PyJWKClient: it PARSES the token to read its `kid` before any key
    lookup, so a malformed token raises a decode error from inside this call.

    `_FakeJWKS` returns a key without looking at the token at all, which is why no existing
    test reached the branch where that decode error is handled - and why a malformed token
    escaped as a 500 in production while the suite stayed green.
    """

    def __init__(self, pub):
        self._pub = pub

    def get_signing_key_from_jwt(self, token):
        pyjwt.decode(token, options={"verify_signature": False})  # raises on garbage
        return type("K", (), {"key": self._pub})()


def _install(monkeypatch, pub):
    monkeypatch.setenv("CLERK_ISSUER", _ISSUER)
    monkeypatch.delenv("CLERK_JWT_ORG_CLAIM", raising=False)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _FakeJWKS(pub))


def _creds(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _sign(priv, **overrides) -> str:
    now = int(time.time())
    claims = {
        "sub": "user_abc",
        "org_id": _CLERK_ORG,
        "iss": _ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + 3600,
    }
    claims.update(overrides)
    return pyjwt.encode(claims, priv, algorithm="RS256")


def test_verify_valid_token_returns_the_opaque_org_claim(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    claims = auth_mod.verify_token(_creds(_sign(priv)))
    assert claims["sub"] == "user_abc"
    # Returned verbatim. Anything that parses this as a UUID is the bug gap 8 describes.
    assert auth_mod.clerk_org_id_from(claims) == _CLERK_ORG
    with pytest.raises(ValueError):
        uuid.UUID(auth_mod.clerk_org_id_from(claims))


def test_org_claim_name_is_configurable(monkeypatch):
    """The Clerk JWT template is customer-configurable, so the claim name must not be fixed."""
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    monkeypatch.setenv("CLERK_JWT_ORG_CLAIM", "o")

    claims = auth_mod.verify_token(_creds(_sign(priv, o="org_custom")))
    assert auth_mod.clerk_org_id_from(claims) == "org_custom"

    # And the default claim no longer satisfies it, or the setting would be decorative.
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(_sign(priv)))
    assert exc.value.status == 401


def test_expired_token_is_401_token_expired(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(_sign(priv, exp=now - 10, iat=now - 3600, nbf=now - 3600)))
    assert exc.value.status == 401
    assert exc.value.code is ErrorCode.TOKEN_EXPIRED


def test_missing_org_claim_is_401_never_500(monkeypatch):
    """A personal-account session: authenticated, but with no tenant to act in."""
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    token = pyjwt.encode(
        {"sub": "u", "iss": _ISSUER, "iat": now, "nbf": now, "exp": now + 3600},
        priv,
        algorithm="RS256",
    )
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(token))
    assert exc.value.status == 401
    assert exc.value.code is ErrorCode.AUTH_REQUIRED


def test_nbf_is_required(monkeypatch):
    """A token with no nbf must be refused rather than silently accepted."""
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    token = pyjwt.encode(
        {"sub": "u", "org_id": _CLERK_ORG, "iss": _ISSUER, "iat": now, "exp": now + 3600},
        priv,
        algorithm="RS256",
    )
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(token))
    assert exc.value.status == 401


def test_token_not_yet_valid_is_refused(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(_sign(priv, nbf=now + 600, iat=now)))
    assert exc.value.status == 401


def test_jwks_outage_is_503_not_500(monkeypatch):
    """PyJWKClientError does not derive from InvalidTokenError, so Plan 0's except missed it.

    The distinction is operational, not cosmetic: a 500 says "this service is broken" and
    pages its owner, while a 503 says "a dependency is down, retry" and points at Clerk.
    """
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _DeadJWKS())
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(_sign(priv)))
    assert exc.value.status == 503
    assert exc.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE


def test_missing_bearer_is_401(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(None)
    assert exc.value.status == 401


# The Svix webhook's signature rejection is covered in tests/test_clerk_webhook.py, alongside
# the rest of that route's behaviour. It is not duplicated here: this file is about verifying
# Clerk SESSION TOKENS, and the webhook authenticates by HMAC over the body instead, with no
# token involved at all. The version that used to live here also built a bare `FastAPI()`
# rather than `create_app()`, so it had no RFC 7807 handlers mounted and was asserting on an
# app shaped differently from every deployed one.


@pytest.mark.parametrize(
    "bad",
    [
        "not-a-real-jwt",  # the exact shape that 500'd: "Not enough segments"
        "a.b",  # two segments
        "....",  # empty segments
        "eyJhbGciOiJSUzI1NiJ9.%%%.zzz",  # undecodable base64 payload
    ],
)
def test_a_malformed_token_is_401_never_500(monkeypatch, bad):
    """A malformed token is the caller's fault, not a server fault.

    `get_signing_key_from_jwt` parses before it looks anything up, so the decode error is
    raised there rather than in the `jwt.decode` below it. That call used to be wrapped in a
    try that caught only PyJWKClientError, so `Authorization: Bearer x` produced an unhandled
    500. Three consequences: attacker-controlled input reached an unhandled exception path,
    expired and truncated tokens were indistinguishable from server faults in monitoring, and
    a client that redirects to sign-in on AUTH_REQUIRED showed an error page instead.
    """
    priv, pub = _keypair()
    monkeypatch.setenv("CLERK_ISSUER", _ISSUER)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _ParsingJWKS(pub))

    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds(bad))
    assert exc.value.status == 401
    assert exc.value.code is ErrorCode.AUTH_REQUIRED
    assert "Not enough segments" not in str(exc.value.detail), "must not echo the parser error"


def test_a_dependency_outage_is_still_503_not_401(monkeypatch):
    """The new branch must not swallow the JWKS-outage case: an unreachable identity provider
    is not the caller presenting a bad token."""
    _, pub = _keypair()
    monkeypatch.setenv("CLERK_ISSUER", _ISSUER)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _DeadJWKS())

    with pytest.raises(ProblemDetail) as exc:
        auth_mod.verify_token(_creds("a.b.c"))
    assert exc.value.status == 503
    assert exc.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE


def test_a_valid_token_still_verifies_through_the_parsing_client(monkeypatch):
    """Guards the fix against being a blanket except that hides real failures."""
    priv, pub = _keypair()
    monkeypatch.setenv("CLERK_ISSUER", _ISSUER)
    monkeypatch.delenv("CLERK_JWT_ORG_CLAIM", raising=False)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _ParsingJWKS(pub))

    claims = auth_mod.verify_token(_creds(_sign(priv)))
    assert auth_mod.clerk_org_id_from(claims) == _CLERK_ORG
