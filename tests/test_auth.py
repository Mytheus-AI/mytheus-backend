"""Clerk RS256 JWKS verification + org_id extraction, and Svix webhook signature rejection."""

from __future__ import annotations

import time
import uuid

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from starlette.testclient import TestClient

from libs.shared import auth as auth_mod

_ISSUER = "https://test.clerk.accounts.dev"


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


def _install(monkeypatch, pub):
    monkeypatch.setenv("CLERK_ISSUER", _ISSUER)
    monkeypatch.setattr(auth_mod, "_jwks_client", lambda: _FakeJWKS(pub))


def _creds(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _sign(priv, **overrides) -> str:
    now = int(time.time())
    claims = {
        "sub": "user_abc",
        "org_id": str(uuid.uuid4()),
        "iss": _ISSUER,
        "iat": now,
        "exp": now + 3600,
    }
    claims.update(overrides)
    return pyjwt.encode(claims, priv, algorithm="RS256")


def test_verify_valid_token_extracts_org_id(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    claims = auth_mod.verify_token(_creds(_sign(priv)))
    assert claims["sub"] == "user_abc"
    assert isinstance(auth_mod.org_id_from(claims), uuid.UUID)


def test_verify_expired_token_401(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    token = _sign(priv, exp=now - 10, iat=now - 3600)
    with pytest.raises(HTTPException) as exc:
        auth_mod.verify_token(_creds(token))
    assert exc.value.status_code == 401


def test_verify_missing_org_id_401(monkeypatch):
    priv, pub = _keypair()
    _install(monkeypatch, pub)
    now = int(time.time())
    token = pyjwt.encode(
        {"sub": "u", "iss": _ISSUER, "iat": now, "exp": now + 3600}, priv, algorithm="RS256"
    )
    with pytest.raises(HTTPException) as exc:
        auth_mod.verify_token(_creds(token))
    assert exc.value.status_code == 401


def test_clerk_webhook_rejects_bad_signature(monkeypatch):
    monkeypatch.setenv("CLERK_WEBHOOK_SECRET", "whsec_" + "A" * 32)
    from services.identity_tenant_service.routes import auth as route_auth

    app = FastAPI()
    app.include_router(route_auth.router)
    client = TestClient(app)
    resp = client.post(
        "/clerk/webhook",
        content=b"{}",
        headers={
            "svix-id": "msg_1",
            "svix-timestamp": str(int(time.time())),
            "svix-signature": "v1,deadbeef",
        },
    )
    assert resp.status_code == 401
