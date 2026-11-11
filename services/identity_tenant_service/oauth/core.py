"""Building blocks of the authorization server: formats, PKCE, redirect rules, the signed request.

Credentials and what each carries:

- Authorization code `mytheus_oac_<org hex>_<secret>` and refresh token
  `mytheus_ort_<org hex>_<secret>`: the tenant is in the text, exactly like a personal access
  token, so the UNAUTHENTICATED token endpoint can set `app.current_org` from the credential and
  look its SHA-256 up under RLS. A credential whose tenant part was edited finds nothing in the
  tenant it names: cross-tenant is the same `invalid_grant` as unknown. Only hashes are stored.
- Access token: a `mytheus_pat_` personal-access-token string (`libs/shared/pat.py`), stored as an
  `api_tokens` row of `kind = 'oauth'` with a one-hour expiry, so the gateway resolves it with no
  new code path.
- The authorization request between `GET /oauth/authorize` and the consent page is a signed,
  15-minute blob (`sign_request`), not a row: before sign-in there is no tenant to store it
  under, and the consent page re-validates the client and redirect anyway. Its key is
  `OAUTH_REQUEST_SIGNING_KEY` when set (32+ characters), otherwise derived from
  `GATEWAY_SIGNING_KEY` with a fixed label (the share-link pattern), so no new secret is needed.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared import oauth_config

CODE_PREFIX = "mytheus_oac_"
REFRESH_PREFIX = "mytheus_ort_"
_SECRET_BYTES = 32

CODE_TTL_SECONDS = 300
REQUEST_TTL_SECONDS = 900
DEFAULT_ACCESS_TTL_SECONDS = 3600
DEFAULT_REFRESH_TTL_DAYS = 30

LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
DEFAULT_TRUSTED_REDIRECT_HOSTS = ("claude.ai", "claude.com")

_VERIFIER = re.compile(r"^[A-Za-z0-9\-._~]{43,128}$")
_CHALLENGE = re.compile(r"^[A-Za-z0-9\-_]{43}$")
_STATE_MAX = 1024


def _int_env(name: str, default: int, low: int, high: int) -> int:
    try:
        return min(high, max(low, int(os.environ.get(name, default))))
    except ValueError:
        return default


def access_ttl_seconds() -> int:
    return _int_env("OAUTH_ACCESS_TOKEN_TTL_SECONDS", DEFAULT_ACCESS_TTL_SECONDS, 300, 86_400)


def refresh_ttl_days() -> int:
    return _int_env("OAUTH_REFRESH_TOKEN_TTL_DAYS", DEFAULT_REFRESH_TTL_DAYS, 1, 365)


# --- credentials ---------------------------------------------------------------------------------


def _format(prefix: str) -> re.Pattern[str]:
    return re.compile(rf"^{prefix}([0-9a-f]{{32}})_([A-Za-z0-9_\-]{{43}})$")


_FORMATS = {CODE_PREFIX: _format(CODE_PREFIX), REFRESH_PREFIX: _format(REFRESH_PREFIX)}


def hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def mint(prefix: str, org_id: uuid.UUID) -> tuple[str, str]:
    """A fresh credential for `org_id` -> (text, sha256 hex)."""
    while True:
        secret = secrets.token_urlsafe(_SECRET_BYTES)
        if len(secret) == 43:
            break
    value = f"{prefix}{org_id.hex}_{secret}"
    return value, hash_secret(value)


def org_of(prefix: str, value: str | None) -> uuid.UUID | None:
    match = _FORMATS[prefix].match(value or "")
    return uuid.UUID(hex=match.group(1)) if match else None


def new_client_id() -> str:
    return "mcp_" + secrets.token_urlsafe(18)


# --- PKCE (RFC 7636, S256 only) ------------------------------------------------------------------


def valid_challenge(challenge: str | None) -> bool:
    return bool(challenge) and bool(_CHALLENGE.match(str(challenge)))


def pkce_ok(verifier: str | None, challenge: str) -> bool:
    if not verifier or not _VERIFIER.match(verifier):
        return False
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    computed = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return hmac.compare_digest(computed, challenge)


def valid_state(state: str | None) -> bool:
    """`state` is optional (PKCE already binds the response to the request) but bounded."""
    return state is None or (0 < len(state) <= _STATE_MAX and state.isprintable())


# --- redirect URIs -------------------------------------------------------------------------------


def redirect_uri_problem(uri: str) -> str | None:
    """Why `uri` cannot be registered, or None. HTTPS only, plus an `http` loopback redirect for
    native apps (RFC 8252 section 7.3; Claude Code uses `http://localhost:<port>/callback`).
    No fragment (RFC 6749 section 3.1.2), no userinfo, no custom schemes."""
    if not isinstance(uri, str) or not uri or len(uri) > 2000:
        return "redirect_uris must be absolute URLs of at most 2000 characters"
    try:
        parts = urlsplit(uri)
        _ = parts.port  # raises on a malformed port
    except ValueError:
        return f"{uri!r} is not a valid URL"
    if parts.fragment or "#" in uri:
        return f"{uri!r} must not contain a fragment"
    if parts.username is not None or parts.password is not None:
        return f"{uri!r} must not contain credentials"
    host = (parts.hostname or "").lower()
    if not host:
        return f"{uri!r} has no host"
    if parts.scheme == "https":
        return None
    if parts.scheme == "http" and host in LOOPBACK_HOSTS:
        return None
    return f"{uri!r} must use https (http is allowed only for localhost loopback redirects)"


def is_loopback(uri: str) -> bool:
    parts = urlsplit(uri)
    return parts.scheme == "http" and (parts.hostname or "").lower() in LOOPBACK_HOSTS


def redirect_matches(registered: list[str], presented: str) -> bool:
    """Exact string match (OAuth 2.1), except a loopback redirect matches on any port: a native
    app binds an ephemeral port per session (RFC 8252 section 7.3). Claude asks for the same
    port-agnostic match on `localhost` as on `127.0.0.1`."""
    if presented in registered:
        return True
    if not is_loopback(presented):
        return False
    p = urlsplit(presented)
    for uri in registered:
        if not is_loopback(uri):
            continue
        r = urlsplit(uri)
        if (r.hostname or "").lower() == (p.hostname or "").lower() and (
            r.path or "/",
            r.query,
        ) == (p.path or "/", p.query):
            return True
    return False


def redirect_host(uri: str) -> str:
    parts = urlsplit(uri)
    host = (parts.hostname or "").lower()
    return f"{host}:{parts.port}" if parts.port and not is_loopback(uri) else host


def trusted_redirect_hosts() -> frozenset[str]:
    raw = os.environ.get("OAUTH_TRUSTED_REDIRECT_HOSTS")
    hosts = raw.split(",") if raw is not None else DEFAULT_TRUSTED_REDIRECT_HOSTS
    return frozenset(h.strip().lower() for h in hosts if h.strip())


def is_trusted_redirect(uri: str) -> bool:
    """A redirect to a host Mytheus recognises (Claude's callback). Display only: the consent
    page marks it as verified; anything else gets a plain warning to check the host."""
    parts = urlsplit(uri)
    return parts.scheme == "https" and (parts.hostname or "").lower() in trusted_redirect_hosts()


def with_params(uri: str, params: dict[str, str | None]) -> str:
    """`uri` with `params` appended to its query (existing query kept, RFC 6749 3.1.2)."""
    parts = urlsplit(uri)
    query = parse_qsl(parts.query, keep_blank_values=True)
    query += [(k, v) for k, v in params.items() if v is not None]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


# --- scope and resource --------------------------------------------------------------------------


class ScopeError(ValueError):
    pass


def effective_scope(raw: str | None) -> str:
    """The PAT scope a requested OAuth `scope` grants: `read_write` if asked, else `read`.

    Unknown scopes are refused (`invalid_scope`); `offline_access` is accepted and ignored
    (refresh tokens are always issued). An empty request gets `read`, the least privilege.
    """
    asked = set((raw or "").split())
    unknown = asked - {*oauth_config.SCOPES, oauth_config.OFFLINE_ACCESS}
    if unknown:
        raise ScopeError(f"Unknown scope: {' '.join(sorted(unknown))}")
    return "read_write" if "read_write" in asked else "read"


def _canonical(uri: str) -> str:
    parts = urlsplit(uri.strip())
    path = parts.path.rstrip("/")
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), path, parts.query, ""))


def resource_ok(resource: str | None) -> bool:
    """RFC 8707: the only resource this server issues tokens for is the MCP server. Scheme and
    host compare case-insensitively and a trailing slash is ignored (MCP 2025-06-18)."""
    if not resource or "#" in resource:
        return False
    return _canonical(resource) == _canonical(oauth_config.mcp_resource())


# --- the signed authorization request ------------------------------------------------------------


class RequestInvalid(Exception):
    pass


class RequestExpired(Exception):
    pass


class SigningNotConfigured(Exception):
    pass


@dataclass(frozen=True)
class AuthRequest:
    client_id: str
    redirect_uri: str
    code_challenge: str
    scope: str
    resource: str
    state: str | None
    exp: int


def _request_key() -> bytes:
    own = os.environ.get("OAUTH_REQUEST_SIGNING_KEY", "").strip()
    if len(own) >= 32:
        return own.encode("utf-8")
    gateway = os.environ.get("GATEWAY_SIGNING_KEY", "").strip()
    if gateway:
        return hmac.new(gateway.encode("utf-8"), b"mytheus/oauth-authorize/v1", "sha256").digest()
    raise SigningNotConfigured


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def sign_request(req: AuthRequest) -> str:
    payload = _b64(json.dumps(asdict(req), separators=(",", ":")).encode("utf-8"))
    sig = _b64(hmac.new(_request_key(), payload.encode("ascii"), hashlib.sha256).digest())
    return f"{payload}.{sig}"


def new_request(
    *,
    client_id: str,
    redirect_uri: str,
    code_challenge: str,
    scope: str,
    resource: str,
    state: str | None,
) -> AuthRequest:
    return AuthRequest(
        client_id=client_id,
        redirect_uri=redirect_uri,
        code_challenge=code_challenge,
        scope=scope,
        resource=resource,
        state=state,
        exp=int(time.time()) + REQUEST_TTL_SECONDS,
    )


def open_request(token: str | None) -> AuthRequest:
    """The request a consent page carries. Raises RequestInvalid (tampered, malformed) or
    RequestExpired (older than 15 minutes); SigningNotConfigured if no key exists."""
    payload, _, sig = (token or "").partition(".")
    if not payload or not sig or len(token or "") > 8000:
        raise RequestInvalid
    expected = _b64(hmac.new(_request_key(), payload.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(expected, sig):
        raise RequestInvalid
    try:
        data: dict[str, Any] = json.loads(_unb64(payload))
        req = AuthRequest(**data)
    except (ValueError, TypeError) as exc:
        raise RequestInvalid from exc
    if req.exp < int(time.time()):
        raise RequestExpired
    return req


# --- clients -------------------------------------------------------------------------------------

_CLIENT = text(
    "SELECT client_id, client_name, redirect_uris, grant_types, response_types, scope, "
    "client_uri, created_at, revoked_at FROM oauth_clients WHERE client_id = :c"
)


async def load_client(session: AsyncSession, client_id: str | None) -> dict[str, Any] | None:
    """An active (not revoked) client, or None. `oauth_clients` is global: no RLS to set."""
    if not client_id or len(client_id) > 100:
        return None
    row = (await session.execute(_CLIENT, {"c": client_id})).mappings().first()
    if row is None or row["revoked_at"] is not None:
        return None
    return dict(row)


# --- responses -----------------------------------------------------------------------------------

NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}


def oauth_error(
    error: str, description: str, *, status: int = 400, headers: dict[str, str] | None = None
) -> JSONResponse:
    """An RFC 6749 section 5.2 / RFC 7591 section 3.2.2 error: `{error, error_description}`.

    Deliberately NOT problem+json: OAuth clients parse `error`, and Claude needs `invalid_grant`
    (not a custom code) to know a refresh token is dead.
    """
    return JSONResponse(
        status_code=status,
        content={"error": error, "error_description": description},
        headers={**NO_STORE, **(headers or {})},
    )
