"""Public URLs and constants for OAuth on the MCP server (Claude custom connectors).

Shared by three services that must agree byte for byte, because an MCP client compares them:

- bff serves the protected resource metadata (RFC 9728) for `/mcp`: `resource` and
  `authorization_servers`;
- identity-tenant-service is the authorization server (RFC 8414 metadata, DCR, authorize,
  token, revoke) and binds every code and token to `mcp_resource()`;
- api-gateway answers a 401 on `/mcp` with `WWW-Authenticate: Bearer resource_metadata=...`.

Claude requires `resource` to equal the server URL exactly as the user typed it
(`https://api.mytheus.cloud/mcp`), and uses only the FIRST `authorization_servers` entry, whose
`/.well-known/oauth-authorization-server` it then fetches. One function per URL, no string
building at call sites.

Env (prod defaults, so an unset variable is right in production):

- `API_PUBLIC_URL` (default `https://api.mytheus.cloud`): the gateway's public origin; the
  issuer and the MCP resource live under it.
- `APP_BASE_URL` (default here `https://dashboard.mytheus.cloud`): the web app, where the
  consent page `/oauth/consent` lives.
"""

from __future__ import annotations

import os

DEFAULT_API_PUBLIC_URL = "https://api.mytheus.cloud"
DEFAULT_APP_BASE_URL = "https://dashboard.mytheus.cloud"

MCP_PATH = "/mcp"
PROTECTED_RESOURCE_METADATA_PATH = "/.well-known/oauth-protected-resource"
AUTHORIZATION_SERVER_METADATA_PATH = "/.well-known/oauth-authorization-server"

# The OAuth scopes ARE the personal-access-token scopes, so an OAuth access token is an
# `api_tokens` row the gateway already understands. `offline_access` is advertised by the
# authorization server only so a client that asks for it (Claude appends it when listed) is not
# refused; refresh tokens are always issued and the scope is otherwise ignored.
SCOPES: tuple[str, ...] = ("read", "read_write")
OFFLINE_ACCESS = "offline_access"


def api_public_url() -> str:
    return (os.environ.get("API_PUBLIC_URL") or DEFAULT_API_PUBLIC_URL).strip().rstrip("/")


def app_base_url() -> str:
    return (os.environ.get("APP_BASE_URL") or DEFAULT_APP_BASE_URL).strip().rstrip("/")


def issuer() -> str:
    """The authorization server's issuer identifier (RFC 8414 section 2): no path, so its
    metadata is at `{issuer}/.well-known/oauth-authorization-server`."""
    return api_public_url()


def mcp_resource() -> str:
    """The MCP server's canonical URI (RFC 8707 / MCP 2025-06-18): what tokens are bound to."""
    return api_public_url() + MCP_PATH


def protected_resource_metadata_url() -> str:
    return api_public_url() + PROTECTED_RESOURCE_METADATA_PATH


def consent_url() -> str:
    return app_base_url() + "/oauth/consent"


def www_authenticate(*, error: str | None = None) -> str:
    """The challenge a 401 from `/mcp` carries (RFC 9728 section 5.1, RFC 6750 section 3)."""
    value = f'Bearer resource_metadata="{protected_resource_metadata_url()}"'
    if error:
        value += f', error="{error}"'
    return value
