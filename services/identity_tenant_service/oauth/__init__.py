"""The OAuth 2.1 authorization server for the MCP server (Claude custom connectors).

Why identity-tenant-service and not a new service or bff. An access token here has to become
something the gateway already resolves: an `api_tokens` row in the `mytheus_pat_` format.
identity is the one writer of `api_tokens` (one writer per store), and it already owns users,
RBAC and the token-management surface, so the authorization server lives where the store it
writes lives. bff stays a non-writer and the gateway stays config-only.

- `core.py`: token formats, PKCE, redirect-URI rules, the signed authorization request.
- `public_routes.py`: the unauthenticated protocol endpoints (RFC 8414 metadata, RFC 7591
  registration, authorize, token, RFC 7009 revoke).
- `app_routes.py`: the signed-in endpoints the web app calls (consent, connected apps).
"""
