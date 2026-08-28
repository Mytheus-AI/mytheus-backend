"""The single definition of "this route is reachable without a bearer token".

Two things need this answer and they must never disagree:

- `scripts/check_authz.py`, the CI gate that fails a route declaring no auth dependency. A
  route on this list is allowed to declare none.
- `libs/shared/openapi.py`, which marks every other operation as requiring the bearer scheme
  and documents the `401` it can return.

If those two ever drifted apart, the emitted spec would advertise a route as public that the
gate treats as protected, or the reverse: a generated client would either omit the token on a
route that needs it, or send one to an endpoint whose whole security model is that it verifies
a provider signature instead. Both are silent until a client breaks in production.

Kept dependency-free on purpose. `check_authz.py` is a static AST walker that must run without
importing the application, so this module imports nothing.

The two categories are different in kind, which is why they are two lists:

- `PUBLIC_PREFIXES` are unauthenticated by design: liveness probes and the API's own
  documentation surface. They carry no tenant data.
- `PUBLIC_CONTAINS` are NOT unauthenticated. They are authenticated by a mechanism a bearer
  token cannot express: a Svix or LiveKit or Stripe signature over the raw request body, or an
  OAuth redirect carrying a state parameter. They verify before they parse. "Public" here means
  "does not take a Clerk JWT", never "anyone may call it".
"""

from __future__ import annotations

PUBLIC_PREFIXES = ("/healthz", "/health", "/openapi", "/docs", "/redoc")
PUBLIC_CONTAINS = ("/webhook", "/oauth", "/clerk/", "/stripe/", "/livekit/", "/callback")


def is_public_path(path: str) -> bool:
    """True when a route at `path` is not expected to carry a Clerk bearer token."""
    prefixed = any(path.startswith(p) for p in PUBLIC_PREFIXES)
    return prefixed or any(c in path for c in PUBLIC_CONTAINS)
