"""The gateway must not run its own idempotency layer.

A gateway layer and the service layer behind it share one org-scoped key. With an in-progress
marker (a duplicate that arrives while the first is still running gets 409), the service's layer
answers 409 to the gateway's own forwarded request: every first POST through the gateway failed
with IDEMPOTENCY_KEY_REUSED. Found by running the real flow locally, not by a unit test.
"""

from __future__ import annotations

from libs.shared.app_factory import create_app
from libs.shared.idempotency import IdempotencyMiddleware


def _has_idempotency(app) -> bool:  # type: ignore[no-untyped-def]
    return any(m.cls is IdempotencyMiddleware for m in app.user_middleware)


def test_the_gateway_app_has_no_idempotency_layer() -> None:
    from services.api_gateway.main import app

    assert not _has_idempotency(app)


def test_services_behind_the_gateway_keep_their_idempotency_layer() -> None:
    assert _has_idempotency(create_app("core-api"))
    assert _has_idempotency(create_app("bff"))
