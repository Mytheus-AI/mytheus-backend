"""integration-service -> mytheus-intelligence: extract a person's own promises from a message.

`POST {RAG_SERVICE_URL}/internal/promises/extract` on rag-service (the intelligence tier's
synchronous HTTP surface; the extractor is `promise_extract@vN`, budget-gated, metered,
PII-redacted and never-record-checked by the shared router there). integration-service presents
its own entry of `MYTHEUS_RAG_CALLER_TOKENS` (`integration-service=<secret>`, the same variable
rag-service verifies) and the tenant in `X-Mytheus-Org`.

Not connected until that secret is set: `get_extractor()` is None and the Slack connector
records messages without extracting anything (so nothing leaves the tenant for a model call).

The answer: {status: ok | budget_denied | never_record, prompt, promises: [{content, due_hint,
due_date, recipient_label, recipient_email, quote, confidence}]}. A 5xx or a network failure is
a `ProviderError` (the inbox row is retried); a 4xx is a programming error and is parked.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from services.integration_service.clients import ProviderError

SERVICE_NAME = "integration-service"
TOKENS_ENV = "MYTHEUS_RAG_CALLER_TOKENS"
URL_ENV = "RAG_SERVICE_URL"
PROVIDER = "intelligence"


def own_secret(raw: str) -> str | None:
    for entry in raw.split(","):
        name, _, secret = entry.strip().partition("=")
        if name.strip() == SERVICE_NAME and secret.strip():
            return secret.strip()
    return None


@dataclass(frozen=True)
class Extraction:
    status: str
    prompt: str | None
    promises: list[dict[str, Any]]


class PromiseExtractor:
    def __init__(
        self,
        base_url: str,
        secret: str,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._secret = secret
        self._transport = transport

    async def extract(
        self,
        org_id: uuid.UUID,
        *,
        channel: str,
        text: str,
        author_name: str | None,
        occurred_at: str | None,
        reference_date: str,
        people: list[dict[str, str | None]],
        context: str | None = None,
    ) -> Extraction:
        headers = {
            "X-Mytheus-Service": SERVICE_NAME,
            "X-Mytheus-Service-Token": self._secret,
            "X-Mytheus-Org": str(org_id),
        }
        body = {
            "channel": channel,
            "text": text,
            "author_name": author_name,
            "occurred_at": occurred_at,
            "reference_date": reference_date,
            "people": people,
            "context": context,
        }
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=60.0) as client:
                res = await client.post(
                    f"{self._base}/internal/promises/extract", json=body, headers=headers
                )
        except httpx.HTTPError as exc:
            raise ProviderError(PROVIDER, "extract_promises", type(exc).__name__) from None
        if res.status_code >= 400:
            raise ProviderError(
                PROVIDER, "extract_promises", f"http_{res.status_code}", res.status_code
            )
        data = res.json()
        items = data.get("promises") if isinstance(data, dict) else None
        return Extraction(
            status=str(data.get("status") or "ok") if isinstance(data, dict) else "ok",
            prompt=str(data["prompt"]) if isinstance(data, dict) and data.get("prompt") else None,
            promises=[p for p in items if isinstance(p, dict)] if isinstance(items, list) else [],
        )


_extractor: PromiseExtractor | None = None
_configured = False


def get_extractor() -> PromiseExtractor | None:
    """The configured extractor, or None ("not connected") when the secret is unset."""
    global _extractor, _configured
    if not _configured:
        _configured = True
        secret = own_secret(os.environ.get(TOKENS_ENV, ""))
        if secret:
            _extractor = PromiseExtractor(
                os.environ.get(URL_ENV, "http://rag-service:8107"), secret
            )
    return _extractor


def set_extractor(extractor: PromiseExtractor | None) -> None:
    """Tests inject a fake (or None for "not connected")."""
    global _extractor, _configured
    _extractor = extractor
    _configured = True
