"""Log redaction for integration-service (plan 3.10 step 2; 21 section 2.2).

Structured logs redact any field whose name matches `*token*`, `*secret*`, `code` or
`client_secret`, at the logging layer, so a careless `logger.info("%s", response.json())` or an
exception message that echoes a provider response cannot put a credential in a log line.

Two passes over every record:

1. Mapping arguments and `extra` attributes: values under a sensitive key become `[REDACTED]`.
2. The rendered message: `key=value` / `"key": "value"` pairs with a sensitive key, bearer
   credentials, and Slack token shapes (`xoxb-...`) are replaced in the text itself.

The filter rewrites the record in place (message fully rendered, args cleared), so every handler
downstream sees only the redacted text.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from typing import Any

REDACTED = "[REDACTED]"

_SENSITIVE_KEY = re.compile(r"(token|secret|password|authorization|^code$|code_verifier)", re.I)

_PATTERNS = (
    # "access_token": "..."  /  'refresh_token': '...'
    re.compile(
        r"""(?P<k>["']?[\w-]*(?:token|secret|password|code_verifier)[\w-]*["']?\s*[:=]\s*)"""
        r"""(?P<q>["']?)(?P<v>[^"'\s,&}]+)""",
        re.I,
    ),
    # code=...  (query strings, form bodies)
    re.compile(r"""(?P<k>(?<![\w])["']?code["']?\s*[:=]\s*)(?P<q>["']?)(?P<v>[^"'\s,&}]+)""", re.I),
    # Authorization: Bearer xyz
    re.compile(r"(?P<k>\b(?:bearer|basic|jwt)\s+)(?P<q>)(?P<v>[A-Za-z0-9._~+/=-]{8,})", re.I),
)
_SLACK_TOKEN = re.compile(r"\bxox[abposr]-[A-Za-z0-9-]+")


def redact_text(text: str) -> str:
    for pattern in _PATTERNS:
        text = pattern.sub(lambda m: f"{m.group('k')}{m.group('q')}{REDACTED}", text)
    return _SLACK_TOKEN.sub(REDACTED, text)


def redact_value(value: Any, key: str | None = None) -> Any:
    if key is not None and _SENSITIVE_KEY.search(key):
        return REDACTED
    if isinstance(value, Mapping):
        return {k: redact_value(v, str(k)) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(redact_value(v) for v in value)
    if isinstance(value, str):
        return redact_text(value)
    return value


_STANDARD_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
}


class RedactingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.args, Mapping):
                record.args = redact_value(record.args)
            elif record.args:
                record.args = tuple(redact_value(a) for a in record.args)
            message = record.getMessage()
        except Exception:  # never let redaction break logging
            message = str(record.msg)
        record.msg = redact_text(message)
        record.args = None
        for attr, value in list(record.__dict__.items()):
            if attr not in _STANDARD_ATTRS:
                record.__dict__[attr] = redact_value(value, attr)
        if record.exc_info and record.exc_info[1] is not None:
            record.exc_text = redact_text(logging.Formatter().formatException(record.exc_info))
            record.exc_info = None
        return True


_FILTER = RedactingFilter()


def install_redaction(*logger_names: str) -> None:
    """Attach the filter to the named loggers (default: the root logger and its handlers).

    Filters on a logger do not apply to records propagated from child loggers, so the filter
    goes on every root handler too, and on `services.integration_service` itself."""
    names = logger_names or ("", "services.integration_service", "httpx", "uvicorn.error")
    for name in names:
        log = logging.getLogger(name)
        if _FILTER not in log.filters:
            log.addFilter(_FILTER)
        for handler in log.handlers:
            if _FILTER not in handler.filters:
                handler.addFilter(_FILTER)


def get_logger(name: str) -> logging.Logger:
    """A logger with the redaction filter attached directly.

    Every integration-service module logs through this, so redaction holds no matter which
    handlers exist when the record is emitted (a logger's own filters run before propagation,
    a handler added later still only ever sees the redacted record)."""
    log = logging.getLogger(name)
    if _FILTER not in log.filters:
        log.addFilter(_FILTER)
    return log
