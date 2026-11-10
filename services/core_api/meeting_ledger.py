"""core-api's side of the in-meeting ledger the agents read (polish program, "Memory inside the
meeting"; mytheus-intelligence `services/agent_runtime_service/memory.py`).

One Redis hash per meeting, `nexus:{org_id}:meeting-ledger:{meeting_id}`, 24 h TTL, with one
writer per field so nobody overwrites anybody:

- `c:{candidate_id}` - the agent runtime, when it emits or updates a card (kind, text, moment);
- `s:{candidate_id}` - HERE, when a person confirms, edits or dismisses a decision or action:
  `{"status": "confirmed"|"edited"|"dismissed", "text": <the edited text or null>,
  "kind": "decision"|"action"}`;
- `q:{user_id}:{suggestion_id}` - HERE, when a person asks, dismisses or saves a suggestion:
  `{"status": "asked"|"dismissed"|"saved", "text": <the question>}`.

The agents then update an open card instead of proposing it again, never re-propose a dismissed
or confirmed one, never overwrite a person's edit, and do not suggest a dismissed question again.

Best effort, like every cache here (`libs/shared/cache.py`): a Redis failure is logged and
swallowed, so a confirm never fails because the agents' memory is unavailable. It is written
before the request's transaction commits; if the commit then fails, the agents treat the card
as resolved while the row is still proposed, which only means they leave it alone.
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from libs.shared.cache import get_redis, redis_key

logger = logging.getLogger(__name__)

LEDGER_TTL_S = 24 * 3600
MAX_TEXT = 200


def ledger_key(org_id: uuid.UUID, meeting_id: uuid.UUID) -> str:
    return redis_key(org_id, "meeting-ledger", meeting_id)


def _clip(text: str | None) -> str | None:
    if text is None:
        return None
    flat = " ".join(text.split())
    return flat[:MAX_TEXT] or None


async def _write(
    org_id: uuid.UUID, meeting_id: uuid.UUID, field: str, value: dict[str, Any]
) -> None:
    key = ledger_key(org_id, meeting_id)
    try:
        redis = get_redis()
        await redis.hset(key, field, json.dumps(value))
        await redis.expire(key, LEDGER_TTL_S)
    except Exception:  # noqa: BLE001 - the agents' memory is an optimisation, never a dependency
        logger.warning("meeting ledger write failed meeting=%s field=%s", meeting_id, field)


async def mark_candidate(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    candidate_id: uuid.UUID,
    *,
    kind: str,
    status: str,
    text: str | None = None,
) -> None:
    await _write(
        org_id,
        meeting_id,
        f"s:{candidate_id}",
        {"status": status, "text": _clip(text), "kind": kind},
    )


async def mark_suggestion(
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID,
    suggestion_id: uuid.UUID,
    *,
    status: str,
    text: str,
) -> None:
    await _write(
        org_id,
        meeting_id,
        f"q:{user_id}:{suggestion_id}",
        {"status": status, "text": _clip(text)},
    )
