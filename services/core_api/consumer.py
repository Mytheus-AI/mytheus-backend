"""Kafka consumer for core-api.

Consumes: `agent-results` (Plan 7-9 section 3.3 step 3: `PersonalListenerAgent` notes into
`worklogs`; section 3.6 step 3: decision/action candidates into `decisions`/`action_items` as
`proposed`, with no event; see `handle_agent_result`) and presence-events (analytics read
model only; no domain write - Plan 3-4 section 3.3 step 5), and since P12 `integration-events`
(`calendar_event_upserted` -> `meetings` + `meeting_participants`, see
`services/core_api/integration_events.py`; every other integration event is a no-op here), and
since polish wave 2 `agent-feedback` (core-api's own feedback events, folded into
`person_memory` by `services/core_api/person_memory.py`; inbox claim first, DLQ on poison).
Presence itself stays owned by `realtime-gateway`/Redis; this consumer never writes
`meeting_participants` or any table `core-api` treats as domain source of truth from the
presence path.

Split the same way `libs/shared/relay.py` splits `relay_once` (testable core) from
`services/relay/main.py`'s `run()` (the confluent-kafka/asyncio harness): `handle_presence_event`
takes an already-decoded message and is exercised directly in tests against a real Postgres +
fakeredis, with no live broker or Schema Registry required. `run_consumer()` is the thin,
mostly-untested wiring around it.

Contract with `realtime-gateway`'s producer: CLOSED, both sides. `PresenceEvent`'s envelope
header, and this topic's `user_id` payload field, are documented as UUID-typed
(`mytheus-events/schemas/presence-events/v1.avsc`, `_canon/03-event-taxonomy.md`'s envelope
table: "org_id ... Tenant root; RLS/cell scoping anchor"), and `realtime-gateway`'s
`presenceEventsProducer.ts` now honours that: it resolves `internalOrgId`/`internalUserId` via
this service's own `GET /api/v1/meetings/:id/participants/:clerkUserId` (reading
`ParticipantCheck.org_id`, added in `ca9a8db`, and `Participant.user_id`, which Task 6 had
already been discarding until it started reading it) before ever producing, and additionally
validates `isUuid(orgId) && isUuid(userId)` immediately before `send()`. Verified end to end by
Task 6 against a real `core-api` process, real Postgres, real RS256 verification. A Clerk-native
id can no longer reach this topic.

History, for context only - NOT a current condition: for a window during this band, the
producer passed the raw JWT `org_id`/`sub` claims straight through unresolved, which would have
made every real message fail the parse below and land in `presence-events.dlq`. That window is
closed; nothing here is presently poison-messaging on this account.

`handle_presence_event`'s strict `uuid.UUID(header["org_id"])` parse and DLQ-on-failure below
are NOT a workaround for the now-closed gap and must not be relaxed as if they were. They are
the standing "add an event consumer" recipe from this repo's `CLAUDE.md` (re-assert `org_id`
from the header before any write), and now serve as defence against a FUTURE regression on the
producer side - this or another producer sending a malformed header again - not as a permanent
crutch for a bug that no longer exists.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.audit import (
    ACTION_A2A_REQUEST_ALLOWED,
    ACTION_A2A_REQUEST_BLOCKED,
    ACTION_A2A_REQUEST_DENIED,
    audit,
)
from libs.shared.inbox import is_duplicate, record_consumed
from libs.shared.metrics import (
    observe_consumer_lag,
    record_dlq,
    record_processed,
    serve_metrics,
    set_service,
)
from libs.shared.tenancy import set_rls
from services.core_api import briefings as briefings_mod
from services.core_api import minutes as minutes_mod
from services.core_api.confidence import SILENT_FROM as _SILENT_FROM
from services.core_api.confidence import SURFACE_ABOVE as _SURFACE_ABOVE
from services.core_api.confidence import thresholds
from services.core_api.delegation_log import log_delegated_answer
from services.core_api.integration_events import (
    IntegrationEventPoison,
    handle_integration_event,
)
from services.core_api.learning import band_for
from services.core_api.person_memory import FeedbackPoison, handle_agent_feedback
from services.core_api.visibility import is_visible

logger = logging.getLogger(__name__)

TOPIC = "presence-events"
AGENT_RESULTS = "agent-results"
INTEGRATION_EVENTS = "integration-events"
# Polish wave 2: a person's feedback on agent output folds into their `person_memory`
# (`services/core_api/person_memory.py`).
AGENT_FEEDBACK = "agent-feedback"
# P12: calendar events become meetings (`services/core_api/integration_events.py`).
TOPICS = [TOPIC, AGENT_RESULTS, INTEGRATION_EVENTS, AGENT_FEEDBACK]
CONSUMER_GROUP = "core-api"

# doc 08 section 6, the uniform confidence policy (see `services/core_api/confidence.py`).
# Re-exported under their historical names.
SURFACE_ABOVE = _SURFACE_ABOVE
SILENT_FROM = _SILENT_FROM
_thresholds = thresholds

# P9 (ADR-0034 D3): QuestionAgent suggestions, one `agent_suggestions` row each, for `user_id`.
_SUGGESTION = ("QuestionAgent", "suggestion")
MAX_SUGGESTION_CHARS = 1000
MAX_CITATIONS = 8
_CITATION_KINDS = frozenset({"transcript", "document", "decision", "action"})

# P10 (contract section 6): ContextGapAgent collaboration entries, one `agent_collaborations`
# row each; the fill fields go on the entry that produced the fill. `kind` is an Avro enum the
# decoder hands over as a str; `str()` below makes a symbol object compare the same way.
_CONTEXT_FILL = ("ContextGapAgent", "context_fill")
NIL_UUID = uuid.UUID(int=0)
MAX_FILL_CHARS = 2000
MAX_QUERY_CHARS = 2000
_HOPS = frozenset({"peer", "related_meeting", "rag"})
_COLLAB_AUDIT = {
    "answered": ACTION_A2A_REQUEST_ALLOWED,
    "no_answer": ACTION_A2A_REQUEST_ALLOWED,
    "deadline_miss": ACTION_A2A_REQUEST_ALLOWED,
    "rag_hit": ACTION_A2A_REQUEST_ALLOWED,
    "rag_miss": ACTION_A2A_REQUEST_ALLOWED,
    "authz_denied": ACTION_A2A_REQUEST_DENIED,
    "org_spoof": ACTION_A2A_REQUEST_BLOCKED,
    "foreign_org": ACTION_A2A_REQUEST_BLOCKED,
}

# ADR-0038 D7/D8: one MeetingAgent record per meeting per window. Each `updates` entry moves
# an agenda item of that meeting to `in_progress` or `covered`; the `nudge` is for the host's
# screen only (realtime-gateway) and is never persisted here.
_MEETING_UPDATE = ("MeetingAgent", "meeting_update")
_AGENT_AGENDA_STATUSES = frozenset({"in_progress", "covered"})
MAX_AGENDA_NOTE_CHARS = 300
MAX_AGENDA_UPDATES = 20

# Minutes for everyone: after the meeting ended, one MeetingAgent `minutes` record per
# participant and one shared record (`user_id` = the nil uuid), into `meeting_minutes`
# (`services/core_api/minutes.py`). When the shared minutes land and the automatic follow-up is
# waiting for them (`meeting_followups.awaiting_minutes_since`), the follow-up is sent once the
# transaction has committed (`AFTER_COMMIT`, drained by `run_consumer`).
_MINUTES = ("MeetingAgent", "minutes")

# Work to run after the current message's transaction commits (never inside it: it calls other
# services). `run_consumer` drains it after the commit and clears it on a rollback.
AFTER_COMMIT: list[Callable[[], Awaitable[Any]]] = []

# ADR-0039 B6/C3: PersonalListenerAgent records carry `payload_json.commitments`
# (`[{content, due_hint}]`, at most 5): promises the participant made, private to `user_id`.
MAX_COMMITMENTS_PER_RECORD = 5
MAX_COMMITMENT_CHARS = 500
MAX_DUE_HINT_CHARS = 100

# Phase 8 Part A contract section 2: the two candidate kinds that get a `proposed` row.
_CANDIDATE_TABLES = {
    ("DecisionAgent", "decision_candidate"): "decisions",
    ("ActionAgent", "action_candidate"): "action_items",
}


class PoisonMessage(Exception):
    """The message cannot be safely applied (schema mismatch, header that names no real
    tenant). The caller routes the raw message to `{TOPIC}.dlq` and commits past it - never
    re-raised into the poll loop, per the "never block the partition" consumer recipe."""


async def handle_presence_event(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one already-Avro-decoded `presence-events` message.

    Returns True if it was newly recorded, False if it was a dedupe no-op. Raises
    `PoisonMessage` if the header does not carry a resolvable tenant - expected not to happen
    against `realtime-gateway`'s current producer (see the module docstring), but kept as the
    standing defence the "add an event consumer" recipe calls for regardless.
    """
    header = value.get("header") or {}
    raw_org_id = header.get("org_id")
    idempotency_key = header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"presence-events message missing header org_id/idempotency_key: {value!r}"
        )

    try:
        org_id = uuid.UUID(str(raw_org_id))
    except ValueError as exc:
        raise PoisonMessage(
            f"presence-events header.org_id is not a UUID ({raw_org_id!r}); the producer "
            "contract requires a resolved internal UUID here - see the module docstring"
        ) from exc

    if await is_duplicate(redis, str(org_id), TOPIC, idempotency_key):
        return False

    await set_rls(session, org_id)

    # Read-model only, per the plan text: no write to meeting_participants or any table this
    # service treats as domain source of truth. A durable analytics sink (its own table) is a
    # data-model decision this task does not have the authority to make unilaterally (CLAUDE.md
    # naming authority: a new table needs `_canon/02-data-model.md` plus an ADR first) - logged
    # structurally instead, and flagged as a concrete follow-up in the task-5 report.
    logger.info(
        "presence-events: %s org=%s meeting=%s user=%s",
        value.get("event_type"),
        org_id,
        value.get("meeting_id"),
        value.get("user_id"),
        extra={
            "org_id": str(org_id),
            "meeting_id": value.get("meeting_id"),
            "user_id": value.get("user_id"),
            "event_type": value.get("event_type"),
        },
    )
    try:
        await record_consumed(session, CONSUMER_GROUP, str(org_id), idempotency_key)
    except IntegrityError as exc:
        # `inbox.org_id` references `organizations`: a tenant that does not exist can never
        # apply. Raised as IntegrityError it crashed the loop and blocked the partition on
        # every restart; as poison it is dead-lettered (same fix as agent-results, 97f2709).
        raise PoisonMessage(f"presence-events names unknown tenant {org_id}") from exc
    return True


def _uuid(raw: Any, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(raw))
    except ValueError as exc:
        raise PoisonMessage(f"agent-results {what} is not a UUID: {raw!r}") from exc


async def handle_agent_result(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one decoded `agent-results` message. True if a `worklogs` row was written.

    `PersonalListenerAgent` notes go to `worklogs` (P7). `DecisionAgent`/`decision_candidate`
    and `ActionAgent`/`action_candidate` go to `decisions` / `action_items` as `proposed` rows
    (P8 Part A; True if any candidate row was new), never to the outbox.
    `QuestionAgent`/`suggestion` goes to `agent_suggestions` as `open` rows for the result's
    `user_id` (P9, ADR-0034; True if any row was new). `MeetingAgent`/`meeting_update` moves
    agenda items (ADR-0038 D7; True if any item changed). Other kinds - including ones this
    consumer does not know yet - are recorded in the inbox and otherwise ignored.

    `surfaced` is decided HERE, not trusted from the payload (ADR-0030 D3): `core-api` can read
    the tenant's `agent_confidence_overrides`, the AI tier cannot. Below `silent` nothing is
    written. Every id in the message is checked against this tenant under RLS before the
    insert, because a foreign key resolves rows the writing transaction cannot see (gap 31):
    a meeting or user from another tenant is poison, never a row.
    """
    header = value.get("header") or {}
    raw_org_id, idempotency_key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org_id or not idempotency_key:
        raise PoisonMessage(
            f"agent-results message missing header org_id/idempotency_key: {value!r}"
        )
    org_id = _uuid(raw_org_id, "header.org_id")
    if await is_duplicate(redis, str(org_id), AGENT_RESULTS, idempotency_key):
        return False
    try:
        return await _apply_agent_result(session, org_id, idempotency_key, value)
    except BaseException:
        # the transaction will roll back: forget the dedupe key so the redelivery is applied
        await redis.delete(f"nexus:{org_id}:dedupe:{AGENT_RESULTS}:{idempotency_key}")
        raise


async def _apply_agent_result(
    session: AsyncSession, org_id: uuid.UUID, idempotency_key: str, value: dict[str, Any]
) -> bool:
    await set_rls(session, org_id)
    # Claim the inbox row FIRST, in this transaction. `record_consumed` is ON CONFLICT DO
    # NOTHING after the effect, which only dedupes the inbox row itself: once the Redis key
    # has expired, a redelivery would write a second note. Claiming first makes the inbox the
    # authority; a rollback releases the claim together with the note.
    try:
        claimed = (
            await session.execute(
                text(
                    "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                    "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, idempotency_key) "
                    "DO NOTHING RETURNING 1"
                ),
                {"g": CONSUMER_GROUP, "o": str(org_id), "k": idempotency_key},
            )
        ).first()
    except IntegrityError as exc:
        # `inbox.org_id` references `organizations`: a tenant that does not exist (deleted, or
        # never real) can never apply, so it is poison. Raised as IntegrityError it would crash
        # the loop and block the partition on every restart. Found by the 2026-11-08 run.
        raise PoisonMessage(f"agent-results names unknown tenant {org_id}") from exc
    if claimed is None:
        return False
    agent, kind = str(value.get("agent")), str(value.get("kind"))
    candidate_table = _CANDIDATE_TABLES.get((agent, kind))
    is_suggestion = (agent, kind) == _SUGGESTION
    is_context_fill = (agent, kind) == _CONTEXT_FILL
    is_meeting_update = (agent, kind) == _MEETING_UPDATE
    is_minutes = (agent, kind) == _MINUTES
    if (
        candidate_table is None
        and not is_suggestion
        and not is_context_fill
        and not is_meeting_update
        and not is_minutes
        and (agent != "PersonalListenerAgent" or kind != "note")
    ):
        # Anything else - including an agent class or kind this consumer has never heard of -
        # is claimed in the inbox and otherwise ignored, never dead-lettered: the decoder reads
        # with the WRITER's schema (no reader schema is passed to AvroDeserializer), so a new
        # enum symbol arrives here as a plain string rather than failing the decode.
        return False

    meeting_id = _uuid(value.get("meeting_id"), "meeting_id")
    user_id = _uuid(value.get("user_id"), "user_id")
    chunk_ids = [str(_uuid(c, "source_chunk_ids[]")) for c in value.get("source_chunk_ids") or []]
    # A responder-side security entry carries the nil meeting when the requester's meeting is
    # unknown (P10 contract 5.9). Only a context_fill may, and only the nil uuid: any other
    # meeting id is checked against the tenant like every other result.
    unknown_meeting = is_context_fill and meeting_id == NIL_UUID
    # The shared minutes are for nobody in particular: they carry the nil user.
    shared_minutes = is_minutes and user_id == NIL_UUID
    visible = (
        await session.execute(
            text(
                "SELECT (SELECT count(*) FROM meetings WHERE id = :m AND deleted_at IS NULL), "
                "(SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL), "
                "(SELECT agent_confidence_overrides FROM tenant_settings)"
            ),
            {"m": str(meeting_id), "u": str(user_id)},
        )
    ).one()
    if (not visible[0] and not unknown_meeting) or (not visible[1] and not shared_minutes):
        raise PoisonMessage(
            f"agent-results names a meeting or user not in tenant {org_id}: "
            f"meeting={meeting_id} user={user_id}"
        )
    if is_meeting_update:
        return await _apply_agenda_updates(session, meeting_id, value.get("payload_json"))
    if is_minutes:
        return await _persist_minutes(
            session, org_id, meeting_id, None if shared_minutes else user_id, value
        )
    # The tenant's thresholds moved by the confirm/dismiss calibration (polish wave 2): the
    # person's own for a note, suggestion or context fill, the workspace's otherwise. The same
    # numbers `GET /internal/agent-policy` gives the agent and the reload routes apply.
    surface, silent = await band_for(session, agent, user_id, visible[2], have_overrides=True)
    if is_context_fill:
        return await _persist_collaborations(
            session,
            org_id,
            meeting_id,
            user_id,
            str(value.get("window_id") or "") or None,
            (surface, silent),
            value.get("payload_json"),
        )
    if is_suggestion:
        return await _persist_suggestions(
            session, org_id, meeting_id, user_id, silent, value.get("payload_json")
        )
    if candidate_table is not None:
        return await _persist_candidates(
            session, org_id, meeting_id, candidate_table, silent, value.get("payload_json")
        )
    confidence = float(value.get("confidence") or 0.0)
    content = str(value.get("content") or "").strip()
    written = await _persist_commitments(
        session,
        org_id,
        meeting_id,
        user_id,
        str(value.get("window_id") or ""),
        value.get("payload_json"),
    )
    if content and confidence >= silent:
        await session.execute(
            text(
                "INSERT INTO worklogs (org_id, meeting_id, user_id, window_id, content, "
                f"relevance_score, confidence, created_by_agent, source_chunk_ids, surfaced, "
                f"{_MOMENT_SQL}) VALUES (:o, :m, :u, :w, :c, :r, :conf, true, "
                f"CAST(:chunks AS uuid[]), :s, {_MOMENT_VALUES})"
            ),
            {
                **await _moment(session, _payload_object(value.get("payload_json"))),
                "o": str(org_id),
                "m": str(meeting_id),
                "u": str(user_id),
                "w": str(value.get("window_id") or ""),
                "c": content,
                "r": max(0.0, min(1.0, float(value.get("relevance_score") or 0.0))),
                "conf": max(0.0, min(1.0, confidence)),
                "chunks": chunk_ids,
                "s": confidence > surface,
            },
        )
        written = True
    return written


def _candidate_confidence(raw: Any) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        return None
    c = float(raw)
    return c if 0.0 <= c <= 1.0 else None


def _optional_uuid(raw: Any) -> uuid.UUID | None:
    if not isinstance(raw, str):
        return None
    try:
        return uuid.UUID(raw)
    except ValueError:
        return None


def _optional_date(raw: Any) -> date | None:
    if not isinstance(raw, str):
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


MAX_WHY_CHARS = 200
MAX_QUOTE_CHARS = 300
_MOMENT_SQL = "moment_id, why, quote, quote_speaker_id, quote_start_ms, quote_end_ms"
_MOMENT_VALUES = ":mo, :why, :qt, :qs, :qa, :qb"


def _ms(raw: Any) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        return None
    return raw


async def _moment(session: AsyncSession, item: Any) -> dict[str, Any]:
    """Polish program (one card per moment): the moment fields of one payload item as SQL
    params (`:mo :why :qt :qs :qa :qb`), each null when absent or malformed. A quote speaker is
    a `users.id` this tenant can see, or null (gap 31: never store another tenant's id)."""
    item = item if isinstance(item, dict) else {}
    speaker = _optional_uuid(item.get("speaker_id"))
    if speaker is not None and not await is_visible(session, "users", speaker):
        speaker = None
    mo = _optional_uuid(item.get("moment_id"))
    return {
        "mo": str(mo) if mo else None,
        "why": _bounded_text(item.get("why"), MAX_WHY_CHARS),
        "qt": _bounded_text(item.get("quote"), MAX_QUOTE_CHARS),
        "qs": str(speaker) if speaker else None,
        "qa": _ms(item.get("start_ms")),
        "qb": _ms(item.get("end_ms")),
    }


async def _persist_candidates(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    table: str,
    silent: float,
    payload_json: Any,
) -> bool:
    """Insert each decision/action candidate as `status = 'proposed'`. True if any row is new.

    Phase 8 Part A contract section 2. NO outbox row and NO topic event, ever: a candidate is
    unconfirmed until a participant presses Confirm (`routes/candidates.py`, the only producer
    of `decisions` / `actions`). The candidate's own deterministic `id` is the primary key, so
    a replayed window that slipped past the inbox is `ON CONFLICT (id) DO NOTHING`.

    A payload that is not JSON is poison (the producer is broken); a candidate with a bad
    shape is skipped and the rest are applied. The band is re-derived from the tenant's
    thresholds here, never trusted from the payload's `surfaced`.

    Polish program (memory inside the meeting): a candidate with `updates_candidate_id` is an
    UPDATE of an existing card of this meeting (`_apply_update`), not a new row. Every row
    stores the candidate's moment fields (`_moment`).
    """
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError as exc:
        raise PoisonMessage("agent-results payload_json is not JSON") from exc
    if payload is None:
        raise PoisonMessage("agent-results candidate result has no payload_json string")
    candidates = payload.get("candidates") if isinstance(payload, dict) else None
    if not isinstance(candidates, list):
        return False
    written = False
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        cand_id = _optional_uuid(cand.get("id"))
        content = cand.get("content")
        confidence = _candidate_confidence(cand.get("confidence"))
        if cand_id is None or not isinstance(content, str) or confidence is None:
            continue
        content = content.strip()
        if not content or confidence < silent:
            continue
        params: dict[str, Any] = {
            "id": str(cand_id),
            "o": str(org_id),
            "m": str(meeting_id),
            "c": content,
            "conf": confidence,
            **await _moment(session, cand),
        }
        if table == "decisions":
            rationale = cand.get("rationale")
            params["r"] = (rationale.strip() or None) if isinstance(rationale, str) else None
            sql = (
                "INSERT INTO decisions (id, org_id, meeting_id, content, rationale, confidence, "
                f"status, {_MOMENT_SQL}) VALUES (:id, :o, :m, :c, :r, :conf, 'proposed', "
                f"{_MOMENT_VALUES}) ON CONFLICT (id) DO NOTHING RETURNING 1"
            )
        else:
            # A foreign key resolves another tenant's user with RLS bypassed (gap 31): an
            # assignee this tenant cannot see is dropped, never stored. ADR-0039 C4: the
            # ActionAgent's `suggested_owner_user_id` (from the org's topic owners) is stored as
            # the candidate's assignee when the agent named none. A proposed action's assignee
            # IS the model's assignee suggestion: the confirming person can change it.
            assignee = _optional_uuid(cand.get("assignee_id")) or _optional_uuid(
                cand.get("suggested_owner_user_id")
            )
            if assignee is not None and not await is_visible(session, "users", assignee):
                assignee = None
            params["a"] = str(assignee) if assignee else None
            params["d"] = _optional_date(cand.get("due_date"))
            sql = (
                "INSERT INTO action_items (id, org_id, meeting_id, content, assignee_id, "
                f"due_date, confidence, status, {_MOMENT_SQL}) VALUES (:id, :o, :m, :c, :a, "
                f":d, :conf, 'proposed', {_MOMENT_VALUES}) ON CONFLICT (id) DO NOTHING RETURNING 1"
            )
        target = _optional_uuid(cand.get("updates_candidate_id"))
        if target is not None:
            applied = await _apply_update(session, table, meeting_id, target, params)
            if applied is not None:
                written = written or applied
                continue  # the card exists: updated, or left alone (resolved / edited)
            params["id"] = str(target)  # the card was never stored (below silent): create it
        if (await session.execute(text(sql), params)).first() is not None:
            written = True
    return written


async def _apply_update(
    session: AsyncSession,
    table: str,
    meeting_id: uuid.UUID,
    target: uuid.UUID,
    params: dict[str, Any],
) -> bool | None:
    """Apply an agent's update to the existing card `target` of this meeting. None when there
    is no such card (the caller inserts it), True when it changed, False when it is left alone:
    only a `proposed` card no person has edited (`edited_at` null) is ever updated, so an agent
    never reopens a confirmed or dismissed card and never overwrites a person's words.

    New text, confidence, the latest words (quote, speaker, time) and the why line replace the
    old; a rationale, an assignee and a due date only fill in or change, never clear (a
    refinement that does not repeat the owner keeps the owner); the moment stays the card's
    first, so every link to it (a promise, a note) still holds. The live card is updated in the
    room by realtime-gateway from the same `agent-results` record (the candidate keeps its id).
    """
    row = (
        await session.execute(
            text(
                f"SELECT status, edited_at FROM {table} "  # noqa: S608 - closed set of tables
                "WHERE id = :t AND meeting_id = :m AND deleted_at IS NULL FOR UPDATE"
            ),
            {"t": str(target), "m": str(meeting_id)},
        )
    ).first()
    if row is None:
        return None
    if row[0] != "proposed" or row[1] is not None:
        return False
    if table == "decisions":
        extra = "rationale = COALESCE(:r, rationale)"
    else:
        extra = "assignee_id = COALESCE(:a, assignee_id), due_date = COALESCE(:d, due_date)"
    await session.execute(
        text(
            f"UPDATE {table} SET content = :c, confidence = :conf, {extra}, "  # noqa: S608
            "moment_id = COALESCE(moment_id, :mo), why = COALESCE(:why, why), "
            "quote = COALESCE(:qt, quote), quote_speaker_id = COALESCE(:qs, quote_speaker_id), "
            "quote_start_ms = COALESCE(:qa, quote_start_ms), "
            "quote_end_ms = COALESCE(:qb, quote_end_ms) WHERE id = :t"
        ),
        {**params, "t": str(target)},
    )
    return True


async def _persist_commitments(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID,
    window_id: str,
    payload_json: Any,
) -> bool:
    """ADR-0039 B6: each `payload_json.commitments` entry of a PersonalListenerAgent record
    becomes an `open` `commitments` row for the record's `user_id` (the only person who ever
    sees it). True if any row is new.

    Tolerant on purpose: notes predate commitments, so a payload that is absent, not JSON, or
    has no `commitments` list writes nothing and never poisons the note. A bad entry is skipped,
    the rest applied, at most `MAX_COMMITMENTS_PER_RECORD`. Not gated on the note's confidence:
    a promise is captured as said. Idempotent under redelivery twice over: the caller's inbox
    claim, and the unique index on `(meeting_id, user_id, source_window_id, md5(content))`
    (`ON CONFLICT DO NOTHING`) for a replay whose inbox row is gone.
    """
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError:
        return False
    entries = payload.get("commitments") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        return False
    written = False
    for entry in entries[:MAX_COMMITMENTS_PER_RECORD]:
        if not isinstance(entry, dict):
            continue
        content = _bounded_text(entry.get("content"), MAX_COMMITMENT_CHARS)
        if content is None:
            continue
        inserted = (
            await session.execute(
                text(
                    "INSERT INTO commitments (org_id, user_id, meeting_id, content, due_hint, "
                    "source_window_id, moment_id) VALUES (:o, :u, :m, :c, :d, :w, :mo) "
                    "ON CONFLICT DO NOTHING RETURNING 1"
                ),
                {
                    "o": str(org_id),
                    "u": str(user_id),
                    "m": str(meeting_id),
                    "c": content,
                    "d": _bounded_text(entry.get("due_hint"), MAX_DUE_HINT_CHARS),
                    "w": window_id,
                    # polish: the moment the promise was made (a confirmed action of the same
                    # moment for this person closes it, routes/candidates.py)
                    "mo": (str(mo) if (mo := _optional_uuid(entry.get("moment_id"))) else None),
                },
            )
        ).first()
        written = written or inserted is not None
    return written


def _payload_object(payload_json: Any) -> dict[str, Any]:
    """The payload as an object, {} when absent or not JSON (a note never fails on it)."""
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _payload_list(payload_json: Any, key: str) -> list[Any] | None:
    """`payload_json[key]` as a list; poison if the payload is not a JSON string."""
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError as exc:
        raise PoisonMessage("agent-results payload_json is not JSON") from exc
    if payload is None:
        raise PoisonMessage(f"agent-results {key} result has no payload_json string")
    items = payload.get(key) if isinstance(payload, dict) else None
    return items if isinstance(items, list) else None


def _priority(raw: Any) -> int | None:
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    return raw if 1 <= raw <= 3 else None


def _citations(raw: Any) -> list[dict[str, Any]]:
    """Only the four ADR-0033 D3 kinds, scalar values only, at most `MAX_CITATIONS`."""
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for c in raw:
        if isinstance(c, dict) and c.get("kind") in _CITATION_KINDS:
            out.append(
                {str(k): v for k, v in c.items() if isinstance(v, (str, int)) and len(str(v)) < 200}
            )
        if len(out) == MAX_CITATIONS:
            break
    return out


async def _persist_suggestions(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID,
    silent: float,
    payload_json: Any,
) -> bool:
    """Insert each QuestionAgent suggestion as an `open` `agent_suggestions` row for `user_id`
    (the participant the agent ran for). True if any row is new. ADR-0034 D3-D5.

    The suggestion's deterministic id is the row id. The table is partitioned, so its primary
    key is `(id, created_at)` and `ON CONFLICT (id)` is impossible: the inbox claim made by the
    caller in this same transaction is what makes a redelivery a no-op, and the `NOT EXISTS`
    guards a replay whose inbox row was lost. Below the tenant's `silent` threshold nothing is
    written; the surface band is applied when the route reads (`confidence` is stored). A bad
    suggestion is skipped, the rest are applied. Nothing is published, ever.
    """
    suggestions = _payload_list(payload_json, "suggestions")
    if suggestions is None:
        return False
    written = False
    for s in suggestions:
        if not isinstance(s, dict):
            continue
        sid = _optional_uuid(s.get("id"))
        question = s.get("question")
        confidence = _candidate_confidence(s.get("confidence"))
        priority = _priority(s.get("priority"))
        if sid is None or not isinstance(question, str) or confidence is None or priority is None:
            continue
        question = question.strip()[:MAX_SUGGESTION_CHARS]
        if not question or confidence < silent:
            continue
        rationale = s.get("rationale")
        inserted = (
            await session.execute(
                text(
                    "INSERT INTO agent_suggestions (id, org_id, user_id, meeting_id, question, "
                    f"priority, status, confidence, rationale, citations, {_MOMENT_SQL}) "
                    "SELECT :id, :o, :u, :m, :q, :p, 'open', :c, :r, CAST(:cit AS jsonb), "
                    f"{_MOMENT_VALUES} "
                    "WHERE NOT EXISTS (SELECT 1 FROM agent_suggestions WHERE id = :id) "
                    "ON CONFLICT DO NOTHING RETURNING 1"
                ),
                {
                    **await _moment(session, s),
                    "id": str(sid),
                    "o": str(org_id),
                    "u": str(user_id),
                    "m": str(meeting_id),
                    "q": question,
                    "p": priority,
                    "c": confidence,
                    "r": (
                        (rationale.strip()[:MAX_SUGGESTION_CHARS] or None)
                        if isinstance(rationale, str)
                        else None
                    ),
                    "cit": json.dumps(_citations(s.get("citations"))),
                },
            )
        ).first()
        written = written or inserted is not None
    return written


async def _apply_agenda_updates(
    session: AsyncSession, meeting_id: uuid.UUID, payload_json: Any
) -> bool:
    """Apply a MeetingAgent `meeting_update`'s `updates` to `meeting_agenda_items` (ADR-0038 D7).
    True if any item changed.

    Per entry, in SQL so the rules hold against the row as it is now, not as it was read:
    - only an item OF THIS MEETING (`meeting_id` in the predicate; RLS already confines it to
      the tenant), so an id from another meeting is a no-op, not a cross-meeting write;
    - never one whose `status_source = 'host'`: the host wins, for good;
    - never `covered` back to `in_progress`;
    - sets `status_source = 'agent'` and `agent_note` (null when the entry has no note).
    A malformed entry is skipped and the rest applied, like suggestions. Re-applying the same
    record changes nothing (the inbox claim already makes a redelivery a no-op; the WHERE clause
    makes a replay with a lost inbox row one too). The `nudge` is ignored here.
    """
    updates = _payload_list(payload_json, "updates")
    if updates is None:
        return False
    changed = False
    seen: set[uuid.UUID] = set()
    for u in updates[:MAX_AGENDA_UPDATES]:
        if not isinstance(u, dict):
            continue
        item_id = _optional_uuid(u.get("item_id"))
        new_status = u.get("status")
        if item_id is None or item_id in seen or new_status not in _AGENT_AGENDA_STATUSES:
            continue
        seen.add(item_id)
        note = _bounded_text(u.get("note"), MAX_AGENDA_NOTE_CHARS)
        updated = (
            await session.execute(
                text(
                    "UPDATE meeting_agenda_items SET status = CAST(:s AS text), "
                    "status_source = 'agent', agent_note = CAST(:n AS text), "
                    # The item's clock (migration d0e1f2a3b4c5), the same rule the host's PATCH
                    # applies: starting stamps `started_at` once, covering stamps `ended_at`.
                    "started_at = CASE WHEN CAST(:s AS text) = 'in_progress' "
                    "  THEN COALESCE(started_at, now()) ELSE started_at END, "
                    "ended_at = CASE WHEN CAST(:s AS text) = 'covered' THEN now() "
                    "  ELSE NULL END, "
                    "updated_at = now() "
                    "WHERE id = :id AND meeting_id = :m "
                    "AND status_source IS DISTINCT FROM 'host' "
                    "AND NOT (status = 'covered' AND CAST(:s AS text) = 'in_progress') "
                    "AND NOT (status = CAST(:s AS text) AND status_source = 'agent' "
                    "         AND agent_note IS NOT DISTINCT FROM CAST(:n AS text)) "
                    "RETURNING 1"
                ),
                {"s": new_status, "n": note, "id": str(item_id), "m": str(meeting_id)},
            )
        ).first()
        changed = changed or updated is not None
    return changed


async def _persist_minutes(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID | None,
    value: dict[str, Any],
) -> bool:
    """Upsert one set of minutes (`minutes.persist`: validated, clipped, participants only).
    For the shared minutes, a follow-up waiting for them is queued to go out after commit."""
    payload = minutes_mod.parse_payload(value.get("payload_json"))
    if payload is None:
        return False  # nothing to file; the inbox claim keeps a redelivery a no-op
    written = await minutes_mod.persist(session, org_id, meeting_id, user_id, payload)
    if written and user_id is not None and payload.get("status") != "unavailable":
        # Beyond the meeting: the person's own minutes are in, so their agent can brief them.
        briefing = await briefings_mod.create_after_meeting(session, org_id, meeting_id, user_id)
        if briefing is not None:
            AFTER_COMMIT.append(lambda: _ring_briefing(org_id, briefing))
    if written and user_id is None:
        waiting = (
            await session.execute(
                text(
                    "UPDATE meeting_followups SET awaiting_minutes_since = NULL, "
                    "updated_at = now() WHERE meeting_id = :m "
                    "AND awaiting_minutes_since IS NOT NULL AND enabled RETURNING 1"
                ),
                {"m": str(meeting_id)},
            )
        ).first()
        if waiting is not None:
            AFTER_COMMIT.append(lambda: _send_follow_up(org_id, meeting_id))
    return written


async def _ring_briefing(org_id: uuid.UUID, briefing_id: uuid.UUID) -> None:
    await briefings_mod.ring(org_id, briefing_id)


async def _send_follow_up(org_id: uuid.UUID, meeting_id: uuid.UUID) -> None:
    from services.core_api.routes.followups import dispatch

    try:
        await dispatch(org_id, meeting_id, actor_id=None, automatic=True)
    except Exception:  # noqa: BLE001 - the host can still send it; never stop the consumer
        logger.exception("deferred follow-up for meeting %s failed", meeting_id)


async def drain_after_commit() -> None:
    """Run (and clear) the work queued by the message that just committed."""
    jobs = list(AFTER_COMMIT)
    AFTER_COMMIT.clear()
    for job in jobs:
        await job()


def _bounded_text(raw: Any, limit: int) -> str | None:
    if not isinstance(raw, str):
        return None
    return raw.strip()[:limit] or None


async def _persist_collaborations(
    session: AsyncSession,
    org_id: uuid.UUID,
    meeting_id: uuid.UUID,
    user_id: uuid.UUID,
    window_id: str | None,
    band: tuple[float, float],
    payload_json: Any,
) -> bool:
    """One `agent_collaborations` row per ContextGapAgent collaboration entry (P10 contract 6).

    The entry's deterministic uuid5 `id` is the primary key, so a replay that slipped past the
    inbox is `ON CONFLICT (id) DO NOTHING`, and the audit row is written only for a row that is
    actually new (one per entry, `actor_type = 'agent'`, acting for the requester). The
    requester is the result's own `user_id`, already checked against this tenant; an entry
    naming anyone else is skipped, never stored. A malformed entry is skipped and the rest are
    applied; a payload that is not JSON is poison.

    The fill (`fill_content`, `confidence`, `surfaced`, `rationale`, `citations`) goes only on
    the row whose id is `fill.id`. Its band is re-derived from the tenant's
    `agent_confidence_overrides`, never trusted from the payload: below `silent` the fill fields
    are not written (the collaboration row still is), above `surface` it is surfaced.
    """
    surface, silent = band
    try:
        payload = json.loads(payload_json) if isinstance(payload_json, str) else None
    except ValueError as exc:
        raise PoisonMessage("agent-results payload_json is not JSON") from exc
    if not isinstance(payload, dict):
        raise PoisonMessage("agent-results context_fill result has no payload_json object")
    # Polish wave 3: an absent person's agent answered from what they pre-approved.
    logged = meeting_id != NIL_UUID and await log_delegated_answer(
        session, org_id, meeting_id, window_id, payload
    )
    entries = payload.get("collaborations")
    if not isinstance(entries, list):
        return logged
    fill = payload.get("fill") if isinstance(payload.get("fill"), dict) else None
    fill_id = _optional_uuid(fill.get("id")) if fill else None
    written = False
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        entry_id = _optional_uuid(entry.get("id"))
        hop, result = entry.get("hop"), entry.get("result")
        success, latency = entry.get("success"), entry.get("latency_ms")
        query = _bounded_text(entry.get("query"), MAX_QUERY_CHARS)
        query_class, responder = entry.get("query_class"), entry.get("responder")
        if (
            entry_id is None
            or hop not in _HOPS
            or result not in _COLLAB_AUDIT
            or not isinstance(success, bool)
            or isinstance(latency, bool)
            or not isinstance(latency, int)
            or latency < 0
            or query is None
            or not isinstance(query_class, str)
            or not isinstance(responder, str)
            or not responder
            or _optional_uuid(entry.get("requester")) != user_id
        ):
            continue
        target_scope = entry.get("target_scope")
        correlation_id = entry.get("correlation_id")
        params: dict[str, Any] = {
            "id": str(entry_id),
            "o": str(org_id),
            "m": str(meeting_id),
            "w": window_id,
            "corr": correlation_id if isinstance(correlation_id, str) else None,
            "req": str(user_id),
            "resp": responder[:200],
            "hop": hop,
            "q": query,
            "qc": query_class[:64],
            "ts": target_scope[:64] if isinstance(target_scope, str) else None,
            "res": result,
            "ok": success,
            "lat": min(latency, 2**31 - 1),
            "fc": None,
            "conf": None,
            "surf": False,
            "rat": None,
            "cit": "[]",
        }
        if fill is not None and fill_id == entry_id:
            confidence = _candidate_confidence(fill.get("confidence"))
            content = _bounded_text(fill.get("content"), MAX_FILL_CHARS)
            if confidence is not None and content is not None and confidence >= silent:
                params.update(
                    fc=content,
                    conf=confidence,
                    surf=confidence > surface,
                    rat=_bounded_text(fill.get("rationale"), MAX_SUGGESTION_CHARS),
                    cit=json.dumps(_citations(fill.get("citations"))),
                )
        inserted = (
            await session.execute(
                text(
                    "INSERT INTO agent_collaborations (id, org_id, meeting_id, window_id, "
                    "correlation_id, requester, responder, hop, query, query_class, "
                    "target_scope, result, success, latency_ms, fill_content, confidence, "
                    "surfaced, rationale, citations) VALUES (:id, :o, :m, :w, :corr, :req, "
                    ":resp, :hop, :q, :qc, :ts, :res, :ok, :lat, :fc, :conf, :surf, :rat, "
                    "CAST(:cit AS jsonb)) ON CONFLICT (id) DO NOTHING RETURNING 1"
                ),
                params,
            )
        ).first()
        if inserted is None:
            continue
        written = True
        await audit(
            session,
            org_id=org_id,
            actor_id=user_id,
            actor_type="agent",
            action=_COLLAB_AUDIT[result],
            target_type="agent_collaboration",
            target_id=entry_id,
            metadata={
                "agent": "ContextGapAgent",
                "meeting_id": str(meeting_id),
                "hop": hop,
                "responder": params["resp"],
                "query_class": params["qc"],
                "target_scope": params["ts"],
                "result": result,
                "success": success,
                "correlation_id": params["corr"],
            },
        )
    return written or logged


def decode(deserializer: Any, raw_value: bytes | None, ctx: Any) -> Any:
    """Decode one record, turning a record that can never decode into `PoisonMessage`.

    A malformed value (a non-UUID string in a `uuid` field, a truncated or foreign payload) raises
    `ValueError`/`TypeError`/`EOFError`/`IndexError` from the Avro reader on every attempt. Left
    uncaught it killed the process, and every restart re-read the same offset, so one bad record
    stopped this consumer for every tenant. Transient failures (the Schema Registry unreachable)
    are other exception types and still propagate, so the record is retried, not dead-lettered.
    """
    try:
        return deserializer(raw_value, ctx)
    except (ValueError, TypeError, EOFError, IndexError) as exc:
        raise PoisonMessage(f"undecodable record: {exc}") from exc


async def run_consumer() -> None:  # pragma: no cover - thin harness, see module docstring
    from confluent_kafka import Consumer
    from confluent_kafka import Producer as KafkaProducer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroDeserializer
    from confluent_kafka.serialization import MessageField, SerializationContext
    from redis.asyncio import from_url as redis_from_url

    from libs.shared.db import get_sessionmaker

    set_service("core-api-consumer")
    serve_metrics()  # no-op unless METRICS_PORT is set
    brokers = os.environ.get("REDPANDA_BROKERS", "localhost:9092")
    sr_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8085")
    redis: Redis[bytes] = redis_from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379"), decode_responses=False
    )

    consumer = Consumer(
        {
            "bootstrap.servers": brokers,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    dlq_producer = KafkaProducer(
        {"bootstrap.servers": brokers, "acks": "all", "enable.idempotence": True}
    )
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": sr_url}))
    session_factory = get_sessionmaker()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("core-api consumer error: %s", msg.error())
                continue
            raw_value = msg.value()
            topic_name = msg.topic() or TOPIC
            _ts_type, _ts = msg.timestamp()
            observe_consumer_lag(CONSUMER_GROUP, topic_name, _ts if _ts_type else None)
            handler = {
                AGENT_RESULTS: handle_agent_result,
                INTEGRATION_EVENTS: handle_integration_event,
                AGENT_FEEDBACK: handle_agent_feedback,
            }.get(topic_name, handle_presence_event)
            try:
                value = decode(
                    deserializer, raw_value, SerializationContext(topic_name, MessageField.VALUE)
                )
                AFTER_COMMIT.clear()
                async with session_factory() as session, session.begin():
                    applied = await handler(session, redis, value)
                await drain_after_commit()
                record_processed(CONSUMER_GROUP, topic_name, "applied" if applied else "duplicate")
            except (PoisonMessage, IntegrationEventPoison, FeedbackPoison) as exc:
                logger.error("%s: poison message routed to DLQ: %s", topic_name, exc)
                dlq_producer.produce(
                    topic=f"{topic_name}.dlq",
                    key=msg.key(),
                    value=json.dumps(
                        {"error": str(exc), "raw": raw_value.hex() if raw_value else None}
                    ).encode(),
                )
                dlq_producer.flush(10)
                record_dlq(CONSUMER_GROUP, topic_name, "POISON")
                record_processed(CONSUMER_GROUP, topic_name, "dlq")
            consumer.commit(message=msg)
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_consumer())
