"""Memory across meetings (polish program wave 2): what each person cares about and has open.

`person_memory` holds, per person, what is LEARNED: weighted topics and recurring questions, plus
the person's own edits (pinned topics, muted topics, the learning switch). What is OPEN is never
copied into it: open actions (`action_items` confirmed and assigned to them) and open promises
(`commitments`) are read live whenever the memory is served, so they are never stale.

Learning is incremental, in core-api's `agent-feedback` consumer (`apply_feedback_event`): each
feedback event the person gives moves the weights of the topics in the item's text, and an asked
or saved suggestion counts toward their recurring questions. A confirmed action also teaches its
assignee. Weights decay with a `TOPIC_HALF_LIFE_DAYS` half-life, so topics follow recent meetings.

Topics come from the item's text without a model call, cheapest first:
1. the org's own vocabulary that the text names: company-brain glossary terms (and their
   aliases, mapped to the term) and topic-owner topics;
2. otherwise up to `MAX_PHRASES_PER_ITEM` key phrases (RAKE-style: runs of content words between
   stopwords and punctuation, one to three words).

Served three ways, all owner-scoped:
- `GET/PATCH/DELETE /api/v1/me/memory` (`routes/me_memory.py`): the full view, editable;
- `GET /internal/person-memory/{user_id}` (`routes/internal_learning.py`) for
  agent-runtime-service: the compact form the PersonalListenerAgent and QuestionAgent prompts
  use (empty when the person switched learning off);
- the bff brief's `memory` block (`brief_memory`).
"""

from __future__ import annotations

import json
import logging
import math
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.inbox import is_duplicate
from libs.shared.tenancy import set_rls

logger = logging.getLogger(__name__)

TOPIC = "agent-feedback"
CONSUMER_GROUP = "core-api"

MAX_TOPICS = 40  # stored
MAX_QUESTIONS = 20  # stored
MAX_MUTED = 100
MAX_PINNED = 20
MAX_LABEL = 80
MAX_QUESTION = 300
MAX_PHRASES_PER_ITEM = 2
TOPIC_HALF_LIFE_DAYS = 30.0
MIN_WEIGHT = 0.05
PINNED_WEIGHT = 5.0
QUESTION_MERGE = 0.6  # token Jaccard at or above which two questions are the same question

# How much one feedback moves the topics in its item: by action, scaled by kind.
ACTION_WEIGHT = {"confirm": 1.0, "edit": 1.0, "ask": 1.0, "save": 0.5, "dismiss": -0.5}
KIND_SCALE = {
    "decision": 1.0,
    "action": 1.0,
    "note": 1.0,
    "suggestion": 0.8,
    "context_fill": 0.6,
}

# Served sizes.
SERVE_TOPICS = 12
SERVE_QUESTIONS = 10
SERVE_OPEN = 20
AGENT_TOPICS = 8
AGENT_QUESTIONS = 5
AGENT_OPEN = 5
BRIEF_TOPICS = 5
BRIEF_QUESTIONS = 3
BRIEF_PROMISES = 5

_WORD = re.compile(r"[A-Za-z][A-Za-z0-9'&+-]*")
_SPLIT = re.compile(r"[.,;:!?()\[\]{}\"/\n–—]+")
_STOP = frozenset("""
    a about above after again against all also am an and any are as at be because been before
    being below between both but by can could did do does doing done down during each either
    else ever every few for from further get gets getting go goes going gone got had has have
    having he her here hers herself him himself his how i if in into is it its itself just
    keep let lets like make makes made may me might more most must my myself need needs next
    no nor not now of off on once only or other our ours ourselves out over own per please
    put re really same say says said see seem seems shall she should so some still such sure
    take than that the their theirs them themselves then there these they thing things this
    those though through to today tomorrow too under until up upon us use used using very via
    want wants was we well were what when where whether which while who whom why will with
    within without would yes yet you your yours yourself yourselves week weeks month months
    year years day days monday tuesday wednesday thursday friday saturday sunday january
    february march april june july august september october november december end start
    first last new one two three four five team everyone someone somebody anyone ok okay
    agree agreed agreement decide decided decision confirm confirmed follow following
    send sent share shared review reviewed update updated check checked discuss discussed
    talk talked call called ask asked tell told look looking work working plan planned
    already able around back bit lot lots maybe probably soon later instead across
    participant you'll we'll i'll it's that's let's don't doesn't won't can't
    """.split())


# --- pure helpers -----------------------------------------------------------------------------


def norm(label: str) -> str:
    return " ".join(label.lower().split())


def clean_label(raw: object, limit: int = MAX_LABEL) -> str | None:
    if not isinstance(raw, str):
        return None
    flat = " ".join(raw.split()).strip(" -")
    return flat[:limit].strip() or None


def _contains(haystack: str, needle: str) -> bool:
    return re.search(rf"(?<![\w-]){re.escape(needle)}(?![\w-])", haystack) is not None


def key_phrases(text_: str, limit: int = MAX_PHRASES_PER_ITEM) -> list[str]:
    """RAKE-style key phrases: runs of content words (not stopwords, at least 3 letters) between
    stopwords and punctuation, one to three words, longest and earliest first."""
    found: list[tuple[int, int, str]] = []
    for pos, chunk in enumerate(_SPLIT.split(text_)):
        run: list[str] = []
        for word in [*_WORD.findall(chunk), ""]:
            lw = word.lower()
            if word and lw not in _STOP and len(lw) >= 3 and not lw.isdigit():
                run.append(word)
                continue
            if run:
                phrase = " ".join(run[:3])
                found.append((-len(run[:3]), pos, phrase))
                run = []
    out: list[str] = []
    for _, _, phrase in sorted(found, key=lambda t: (t[0], t[1])):
        if norm(phrase) not in {norm(p) for p in out}:
            out.append(phrase)
        if len(out) == limit:
            break
    return out


def extract_topics(text_: str, vocabulary: Sequence[tuple[str, Sequence[str]]]) -> list[str]:
    """The topics an item's text is about: vocabulary terms it names (an alias counts for its
    term), else its key phrases."""
    lowered = text_.lower()
    hits: list[str] = []
    for term, aliases in vocabulary:
        names = [term, *aliases]
        if any(n and _contains(lowered, norm(n)) for n in names):
            if norm(term) not in {norm(h) for h in hits}:
                hits.append(term)
    return hits if hits else key_phrases(text_)


def parse_ts(raw: object) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        ts = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def decay(weight: float, since: datetime | None, now: datetime) -> float:
    if since is None:
        return weight
    days = max(0.0, (now - since).total_seconds() / 86400.0)
    return weight * math.pow(0.5, days / TOPIC_HALF_LIFE_DAYS)


def merge_topics(
    topics: list[dict[str, Any]],
    labels: Iterable[str],
    delta: float,
    muted: Iterable[str],
    now: datetime,
) -> list[dict[str, Any]]:
    """Decay every learned topic to `now`, add `delta` to each of `labels` (a new topic only on a
    positive delta), drop the faded ones and keep the `MAX_TOPICS` heaviest. Pinned topics
    never decay or fade; muted ones are never learned."""
    muted_set = {norm(m) for m in muted}
    by_key: dict[str, dict[str, Any]] = {}
    for t in topics:
        label = clean_label(t.get("label")) if isinstance(t, dict) else None
        if label is None:
            continue
        entry = dict(t)
        entry["label"] = label
        if not entry.get("pinned"):
            since = parse_ts(entry.get("decayed_at")) or parse_ts(entry.get("last_seen_at"))
            entry["weight"] = decay(float(entry.get("weight") or 0.0), since, now)
        entry["decayed_at"] = now.isoformat()
        by_key.setdefault(norm(label), entry)
    for raw in labels:
        label = clean_label(raw)
        if label is None or norm(label) in muted_set:
            continue
        key = norm(label)
        found = by_key.get(key)
        if found is None:
            if delta <= 0:
                continue
            found = {"label": label, "weight": 0.0, "mentions": 0, "pinned": False}
            found["decayed_at"] = now.isoformat()
            by_key[key] = found
        entry = found
        if not entry.get("pinned"):
            entry["weight"] = max(0.0, float(entry.get("weight") or 0.0) + delta)
        if delta > 0:
            entry["mentions"] = int(entry.get("mentions") or 0) + 1
        entry["last_seen_at"] = now.isoformat()
    kept = [
        {**e, "weight": round(float(e.get("weight") or 0.0), 4)}
        for e in by_key.values()
        if e.get("pinned") or float(e.get("weight") or 0.0) >= MIN_WEIGHT
    ]
    kept.sort(key=lambda e: (not e.get("pinned"), -float(e["weight"]), norm(e["label"])))
    return kept[:MAX_TOPICS]


def _tokens(question: str) -> set[str]:
    return {w.lower() for w in _WORD.findall(question) if w.lower() not in _STOP}


def merge_question(
    questions: list[dict[str, Any]], question: str, now: datetime
) -> list[dict[str, Any]]:
    """Count `question` toward the person's recurring questions: the same question asked again
    (token Jaccard >= `QUESTION_MERGE`) increments its count, otherwise it is added. Keeps the
    `MAX_QUESTIONS` most recent."""
    text_ = clean_label(question, MAX_QUESTION)
    if text_ is None:
        return questions
    mine = _tokens(text_)
    out = [dict(q) for q in questions if isinstance(q, dict) and isinstance(q.get("text"), str)]
    for q in out:
        theirs = _tokens(q["text"])
        union = mine | theirs
        if union and len(mine & theirs) / len(union) >= QUESTION_MERGE:
            q["count"] = int(q.get("count") or 1) + 1
            q["last_at"] = now.isoformat()
            q["text"] = text_
            break
    else:
        out.append({"text": text_, "count": 1, "last_at": now.isoformat()})
    out.sort(key=lambda q: str(q.get("last_at") or ""), reverse=True)
    return out[:MAX_QUESTIONS]


def ranked_questions(questions: list[Any], limit: int) -> list[dict[str, Any]]:
    """Recurring first (asked more than once), then most recent."""
    valid = [q for q in questions if isinstance(q, dict) and isinstance(q.get("text"), str)]
    valid.sort(key=lambda q: str(q.get("last_at") or ""), reverse=True)
    valid.sort(key=lambda q: -int(q.get("count") or 1))  # stable: recency within a count
    return valid[:limit]


def _jsonb(raw: Any) -> list[Any]:
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except ValueError:
            return []
        return value if isinstance(value, list) else []
    return []


# --- storage ------------------------------------------------------------------------------------


async def load(session: AsyncSession, user_id: uuid.UUID) -> dict[str, Any] | None:
    row = (
        (
            await session.execute(
                text(
                    "SELECT topics, questions, muted_topics, learning_enabled, created_at, "
                    "updated_at FROM person_memory WHERE user_id = :u"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    return {
        "topics": _jsonb(row["topics"]),
        "questions": _jsonb(row["questions"]),
        "muted_topics": [m for m in _jsonb(row["muted_topics"]) if isinstance(m, str)],
        "learning_enabled": bool(row["learning_enabled"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


async def save(
    session: AsyncSession, org_id: uuid.UUID, user_id: uuid.UUID, memory: Mapping[str, Any]
) -> None:
    await session.execute(
        text(
            "INSERT INTO person_memory (org_id, user_id, topics, questions, muted_topics, "
            "learning_enabled) VALUES (:o, :u, CAST(:t AS jsonb), CAST(:q AS jsonb), "
            "CAST(:m AS jsonb), :e) ON CONFLICT (org_id, user_id) DO UPDATE SET "
            "topics = EXCLUDED.topics, questions = EXCLUDED.questions, "
            "muted_topics = EXCLUDED.muted_topics, learning_enabled = EXCLUDED.learning_enabled, "
            "updated_at = now()"
        ),
        {
            "o": str(org_id),
            "u": str(user_id),
            "t": json.dumps(memory.get("topics") or []),
            "q": json.dumps(memory.get("questions") or []),
            "m": json.dumps(memory.get("muted_topics") or []),
            "e": bool(memory.get("learning_enabled", True)),
        },
    )


def empty() -> dict[str, Any]:
    return {"topics": [], "questions": [], "muted_topics": [], "learning_enabled": True}


async def vocabulary(session: AsyncSession) -> list[tuple[str, list[str]]]:
    """The org's own topic names: glossary terms with aliases, and topic-owner topics. Read
    under RLS (identity-tenant-service writes them; this is a read)."""
    rows = (
        await session.execute(
            text(
                "SELECT term, aliases FROM org_glossary_terms "
                "UNION ALL SELECT topic, ARRAY[]::text[] FROM topic_owners"
            )
        )
    ).all()
    return [(str(r[0]), [str(a) for a in (r[1] or [])]) for r in rows]


async def learn(
    session: AsyncSession,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    *,
    content: str | None,
    kind: str,
    action: str,
    vocab: Sequence[tuple[str, Sequence[str]]],
    now: datetime,
) -> bool:
    """Fold one feedback into `user_id`'s memory. True if the memory changed."""
    memory = await load(session, user_id) or empty()
    if not memory["learning_enabled"] or not content:
        return False
    delta = ACTION_WEIGHT.get(action, 0.0) * KIND_SCALE.get(kind, 0.5)
    labels = extract_topics(content, vocab)
    memory["topics"] = merge_topics(memory["topics"], labels, delta, memory["muted_topics"], now)
    if kind == "suggestion" and action in {"ask", "save"}:
        memory["questions"] = merge_question(memory["questions"], content, now)
    await save(session, org_id, user_id, memory)
    return True


# --- the agent-feedback consumer ------------------------------------------------------------


class FeedbackPoison(Exception):
    """An agent-feedback record that can never apply (no tenant, malformed ids)."""


def _uuid(raw: Any, what: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(raw))
    except ValueError as exc:
        raise FeedbackPoison(f"agent-feedback {what} is not a UUID: {raw!r}") from exc


async def handle_agent_feedback(
    session: AsyncSession, redis: Redis[bytes], value: dict[str, Any]
) -> bool:
    """Apply one decoded `agent-feedback` event to `person_memory`. True if a memory changed.

    The inbox claim comes first, in this transaction, so a redelivery (or a replay whose Redis
    key expired) is a no-op; a rollback releases the claim with the change. A person or meeting
    this tenant cannot see is skipped, never written."""
    header = value.get("header") or {}
    raw_org, key = header.get("org_id"), header.get("idempotency_key")
    if not raw_org or not key:
        raise FeedbackPoison("agent-feedback message missing header org_id/idempotency_key")
    org_id = _uuid(raw_org, "header.org_id")
    if await is_duplicate(redis, str(org_id), TOPIC, str(key)):
        return False
    try:
        return await _apply(session, org_id, str(key), value)
    except BaseException:
        await redis.delete(f"nexus:{org_id}:dedupe:{TOPIC}:{key}")
        raise


async def _apply(session: AsyncSession, org_id: uuid.UUID, key: str, value: dict[str, Any]) -> bool:
    await set_rls(session, org_id)
    try:
        claimed = (
            await session.execute(
                text(
                    "INSERT INTO inbox (consumer_group, org_id, idempotency_key) "
                    "VALUES (:g, :o, :k) ON CONFLICT (consumer_group, org_id, idempotency_key) "
                    "DO NOTHING RETURNING 1"
                ),
                {"g": CONSUMER_GROUP, "o": str(org_id), "k": key},
            )
        ).first()
    except IntegrityError as exc:
        raise FeedbackPoison(f"agent-feedback names unknown tenant {org_id}") from exc
    if claimed is None:
        return False
    user_id = _uuid(value.get("user_id"), "user_id")
    kind, action = str(value.get("kind") or ""), str(value.get("action") or "")
    content = value.get("content") if isinstance(value.get("content"), str) else None
    if action == "edit":
        content = _edited_text(value.get("edit_json")) or content
    known = (
        await session.execute(
            text("SELECT count(*) FROM users WHERE id = :u AND deleted_at IS NULL"),
            {"u": str(user_id)},
        )
    ).scalar_one()
    if not known or action not in ACTION_WEIGHT or not content:
        return False
    now = datetime.now(UTC)
    vocab = await vocabulary(session)
    changed = await learn(
        session, org_id, user_id, content=content, kind=kind, action=action, vocab=vocab, now=now
    )
    if kind == "action" and action == "confirm":
        # The assignee has it open now: it is about their work too.
        assignee = (
            await session.execute(
                text("SELECT assignee_id FROM action_items WHERE id = :id"),
                {"id": str(_uuid(value.get("candidate_id"), "candidate_id"))},
            )
        ).scalar()
        if assignee is not None and assignee != user_id:
            changed = (
                await learn(
                    session,
                    org_id,
                    assignee,
                    content=content,
                    kind=kind,
                    action=action,
                    vocab=vocab,
                    now=now,
                )
                or changed
            )
    return changed


def _edited_text(raw: Any) -> str | None:
    """The new text of an edit (`content` field of the diff), if it carried one."""
    try:
        diff = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        return None
    entry = diff.get("content") if isinstance(diff, dict) else None
    after = entry.get("after") if isinstance(entry, dict) else None
    return after if isinstance(after, str) and after else None


# --- reads ------------------------------------------------------------------------------------


async def open_actions(
    session: AsyncSession, user_id: uuid.UUID, limit: int
) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in (
            await session.execute(
                text(
                    "SELECT a.id, a.content, a.due_date, a.meeting_id FROM action_items a "
                    "LEFT JOIN meetings m ON m.id = a.meeting_id "
                    "WHERE a.assignee_id = :u AND a.status = 'confirmed' "
                    "AND a.deleted_at IS NULL AND (m.id IS NULL OR m.deleted_at IS NULL) "
                    "ORDER BY a.due_date ASC NULLS LAST, a.created_at DESC LIMIT :lim"
                ),
                {"u": str(user_id), "lim": limit},
            )
        )
        .mappings()
        .all()
    ]


async def open_promises(
    session: AsyncSession, user_id: uuid.UUID, limit: int
) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in (
            await session.execute(
                text(
                    "SELECT c.id, c.content, c.due_hint, c.meeting_id FROM commitments c "
                    "JOIN meetings m ON m.id = c.meeting_id WHERE c.user_id = :u "
                    "AND c.status = 'open' AND m.deleted_at IS NULL "
                    "ORDER BY c.created_at DESC LIMIT :lim"
                ),
                {"u": str(user_id), "lim": limit},
            )
        )
        .mappings()
        .all()
    ]


async def open_action_count(session: AsyncSession, user_id: uuid.UUID) -> int:
    return int(
        (
            await session.execute(
                text(
                    "SELECT count(*) FROM action_items WHERE assignee_id = :u "
                    "AND status = 'confirmed' AND deleted_at IS NULL"
                ),
                {"u": str(user_id)},
            )
        ).scalar_one()
    )


def top_topics(topics: list[Any], limit: int, boost: Sequence[str] = ()) -> list[dict[str, Any]]:
    """Pinned first, then the topics named in `boost` (e.g. this meeting's title and agenda),
    then by weight."""
    boost_text = " ".join(norm(b) for b in boost)
    valid = [t for t in topics if isinstance(t, dict) and isinstance(t.get("label"), str)]

    def rank(t: dict[str, Any]) -> tuple[bool, bool, float]:
        hit = bool(boost_text) and _contains(boost_text, norm(t["label"]))
        return (not t.get("pinned"), not hit, -float(t.get("weight") or 0.0))

    return sorted(valid, key=rank)[:limit]


async def brief_memory(
    session: AsyncSession, user_id: uuid.UUID, boost: Sequence[str]
) -> dict[str, Any] | None:
    """The brief's memory block, or None when there is nothing to show or learning is off."""
    memory = await load(session, user_id)
    promises = await open_promises(session, user_id, BRIEF_PROMISES)
    total = await open_action_count(session, user_id)
    if memory is not None and not memory["learning_enabled"]:
        memory = None
    topics = top_topics(memory["topics"], BRIEF_TOPICS, boost) if memory else []
    questions = ranked_questions(memory["questions"], BRIEF_QUESTIONS) if memory else []
    if not topics and not questions and not promises and not total:
        return None
    return {
        "topics": [t["label"] for t in topics],
        "recurring_questions": [q["text"] for q in questions if int(q.get("count") or 1) > 1],
        "open_promises": promises,
        "open_actions_total": total,
    }


async def forget(session: AsyncSession, user_id: uuid.UUID) -> None:
    """DELETE /api/v1/me/memory: the learned memory goes, and the text of the person's feedback
    (the guidance examples) is scrubbed. The counts stay: they only tune thresholds."""
    await session.execute(text("DELETE FROM person_memory WHERE user_id = :u"), {"u": str(user_id)})
    await session.execute(
        text(
            "UPDATE agent_feedback SET content = NULL, edit_diff = NULL WHERE user_id = :u "
            "AND (content IS NOT NULL OR edit_diff IS NOT NULL)"
        ),
        {"u": str(user_id)},
    )
