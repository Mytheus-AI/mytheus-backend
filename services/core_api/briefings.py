"""A briefing from your agent ("Beyond the meeting", Layer 1): the shapes, the source the agent
writes from, the validation of what it wrote, and the hand-off between core-api and the agent.

What it is. About 90 seconds of speech (about 220 words; 45 seconds when the person chose
`short`), written by the person's own agent in its persona's voice: what changed for them, who is
waiting on them, their promises, and the questions left open, including the ones carried to the
next agenda. Private to its person.

When. (1) After a meeting: core-api's `agent-results` consumer files the person's own minutes
(`minutes.persist`) and, when their briefings are on, creates one `meeting_end` briefing for that
meeting (`create_after_meeting`; at most one per meeting and person). (2) On demand: "Brief me"
(`POST /api/v1/briefings`), for a meeting or for "since my last briefing".

The hand-off (no new topic; core-api stays the one writer):
1. core-api commits a `pending` row and rings the doorbell after the commit: a Redis PUBLISH on
   `nexus:{org_id}:briefings:ring` with the briefing id (`ring`).
2. agent-runtime-service listens on `nexus:*:briefings:ring`, claims the briefing over the
   internal route `POST /internal/briefings/{id}/claim` (status `composing`, attempts + 1),
   which answers the source (`build_source`), writes the script through its router (budget
   gated, metered, never-record respected) and posts it back to
   `POST /internal/briefings/{id}/result`, validated here (`clean_segments`).
3. A missed ring (the runtime restarting) is recovered on read: any read that finds a briefing
   still `pending` after `RERING_AFTER_S`, or `composing` after `STALE_COMPOSING_S`, rings again;
   after `MAX_ATTEMPTS` it is `unavailable` (`timeout`) and the apps offer "Try again".
4. When the server voice is connected (`briefing_audio`), the audio is rendered after the commit
   and stored; the notification waits for it (or its failure) so a tap plays the real voice.
   Otherwise the person is notified at once (in-app, mirrored to their phone by
   notification-service): only for `meeting_end` briefings with something in them.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from libs.shared.cache import get_redis, redis_key
from libs.shared.db import get_sessionmaker
from libs.shared.events import EventEnvelope
from libs.shared.outbox import enqueue
from libs.shared.tenancy import set_rls
from services.core_api import briefing_audio, minutes
from services.core_api.records import person_name

logger = logging.getLogger(__name__)

PRODUCER = "core-api"
MAX_ATTEMPTS = 3
RERING_AFTER_S = 20
STALE_COMPOSING_S = 120
STALE_AUDIO_S = 180
SINCE_DAYS = 7
MAX_SEGMENTS = 12
MAX_SEGMENT_CHARS = 420
MAX_SOURCE_ITEMS = 8
WORDS_PER_MINUTE = 150
TARGET_WORDS = {"short": 110, "standard": 220}
MAX_DAY_MS = 24 * 3600 * 1000
ON_DEMAND_PER_HOUR = 12

# The languages the briefing can be written in: the caption-translation languages
# (mytheus-intelligence `agent_runtime_service/translation.py`, mytheus-ui CAPTION_LANGUAGES).
LANGUAGES: dict[str, str] = {
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "pt": "Portuguese",
    "it": "Italian",
    "nl": "Dutch",
    "hi": "Hindi",
    "ja": "Japanese",
    "ko": "Korean",
    "zh": "Chinese",
    "ar": "Arabic",
}

Trigger = Literal["meeting_end", "on_demand"]
Status = Literal["pending", "composing", "ready", "empty", "unavailable"]
Reason = Literal["budget", "model_error", "timeout"]
Length = Literal["short", "standard"]
AudioStatus = Literal["none", "pending", "ready", "failed"]
SegmentKind = Literal["opening", "changed", "waiting", "promise", "question", "closing"]
LinkType = Literal["meeting", "decision", "action", "promise", "question"]
Personality = Literal["balanced", "concise", "detailed", "direct", "friendly"]

_KINDS = frozenset({"opening", "changed", "waiting", "promise", "question", "closing"})
_LINK_TYPES = frozenset({"meeting", "decision", "action", "promise", "question"})
_PERSONALITIES = frozenset({"balanced", "concise", "detailed", "direct", "friendly"})
_PROMPT_RE = re.compile(r"^[a-z_]+@v\d+$")
_WORD = re.compile(r"\S+")


# --- shapes on the wire -----------------------------------------------------------------------


class BriefingLink(BaseModel):
    """Where "open the moment" goes: the moment in the meeting (`at_ms` on its clock, for the
    transcript) or the record itself."""

    type: LinkType
    meeting_id: uuid.UUID
    id: uuid.UUID | None = Field(default=None, description="The decision, action or promise.")
    at_ms: int | None = Field(default=None, description="The moment, on the meeting's clock.")


class BriefingSegment(BaseModel):
    id: str = Field(description="Stable within the briefing: s1, s2, ...")
    kind: SegmentKind
    text: str = Field(description="What the agent says, in the briefing's language.")
    link: BriefingLink | None = None


class BriefingAgent(BaseModel):
    """The persona the briefing was written in, as it was then."""

    name: str
    seed: str | None = Field(default=None, description="The agent's doodle seed, or null.")
    personality: Personality = "balanced"


class BriefingAudio(BaseModel):
    status: AudioStatus = Field(
        description="`none`: no server voice (speak the script on the device); `ready`: fetch "
        "`GET /api/v1/briefings/{id}/audio`; `pending`; `failed` (speak on the device)."
    )
    voice: str | None = None


class Briefing(BaseModel):
    id: uuid.UUID
    trigger: Trigger
    status: Status
    reason: Reason | None = None
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    length: Length
    language: str
    agent: BriefingAgent
    segments: list[BriefingSegment]
    word_count: int
    duration_s: int
    audio: BriefingAudio
    prompt_version: str | None = None
    created_at: datetime
    ready_at: datetime | None = None
    listened_at: datetime | None = None


class BriefingPreferences(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="A briefing after every meeting.")
    auto_play: bool = Field(default=False, description="Start playing when it opens.")
    length: Length = Field(default="standard", description="short ~45 s, standard ~90 s.")
    language: str = Field(default="en", min_length=2, max_length=8)
    voice: str | None = Field(
        default=None, max_length=200, description="The device voice picked, as named on it."
    )
    server_voice: str | None = Field(
        default=None, max_length=100, description="The server voice id (when connected)."
    )
    rate: float = Field(default=1.0, ge=0.5, le=2.0, description="Playback speed.")


class ServerVoiceOption(BaseModel):
    id: str
    name: str
    description: str


class BriefingLanguage(BaseModel):
    code: str
    name: str


class BriefingSettings(BriefingPreferences):
    model_config = ConfigDict(extra="ignore")

    server_voice_connected: bool = Field(
        description="Whether this server renders audio (TTS_PROVIDER). False: device speech."
    )
    server_voices: list[ServerVoiceOption]
    languages: list[BriefingLanguage]


# --- preferences ------------------------------------------------------------------------------


async def preferences(session: AsyncSession, user_id: uuid.UUID) -> BriefingPreferences:
    row = (
        (
            await session.execute(
                text(
                    "SELECT enabled, auto_play, length, language, voice, server_voice, rate "
                    "FROM briefing_preferences WHERE user_id = :u"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return BriefingPreferences()
    data = dict(row)
    data["rate"] = round(float(data["rate"]), 2)
    if data["language"] not in LANGUAGES:
        data["language"] = "en"
    return BriefingPreferences(**data)


def settings_view(prefs: BriefingPreferences) -> BriefingSettings:
    return BriefingSettings(
        **prefs.model_dump(),
        server_voice_connected=briefing_audio.connected(),
        server_voices=[
            ServerVoiceOption(id=v.id, name=v.name, description=v.description)
            for v in briefing_audio.SERVER_VOICES
        ],
        languages=[BriefingLanguage(code=c, name=n) for c, n in LANGUAGES.items()],
    )


async def save_preferences(
    session: AsyncSession, org_id: uuid.UUID, user_id: uuid.UUID, prefs: BriefingPreferences
) -> None:
    await session.execute(
        text(
            "INSERT INTO briefing_preferences (user_id, org_id, enabled, auto_play, length, "
            "language, voice, server_voice, rate) VALUES (:u, :o, :e, :a, :l, :lang, :v, :sv, "
            ":r) ON CONFLICT (user_id) DO UPDATE SET enabled = EXCLUDED.enabled, "
            "auto_play = EXCLUDED.auto_play, length = EXCLUDED.length, "
            "language = EXCLUDED.language, voice = EXCLUDED.voice, "
            "server_voice = EXCLUDED.server_voice, rate = EXCLUDED.rate, updated_at = now()"
        ),
        {
            "u": str(user_id),
            "o": str(org_id),
            "e": prefs.enabled,
            "a": prefs.auto_play,
            "l": prefs.length,
            "lang": prefs.language,
            "v": prefs.voice,
            "sv": prefs.server_voice,
            "r": prefs.rate,
        },
    )


# --- the persona ------------------------------------------------------------------------------


async def persona(session: AsyncSession, user_id: uuid.UUID) -> tuple[BriefingAgent, str, str]:
    """(the agent, the person's display name, their first name). ADR-0038 D2: a null agent name
    reads "{first name}'s agent"."""
    row = (
        (
            await session.execute(
                text(
                    "SELECT email, display_name, agent_name, agent_avatar_seed, agent_personality, "
                    "agent_instructions FROM users WHERE id = :u"
                ),
                {"u": str(user_id)},
            )
        )
        .mappings()
        .first()
    )
    data: dict[str, Any] = dict(row) if row is not None else {}
    name = person_name(data.get("display_name"), data.get("email")) or "you"
    first = name.split()[0] if name.split() else "you"
    agent_name = " ".join(str(data.get("agent_name") or "").split())[:40]
    personality = str(data.get("agent_personality") or "balanced")
    seed = data.get("agent_avatar_seed")
    return (
        BriefingAgent.model_validate(
            {
                "name": agent_name or f"{first}'s Myte",
                "seed": str(seed) if seed else None,
                "personality": personality if personality in _PERSONALITIES else "balanced",
            }
        ),
        name,
        first,
    )


async def instructions_of(session: AsyncSession, user_id: uuid.UUID) -> str | None:
    raw = (
        await session.execute(
            text("SELECT agent_instructions FROM users WHERE id = :u"), {"u": str(user_id)}
        )
    ).scalar()
    value = " ".join(str(raw or "").split())
    return value[:500] or None


# --- rows -------------------------------------------------------------------------------------

_COLS = (
    "b.id, b.trigger, b.status, b.reason, b.meeting_id, m.title AS meeting_title, b.length, "
    "b.language, b.content, b.agent, b.prompt_version, b.attempts, b.rung_at, b.claimed_at, "
    "b.audio_status, b.audio_key, b.audio_voice, b.created_at, b.ready_at, b.listened_at, "
    "b.updated_at, b.user_id, b.notified_at"
)
_FROM = "FROM agent_briefings b LEFT JOIN meetings m ON m.id = b.meeting_id"


def _json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else {}
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def audio_status_of(row: Any, now: datetime | None = None) -> AudioStatus:
    status = str(row["audio_status"])
    if status == "pending":
        now = now or datetime.now(UTC)
        if row["updated_at"] and now - row["updated_at"] > timedelta(seconds=STALE_AUDIO_S):
            return "failed"
    return status  # type: ignore[return-value]


def from_row(row: Any) -> Briefing:
    content = _json(row["content"])
    agent = _json(row["agent"])
    segments: list[BriefingSegment] = []
    for s in content.get("segments") or []:
        try:
            segments.append(BriefingSegment.model_validate(s))
        except ValueError:
            continue
    return Briefing(
        id=row["id"],
        trigger=row["trigger"],
        status=row["status"],
        reason=row["reason"],
        meeting_id=row["meeting_id"],
        meeting_title=row["meeting_title"],
        length=row["length"],
        language=row["language"],
        agent=BriefingAgent.model_validate(
            {
                "name": agent.get("name") or "Your Myte",
                "seed": agent.get("seed"),
                "personality": (
                    agent.get("personality")
                    if agent.get("personality") in _PERSONALITIES
                    else "balanced"
                ),
            }
        ),
        segments=segments,
        word_count=int(content.get("word_count") or 0),
        duration_s=int(content.get("duration_s") or 0),
        audio=BriefingAudio(status=audio_status_of(row), voice=row["audio_voice"]),
        prompt_version=row["prompt_version"],
        created_at=row["created_at"],
        ready_at=row["ready_at"],
        listened_at=row["listened_at"],
    )


async def fetch(
    session: AsyncSession, briefing_id: uuid.UUID, user_id: uuid.UUID | None = None
) -> Any:
    """The row (owner-scoped when `user_id` is given), or None."""
    where = "b.id = :id" + (" AND b.user_id = :u" if user_id is not None else "")
    params = {"id": str(briefing_id)}
    if user_id is not None:
        params["u"] = str(user_id)
    return (
        (
            await session.execute(
                text(f"SELECT {_COLS} {_FROM} WHERE {where}"),  # noqa: S608 - constant parts
                params,
            )
        )
        .mappings()
        .first()
    )


async def latest(
    session: AsyncSession, user_id: uuid.UUID, meeting_id: uuid.UUID | None = None
) -> Any:
    where = "b.user_id = :u" + (" AND b.meeting_id = :m" if meeting_id else "")
    params = {"u": str(user_id)}
    if meeting_id:
        params["m"] = str(meeting_id)
    return (
        (
            await session.execute(
                text(  # noqa: S608 - constant parts
                    f"SELECT {_COLS} {_FROM} WHERE {where} AND (m.id IS NULL OR "
                    "m.deleted_at IS NULL) ORDER BY b.created_at DESC LIMIT 1"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )


async def recent(session: AsyncSession, user_id: uuid.UUID, limit: int) -> list[Any]:
    return list(
        (
            await session.execute(
                text(  # noqa: S608 - constant parts
                    f"SELECT {_COLS} {_FROM} WHERE b.user_id = :u AND (m.id IS NULL OR "
                    "m.deleted_at IS NULL) ORDER BY b.created_at DESC LIMIT :lim"
                ),
                {"u": str(user_id), "lim": limit},
            )
        )
        .mappings()
        .all()
    )


async def create(
    session: AsyncSession,
    org_id: uuid.UUID,
    user_id: uuid.UUID,
    meeting_id: uuid.UUID | None,
    trigger: Trigger,
) -> uuid.UUID | None:
    """A new `pending` briefing in the person's current length and language, with the persona
    as it is now. None when a `meeting_end` briefing for this meeting already exists."""
    prefs = await preferences(session, user_id)
    agent, _, _ = await persona(session, user_id)
    conflict = (
        "ON CONFLICT (meeting_id, user_id) WHERE trigger = 'meeting_end' DO NOTHING "
        if trigger == "meeting_end"
        else ""
    )
    return (
        await session.execute(
            text(
                "INSERT INTO agent_briefings (org_id, user_id, meeting_id, trigger, length, "
                "language, agent) VALUES (:o, :u, :m, :t, :l, :lang, CAST(:a AS jsonb)) "
                f"{conflict}RETURNING id"
            ),
            {
                "o": str(org_id),
                "u": str(user_id),
                "m": str(meeting_id) if meeting_id else None,
                "t": trigger,
                "l": prefs.length,
                "lang": prefs.language,
                "a": agent.model_dump_json(),
            },
        )
    ).scalar()


async def create_after_meeting(
    session: AsyncSession, org_id: uuid.UUID, meeting_id: uuid.UUID, user_id: uuid.UUID
) -> uuid.UUID | None:
    """The consumer's hook once a person's own minutes are filed: a briefing when theirs are on."""
    prefs = await preferences(session, user_id)
    if not prefs.enabled:
        return None
    return await create(session, org_id, user_id, meeting_id, "meeting_end")


async def on_demand_count(session: AsyncSession, user_id: uuid.UUID) -> int:
    return int(
        (
            await session.execute(
                text(
                    "SELECT count(*) FROM agent_briefings WHERE user_id = :u "
                    "AND trigger = 'on_demand' AND created_at > now() - interval '1 hour'"
                ),
                {"u": str(user_id)},
            )
        ).scalar_one()
    )


async def open_on_demand(
    session: AsyncSession, user_id: uuid.UUID, meeting_id: uuid.UUID | None
) -> uuid.UUID | None:
    """A "Brief me" for the same thing still being written (asked twice: one briefing)."""
    return (
        await session.execute(
            text(
                "SELECT id FROM agent_briefings WHERE user_id = :u AND trigger = 'on_demand' "
                "AND status IN ('pending', 'composing') AND meeting_id IS NOT DISTINCT FROM "
                "CAST(:m AS uuid) AND created_at > now() - interval '5 minutes' "
                "ORDER BY created_at DESC LIMIT 1"
            ),
            {"u": str(user_id), "m": str(meeting_id) if meeting_id else None},
        )
    ).scalar()


# --- the doorbell -----------------------------------------------------------------------------


def ring_channel(org_id: uuid.UUID) -> str:
    return redis_key(org_id, "briefings", "ring")


async def ring(org_id: uuid.UUID, briefing_id: uuid.UUID) -> bool:
    """Tell agent-runtime-service there is a briefing to write. Best effort: a missed ring is
    recovered by the next read (`recover`)."""
    try:
        await get_redis().publish(ring_channel(org_id), str(briefing_id))
        return True
    except Exception:  # noqa: BLE001 - recovered on read
        logger.warning("briefing doorbell unreachable for %s", briefing_id)
        return False


async def recover(session: AsyncSession, org_id: uuid.UUID, row: Any) -> Any:
    """Re-ring a briefing the agent has not picked up, give up after `MAX_ATTEMPTS`, and finish
    a stale audio render. Returns the (possibly updated) row; rings after the caller commits
    are not needed because PUBLISH is outside the transaction and idempotent for the runtime
    (its claim decides)."""
    if row is None:
        return row
    now = datetime.now(UTC)
    status = row["status"]
    changed = False
    if status == "composing" and row["claimed_at"] is not None:
        if now - row["claimed_at"] > timedelta(seconds=STALE_COMPOSING_S):
            if int(row["attempts"]) >= MAX_ATTEMPTS:
                await _give_up(session, row["id"])
            else:
                await session.execute(
                    text(
                        "UPDATE agent_briefings SET status = 'pending', rung_at = now(), "
                        "updated_at = now() WHERE id = :id AND status = 'composing'"
                    ),
                    {"id": str(row["id"])},
                )
                await ring(org_id, row["id"])
            changed = True
    elif status == "pending":
        due = row["rung_at"] is None or now - row["rung_at"] > timedelta(seconds=RERING_AFTER_S)
        if due:
            if int(row["attempts"]) >= MAX_ATTEMPTS:
                await _give_up(session, row["id"])
            else:
                await session.execute(
                    text("UPDATE agent_briefings SET rung_at = now() WHERE id = :id"),
                    {"id": str(row["id"])},
                )
                await ring(org_id, row["id"])
            changed = True
    elif (
        row["audio_status"] == "pending"
        and audio_status_of(row, now) == "failed"
        and row["notified_at"] is None
    ):
        await session.execute(
            text(
                "UPDATE agent_briefings SET audio_status = 'failed', updated_at = now() "
                "WHERE id = :id AND audio_status = 'pending'"
            ),
            {"id": str(row["id"])},
        )
        await notify(session, org_id, row["id"])
        changed = True
    return await fetch(session, row["id"]) if changed else row


async def _give_up(session: AsyncSession, briefing_id: uuid.UUID) -> None:
    await session.execute(
        text(
            "UPDATE agent_briefings SET status = 'unavailable', reason = 'timeout', "
            "updated_at = now() WHERE id = :id AND status IN ('pending', 'composing')"
        ),
        {"id": str(briefing_id)},
    )


# --- the source the agent writes from --------------------------------------------------------


class SourceItem(BaseModel):
    id: uuid.UUID | None = None
    meeting_id: uuid.UUID | None = None
    meeting_title: str | None = None
    text: str
    status: str | None = None
    who: str | None = Field(default=None, description="The person waiting, or who asked.")
    due: str | None = None
    at_ms: int | None = None
    carried_to: str | None = Field(
        default=None, description="The follow-up meeting the question was carried to."
    )
    carried_to_meeting_id: uuid.UUID | None = None


class SourceMeeting(BaseModel):
    id: uuid.UUID
    title: str
    ended_at: datetime | None = None


class SourceMinutes(BaseModel):
    summary: str
    decisions: list[str]
    open_items: list[str]


class SourcePerson(BaseModel):
    name: str
    first_name: str


class SourceAgent(BriefingAgent):
    instructions: str | None = None


class BriefingSource(BaseModel):
    """`POST /internal/briefings/{id}/claim`: everything the agent writes from. Only the
    person's own records and their own minutes; no transcript."""

    briefing_id: uuid.UUID
    user_id: uuid.UUID
    trigger: Trigger
    length: Length
    target_words: int
    language: str
    language_name: str
    person: SourcePerson
    agent: SourceAgent
    since: datetime
    meeting: SourceMeeting | None = None
    minutes: SourceMinutes | None = None
    changed: list[SourceItem]
    waiting: list[SourceItem]
    promises: list[SourceItem]
    questions: list[SourceItem]
    topics: list[str]


def _short(raw: Any, limit: int = 300) -> str:
    value = " ".join(str(raw or "").split())
    return value if len(value) <= limit else value[: limit - 1].rstrip() + "…"


def _due(raw: Any) -> str | None:
    if isinstance(raw, date):
        return raw.isoformat()
    return _short(raw, 60) or None


async def _since(session: AsyncSession, user_id: uuid.UUID, briefing_id: uuid.UUID) -> datetime:
    previous = (
        await session.execute(
            text(
                "SELECT max(created_at) FROM agent_briefings WHERE user_id = :u AND id <> :id "
                "AND status = 'ready'"
            ),
            {"u": str(user_id), "id": str(briefing_id)},
        )
    ).scalar()
    floor = datetime.now(UTC) - timedelta(days=SINCE_DAYS)
    return max(previous, floor) if previous else floor


async def _source_meeting(
    session: AsyncSession, user_id: uuid.UUID, meeting_id: uuid.UUID | None
) -> SourceMeeting | None:
    """The meeting the briefing is about: the given one, else the person's latest ended meeting
    of the last week."""
    where = (
        "m.id = :m"
        if meeting_id
        else ("m.status = 'ended' AND m.updated_at > now() - interval '7 days'")
    )
    params: dict[str, Any] = {"u": str(user_id)}
    if meeting_id:
        params["m"] = str(meeting_id)
    row = (
        (
            await session.execute(
                text(
                    "SELECT m.id, m.title, COALESCE(m.closed_at, m.scheduled_end, m.updated_at) "
                    "AS ended_at FROM meetings m JOIN meeting_participants p "
                    "ON p.meeting_id = m.id AND p.user_id = :u "
                    f"WHERE {where} AND m.deleted_at IS NULL "
                    "ORDER BY m.updated_at DESC LIMIT 1"
                ),
                params,
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return None
    return SourceMeeting(
        id=row["id"],
        title=_short(row["title"], 200) or "Untitled meeting",
        ended_at=row["ended_at"],
    )


async def build_source(session: AsyncSession, row: Any) -> BriefingSource:
    user_id: uuid.UUID = row["user_id"]
    agent, name, first = await persona(session, user_id)
    snapshot = _json(row["agent"])
    if snapshot.get("name"):
        agent = BriefingAgent.model_validate(
            {**agent.model_dump(), **{k: v for k, v in snapshot.items() if k in ("name", "seed")}}
        )
    since = await _since(session, user_id, row["id"])
    meeting = await _source_meeting(session, user_id, row["meeting_id"])
    u = {"u": str(user_id), "since": since}

    mine = await minutes.personal_minutes(session, meeting.id, user_id) if meeting else None
    source_minutes = (
        SourceMinutes(
            summary=_short(mine.summary, 600),
            decisions=[_short(d.text) for d in mine.decisions[:MAX_SOURCE_ITEMS]],
            open_items=[_short(o.text) for o in mine.open_items[:MAX_SOURCE_ITEMS]],
        )
        if mine is not None and mine.status == "ready"
        else None
    )

    # What changed for you: decisions of meetings you were in, settled or changed since the last
    # briefing (this meeting's first), reversals of decisions behind your tasks, and new tasks
    # that are now yours.
    changed: list[SourceItem] = []
    for r in (
        await session.execute(
            text(
                "SELECT d.id, d.meeting_id, m.title, d.content, d.status, d.quote_start_ms "
                "FROM decisions d JOIN meetings m ON m.id = d.meeting_id "
                "JOIN meeting_participants p ON p.meeting_id = d.meeting_id AND p.user_id = :u "
                "WHERE d.deleted_at IS NULL AND m.deleted_at IS NULL "
                "AND d.status IN ('confirmed', 'superseded') "
                "AND (COALESCE(d.status_changed_at, d.created_at) > :since OR d.meeting_id = :m) "
                "ORDER BY (d.meeting_id = :m) DESC, COALESCE(d.status_changed_at, d.created_at) "
                "DESC LIMIT :lim"
            ),
            {**u, "m": str(meeting.id) if meeting else None, "lim": MAX_SOURCE_ITEMS},
        )
    ).all():
        changed.append(
            SourceItem(
                id=r[0],
                meeting_id=r[1],
                meeting_title=_short(r[2], 200),
                text=_short(r[3]),
                status="replaced by a later decision" if r[4] == "superseded" else "decided",
                at_ms=r[5],
            )
        )
    for r in (
        await session.execute(
            text(
                "SELECT d.id, d.meeting_id, m.title, d.content FROM decision_reversals x "
                "JOIN decisions d ON d.id = x.new_decision_id "
                "JOIN meetings m ON m.id = d.meeting_id "
                "WHERE x.owner_user_id = :u AND x.status = 'open' AND x.created_at > :since "
                "ORDER BY x.created_at DESC LIMIT 3"
            ),
            u,
        )
    ).all():
        changed.append(
            SourceItem(
                id=r[0],
                meeting_id=r[1],
                meeting_title=_short(r[2], 200),
                text=_short(r[3]),
                status="reversed an earlier decision your task depends on",
            )
        )
    for r in (
        await session.execute(
            text(
                "SELECT a.id, a.meeting_id, m.title, a.content, a.due_date, a.quote_start_ms "
                "FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id "
                "WHERE a.assignee_id = :u AND a.status = 'confirmed' AND a.deleted_at IS NULL "
                "AND COALESCE(a.status_changed_at, a.created_at) > :since "
                "AND (m.id IS NULL OR m.deleted_at IS NULL) "
                "ORDER BY a.created_at DESC LIMIT 4"
            ),
            u,
        )
    ).all():
        changed.append(
            SourceItem(
                id=r[0],
                meeting_id=r[1],
                meeting_title=_short(r[2], 200) if r[2] else None,
                text=_short(r[3]),
                status="now yours",
                due=_due(r[4]),
                at_ms=r[5],
            )
        )

    # Who is waiting on you: status asks you have not answered, then your tasks due within three
    # days (or late), with the person who is waiting (whoever asked, else the meeting's host).
    waiting: list[SourceItem] = []
    for r in (
        await session.execute(
            text(
                "SELECT a.id, a.meeting_id, m.title, a.content, a.due_date, u.email, "
                "a.quote_start_ms, u.display_name FROM action_status_requests s "
                "JOIN action_items a ON a.id = s.action_id "
                "LEFT JOIN meetings m ON m.id = a.meeting_id "
                "LEFT JOIN users u ON u.id = s.requested_by "
                "WHERE s.user_id = :u AND s.state = 'pending' AND a.deleted_at IS NULL "
                "ORDER BY s.requested_at DESC LIMIT 4"
            ),
            u,
        )
    ).all():
        waiting.append(
            SourceItem(
                id=r[0],
                meeting_id=r[1],
                meeting_title=_short(r[2], 200) if r[2] else None,
                text=_short(r[3]),
                status="asked you for an update",
                who=person_name(r[7], r[5]),
                due=_due(r[4]),
                at_ms=r[6],
            )
        )
    seen = {w.id for w in waiting}
    for r in (
        await session.execute(
            text(
                "SELECT a.id, a.meeting_id, m.title, a.content, a.due_date, "
                "h.email, a.quote_start_ms, h.display_name "
                "FROM action_items a LEFT JOIN meetings m ON m.id = a.meeting_id "
                "LEFT JOIN LATERAL (SELECT u.email, u.display_name FROM meeting_participants p "
                " JOIN users u ON u.id = p.user_id "
                " WHERE p.meeting_id = a.meeting_id AND p.role_in_meeting = 'host' "
                " AND p.user_id <> :u LIMIT 1) h ON true "
                "WHERE a.assignee_id = :u AND a.status = 'confirmed' AND a.deleted_at IS NULL "
                "AND a.due_date IS NOT NULL AND a.due_date <= (now() + interval '3 days')::date "
                "AND (m.id IS NULL OR m.deleted_at IS NULL) ORDER BY a.due_date LIMIT 6"
            ),
            u,
        )
    ).all():
        if r[0] in seen:
            continue
        overdue = r[4] < datetime.now(UTC).date()
        waiting.append(
            SourceItem(
                id=r[0],
                meeting_id=r[1],
                meeting_title=_short(r[2], 200) if r[2] else None,
                text=_short(r[3]),
                status="overdue" if overdue else "due soon",
                who=person_name(r[7], r[5]),
                due=_due(r[4]),
                at_ms=r[6],
            )
        )
    waiting = waiting[:MAX_SOURCE_ITEMS]
    # A task someone is waiting on is said once, under "waiting", not again as "now yours".
    waiting_ids = {w.id for w in waiting}
    changed = [c for c in changed if c.id is None or c.id not in waiting_ids]

    # Your promises: open commitments, this meeting's first; the moment is the linked action's.
    promises = [
        SourceItem(
            id=r[0],
            meeting_id=r[1],
            meeting_title=_short(r[2], 200),
            text=_short(r[3]),
            due=_short(r[4], 60) or None,
            at_ms=r[5],
        )
        for r in (
            await session.execute(
                text(
                    "SELECT c.id, c.meeting_id, m.title, c.content, c.due_hint, a.quote_start_ms "
                    "FROM commitments c JOIN meetings m ON m.id = c.meeting_id "
                    "LEFT JOIN action_items a ON a.id = c.action_item_id "
                    "WHERE c.user_id = :u AND c.status = 'open' AND m.deleted_at IS NULL "
                    "ORDER BY (c.meeting_id = :m) DESC, c.created_at DESC LIMIT :lim"
                ),
                {**u, "m": str(meeting.id) if meeting else None, "lim": MAX_SOURCE_ITEMS},
            )
        ).all()
    ]

    # Questions left open: this meeting's unanswered questions (your minutes), and the ones your
    # agent saved for later; each says where it was carried, when a follow-up is on the calendar.
    questions: list[SourceItem] = []
    if meeting is not None:
        follow = (
            (
                await session.execute(
                    text(
                        "SELECT id, title, scheduled_start FROM meetings "
                        "WHERE previous_meeting_id = :m AND deleted_at IS NULL "
                        "AND status <> 'ended' ORDER BY scheduled_start NULLS LAST LIMIT 1"
                    ),
                    {"m": str(meeting.id)},
                )
            )
            .mappings()
            .first()
        )
        carried = None
        if follow is not None:
            when = follow["scheduled_start"]
            carried = _short(follow["title"], 120) or "the follow-up"
            if when is not None:
                carried += f" ({when:%a %d %b})"
        for q in mine.unanswered_questions[:MAX_SOURCE_ITEMS] if mine else []:
            questions.append(
                SourceItem(
                    meeting_id=meeting.id,
                    meeting_title=meeting.title,
                    text=_short(q.text),
                    who=q.asked_by_name,
                    carried_to=carried,
                    carried_to_meeting_id=follow["id"] if follow else None,
                )
            )
        for r in (
            await session.execute(
                text(
                    "SELECT question, quote_start_ms FROM agent_suggestions WHERE meeting_id = :m "
                    "AND user_id = :u AND status = 'saved' AND deleted_at IS NULL "
                    "ORDER BY created_at LIMIT 4"
                ),
                {"m": str(meeting.id), "u": str(user_id)},
            )
        ).all():
            questions.append(
                SourceItem(
                    meeting_id=meeting.id,
                    meeting_title=meeting.title,
                    text=_short(r[0]),
                    status="your Myte saved it for next time",
                    at_ms=r[1],
                    carried_to=carried,
                    carried_to_meeting_id=follow["id"] if follow else None,
                )
            )
    questions = questions[:MAX_SOURCE_ITEMS]

    memory = (
        await session.execute(
            text("SELECT topics, learning_enabled FROM person_memory WHERE user_id = :u"),
            {"u": str(user_id)},
        )
    ).first()
    topics: list[str] = []
    if memory is not None and memory[1]:
        raw = memory[0] if isinstance(memory[0], list) else json.loads(memory[0] or "[]")
        ranked = sorted(
            (t for t in raw if isinstance(t, dict) and isinstance(t.get("label"), str)),
            key=lambda t: (not t.get("pinned"), -float(t.get("weight") or 0.0)),
        )
        topics = [_short(t["label"], 80) for t in ranked[:6]]

    language = row["language"] if row["language"] in LANGUAGES else "en"
    return BriefingSource(
        briefing_id=row["id"],
        user_id=user_id,
        trigger=row["trigger"],
        length=row["length"],
        target_words=TARGET_WORDS.get(row["length"], 220),
        language=language,
        language_name=LANGUAGES[language],
        person=SourcePerson(name=name, first_name=first),
        agent=SourceAgent(
            **agent.model_dump(), instructions=await instructions_of(session, user_id)
        ),
        since=since,
        meeting=meeting,
        minutes=source_minutes,
        changed=changed[:MAX_SOURCE_ITEMS],
        waiting=waiting,
        promises=promises,
        questions=questions,
        topics=topics,
    )


async def claim(session: AsyncSession, briefing_id: uuid.UUID) -> Any:
    """`pending` (or a stale `composing`) -> `composing`, attempts + 1. None when there is
    nothing to write (done, gone, or out of attempts)."""
    return (
        (
            await session.execute(
                text(
                    "UPDATE agent_briefings SET status = 'composing', claimed_at = now(), "
                    "attempts = attempts + 1, updated_at = now() WHERE id = :id "
                    "AND attempts < :max AND (status = 'pending' OR (status = 'composing' "
                    "AND claimed_at < now() - make_interval(secs => :stale))) RETURNING id"
                ),
                {"id": str(briefing_id), "max": MAX_ATTEMPTS, "stale": STALE_COMPOSING_S},
            )
        )
        .mappings()
        .first()
    )


# --- what the agent wrote ---------------------------------------------------------------------


class ResultLink(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str
    meeting_id: uuid.UUID | None = None
    id: uuid.UUID | None = None
    at_ms: int | None = None


class ResultSegment(BaseModel):
    model_config = ConfigDict(extra="ignore")

    kind: str
    text: str = Field(max_length=2000)
    link: ResultLink | None = None


class BriefingResult(BaseModel):
    """`POST /internal/briefings/{id}/result` from agent-runtime-service."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["ready", "empty", "unavailable"]
    reason: Reason | None = None
    prompt_version: str = Field(max_length=64)
    language: str | None = Field(default=None, max_length=8)
    segments: list[ResultSegment] = Field(default_factory=list, max_length=40)


class ResultAccepted(BaseModel):
    accepted: bool
    status: Status


def words(value: str) -> int:
    return len(_WORD.findall(value))


async def _allowed_links(
    session: AsyncSession, user_id: uuid.UUID, links: list[ResultLink]
) -> set[tuple[str, uuid.UUID | None, uuid.UUID]]:
    """(type, id, meeting) triples the person may be pointed at: meetings they were in, and in
    them decisions, actions and their own promises."""
    meeting_ids = {str(link.meeting_id) for link in links if link.meeting_id}
    if not meeting_ids:
        return set()
    mine = {
        r[0]
        for r in (
            await session.execute(
                text(
                    "SELECT p.meeting_id FROM meeting_participants p JOIN meetings m "
                    "ON m.id = p.meeting_id WHERE p.user_id = :u AND m.deleted_at IS NULL "
                    "AND p.meeting_id = ANY(CAST(:ms AS uuid[]))"
                ),
                {"u": str(user_id), "ms": list(meeting_ids)},
            )
        ).all()
    }
    allowed: set[tuple[str, uuid.UUID | None, uuid.UUID]] = set()
    for m in mine:
        allowed.add(("meeting", None, m))
        allowed.add(("question", None, m))
    ids = [str(link.id) for link in links if link.id]
    if ids and mine:
        ms = [str(m) for m in mine]
        for kind, sql in (
            ("decision", "SELECT id, meeting_id FROM decisions WHERE deleted_at IS NULL"),
            ("action", "SELECT id, meeting_id FROM action_items WHERE deleted_at IS NULL"),
            ("promise", "SELECT id, meeting_id FROM commitments WHERE user_id = :u"),
        ):
            for r in (
                await session.execute(
                    text(
                        f"{sql} AND id = ANY(CAST(:ids AS uuid[])) "
                        "AND meeting_id = ANY(CAST(:ms AS uuid[]))"
                    ),
                    {"ids": ids, "ms": ms, "u": str(user_id)},
                )
            ).all():
                allowed.add((kind, r[0], r[1]))
    return allowed


async def clean_segments(
    session: AsyncSession, user_id: uuid.UUID, raw: list[ResultSegment], length: str
) -> list[dict[str, Any]]:
    """The segments reduced to the stored shape: known kinds, texts clipped, empty ones
    dropped, every link checked against what the person may open (an unknown one is dropped,
    the sentence kept), and the whole bounded to 1.6x the target length."""
    allowed = await _allowed_links(session, user_id, [s.link for s in raw if s.link])
    budget = int(TARGET_WORDS.get(length, 220) * 1.6)
    out: list[dict[str, Any]] = []
    total = 0
    for seg in raw:
        body = _short(seg.text, MAX_SEGMENT_CHARS)
        if not body:
            continue
        n = words(body)
        if out and total + n > budget:
            break
        link = None
        if seg.link and seg.link.type in _LINK_TYPES and seg.link.meeting_id:
            ref = seg.link.id if seg.link.type in ("decision", "action", "promise") else None
            if (seg.link.type, ref, seg.link.meeting_id) in allowed:
                at = seg.link.at_ms
                link = {
                    "type": seg.link.type,
                    "meeting_id": str(seg.link.meeting_id),
                    "id": str(ref) if ref else None,
                    "at_ms": at if isinstance(at, int) and 0 <= at <= MAX_DAY_MS else None,
                }
        out.append(
            {
                "id": f"s{len(out) + 1}",
                "kind": seg.kind if seg.kind in _KINDS else "changed",
                "text": body,
                "link": link,
            }
        )
        total += n
        if len(out) == MAX_SEGMENTS:
            break
    return out


async def apply_result(
    session: AsyncSession, org_id: uuid.UUID, row: Any, body: BriefingResult
) -> tuple[bool, Status, bool]:
    """Store what the agent wrote. Returns (accepted, status, render_audio). A briefing that is
    already finished keeps its first answer."""
    if row["status"] not in ("pending", "composing"):
        return False, row["status"], False
    segments = (
        await clean_segments(session, row["user_id"], body.segments, row["length"])
        if body.status == "ready"
        else []
    )
    status: Status = body.status
    if status == "ready" and not segments:
        status = "empty"
    reason = body.reason if status == "unavailable" else None
    if status == "unavailable" and reason is None:
        reason = "model_error"
    total = sum(words(s["text"]) for s in segments)
    content = {
        "segments": segments,
        "word_count": total,
        "duration_s": round(total * 60 / WORDS_PER_MINUTE),
    }
    language = body.language if body.language in LANGUAGES else row["language"]
    prompt = body.prompt_version if _PROMPT_RE.match(body.prompt_version) else "briefing@unknown"
    audio = status == "ready" and briefing_audio.connected()
    await session.execute(
        text(
            "UPDATE agent_briefings SET status = :s, reason = :r, content = CAST(:c AS jsonb), "
            "prompt_version = :p, language = :lang, ready_at = now(), updated_at = now(), "
            "audio_status = :a WHERE id = :id"
        ),
        {
            "s": status,
            "r": reason,
            "c": json.dumps(content),
            "p": prompt,
            "lang": language,
            "a": "pending" if audio else "none",
            "id": str(row["id"]),
        },
    )
    if status == "ready" and not audio:
        await notify(session, org_id, row["id"])
    return True, status, audio


def script_of(briefing: Briefing) -> str:
    return " ".join(s.text for s in briefing.segments)


async def notify(session: AsyncSession, org_id: uuid.UUID, briefing_id: uuid.UUID) -> bool:
    """The in-app notification (mirrored to the person's phone by notification-service) for an
    after-meeting briefing with something in it, once. On-demand briefings were asked for on
    screen: no notification."""
    row = (
        (
            await session.execute(
                text(
                    "UPDATE agent_briefings SET notified_at = now() WHERE id = :id "
                    "AND notified_at IS NULL AND status = 'ready' AND trigger = 'meeting_end' "
                    "RETURNING user_id, agent, content, "
                    "(SELECT title FROM meetings WHERE id = meeting_id) AS title"
                ),
                {"id": str(briefing_id)},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return False
    agent = _json(row["agent"]).get("name") or "Your Myte"
    content = _json(row["content"])
    minutes_long = max(1, round(int(content.get("duration_s") or 60) / 60))
    span = "about a minute" if minutes_long <= 1 else f"about {minutes_long} minutes"
    await enqueue(
        session,
        EventEnvelope(
            topic="notifications",
            org_id=str(org_id),
            producer=PRODUCER,
            idempotency_key=f"notif:briefing:{briefing_id}:in_app:{row['user_id']}",
            payload={
                "source_event_id": f"briefing:{briefing_id}",
                "channel": "in_app",
                "user_id": str(row["user_id"]),
                "priority": 3,
                "title": f"{agent} has your briefing",
                "body": f"{_short(row['title'], 80) or 'Your meeting'}: {span} to listen to.",
                "link": f"/briefings/{briefing_id}",
            },
        ),
    )
    return True


# --- the server voice -------------------------------------------------------------------------

_tasks: set[asyncio.Task[AudioStatus]] = set()


def schedule_audio(org_id: uuid.UUID, briefing_id: uuid.UUID) -> None:
    """Render the audio after the response (the commit already happened)."""
    task = asyncio.create_task(render_audio(org_id, briefing_id))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)


async def render_audio(org_id: uuid.UUID, briefing_id: uuid.UUID) -> AudioStatus:
    backend = briefing_audio.backend()
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        row = await fetch(session, briefing_id)
        if row is None or row["status"] != "ready" or row["audio_status"] != "pending":
            return "none"
        briefing = from_row(row)
        prefs = await preferences(session, row["user_id"])
    if backend is None:
        outcome: AudioStatus = "failed"
        key = voice = None
        size = 0
    else:
        voice = briefing_audio.voice_for(prefs.server_voice, briefing.agent.personality)
        key = briefing_audio.audio_key(org_id, row["user_id"], briefing_id)
        try:
            data = await backend.synthesize(
                script_of(briefing), voice=voice, language=briefing.language
            )
            await backend.put(key, data, content_type=briefing_audio.MIME)
            outcome, size = "ready", len(data)
        except Exception:  # noqa: BLE001 - the apps speak it on the device instead
            logger.warning("briefing audio failed for %s", briefing_id, exc_info=True)
            outcome, size = "failed", 0
    async with get_sessionmaker()() as session, session.begin():
        await set_rls(session, org_id)
        await session.execute(
            text(
                "UPDATE agent_briefings SET audio_status = :s, audio_key = :k, audio_mime = :mime, "
                "audio_bytes = :b, audio_voice = :v, updated_at = now() WHERE id = :id"
            ),
            {
                "s": outcome,
                "k": key if outcome == "ready" else None,
                "mime": briefing_audio.MIME if outcome == "ready" else None,
                "b": size or None,
                "v": voice if outcome == "ready" else None,
                "id": str(briefing_id),
            },
        )
        await notify(session, org_id, briefing_id)
    return outcome


async def erase_audio(
    session: AsyncSession,
    org_id: uuid.UUID,
    *,
    user_id: uuid.UUID | None = None,
    meeting_id: uuid.UUID | None = None,
) -> int:
    """An erasure's audio leg: the stored voices of a person's briefings (their whole prefix) or
    of a meeting's briefings. The rows go with the erasure's own deletes. Best effort: an object
    left behind is unreachable (no row points at it)."""
    backend = briefing_audio.backend()
    if backend is None:
        return 0
    try:
        if user_id is not None:
            return await backend.delete_prefix(briefing_audio.user_prefix(org_id, user_id))
        keys = (
            await session.execute(
                text(
                    "SELECT audio_key FROM agent_briefings WHERE meeting_id = :m "
                    "AND audio_key IS NOT NULL"
                ),
                {"m": str(meeting_id)},
            )
        ).scalars()
        removed = 0
        for key in keys:
            await backend.delete(str(key))
            removed += 1
        return removed
    except Exception:  # noqa: BLE001 - never block an erasure on object storage
        logger.warning("briefing audio erase failed", exc_info=True)
        return 0
