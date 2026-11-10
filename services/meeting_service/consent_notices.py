"""The recording and transcription notices a participant can agree to, by version.

A consent receipt (`GET /api/meetings/{id}/consent-receipt`) must say what the person agreed
to, not what the notice says today. `meeting_participants.consent_version` names the text; this
catalog maps each version to its clauses, so a receipt for an older agreement still shows the
words that were on screen. The UI owns the live copy (`mytheus-ui`
`src/components/call/consent.tsx`, `CONSENT_VERSION`); every version it has shipped is listed
here, verbatim, and a new UI version must add its entry here in the same change.

The audio/video clause depends on whether the deployment records calls (`RECORDING_ENABLED`),
which the notice showed at the time; the receipt renders it from the same setting.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Clause:
    key: str
    title: str
    body: str


@dataclass(frozen=True)
class Notice:
    version: str
    clauses: tuple[Clause, ...]
    # The two variants of the audio/video clause, keyed by whether calls are recorded.
    recording: tuple[Clause, Clause]

    def render(self, recording: bool) -> list[Clause]:
        av = self.recording[0] if recording else self.recording[1]
        return [self.clauses[0], av, *self.clauses[1:]]


_TRANSCRIPT = Clause(
    "transcript",
    "A transcript is kept",
    "What is said is transcribed with who said it, and kept with the meeting in your workspace "
    "for as long as its retention setting allows.",
)
_RECORDED = Clause(
    "audio_video",
    "Audio and video are recorded",
    "The call is recorded and stored with the meeting in your workspace.",
)
_NOT_RECORDED = Clause(
    "audio_video",
    "Audio and video are not recorded",
    "Speech is sent to our transcription provider to write the transcript. It is not kept as "
    "audio.",
)
_NOTES = Clause(
    "agent_notes",
    "Your agent's notes are yours",
    "Only you can see the notes your agent takes for you.",
)
_DECISIONS = Clause(
    "decisions",
    "Nothing is decided for you",
    "Decisions and action items are saved only when someone in the meeting confirms them.",
)
_CHAT = Clause(
    "chat",
    "Chat is kept",
    "Messages in the meeting chat are stored with the meeting, so people who join later can "
    "read them.",
)
_AGREEMENT = Clause(
    "agreement",
    "Your agreement is recorded",
    "We store when you agreed and which version of this notice you saw. You are asked once per "
    "meeting.",
)
_OFF_RECORD = Clause(
    "off_record",
    "You can go off the record",
    "Anyone can ask the host to go off the record. Until it ends, nothing said is transcribed or "
    "kept and every agent pauses; the transcript shows only that a gap happened.",
)

NOTICES: dict[str, Notice] = {
    "room-notice-2026-11-11": Notice(
        "room-notice-2026-11-11",
        (_TRANSCRIPT, _NOTES, _DECISIONS, _CHAT, _AGREEMENT),
        (_RECORDED, _NOT_RECORDED),
    ),
    "room-notice-2026-11-11.2": Notice(
        "room-notice-2026-11-11.2",
        (_TRANSCRIPT, _NOTES, _DECISIONS, _CHAT, _OFF_RECORD, _AGREEMENT),
        (_RECORDED, _NOT_RECORDED),
    ),
}

CURRENT_VERSION = "room-notice-2026-11-11.2"


def clauses_for(version: str | None, recording: bool) -> list[Clause] | None:
    """The clauses of `version` as they were shown, or None for a version this catalog does not
    know (an older client's label): the receipt then says so instead of guessing."""
    notice = NOTICES.get(version or "")
    return notice.render(recording) if notice is not None else None
