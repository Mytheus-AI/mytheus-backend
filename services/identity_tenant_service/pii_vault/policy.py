"""The unavailable-NER decision, as code the redaction boundaries can call.

ADR-0020 left this to Security/Compliance. DECIDED by the lead (2026-11-06), confirming the
default ADR-0020's amendment proposed: when `ner-service` is unreachable and the tenant has
`pii_redaction_enabled`, the final is HELD (not persisted), never persisted regex-only and
never persisted raw. Losing (or delaying) a final is recoverable - it can be retried from the
held buffer, or re-transcribed from the recording - while a persisted row carrying names and
organizations the regex layer never covered is a disclosure that cannot be taken back once
embedded and fanned out.

Kept here, beside the vault, so every boundary imports one answer rather than three copies.
Nothing calls it yet; wiring into `transcription-service` is owned by another phase.
"""

from __future__ import annotations

from enum import StrEnum


class FinalDisposition(StrEnum):
    PERSIST = "persist"  # redaction off, or full (regex + NER) redaction succeeded
    HOLD = "hold"  # do not persist; retry or drop per the boundary's buffer policy


def final_disposition(*, pii_redaction_enabled: bool, ner_available: bool) -> FinalDisposition:
    """Decided (ADR-0020 amendment, lead 2026-11-06): fail closed when NER is unavailable."""
    if not pii_redaction_enabled:
        return FinalDisposition.PERSIST
    return FinalDisposition.PERSIST if ner_available else FinalDisposition.HOLD
