"""Entity types and the normalization that decides when two raw values are "the same".

Normalization feeds only the blind index (`value_hmac`), never the ciphertext: the vault
stores the raw value as first seen, and resolves a token back to exactly that. What
normalization buys is coherence - `Alice@Example.com` in a transcript and `alice@example.com`
in an uploaded document mint the same token - which is the property ADR-0020 exists for.

Kept deliberately conservative. Over-normalizing merges two different people into one token,
which is a correctness bug that leaks one person's identity into another's records; under-
normalizing only costs a second token for the same person.
"""

from __future__ import annotations

import re
import unicodedata
from enum import StrEnum

MAX_VALUE_CHARS = 1024


class EntityType(StrEnum):
    """Canon `_canon/02-data-model.md` `pii_tokens.entity_type`. Mirrored by a CHECK."""

    EMAIL = "EMAIL"
    PHONE = "PHONE"
    IBAN = "IBAN"
    CARD = "CARD"
    GOVID = "GOVID"
    PERSON = "PERSON"
    ORG = "ORG"
    LOC = "LOC"


_WS = re.compile(r"\s+")
_SEPARATORS = re.compile(r"[\s\-./()]+")


def normalize(entity_type: EntityType, value: str) -> str:
    """Canonical form for the blind index. Raises `ValueError` for empty or oversized input."""
    if not isinstance(value, str):
        raise TypeError("value must be str")
    if len(value) > MAX_VALUE_CHARS:
        raise ValueError(f"value exceeds {MAX_VALUE_CHARS} characters")
    v = unicodedata.normalize("NFKC", value).strip()
    if entity_type is EntityType.EMAIL:
        out = v.lower()
    elif entity_type is EntityType.PHONE:
        # Keep a leading + (it distinguishes an international number) and digits only.
        digits = re.sub(r"\D", "", v)
        out = ("+" + digits) if v.startswith("+") else digits
    elif entity_type in (EntityType.IBAN, EntityType.CARD, EntityType.GOVID):
        out = _SEPARATORS.sub("", v).upper()
    else:  # PERSON, ORG, LOC
        out = _WS.sub(" ", v).casefold()
    if not out:
        raise ValueError("value is empty after normalization")
    return out
