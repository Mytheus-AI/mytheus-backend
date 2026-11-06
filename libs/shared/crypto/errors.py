"""The crypto library's error vocabulary.

Every message here is written so it can be logged: none of them ever carries key material,
plaintext, ciphertext bytes, or a wrapped key. A failed decrypt says THAT it failed, never
what it was given, because the input to a decrypt of a tampered blob is attacker-chosen and
the input to a decrypt of a genuine one is a secret.
"""

from __future__ import annotations


class CryptoError(Exception):
    """Base class. Catch this to treat every crypto failure as one fail-closed outcome."""


class KeyConfigurationError(CryptoError):
    """The key provider cannot be built from the environment. Raised at startup, not per call.

    Fail closed: a service that cannot find its key-encryption key refuses to start rather
    than generating one, because a generated key encrypts data nobody can ever read again.
    """


class KeyUnavailableError(CryptoError):
    """A blob names a key this provider does not hold (unknown or decommissioned KEK id)."""


class DecryptionError(CryptoError):
    """Authentication failed: the blob was tampered with, truncated, or bound to other context.

    Deliberately one type for every cause - wrong tenant, wrong purpose, flipped bit, swapped
    wrapped key - so a caller cannot build an oracle out of distinguishing them.
    """


class MalformedEnvelopeError(DecryptionError):
    """The blob does not parse as an envelope at all. A subclass so callers may ignore it."""
