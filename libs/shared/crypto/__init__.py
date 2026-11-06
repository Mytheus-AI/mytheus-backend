"""Field-level envelope encryption and keyed blind indexes (gap 22, ADR-0024, ADR-0026).

    from libs.shared.crypto import EnvelopeCipher, BlindIndex, key_provider_from_env

    provider = key_provider_from_env()        # fails closed at startup if misconfigured
    cipher = EnvelopeCipher(provider)
    blob = cipher.encrypt_str(org_id, "oauth_tokens.access_token", token,
                              {"connection_id": str(connection_id)})

See `envelope.py` for the scheme and rotation, `providers.py` for custody and environment.
"""

from libs.shared.crypto.blind_index import BlindIndex
from libs.shared.crypto.envelope import EnvelopeCipher, EnvelopeHeader, parse_envelope
from libs.shared.crypto.errors import (
    CryptoError,
    DecryptionError,
    KeyConfigurationError,
    KeyUnavailableError,
    MalformedEnvelopeError,
)
from libs.shared.crypto.providers import (
    FileKeyProvider,
    KeyProvider,
    KmsKeyProvider,
    LocalKeyProvider,
    RotatingKeyProvider,
    WrappedKey,
    key_provider_from_env,
)

__all__ = [
    "BlindIndex",
    "CryptoError",
    "DecryptionError",
    "EnvelopeCipher",
    "EnvelopeHeader",
    "FileKeyProvider",
    "KeyConfigurationError",
    "KeyProvider",
    "KeyUnavailableError",
    "KmsKeyProvider",
    "LocalKeyProvider",
    "MalformedEnvelopeError",
    "RotatingKeyProvider",
    "WrappedKey",
    "key_provider_from_env",
    "parse_envelope",
]
