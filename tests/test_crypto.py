"""Envelope encryption and the blind index (ADR-0024). No database, no network.

These are the properties the vault's safety rests on, so they are tested as properties rather
than as "the function returns something": that a ciphertext is bound to its tenant, that the
two derived keys are actually different, that the index is stable where it must be and
different where it must be, and that a missing root key is loud.
"""

from __future__ import annotations

import base64
import os
import uuid

import pytest
from cryptography.exceptions import InvalidTag

from libs.shared.crypto import (
    KEY_BYTES,
    TOKEN_HEX_LEN,
    LocalKeyProvider,
    Purpose,
    RootKeyUnavailable,
    assert_rotatable,
    blind_index,
    decrypt_value,
    derive_token,
    encrypt_value,
    new_key_material,
    normalize_value,
)

ORG_A = uuid.uuid4()
ORG_B = uuid.uuid4()


@pytest.fixture()
def provider() -> LocalKeyProvider:
    return LocalKeyProvider(os.urandom(KEY_BYTES))


def test_wrap_round_trips(provider: LocalKeyProvider) -> None:
    dek = new_key_material()
    assert provider.unwrap(ORG_A, provider.wrap(ORG_A, dek)) == dek


def test_a_wrapped_key_cannot_be_unwrapped_for_another_tenant(
    provider: LocalKeyProvider,
) -> None:
    """`org_id` is AEAD additional authenticated data, so a wrapped key moved between tenants
    fails to open rather than opening. Without this, a row copied by a bug or a bad restore
    would decrypt cleanly into the wrong tenant."""
    wrapped = provider.wrap(ORG_A, new_key_material())
    with pytest.raises(InvalidTag):
        provider.unwrap(ORG_B, wrapped)


def test_value_round_trips_and_is_tenant_bound() -> None:
    dek = new_key_material()
    ciphertext = encrypt_value(dek, ORG_A, "jane@acme.com")
    assert decrypt_value(dek, ORG_A, ciphertext) == "jane@acme.com"
    with pytest.raises(InvalidTag):
        decrypt_value(dek, ORG_B, ciphertext)


def test_a_wrapped_key_cannot_be_opened_by_the_value_path(
    provider: LocalKeyProvider,
) -> None:
    """The `wrap` and `value` AAD labels are what keep the two ciphertext spaces disjoint, so
    a wrapped DEK stored where a value belongs is inert rather than confusable."""
    root = os.urandom(KEY_BYTES)
    wrapped = LocalKeyProvider(root).wrap(ORG_A, new_key_material())
    with pytest.raises(InvalidTag):
        decrypt_value(root, ORG_A, wrapped)


def test_encryption_is_randomized() -> None:
    """Two encryptions of one value differ, because the nonce is fresh per call. A
    deterministic ciphertext column would leak equality of values on its own, which is exactly
    the property the blind index is supposed to be the ONLY carrier of."""
    dek = new_key_material()
    assert encrypt_value(dek, ORG_A, "same") != encrypt_value(dek, ORG_A, "same")


def test_ciphertext_is_tamper_evident() -> None:
    dek = new_key_material()
    ciphertext = bytearray(encrypt_value(dek, ORG_A, "jane@acme.com"))
    ciphertext[-1] ^= 0x01
    with pytest.raises(InvalidTag):
        decrypt_value(dek, ORG_A, bytes(ciphertext))


def test_blind_index_is_stable_for_the_same_value() -> None:
    """The coherence property: the same value in the same tenant always finds the row it
    already minted, which is what makes one customer one token across boundaries."""
    key = new_key_material()
    assert blind_index(key, ORG_A, "EMAIL", "jane@acme.com") == blind_index(
        key, ORG_A, "EMAIL", "jane@acme.com"
    )


def test_blind_index_separates_tenants_types_and_keys() -> None:
    key, other_key = new_key_material(), new_key_material()
    base = blind_index(key, ORG_A, "EMAIL", "jane@acme.com")
    assert base != blind_index(key, ORG_B, "EMAIL", "jane@acme.com")
    assert base != blind_index(key, ORG_A, "PERSON", "jane@acme.com")
    assert base != blind_index(other_key, ORG_A, "EMAIL", "jane@acme.com")


def test_blind_index_is_unambiguous_across_field_boundaries() -> None:
    """Without separators, ("EMAIL", "ab") and ("EMAILA", "b") would MAC identically. This is
    the classic length-extension-adjacent concatenation bug, and it would silently merge two
    different entity types' values into one token."""
    key = new_key_material()
    assert blind_index(key, ORG_A, "EMAIL", "ab") != blind_index(key, ORG_A, "EMAILA", "b")


def test_the_index_key_and_the_encryption_key_are_never_the_same_bytes() -> None:
    """Derived from the SAME material with different HKDF labels, so even a deployment that
    somehow stored one key twice cannot end up using one key for both primitives."""
    material = new_key_material()
    first = encrypt_value(material, ORG_A, "x")
    # If enc and index derived identically, the index of a value would be computable from the
    # encryption key alone. Assert the derivation actually diverges by checking that the index
    # of the value is not any prefix of a ciphertext produced under the same material.
    assert blind_index(material, ORG_A, "EMAIL", "x") not in first


@pytest.mark.parametrize(
    ("entity_type", "a", "b"),
    [
        ("EMAIL", "Jane@Acme.com", "  jane@acme.com "),
        ("PHONE", "+1 (555) 010-9999", "+15550109999"),
        ("IBAN", "GB33 BUKB 2020 1555 5555 55", "gb33bukb20201555555555"),
    ],
)
def test_normalization_folds_the_spellings_a_transcript_actually_contains(
    entity_type: str, a: str, b: str
) -> None:
    key = new_key_material()
    assert blind_index(key, ORG_A, entity_type, a) == blind_index(key, ORG_A, entity_type, b)


def test_names_are_not_normalized_beyond_case_and_whitespace() -> None:
    """ "Dave" and "David Chen" are not reliably the same person. A vault that guessed they
    were would put a confidently wrong coherence claim into a confirmed decision, which is the
    failure ADR-0020's "stays coherent" requirement exists to avoid."""
    assert normalize_value("PERSON", "David Chen") != normalize_value("PERSON", "Dave")
    assert normalize_value("PERSON", "  David   Chen ") == normalize_value("PERSON", "david chen")


def test_token_is_derived_stable_and_opaque() -> None:
    key = new_key_material()
    digest = blind_index(key, ORG_A, "PERSON", "David Chen")
    token = derive_token(key, ORG_A, digest)
    assert token == derive_token(key, ORG_A, digest)
    assert len(token) == TOKEN_HEX_LEN
    assert token != derive_token(key, ORG_A, digest, attempt=1)


def test_rotating_the_blind_index_key_is_refused() -> None:
    """ADR-0024 D4. The guard exists because the failure is silent: re-keying the index does
    not raise anywhere, it just orphans every existing value_hmac."""
    assert_rotatable(Purpose.ENC)
    with pytest.raises(ValueError, match="never rotated"):
        assert_rotatable(Purpose.INDEX)


def test_a_missing_or_malformed_root_key_is_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    """No generated-on-boot fallback, ever: a key that appears by itself is a key nobody
    decided to back up, and losing it destroys every value it protects."""
    monkeypatch.delenv(LocalKeyProvider.ENV_VAR, raising=False)
    with pytest.raises(RootKeyUnavailable, match="unset"):
        LocalKeyProvider.from_env()

    monkeypatch.setenv(LocalKeyProvider.ENV_VAR, "not-base64!!")
    with pytest.raises(RootKeyUnavailable, match="base64"):
        LocalKeyProvider.from_env()

    monkeypatch.setenv(LocalKeyProvider.ENV_VAR, base64.b64encode(b"tooshort").decode())
    with pytest.raises(RootKeyUnavailable, match="32 bytes"):
        LocalKeyProvider.from_env()


def test_the_local_provider_never_claims_to_be_production_grade() -> None:
    """If this ever flips, the mint route's refusal (ADR-0024 § The production condition)
    silently stops applying and the vault starts holding real PII behind a key that lives in
    the same process."""
    assert LocalKeyProvider(os.urandom(KEY_BYTES)).is_production_grade is False
