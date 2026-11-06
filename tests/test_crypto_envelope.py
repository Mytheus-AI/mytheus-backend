"""Unit tests for `libs/shared/crypto` (gap 22, ADR-0024). No database, no network.

Every key here is generated with `os.urandom` at test time; nothing that looks like a key is
committed to the repository.
"""

from __future__ import annotations

import base64
import os
import uuid

import pytest

from libs.shared.crypto import (
    BlindIndex,
    DecryptionError,
    EnvelopeCipher,
    KeyConfigurationError,
    KeyUnavailableError,
    KmsKeyProvider,
    LocalKeyProvider,
    MalformedEnvelopeError,
    key_provider_from_env,
    parse_envelope,
)

PURPOSE = "oauth_tokens.access_token"


def _b64() -> str:
    return base64.b64encode(os.urandom(32)).decode()


def _env(**overrides: str) -> dict[str, str]:
    env = {
        "ENVIRONMENT": "test",
        "MYTHEUS_KEK_ID": "local-1",
        "MYTHEUS_KEK_BASE64": _b64(),
        "MYTHEUS_BLIND_INDEX_KEY_BASE64": _b64(),
    }
    env.update(overrides)
    return env


@pytest.fixture
def env() -> dict[str, str]:
    return _env()


@pytest.fixture
def cipher(env: dict[str, str]) -> EnvelopeCipher:
    return EnvelopeCipher(LocalKeyProvider.from_env(env))


# --- round trip and binding ---------------------------------------------------------------


def test_round_trip(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    blob = cipher.encrypt_str(org, PURPOSE, "ya29.secret", {"connection_id": "c1"})
    assert b"ya29.secret" not in blob
    assert cipher.decrypt_str(org, PURPOSE, blob, {"connection_id": "c1"}) == "ya29.secret"


def test_same_plaintext_encrypts_differently(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    assert cipher.encrypt(org, PURPOSE, b"x") != cipher.encrypt(org, PURPOSE, b"x")


def test_wrong_tenant_aad_fails(cipher: EnvelopeCipher) -> None:
    blob = cipher.encrypt(uuid.uuid4(), PURPOSE, b"secret")
    with pytest.raises(DecryptionError):
        cipher.decrypt(uuid.uuid4(), PURPOSE, blob)


def test_wrong_tenant_fails_even_on_a_fresh_process(env: dict[str, str]) -> None:
    """No unwrap cache to lean on: the KEK wrap itself must refuse the other tenant."""
    blob = EnvelopeCipher(LocalKeyProvider.from_env(env)).encrypt(uuid.uuid4(), PURPOSE, b"s")
    fresh = EnvelopeCipher(LocalKeyProvider.from_env(env))
    with pytest.raises(DecryptionError):
        fresh.decrypt(uuid.uuid4(), PURPOSE, blob)


def test_wrong_purpose_fails(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    blob = cipher.encrypt(org, PURPOSE, b"secret")
    with pytest.raises(DecryptionError):
        cipher.decrypt(org, "oauth_tokens.refresh_token", blob)


def test_wrong_row_context_fails(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    blob = cipher.encrypt(org, PURPOSE, b"secret", {"connection_id": "a"})
    with pytest.raises(DecryptionError):
        cipher.decrypt(org, PURPOSE, blob, {"connection_id": "b"})
    with pytest.raises(DecryptionError):
        cipher.decrypt(org, PURPOSE, blob)


def test_tenants_never_share_a_dek(cipher: EnvelopeCipher) -> None:
    a = parse_envelope(cipher.encrypt(uuid.uuid4(), PURPOSE, b"x"))
    b = parse_envelope(cipher.encrypt(uuid.uuid4(), PURPOSE, b"x"))
    assert a.dek_id != b.dek_id


def test_dek_is_reused_within_tenant_and_purpose(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    a = parse_envelope(cipher.encrypt(org, PURPOSE, b"x"))
    b = parse_envelope(cipher.encrypt(org, PURPOSE, b"y"))
    assert a.dek_id == b.dek_id


def test_different_kek_cannot_decrypt(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    blob = EnvelopeCipher(LocalKeyProvider.from_env(env)).encrypt(org, PURPOSE, b"s")
    other = EnvelopeCipher(LocalKeyProvider.from_env({**env, "MYTHEUS_KEK_BASE64": _b64()}))
    with pytest.raises(DecryptionError):
        other.decrypt(org, PURPOSE, blob)


# --- tamper -------------------------------------------------------------------------------


def test_every_single_bit_flip_is_rejected(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    blob = cipher.encrypt(org, PURPOSE, b"secret-value")
    for i in range(len(blob)):
        tampered = bytearray(blob)
        tampered[i] ^= 0x01
        with pytest.raises((DecryptionError, KeyUnavailableError)):
            cipher.decrypt(org, PURPOSE, bytes(tampered))


def test_truncation_is_rejected(cipher: EnvelopeCipher) -> None:
    org = uuid.uuid4()
    blob = cipher.encrypt(org, PURPOSE, b"secret-value")
    for n in range(len(blob)):
        with pytest.raises(DecryptionError):
            cipher.decrypt(org, PURPOSE, blob[:n])


def test_garbage_is_malformed(cipher: EnvelopeCipher) -> None:
    with pytest.raises(MalformedEnvelopeError):
        cipher.decrypt(uuid.uuid4(), PURPOSE, b"not an envelope at all")


def test_swapping_wrapped_dek_between_tenants_fails(cipher: EnvelopeCipher) -> None:
    """Graft tenant B's header (its wrapped DEK) onto tenant A's ciphertext."""
    org_a, org_b = uuid.uuid4(), uuid.uuid4()
    blob_a = cipher.encrypt(org_a, PURPOSE, b"secret")
    blob_b = cipher.encrypt(org_b, PURPOSE, b"secret")
    ha, hb = parse_envelope(blob_a), parse_envelope(blob_b)
    head_len_a = len(blob_a) - (12 + len(b"secret") + 16)
    head_len_b = len(blob_b) - (12 + len(b"secret") + 16)
    assert ha.dek_id != hb.dek_id
    franken = blob_b[:head_len_b] + blob_a[head_len_a:]
    for org in (org_a, org_b):
        with pytest.raises(DecryptionError):
            cipher.decrypt(org, PURPOSE, franken)


# --- rotation -----------------------------------------------------------------------------


def test_kek_rotation_old_blobs_still_decrypt_and_rewrap(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    old_kek = env["MYTHEUS_KEK_BASE64"]
    v1 = EnvelopeCipher(LocalKeyProvider.from_env(env))
    blob = v1.encrypt(org, PURPOSE, b"secret", {"connection_id": "c"})
    assert parse_envelope(blob).kek_id == "local-1"

    rotated_env = {
        **env,
        "MYTHEUS_KEK_ID": "local-2",
        "MYTHEUS_KEK_BASE64": _b64(),
        "MYTHEUS_KEK_RETIRED": f"local-1={old_kek}",
    }
    v2 = EnvelopeCipher(LocalKeyProvider.from_env(rotated_env))
    assert v2.needs_rewrap(blob)
    assert v2.decrypt(org, PURPOSE, blob, {"connection_id": "c"}) == b"secret"

    rewrapped = v2.rewrap(org, PURPOSE, blob)
    h_old, h_new = parse_envelope(blob), parse_envelope(rewrapped)
    assert h_new.kek_id == "local-2" and h_new.dek_id == h_old.dek_id
    assert not v2.needs_rewrap(rewrapped)
    # Data ciphertext is byte-identical; only the header changed.
    assert rewrapped.endswith(blob[-(12 + 6 + 16) :])

    # Once local-1 is decommissioned, only the rewrapped blob survives.
    final_env = {**rotated_env, "MYTHEUS_KEK_RETIRED": ""}
    v3 = EnvelopeCipher(LocalKeyProvider.from_env(final_env))
    assert v3.decrypt(org, PURPOSE, rewrapped, {"connection_id": "c"}) == b"secret"
    with pytest.raises(KeyUnavailableError):
        v3.decrypt(org, PURPOSE, blob, {"connection_id": "c"})


def test_rewrap_refuses_wrong_tenant(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    old_kek = env["MYTHEUS_KEK_BASE64"]
    blob = EnvelopeCipher(LocalKeyProvider.from_env(env)).encrypt(org, PURPOSE, b"s")
    v2 = EnvelopeCipher(
        LocalKeyProvider.from_env(
            {
                **env,
                "MYTHEUS_KEK_ID": "local-2",
                "MYTHEUS_KEK_BASE64": _b64(),
                "MYTHEUS_KEK_RETIRED": f"local-1={old_kek}",
            }
        )
    )
    with pytest.raises(DecryptionError):
        v2.rewrap(uuid.uuid4(), PURPOSE, blob)


def test_new_writes_switch_to_the_new_kek_immediately(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    blob = EnvelopeCipher(LocalKeyProvider.from_env(env)).encrypt(org, PURPOSE, b"s")
    v2 = EnvelopeCipher(
        LocalKeyProvider.from_env(
            {
                **env,
                "MYTHEUS_KEK_ID": "local-2",
                "MYTHEUS_KEK_BASE64": _b64(),
                "MYTHEUS_KEK_RETIRED": f"local-1={env['MYTHEUS_KEK_BASE64']}",
            }
        )
    )
    v2.decrypt(org, PURPOSE, blob)  # warms the unwrap cache with the old DEK
    assert parse_envelope(v2.encrypt(org, PURPOSE, b"t")).kek_id == "local-2"


def test_dek_rotates_by_use_count(env: dict[str, str]) -> None:
    c = EnvelopeCipher(LocalKeyProvider.from_env(env), dek_max_uses=2)
    org = uuid.uuid4()
    blobs = [c.encrypt(org, PURPOSE, b"x") for _ in range(5)]
    ids = [parse_envelope(b).dek_id for b in blobs]
    assert ids[0] == ids[1] != ids[2] == ids[3] != ids[4]
    assert all(c.decrypt(org, PURPOSE, b) == b"x" for b in blobs)


def test_dek_rotates_by_age(env: dict[str, str]) -> None:
    now = [0.0]
    c = EnvelopeCipher(LocalKeyProvider.from_env(env), dek_max_age_seconds=10, clock=lambda: now[0])
    org = uuid.uuid4()
    first = parse_envelope(c.encrypt(org, PURPOSE, b"x")).dek_id
    now[0] = 11.0
    assert parse_envelope(c.encrypt(org, PURPOSE, b"x")).dek_id != first


def test_reencrypt_replaces_the_dek(env: dict[str, str]) -> None:
    c = EnvelopeCipher(LocalKeyProvider.from_env(env), dek_max_uses=1)
    org = uuid.uuid4()
    blob = c.encrypt(org, PURPOSE, b"x", {"connection_id": "c"})
    again = c.reencrypt(org, PURPOSE, blob, {"connection_id": "c"})
    assert parse_envelope(again).dek_id != parse_envelope(blob).dek_id
    assert c.decrypt(org, PURPOSE, again, {"connection_id": "c"}) == b"x"


# --- input validation ---------------------------------------------------------------------


def test_org_id_must_be_a_uuid(cipher: EnvelopeCipher) -> None:
    with pytest.raises(TypeError):
        cipher.encrypt(str(uuid.uuid4()), PURPOSE, b"x")  # type: ignore[arg-type]


@pytest.mark.parametrize("purpose", ["", "Upper", "has space", "x" * 65])
def test_bad_purpose_is_refused(cipher: EnvelopeCipher, purpose: str) -> None:
    with pytest.raises(ValueError):
        cipher.encrypt(uuid.uuid4(), purpose, b"x")


def test_context_cannot_shadow_reserved_keys(cipher: EnvelopeCipher) -> None:
    with pytest.raises(ValueError):
        cipher.encrypt(uuid.uuid4(), PURPOSE, b"x", {"org_id": str(uuid.uuid4())})


# --- provider configuration: fail closed --------------------------------------------------


@pytest.mark.parametrize("environment", ["production", "staging", "", "PRODUCTION"])
def test_local_provider_refuses_non_dev(environment: str) -> None:
    with pytest.raises(KeyConfigurationError):
        LocalKeyProvider.from_env(_env(ENVIRONMENT=environment))


def test_local_provider_refuses_when_environment_unset() -> None:
    env = _env()
    del env["ENVIRONMENT"]
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(env)


def test_local_provider_refuses_missing_kek_even_in_dev() -> None:
    with pytest.raises(KeyConfigurationError):
        LocalKeyProvider.from_env(_env(MYTHEUS_KEK_BASE64=""))


def test_local_provider_refuses_missing_index_key() -> None:
    with pytest.raises(KeyConfigurationError):
        LocalKeyProvider.from_env(_env(MYTHEUS_BLIND_INDEX_KEY_BASE64=""))


@pytest.mark.parametrize(
    "bad",
    ["not base64!!", base64.b64encode(b"short").decode(), base64.b64encode(b"x" * 33).decode()],
)
def test_local_provider_refuses_bad_key_material(bad: str) -> None:
    with pytest.raises(KeyConfigurationError):
        LocalKeyProvider.from_env(_env(MYTHEUS_KEK_BASE64=bad))


def test_local_provider_refuses_malformed_retired_list() -> None:
    with pytest.raises(KeyConfigurationError):
        LocalKeyProvider.from_env(_env(MYTHEUS_KEK_RETIRED="no-equals-sign"))


def test_unknown_provider_kind_is_refused() -> None:
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(_env(MYTHEUS_KEY_CUSTODY="vault"))


def test_kms_provider_is_a_boundary_only() -> None:
    p = key_provider_from_env(_env(MYTHEUS_KEY_CUSTODY="kms", MYTHEUS_KMS_KEY_URI="aws-kms://x"))
    assert isinstance(p, KmsKeyProvider)
    with pytest.raises(NotImplementedError):
        EnvelopeCipher(p).encrypt(uuid.uuid4(), PURPOSE, b"x")
    with pytest.raises(NotImplementedError):
        p.derive_tenant_key(uuid.uuid4(), PURPOSE)


def test_kms_provider_requires_a_key_uri() -> None:
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(_env(MYTHEUS_KEY_CUSTODY="kms"))


# --- no key material in repr / errors -----------------------------------------------------


def test_reprs_and_errors_carry_no_key_material(env: dict[str, str]) -> None:
    provider = LocalKeyProvider.from_env(env)
    c = EnvelopeCipher(provider)
    org = uuid.uuid4()
    blob = c.encrypt(org, PURPOSE, b"plaintext-marker")
    kek = base64.b64decode(env["MYTHEUS_KEK_BASE64"])
    texts = [repr(provider), repr(c), repr(parse_envelope(blob)), repr(BlindIndex(provider))]
    try:
        c.decrypt(uuid.uuid4(), PURPOSE, blob)
    except DecryptionError as exc:
        texts.append(str(exc))
    for t in texts:
        assert env["MYTHEUS_KEK_BASE64"] not in t
        assert kek.hex() not in t
        assert "plaintext-marker" not in t
        assert parse_envelope(blob).wrapped_dek.hex() not in t


# --- blind index --------------------------------------------------------------------------


def test_blind_index_is_deterministic_across_processes(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    a = BlindIndex(LocalKeyProvider.from_env(env)).compute(org, "pii_tokens.value", b"a@b.co")
    b = BlindIndex(LocalKeyProvider.from_env(env)).compute(org, "pii_tokens.value", b"a@b.co")
    assert a == b and len(a) == 32


def test_blind_index_differs_per_tenant_and_purpose(env: dict[str, str]) -> None:
    idx = BlindIndex(LocalKeyProvider.from_env(env))
    org = uuid.uuid4()
    base = idx.compute(org, "pii_tokens.value", b"a@b.co")
    assert base != idx.compute(uuid.uuid4(), "pii_tokens.value", b"a@b.co")
    assert base != idx.compute(org, "other.purpose", b"a@b.co")


def test_blind_index_survives_kek_rotation(env: dict[str, str]) -> None:
    org = uuid.uuid4()
    before = BlindIndex(LocalKeyProvider.from_env(env)).compute(org, "p.v", b"x")
    rotated = {**env, "MYTHEUS_KEK_ID": "local-2", "MYTHEUS_KEK_BASE64": _b64()}
    after = BlindIndex(LocalKeyProvider.from_env(rotated)).compute(org, "p.v", b"x")
    assert before == after
