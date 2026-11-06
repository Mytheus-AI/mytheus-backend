"""`FileKeyProvider` and custodian selection (ADR-0026): production key custody on the VPS.

Key files are written to pytest's `tmp_path` with keys from `os.urandom` at test time.
"""

from __future__ import annotations

import base64
import os
import uuid
from pathlib import Path

import pytest

from libs.shared.crypto import (
    EnvelopeCipher,
    FileKeyProvider,
    KeyConfigurationError,
    KeyUnavailableError,
    LocalKeyProvider,
    RotatingKeyProvider,
    key_provider_from_env,
    parse_envelope,
)
from libs.shared.crypto.providers import read_key_file

PURPOSE = "pii_tokens.value"


def _key_file(tmp_path: Path, name: str, mode: int = 0o400, content: str | None = None) -> str:
    path = tmp_path / name
    path.write_text(
        base64.b64encode(os.urandom(32)).decode() + "\n" if content is None else content
    )
    path.chmod(mode)
    return str(path)


def _prod_env(tmp_path: Path, **overrides: str) -> dict[str, str]:
    # No ENVIRONMENT at all: exactly what prod.env on the VPS looks like.
    env = {
        "MYTHEUS_KEY_CUSTODY": "local-file",
        "MYTHEUS_KEK_FILE": _key_file(tmp_path, "kek"),
        "MYTHEUS_BLIND_INDEX_KEY_FILE": _key_file(tmp_path, "idx"),
    }
    env.update(overrides)
    return env


def test_round_trip_in_production_mode(tmp_path: Path) -> None:
    provider = key_provider_from_env(_prod_env(tmp_path))
    assert isinstance(provider, FileKeyProvider)
    assert provider.active_kek_id == "file-1"
    c = EnvelopeCipher(provider)
    org = uuid.uuid4()
    blob = c.encrypt(org, PURPOSE, b"secret", {"row": "1"})
    assert parse_envelope(blob).kek_id == "file-1"
    assert c.decrypt(org, PURPOSE, blob, {"row": "1"}) == b"secret"


@pytest.mark.parametrize("mode", [0o400, 0o600])
def test_private_modes_are_accepted(tmp_path: Path, mode: int) -> None:
    assert len(read_key_file(_key_file(tmp_path, "k", mode), name="t")) == 32


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o604, 0o440, 0o700, 0o666])
def test_loose_permissions_are_refused(tmp_path: Path, mode: int) -> None:
    with pytest.raises(KeyConfigurationError, match="must be 0400 or 0600"):
        read_key_file(_key_file(tmp_path, "k", mode), name="t")


def test_loose_kek_file_refuses_startup(tmp_path: Path) -> None:
    env = _prod_env(tmp_path, MYTHEUS_KEK_FILE=_key_file(tmp_path, "loose", 0o644))
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(env)


def test_loose_blind_index_file_refuses_startup(tmp_path: Path) -> None:
    env = _prod_env(tmp_path, MYTHEUS_BLIND_INDEX_KEY_FILE=_key_file(tmp_path, "loose", 0o644))
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(env)


def test_missing_file_is_refused(tmp_path: Path) -> None:
    with pytest.raises(KeyConfigurationError, match="does not exist"):
        key_provider_from_env(_prod_env(tmp_path, MYTHEUS_KEK_FILE=str(tmp_path / "absent")))


def test_default_path_is_used_when_unset(tmp_path: Path) -> None:
    env = _prod_env(tmp_path)
    del env["MYTHEUS_KEK_FILE"]
    if os.path.exists("/run/secrets/mytheus_kek"):
        pytest.skip("a real key exists at the default path on this machine")
    with pytest.raises(KeyConfigurationError, match="/run/secrets/mytheus_kek"):
        key_provider_from_env(env)


@pytest.mark.parametrize(
    "content",
    [
        "",
        "   \n",
        "not base64!!",
        base64.b64encode(b"short").decode(),
        base64.b64encode(b"x" * 33).decode(),
    ],
)
def test_empty_short_or_garbage_key_is_refused(tmp_path: Path, content: str) -> None:
    with pytest.raises(KeyConfigurationError):
        read_key_file(_key_file(tmp_path, "k", content=content), name="t")


def test_directory_is_refused(tmp_path: Path) -> None:
    d = tmp_path / "dir"
    d.mkdir(mode=0o700)
    d.chmod(0o400)
    try:
        with pytest.raises(KeyConfigurationError, match="not a regular file"):
            read_key_file(str(d), name="t")
    finally:
        d.chmod(0o700)


def test_error_messages_never_contain_key_material(tmp_path: Path) -> None:
    secret = base64.b64encode(os.urandom(31)).decode()  # wrong length, so it is refused
    with pytest.raises(KeyConfigurationError) as exc:
        read_key_file(_key_file(tmp_path, "k", content=secret), name="t")
    assert secret not in str(exc.value)


@pytest.mark.parametrize(
    "var", ["MYTHEUS_KEK_BASE64", "MYTHEUS_BLIND_INDEX_KEY_BASE64", "MYTHEUS_KEK_RETIRED"]
)
def test_production_refuses_a_key_from_an_env_var(tmp_path: Path, var: str) -> None:
    env = _prod_env(tmp_path, **{var: base64.b64encode(os.urandom(32)).decode()})
    with pytest.raises(KeyConfigurationError, match="refuses key material in environment"):
        key_provider_from_env(env)


def test_production_without_explicit_custody_fails_closed(tmp_path: Path) -> None:
    env = _prod_env(tmp_path)
    del env["MYTHEUS_KEY_CUSTODY"]
    with pytest.raises(KeyConfigurationError, match="MYTHEUS_KEY_CUSTODY is unset"):
        key_provider_from_env(env)


def test_production_cannot_select_the_env_provider(tmp_path: Path) -> None:
    env = {
        "MYTHEUS_KEY_CUSTODY": "local-env",
        "MYTHEUS_KEK_BASE64": base64.b64encode(os.urandom(32)).decode(),
        "MYTHEUS_BLIND_INDEX_KEY_BASE64": base64.b64encode(os.urandom(32)).decode(),
    }
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env(env)
    with pytest.raises(KeyConfigurationError):
        key_provider_from_env({**env, "ENVIRONMENT": "production"})


def test_unknown_custody_is_refused(tmp_path: Path) -> None:
    with pytest.raises(KeyConfigurationError, match="unknown"):
        key_provider_from_env(_prod_env(tmp_path, MYTHEUS_KEY_CUSTODY="vault"))


# --- rotation -----------------------------------------------------------------------------


def test_file_kek_rotation_rewrap(tmp_path: Path) -> None:
    org = uuid.uuid4()
    v1_env = _prod_env(tmp_path)
    v1 = EnvelopeCipher(key_provider_from_env(v1_env))
    blob = v1.encrypt(org, PURPOSE, b"secret", {"row": "1"})

    v2_env = {
        **v1_env,
        "MYTHEUS_KEK_ID": "file-2",
        "MYTHEUS_KEK_FILE": _key_file(tmp_path, "kek2"),
        "MYTHEUS_KEK_RETIRED_FILES": f"file-1={v1_env['MYTHEUS_KEK_FILE']}",
    }
    v2 = EnvelopeCipher(key_provider_from_env(v2_env))
    assert v2.needs_rewrap(blob)
    assert v2.decrypt(org, PURPOSE, blob, {"row": "1"}) == b"secret"
    rewrapped = v2.rewrap(org, PURPOSE, blob)
    assert parse_envelope(rewrapped).kek_id == "file-2"

    v3 = EnvelopeCipher(key_provider_from_env({**v2_env, "MYTHEUS_KEK_RETIRED_FILES": ""}))
    assert v3.decrypt(org, PURPOSE, rewrapped, {"row": "1"}) == b"secret"
    with pytest.raises(KeyUnavailableError):
        v3.decrypt(org, PURPOSE, blob, {"row": "1"})


def test_custodian_swap_needs_no_data_migration(tmp_path: Path) -> None:
    """File KEK -> a different custodian (a stand-in for KMS) via RotatingKeyProvider.

    The blind index stays with the file provider, so every `value_hmac` is unchanged.
    """
    org = uuid.uuid4()
    file_provider = FileKeyProvider.from_env(_prod_env(tmp_path))
    blob = EnvelopeCipher(file_provider).encrypt(org, PURPOSE, b"secret")
    stand_in_kms = LocalKeyProvider.from_env(
        {
            "ENVIRONMENT": "test",
            "MYTHEUS_KEK_ID": "kms-1",
            "MYTHEUS_KEK_BASE64": base64.b64encode(os.urandom(32)).decode(),
            "MYTHEUS_BLIND_INDEX_KEY_BASE64": base64.b64encode(os.urandom(32)).decode(),
        }
    )
    rotating = RotatingKeyProvider(stand_in_kms, [file_provider], index_provider=file_provider)
    c = EnvelopeCipher(rotating)
    assert c.needs_rewrap(blob)
    moved = c.rewrap(org, PURPOSE, blob)
    assert parse_envelope(moved).kek_id == "kms-1"
    assert EnvelopeCipher(stand_in_kms).decrypt(org, PURPOSE, moved) == b"secret"
    assert rotating.derive_tenant_key(org, PURPOSE) == file_provider.derive_tenant_key(org, PURPOSE)


def test_rotating_provider_refuses_duplicate_kek_ids(tmp_path: Path) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    a = FileKeyProvider.from_env(_prod_env(tmp_path / "a"))
    b = FileKeyProvider.from_env(_prod_env(tmp_path / "b"))  # same default id "file-1"
    with pytest.raises(KeyConfigurationError, match="two custodians"):
        RotatingKeyProvider(a, [b])
