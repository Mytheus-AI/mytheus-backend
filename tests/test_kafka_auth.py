"""kafka_conf / registry_conf: SASL only when a principal is configured, and never half-set."""

import pytest

from libs.shared.kafka_auth import kafka_conf, registry_conf

BASE = {"bootstrap.servers": "redpanda:29092", "acks": "all"}
ENV = (
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SASL_MECHANISM",
    "KAFKA_SECURITY_PROTOCOL",
    "SCHEMA_REGISTRY_USERNAME",
    "SCHEMA_REGISTRY_PASSWORD",
)


@pytest.fixture(autouse=True)
def _clean(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


def test_no_principal_leaves_the_config_unchanged() -> None:
    assert kafka_conf(dict(BASE)) == BASE
    assert registry_conf({"url": "http://redpanda:8081"}) == {"url": "http://redpanda:8081"}


def test_principal_adds_scram_and_registry_basic_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pw")
    conf = kafka_conf(dict(BASE))
    assert conf["security.protocol"] == "SASL_PLAINTEXT"
    assert conf["sasl.mechanism"] == "SCRAM-SHA-256"
    assert (conf["sasl.username"], conf["sasl.password"]) == ("svc", "pw")
    assert conf["bootstrap.servers"] == "redpanda:29092"
    assert registry_conf({"url": "u"})["basic.auth.user.info"] == "svc:pw"


def test_registry_principal_overrides_the_kafka_one(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "svc")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "pw")
    monkeypatch.setenv("SCHEMA_REGISTRY_USERNAME", "sr")
    monkeypatch.setenv("SCHEMA_REGISTRY_PASSWORD", "srpw")
    assert registry_conf({"url": "u"})["basic.auth.user.info"] == "sr:srpw"


@pytest.mark.parametrize("present", ["KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD"])
def test_half_set_secret_fails_closed(monkeypatch: pytest.MonkeyPatch, present: str) -> None:
    monkeypatch.setenv(present, "x")
    with pytest.raises(RuntimeError):
        kafka_conf(dict(BASE))
