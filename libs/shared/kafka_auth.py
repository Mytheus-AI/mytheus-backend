"""Broker and Schema Registry credentials for every Kafka client (Redpanda SASL/SCRAM).

Every `confluent_kafka` Producer / Consumer / AdminClient config goes through `kafka_conf`, and
every `SchemaRegistryClient` config through `registry_conf`. With no credential in the
environment both return their input unchanged, so local compose, CI and tests keep connecting
to a PLAINTEXT broker exactly as before.

Environment:
    KAFKA_SASL_USERNAME / KAFKA_SASL_PASSWORD   this service's SCRAM principal on Redpanda
    KAFKA_SASL_MECHANISM                        default SCRAM-SHA-256
    KAFKA_SECURITY_PROTOCOL                     default SASL_PLAINTEXT (SASL_SSL with TLS)
    SCHEMA_REGISTRY_USERNAME / _PASSWORD        HTTP basic auth on the registry; default to the
                                                Kafka principal (Redpanda checks the same users)

A username without a password (or the reverse) is a configuration error and raises, so a
half-set secret fails at startup instead of falling back to an unauthenticated connection.
"""

from __future__ import annotations

import os
from typing import Any


def _pair(user_var: str, pass_var: str) -> tuple[str, str] | None:
    user = os.environ.get(user_var, "").strip()
    password = os.environ.get(pass_var, "")
    if not user and not password:
        return None
    if not user or not password:
        raise RuntimeError(f"{user_var} and {pass_var} must be set together")
    return user, password


def kafka_credentials() -> tuple[str, str] | None:
    return _pair("KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD")


def kafka_conf(conf: dict[str, Any]) -> dict[str, Any]:
    """`conf` plus the SASL settings, when a principal is configured."""
    creds = kafka_credentials()
    if creds is None:
        return conf
    return {
        **conf,
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "sasl.mechanism": os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": creds[0],
        "sasl.password": creds[1],
    }


def registry_conf(conf: dict[str, Any]) -> dict[str, Any]:
    """`conf` plus HTTP basic auth for the Schema Registry, when a principal is configured."""
    creds = _pair("SCHEMA_REGISTRY_USERNAME", "SCHEMA_REGISTRY_PASSWORD") or kafka_credentials()
    if creds is None:
        return conf
    return {**conf, "basic.auth.user.info": f"{creds[0]}:{creds[1]}"}
