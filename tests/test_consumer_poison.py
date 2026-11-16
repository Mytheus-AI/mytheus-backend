"""Regression tests for the 2026-11-16 consumer outage.

1. A record that was not written by a Schema Registry serializer (a plain-text "probe") made
   confluent_kafka raise SerializationError, which no consumer treated as poison: core-api and
   notification-service crash-looped on it. Every decode helper must turn it into PoisonMessage.
2. notification-service passed the Avro header's correlation_id / event_id (uuid.UUID objects
   for `uuid` logical types) into an outbox envelope whose JSON encoder rejected them.
"""

import json
import uuid
from typing import Any

import pytest
from confluent_kafka.serialization import SerializationError

from libs.shared.events import EventEnvelope
from libs.shared.outbox import _json_default
from libs.shared.poison import UNDECODABLE
from services.core_api import consumer as core_consumer
from services.integration_service import consumer as integration_consumer
from services.notification_service import consumer as notification_consumer


def _raises(exc: BaseException) -> Any:
    def deserializer(raw: bytes | None, ctx: Any) -> Any:
        raise exc

    return deserializer


@pytest.mark.parametrize(
    "module", [core_consumer, integration_consumer, notification_consumer], ids=lambda m: m.__name__
)
def test_a_non_registry_record_is_poison_not_a_crash(module: Any) -> None:
    err = SerializationError(
        "Expecting data framing of length 6 bytes or more but total data size is 5 bytes."
    )
    with pytest.raises(module.PoisonMessage):
        module.decode(_raises(err), b"probe", None)


@pytest.mark.parametrize(
    "module", [core_consumer, integration_consumer, notification_consumer], ids=lambda m: m.__name__
)
def test_a_transient_failure_still_propagates(module: Any) -> None:
    """A Schema Registry outage must be retried, never dead-lettered."""

    class RegistryDown(Exception):
        pass

    with pytest.raises(RegistryDown):
        module.decode(_raises(RegistryDown()), b"\x00\x00\x00\x00\x01x", None)


def test_serialization_error_is_in_the_shared_poison_list() -> None:
    assert SerializationError in UNDECODABLE


def test_outbox_payload_accepts_uuid_header_ids() -> None:
    corr, cause = uuid.uuid4(), uuid.uuid4()
    env = EventEnvelope(
        topic="notification-delivery-events",
        org_id=str(uuid.uuid4()),
        producer="notification-service",
        idempotency_key="k",
        correlation_id=corr,  # type: ignore[arg-type]  # what the Avro reader hands over
        causation_id=cause,  # type: ignore[arg-type]
        payload={"channel": "in_app"},
    )
    text = json.dumps(env.to_value(), default=_json_default)
    assert str(corr) in text and str(cause) in text


def test_outbox_encoder_still_rejects_other_objects() -> None:
    with pytest.raises(TypeError):
        json.dumps({"x": object()}, default=_json_default)


def test_notification_header_ids_leave_as_text() -> None:
    raw = uuid.uuid4()
    assert notification_consumer._id_str(raw) == str(raw)
    assert notification_consumer._id_str(None) is None
