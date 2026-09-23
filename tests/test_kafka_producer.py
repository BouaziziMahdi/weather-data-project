import json
from unittest.mock import Mock

import pytest

from src.messaging import kafka_producer


@pytest.fixture
def producer(monkeypatch):
    instance = Mock()
    instance.flush.return_value = 0
    factory = Mock(return_value=instance)
    monkeypatch.setattr(kafka_producer, "Producer", factory)
    return factory, instance


def test_publish_serializes_weather(producer):
    factory, instance = producer
    payload = {"city": "Tunis", "current": {"temperature_2m": 0}}
    kafka_producer.publish_weather(payload)
    factory.assert_called_once_with({
        "bootstrap.servers": kafka_producer.KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all", "enable.idempotence": True,
    })
    instance.produce.assert_called_once()
    kwargs = instance.produce.call_args.kwargs
    assert kwargs["topic"] == "weather"
    assert json.loads(kwargs["value"].decode("utf-8")) == payload
    instance.flush.assert_called_once_with(10)


def test_flush_timeout_is_failure(producer):
    _, instance = producer
    instance.flush.return_value = 2
    with pytest.raises(RuntimeError, match="2 message"):
        kafka_producer.publish_weather({})


def test_full_queue_propagates(producer):
    _, instance = producer
    instance.produce.side_effect = BufferError("queue full")
    with pytest.raises(BufferError):
        kafka_producer.publish_weather({})


def test_unserializable_payload_is_not_sent(producer):
    _, instance = producer
    with pytest.raises(TypeError):
        kafka_producer.publish_weather({"invalid": object()})
    instance.produce.assert_not_called()
