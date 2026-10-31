"""
Broker adapter contract tests using client doubles.

These verify ACK waiting, deadline handling, keying, and configuration only.
They are not evidence of a working Pub/Sub topic or Kafka cluster.
"""
from concurrent.futures import TimeoutError as FutureTimeout

import pytest

from services.bed_allocation.outbox.publishers import (
    BrokerEventPublisher, BrokerPublishError, KafkaPublisher, PubSubPublisher, PublisherConfigError,
    build_event_publisher_from_env, kafka_config_from_env,
)


class FakeFuture:
    def __init__(self, result=None, error=None):
        self._result, self._error = result, error
        self.timeout = None

    def result(self, timeout=None):
        self.timeout = timeout
        if self._error:
            raise self._error
        return self._result


class FakePubSubClient:
    def __init__(self, future):
        self.future = future
        self.calls = []

    def topic_path(self, project, topic):
        return f"projects/{project}/topics/{topic}"

    def publish(self, path, data, **attributes):
        self.calls.append((path, data, attributes))
        return self.future


class FakeKafkaError:
    def name(self):
        return "MSG_SIZE_TOO_LARGE"


class FakeProducer:
    def __init__(self, deliver=True, error=None):
        self.deliver, self.error = deliver, error
        self.produced = []

    def produce(self, topic, key, value, on_delivery):
        self.produced.append((topic, key, value))
        self._callback = on_delivery

    def flush(self, timeout):
        if self.deliver:
            self._callback(self.error, None)
            return 0
        return 1


def test_pubsub_waits_for_server_ack_with_deadline():
    future = FakeFuture(result="msg-1")
    client = FakePubSubClient(future)
    publisher = PubSubPublisher("proj", timeout_seconds=7, client=client)
    assert publisher.publish("notif", '{"a":1}', {"event_id": "e1", "stream_sequence": 3}) is True
    assert future.timeout == 7
    path, data, attributes = client.calls[0]
    assert path == "projects/proj/topics/notif" and data == b'{"a":1}'
    assert attributes == {"event_id": "e1", "stream_sequence": "3"}


def test_pubsub_deadline_raises_for_retry():
    publisher = PubSubPublisher("proj", client=FakePubSubClient(FakeFuture(error=FutureTimeout())))
    with pytest.raises(FutureTimeout):
        publisher.publish("notif", "{}", {})


def test_kafka_keys_by_pool_and_requires_delivery_report():
    producer = FakeProducer()
    publisher = KafkaPublisher({"bootstrap.servers": "kafka:9092"}, producer=producer)
    assert publisher.publish("bed-events", "POOL-A", '{"x":1}') is True
    assert producer.produced == [("bed-events", b"POOL-A", b'{"x":1}')]

    with pytest.raises(TimeoutError):
        KafkaPublisher({"bootstrap.servers": "k"}, producer=FakeProducer(deliver=False)).publish("t", "P", "{}")
    with pytest.raises(BrokerPublishError, match="MSG_SIZE_TOO_LARGE"):
        KafkaPublisher({"bootstrap.servers": "k"}, producer=FakeProducer(error=FakeKafkaError())).publish("t", "P", "{}")


def test_broker_publisher_maps_logical_topics():
    client = FakePubSubClient(FakeFuture(result="m"))
    producer = FakeProducer()
    publisher = BrokerEventPublisher(
        PubSubPublisher("proj", client=client), KafkaPublisher({"bootstrap.servers": "k"}, producer=producer),
        {"bed-notifications": "real-notifications", "bed-events": "real-bed-events"},
    )
    publisher.publish_pubsub("bed-notifications", "{}", {})
    publisher.publish_kafka("bed-events", "POOL-A", "{}")
    assert client.calls[0][0].endswith("/real-notifications")
    assert producer.produced[0][0] == "real-bed-events"


BASE_ENV = {
    "ENVIRONMENT": "production", "PUBSUB_PROJECT_ID": "proj", "PUBSUB_TOPIC_NOTIFICATIONS": "n",
    "KAFKA_TOPIC_BED_EVENTS": "b", "KAFKA_BOOTSTRAP_SERVERS": "kafka:9093",
    "KAFKA_SECURITY_PROTOCOL": "SASL_SSL", "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
    "KAFKA_SASL_USERNAME": "user", "KAFKA_SASL_PASSWORD": "placeholder",
}


@pytest.mark.parametrize("missing", ["PUBSUB_PROJECT_ID", "PUBSUB_TOPIC_NOTIFICATIONS", "KAFKA_TOPIC_BED_EVENTS",
                                     "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SASL_PASSWORD"])
def test_missing_broker_settings_fail_before_any_client_is_built(missing):
    env = {k: v for k, v in BASE_ENV.items() if k != missing}
    with pytest.raises(PublisherConfigError, match=missing):
        build_event_publisher_from_env(env)


def test_emulator_rejected_outside_test_profiles():
    with pytest.raises(PublisherConfigError, match="PUBSUB_EMULATOR_HOST"):
        build_event_publisher_from_env({**BASE_ENV, "PUBSUB_EMULATOR_HOST": "localhost:8085"})


def test_kafka_security_config_comes_from_environment():
    config = kafka_config_from_env(BASE_ENV)
    assert config["security.protocol"] == "SASL_SSL" and config["sasl.mechanism"] == "SCRAM-SHA-512"
    plaintext = kafka_config_from_env({"KAFKA_BOOTSTRAP_SERVERS": "k:9092", "KAFKA_SECURITY_PROTOCOL": "PLAINTEXT"})
    assert "sasl.username" not in plaintext
