"""
Broker adapters for the outbox dispatcher.

Each publish blocks until the broker acknowledges the message or the configured
deadline passes. A deadline or broker error raises, so the row is retried; the
broker may still have accepted it, which is the documented at-least-once duplicate.
Client libraries are imported only when a real adapter is constructed; there is
no fallback to an in-memory publisher.
"""
import os
import threading

from services.bed_allocation.outbox.dispatcher import EventPublisher


class PublisherConfigError(RuntimeError):
    pass


class BrokerPublishError(RuntimeError):
    """Broker rejected or failed to confirm a message. Carries no payload content."""


class PubSubPublisher:
    def __init__(self, project_id: str, timeout_seconds: float = 10.0, client=None):
        if not project_id:
            raise PublisherConfigError("PUBSUB_PROJECT_ID must be configured")
        if client is None:
            try:
                from google.cloud import pubsub_v1
            except ImportError as error:
                raise PublisherConfigError("google-cloud-pubsub is not installed") from error
            client = pubsub_v1.PublisherClient()
        self.client = client
        self.project_id = project_id
        self.timeout_seconds = timeout_seconds

    def publish(self, topic: str, payload_json: str, attributes: dict) -> bool:
        topic_path = self.client.topic_path(self.project_id, topic)
        future = self.client.publish(
            topic_path, payload_json.encode("utf-8"), **{key: str(value) for key, value in attributes.items()}
        )
        # result() returns the server-assigned message ID only after the publish is acknowledged.
        message_id = future.result(timeout=self.timeout_seconds)
        if not message_id:
            raise BrokerPublishError("Pub/Sub returned no message ID")
        return True


class KafkaPublisher:
    def __init__(self, config: dict, timeout_seconds: float = 10.0, producer=None):
        if not config.get("bootstrap.servers"):
            raise PublisherConfigError("KAFKA_BOOTSTRAP_SERVERS must be configured")
        if producer is None:
            try:
                from confluent_kafka import Producer
            except ImportError as error:
                raise PublisherConfigError("confluent-kafka is not installed") from error
            producer = Producer({
                # Wait for all in-sync replicas; idempotence prevents producer-retry duplicates
                # within one session (not across outbox redeliveries).
                "acks": "all",
                "enable.idempotence": True,
                # librdkafka requires delivery.timeout >= linger + request.timeout.
                "linger.ms": 5,
                "request.timeout.ms": max(1000, int(timeout_seconds * 500)),
                "delivery.timeout.ms": int(timeout_seconds * 1000),
                **config,
            })
        self.producer = producer
        self.timeout_seconds = timeout_seconds
        self._lock = threading.Lock()

    def publish(self, topic: str, key: str, payload_json: str) -> bool:
        outcome = {}
        done = threading.Event()

        def on_delivery(error, _message):
            outcome["error"] = error
            done.set()

        with self._lock:
            self.producer.produce(topic, key=key.encode("utf-8"), value=payload_json.encode("utf-8"),
                                  on_delivery=on_delivery)
            self.producer.flush(self.timeout_seconds)
        if not done.is_set():
            raise TimeoutError("Kafka delivery was not confirmed before the deadline")
        if outcome["error"] is not None:
            code = getattr(outcome["error"], "name", lambda: "UNKNOWN")()
            raise BrokerPublishError(f"Kafka delivery failed: {code}")
        return True


class BrokerEventPublisher(EventPublisher):
    """Maps logical outbox topics to configured broker topics."""

    def __init__(self, pubsub: PubSubPublisher, kafka: KafkaPublisher, topic_map: dict | None = None):
        self.pubsub = pubsub
        self.kafka = kafka
        self.topic_map = topic_map or {}

    def publish_pubsub(self, topic: str, payload_json: str, attributes: dict) -> bool:
        return self.pubsub.publish(self.topic_map.get(topic, topic), payload_json, attributes)

    def publish_kafka(self, topic: str, key: str, payload_json: str) -> bool:
        return self.kafka.publish(self.topic_map.get(topic, topic), key, payload_json)


def _required(env, name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise PublisherConfigError(f"{name} must be configured for the outbox worker")
    return value


def kafka_config_from_env(env=None) -> dict:
    env = os.environ if env is None else env
    config = {"bootstrap.servers": _required(env, "KAFKA_BOOTSTRAP_SERVERS"),
              "security.protocol": env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL").strip()}
    if config["security.protocol"].startswith("SASL"):
        config["sasl.mechanism"] = _required(env, "KAFKA_SASL_MECHANISM")
        config["sasl.username"] = _required(env, "KAFKA_SASL_USERNAME")
        config["sasl.password"] = _required(env, "KAFKA_SASL_PASSWORD")
    if env.get("KAFKA_SSL_CA_LOCATION", "").strip():
        config["ssl.ca.location"] = env["KAFKA_SSL_CA_LOCATION"].strip()
    return config


def build_event_publisher_from_env(env=None) -> BrokerEventPublisher:
    env = os.environ if env is None else env
    environment = env.get("ENVIRONMENT", "production").strip().lower()
    if env.get("PUBSUB_EMULATOR_HOST", "").strip() and environment not in {"test", "local"}:
        raise PublisherConfigError("PUBSUB_EMULATOR_HOST is not permitted outside ENVIRONMENT=test/local")
    timeout = float(env.get("OUTBOX_PUBLISH_TIMEOUT_SECONDS", "10"))
    if not 1 <= timeout <= 60:
        raise PublisherConfigError("OUTBOX_PUBLISH_TIMEOUT_SECONDS must be between 1 and 60")
    # Validate all settings before constructing any client.
    project_id = _required(env, "PUBSUB_PROJECT_ID")
    kafka_config = kafka_config_from_env(env)
    topic_map = {
        "bed-notifications": _required(env, "PUBSUB_TOPIC_NOTIFICATIONS"),
        "bed-events": _required(env, "KAFKA_TOPIC_BED_EVENTS"),
    }
    return BrokerEventPublisher(PubSubPublisher(project_id, timeout), KafkaPublisher(kafka_config, timeout), topic_map)
