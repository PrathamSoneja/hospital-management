"""In-memory publisher used by tracked tests only."""

import json


class FakeEventPublisher:
    def __init__(self):
        self.pubsub_messages: list[dict] = []
        self.kafka_messages: list[dict] = []
        self.simulate_failure = False

    def publish_pubsub(self, topic: str, payload_json: str, attributes: dict) -> bool:
        if self.simulate_failure:
            return False
        self.pubsub_messages.append({"topic": topic, "payload": json.loads(payload_json), "attributes": attributes})
        return True

    def publish_kafka(self, topic: str, key: str, payload_json: str) -> bool:
        if self.simulate_failure:
            return False
        self.kafka_messages.append({"topic": topic, "key": key, "payload": json.loads(payload_json)})
        return True
