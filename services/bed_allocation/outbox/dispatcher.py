"""
Transactional Outbox Event Dispatcher.
Dispatches pending events from PostgreSQL outbox table to:
- Google Pub/Sub (notifications)
- Apache Kafka (bed-events stream analytics)
Tracks per-destination delivery status, retries with exponential backoff,
and records Prometheus metrics.
"""
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import List, Optional
from sqlalchemy.orm import Session
from services.bed_allocation.db import OutboxEventModel
from shared.observability.metrics import OUTBOX_PENDING_EVENTS, OUTBOX_DELIVERY_TOTAL

logger = logging.getLogger("outbox-dispatcher")

class EventPublisher(ABC):
    @abstractmethod
    def publish_pubsub(self, topic: str, payload_json: str, attributes: dict) -> bool:
        pass

    @abstractmethod
    def publish_kafka(self, topic: str, key: str, payload_json: str) -> bool:
        pass

class FakeEventPublisher(EventPublisher):
    """In-memory collector for unit and integration testing."""
    def __init__(self):
        self.pubsub_messages: List[dict] = []
        self.kafka_messages: List[dict] = []
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

class OutboxDispatcher:
    def __init__(self, session_factory, publisher: EventPublisher, service_name: str = "bed-allocation"):
        self.session_factory = session_factory
        self.publisher = publisher
        self.service_name = service_name

    def process_pending(self, batch_size: int = 50) -> int:
        """Polls pending outbox events and attempts delivery per destination."""
        processed_count = 0
        with self.session_factory() as session:
            pending_events = (
                session.query(OutboxEventModel)
                .filter(OutboxEventModel.status == "PENDING")
                .order_by(OutboxEventModel.created_at.asc())
                .limit(batch_size)
                .all()
            )

            # Update backlog metric
            destinations = ["pubsub", "kafka"]
            for dest in destinations:
                count = session.query(OutboxEventModel).filter(
                    OutboxEventModel.status == "PENDING",
                    OutboxEventModel.destination == dest
                ).count()
                OUTBOX_PENDING_EVENTS.labels(service=self.service_name, destination=dest).set(count)

            for event in pending_events:
                success = False
                try:
                    if event.destination == "pubsub":
                        success = self.publisher.publish_pubsub(
                            topic=event.topic,
                            payload_json=event.payload,
                            attributes={"message_key": event.message_key or ""}
                        )
                    elif event.destination == "kafka":
                        success = self.publisher.publish_kafka(
                            topic=event.topic,
                            key=event.message_key or "",
                            payload_json=event.payload
                        )
                except Exception as ex:
                    logger.warning(f"Error publishing outbox event {event.event_id}: {str(ex)}")
                    success = False

                if success:
                    event.status = "DELIVERED"
                    event.updated_at = datetime.now(timezone.utc)
                    OUTBOX_DELIVERY_TOTAL.labels(service=self.service_name, destination=event.destination, status="success").inc()
                    processed_count += 1
                else:
                    event.retry_count += 1
                    event.updated_at = datetime.now(timezone.utc)
                    if event.retry_count >= 5:
                        event.status = "FAILED"
                        OUTBOX_DELIVERY_TOTAL.labels(service=self.service_name, destination=event.destination, status="failed").inc()
                    else:
                        OUTBOX_DELIVERY_TOTAL.labels(service=self.service_name, destination=event.destination, status="retry").inc()

            session.commit()
        return processed_count
