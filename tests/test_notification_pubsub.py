"""End to end: Bed Allocation's real Pub/Sub publisher -> Pub/Sub emulator -> the Notification service.

The only step not exercised is Eventarc's HTTP push itself: the test pulls each message and posts it to the
service in exactly the envelope Pub/Sub push uses. Runs against local emulators only, never cloud.
"""
import base64
import json
import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from services.bed_allocation.outbox.publishers import PubSubPublisher
from services.notification.main import build_app
from services.notification.store import FirestoreNotificationStore
from tests.test_notification import make_event

pytestmark = pytest.mark.skipif(
    not (os.environ.get("PUBSUB_EMULATOR_HOST") and os.environ.get("FIRESTORE_EMULATOR_HOST")),
    reason="requires the Pub/Sub and Firestore emulators; cloud services are never used by this test",
)


@pytest.fixture
def pubsub():
    """A fresh emulator-only topic and subscription per test."""
    from google.cloud import pubsub_v1

    project = f"notification-e2e-{uuid.uuid4().hex[:12]}"
    topic_name = "bed-notifications"
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic = publisher.create_topic(name=publisher.topic_path(project, topic_name)).name
    subscription = subscriber.create_subscription(
        name=subscriber.subscription_path(project, "notification-test"), topic=topic
    ).name
    yield project, topic_name, subscription, subscriber
    subscriber.delete_subscription(subscription=subscription)
    publisher.delete_topic(topic=topic)


def publish(project, topic, event):
    """Publish the way Bed Allocation's outbox does: JSON payload plus the three contract attributes."""
    PubSubPublisher(project).publish(topic, json.dumps(event), {
        "event_id": event["event_id"],
        "pool_id": event["pool_id"],
        "stream_sequence": event["stream_sequence"],
    })


def pull(subscriber, subscription, expected, timeout=30):
    """Pull and acknowledge until `expected` messages have arrived."""
    received = []
    deadline = time.monotonic() + timeout
    while len(received) < expected and time.monotonic() < deadline:
        response = subscriber.pull(subscription=subscription, max_messages=10, timeout=5)
        if response.received_messages:
            subscriber.acknowledge(subscription=subscription,
                                   ack_ids=[m.ack_id for m in response.received_messages])
            received.extend(m.message for m in response.received_messages)
    assert len(received) == expected, f"only {len(received)} of {expected} messages arrived"
    return sorted(received, key=lambda message: int(message.attributes["stream_sequence"]))


def as_push(message, subscription):
    """Wrap a pulled message in the envelope Pub/Sub push (and so Eventarc) sends."""
    return {
        "message": {
            "data": base64.b64encode(message.data).decode(),
            "attributes": dict(message.attributes),
            "messageId": message.message_id,
        },
        "subscription": subscription,
    }


def test_published_events_arrive_once_each_in_order_of_sequence(pubsub):
    project, topic, subscription, subscriber = pubsub
    events = [
        make_event(stream_sequence=1),
        make_event(stream_sequence=2, event_type="ALLOCATION_WAITLISTED", bed_id=None, ward=None),
        make_event(stream_sequence=3, event_type="BED_RELEASED"),
    ]
    for event in events:
        publish(project, topic, event)
    service = TestClient(build_app(FirestoreNotificationStore(project)))

    messages = pull(subscriber, subscription, expected=3)
    results = [service.post("/", json=as_push(message, subscription)).json() for message in messages]
    redelivery = service.post("/", json=as_push(messages[0], subscription)).json()  # At-least-once delivery.

    assert [r["status"] for r in results] == ["delivered"] * 3
    assert [r["event_id"] for r in results] == [e["event_id"] for e in events]
    assert all(r["missing_before"] == 0 for r in results)
    assert "waitlisted" in results[1]["message"]
    assert redelivery["status"] == "duplicate"


def test_attributes_match_the_payload(pubsub):
    project, topic, subscription, subscriber = pubsub
    event = make_event(stream_sequence=7, pool_id="WARDB-GEN")
    publish(project, topic, event)

    (message,) = pull(subscriber, subscription, expected=1)

    assert json.loads(message.data) == event
    assert message.attributes["event_id"] == event["event_id"]
    assert message.attributes["pool_id"] == "WARDB-GEN"
    assert message.attributes["stream_sequence"] == "7"