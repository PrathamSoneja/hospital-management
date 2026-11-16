"""Tests for the Notification Cloud Run function, against the Firestore emulator."""
import os
import uuid

import pytest
from cloudevents.http import CloudEvent

from services.notification import function
from services.notification.store import FirestoreNotificationStore
from tests.test_notification import make_event, push

pytestmark = pytest.mark.skipif(
    not os.environ.get("FIRESTORE_EMULATOR_HOST"),
    reason="requires the Firestore emulator; cloud Firestore is never used by this test",
)


@pytest.fixture
def store():
    return FirestoreNotificationStore(project=f"notification-fn-test-{uuid.uuid4().hex}")


def as_cloud_event(event):
    """Wrap an event the way Eventarc delivers a Pub/Sub message to a function."""
    return CloudEvent(
        {"type": "google.cloud.pubsub.topic.v1.messagePublished",
         "source": "//pubsub.googleapis.com/projects/test/topics/bed-notifications"},
        push(event),
    )


def test_a_valid_event_is_delivered_once(store):
    event = make_event()

    assert function.process(push(event), store) == "delivered"
    assert function.process(push(event), store) == "duplicate"  # At-least-once redelivery.


def test_null_ward_and_stream_sequence_are_accepted(store):
    event = make_event(event_type="ALLOCATION_WAITLISTED", bed_id=None, ward=None, stream_sequence=3)

    assert function.process(push(event), store) == "delivered"


def test_a_malformed_message_is_acknowledged_not_retried(store):
    event = make_event()
    del event["stream_sequence"]

    assert function.process(push(event), store) == "rejected"  # Returns normally, so Pub/Sub acks it.
    assert function.process({"hello": "world"}, store) == "rejected"


def test_firestore_trouble_raises_so_pubsub_retries():
    class BrokenStore:
        def record(self, event, text):
            raise RuntimeError("Firestore is down")

    with pytest.raises(RuntimeError):
        function.process(push(make_event()), BrokenStore())


def test_the_entry_point_handles_an_eventarc_cloud_event(store, monkeypatch):
    monkeypatch.setattr(function, "get_store", lambda: store)
    event = make_event()

    function.handle_bed_notification(as_cloud_event(event))
    function.handle_bed_notification(as_cloud_event(event))

    # The second call was a duplicate, so only one notification document exists.
    saved = list(store.client.collection("notifications").stream())
    assert [doc.id for doc in saved] == [event["event_id"]]