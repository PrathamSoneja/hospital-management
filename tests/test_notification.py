"""Tests for the Notification service's Eventarc/Pub/Sub handler, against the Firestore emulator."""
import base64
import json
import os
import uuid

import pytest
from fastapi.testclient import TestClient

from services.notification.main import build_app
from services.notification.store import FirestoreNotificationStore

pytestmark = pytest.mark.skipif(
    not os.environ.get("FIRESTORE_EMULATOR_HOST"),
    reason="requires the Firestore emulator; cloud Firestore is never used by this test",
)


@pytest.fixture
def store():
    # A fresh emulator-only project per test, so tests never see each other's data.
    return FirestoreNotificationStore(project=f"notification-test-{uuid.uuid4().hex}")


@pytest.fixture
def client(store):
    return TestClient(build_app(store))


def make_event(**changes):
    event = {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "stream_sequence": 1,
        "event_type": "BED_ALLOCATED",
        "occurred_at": "2026-11-12T10:00:00+00:00",
        "request_id": "req-1",
        "allocation_id": "alloc-1",
        "admission_id": "adm-1",
        "pool_id": "WARDA-ICU",
        "bed_id": "bed-warda-icu-01",
        "ward": "ICU-A",
    }
    event.update(changes)
    return event


def push(event):
    """Wrap an event the way Pub/Sub push (and so Eventarc) delivers it."""
    data = base64.b64encode(json.dumps(event).encode()).decode()
    return {
        "message": {
            "data": data,
            "attributes": {"event_id": event.get("event_id", ""), "pool_id": event.get("pool_id", "")},
            "messageId": "1",
            "publishTime": "2026-11-12T10:00:01Z",
        },
        "subscription": "projects/test/subscriptions/eventarc-test",
    }


def test_allocated_bed_is_delivered_with_its_ward(client):
    response = client.post("/", json=push(make_event()))

    assert response.status_code == 200
    assert response.json()["status"] == "delivered"
    assert response.json()["message"] == "[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A allocated to admission adm-1."


def test_waitlisted_event_with_null_ward_is_accepted(client):
    event = make_event(event_type="ALLOCATION_WAITLISTED", bed_id=None, ward=None)

    response = client.post("/", json=push(event))

    assert response.json()["status"] == "delivered"
    assert response.json()["message"] == "[WARDA-ICU] Admission adm-1 waitlisted: no bed free in this pool."


def test_released_and_conflict_events_are_delivered(client):
    released = client.post("/", json=push(make_event(event_type="BED_RELEASED", stream_sequence=1))).json()
    conflict = client.post("/", json=push(make_event(event_type="ALLOCATION_CONFLICT", bed_id=None, ward=None,
                                                     stream_sequence=2))).json()

    assert released["message"] == "[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A released by admission adm-1."
    assert "alloc-1" in conflict["message"] and conflict["status"] == "delivered"


def test_event_missing_stream_sequence_is_rejected_but_acknowledged(client):
    event = make_event()
    del event["stream_sequence"]

    response = client.post("/", json=push(event))

    assert response.status_code == 200  # Acknowledged, so Pub/Sub does not retry it forever.
    assert response.json()["status"] == "rejected"
    assert "stream_sequence" in response.json()["reason"]


def test_unknown_event_type_is_rejected(client):
    assert client.post("/", json=push(make_event(event_type="BED_TELEPORTED"))).json()["status"] == "rejected"


def test_a_body_that_is_not_a_push_message_is_rejected(client):
    assert client.post("/", json={"hello": "world"}).json()["status"] == "rejected"
    assert client.post("/", json={"message": {"data": "%%%not-base64%%%"}}).json()["status"] == "rejected"


def test_the_same_event_delivered_twice_is_sent_once(client):
    event = make_event()

    first = client.post("/", json=push(event)).json()
    again = client.post("/", json=push(event)).json()

    assert first["status"] == "delivered"
    assert again["status"] == "duplicate"


def test_a_duplicate_is_caught_by_a_second_copy_of_the_service(store):
    # Cloud Run can run several copies; they share Firestore, so a redelivery to another copy is still caught.
    event = make_event()
    first_copy = TestClient(build_app(store))
    second_copy = TestClient(build_app(FirestoreNotificationStore(project=store.client.project)))

    assert first_copy.post("/", json=push(event)).json()["status"] == "delivered"
    assert second_copy.post("/", json=push(event)).json()["status"] == "duplicate"


def test_a_gap_in_stream_sequence_is_reported(client):
    client.post("/", json=push(make_event(stream_sequence=1)))

    jumped = client.post("/", json=push(make_event(stream_sequence=4))).json()

    assert jumped["status"] == "delivered"
    assert jumped["missing_before"] == 2  # Sequences 2 and 3 never arrived.


def test_a_late_lower_sequence_is_still_delivered(client):
    client.post("/", json=push(make_event(stream_sequence=5)))

    late = client.post("/", json=push(make_event(stream_sequence=3))).json()

    assert late["status"] == "delivered"  # The contract says never drop lower sequences.
    assert late["late"] is True


def test_sequences_are_tracked_per_pool(client):
    client.post("/", json=push(make_event(pool_id="WARDA-ICU", stream_sequence=7)))

    other_pool = client.post("/", json=push(make_event(pool_id="WARDB-GEN", stream_sequence=1))).json()

    assert other_pool["missing_before"] == 0 and other_pool["late"] is False


def test_firestore_trouble_asks_pubsub_to_retry():
    class BrokenStore:
        def record(self, event, text):
            raise RuntimeError("Firestore is down")

        def check(self):
            return False

    client = TestClient(build_app(BrokenStore()))

    assert client.post("/", json=push(make_event())).status_code == 503
    assert client.get("/readyz").status_code == 503


def test_health_endpoints_respond(client):
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200