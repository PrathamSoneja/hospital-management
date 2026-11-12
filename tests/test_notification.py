"""Tests for the Notification service's Eventarc/Pub/Sub handler. No cloud or emulator needed."""
import base64
import json
import uuid

from fastapi.testclient import TestClient

from services.notification.main import build_app


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


def post(event_or_body, wrap=True):
    client = TestClient(build_app())
    return client.post("/", json=push(event_or_body) if wrap else event_or_body)


def test_allocated_bed_is_delivered_with_its_ward():
    response = post(make_event())

    assert response.status_code == 200
    assert response.json()["status"] == "delivered"
    assert response.json()["message"] == "[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A allocated to admission adm-1."


def test_waitlisted_event_with_null_ward_is_accepted():
    response = post(make_event(event_type="ALLOCATION_WAITLISTED", bed_id=None, ward=None))

    assert response.json()["status"] == "delivered"
    assert response.json()["message"] == "[WARDA-ICU] Admission adm-1 waitlisted: no bed free in this pool."


def test_released_and_conflict_events_are_delivered():
    released = post(make_event(event_type="BED_RELEASED")).json()
    conflict = post(make_event(event_type="ALLOCATION_CONFLICT", bed_id=None, ward=None)).json()

    assert released["message"] == "[WARDA-ICU] Bed bed-warda-icu-01 in ward ICU-A released by admission adm-1."
    assert "alloc-1" in conflict["message"] and conflict["status"] == "delivered"


def test_event_missing_stream_sequence_is_rejected_but_acknowledged():
    event = make_event()
    del event["stream_sequence"]

    response = post(event)

    assert response.status_code == 200  # Acknowledged, so Pub/Sub does not retry it forever.
    assert response.json()["status"] == "rejected"
    assert "stream_sequence" in response.json()["reason"]


def test_unknown_event_type_is_rejected():
    response = post(make_event(event_type="BED_TELEPORTED"))

    assert response.json()["status"] == "rejected"


def test_a_body_that_is_not_a_push_message_is_rejected():
    assert post({"hello": "world"}, wrap=False).json()["status"] == "rejected"
    assert post({"message": {"data": "%%%not-base64%%%"}}, wrap=False).json()["status"] == "rejected"


def test_health_endpoints_respond():
    client = TestClient(build_app())

    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200