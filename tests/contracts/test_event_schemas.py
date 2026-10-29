"""
Schema validation tests for asynchronous event contracts.
Validates Pub/Sub notifications, Kafka bed-events, and Flink bed-stats
against contracts/events/*.schema.json.
"""
import json
import pytest
from pathlib import Path
from jsonschema import validate, ValidationError

CONTRACTS_DIR = Path(__file__).parent.parent.parent / "contracts" / "events"

@pytest.fixture
def notification_schema():
    with open(CONTRACTS_DIR / "notification.schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def bed_event_schema():
    with open(CONTRACTS_DIR / "bed-event.schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

@pytest.fixture
def bed_stats_schema():
    with open(CONTRACTS_DIR / "bed-stats.schema.json", "r", encoding="utf-8") as f:
        return json.load(f)

def test_valid_notification_events(notification_schema):
    valid_allocated = {
        "schema_version": "1.0.0",
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "event_type": "BED_ALLOCATED",
        "occurred_at": "2026-10-28T12:00:00Z",
        "request_id": "req-001",
        "allocation_id": "alloc-001",
        "admission_id": "adm-001",
        "pool_id": "WARDA-ICU",
        "bed_id": "bed-101",
        "ward": "ICU-A"
    }
    validate(instance=valid_allocated, schema=notification_schema)

    valid_waitlisted = {
        "schema_version": "1.0.0",
        "event_id": "550e8400-e29b-41d4-a716-446655440001",
        "event_type": "ALLOCATION_WAITLISTED",
        "occurred_at": "2026-10-28T12:05:00Z",
        "request_id": "req-002",
        "allocation_id": "alloc-002",
        "admission_id": "adm-002",
        "pool_id": "WARDA-ICU",
        "bed_id": None,
        "ward": "ICU-A"
    }
    validate(instance=valid_waitlisted, schema=notification_schema)

def test_invalid_notification_event_rejected(notification_schema):
    # Invalid event_type
    invalid_event = {
        "schema_version": "1.0.0",
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "event_type": "UNKNOWN_ACTION",
        "occurred_at": "2026-10-28T12:00:00Z",
        "request_id": "req-001",
        "allocation_id": "alloc-001",
        "admission_id": "adm-001",
        "pool_id": "WARDA-ICU",
        "ward": "ICU-A"
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_event, schema=notification_schema)

def test_valid_bed_event_stream(bed_event_schema):
    valid_bed_event = {
        "schema_version": "1.0.0",
        "event_id": "evt-001",
        "event_type": "BED_ALLOCATED",
        "event_time": "2026-10-28T12:00:00Z",
        "request_id": "req-001",
        "allocation_id": "alloc-001",
        "admission_id": "adm-001",
        "patient_id": "pat-001",
        "pool_id": "WARDA-ICU",
        "bed_id": "bed-101",
        "lock_wait_ms": 12,
        "lock_contended": True,
        "allocation_wait_ms": 45,
        "occupancy_delta": 1
    }
    validate(instance=valid_bed_event, schema=bed_event_schema)

def test_valid_bed_stats_analytics_event(bed_stats_schema):
    valid_stats = {
        "schema_version": "1.0.0",
        "pool_id": "WARDA-ICU",
        "window_start": "2026-10-28T12:00:00Z",
        "window_end": "2026-10-28T12:01:00Z",
        "requests": 50,
        "allocated": 10,
        "waitlisted": 40,
        "conflicts": 0,
        "occupancy_at_window_end": 10,
        "average_lock_wait_ms": 15.4,
        "average_allocation_wait_ms": 82.1,
        "contention_rate": 0.80
    }
    validate(instance=valid_stats, schema=bed_stats_schema)
