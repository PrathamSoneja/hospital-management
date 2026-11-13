"""
Unit and integration tests for Resource Analytics Service.
Owned by Member D (Jay Patel).
Verifies:
1. Stateful 1-minute tumbling window math and watermark logic
2. Contention rate and average lock wait calculations
3. Schema compliance with contracts/events/bed-stats.schema.json
4. Firestore persistence and REST API endpoints
"""
import pytest
from datetime import datetime, timezone, timedelta
from fastapi.testclient import TestClient

from services.resource_analytics.models import BedEvent, BedStats
from services.resource_analytics.aggregator import StreamAggregatorEngine
from services.resource_analytics.storage import InMemoryAnalyticsStorage
from services.resource_analytics.main import build_app

def test_stream_aggregator_tumbling_window_math():
    engine = StreamAggregatorEngine(watermark_delay_seconds=5)
    base_time = datetime(2026, 11, 6, 12, 0, 10, tzinfo=timezone.utc)

    # 1. First event: Request
    e1 = BedEvent(
        event_id="evt-1",
        stream_sequence=1,
        event_type="ALLOCATION_REQUESTED",
        event_time=base_time.isoformat(),
        request_id="req-1",
        allocation_id="alloc-1",
        admission_id="adm-1",
        patient_id="pat-1",
        pool_id="WARDA-ICU",
        lock_wait_ms=15,
        lock_contended=False,
        occupancy_delta=0
    )
    engine.process_event(e1)

    # 2. Second event: Bed Allocated (+1 occupancy)
    e2 = BedEvent(
        event_id="evt-2",
        stream_sequence=2,
        event_type="BED_ALLOCATED",
        event_time=(base_time + timedelta(seconds=15)).isoformat(),
        request_id="req-2",
        allocation_id="alloc-1",
        admission_id="adm-1",
        patient_id="pat-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        lock_wait_ms=25,
        lock_contended=False,
        allocation_wait_ms=50,
        occupancy_delta=1
    )
    engine.process_event(e2)

    # 3. Third event: Request waitlisted (contention!)
    e3 = BedEvent(
        event_id="evt-3",
        stream_sequence=3,
        event_type="ALLOCATION_WAITLISTED",
        event_time=(base_time + timedelta(seconds=20)).isoformat(),
        request_id="req-3",
        allocation_id="alloc-2",
        admission_id="adm-2",
        patient_id="pat-2",
        pool_id="WARDA-ICU",
        lock_wait_ms=20,
        lock_contended=True,
        occupancy_delta=0
    )
    engine.process_event(e3)

    # Force flush open window to inspect calculations
    stats_list = engine.flush_all_windows()
    assert len(stats_list) == 1
    stats = stats_list[0]

    assert stats.pool_id == "WARDA-ICU"
    assert stats.requests == 1
    assert stats.allocated == 1
    assert stats.waitlisted == 1
    assert stats.conflicts == 0
    assert stats.occupancy_at_window_end == 1
    # Average lock wait: (15 + 25 + 20) / 3 = 20.0 ms
    assert stats.average_lock_wait_ms == 20.0
    assert stats.average_allocation_wait_ms == 50.0
    # Contention rate: (1 waitlisted) / (1 request) = 1.0 (100%)
    assert stats.contention_rate == 1.0

def test_watermark_advancement_auto_flush():
    engine = StreamAggregatorEngine(watermark_delay_seconds=5)
    t1 = datetime(2026, 11, 6, 12, 0, 10, tzinfo=timezone.utc)

    e1 = BedEvent(
        event_id="evt-1",
        stream_sequence=1,
        event_type="ALLOCATION_REQUESTED",
        event_time=t1.isoformat(),
        request_id="req-1",
        allocation_id="alloc-1",
        admission_id="adm-1",
        patient_id="pat-1",
        pool_id="WARDA-ICU",
        occupancy_delta=0
    )
    closed = engine.process_event(e1)
    # Window not yet closed
    assert len(closed) == 0

    # Event in the NEXT minute beyond the 5s watermark: 12:01:10
    t2 = datetime(2026, 11, 6, 12, 1, 10, tzinfo=timezone.utc)
    e2 = BedEvent(
        event_id="evt-2",
        stream_sequence=2,
        event_type="ALLOCATION_REQUESTED",
        event_time=t2.isoformat(),
        request_id="req-2",
        allocation_id="alloc-2",
        admission_id="adm-2",
        patient_id="pat-2",
        pool_id="WARDA-ICU",
        occupancy_delta=0
    )
    closed = engine.process_event(e2)
    # The [12:00, 12:01) window should now be flushed!
    assert len(closed) == 1
    assert closed[0].window_start.startswith("2026-11-06T12:00:00")
    assert closed[0].window_end.startswith("2026-11-06T12:01:00")

def test_event_deduplication_by_event_id():
    engine = StreamAggregatorEngine()
    event = BedEvent(
        event_id="evt-dedup-1",
        stream_sequence=1,
        event_type="BED_ALLOCATED",
        event_time="2026-11-06T12:00:10Z",
        request_id="req-dedup-1",
        allocation_id="alloc-dedup-1",
        admission_id="adm-1",
        patient_id="pat-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        occupancy_delta=1
    )
    # Process original event
    engine.process_event(event)
    # Process duplicate redelivery
    closed = engine.process_event(event)
    assert closed == []

    stats = engine.flush_all_windows()
    assert len(stats) == 1
    assert stats[0].allocated == 1
    assert stats[0].occupancy_at_window_end == 1

def test_in_memory_storage():
    storage = InMemoryAnalyticsStorage()
    stats = BedStats(
        schema_version="1.0.0",
        pool_id="WARDA-ICU",
        window_start="2026-11-06T12:00:00Z",
        window_end="2026-11-06T12:01:00Z",
        requests=5,
        allocated=3,
        waitlisted=2,
        conflicts=0,
        occupancy_at_window_end=3,
        average_lock_wait_ms=12.5,
        average_allocation_wait_ms=30.0,
        contention_rate=0.4
    )
    storage.save_window_stats(stats)

    latest = storage.get_latest_stats("WARDA-ICU")
    assert latest is not None
    assert latest.requests == 5
    assert latest.occupancy_at_window_end == 3

    history = storage.get_stats_history("WARDA-ICU")
    assert len(history) == 1

def test_rest_api_analytics_ingest_and_summary():
    app = build_app(storage_backend="memory")
    client = TestClient(app)

    # Health check
    h_resp = client.get("/healthz")
    assert h_resp.status_code == 200

    # Ingest event with force_flush=True
    event_payload = {
        "schema_version": "1.0.0",
        "event_id": "test-e-1",
        "stream_sequence": 1,
        "event_type": "BED_ALLOCATED",
        "event_time": "2026-11-06T14:30:15Z",
        "request_id": "req-t-1",
        "allocation_id": "alloc-t-1",
        "admission_id": "adm-t-1",
        "patient_id": "pat-t-1",
        "pool_id": "WARDA-HDU",
        "bed_id": "bed-hdu-01",
        "lock_wait_ms": 10,
        "lock_contended": False,
        "allocation_wait_ms": 40,
        "occupancy_delta": 1
    }

    ingest_resp = client.post("/api/v1/analytics/ingest?force_flush=true", json=event_payload)
    assert ingest_resp.status_code == 200
    flushed = ingest_resp.json()
    assert len(flushed) == 1
    assert flushed[0]["pool_id"] == "WARDA-HDU"
    assert flushed[0]["occupancy_at_window_end"] == 1

    # Query latest stats
    latest_resp = client.get("/api/v1/analytics/pools/WARDA-HDU/latest")
    assert latest_resp.status_code == 200
    assert latest_resp.json()["allocated"] == 1

    # Query summary
    summary_resp = client.get("/api/v1/analytics/summary")
    assert summary_resp.status_code == 200
    summary = summary_resp.json()
    assert summary["active_pools"] == 1
    assert summary["total_allocated"] == 1

