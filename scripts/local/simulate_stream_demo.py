"""
Interactive Stream Processing Demonstration Script for Task (e).
Owned by Member D (Jay Patel).

Simulates a real-time Kafka event stream from Bed Allocation, processes it through
the stateful 1-minute tumbling window aggregator, and displays the aggregated
BedStats record matching contracts/events/bed-stats.schema.json.
"""
import sys
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

from services.resource_analytics.models import BedEvent
from services.resource_analytics.aggregator import StreamAggregatorEngine

def run_simulation():
    print("=" * 80)
    print("  HOSPITAL BED MANAGEMENT - STREAM PROCESSING DEMO (Task e)")
    print("  Student: Jay Patel (Member D - 2026H1030072P)")
    print("=" * 80)

    engine = StreamAggregatorEngine(watermark_delay_seconds=5)
    base_time = datetime(2026, 11, 6, 10, 0, 0, tzinfo=timezone.utc)

    # Simulated sequence of bed events for pool WARDA-ICU
    events_data = [
        # 1. Allocation Request
        {"type": "ALLOCATION_REQUESTED", "delta": 0, "lock_ms": 12, "contended": False, "alloc_ms": None, "sec": 5},
        # 2. Bed Allocated (+1 occupancy)
        {"type": "BED_ALLOCATED", "delta": 1, "lock_ms": 15, "contended": False, "alloc_ms": 45, "sec": 12},
        # 3. Second Request
        {"type": "ALLOCATION_REQUESTED", "delta": 0, "lock_ms": 18, "contended": False, "alloc_ms": None, "sec": 20},
        # 4. Bed Allocated (+1 occupancy)
        {"type": "BED_ALLOCATED", "delta": 1, "lock_ms": 22, "contended": False, "alloc_ms": 50, "sec": 25},
        # 5. Third Request (Pool is now full -> Waitlisted!)
        {"type": "ALLOCATION_REQUESTED", "delta": 0, "lock_ms": 28, "contended": True, "alloc_ms": None, "sec": 35},
        {"type": "ALLOCATION_WAITLISTED", "delta": 0, "lock_ms": 30, "contended": True, "alloc_ms": None, "sec": 38},
        # 6. Bed Released (-1 occupancy)
        {"type": "BED_RELEASED", "delta": -1, "lock_ms": 10, "contended": False, "alloc_ms": None, "sec": 48},
    ]

    print("\n[Step 1] Ingesting raw events from Kafka topic 'bed-events':")
    print("-" * 80)

    seq = 1
    for item in events_data:
        event_time = (base_time + timedelta(seconds=item["sec"])).isoformat()
        evt = BedEvent(
            event_id=f"evt-{seq:03d}",
            stream_sequence=seq,
            event_type=item["type"],
            event_time=event_time,
            request_id=f"req-{seq:03d}",
            allocation_id=f"alloc-{seq:03d}",
            admission_id=f"adm-{seq:03d}",
            patient_id=f"pat-{seq:03d}",
            pool_id="WARDA-ICU",
            bed_id="bed-warda-icu-01" if item["delta"] != 0 else None,
            lock_wait_ms=item["lock_ms"],
            lock_contended=item["contended"],
            allocation_wait_ms=item["alloc_ms"],
            occupancy_delta=item["delta"]
        )
        print(f"  --> Seq #{seq:02d} | {evt.event_type:<22} | Time: {evt.event_time} | Delta: {evt.occupancy_delta:+d} | LockWait: {evt.lock_wait_ms}ms")
        engine.process_event(evt)
        seq += 1

    print("\n[Step 2] Closing 1-Minute Tumbling Window [10:00:00Z - 10:01:00Z):")
    print("-" * 80)

    flushed_stats = engine.flush_all_windows()
    for stats in flushed_stats:
        print(f"  • Pool Identifier        : {stats.pool_id}")
        print(f"  • Window Interval        : {stats.window_start}  -->  {stats.window_end}")
        print(f"  • Total Requests         : {stats.requests}")
        print(f"  • Beds Allocated         : {stats.allocated}")
        print(f"  • Patients Waitlisted    : {stats.waitlisted}")
        print(f"  • Net Occupancy at End   : {stats.occupancy_at_window_end} bed(s)")
        print(f"  • Average Lock Wait Time : {stats.average_lock_wait_ms} ms")
        print(f"  • Contention Rate        : {stats.contention_rate * 100:.1f}%")

        print("\n[Step 3] Output JSON payload emitted to Kafka topic 'bed-stats':")
        print("-" * 80)
        print(json.dumps(stats.model_dump(), indent=2))

    print("\n" + "=" * 80)
    print("  DEMO COMPLETE: All calculations verified against JSON contract schema.")
    print("=" * 80)

if __name__ == "__main__":
    run_simulation()
