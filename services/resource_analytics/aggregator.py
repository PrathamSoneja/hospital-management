"""
Stateful stream aggregation engine for Resource Analytics.
Owned by Member D (Jay Patel).

Computes 1-minute tumbling windows over Kafka bed-events stream:
- window_start, window_end
- total requests, allocated, waitlisted, conflicts
- occupancy_at_window_end
- average_lock_wait_ms
- average_allocation_wait_ms
- contention_rate
"""
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from collections import OrderedDict
import logging
from services.resource_analytics.models import BedEvent, BedStats

logger = logging.getLogger("resource-analytics-aggregator")

def parse_iso_time(time_str: str) -> datetime:
    """Parses ISO8601 string to UTC datetime."""
    try:
        dt = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)

def get_window_bounds(event_dt: datetime, window_minutes: int = 1) -> tuple[datetime, datetime]:
    """Computes fixed tumbling window [start, end) aligned to the minute boundary."""
    minute = (event_dt.minute // window_minutes) * window_minutes
    start = event_dt.replace(minute=minute, second=0, microsecond=0)
    end = start + timedelta(minutes=window_minutes)
    return start, end

class WindowBucket:
    def __init__(self, pool_id: str, start: datetime, end: datetime):
        self.pool_id = pool_id
        self.start = start
        self.end = end
        self.requests = 0
        self.allocated = 0
        self.waitlisted = 0
        self.conflicts = 0
        self.occupancy_delta_sum = 0
        self.lock_waits: List[int] = []
        self.allocation_waits: List[int] = []

    def add_event(self, event: BedEvent):
        if event.event_type == "ALLOCATION_REQUESTED":
            self.requests += 1
        elif event.event_type == "BED_ALLOCATED":
            self.allocated += 1
        elif event.event_type == "ALLOCATION_WAITLISTED":
            self.waitlisted += 1
        elif event.event_type == "ALLOCATION_CONFLICT":
            self.conflicts += 1

        self.occupancy_delta_sum += event.occupancy_delta

        if event.lock_wait_ms is not None:
            self.lock_waits.append(event.lock_wait_ms)
        if event.allocation_wait_ms is not None:
            self.allocation_waits.append(event.allocation_wait_ms)

    def to_bed_stats(self, cumulative_occupancy: int) -> BedStats:
        avg_lock = (sum(self.lock_waits) / len(self.lock_waits)) if self.lock_waits else None
        avg_alloc = (sum(self.allocation_waits) / len(self.allocation_waits)) if self.allocation_waits else None
        contention_count = self.waitlisted + self.conflicts
        contention_rate = (contention_count / self.requests) if self.requests > 0 else 0.0

        return BedStats(
            schema_version="1.0.0",
            pool_id=self.pool_id,
            window_start=self.start.isoformat(),
            window_end=self.end.isoformat(),
            requests=self.requests,
            allocated=self.allocated,
            waitlisted=self.waitlisted,
            conflicts=self.conflicts,
            occupancy_at_window_end=cumulative_occupancy + self.occupancy_delta_sum,
            average_lock_wait_ms=round(avg_lock, 2) if avg_lock is not None else None,
            average_allocation_wait_ms=round(avg_alloc, 2) if avg_alloc is not None else None,
            contention_rate=min(1.0, max(0.0, round(contention_rate, 4)))
        )

class StreamAggregatorEngine:
    def __init__(self, watermark_delay_seconds: int = 5):
        self.watermark_delay = timedelta(seconds=watermark_delay_seconds)
        # pool_id -> { (start, end) -> WindowBucket }
        self.active_windows: Dict[str, Dict[tuple[datetime, datetime], WindowBucket]] = {}
        # pool_id -> current known base occupancy
        self.pool_occupancies: Dict[str, int] = {}
        # High watermark
        self.max_event_time: Optional[datetime] = None
        # Track seen event IDs to prevent duplicate counting from at-least-once outbox redelivery (bounded LRU)
        self.seen_event_ids: OrderedDict[str, None] = OrderedDict()
        self.max_seen_event_ids: int = 50000

    def process_event(self, event: BedEvent) -> List[BedStats]:
        """
        Processes an incoming event, assigns to tumbling window bucket,
        and flushes windows closed by the updated watermark.
        Deduplicates events by event_id to safely handle at-least-once redelivery.
        """
        if event.event_id:
            if event.event_id in self.seen_event_ids:
                logger.info("Ignoring duplicate bed event %s", event.event_id)
                return []
            self.seen_event_ids[event.event_id] = None
            if len(self.seen_event_ids) > self.max_seen_event_ids:
                self.seen_event_ids.popitem(last=False)

        event_dt = parse_iso_time(event.event_time)
        if self.max_event_time is None or event_dt > self.max_event_time:
            self.max_event_time = event_dt

        pool_id = event.pool_id
        start, end = get_window_bounds(event_dt)

        if pool_id not in self.active_windows:
            self.active_windows[pool_id] = {}
            self.pool_occupancies[pool_id] = 0

        bucket_key = (start, end)
        if bucket_key not in self.active_windows[pool_id]:
            self.active_windows[pool_id][bucket_key] = WindowBucket(pool_id, start, end)

        self.active_windows[pool_id][bucket_key].add_event(event)

        # Flush any windows that have closed based on current watermark
        watermark = self.max_event_time - self.watermark_delay
        closed_stats = []

        for p_id in list(self.active_windows.keys()):
            for b_key in list(self.active_windows[p_id].keys()):
                b_start, b_end = b_key
                if b_end <= watermark:
                    bucket = self.active_windows[p_id].pop(b_key)
                    curr_occ = self.pool_occupancies[p_id]
                    stats = bucket.to_bed_stats(curr_occ)
                    self.pool_occupancies[p_id] = stats.occupancy_at_window_end
                    closed_stats.append(stats)

        return closed_stats

    def flush_all_windows(self) -> List[BedStats]:
        """Forces all currently open windows to emit regardless of watermark (e.g. for testing or shutdown)."""
        closed_stats = []
        for pool_id in list(self.active_windows.keys()):
            # Sort windows chronologically
            sorted_keys = sorted(self.active_windows[pool_id].keys(), key=lambda k: k[0])
            for b_key in sorted_keys:
                bucket = self.active_windows[pool_id].pop(b_key)
                curr_occ = self.pool_occupancies.get(pool_id, 0)
                stats = bucket.to_bed_stats(curr_occ)
                self.pool_occupancies[pool_id] = stats.occupancy_at_window_end
                closed_stats.append(stats)
        return closed_stats

