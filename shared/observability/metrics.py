"""
Prometheus metrics registry with bounded cardinalities.
Prohibits unbounded labels (e.g., patient IDs, request IDs, allocation IDs, lock IDs).
"""
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

# HTTP Traffic Metrics
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total count of HTTP requests processed",
    ["service", "method", "endpoint", "status"]
)

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Histogram of HTTP request durations in seconds",
    ["service", "method", "endpoint"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
)

# gRPC Traffic Metrics
GRPC_REQUESTS_TOTAL = Counter(
    "grpc_requests_total",
    "Total count of gRPC requests processed",
    ["service", "method", "code"]
)

GRPC_REQUEST_DURATION_SECONDS = Histogram(
    "grpc_request_duration_seconds",
    "Histogram of gRPC request durations in seconds",
    ["service", "method"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

# Bed Allocation Business & Lock Metrics
ALLOCATION_REQUESTS_TOTAL = Counter(
    "allocation_requests_total",
    "Total allocation requests partitioned by outcome",
    ["service", "pool_id", "outcome"] # outcome: ALLOCATED, WAITLISTED, REJECTED, CONFLICT
)

LOCK_WAIT_SECONDS = Histogram(
    "lock_wait_seconds",
    "Time spent waiting to acquire distributed pool lock",
    ["service", "pool_id"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

ALLOCATION_PROCESSING_SECONDS = Histogram(
    "allocation_processing_seconds",
    "Total end-to-end processing latency for allocation workflows",
    ["service", "pool_id", "outcome"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
)

# Waitlist Gauge
# Global gauge documentation: updated by master/leader or per-pool scraper to prevent double counting
WAITLIST_ENTRIES = Gauge(
    "waitlist_entries",
    "Current count of active waitlisted admissions per pool",
    ["service", "pool_id"]
)

# Distributed Coordination Metrics
LEASE_EXPIRATIONS_TOTAL = Counter(
    "lease_expirations_total",
    "Total number of lock leases that expired prior to explicit release",
    ["service", "pool_id"]
)

STALE_FENCING_REJECTIONS_TOTAL = Counter(
    "stale_fencing_rejections_total",
    "Count of mutations rejected due to stale fencing tokens",
    ["service", "pool_id"]
)

# Outbox Metrics
OUTBOX_PENDING_EVENTS = Gauge(
    "outbox_pending_events",
    "Current backlog of undelivered transactional outbox events",
    ["service", "destination"]
)

OUTBOX_DELIVERY_TOTAL = Counter(
    "outbox_delivery_total",
    "Total outbox event delivery attempts",
    ["service", "destination", "status"] # status: success, retry, failed
)
