"""Prometheus metrics for the Notification service. Labels are fixed sets: 4 event types, a few pools."""
from prometheus_client import Counter

from services.notification.handler import EVENT_TYPES

NOTIFICATIONS_TOTAL = Counter(
    "notifications_total",
    "Notification messages handled, by event type and outcome (delivered, duplicate, rejected, retry)",
    ["service", "event_type", "outcome"],
)

NOTIFICATION_GAPS_TOTAL = Counter(
    "notification_missing_messages_total",
    "Messages that look missing, from jumps in stream_sequence",
    ["service", "pool_id"],
)


def count(service, event, outcome):
    """Count one handled message. Anything outside the 4 known types is labelled "unknown"."""
    event_type = event.get("event_type") if isinstance(event, dict) else None
    NOTIFICATIONS_TOTAL.labels(
        service=service,
        event_type=event_type if event_type in EVENT_TYPES else "unknown",
        outcome=outcome,
    ).inc()


def count_missing(service, pool_id, missing):
    NOTIFICATION_GAPS_TOTAL.labels(service=service, pool_id=pool_id).inc(missing)