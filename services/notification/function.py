"""Notification as a serverless function (Nisarg, task b): Cloud Run functions (2nd gen) entry point.

Eventarc delivers each Pub/Sub message on topic bed-notifications as a CloudEvent. This reuses the same
checking, wording and Firestore deduplication as main.py, so behaviour is identical:
- a malformed message is acknowledged (returning normally), because retrying can never fix it;
- a Firestore error raises, so the function fails and Pub/Sub retries, then dead-letters after 10 attempts.
"""
import os

import functions_framework

from services.notification.handler import BadMessage, decode_push, render, validate
from services.notification.store import FirestoreNotificationStore
from shared.observability.logging import setup_logger

SERVICE = "notification-function"
logger = setup_logger(SERVICE)

_store = None


def get_store():
    """Create the Firestore client once per instance, on first use (not at import, so tests stay offline)."""
    global _store
    if _store is None:
        project = os.environ.get("FIRESTORE_PROJECT_ID") or os.environ["GOOGLE_CLOUD_PROJECT"]
        _store = FirestoreNotificationStore(project)
    return _store


def process(data, store):
    """Handle one Pub/Sub MessagePublished payload ({"message": {...}, "subscription": ...}). Return the outcome."""
    try:
        event, _ = decode_push(data)
        validate(event)
    except BadMessage as error:
        logger.error(f"notification rejected: {error}", extra={"event": "notification_rejected"})
        return "rejected"

    text = render(event)
    log_fields = {"request_id": event["request_id"], "allocation_id": event["allocation_id"],
                  "pool_id": event["pool_id"]}
    outcome = store.record(event, text)  # Raises on Firestore trouble, so Pub/Sub delivers it again.

    if not outcome.is_new:
        logger.info("duplicate notification skipped", extra={"event": "notification_duplicate", **log_fields})
        return "duplicate"
    if outcome.missing:
        logger.warning(f"{outcome.missing} notification(s) missing before sequence {event['stream_sequence']}",
                       extra={"event": "notification_gap", **log_fields})
    logger.info(text, extra={"event": "staff_notification", **log_fields})
    return "delivered"


@functions_framework.cloud_event
def handle_bed_notification(cloud_event):
    """Entry point named in Terraform (build_config.entry_point)."""
    process(cloud_event.data, get_store())