"""Remember which notifications were sent, in Firestore, so duplicates are skipped and gaps are spotted.

Pub/Sub delivers at least once, and Cloud Run may run several copies of this service, so this memory
has to be shared and durable. Firestore transactions make "check, then record" one indivisible step.
"""
from datetime import datetime, timezone

from google.cloud import firestore

NOTIFICATIONS = "notifications"
STREAMS = "notification_streams"


class Outcome:
    """What happened to one event: was it new, and how many earlier messages look missing."""

    def __init__(self, is_new, missing=0, late=False):
        self.is_new = is_new
        self.missing = missing  # Messages skipped between the last one seen and this one.
        self.late = late        # Arrived after a higher stream_sequence. Still delivered, never dropped.


class FirestoreNotificationStore:
    def __init__(self, project):
        self.client = firestore.Client(project=project)

    def record(self, event, text):
        """Record one event exactly once. Return an Outcome."""
        notification_ref = self.client.collection(NOTIFICATIONS).document(event["event_id"])
        stream_ref = self.client.collection(STREAMS).document(event["pool_id"])
        sequence = event["stream_sequence"]

        @firestore.transactional
        def apply(transaction):
            if notification_ref.get(transaction=transaction).exists:
                return Outcome(is_new=False)  # Already sent: this is a redelivery.

            stream = stream_ref.get(transaction=transaction)
            highest = stream.get("highest_sequence") if stream.exists else None
            # The first message seen for a pool sets the starting point, so it never counts as a gap.
            missing = sequence - highest - 1 if highest is not None and sequence > highest + 1 else 0
            late = highest is not None and sequence < highest

            transaction.set(notification_ref, {
                **event,
                "message": text,
                "received_at": datetime.now(timezone.utc).isoformat(),
            })
            transaction.set(stream_ref, {"highest_sequence": max(sequence, highest or 0)})
            return Outcome(is_new=True, missing=missing, late=late)

        return apply(self.client.transaction())

    def check(self):
        """Readiness: can we reach Firestore?"""
        try:
            next(iter(self.client.collection(STREAMS).limit(1).stream()), None)
            return True
        except Exception:
            return False