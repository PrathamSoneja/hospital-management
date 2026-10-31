"""
Transactional Outbox Event Dispatcher.
Dispatches pending events from the PostgreSQL outbox table to:
- Google Pub/Sub (notifications)
- Apache Kafka (bed-events stream analytics)

Delivery is at-least-once. Each (destination, pool_id) stream is ordered by its
producer-assigned stream_sequence; an unfinished earlier row blocks only later
rows of the same stream. Claim tokens fence database updates, not broker writes:
a delayed publish from a worker whose claim expired can still reach the broker,
so consumers deduplicate on event_id and use stream_sequence to detect gaps.
"""
import logging
import re
import threading
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from sqlalchemy import and_, or_, exists, func
from sqlalchemy.orm import aliased
from services.bed_allocation.db import OutboxEventModel
from shared.observability.metrics import OUTBOX_PENDING_EVENTS, OUTBOX_DELIVERY_TOTAL

logger = logging.getLogger("outbox-dispatcher")

# DELIVERED is broker-confirmed; RESOLVED is an explicit operator decision. Nothing
# else unblocks a stream.
FINISHED_STATUSES = ("DELIVERED", "RESOLVED")


def _safe_error(name: str | None) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]", "_", name or "PublishNotConfirmed")[:512]


class EventPublisher(ABC):
    """Returns True only after the broker acknowledged the message."""

    @abstractmethod
    def publish_pubsub(self, topic: str, payload_json: str, attributes: dict) -> bool:
        pass

    @abstractmethod
    def publish_kafka(self, topic: str, key: str, payload_json: str) -> bool:
        pass


class OutboxDispatcher:
    def __init__(self, session_factory, publisher: EventPublisher, service_name: str = "bed-allocation",
                 owner_id: str | None = None, claim_seconds: int = 30, max_attempts: int = 12,
                 base_backoff_seconds: int = 1, max_backoff_seconds: int = 300):
        self.session_factory = session_factory
        self.publisher = publisher
        self.service_name = service_name
        self.owner_id = owner_id or f"outbox-{uuid.uuid4().hex}"
        self.claim_seconds = claim_seconds
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.max_backoff_seconds = max_backoff_seconds

    def backoff_seconds(self, attempt_count: int) -> int:
        return min(self.base_backoff_seconds * (2 ** max(0, attempt_count - 1)), self.max_backoff_seconds)

    def process_pending(self, batch_size: int = 50, should_stop=None) -> int:
        """Claim in a short transaction, publish without a DB transaction, then token-guard ACK/retry."""
        claims = self._claim(batch_size)
        processed_count = 0
        for index, claim in enumerate(claims):
            if should_stop is not None and should_stop():
                # Nothing was sent for these rows; hand them back instead of waiting for expiry.
                for unsent in claims[index:]:
                    self._abandon_claim(unsent)
                break
            try:
                success = self._publish(claim)
            except Exception as error:
                logger.warning("Outbox publish failed event_id=%s error_type=%s", claim["event_id"], type(error).__name__)
                success = False
                error_name = type(error).__name__
            else:
                error_name = None if success else "PublishNotConfirmed"

            if self._finish_claim(claim, success, error_name):
                if success:
                    processed_count += 1
                    outcome = "success"
                else:
                    outcome = "poison" if claim["attempt_count"] >= self.max_attempts else "retry"
            else:
                # A newer claim owns the row; this worker's result is not recorded.
                outcome = "stale_claim"
            OUTBOX_DELIVERY_TOTAL.labels(service=self.service_name, destination=claim["destination"], status=outcome).inc()
        self._update_backlog_metrics()
        return processed_count

    def _publish(self, claim) -> bool:
        if claim["destination"] == "pubsub":
            return self.publisher.publish_pubsub(
                topic=claim["topic"], payload_json=claim["payload"],
                attributes={"event_id": claim["event_id"], "pool_id": claim["pool_id"],
                            "stream_sequence": str(claim["stream_sequence"])},
            )
        if claim["destination"] == "kafka":
            # Kafka partitions by key; pool_id keeps one pool stream on one partition.
            return self.publisher.publish_kafka(topic=claim["topic"], key=claim["pool_id"], payload_json=claim["payload"])
        raise ValueError("unsupported outbox destination")

    def _claim(self, batch_size: int):
        now = datetime.now(timezone.utc)
        # Unresolved rows without a pool identity block their whole destination; they are never auto-skipped.
        unknown_legacy = aliased(OutboxEventModel)
        any_unassigned = exists().where(
            unknown_legacy.destination == OutboxEventModel.destination,
            unknown_legacy.pool_id.is_(None),
            unknown_legacy.status.not_in(FINISHED_STATUSES),
        )
        # Any unfinished earlier row (including null-sequence legacy rows) blocks its own stream only.
        predecessor = aliased(OutboxEventModel)
        has_predecessor = exists().where(
            predecessor.destination == OutboxEventModel.destination,
            predecessor.pool_id == OutboxEventModel.pool_id,
            predecessor.status.not_in(FINISHED_STATUSES),
            or_(predecessor.stream_sequence.is_(None), predecessor.stream_sequence < OutboxEventModel.stream_sequence),
        )
        candidates = []
        with self.session_factory() as session:
            rows = session.query(OutboxEventModel).filter(
                OutboxEventModel.stream_sequence.is_not(None),
                OutboxEventModel.pool_id.is_not(None),
                OutboxEventModel.available_at <= now,
                or_(
                    OutboxEventModel.status == "PENDING",
                    and_(OutboxEventModel.status == "IN_FLIGHT", OutboxEventModel.claim_until <= now),
                ),
                ~any_unassigned,
                ~has_predecessor,
            ).order_by(
                OutboxEventModel.destination, OutboxEventModel.pool_id, OutboxEventModel.stream_sequence
            ).with_for_update(skip_locked=True).limit(batch_size).all()
            for row in rows:
                token = uuid.uuid4().hex
                row.status = "IN_FLIGHT"
                row.claim_owner = self.owner_id
                row.claim_token = token
                row.claim_until = now + timedelta(seconds=self.claim_seconds)
                row.attempt_count += 1
                row.retry_count = row.attempt_count
                row.updated_at = now
                candidates.append({
                    "event_id": row.event_id, "destination": row.destination, "topic": row.topic,
                    "pool_id": row.pool_id, "payload": row.payload,
                    "stream_sequence": row.stream_sequence, "claim_token": token,
                    "attempt_count": row.attempt_count,
                })
            session.commit()
        return candidates

    def _owned_row(self, session, claim):
        return session.query(OutboxEventModel).filter_by(
            event_id=claim["event_id"], status="IN_FLIGHT", claim_token=claim["claim_token"]
        ).with_for_update().first()

    def _finish_claim(self, claim, success: bool, error_name: str | None) -> bool:
        now = datetime.now(timezone.utc)
        with self.session_factory() as session:
            row = self._owned_row(session, claim)
            if row is None:
                session.rollback()
                return False
            row.claim_owner = row.claim_token = None
            row.claim_until = None
            row.updated_at = now
            if success:
                row.status = "DELIVERED"
                row.last_error = None
            else:
                row.last_error = _safe_error(error_name)
                if row.attempt_count >= self.max_attempts:
                    # Poison stays visible and blocks only its own stream until an operator acts.
                    row.status = "POISONED"
                else:
                    row.status = "PENDING"
                    row.available_at = now + timedelta(seconds=self.backoff_seconds(row.attempt_count))
            session.commit()
            return True

    def _abandon_claim(self, claim) -> bool:
        now = datetime.now(timezone.utc)
        with self.session_factory() as session:
            row = self._owned_row(session, claim)
            if row is None:
                session.rollback()
                return False
            row.status = "PENDING"
            row.claim_owner = row.claim_token = None
            row.claim_until = None
            row.attempt_count = max(0, row.attempt_count - 1)
            row.retry_count = row.attempt_count
            row.updated_at = now
            session.commit()
            return True

    def _update_backlog_metrics(self):
        # Every replica reports the same table-wide count; dashboards aggregate with max(), not sum().
        with self.session_factory() as session:
            for destination in ("pubsub", "kafka"):
                count = session.query(OutboxEventModel).filter(
                    OutboxEventModel.destination == destination,
                    OutboxEventModel.status.not_in(FINISHED_STATUSES),
                ).count()
                OUTBOX_PENDING_EVENTS.labels(service=self.service_name, destination=destination).set(count)

    # ----- Operator procedures: explicit, per event, never automatic -----

    def stream_report(self):
        """Counts of unfinished rows by destination/status, plus legacy null-sequence rows."""
        with self.session_factory() as session:
            rows = session.query(
                OutboxEventModel.destination, OutboxEventModel.status,
                OutboxEventModel.stream_sequence.is_(None), func.count(),
            ).filter(OutboxEventModel.status.not_in(FINISHED_STATUSES)).group_by(
                OutboxEventModel.destination, OutboxEventModel.status, OutboxEventModel.stream_sequence.is_(None)
            ).all()
        report = {}
        for destination, status, legacy, count in rows:
            entry = report.setdefault(destination, {"legacy_unsequenced": 0})
            entry[status] = entry.get(status, 0) + count
            if legacy:
                entry["legacy_unsequenced"] += count
        return report

    def legacy_blockers(self):
        """Return unresolved pre-sequence rows for operator review; never auto-resolve them."""
        with self.session_factory() as session:
            rows = session.query(OutboxEventModel).filter(
                OutboxEventModel.stream_sequence.is_(None), OutboxEventModel.status.not_in(FINISHED_STATUSES)
            ).order_by(OutboxEventModel.destination, OutboxEventModel.pool_id, OutboxEventModel.created_at).all()
            return [{"event_id": row.event_id, "destination": row.destination,
                     "pool_id": row.pool_id, "status": row.status, "created_at": row.created_at.isoformat()}
                    for row in rows]

    def poison_rows(self):
        with self.session_factory() as session:
            rows = session.query(OutboxEventModel).filter(OutboxEventModel.status == "POISONED").all()
            return [{"event_id": row.event_id, "destination": row.destination, "pool_id": row.pool_id,
                     "stream_sequence": row.stream_sequence, "attempt_count": row.attempt_count,
                     "last_error": row.last_error} for row in rows]

    def retry_poisoned(self, event_id: str) -> bool:
        """Re-queue one poisoned row with a fresh attempt budget after the cause is fixed."""
        now = datetime.now(timezone.utc)
        with self.session_factory() as session:
            row = session.query(OutboxEventModel).filter_by(event_id=event_id, status="POISONED").with_for_update().first()
            if row is None:
                session.rollback()
                return False
            row.status = "PENDING"
            row.attempt_count = 0
            row.retry_count = 0
            row.available_at = now
            row.updated_at = now
            session.commit()
            return True

    def resolve(self, event_id: str, note: str) -> bool:
        """Mark a poisoned or legacy row as handled outside the dispatcher, unblocking its stream.

        The row is kept for audit. Use only after the operator has delivered the event by
        other means or confirmed with the consumer owner that it must not be delivered.
        """
        now = datetime.now(timezone.utc)
        with self.session_factory() as session:
            row = session.query(OutboxEventModel).filter(
                OutboxEventModel.event_id == event_id,
                or_(OutboxEventModel.status == "POISONED",
                    and_(OutboxEventModel.stream_sequence.is_(None), OutboxEventModel.status.not_in(FINISHED_STATUSES))),
            ).with_for_update().first()
            if row is None or row.status == "IN_FLIGHT":
                session.rollback()
                return False
            row.status = "RESOLVED"
            row.last_error = _safe_error(f"operator_resolved:{note}")
            row.claim_owner = row.claim_token = None
            row.claim_until = None
            row.updated_at = now
            session.commit()
            return True


class OutboxWorker:
    """Supervised background loop. Crash recovery relies on claim expiry, not on this process."""

    def __init__(self, dispatcher: OutboxDispatcher, poll_seconds: float = 1.0, batch_size: int = 50,
                 max_error_backoff_seconds: float = 30.0):
        self.dispatcher = dispatcher
        self.poll_seconds = poll_seconds
        self.batch_size = batch_size
        self.max_error_backoff_seconds = max_error_backoff_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.consecutive_failures = 0

    def start(self):
        if self._thread is not None:
            raise RuntimeError("Outbox worker already started")
        self._thread = threading.Thread(target=self._run, name="allocation-outbox", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> bool:
        self._stop.set()
        if self._thread is None:
            return True
        self._thread.join(timeout=timeout)
        return not self._thread.is_alive()

    def run_once(self) -> int:
        return self.dispatcher.process_pending(self.batch_size, should_stop=self._stop.is_set)

    def _run(self):
        while not self._stop.is_set():
            try:
                delivered = self.run_once()
            except Exception as error:
                # Database or programming errors: back off and keep the loop alive.
                self.consecutive_failures += 1
                logger.error("Outbox pass failed error_type=%s", type(error).__name__)
                delay = min(self.poll_seconds * (2 ** self.consecutive_failures), self.max_error_backoff_seconds)
            else:
                self.consecutive_failures = 0
                delay = 0 if delivered >= self.batch_size else self.poll_seconds
            self._stop.wait(delay)
