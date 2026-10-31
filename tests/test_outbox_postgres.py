"""
Outbox sequencing, claim, retry, and blocking behavior on real PostgreSQL.

Requires TEST_POSTGRES_URL to point at a disposable local database; each test
runs in its own temporary schema. Broker delivery uses tracked publisher doubles:
these tests do not verify Pub/Sub or Kafka.
"""
import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import jsonschema
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.bed_allocation.db import Base, OutboxEventModel, OutboxStreamCounterModel
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.outbox.dispatcher import OutboxDispatcher, OutboxWorker
from services.bed_allocation.service import BedAllocationDomainService, _next_outbox_sequence
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakeLockCoordinatorClient,
    FakePatientRegistryClient, FakeStaffSchedulerClient,
)
from tests.support.triage_fakes import MockFencingValidator

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts" / "events"


@pytest.fixture
def pg_sessions():
    database_url = os.environ.get("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("Set TEST_POSTGRES_URL to an isolated local PostgreSQL database")
    if urlsplit(database_url).hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("TEST_POSTGRES_URL must point to local PostgreSQL; cloud databases are not permitted")
    admin_engine = create_engine(database_url)
    schema = f"test_outbox_{uuid.uuid4().hex}"
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"}, pool_size=12, max_overflow=4)
    try:
        Base.metadata.create_all(bind=engine)
        yield sessionmaker(bind=engine, autoflush=False, autocommit=False)
    finally:
        engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin_engine.dispose()


class RecordingPublisher:
    """Thread-safe publisher double that checks per-stream ordering as it receives messages."""

    def __init__(self, delay: float = 0.0):
        self.lock = threading.Lock()
        self.messages = []
        self.in_flight = set()
        self.failing = set()  # (destination, pool_id)
        self.delay = delay
        self.violations = []

    def _record(self, destination, topic, key, payload_json):
        payload = json.loads(payload_json)
        stream = (destination, payload["pool_id"])
        with self.lock:
            if stream in self.in_flight:
                self.violations.append(("concurrent", stream))
            self.in_flight.add(stream)
        try:
            if self.delay:
                time.sleep(self.delay)
            if stream in self.failing:
                raise ConnectionError("broker unavailable")
            with self.lock:
                self.messages.append({"destination": destination, "topic": topic, "key": key, "payload": payload})
            return True
        finally:
            with self.lock:
                self.in_flight.discard(stream)

    def publish_pubsub(self, topic, payload_json, attributes):
        assert attributes["event_id"] == json.loads(payload_json)["event_id"]
        return self._record("pubsub", topic, None, payload_json)

    def publish_kafka(self, topic, key, payload_json):
        assert key == json.loads(payload_json)["pool_id"]
        return self._record("kafka", topic, key, payload_json)

    def sequences(self, destination, pool_id):
        return [m["payload"]["stream_sequence"] for m in self.messages
                if m["destination"] == destination and m["payload"]["pool_id"] == pool_id]


def add_event(session, destination, pool_id, sequenced=True, status="PENDING"):
    sequence = _next_outbox_sequence(session, destination, pool_id) if sequenced else None
    event_id = str(uuid.uuid4())
    payload = {"event_id": event_id, "pool_id": pool_id, "stream_sequence": sequence}
    session.add(OutboxEventModel(
        event_id=event_id, destination=destination, pool_id=pool_id, stream_sequence=sequence,
        topic="bed-events" if destination == "kafka" else "bed-notifications", message_key=pool_id,
        payload=json.dumps(payload), status=status,
        available_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    ))
    return event_id, sequence


def seed(sessions, specs):
    ids = []
    with sessions() as session:
        for spec in specs:
            ids.append(add_event(session, *spec))
        session.commit()
    return ids


def make_due(sessions, event_id=None):
    with sessions() as session:
        query = session.query(OutboxEventModel)
        if event_id:
            query = query.filter_by(event_id=event_id)
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        for row in query.all():
            row.available_at = past
            if row.status == "IN_FLIGHT":
                row.claim_until = past
        session.commit()


def row(sessions, event_id):
    with sessions() as session:
        return session.query(OutboxEventModel).filter_by(event_id=event_id).one()


def test_concurrent_producers_get_contiguous_sequences_and_rollback_consumes_none(pg_sessions):
    barrier = threading.Barrier(16)
    errors = []

    def produce(index):
        try:
            with pg_sessions() as session:
                barrier.wait()
                # Same deterministic counter order as the service: kafka then pubsub.
                add_event(session, "kafka", "POOL-A")
                add_event(session, "pubsub", "POOL-A")
                if index % 2:
                    session.rollback()
                else:
                    session.commit()
        except Exception as error:  # pragma: no cover - reported below
            errors.append(error)

    threads = [threading.Thread(target=produce, args=(i,)) for i in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert not errors
    with pg_sessions() as session:
        for destination in ("kafka", "pubsub"):
            sequences = sorted(r.stream_sequence for r in session.query(OutboxEventModel).filter_by(destination=destination))
            assert sequences == list(range(1, 9))
            counter = session.get(OutboxStreamCounterModel, (destination, "POOL-A"))
            assert counter.last_sequence == 8


def test_duplicate_stream_sequence_is_rejected(pg_sessions):
    from sqlalchemy.exc import IntegrityError
    seed(pg_sessions, [("kafka", "POOL-A")])
    with pg_sessions() as session:
        session.add(OutboxEventModel(event_id=str(uuid.uuid4()), destination="kafka", pool_id="POOL-A",
                                     stream_sequence=1, topic="bed-events", payload="{}"))
        with pytest.raises(IntegrityError):
            session.commit()


def _service(sessions):
    patient = FakePatientRegistryClient()
    for n in (1, 2, 3):
        patient.seed_admission(f"adm-{n}", f"pat-{n}", "OPEN")
    triage_domain = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    for n in (1, 2, 3):
        triage_domain.set_triage(f"adm-{n}", n, "POOL-A")
    beds = FakeBedRegistryClient()
    beds.seed_bed("bed-1", "POOL-A", ward="WARD-NORTH")
    beds.seed_bed("bed-2", "POOL-A", ward="WARD-NORTH")
    return BedAllocationDomainService(
        session_factory=sessions, patient_client=patient, triage_client=DirectTriageClient(triage_domain),
        lock_client=FakeLockCoordinatorClient(), bed_client=beds,
        staff_client=FakeStaffSchedulerClient(initial_capacity=10), owner_id="pg-outbox-test",
    )


def test_business_events_are_schema_valid_stable_and_not_duplicated_on_replay(pg_sessions):
    schemas = {
        "kafka": json.loads((CONTRACTS / "bed-event.schema.json").read_text()),
        "pubsub": json.loads((CONTRACTS / "notification.schema.json").read_text()),
    }
    service = _service(pg_sessions)
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "req-1", "key-1")
    service.create_allocation(CreateAllocationRequest(admission_id="adm-2", pool_id="POOL-A"), "req-2", "key-2")
    waiting = service.create_allocation(CreateAllocationRequest(admission_id="adm-3", pool_id="POOL-A"), "req-3", "key-3")
    assert waiting.status == "WAITLISTED"
    with pg_sessions() as session:
        before = {(r.event_id, r.stream_sequence) for r in session.query(OutboxEventModel)}

    # Replay of a completed operation and a re-run of finalization add nothing.
    replay = service.create_allocation(CreateAllocationRequest(admission_id="adm-1", pool_id="POOL-A"), "req-1b", "key-1")
    assert replay.allocation_id == first.allocation_id
    service._finalize_create(first.allocation_id, "req-1", "key-1", {"admission_id": "adm-1", "pool_id": "POOL-A"}, "ALLOCATED", 0, time.perf_counter())
    with pg_sessions() as session:
        assert {(r.event_id, r.stream_sequence) for r in session.query(OutboxEventModel)} == before

    released = service.release_allocation(first.allocation_id, "discharged", "req-rel", "key-rel")
    assert released.promoted_allocation_id == waiting.allocation_id

    with pg_sessions() as session:
        rows = session.query(OutboxEventModel).order_by(OutboxEventModel.destination, OutboxEventModel.stream_sequence).all()
        for event in rows:
            payload = json.loads(event.payload)
            jsonschema.validate(payload, schemas[event.destination])
            assert payload["event_id"] == event.event_id and payload["stream_sequence"] == event.stream_sequence
        kafka = [json.loads(r.payload) for r in rows if r.destination == "kafka"]
        pubsub = [json.loads(r.payload) for r in rows if r.destination == "pubsub"]

    assert [e["stream_sequence"] for e in kafka] == list(range(1, len(kafka) + 1))
    assert [e["stream_sequence"] for e in pubsub] == list(range(1, len(pubsub) + 1))
    assert [(e["event_type"], e["occupancy_delta"]) for e in kafka] == [
        ("BED_ALLOCATED", 1), ("BED_ALLOCATED", 1), ("ALLOCATION_WAITLISTED", 0),
        ("BED_RELEASED", -1), ("BED_ALLOCATED", 1),
    ]
    assert sum(e["occupancy_delta"] for e in kafka) == 2
    assert {e["patient_id"] for e in kafka} == {"pat-1", "pat-2", "pat-3"}
    # The promotion shares the release's lock acquisition, so it reports no second wait.
    assert kafka[3]["lock_wait_ms"] is not None and kafka[4]["lock_wait_ms"] is None
    assert kafka[2]["allocation_wait_ms"] is None
    # Notification ward comes from the Bed Registry; a bedless waitlist notification carries null.
    assert [(e["event_type"], e["ward"]) for e in pubsub] == [
        ("BED_ALLOCATED", "WARD-NORTH"), ("BED_ALLOCATED", "WARD-NORTH"), ("ALLOCATION_WAITLISTED", None),
        ("BED_RELEASED", "WARD-NORTH"), ("BED_ALLOCATED", "WARD-NORTH"),
    ]


def test_expired_claim_is_reclaimed_and_stale_ack_cannot_overwrite(pg_sessions):
    [(event_id, sequence)] = seed(pg_sessions, [("kafka", "POOL-A")])
    first = OutboxDispatcher(pg_sessions, RecordingPublisher(), owner_id="worker-1")
    second = OutboxDispatcher(pg_sessions, RecordingPublisher(), owner_id="worker-2")
    [claim_one] = first._claim(10)
    assert second._claim(10) == []  # live claim blocks other workers

    make_due(pg_sessions, event_id)
    [claim_two] = second._claim(10)
    assert (claim_two["event_id"], claim_two["stream_sequence"]) == (event_id, sequence)
    assert claim_two["claim_token"] != claim_one["claim_token"]

    assert first._finish_claim(claim_one, False, "TimeoutError") is False
    assert first._finish_claim(claim_one, True, None) is False
    assert row(pg_sessions, event_id).claim_owner == "worker-2"
    assert second._finish_claim(claim_two, True, None) is True
    final = row(pg_sessions, event_id)
    assert (final.status, final.attempt_count, final.claim_token) == ("DELIVERED", 2, None)


def test_crash_before_ack_redelivers_same_event_id_and_sequence(pg_sessions):
    [(event_id, sequence)] = seed(pg_sessions, [("pubsub", "POOL-A")])
    publisher = RecordingPublisher()
    crashed = OutboxDispatcher(pg_sessions, publisher, owner_id="crashed")
    [claim] = crashed._claim(10)
    crashed._publish(claim)  # broker accepted; the process dies before recording the ACK
    make_due(pg_sessions, event_id)

    restarted = OutboxDispatcher(pg_sessions, publisher, owner_id="restarted")
    assert restarted.process_pending() == 1
    delivered = [(m["payload"]["event_id"], m["payload"]["stream_sequence"]) for m in publisher.messages]
    assert delivered == [(event_id, sequence), (event_id, sequence)]  # at-least-once duplicate
    assert row(pg_sessions, event_id).status == "DELIVERED"


def test_concurrent_workers_keep_per_stream_order_and_deliver_once(pg_sessions):
    specs = [(destination, pool) for _ in range(8) for pool in ("POOL-A", "POOL-B", "POOL-C")
             for destination in ("kafka", "pubsub")]
    seed(pg_sessions, specs)
    publisher = RecordingPublisher(delay=0.005)
    dispatchers = [OutboxDispatcher(pg_sessions, publisher, owner_id=f"worker-{i}") for i in range(4)]
    deadline = time.time() + 30

    def run(dispatcher):
        while time.time() < deadline:
            with pg_sessions() as session:
                if session.query(OutboxEventModel).filter(OutboxEventModel.status != "DELIVERED").count() == 0:
                    return
            dispatcher.process_pending(batch_size=4)

    threads = [threading.Thread(target=run, args=(d,)) for d in dispatchers]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(40)
    assert publisher.violations == []
    assert len(publisher.messages) == len(specs)
    for pool in ("POOL-A", "POOL-B", "POOL-C"):
        for destination in ("kafka", "pubsub"):
            assert publisher.sequences(destination, pool) == list(range(1, 9))


def test_backoff_poison_blocks_only_its_stream_until_operator_retry(pg_sessions):
    ids = seed(pg_sessions, [("kafka", "POOL-A"), ("kafka", "POOL-A"), ("pubsub", "POOL-A"), ("kafka", "POOL-B")])
    head, behind = ids[0][0], ids[1][0]
    publisher = RecordingPublisher()
    publisher.failing.add(("kafka", "POOL-A"))
    dispatcher = OutboxDispatcher(pg_sessions, publisher, max_attempts=3, base_backoff_seconds=2, max_backoff_seconds=3)

    assert dispatcher.process_pending() == 2  # other destination and other pool continue
    delays = []
    for _ in range(3):
        failed = row(pg_sessions, head)
        delays.append((failed.available_at - failed.updated_at).total_seconds())
        make_due(pg_sessions, head)
        dispatcher.process_pending()
    poisoned = row(pg_sessions, head)
    assert [round(d) for d in delays[:2]] == [2, 3]  # exponential, capped at max_backoff_seconds
    assert (poisoned.status, poisoned.attempt_count, poisoned.last_error) == ("POISONED", 3, "ConnectionError")
    assert row(pg_sessions, behind).status == "PENDING"
    assert dispatcher.process_pending() == 0
    assert [p["event_id"] for p in dispatcher.poison_rows()] == [head]

    publisher.failing.clear()
    assert dispatcher.retry_poisoned(head) is True
    assert dispatcher.process_pending() == 1  # only the stream head is claimable per pass
    assert dispatcher.process_pending() == 1
    assert publisher.sequences("kafka", "POOL-A") == [1, 2]


def test_legacy_unsequenced_rows_block_affected_streams_until_resolved(pg_sessions):
    with pg_sessions() as session:
        pool_legacy, _ = add_event(session, "kafka", "POOL-A", sequenced=False)
        session.add(OutboxEventModel(event_id="legacy-no-pool", destination="pubsub", pool_id=None,
                                     stream_sequence=None, topic="bed-notifications", payload="{}"))
        session.commit()
    seed(pg_sessions, [("kafka", "POOL-A"), ("kafka", "POOL-B"), ("pubsub", "POOL-B")])
    publisher = RecordingPublisher()
    dispatcher = OutboxDispatcher(pg_sessions, publisher)

    assert dispatcher.process_pending() == 1  # only kafka/POOL-B is unblocked
    assert publisher.sequences("kafka", "POOL-B") == [1]
    report = dispatcher.stream_report()
    assert report["kafka"]["legacy_unsequenced"] == 1 and report["pubsub"]["legacy_unsequenced"] == 1
    assert {r["event_id"] for r in dispatcher.legacy_blockers()} == {pool_legacy, "legacy-no-pool"}

    assert dispatcher.resolve(pool_legacy, "delivered manually") is True
    assert dispatcher.resolve("legacy-no-pool", "consumer owner confirmed obsolete") is True
    assert dispatcher.process_pending() == 2
    assert row(pg_sessions, pool_legacy).status == "RESOLVED"
    assert all(m["payload"].get("stream_sequence") for m in publisher.messages)  # legacy rows never published
    assert dispatcher.resolve(pool_legacy, "again") is False


def test_worker_survives_failed_pass_and_stops_cleanly(pg_sessions):
    seed(pg_sessions, [("kafka", "POOL-A")])
    dispatcher = OutboxDispatcher(pg_sessions, RecordingPublisher())
    original = dispatcher.process_pending
    calls = {"n": 0}

    def flaky(batch_size, should_stop=None):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("database unavailable")
        return original(batch_size, should_stop)

    dispatcher.process_pending = flaky
    worker = OutboxWorker(dispatcher, poll_seconds=0.01)
    worker.start()
    deadline = time.time() + 10
    while time.time() < deadline and calls["n"] < 3:
        time.sleep(0.02)
    assert worker.stop(timeout=5) is True
    assert calls["n"] >= 2
    with pg_sessions() as session:
        assert session.query(OutboxEventModel).one().status == "DELIVERED"


def test_shutdown_returns_unsent_claims_without_spending_attempts(pg_sessions):
    ids = seed(pg_sessions, [("kafka", "POOL-A"), ("kafka", "POOL-B"), ("kafka", "POOL-C")])
    publisher = RecordingPublisher()
    dispatcher = OutboxDispatcher(pg_sessions, publisher)
    stops = iter([False, True, True])
    assert dispatcher.process_pending(should_stop=lambda: next(stops)) == 1
    states = sorted((row(pg_sessions, event_id).status, row(pg_sessions, event_id).attempt_count) for event_id, _ in ids)
    assert states == [("DELIVERED", 1), ("PENDING", 0), ("PENDING", 0)]
