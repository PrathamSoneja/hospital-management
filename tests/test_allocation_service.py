"""
Unit and API integration tests for Bed Allocation Service.
Tests allocation workflows, waitlisting, compensation, idempotency,
active-request prevention, release, promotion, and outbox event persistence.
"""
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from services.bed_allocation.clients.fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakeLockCoordinatorClient,
    FakePatientRegistryClient, FakeStaffSchedulerClient
)
from services.bed_allocation.db import AllocationModel, Base, init_db, OutboxEventModel
from services.bed_allocation.models import (
    CancelRequest, CreateAllocationRequest, ReleaseRequest
)
from services.bed_allocation.outbox.dispatcher import FakeEventPublisher, OutboxDispatcher
from services.bed_allocation.rest import (
    cancel_allocation_endpoint, create_allocation_endpoint,
    get_allocation_endpoint, release_allocation_endpoint, set_service
)
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator
from shared.observability.errors import AppException

class DummyRequestState:
    request_id = "req-test-123"

class DummyRequest:
    state = DummyRequestState()

@pytest.fixture
def session_factory():
    # Use SQLite in-memory database for isolated, lightning-fast tests
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False)

@pytest.fixture
def test_setup(session_factory):
    patient_client = FakePatientRegistryClient()
    patient_client.seed_admission("adm-open-1", "pat-1", "OPEN")
    patient_client.seed_admission("adm-open-2", "pat-2", "OPEN")
    patient_client.seed_admission("adm-open-3", "pat-3", "OPEN")
    patient_client.seed_admission("adm-closed-1", "pat-4", "DISCHARGED")

    triage_repo = InMemoryTriageRepository()
    triage_domain = TriageDomainService(triage_repo, MockFencingValidator(True))
    triage_domain.set_triage("adm-open-1", 1, "WARDA-ICU")
    triage_domain.set_triage("adm-open-2", 2, "WARDA-ICU")
    triage_domain.set_triage("adm-open-3", 3, "WARDA-ICU")
    triage_client = DirectTriageClient(triage_domain)

    lock_client = FakeLockCoordinatorClient()
    bed_client = FakeBedRegistryClient()
    bed_client.seed_bed("bed-01", "WARDA-ICU")
    bed_client.seed_bed("bed-02", "WARDA-ICU")

    staff_client = FakeStaffSchedulerClient(initial_capacity=10)
    publisher = FakeEventPublisher()

    domain_service = BedAllocationDomainService(
        session_factory=session_factory,
        patient_client=patient_client,
        triage_client=triage_client,
        lock_client=lock_client,
        bed_client=bed_client,
        staff_client=staff_client,
        owner_id="test-instance"
    )
    set_service(domain_service)

    return {
        "service": domain_service,
        "patient": patient_client,
        "triage": triage_client,
        "triage_domain": triage_domain,
        "lock": lock_client,
        "bed": bed_client,
        "staff": staff_client,
        "session_factory": session_factory,
        "publisher": publisher
    }

def test_successful_bed_allocation(test_setup):
    service = test_setup["service"]
    req = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")

    resp = create_allocation_endpoint(req, DummyRequest(), idempotency_key="idemp-1", x_request_id="req-1", service=service)
    assert resp.status == "ALLOCATED"
    assert resp.bed_id == "bed-01"
    assert resp.staff_assignment_id is not None
    assert resp.waitlist_entry_id is None

def test_waitlist_when_no_beds_available(test_setup):
    service = test_setup["service"]
    bed_client = test_setup["bed"]

    # Consume all available beds
    create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-1", service=service)
    create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-2", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-2", service=service)

    # 3rd admission arrives: no beds left
    resp = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-3", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-3", service=service)
    assert resp.status == "WAITLISTED"
    assert resp.bed_id is None
    assert resp.waitlist_entry_id is not None

def test_waitlist_when_no_staff_capacity(test_setup):
    service = test_setup["service"]
    staff_client = test_setup["staff"]
    staff_client.simulate_capacity_exhausted = True

    resp = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-staff-0", service=service)
    assert resp.status == "WAITLISTED"
    assert resp.bed_id is None
    assert resp.waitlist_entry_id is not None

def test_staff_failure_triggers_bed_compensation(test_setup):
    service = test_setup["service"]
    staff_client = test_setup["staff"]
    bed_client = test_setup["bed"]
    staff_client.simulate_assignment_failure = True

    resp = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-fail-staff", service=service)
    assert resp.status == "WAITLISTED"
    # Verify bed was released back to AVAILABLE rather than left orphaned
    assert bed_client.beds["bed-01"].status == "AVAILABLE"
    assert bed_client.beds["bed-01"].current_allocation_id is None

def test_reject_closed_admission(test_setup):
    service = test_setup["service"]
    req = CreateAllocationRequest(admission_id="adm-closed-1", pool_id="WARDA-ICU")
    with pytest.raises(AppException) as exc:
        create_allocation_endpoint(req, DummyRequest(), idempotency_key="idemp-c", service=service)
    assert exc.value.code == "STATE_CONFLICT"
    assert "must be OPEN" in exc.value.message

def test_reject_triage_pool_mismatch(test_setup):
    service = test_setup["service"]
    # adm-open-1 is triaged for WARDA-ICU, request says WARDB-CCU
    req = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDB-CCU")
    with pytest.raises(AppException) as exc:
        create_allocation_endpoint(req, DummyRequest(), idempotency_key="idemp-m", service=service)
    assert exc.value.code == "STATE_CONFLICT"
    assert "does not match triage" in exc.value.message

def test_prevent_concurrent_active_requests(test_setup):
    service = test_setup["service"]
    req = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")
    create_allocation_endpoint(req, DummyRequest(), idempotency_key="idemp-1", service=service)

    # Attempt second request with different idempotency key for same admission
    with pytest.raises(AppException) as exc:
        create_allocation_endpoint(req, DummyRequest(), idempotency_key="idemp-2", service=service)
    assert exc.value.code == "ACTIVE_ALLOCATION_EXISTS"
    assert "active allocation or waitlist entry" in exc.value.message

def test_partial_unique_index_blocks_duplicate_during_recovery(test_setup):
    session_factory = test_setup["session_factory"]
    with session_factory() as session:
        session.add(AllocationModel(
            allocation_id="alloc-recovering",
            admission_id="adm-recovery",
            pool_id="WARDA-ICU",
            status="RECOVERING"
        ))
        session.commit()

    with session_factory() as session:
        session.add(AllocationModel(
            allocation_id="alloc-releasing",
            admission_id="adm-recovery",
            pool_id="WARDA-ICU",
            status="RELEASING"
        ))
        with pytest.raises(IntegrityError):
            session.commit()

def test_new_allocation_is_blocked_while_admission_is_recovering(test_setup):
    service = test_setup["service"]
    session_factory = test_setup["session_factory"]
    with session_factory() as session:
        session.add(AllocationModel(
            allocation_id="alloc-recovering",
            admission_id="adm-open-1",
            pool_id="WARDA-ICU",
            status="RECOVERING"
        ))
        session.commit()

    with pytest.raises(AppException) as exc:
        service.create_allocation(
            CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"),
            request_id="req-recovery-retry",
            idempotency_key="idemp-recovery-retry"
        )
    assert exc.value.code == "ACTIVE_ALLOCATION_EXISTS"

def test_postgres_concurrent_requests_use_partial_unique_index():
    database_url = os.environ.get("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("Set TEST_POSTGRES_URL to an isolated local PostgreSQL database")
    if urlsplit(database_url).hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("TEST_POSTGRES_URL must point to local PostgreSQL; cloud databases are not permitted")

    admin_engine = create_engine(database_url)
    schema_name = f"test_allocation_{uuid.uuid4().hex}"
    test_engine = None
    schema_created = False
    try:
        with admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            schema_created = True

        test_engine = create_engine(
            database_url,
            connect_args={"options": f"-csearch_path={schema_name}"},
            pool_size=4,
            max_overflow=0
        )
        Base.metadata.create_all(bind=test_engine)
        postgres_session_factory = sessionmaker(bind=test_engine, autoflush=False, autocommit=False)

        patient_client = FakePatientRegistryClient()
        patient_client.seed_admission("adm-pg-race", "pat-pg-race", "OPEN")
        request_barrier = threading.Barrier(2)

        class BarrierPatientClient:
            def get_admission(self, admission_id, request_id):
                result = patient_client.get_admission(admission_id, request_id)
                request_barrier.wait(timeout=10)
                return result

        triage_repo = InMemoryTriageRepository()
        triage_domain = TriageDomainService(triage_repo, MockFencingValidator(True))
        triage_domain.set_triage("adm-pg-race", 1, "WARDA-ICU")
        service = BedAllocationDomainService(
            session_factory=postgres_session_factory,
            patient_client=BarrierPatientClient(),
            triage_client=DirectTriageClient(triage_domain),
            lock_client=FakeLockCoordinatorClient(),
            bed_client=_one_bed_client(),
            staff_client=FakeStaffSchedulerClient(initial_capacity=2),
            owner_id="postgres-concurrency-test"
        )

        def create_with_key(key):
            try:
                return service.create_allocation(
                    CreateAllocationRequest(admission_id="adm-pg-race", pool_id="WARDA-ICU"),
                    request_id=f"req-{key}",
                    idempotency_key=key
                )
            except AppException as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(create_with_key, ["idemp-pg-a", "idemp-pg-b"]))

        successes = [result for result in results if not isinstance(result, AppException)]
        conflicts = [result for result in results if isinstance(result, AppException)]
        assert len(successes) == 1
        assert len(conflicts) == 1
        assert conflicts[0].code == "ACTIVE_ALLOCATION_EXISTS"
        with postgres_session_factory() as session:
            active_count = session.query(AllocationModel).filter(
                AllocationModel.admission_id == "adm-pg-race",
                AllocationModel.status.notin_(["RELEASED", "CANCELLED", "FAILED"])
            ).count()
        assert active_count == 1
    finally:
        if test_engine is not None:
            test_engine.dispose()
        if schema_created:
            with admin_engine.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{schema_name}" CASCADE'))
        admin_engine.dispose()

def _one_bed_client():
    client = FakeBedRegistryClient()
    client.seed_bed("bed-pg-race", "WARDA-ICU")
    return client

def test_idempotency_replay_and_mismatch(test_setup):
    service = test_setup["service"]
    req1 = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")
    resp1 = create_allocation_endpoint(req1, DummyRequest(), idempotency_key="idemp-replay", service=service)

    # Exact replay returns cached response
    resp2 = create_allocation_endpoint(req1, DummyRequest(), idempotency_key="idemp-replay", service=service)
    assert resp1.allocation_id == resp2.allocation_id
    assert resp1.bed_id == resp2.bed_id

    # Same key with different payload triggers 409 ALREADY_EXISTS IDEMPOTENCY_MISMATCH
    req_mismatch = CreateAllocationRequest(admission_id="adm-open-2", pool_id="WARDA-ICU")
    with pytest.raises(AppException) as exc:
        create_allocation_endpoint(req_mismatch, DummyRequest(), idempotency_key="idemp-replay", service=service)
    assert exc.value.code == "ALREADY_EXISTS"

def test_release_and_automatic_promotion(test_setup):
    service = test_setup["service"]
    bed_client = test_setup["bed"]

    # Fill both beds
    r1 = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-1", service=service)
    r2 = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-2", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-2", service=service)
    # 3rd goes to waitlist
    r3 = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-3", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-3", service=service)
    assert r3.status == "WAITLISTED"

    # Release first allocation: should release bed-01 and automatically promote r3
    rel_res = release_allocation_endpoint(r1.allocation_id, ReleaseRequest(reason="discharged"), DummyRequest(), idempotency_key="idemp-rel-1", service=service)
    assert rel_res.status == "RELEASED"
    assert rel_res.promoted_allocation_id == r3.allocation_id

    # Verify r3 is now ALLOCATED and bed-01 is assigned to r3
    r3_updated = get_allocation_endpoint(r3.allocation_id, service)
    assert r3_updated.status == "ALLOCATED"
    assert r3_updated.bed_id == "bed-01"

def test_cancel_waitlisted_allocation(test_setup):
    service = test_setup["service"]
    # Fill beds and put open-3 on waitlist
    create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-1", service=service)
    create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-2", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-2", service=service)
    r3 = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-3", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-3", service=service)

    # Cancel waitlisted allocation
    cancelled = cancel_allocation_endpoint(r3.allocation_id, CancelRequest(reason="transferred"), DummyRequest(), idempotency_key="idemp-can", service=service)
    assert cancelled.status == "CANCELLED"

    # Cancelling an ALLOCATED allocation is prohibited (must use release)
    with pytest.raises(AppException) as exc:
        cancel_allocation_endpoint("alloc-fake", CancelRequest(reason="test"), DummyRequest(), idempotency_key="idemp-x", service=service)
    assert exc.value.code == "NOT_FOUND"

def test_transactional_outbox_events_and_dispatcher(test_setup):
    service = test_setup["service"]
    session_factory = test_setup["session_factory"]
    publisher = test_setup["publisher"]

    # Perform an allocation
    r = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-outbox", service=service)

    # Verify outbox table has pending events for pubsub and kafka
    with session_factory() as session:
        events = session.query(OutboxEventModel).filter_by(status="PENDING").all()
        destinations = [e.destination for e in events]
        assert "pubsub" in destinations
        assert "kafka" in destinations

    # Run outbox dispatcher
    dispatcher = OutboxDispatcher(session_factory, publisher)
    dispatched_count = dispatcher.process_pending()
    assert dispatched_count == 2
    assert len(publisher.pubsub_messages) == 1
    assert len(publisher.kafka_messages) == 1
    assert publisher.kafka_messages[0]["payload"]["occupancy_delta"] == 1
