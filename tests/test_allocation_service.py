"""
Unit and API integration tests for Bed Allocation Service.
Tests allocation workflows, waitlisting, compensation, idempotency,
active-request prevention, release, promotion, and outbox event persistence.
"""
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from fastapi import Response
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakeLockCoordinatorClient,
    FakePatientRegistryClient, FakeStaffSchedulerClient
)
from services.bed_allocation.db import AllocationModel, Base, IdempotencyModel, init_db, OperationRecoveryModel, OutboxEventModel
from services.bed_allocation.models import (
    CancelRequest, CreateAllocationRequest, ReleaseRequest
)
from services.bed_allocation.outbox.dispatcher import OutboxDispatcher
from tests.support.event_publisher import FakeEventPublisher
from services.bed_allocation.rest import (
    cancel_allocation_endpoint, create_allocation_endpoint,
    get_allocation_endpoint, release_allocation_endpoint, set_service
)
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from tests.support.triage_fakes import MockFencingValidator
from shared.observability.errors import AppException

class DummyRequestState:
    request_id = "req-test-123"

class DummyRequest:
    state = DummyRequestState()

@pytest.fixture
def session_factory():
    # Use SQLite in-memory database for isolated, lightning-fast tests
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
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

def test_uncertain_staff_failure_does_not_become_waitlisted(test_setup):
    service = test_setup["service"]
    staff_client = test_setup["staff"]
    bed_client = test_setup["bed"]
    staff_client.simulate_assignment_failure = True

    resp = create_allocation_endpoint(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), DummyRequest(), idempotency_key="idemp-fail-staff", service=service)
    assert resp.status == "RECOVERING"
    assert resp.pending_action == "ASSIGN_STAFF"
    # Capacity remained available, so this was not a confirmed shortage. Keep
    # the owned bed reserved until the stable staff operation can be reconciled.
    assert bed_client.beds["bed-01"].current_allocation_id == resp.allocation_id


def test_matching_retry_during_processing_returns_202_and_same_allocation(test_setup):
    service = test_setup["service"]
    bed_client = test_setup["bed"]
    entered = threading.Event()
    resume = threading.Event()
    original_allocate = bed_client.allocate_bed

    def pause_after_claim(context, bed_id, allocation_id):
        entered.set()
        assert resume.wait(timeout=5)
        return original_allocate(context, bed_id, allocation_id)

    bed_client.allocate_bed = pause_after_claim
    req = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")
    with ThreadPoolExecutor(max_workers=1) as executor:
        first_call = executor.submit(
            create_allocation_endpoint, req, DummyRequest(),
            idempotency_key="idemp-processing", service=service,
        )
        assert entered.wait(timeout=5)
        response = Response()
        retry = create_allocation_endpoint(
            req, DummyRequest(), idempotency_key="idemp-processing", service=service, response=response
        )
        assert response.status_code == 202
        assert response.headers["Location"] == f"/api/v1/allocations/{retry.allocation_id}"
        assert retry.status == "PROCESSING"
        resume.set()
        completed = first_call.result(timeout=5)
    assert completed.allocation_id == retry.allocation_id
    assert get_allocation_endpoint(completed.allocation_id, service).status == "ALLOCATED"


def test_recovery_replays_uncertain_staff_operation_after_service_recreation(test_setup):
    original = test_setup["service"]
    staff = test_setup["staff"]
    staff.simulate_uncertain_outcome = True
    request = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")
    accepted = original.create_allocation(request, "req-uncertain", "idemp-uncertain")
    assert accepted.status == "RECOVERING"
    assert accepted.pending_action == "ASSIGN_STAFF"

    staff.simulate_uncertain_outcome = False
    restarted = BedAllocationDomainService(
        session_factory=original.session_factory,
        patient_client=original.patient_client,
        triage_client=original.triage_client,
        lock_client=original.lock_client,
        bed_client=original.bed_client,
        staff_client=staff,
        owner_id="restarted-allocation-instance",
    )
    assert restarted.recover_pending() == 1
    current = restarted.get_allocation(accepted.allocation_id)
    assert current.status == "ALLOCATED"
    assert current.staff_assignment_id in staff.assignments
    with original.session_factory() as session:
        claim = session.query(IdempotencyModel).filter_by(
            method="POST", operation="CREATE", target_id="adm-open-1", idempotency_key="idemp-uncertain"
        ).one()
        assert claim.state == "COMPLETED"
        operation = session.query(OperationRecoveryModel).filter_by(
            operation_id=f"{accepted.allocation_id}:assign-staff"
        ).one()
        assert operation.status == "SUCCEEDED"


def test_recovery_after_final_commit_crash_does_not_repeat_remote_effects(test_setup):
    service = test_setup["service"]
    request = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU")
    original_finalize = service._finalize_create

    def crash_before_finalize(*args, **kwargs):
        raise RuntimeError("injected process crash before final transaction")

    service._finalize_create = crash_before_finalize
    with pytest.raises(RuntimeError):
        service.create_allocation(request, "req-crash-final", "idemp-crash-final")
    service._finalize_create = original_finalize

    with service.session_factory() as session:
        interrupted = session.query(AllocationModel).filter_by(admission_id="adm-open-1").one()
        interrupted_allocation_id = interrupted.allocation_id
        assert interrupted.status == "PROCESSING"
        assert interrupted.pending_action == "FINALIZE_ALLOCATED"
        interrupted.recovery_claim_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        session.commit()

    restarted = BedAllocationDomainService(
        session_factory=service.session_factory,
        patient_client=service.patient_client,
        triage_client=service.triage_client,
        lock_client=service.lock_client,
        bed_client=service.bed_client,
        staff_client=service.staff_client,
        owner_id="restarted-finalizer",
    )
    assert restarted.recover_pending() == 1
    current = restarted.get_allocation(interrupted_allocation_id)
    assert current.status == "ALLOCATED"
    assert len(service.staff_client.assignments) == 1
    assert service.bed_client.beds[current.bed_id].current_allocation_id == current.allocation_id

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
        patient_client.seed_admission("adm-pg-replay", "pat-pg-replay", "OPEN")
        patient_client.seed_admission("adm-pg-worker", "pat-pg-worker", "OPEN")
        request_barrier = threading.Barrier(2)

        class BarrierPatientClient:
            def get_admission(self, admission_id, request_id):
                result = patient_client.get_admission(admission_id, request_id)
                request_barrier.wait(timeout=10)
                return result

        triage_repo = InMemoryTriageRepository()
        triage_domain = TriageDomainService(triage_repo, MockFencingValidator(True))
        triage_domain.set_triage("adm-pg-race", 1, "WARDA-ICU")
        triage_domain.set_triage("adm-pg-replay", 1, "WARDA-ICU")
        triage_domain.set_triage("adm-pg-worker", 1, "WARDA-ICU")
        lock_client = FakeLockCoordinatorClient()
        lock_calls = [0]
        original_acquire = lock_client.acquire
        def count_acquire(*args, **kwargs):
            lock_calls[0] += 1
            return original_acquire(*args, **kwargs)
        lock_client.acquire = count_acquire
        bed_client = FakeBedRegistryClient()
        bed_client.seed_bed("bed-pg-race-1", "WARDA-ICU")
        bed_client.seed_bed("bed-pg-race-2", "WARDA-ICU")
        bed_client.seed_bed("bed-pg-worker", "WARDA-ICU")
        staff_client = FakeStaffSchedulerClient(initial_capacity=3)
        service = BedAllocationDomainService(
            session_factory=postgres_session_factory,
            patient_client=BarrierPatientClient(),
            triage_client=DirectTriageClient(triage_domain),
            lock_client=lock_client,
            bed_client=bed_client,
            staff_client=staff_client,
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

        # Matching concurrent retries share the scoped durable claim and one
        # workflow, returning the same allocation ID to both callers.
        request_barrier = threading.Barrier(2)
        start_lock_count = lock_calls[0]
        with ThreadPoolExecutor(max_workers=2) as executor:
            replay_results = list(executor.map(lambda _: service.create_allocation(
                CreateAllocationRequest(admission_id="adm-pg-replay", pool_id="WARDA-ICU"),
                request_id="req-pg-replay",
                idempotency_key="idemp-pg-replay",
            ), range(2)))
        assert all(not isinstance(result, AppException) for result in replay_results)
        assert replay_results[0].allocation_id == replay_results[1].allocation_id
        assert lock_calls[0] == start_lock_count + 1

        # A matching scope with a changed payload conflicts before another
        # workflow starts.
        with pytest.raises(AppException) as mismatch:
            service.create_allocation(
                CreateAllocationRequest(admission_id="adm-pg-replay", pool_id="OTHER"),
                request_id="req-pg-mismatch",
                idempotency_key="idemp-pg-replay",
            )
        assert mismatch.value.status_code == 409
        assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"

        # Two workers race for one expired PostgreSQL lease. SKIP LOCKED lets
        # one own the recovery; the downstream operation remains deduplicated.
        request_barrier = threading.Barrier(1)
        staff_client.simulate_uncertain_outcome = True
        interrupted = service.create_allocation(
            CreateAllocationRequest(admission_id="adm-pg-worker", pool_id="WARDA-ICU"),
            request_id="req-pg-worker", idempotency_key="idemp-pg-worker",
        )
        assert interrupted.status == "RECOVERING"
        staff_client.simulate_uncertain_outcome = False
        with postgres_session_factory() as session:
            row = session.query(AllocationModel).filter_by(allocation_id=interrupted.allocation_id).one()
            row.recovery_claim_until = datetime.now(timezone.utc) - timedelta(seconds=1)
            session.commit()
        second_worker = BedAllocationDomainService(
            session_factory=postgres_session_factory,
            patient_client=service.patient_client, triage_client=service.triage_client,
            lock_client=lock_client, bed_client=bed_client, staff_client=staff_client,
            owner_id="postgres-recovery-worker-2",
        )
        with ThreadPoolExecutor(max_workers=2) as executor:
            claims = list(executor.map(lambda worker: worker.recover_pending(), (service, second_worker)))
        assert sum(claims) == 1
        assert second_worker.get_allocation(interrupted.allocation_id).status == "ALLOCATED"
        assert sum(1 for allocation_id, _ in staff_client.assignments.values() if allocation_id == interrupted.allocation_id) == 1

        staff_client.simulate_uncertain_release = True
        uncertain_release = service.release_allocation(interrupted.allocation_id, "done", "req-pg-release-uncertain", "idemp-pg-release-uncertain")
        assert uncertain_release.status == "RECOVERING"
        assert bed_client.beds[second_worker.get_allocation(interrupted.allocation_id).bed_id].current_allocation_id is None
        assert any(allocation_id == interrupted.allocation_id for allocation_id, _ in staff_client.assignments.values())
        staff_client.simulate_uncertain_release = False
        with ThreadPoolExecutor(max_workers=2) as executor:
            release_claims = list(executor.map(lambda worker: worker.recover_pending(), (service, second_worker)))
        assert sum(release_claims) == 1
        assert second_worker.get_allocation(interrupted.allocation_id).status == "RELEASED"
        assert not any(allocation_id == interrupted.allocation_id for allocation_id, _ in staff_client.assignments.values())

        # PostgreSQL-backed release commits before a failed promotion. The
        # waitlist stays intact and a later worker can finish the promotion.
        patient_client.seed_admission("adm-pg-wait", "pat-pg-wait", "OPEN")
        triage_domain.set_triage("adm-pg-wait", 1, "WARDA-ICU")
        staff_client.simulate_capacity_exhausted = True
        waitlisted = service.create_allocation(
            CreateAllocationRequest(admission_id="adm-pg-wait", pool_id="WARDA-ICU"),
            request_id="req-pg-wait", idempotency_key="idemp-pg-wait",
        )
        assert waitlisted.status == "WAITLISTED"
        staff_client.simulate_capacity_exhausted = False
        staff_client.simulate_assignment_failure = True
        promoted_from = successes[0].allocation_id
        released = service.release_allocation(promoted_from, "done", "req-pg-release", "idemp-pg-release")
        assert released.status == "RELEASED"
        assert second_worker.get_allocation(promoted_from).status == "RELEASED"
        assert triage_domain.peek_next("WARDA-ICU").allocation_id == waitlisted.allocation_id
        staff_client.simulate_assignment_failure = False
        assert second_worker.recover_pending() == 1
        assert second_worker.get_allocation(waitlisted.allocation_id).status == "ALLOCATED"
        assert triage_domain.peek_next("WARDA-ICU") is None

        patient_client.seed_admission("adm-pg-release-target", "pat-pg-release-target", "OPEN")
        triage_domain.set_triage("adm-pg-release-target", 1, "WARDA-ICU")
        staff_client.set_capacity("WARDA-ICU", 5)
        another = service.create_allocation(
            CreateAllocationRequest(admission_id="adm-pg-release-target", pool_id="WARDA-ICU"),
            request_id="req-pg-release-target", idempotency_key="idemp-pg-release-target",
        )
        release_entered, release_resume = threading.Event(), threading.Event()
        original_release_assignment = staff_client.release_assignment
        def pause_staff_release(*args, **kwargs):
            release_entered.set()
            assert release_resume.wait(timeout=5)
            return original_release_assignment(*args, **kwargs)
        staff_client.release_assignment = pause_staff_release
        with ThreadPoolExecutor(max_workers=1) as executor:
            release_future = executor.submit(
                service.release_allocation, another.allocation_id, "done", "req-pg-release-target", "idemp-pg-release"
            )
            assert release_entered.wait(timeout=5)
            replay_response = Response()
            pending_replay = release_allocation_endpoint(
                another.allocation_id, ReleaseRequest(reason="done"), DummyRequest(),
                idempotency_key="idemp-pg-release", service=service, response=replay_response,
            )
            assert replay_response.status_code == 202
            assert pending_replay.status == "RELEASING"
            with pytest.raises(AppException) as mismatch:
                service.release_allocation(another.allocation_id, "different", "req-pg-mismatch", "idemp-pg-release")
            assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"
            with pytest.raises(AppException) as competing:
                service.release_allocation(another.allocation_id, "done", "req-pg-other-key", "idemp-pg-other-key")
            assert competing.value.status_code == 409
            release_resume.set()
            second_release = release_future.result(timeout=5)
        staff_client.release_assignment = original_release_assignment
        assert second_release.status == "RELEASED"

        patient_client.seed_admission("adm-pg-release-target-2", "pat-pg-release-target-2", "OPEN")
        triage_domain.set_triage("adm-pg-release-target-2", 1, "WARDA-ICU")
        another_target = service.create_allocation(
            CreateAllocationRequest(admission_id="adm-pg-release-target-2", pool_id="WARDA-ICU"),
            request_id="req-pg-release-target-2", idempotency_key="idemp-pg-release-target-2",
        )
        service.release_allocation(another_target.allocation_id, "done", "req-pg-release-target-2", "idemp-pg-release")
        with postgres_session_factory() as session:
            assert session.query(IdempotencyModel).filter_by(operation="RELEASE", idempotency_key="idemp-pg-release").count() == 3

        patient_client.seed_admission("adm-pg-cancel", "pat-pg-cancel", "OPEN")
        triage_domain.set_triage("adm-pg-cancel", 1, "WARDA-ICU")
        staff_client.simulate_capacity_exhausted = True
        cancel_target = service.create_allocation(
            CreateAllocationRequest(admission_id="adm-pg-cancel", pool_id="WARDA-ICU"),
            request_id="req-pg-cancel", idempotency_key="idemp-pg-cancel-create",
        )
        assert cancel_target.status == "WAITLISTED"
        cancelled = service.cancel_allocation(cancel_target.allocation_id, "transfer", "req-pg-cancel", "idemp-pg-cancel")
        assert cancelled.status == "CANCELLED"
        assert service.cancel_allocation(cancel_target.allocation_id, "transfer", "req-pg-cancel-retry", "idemp-pg-cancel").status == "CANCELLED"
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

    # Same scope/key with different payload triggers 409 IDEMPOTENCY_MISMATCH.
    req_mismatch = CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDB-CCU")
    with pytest.raises(AppException) as exc:
        create_allocation_endpoint(req_mismatch, DummyRequest(), idempotency_key="idemp-replay", service=service)
    assert exc.value.code == "IDEMPOTENCY_MISMATCH"

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

def test_release_claim_replay_mismatch_and_target_isolation(test_setup):
    service = test_setup["service"]
    request = lambda admission: CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU")
    first = service.create_allocation(request("adm-open-1"), "r1", "create-r1")
    second = service.create_allocation(request("adm-open-2"), "r2", "create-r2")
    released = service.release_allocation(first.allocation_id, "done", "rr1", "shared-release-key")
    replay = service.release_allocation(first.allocation_id, "done", "rr2", "shared-release-key")
    assert replay.model_dump() == released.model_dump()
    with pytest.raises(AppException) as mismatch:
        service.release_allocation(first.allocation_id, "different", "rr3", "shared-release-key")
    assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"
    # The target ID is part of scope, so this key is independent for another allocation.
    other = service.release_allocation(second.allocation_id, "done", "rr4", "shared-release-key")
    assert other.allocation_id == second.allocation_id
    with test_setup["session_factory"]() as session:
        assert session.query(IdempotencyModel).filter_by(operation="RELEASE", target_id=first.allocation_id, idempotency_key="shared-release-key").count() == 1
        assert session.query(IdempotencyModel).filter_by(operation="RELEASE", target_id=second.allocation_id, idempotency_key="shared-release-key").count() == 1

def test_release_matching_in_progress_retry_returns_202(test_setup):
    service = test_setup["service"]
    allocation = service.create_allocation(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), "r1", "create-r1")
    staff = test_setup["staff"]
    entered, resume = threading.Event(), threading.Event()
    original = staff.release_assignment
    def pause_release(*args, **kwargs):
        entered.set()
        assert resume.wait(timeout=5)
        return original(*args, **kwargs)
    staff.release_assignment = pause_release
    with ThreadPoolExecutor(max_workers=1) as executor:
        first_call = executor.submit(service.release_allocation, allocation.allocation_id, "done", "rel-1", "rel-processing")
        assert entered.wait(timeout=5)
        response = Response()
        retry = release_allocation_endpoint(allocation.allocation_id, ReleaseRequest(reason="done"), DummyRequest(), idempotency_key="rel-processing", service=service, response=response)
        assert response.status_code == 202
        assert response.headers["Location"] == f"/api/v1/allocations/{allocation.allocation_id}"
        assert retry.status == "RELEASING"
        resume.set()
        result = first_call.result(timeout=5)
    assert result.status == "RELEASED"

def test_release_recovers_when_bed_released_but_staff_outcome_unknown(test_setup):
    service = test_setup["service"]
    allocation = service.create_allocation(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), "r1", "create-r1")
    staff, beds = test_setup["staff"], test_setup["bed"]
    staff.simulate_uncertain_release = True
    accepted = service.release_allocation(allocation.allocation_id, "done", "release-unknown", "release-unknown-key")
    assert accepted.status == "RECOVERING"
    assert beds.beds[allocation.bed_id].current_allocation_id is None
    assert allocation.staff_assignment_id in staff.assignments
    staff.simulate_uncertain_release = False
    restarted = BedAllocationDomainService(
        service.session_factory, service.patient_client, service.triage_client,
        service.lock_client, beds, staff, owner_id="release-recovery-instance",
    )
    assert restarted.recover_pending() == 1
    current = restarted.get_allocation(allocation.allocation_id)
    assert current.status == "RELEASED"
    assert allocation.staff_assignment_id not in staff.assignments

def test_release_commit_survives_promotion_failure_and_preserves_queue(test_setup):
    service = test_setup["service"]
    create = lambda admission, key: service.create_allocation(CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU"), key, key)
    first = create("adm-open-1", "first")
    create("adm-open-2", "second")
    waiting = create("adm-open-3", "third")
    staff = test_setup["staff"]
    staff.simulate_assignment_failure = True
    released = service.release_allocation(first.allocation_id, "done", "release-promo", "release-promo-key")
    assert released.status == "RELEASED"
    assert service.get_allocation(first.allocation_id).status == "RELEASED"
    assert test_setup["triage_domain"].peek_next("WARDA-ICU").allocation_id == waiting.allocation_id
    assert test_setup["bed"].beds[first.bed_id].current_allocation_id is None
    with service.session_factory() as session:
        release = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        assert release.pending_action == "PROMOTE"
        assert session.query(IdempotencyModel).filter_by(operation="RELEASE", target_id=first.allocation_id, idempotency_key="release-promo-key").one().state == "COMPLETED"
    staff.simulate_assignment_failure = False
    assert service.recover_pending() == 1
    assert service.get_allocation(waiting.allocation_id).status == "ALLOCATED"
    assert test_setup["triage_domain"].peek_next("WARDA-ICU") is None

def test_unknown_triage_remove_outcome_is_left_for_reconciliation(test_setup):
    service = test_setup["service"]
    create = lambda admission, key: service.create_allocation(CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU"), key, key)
    first = create("adm-open-1", "unknown-remove-first")
    create("adm-open-2", "unknown-remove-second")
    waiting = create("adm-open-3", "unknown-remove-waiting")
    triage = test_setup["triage"]

    def unknown_remove(*args, **kwargs):
        raise AppException("OUTCOME_UNKNOWN", "queue operation requires reconciliation", 503, retryable=True)

    triage.remove_entry = unknown_remove
    assert service.release_allocation(first.allocation_id, "done", "unknown-remove-release", "unknown-remove-key").status == "RELEASED"
    with service.session_factory() as session:
        release = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        workflow = __import__("json").loads(release.workflow_data)
        assert release.status == "RELEASED"
        assert release.pending_action == "RECONCILIATION_REQUIRED"
        assert workflow["reconciliation_needed"] is True
        assert workflow["reconciliation_reason"] == "TRIAGE_OUTCOME_UNKNOWN"
    assert triage.service.peek_next("WARDA-ICU").allocation_id == waiting.allocation_id
    assert service.recover_pending() == 0

def test_cancel_claim_is_durable_before_queue_removal_and_retryable(test_setup):
    service = test_setup["service"]
    create = lambda admission, key: service.create_allocation(CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU"), key, key)
    create("adm-open-1", "first")
    create("adm-open-2", "second")
    waiting = create("adm-open-3", "third")
    triage = test_setup["triage"]
    entered, resume = threading.Event(), threading.Event()
    original = triage.remove_entry
    def pause_remove(*args, **kwargs):
        with service.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=waiting.allocation_id).one()
            claim = session.query(IdempotencyModel).filter_by(operation="CANCEL", target_id=waiting.allocation_id, idempotency_key="cancel-key").one()
            assert record.status == "CANCELLING"
            assert claim.state == "IN_PROGRESS"
        entered.set()
        assert resume.wait(timeout=5)
        return original(*args, **kwargs)
    triage.remove_entry = pause_remove
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            cancel_allocation_endpoint, waiting.allocation_id, CancelRequest(reason="transfer"), DummyRequest(),
            idempotency_key="cancel-key", service=service,
        )
        assert entered.wait(timeout=5)
        response = Response()
        replay = cancel_allocation_endpoint(
            waiting.allocation_id, CancelRequest(reason="transfer"), DummyRequest(),
            idempotency_key="cancel-key", service=service, response=response,
        )
        assert replay.status == "CANCELLING"
        assert response.status_code == 202
        assert response.headers["Location"] == f"/api/v1/allocations/{waiting.allocation_id}"
        resume.set()
        cancelled = future.result(timeout=5)
    triage.remove_entry = original
    assert cancelled.status == "CANCELLED"
    assert service.cancel_allocation(waiting.allocation_id, "transfer", "cancel-r2", "cancel-key").status == "CANCELLED"

def test_cancel_recovers_after_unknown_remove_outcome(test_setup):
    service = test_setup["service"]
    create = lambda admission, key: service.create_allocation(CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU"), key, key)
    create("adm-open-1", "first")
    create("adm-open-2", "second")
    waiting = create("adm-open-3", "third")
    triage = test_setup["triage"]
    original = triage.remove_entry
    triage.remove_entry = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("injected unknown remove result"))
    accepted = service.cancel_allocation(waiting.allocation_id, "transfer", "cancel-unknown", "cancel-unknown-key")
    triage.remove_entry = original
    assert accepted.status == "RECOVERING"
    with service.session_factory() as session:
        claim = session.query(IdempotencyModel).filter_by(operation="CANCEL", target_id=waiting.allocation_id, idempotency_key="cancel-unknown-key").one()
        assert claim.state == "IN_PROGRESS"
    assert service.recover_pending() == 1
    assert service.get_allocation(waiting.allocation_id).status == "CANCELLED"
    assert test_setup["triage_domain"].peek_next("WARDA-ICU") is None

def test_crash_after_release_commit_keeps_release_and_promotion_claimable(test_setup):
    service = test_setup["service"]
    first = service.create_allocation(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), "r1", "create-r1")
    original = service._promote_next_locked
    def crash(*args, **kwargs):
        raise RuntimeError("injected crash after release commit")
    service._promote_next_locked = crash
    result = service.release_allocation(first.allocation_id, "done", "r-release", "release-crash")
    service._promote_next_locked = original
    assert result.status == "RELEASED"
    with service.session_factory() as session:
        record = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        assert record.status == "RELEASED"
        assert record.pending_action == "PROMOTE"
    assert service.recover_pending() == 1
    assert service.get_allocation(first.allocation_id).status == "RELEASED"

def test_release_retries_after_remote_effect_before_outcome_commit(test_setup):
    service = test_setup["service"]
    allocation = service.create_allocation(CreateAllocationRequest(admission_id="adm-open-1", pool_id="WARDA-ICU"), "r1", "create-r1")
    beds = test_setup["bed"]
    original = beds.release_bed
    did_effect = {"value": False}
    def effect_then_timeout(*args, **kwargs):
        result = original(*args, **kwargs)
        if not did_effect["value"]:
            did_effect["value"] = True
            raise RuntimeError("injected lost acknowledgement after bed release")
        return result
    beds.release_bed = effect_then_timeout
    accepted = service.release_allocation(allocation.allocation_id, "done", "r-release", "release-effect-crash")
    assert accepted.status == "RECOVERING"
    assert beds.beds[allocation.bed_id].current_allocation_id is None
    beds.release_bed = original
    assert service.recover_pending() == 1
    assert service.get_allocation(allocation.allocation_id).status == "RELEASED"
    assert did_effect["value"]

def test_promotion_compensation_failure_keeps_queue_and_releases_only_owned_bed(test_setup):
    service = test_setup["service"]
    create = lambda admission, key: service.create_allocation(CreateAllocationRequest(admission_id=admission, pool_id="WARDA-ICU"), key, key)
    first = create("adm-open-1", "first")
    create("adm-open-2", "second")
    waiting = create("adm-open-3", "third")
    staff, beds = test_setup["staff"], test_setup["bed"]
    staff.simulate_assignment_failure = True
    beds.simulate_compensation_failure = True
    assert service.release_allocation(first.allocation_id, "done", "r-release", "release-comp-fail").status == "RELEASED"
    assert beds.beds["bed-01"].current_allocation_id == waiting.allocation_id
    assert test_setup["triage_domain"].peek_next("WARDA-ICU").allocation_id == waiting.allocation_id
    with service.session_factory() as session:
        record = session.query(AllocationModel).filter_by(allocation_id=first.allocation_id).one()
        assert record.status == "RELEASED"
        assert record.pending_action == "PROMOTE"
    beds.simulate_compensation_failure = False
    staff.simulate_assignment_failure = False
    assert service.recover_pending() == 1
    assert beds.beds["bed-01"].current_allocation_id is None
    assert test_setup["triage_domain"].peek_next("WARDA-ICU").allocation_id == waiting.allocation_id
    assert service.recover_pending() == 1
    assert service.get_allocation(waiting.allocation_id).status == "ALLOCATED"

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
