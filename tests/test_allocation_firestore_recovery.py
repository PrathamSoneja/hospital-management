"""Cross-service recovery test: disposable PostgreSQL plus local Firestore emulator."""
import hashlib
import os
import uuid
from urllib.parse import urlsplit

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from services.bed_allocation.clients.fakes import (
    FakeBedRegistryClient, FakeLockCoordinatorClient, FakePatientRegistryClient,
    FakeStaffSchedulerClient, DirectTriageClient,
)
from services.bed_allocation.db import AllocationModel, Base
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.firestore import FirestoreTriageRepository
from services.triage.service import TriageDomainService
from services.triage.validation import MockFencingValidator
from shared.observability.errors import AppException


@pytest.mark.skipif(
    not os.environ.get("TEST_POSTGRES_URL") or not os.environ.get("FIRESTORE_EMULATOR_HOST"),
    reason="requires disposable local PostgreSQL and Firestore emulator",
)
def test_postgres_promotion_recovers_after_firestore_remove_commit_and_lost_response():
    database_url = os.environ["TEST_POSTGRES_URL"]
    if urlsplit(database_url).hostname not in {"localhost", "127.0.0.1", "::1"}:
        pytest.fail("TEST_POSTGRES_URL must point to local PostgreSQL")
    admin = create_engine(database_url)
    schema = f"test_fs_recovery_{uuid.uuid4().hex}"
    engine = None
    created = False
    try:
        with admin.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            created = True
        engine = create_engine(database_url, connect_args={"options": f"-csearch_path={schema}"}, pool_size=4)
        Base.metadata.create_all(bind=engine)
        sessions = sessionmaker(bind=engine, autoflush=False, autocommit=False)

        repo = FirestoreTriageRepository(project_id=f"allocation-recovery-{uuid.uuid4().hex}")
        triage_domain = TriageDomainService(repo, MockFencingValidator(accept_all=True))
        patient = FakePatientRegistryClient()
        patient.seed_admission("emulator-adm-1", "emulator-patient-1", "OPEN")
        patient.seed_admission("emulator-adm-2", "emulator-patient-2", "OPEN")
        triage_domain.set_triage("emulator-adm-1", 1, "WARDA-ICU")
        triage_domain.set_triage("emulator-adm-2", 2, "WARDA-ICU")
        triage_client = DirectTriageClient(triage_domain)
        original_remove = triage_client.remove_entry
        lose_first_response = [True]
        remove_calls = []

        def remove_then_lose_response(context, allocation_id, reason):
            remove_calls.append(context.operation_id)
            removed = original_remove(context, allocation_id, reason)
            if lose_first_response[0]:
                lose_first_response[0] = False
                raise AppException("SERVICE_UNAVAILABLE", "simulated lost response after Firestore commit", 503, retryable=True)
            return removed

        triage_client.remove_entry = remove_then_lose_response
        beds = FakeBedRegistryClient()
        beds.seed_bed("emulator-bed-1", "WARDA-ICU")
        staff = FakeStaffSchedulerClient(initial_capacity=5)
        service = BedAllocationDomainService(
            session_factory=sessions, patient_client=patient, triage_client=triage_client,
            lock_client=FakeLockCoordinatorClient(), bed_client=beds, staff_client=staff,
            owner_id="postgres-firestore-recovery-test",
        )

        first = service.create_allocation(CreateAllocationRequest(admission_id="emulator-adm-1", pool_id="WARDA-ICU"), "r1", "create-1")
        waiting = service.create_allocation(CreateAllocationRequest(admission_id="emulator-adm-2", pool_id="WARDA-ICU"), "r2", "create-2")
        assert waiting.status == "WAITLISTED"
        released = service.release_allocation(first.allocation_id, "done", "release-1", "release-key")
        assert released.status == "RELEASED"
        assert triage_domain.peek_next("WARDA-ICU") is None
        with sessions() as session:
            assert session.query(AllocationModel).filter_by(allocation_id=waiting.allocation_id).one().status == "WAITLISTED"

        restarted_service = BedAllocationDomainService(
            session_factory=sessions, patient_client=patient, triage_client=triage_client,
            lock_client=service.lock_client, bed_client=beds, staff_client=staff,
            owner_id="postgres-firestore-recovery-restart",
        )
        assert restarted_service.recover_pending() == 1
        assert remove_calls == [f"{waiting.allocation_id}:prom-remove:1"] * 2
        assert restarted_service.get_allocation(first.allocation_id).status == "RELEASED"
        assert restarted_service.get_allocation(waiting.allocation_id).status == "ALLOCATED"
        op_id = f"{waiting.allocation_id}:prom-remove:1"
        ledger = repo.operation_col.document(hashlib.sha256(op_id.encode()).hexdigest()).get().to_dict()
        assert ledger["state"] == "COMPLETED" and ledger["result_success"] is True
        assert ledger["entry_id"] and ledger["completed_at"] is not None
    finally:
        if engine is not None:
            engine.dispose()
        if created:
            with admin.begin() as connection:
                connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        admin.dispose()
