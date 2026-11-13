"""
Allocation's real adapters against teammates' merged service code, run locally without cloud:

* Staff Scheduler (Ruchir): its gRPC server over its Firestore repository on the emulator,
  driven by Allocation's GrpcStaffSchedulerClient.
* Patient Registry (Ruchir): its FastAPI app over HTTP (SQLite engine and the in-memory
  document storage its own tests inject; production needs a GCS bucket), driven by
  Allocation's HttpPatientRegistryClient.

* Resource Analytics (Jay): its stream aggregator fed contract-valid Allocation events.

Bed Registry and Lock Coordinator run as real processes in tests/integration/test_actual_services.py.
"""
import os
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

import json
from pathlib import Path

import jsonschema
import pytest
import uvicorn
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from services.bed_allocation.clients.grpc_clients import GrpcStaffSchedulerClient, HttpPatientRegistryClient
from services.patient_registry.main import create_app
from services.patient_registry.models import Base as PatientBase
from services.resource_analytics.aggregator import StreamAggregatorEngine
from services.resource_analytics.models import BedEvent
from services.staff_scheduler.grpc_server import create_grpc_server
from services.staff_scheduler.models import ShiftCreate, StaffCreate
from services.staff_scheduler.repository.firestore import FirestoreStaffRepository
from services.staff_scheduler.service import StaffSchedulerService
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import AppException
from tests.sandbox.runtime import free_port
from tests.test_patient_registry import MemoryDocumentStorage

POOL = "WARDA-ICU"


def _context(operation_id, token):
    return MutationContext(operation_id=operation_id, request_id=f"req-{operation_id}", pool_id=POOL,
                           lock_id="lock-1", owner_id="allocator-1", fencing_token=token)


@pytest.fixture
def staff_client():
    if not os.environ.get("FIRESTORE_EMULATOR_HOST"):
        pytest.skip("requires the local Firestore emulator; cloud Firestore is never used")
    service = StaffSchedulerService(FirestoreStaffRepository(project_id=f"boundary-{uuid.uuid4().hex[:8]}"))
    now = datetime.now(timezone.utc)
    service.create_staff(StaffCreate(staff_id="nurse-1", display_name="Nurse One", pool_ids=[POOL]),
                         idempotency_key="staff:nurse-1")
    service.create_shift("nurse-1", "day", ShiftCreate(starts_at=now - timedelta(hours=1), ends_at=now + timedelta(hours=8)),
                         idempotency_key="shift:nurse-1")
    port = free_port()
    server = create_grpc_server(service, host="127.0.0.1", port=port)
    server.start()
    try:
        yield GrpcStaffSchedulerClient(f"127.0.0.1:{port}", default_timeout_s=5.0)
    finally:
        server.stop(grace=0)


@pytest.mark.xfail(strict=True, raises=AppException, reason=(
    "Staff Scheduler defect (Ruchir): FirestoreStaffRepository._on_shift passes a CollectionReference to "
    "transaction.get(), so AssignStaff fails INTERNAL on Firestore. Proposed one-line fix: "
    "docs/proposed-fixes/staff-scheduler-transaction-get.patch (verified: this test passes with it). "
    "strict=True turns the fix into a failure here so this marker gets removed."))
def test_staff_scheduler_boundary_with_allocation_adapter(staff_client):
    assert staff_client.check_capacity(POOL, "req-1") == (True, 1)

    ok, assignment_id, staff_id, _ = staff_client.assign_staff(_context("alloc-1:assign-staff", 5), "alloc-1", "bed-1")
    assert ok and assignment_id and staff_id == "nurse-1"
    replay = staff_client.assign_staff(_context("alloc-1:assign-staff", 5), "alloc-1", "bed-1")
    assert replay[:3] == (True, assignment_id, staff_id)  # same operation ID -> same assignment

    # The only nurse is busy: success=False with confirmed exhaustion, which Allocation waitlists.
    assert staff_client.assign_staff(_context("alloc-2:assign-staff", 5), "alloc-2", "bed-2")[0] is False
    assert staff_client.check_capacity(POOL, "req-2") == (False, 0)

    # A stale fencing token is refused (FAILED_PRECONDITION); Allocation's adapter surfaces it as a
    # retryable unknown outcome, so the workflow recovers under a newer lease instead of guessing.
    with pytest.raises(AppException) as stale:
        staff_client.assign_staff(_context("alloc-3:assign-staff", 4), "alloc-3", "bed-3")
    assert stale.value.retryable

    assert staff_client.release_assignment(_context("alloc-1:release-staff", 6), assignment_id, "alloc-1") == (True, "")
    assert staff_client.release_assignment(_context("alloc-1:release-staff", 6), assignment_id, "alloc-1")[0] is True
    assert staff_client.check_capacity(POOL, "req-3") == (True, 1)


@pytest.fixture
def patient_base_url():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    PatientBase.metadata.create_all(bind=engine)
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(create_app(document_storage=MemoryDocumentStorage(), engine=engine),
                                           host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.05)
    try:
        yield f"http://127.0.0.1:{port}/api/v1"
    finally:
        server.should_exit = True
        thread.join(10)
        engine.dispose()


def test_patient_registry_boundary_with_allocation_adapter(patient_base_url):
    import httpx
    patient = httpx.post(f"{patient_base_url}/patients", headers={"Idempotency-Key": "p-1"},
                         json={"first_name": "Test", "last_name": "Patient", "date_of_birth": "1980-01-01"}).json()
    admission = httpx.post(f"{patient_base_url}/admissions", headers={"Idempotency-Key": "a-1"},
                           json={"patient_id": patient["patient_id"]}).json()

    client = HttpPatientRegistryClient(patient_base_url, default_timeout_s=5.0)
    found = client.get_admission(admission["admission_id"], "req-boundary")
    assert (found.admission_id, found.patient_id, found.status) == (admission["admission_id"], patient["patient_id"], "OPEN")
    assert client.get_admission("adm-does-not-exist", "req-boundary") is None  # 404 -> not found, not an outage


def test_resource_analytics_counts_a_redelivered_allocation_event_once():
    event = {  # the shape Allocation's outbox publishes to Kafka (validated against the contract below)
        "schema_version": "1.0.0", "event_id": "evt-1", "stream_sequence": 1, "event_type": "BED_ALLOCATED",
        "event_time": "2026-11-12T10:00:01+00:00", "request_id": "req-1", "allocation_id": "alloc-1",
        "admission_id": "adm-x", "patient_id": "pat-x", "pool_id": POOL, "bed_id": "bed-1", "lock_wait_ms": 3,
        "lock_contended": False, "allocation_wait_ms": 40, "occupancy_delta": 1,
    }
    schema = json.loads((Path(__file__).resolve().parents[1] / "contracts/events/bed-event.schema.json").read_text())
    jsonschema.validate(event, schema)
    engine = StreamAggregatorEngine()
    engine.process_event(BedEvent(**event))
    engine.process_event(BedEvent(**event))  # broker redelivery: same event_id and stream_sequence
    stats = engine.flush_all_windows()
    assert [(s.allocated, s.occupancy_at_window_end) for s in stats] == [(1, 1)]
