from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from google.auth.credentials import AnonymousCredentials
from google.cloud import firestore
from google.cloud.firestore_v1 import Query

from services.staff_scheduler.grpc_server import StaffSchedulerGrpcServicer
from services.staff_scheduler.main import build_app
from services.staff_scheduler.models import StaffCreate, ShiftCreate
from services.staff_scheduler.repository.firestore import FirestoreStaffRepository
from services.staff_scheduler.repository.memory import InMemoryStaffRepository
from services.staff_scheduler.service import StaffSchedulerService
from shared.generated.common_pb2 import MutationContext
from shared.generated import staff_pb2
from shared.observability.errors import AppException


class FakeGrpcContext:
    def __init__(self) -> None:
        self.code = None
        self.details = ""

    def invocation_metadata(self) -> tuple:
        return ()

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


@pytest.fixture
def repository() -> InMemoryStaffRepository:
    return InMemoryStaffRepository()


@pytest.fixture
def staff_service(repository: InMemoryStaffRepository) -> StaffSchedulerService:
    return StaffSchedulerService(repository)


@pytest.fixture
def shift_window() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    return now - timedelta(hours=1), now + timedelta(hours=8)


def create_staff_with_shift(
    service: StaffSchedulerService,
    staff_id: str,
    shift_window: tuple[datetime, datetime],
) -> None:
    service.create_staff(
        StaffCreate(
            staff_id=staff_id,
            display_name=f"Nurse {staff_id}",
            pool_ids=["WARDA-ICU"],
        ),
        idempotency_key=f"staff:{staff_id}",
    )
    service.create_shift(
        staff_id,
        "day-shift",
        ShiftCreate(starts_at=shift_window[0], ends_at=shift_window[1]),
        idempotency_key=f"shift:{staff_id}",
    )


def mutation_context(operation_id: str, fencing_token: int = 1) -> MutationContext:
    return MutationContext(
        operation_id=operation_id,
        request_id="req-staff-test",
        pool_id="WARDA-ICU",
        lock_id="lock-1",
        owner_id="allocator-1",
        fencing_token=fencing_token,
    )


def test_staff_rest_creates_roster_and_idempotent_shift(repository, shift_window) -> None:
    app = build_app(repository=repository, start_grpc=False)
    with TestClient(app) as client:
        staff_response = client.post(
            "/api/v1/staff",
            headers={"Idempotency-Key": "staff-create-1"},
            json={
                "staff_id": "nurse-1",
                "display_name": "Nurse One",
                "role": "NURSE",
                "pool_ids": ["WARDA-ICU"],
            },
        )
        assert staff_response.status_code == 201
        staff = staff_response.json()

        replay = client.post(
            "/api/v1/staff",
            headers={"Idempotency-Key": "staff-create-1"},
            json={
                "staff_id": "nurse-1",
                "display_name": "Nurse One",
                "role": "NURSE",
                "pool_ids": ["WARDA-ICU"],
            },
        )
        assert replay.status_code == 201
        assert replay.json()["staff_id"] == staff["staff_id"]

        shift_body = {
            "starts_at": shift_window[0].isoformat(),
            "ends_at": shift_window[1].isoformat(),
        }
        shift_response = client.put(
            "/api/v1/staff/nurse-1/shifts/day-shift",
            headers={"Idempotency-Key": "shift-create-1"},
            json=shift_body,
        )
        assert shift_response.status_code == 200
        assert shift_response.json()["shift_id"] == "day-shift"
        assert client.get("/api/v1/staff/nurse-1/shifts").json()[0]["shift_id"] == "day-shift"

        mismatch = client.put(
            "/api/v1/staff/nurse-1/shifts/day-shift",
            headers={"Idempotency-Key": "shift-create-1"},
            json={
                "starts_at": shift_window[0].isoformat(),
                "ends_at": (shift_window[1] + timedelta(hours=1)).isoformat(),
            },
        )
        assert mismatch.status_code == 409
        assert mismatch.json()["error"]["code"] == "IDEMPOTENCY_MISMATCH"


def test_capacity_requires_active_nurse_in_active_shift(staff_service, shift_window) -> None:
    create_staff_with_shift(staff_service, "nurse-1", shift_window)
    staff_service.create_staff(
        StaffCreate(
            staff_id="nurse-off-duty",
            display_name="Off duty",
            pool_ids=["WARDA-ICU"],
        ),
        idempotency_key="staff:off-duty",
    )
    staff_service.create_staff(
        StaffCreate(
            staff_id="nurse-other-pool",
            display_name="Other pool",
            pool_ids=["WARDB-GEN"],
        ),
        idempotency_key="staff:other-pool",
    )

    assert staff_service.check_capacity("WARDA-ICU") == 1
    assert staff_service.check_capacity("WARDB-GEN") == 0


def test_firestore_transactional_shift_read_uses_query() -> None:
    client = firestore.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
    )
    staff_ref = client.collection("staff").document("nurse-1")
    now = datetime.now(timezone.utc)
    shift = Mock()
    shift.to_dict.return_value = {
        "active": True,
        "starts_at": now - timedelta(minutes=5),
        "ends_at": now + timedelta(minutes=5),
    }
    transaction = Mock()
    transaction.get.return_value = [shift]

    assert FirestoreStaffRepository._on_shift(staff_ref, now, transaction)
    assert isinstance(transaction.get.call_args.args[0], Query)


def test_grpc_check_assign_replay_and_release(staff_service, shift_window) -> None:
    create_staff_with_shift(staff_service, "nurse-1", shift_window)
    servicer = StaffSchedulerGrpcServicer(staff_service)
    context = FakeGrpcContext()

    capacity = servicer.CheckCapacity(
        staff_pb2.CheckCapacityRequest(pool_id="WARDA-ICU", request_id="req-1"),
        context,
    )
    assert capacity.has_capacity
    assert capacity.available_count == 1

    request = staff_pb2.AssignStaffRequest(
        context=mutation_context("allocation-1:assign-staff"),
        allocation_id="allocation-1",
        bed_id="bed-1",
    )
    assigned = servicer.AssignStaff(request, context)
    assert assigned.success
    assert assigned.staff_id == "nurse-1"
    assert staff_service.check_capacity("WARDA-ICU") == 0

    replay = servicer.AssignStaff(request, context)
    assert replay.staff_assignment_id == assigned.staff_assignment_id
    assert staff_service.get_staff("nurse-1").active_assignment_count == 1

    released = servicer.ReleaseAssignment(
        staff_pb2.ReleaseAssignmentRequest(
            context=mutation_context("allocation-1:release-staff", fencing_token=2),
            staff_assignment_id=assigned.staff_assignment_id,
            allocation_id="allocation-1",
        ),
        context,
    )
    assert released.success
    assert staff_service.check_capacity("WARDA-ICU") == 1


def test_concurrent_assignments_never_exceed_staff_capacity(staff_service, shift_window) -> None:
    create_staff_with_shift(staff_service, "nurse-1", shift_window)

    def assign(index: int) -> bool:
        context = mutation_context(f"allocation-{index}:assign", fencing_token=1)
        return staff_service.assign(context, f"allocation-{index}", f"bed-{index}") is not None

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(assign, range(8)))

    assert sum(outcomes) == 1
    assert staff_service.get_staff("nurse-1").active_assignment_count == 1


def test_stale_fencing_token_is_rejected(staff_service, shift_window) -> None:
    create_staff_with_shift(staff_service, "nurse-1", shift_window)
    assignment = staff_service.assign(
        mutation_context("assignment-new", fencing_token=2),
        "allocation-new",
        "bed-new",
    )
    assert assignment is not None

    with pytest.raises(AppException) as error:
        staff_service.assign(
            mutation_context("assignment-stale", fencing_token=1),
            "allocation-stale",
            "bed-stale",
        )
    assert error.value.code == "FAILED_PRECONDITION"
