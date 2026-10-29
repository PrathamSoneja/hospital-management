"""
Real gRPC and REST clients connecting to teammates' services in multi-service deployment.
Enforces configurable deadlines and request-ID propagation.
"""
import grpc
import httpx
from typing import List, Optional, Tuple
from services.bed_allocation.clients.base import (
    BedDto, BedRegistryClient, LockCoordinatorClient, LockLease,
    PatientAdmissionDto, PatientRegistryClient, StaffSchedulerClient,
    TriageClient, TriageDto, WaitlistEntryDto
)
from shared.generated import (
    bed_pb2, bed_pb2_grpc, lock_pb2, lock_pb2_grpc,
    staff_pb2, staff_pb2_grpc, triage_pb2, triage_pb2_grpc
)
from shared.observability.errors import AppException
from starlette import status

class GrpcLockCoordinatorClient(LockCoordinatorClient):
    def __init__(self, target: str, default_timeout_s: float = 3.0):
        self.channel = grpc.insecure_channel(target)
        self.stub = lock_pb2_grpc.LockCoordinatorStub(self.channel)
        self.default_timeout_s = default_timeout_s

    def acquire(self, pool_id: str, owner_id: str, acquire_id: str, lease_ms: int, wait_timeout_ms: int) -> LockLease:
        req = lock_pb2.AcquireLockRequest(
            pool_id=pool_id, owner_id=owner_id, acquire_id=acquire_id,
            lease_ms=lease_ms, wait_timeout_ms=wait_timeout_ms
        )
        try:
            resp = self.stub.Acquire(req, timeout=(wait_timeout_ms / 1000.0) + self.default_timeout_s)
            return LockLease(
                lock_id=resp.lock_id, pool_id=resp.pool_id, owner_id=resp.owner_id,
                fencing_token=resp.fencing_token, expires_at=resp.expires_at,
                lock_wait_ms=resp.lock_wait_ms
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise AppException("DEADLINE_EXCEEDED", f"Lock acquisition timed out on pool {pool_id}", status.HTTP_504_GATEWAY_TIMEOUT, retryable=True)
            raise AppException("SERVICE_UNAVAILABLE", f"Lock Coordinator unreachable: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def release(self, lock_id: str, owner_id: str) -> bool:
        req = lock_pb2.ReleaseLockRequest(lock_id=lock_id, owner_id=owner_id)
        try:
            resp = self.stub.Release(req, timeout=self.default_timeout_s)
            return resp.success
        except grpc.RpcError:
            return False

    def validate(self, lock_id: str, owner_id: str, pool_id: str, fencing_token: int) -> bool:
        req = lock_pb2.ValidateLockRequest(
            lock_id=lock_id, owner_id=owner_id, pool_id=pool_id, fencing_token=fencing_token
        )
        try:
            resp = self.stub.Validate(req, timeout=self.default_timeout_s)
            return resp.valid
        except grpc.RpcError:
            return False

class GrpcTriageClient(TriageClient):
    def __init__(self, target: str, default_timeout_s: float = 3.0):
        self.channel = grpc.insecure_channel(target)
        self.stub = triage_pb2_grpc.TriageServiceStub(self.channel)
        self.default_timeout_s = default_timeout_s

    def get_triage(self, admission_id: str, request_id: str) -> Optional[TriageDto]:
        req = triage_pb2.GetTriageRequest(admission_id=admission_id, request_id=request_id)
        try:
            resp = self.stub.GetTriage(req, timeout=self.default_timeout_s)
            if not resp.found:
                return None
            return TriageDto(
                admission_id=resp.triage.admission_id,
                triage_level=resp.triage.triage_level,
                pool_id=resp.triage.pool_id,
                updated_at=resp.triage.updated_at
            )
        except grpc.RpcError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Triage service unreachable: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def enqueue(self, context, allocation_id: str, admission_id: str) -> WaitlistEntryDto:
        req = triage_pb2.EnqueueWaitlistRequest(context=context, allocation_id=allocation_id, admission_id=admission_id)
        try:
            resp = self.stub.Enqueue(req, timeout=self.default_timeout_s)
            return WaitlistEntryDto(
                entry_id=resp.entry.entry_id,
                allocation_id=resp.entry.allocation_id,
                admission_id=resp.entry.admission_id,
                pool_id=resp.entry.pool_id,
                triage_level=resp.entry.triage_level,
                queued_at=resp.entry.queued_at
            )
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                raise AppException("STATE_CONFLICT", e.details(), status.HTTP_409_CONFLICT)
            raise AppException("SERVICE_UNAVAILABLE", f"Triage enqueue failed: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def peek_next(self, pool_id: str, request_id: str) -> Optional[WaitlistEntryDto]:
        req = triage_pb2.PeekNextWaitlistRequest(pool_id=pool_id, request_id=request_id)
        try:
            resp = self.stub.PeekNext(req, timeout=self.default_timeout_s)
            if not resp.found:
                return None
            return WaitlistEntryDto(
                entry_id=resp.entry.entry_id,
                allocation_id=resp.entry.allocation_id,
                admission_id=resp.entry.admission_id,
                pool_id=resp.entry.pool_id,
                triage_level=resp.entry.triage_level,
                queued_at=resp.entry.queued_at
            )
        except grpc.RpcError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Triage peek failed: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def remove_entry(self, context, allocation_id: str, reason: str) -> bool:
        req = triage_pb2.RemoveWaitlistEntryRequest(context=context, allocation_id=allocation_id, reason=reason)
        try:
            resp = self.stub.RemoveEntry(req, timeout=self.default_timeout_s)
            return resp.success
        except grpc.RpcError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Triage remove entry failed: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

class GrpcBedRegistryClient(BedRegistryClient):
    def __init__(self, target: str, default_timeout_s: float = 3.0):
        self.channel = grpc.insecure_channel(target)
        self.stub = bed_pb2_grpc.BedServiceStub(self.channel)
        self.default_timeout_s = default_timeout_s

    def list_available_beds(self, pool_id: str, request_id: str) -> List[BedDto]:
        req = bed_pb2.ListAvailableBedsRequest(pool_id=pool_id, request_id=request_id)
        try:
            resp = self.stub.ListAvailableBeds(req, timeout=self.default_timeout_s)
            return [
                BedDto(bed_id=b.bed_id, pool_id=b.pool_id, ward=b.ward, status=b.status, current_allocation_id=b.current_allocation_id or None)
                for b in resp.beds
            ]
        except grpc.RpcError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Bed Registry unreachable: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def allocate_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, Optional[BedDto], str]:
        req = bed_pb2.AllocateBedRequest(context=context, bed_id=bed_id, allocation_id=allocation_id)
        try:
            resp = self.stub.AllocateBed(req, timeout=self.default_timeout_s)
            if not resp.success:
                return False, None, resp.error_message
            bed = BedDto(
                bed_id=resp.bed.bed_id, pool_id=resp.bed.pool_id, ward=resp.bed.ward,
                status=resp.bed.status, current_allocation_id=resp.bed.current_allocation_id or None
            )
            return True, bed, ""
        except grpc.RpcError as e:
            return False, None, f"Bed allocation RPC error: {e.details()}"

    def release_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, str]:
        req = bed_pb2.ReleaseBedRequest(context=context, bed_id=bed_id, allocation_id=allocation_id)
        try:
            resp = self.stub.ReleaseBed(req, timeout=self.default_timeout_s)
            return resp.success, resp.error_message
        except grpc.RpcError as e:
            return False, f"Bed release RPC error: {e.details()}"

class GrpcStaffSchedulerClient(StaffSchedulerClient):
    def __init__(self, target: str, default_timeout_s: float = 3.0):
        self.channel = grpc.insecure_channel(target)
        self.stub = staff_pb2_grpc.StaffSchedulerStub(self.channel)
        self.default_timeout_s = default_timeout_s

    def check_capacity(self, pool_id: str, request_id: str) -> Tuple[bool, int]:
        req = staff_pb2.CheckCapacityRequest(pool_id=pool_id, request_id=request_id)
        try:
            resp = self.stub.CheckCapacity(req, timeout=self.default_timeout_s)
            return resp.has_capacity, resp.available_count
        except grpc.RpcError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Staff Scheduler unreachable: {e.details()}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def assign_staff(self, context, allocation_id: str, bed_id: str) -> Tuple[bool, Optional[str], Optional[str], str]:
        req = staff_pb2.AssignStaffRequest(context=context, allocation_id=allocation_id, bed_id=bed_id)
        try:
            resp = self.stub.AssignStaff(req, timeout=self.default_timeout_s)
            return resp.success, resp.staff_assignment_id, resp.staff_id, resp.error_message
        except grpc.RpcError as e:
            return False, None, None, f"Staff assignment RPC error: {e.details()}"

    def release_assignment(self, context, staff_assignment_id: str, allocation_id: str) -> Tuple[bool, str]:
        req = staff_pb2.ReleaseAssignmentRequest(context=context, staff_assignment_id=staff_assignment_id, allocation_id=allocation_id)
        try:
            resp = self.stub.ReleaseAssignment(req, timeout=self.default_timeout_s)
            return resp.success, resp.error_message
        except grpc.RpcError as e:
            return False, f"Staff release RPC error: {e.details()}"

class HttpPatientRegistryClient(PatientRegistryClient):
    def __init__(self, base_url: str, default_timeout_s: float = 3.0):
        self.base_url = base_url.rstrip("/")
        self.default_timeout_s = default_timeout_s

    def get_admission(self, admission_id: str, request_id: str) -> Optional[PatientAdmissionDto]:
        url = f"{self.base_url}/admissions/{admission_id}"
        headers = {"X-Request-ID": request_id}
        try:
            with httpx.Client(timeout=self.default_timeout_s) as client:
                resp = client.get(url, headers=headers)
                if resp.status_code == 404:
                    return None
                if resp.status_code != 200:
                    raise AppException("SERVICE_UNAVAILABLE", f"Patient Registry returned {resp.status_code}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
                data = resp.json()
                return PatientAdmissionDto(
                    admission_id=data["admission_id"],
                    patient_id=data.get("patient_id", f"pat-{admission_id}"),
                    status=data.get("status", "OPEN")
                )
        except httpx.RequestError as e:
            raise AppException("SERVICE_UNAVAILABLE", f"Patient Registry unreachable: {str(e)}", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
