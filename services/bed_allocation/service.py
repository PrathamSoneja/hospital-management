"""
Pure domain business logic and workflow orchestration for Bed Allocation Service.
Implements:
- Distributed pool lock handling with fencing tokens
- Durable idempotency with payload mismatch detection
- Atomic prevention of multiple active requests per admission
- No-staff capacity waitlist policy & failure compensation
- Transactional outbox persistence
- Non-reentrant lock promotion upon release
- Idempotent release and cancellation
"""
import hashlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from sqlalchemy import or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from services.bed_allocation.clients.base import (
    BedRegistryClient, LockCoordinatorClient, PatientRegistryClient,
    StaffSchedulerClient, TriageClient
)
from services.bed_allocation.db import (
    AllocationModel, IdempotencyModel, OperationRecoveryModel, OutboxEventModel
)
from services.bed_allocation.models import AllocationResponse, CreateAllocationRequest, ReleaseResponse
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import AppException
from shared.observability.metrics import (
    ALLOCATION_PROCESSING_SECONDS, ALLOCATION_REQUESTS_TOTAL,
    LOCK_WAIT_SECONDS
)
from starlette import status
import time

ACTIVE_ADMISSION_INDEX = "uq_allocations_active_admission"

def _is_active_admission_conflict(error: IntegrityError) -> bool:
    original = error.orig
    constraint_name = getattr(getattr(original, "diag", None), "constraint_name", None)
    if constraint_name == ACTIVE_ADMISSION_INDEX:
        return True
    return "UNIQUE constraint failed: allocations.admission_id" in str(original)


def _is_idempotency_claim_conflict(error: IntegrityError) -> bool:
    original = error.orig
    constraint_name = getattr(getattr(original, "diag", None), "constraint_name", None)
    if constraint_name == "idempotency_keys_pkey":
        return True
    message = str(original)
    return "idempotency_keys.method" in message or "idempotency_keys_pkey" in message

class BedAllocationDomainService:
    def __init__(
        self,
        session_factory: sessionmaker,
        patient_client: PatientRegistryClient,
        triage_client: TriageClient,
        lock_client: LockCoordinatorClient,
        bed_client: BedRegistryClient,
        staff_client: StaffSchedulerClient,
        owner_id: str = "bed-alloc-instance-1"
    ):
        self.session_factory = session_factory
        self.patient_client = patient_client
        self.triage_client = triage_client
        self.lock_client = lock_client
        self.bed_client = bed_client
        self.staff_client = staff_client
        self.owner_id = owner_id

    def _hash_payload(self, payload: dict) -> str:
        serialized = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _idempotency_scope(self, endpoint: str, payload: dict) -> dict:
        method, path = endpoint.split(" ", 1)
        if path == "/api/v1/allocations":
            operation = "CREATE"
            target_id = str(payload["admission_id"])
        else:
            parts = path.strip("/").split("/")
            target_id = parts[3]
            operation = parts[4].upper()
        return {
            "method": method.upper(),
            "operation": operation,
            "target_id": target_id,
        }

    def _allocation_response(self, record: AllocationModel) -> AllocationResponse:
        return AllocationResponse(
            allocation_id=record.allocation_id,
            admission_id=record.admission_id,
            pool_id=record.pool_id,
            status=record.status,
            bed_id=record.bed_id,
            staff_assignment_id=record.staff_assignment_id,
            waitlist_entry_id=record.waitlist_entry_id,
            pending_action=record.pending_action,
            created_at=record.created_at.isoformat(),
        )

    def _check_idempotency(self, session: Session, key: Optional[str], endpoint: str, payload: dict):
        if not key:
            return None
        scope = self._idempotency_scope(endpoint, payload)
        existing = session.query(IdempotencyModel).filter_by(**scope, idempotency_key=key).first()
        if not existing:
            return None
        req_hash = self._hash_payload(payload)
        if existing.request_hash != req_hash:
            raise AppException(
                code="IDEMPOTENCY_MISMATCH",
                message="Idempotency key was previously used with a different request payload",
                status_code=status.HTTP_409_CONFLICT
            )
        if existing.state == "COMPLETED":
            return json.loads(existing.response_body)
        record = session.query(AllocationModel).filter_by(allocation_id=existing.allocation_id).first()
        if not record:
            raise AppException(
                code="SERVICE_UNAVAILABLE",
                message="The accepted operation is not currently readable; retry with the same idempotency key",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                retryable=True,
            )
        return self._allocation_response(record)

    def _save_idempotency(self, session: Session, key: Optional[str], endpoint: str, payload: dict, resp_status: int, response_data: dict):
        if not key:
            return
        scope = self._idempotency_scope(endpoint, payload)
        model = session.query(IdempotencyModel).filter_by(**scope, idempotency_key=key).first()
        if model is None:
            model = IdempotencyModel(
                **scope,
                idempotency_key=key,
                endpoint=endpoint,
                request_hash=self._hash_payload(payload),
                allocation_id=response_data.get("allocation_id"),
                action_id=response_data.get("allocation_id"),
                created_at=datetime.now(timezone.utc),
            )
            session.add(model)
        model.state = "COMPLETED"
        model.allocation_id = response_data.get("allocation_id", model.allocation_id)
        model.action_id = model.allocation_id
        model.response_status = resp_status
        model.response_body = json.dumps(response_data)
        model.updated_at = datetime.now(timezone.utc)

    def _record_step(
        self, allocation_id: str, action: str, operation_id: str, step: str,
        state: str, details: Optional[dict] = None, allocation_status: str = "PROCESSING",
    ) -> None:
        now = datetime.now(timezone.utc)
        with self.session_factory() as session:
            allocation = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().first()
            if allocation is None:
                raise RuntimeError(f"Allocation {allocation_id} disappeared during workflow")
            allocation.status = allocation_status
            allocation.pending_action = action
            allocation.recovery_claim_owner = self.owner_id
            allocation.recovery_claim_until = now + timedelta(seconds=60)
            allocation.updated_at = now
            operation = session.query(OperationRecoveryModel).filter_by(operation_id=operation_id).first()
            if operation is None:
                operation = OperationRecoveryModel(
                    operation_id=operation_id,
                    allocation_id=allocation_id,
                    step=step,
                    status=state,
                    details=json.dumps(details or {}),
                    updated_at=now,
                )
                session.add(operation)
            else:
                operation.status = state
                operation.details = json.dumps(details or {})
                operation.updated_at = now
            session.commit()

    def _mark_recovering(self, allocation_id: str, action: str, operation_id: str, step: str, error: Exception) -> AllocationResponse:
        # Store the action and exception class only; downstream text can contain
        # identifiers or secrets and is not needed to resume by stable operation ID.
        self._record_step(
            allocation_id, action, operation_id, step, "UNCERTAIN",
            {"error_type": type(error).__name__}, allocation_status="RECOVERING",
        )
        with self.session_factory() as session:
            allocation = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            allocation.recovery_claim_until = datetime.now(timezone.utc)
            session.commit()
            return self._allocation_response(allocation)

    def create_allocation(self, req: CreateAllocationRequest, request_id: str, idempotency_key: Optional[str] = None):
        start_time = time.perf_counter()
        with self.session_factory() as session:
            # 1. Durable Idempotency Check
            cached = self._check_idempotency(session, idempotency_key, "POST /api/v1/allocations", req.model_dump())
            if cached:
                return cached if isinstance(cached, AllocationResponse) else AllocationResponse(**cached)

            # 2. Atomic check for active requests per admission
            active = (
                session.query(AllocationModel)
                .filter(
                    AllocationModel.admission_id == req.admission_id,
                    AllocationModel.status.notin_(["RELEASED", "CANCELLED", "FAILED"])
                )
                .first()
            )
            if active:
                raise AppException(
                    code="ACTIVE_ALLOCATION_EXISTS",
                    message=f"An active allocation or waitlist entry ({active.allocation_id}) already exists for admission '{req.admission_id}'",
                    status_code=status.HTTP_409_CONFLICT
                )

            # 3. Load & validate admission from Patient Registry
            adm = self.patient_client.get_admission(req.admission_id, request_id)
            if not adm:
                raise AppException(
                    code="NOT_FOUND",
                    message=f"Admission '{req.admission_id}' does not exist in Patient Registry",
                    status_code=status.HTTP_404_NOT_FOUND
                )
            if adm.status != "OPEN":
                raise AppException(
                    code="STATE_CONFLICT",
                    message=f"Admission '{req.admission_id}' is {adm.status}, must be OPEN to allocate resources",
                    status_code=status.HTTP_409_CONFLICT
                )

            # 4. Load & validate Triage assessment
            triage = self.triage_client.get_triage(req.admission_id, request_id)
            if not triage:
                raise AppException(
                    code="NOT_FOUND",
                    message=f"No triage record found for admission '{req.admission_id}'. Patient must be triaged first.",
                    status_code=status.HTTP_404_NOT_FOUND
                )
            if triage.pool_id != req.pool_id:
                raise AppException(
                    code="STATE_CONFLICT",
                    message=f"Requested pool '{req.pool_id}' does not match triage assessed pool '{triage.pool_id}'",
                    status_code=status.HTTP_409_CONFLICT
                )

            # 5. Atomically persist the active admission and idempotency claim
            # before any downstream mutation. A unique scoped claim makes a
            # concurrent matching retry observe this same allocation.
            allocation_id = f"alloc-{uuid.uuid4().hex[:10]}"
            alloc_record = AllocationModel(
                allocation_id=allocation_id,
                admission_id=req.admission_id,
                pool_id=req.pool_id,
                status="PROCESSING",
                pending_action="CHECK_PRIORITY",
                workflow_data=json.dumps({"patient_id": adm.patient_id}),
            )
            try:
                if idempotency_key:
                    scope = self._idempotency_scope("POST /api/v1/allocations", req.model_dump())
                    session.add(IdempotencyModel(
                        **scope,
                        idempotency_key=idempotency_key,
                        endpoint="POST /api/v1/allocations",
                        request_hash=self._hash_payload(req.model_dump()),
                        state="IN_PROGRESS",
                        allocation_id=allocation_id,
                        action_id=f"{allocation_id}:create",
                        response_status=None,
                        response_body=None,
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    ))
                    session.flush()
                session.add(alloc_record)
                session.commit()
            except IntegrityError as error:
                session.rollback()
                if idempotency_key and _is_idempotency_claim_conflict(error):
                    with self.session_factory() as retry_session:
                        cached = self._check_idempotency(
                            retry_session, idempotency_key, "POST /api/v1/allocations", req.model_dump()
                        )
                    if isinstance(cached, AllocationResponse):
                        return cached
                    if cached:
                        return AllocationResponse(**cached)
                if _is_active_admission_conflict(error):
                    raise AppException(
                        code="ACTIVE_ALLOCATION_EXISTS",
                        message=f"An active allocation or waitlist entry already exists for admission '{req.admission_id}'",
                        status_code=status.HTTP_409_CONFLICT
                    ) from error
                raise

        return self._run_create_workflow(
            allocation_id=allocation_id,
            request_id=request_id,
            idempotency_key=idempotency_key,
            request_payload=req.model_dump(),
            started_at=start_time,
        )

    def _read_operation(self, operation_id: str) -> Optional[dict]:
        with self.session_factory() as session:
            operation = session.query(OperationRecoveryModel).filter_by(operation_id=operation_id).first()
            if not operation:
                return None
            return {"status": operation.status, "details": json.loads(operation.details or "{}")}

    def _update_allocation(self, allocation_id: str, **values) -> None:
        with self.session_factory() as session:
            allocation = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().first()
            for name, value in values.items():
                setattr(allocation, name, value)
            allocation.updated_at = datetime.now(timezone.utc)
            session.commit()

    def _run_create_workflow(
        self, allocation_id: str, request_id: str, idempotency_key: Optional[str],
        request_payload: dict, started_at: Optional[float] = None,
    ) -> AllocationResponse:
        started_at = started_at or time.perf_counter()
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            if record is None:
                raise AppException("NOT_FOUND", "Accepted allocation no longer exists", status.HTTP_404_NOT_FOUND)
            req = CreateAllocationRequest(admission_id=record.admission_id, pool_id=record.pool_id)
            workflow = json.loads(record.workflow_data or "{}")
            action = record.pending_action or "FINALIZE_CREATE"
            record.status = "PROCESSING"
            record.recovery_claim_owner = self.owner_id
            record.recovery_claim_until = datetime.now(timezone.utc) + timedelta(seconds=60)
            session.commit()

        t_lock_start = time.perf_counter()
        try:
            lease = self.lock_client.acquire(
                pool_id=req.pool_id,
                owner_id=self.owner_id,
                acquire_id=f"acq-{allocation_id}",
                lease_ms=10000,
                wait_timeout_ms=5000,
            )
        except Exception as error:
            return self._mark_recovering(
                allocation_id, "CHECK_PRIORITY", f"{allocation_id}:acquire-lock", "ACQUIRE_LOCK", error
            )
        lock_wait_s = time.perf_counter() - t_lock_start
        LOCK_WAIT_SECONDS.labels(service="bed-allocation", pool_id=req.pool_id).observe(lock_wait_s)
        lock_wait_ms = int(lock_wait_s * 1000)
        lease_released = False
        try:
            final_status: Optional[str] = None
            bed_operation_id = f"{allocation_id}:reserve-bed"
            staff_operation_id = f"{allocation_id}:assign-staff"

            if action == "CHECK_PRIORITY":
                action = "CHECK_PRIORITY"
                peeked = self.triage_client.peek_next(req.pool_id, request_id)
                if peeked and peeked.admission_id != req.admission_id:
                    final_status = "WAITLISTED"
                else:
                    beds = self.bed_client.list_available_beds(req.pool_id, request_id)
                    if not beds:
                        final_status = "WAITLISTED"
                    else:
                        has_capacity, _ = self.staff_client.check_capacity(req.pool_id, request_id)
                        if not has_capacity:
                            final_status = "WAITLISTED"
                        else:
                            self._update_allocation(allocation_id, bed_id=beds[0].bed_id)
                            self._record_step(
                                allocation_id, "RESERVE_BED", bed_operation_id, "RESERVE_BED", "INTENDED",
                                {"bed_id": beds[0].bed_id},
                            )
                            action = "RESERVE_BED"

            if action == "RESERVE_BED":
                operation = self._read_operation(bed_operation_id)
                if operation and operation["status"] == "SUCCEEDED":
                    action = "ASSIGN_STAFF"
                else:
                    with self.session_factory() as session:
                        record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                        bed_id = record.bed_id
                    ctx_bed = MutationContext(
                        operation_id=bed_operation_id,
                        request_id=request_id,
                        pool_id=req.pool_id,
                        lock_id=lease.lock_id,
                        owner_id=lease.owner_id,
                        fencing_token=lease.fencing_token,
                    )
                    try:
                        bed_ok, _, bed_error = self.bed_client.allocate_bed(ctx_bed, bed_id, allocation_id)
                    except Exception as error:
                        return self._mark_recovering(allocation_id, "RESERVE_BED", bed_operation_id, "RESERVE_BED", error)
                    if not bed_ok:
                        self._record_step(
                            allocation_id, "RESERVE_BED", bed_operation_id, "RESERVE_BED", "UNCERTAIN",
                            {"response": "unsuccessful"}, allocation_status="RECOVERING",
                        )
                        return self._allocation_snapshot(allocation_id)
                    self._record_step(
                        allocation_id, "ASSIGN_STAFF", bed_operation_id, "RESERVE_BED", "SUCCEEDED",
                        {"bed_id": bed_id},
                    )
                    action = "ASSIGN_STAFF"

            if action == "ASSIGN_STAFF":
                operation = self._read_operation(staff_operation_id)
                if operation and operation["status"] == "SUCCEEDED":
                    staff_assignment_id = operation["details"].get("staff_assignment_id")
                    staff_id = operation["details"].get("staff_id")
                else:
                    with self.session_factory() as session:
                        record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                        bed_id = record.bed_id
                    self._record_step(
                        allocation_id, "ASSIGN_STAFF", staff_operation_id, "ASSIGN_STAFF", "INTENDED",
                        {"bed_id": bed_id},
                    )
                    ctx_staff = MutationContext(
                        operation_id=staff_operation_id,
                        request_id=request_id,
                        pool_id=req.pool_id,
                        lock_id=lease.lock_id,
                        owner_id=lease.owner_id,
                        fencing_token=lease.fencing_token,
                    )
                    try:
                        staff_ok, staff_assignment_id, staff_id, staff_error = self.staff_client.assign_staff(
                            ctx_staff, allocation_id, bed_id
                        )
                    except Exception as error:
                        return self._mark_recovering(
                            allocation_id, "ASSIGN_STAFF", staff_operation_id, "ASSIGN_STAFF", error
                        )
                    if not staff_ok:
                        has_capacity, _ = self.staff_client.check_capacity(req.pool_id, request_id)
                        if has_capacity:
                            return self._mark_recovering(
                                allocation_id,
                                "ASSIGN_STAFF",
                                staff_operation_id,
                                "ASSIGN_STAFF",
                                RuntimeError("Staff assignment was rejected without confirmed capacity exhaustion"),
                            )
                        workflow["waitlist_reason"] = "STAFF_CAPACITY_CONFIRMED"
                        self._update_allocation(allocation_id, workflow_data=json.dumps(workflow))
                        action = "COMPENSATE_BED"
                    else:
                        self._record_step(
                            allocation_id, "FINALIZE_ALLOCATED", staff_operation_id, "ASSIGN_STAFF", "SUCCEEDED",
                            {"staff_assignment_id": staff_assignment_id, "staff_id": staff_id},
                        )
                        self._update_allocation(
                            allocation_id,
                            staff_assignment_id=staff_assignment_id,
                            workflow_data=json.dumps({**workflow, "staff_id": staff_id}),
                        )
                        action = "FINALIZE_ALLOCATED"
                if operation and operation["status"] == "SUCCEEDED":
                    self._update_allocation(
                        allocation_id,
                        staff_assignment_id=staff_assignment_id,
                        workflow_data=json.dumps({**workflow, "staff_id": staff_id}),
                    )
                    action = "FINALIZE_ALLOCATED"

            if action == "COMPENSATE_BED":
                with self.session_factory() as session:
                    record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                    bed_id = record.bed_id
                compensation_id = f"{allocation_id}:compensate-bed"
                compensation = self._read_operation(compensation_id)
                if not compensation or compensation["status"] != "SUCCEEDED":
                    self._record_step(
                        allocation_id, "COMPENSATE_BED", compensation_id, "COMPENSATE_BED", "INTENDED",
                        {"bed_id": bed_id, "expected_allocation_id": allocation_id},
                    )
                    ctx_comp = MutationContext(
                        operation_id=compensation_id,
                        request_id=request_id,
                        pool_id=req.pool_id,
                        lock_id=lease.lock_id,
                        owner_id=lease.owner_id,
                        fencing_token=lease.fencing_token,
                    )
                    try:
                        released, _ = self.bed_client.release_bed(ctx_comp, bed_id, allocation_id)
                    except Exception as error:
                        return self._mark_recovering(
                            allocation_id, "COMPENSATE_BED", compensation_id, "COMPENSATE_BED", error
                        )
                    if not released:
                        return self._mark_recovering(
                            allocation_id,
                            "COMPENSATE_BED",
                            compensation_id,
                            "COMPENSATE_BED",
                            RuntimeError("Ownership-safe bed compensation was not confirmed"),
                        )
                    self._record_step(
                        allocation_id, "COMPENSATE_BED", compensation_id, "COMPENSATE_BED", "SUCCEEDED",
                        {"bed_id": bed_id},
                    )
                self._update_allocation(allocation_id, bed_id=None)
                final_status = "WAITLISTED"
                action = "ENQUEUE_WAITLIST"

            if final_status == "WAITLISTED" or action == "ENQUEUE_WAITLIST":
                enqueue_id = f"{allocation_id}:enqueue-waitlist"
                operation = self._read_operation(enqueue_id)
                if operation and operation["status"] == "SUCCEEDED":
                    waitlist_entry_id = operation["details"].get("entry_id")
                else:
                    self._record_step(
                        allocation_id, "ENQUEUE_WAITLIST", enqueue_id, "ENQUEUE_WAITLIST", "INTENDED",
                        {"admission_id": req.admission_id, "pool_id": req.pool_id},
                    )
                    ctx_enq = MutationContext(
                        operation_id=enqueue_id,
                        request_id=request_id,
                        pool_id=req.pool_id,
                        lock_id=lease.lock_id,
                        owner_id=lease.owner_id,
                        fencing_token=lease.fencing_token,
                    )
                    try:
                        entry = self.triage_client.enqueue(ctx_enq, allocation_id, req.admission_id)
                    except Exception as error:
                        return self._mark_recovering(
                            allocation_id, "ENQUEUE_WAITLIST", enqueue_id, "ENQUEUE_WAITLIST", error
                        )
                    waitlist_entry_id = entry.entry_id
                    self._record_step(
                        allocation_id, "FINALIZE_WAITLISTED", enqueue_id, "ENQUEUE_WAITLIST", "SUCCEEDED",
                        {"entry_id": waitlist_entry_id},
                    )
                    self._update_allocation(allocation_id, waitlist_entry_id=waitlist_entry_id)
                final_status = "WAITLISTED"
                action = "FINALIZE_WAITLISTED"

            if action == "FINALIZE_ALLOCATED":
                final_status = "ALLOCATED"
            elif action == "FINALIZE_WAITLISTED":
                final_status = "WAITLISTED"
            elif final_status is None:
                return self._mark_recovering(
                    allocation_id, action, f"{allocation_id}:{action.lower()}", action,
                    RuntimeError("Workflow state requires reconciliation"),
                )
        except Exception as error:
            operation_id = f"{allocation_id}:{action.lower().replace('_', '-')}"
            return self._mark_recovering(allocation_id, action, operation_id, action, error)
        finally:
            lease_released = self.lock_client.release(lease.lock_id, lease.owner_id)

        if not lease_released:
            return self._mark_recovering(
                allocation_id, "RELEASE_POOL_LOCK", f"{allocation_id}:release-lock", "RELEASE_LOCK",
                RuntimeError("Pool lock release was not confirmed"),
            )
        return self._finalize_create(
            allocation_id, request_id, idempotency_key, request_payload, final_status, lock_wait_ms, started_at
        )

    def recover_pending(self, batch_size: int = 20, lease_seconds: int = 60) -> int:
        """Claim expired Allocation workflows and resume them with stable operation IDs."""
        now = datetime.now(timezone.utc)
        worker_id = f"{self.owner_id}:recovery:{uuid.uuid4().hex}"
        with self.session_factory() as session:
            candidates = (
                session.query(AllocationModel)
                .filter(
                    AllocationModel.status.in_(["PROCESSING", "RECOVERING"]),
                    AllocationModel.pending_action.isnot(None),
                    or_(AllocationModel.recovery_claim_until.is_(None), AllocationModel.recovery_claim_until <= now),
                )
                .order_by(AllocationModel.updated_at.asc(), AllocationModel.allocation_id.asc())
                .with_for_update(skip_locked=True)
                .limit(batch_size)
                .all()
            )
            claimed_ids = []
            for record in candidates:
                record.recovery_claim_owner = worker_id
                record.recovery_claim_until = now + timedelta(seconds=lease_seconds)
                record.recovery_attempts = (record.recovery_attempts or 0) + 1
                record.status = "RECOVERING"
                record.updated_at = now
                claimed_ids.append(record.allocation_id)
            session.commit()

        for allocation_id in claimed_ids:
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                if not record or record.pending_action is None:
                    continue
                admission_id = record.admission_id
                pool_id = record.pool_id
                endpoint = "POST /api/v1/allocations"
                payload = {"admission_id": admission_id, "pool_id": pool_id}
                claim = session.query(IdempotencyModel).filter_by(
                    method="POST", operation="CREATE", target_id=admission_id,
                    allocation_id=allocation_id,
                ).first()
                key = claim.idempotency_key if claim else None
            try:
                self._run_create_workflow(
                    allocation_id, f"recovery-{uuid.uuid4().hex[:12]}", key, payload
                )
            except Exception:
                # Leave the durable row claimable on its lease timeout. Reconciliation
                # failures must never be converted into terminal allocation failures.
                continue
        return len(claimed_ids)

    def _allocation_snapshot(self, allocation_id: str) -> AllocationResponse:
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            return self._allocation_response(record)

    def _finalize_create(
        self, allocation_id: str, request_id: str, idempotency_key: Optional[str],
        request_payload: dict, final_status: str, lock_wait_ms: int, started_at: float,
    ) -> AllocationResponse:
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().first()
            workflow = json.loads(record.workflow_data or "{}")
            now = datetime.now(timezone.utc)
            event_type = "BED_ALLOCATED" if final_status == "ALLOCATED" else "ALLOCATION_WAITLISTED"
            for destination in ("pubsub", "kafka"):
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"allocation:{allocation_id}:{destination}:{event_type}"))
                if session.query(OutboxEventModel).filter_by(event_id=event_id).first():
                    continue
                if destination == "pubsub":
                    payload = {
                        "schema_version": "1.0.0", "event_id": event_id, "event_type": event_type,
                        "occurred_at": now.isoformat(), "request_id": request_id,
                        "allocation_id": allocation_id, "admission_id": record.admission_id,
                        "pool_id": record.pool_id, "bed_id": record.bed_id, "ward": "ICU-A",
                    }
                    topic = "bed-notifications"
                else:
                    payload = {
                        "schema_version": "1.0.0", "event_id": event_id, "event_type": event_type,
                        "event_time": now.isoformat(), "request_id": request_id,
                        "allocation_id": allocation_id, "admission_id": record.admission_id,
                        "patient_id": workflow.get("patient_id", f"pat-{record.admission_id}"),
                        "pool_id": record.pool_id, "bed_id": record.bed_id,
                        "lock_wait_ms": lock_wait_ms, "lock_contended": lock_wait_ms > 0,
                        "allocation_wait_ms": 0, "occupancy_delta": 1 if final_status == "ALLOCATED" else 0,
                    }
                    topic = "bed-events"
                session.add(OutboxEventModel(
                    event_id=event_id, destination=destination, topic=topic,
                    message_key=record.pool_id, payload=json.dumps(payload),
                ))
            record.status = final_status
            record.pending_action = None
            record.recovery_claim_owner = None
            record.recovery_claim_until = None
            record.updated_at = now
            response = self._allocation_response(record)
            self._save_idempotency(
                session, idempotency_key, "POST /api/v1/allocations", request_payload, 201, response.model_dump()
            )
            session.commit()
        ALLOCATION_REQUESTS_TOTAL.labels(service="bed-allocation", pool_id=response.pool_id, outcome=final_status).inc()
        ALLOCATION_PROCESSING_SECONDS.labels(service="bed-allocation", pool_id=response.pool_id, outcome=final_status).observe(time.perf_counter() - started_at)
        return response

    def release_allocation(self, allocation_id: str, reason: str, request_id: str, idempotency_key: Optional[str] = None) -> ReleaseResponse:
        with self.session_factory() as session:
            cached = self._check_idempotency(session, idempotency_key, f"POST /api/v1/allocations/{allocation_id}/release", {"reason": reason})
            if cached:
                return ReleaseResponse(**cached)

            alloc = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            if not alloc:
                raise AppException(
                    code="NOT_FOUND",
                    message=f"Allocation '{allocation_id}' does not exist",
                    status_code=status.HTTP_404_NOT_FOUND
                )

            # Idempotent release check
            if alloc.status == "RELEASED":
                return ReleaseResponse(allocation_id=allocation_id, status="RELEASED", promoted_allocation_id=None)

            if alloc.status != "ALLOCATED":
                raise AppException(
                    code="STATE_CONFLICT",
                    message=f"Cannot release allocation in '{alloc.status}' status (must be ALLOCATED)",
                    status_code=status.HTTP_409_CONFLICT
                )

            pool_id = alloc.pool_id
            bed_id = alloc.bed_id
            staff_asgn_id = alloc.staff_assignment_id
            admission_id = alloc.admission_id

        # Acquire distributed pool lock
        lease = self.lock_client.acquire(
            pool_id=pool_id,
            owner_id=self.owner_id,
            acquire_id=f"acq-rel-{allocation_id}",
            lease_ms=10000,
            wait_timeout_ms=5000
        )

        promoted_allocation_id: Optional[str] = None
        try:
            # 1. Release Bed
            ctx_rel_bed = MutationContext(
                operation_id=f"{allocation_id}:release-bed",
                request_id=request_id,
                pool_id=pool_id,
                lock_id=lease.lock_id,
                owner_id=lease.owner_id,
                fencing_token=lease.fencing_token
            )
            if bed_id:
                self.bed_client.release_bed(ctx_rel_bed, bed_id, allocation_id)

            # 2. Release Staff
            ctx_rel_staff = MutationContext(
                operation_id=f"{allocation_id}:release-staff",
                request_id=request_id,
                pool_id=pool_id,
                lock_id=lease.lock_id,
                owner_id=lease.owner_id,
                fencing_token=lease.fencing_token
            )
            if staff_asgn_id:
                self.staff_client.release_assignment(ctx_rel_staff, staff_asgn_id, allocation_id)

            # 3. Attempt promotion of next waiting patient
            peeked = self.triage_client.peek_next(pool_id, request_id)
            if peeked:
                # Try to allocate bed for peeked waiting allocation
                candidate_alloc_id = peeked.allocation_id
                # Check beds and staff
                available_beds = self.bed_client.list_available_beds(pool_id, request_id)
                has_cap, _ = self.staff_client.check_capacity(pool_id, request_id)
                if available_beds and has_cap:
                    target_bed = available_beds[0]
                    ctx_prom_bed = MutationContext(
                        operation_id=f"{candidate_alloc_id}:prom-bed",
                        request_id=request_id,
                        pool_id=pool_id,
                        lock_id=lease.lock_id,
                        owner_id=lease.owner_id,
                        fencing_token=lease.fencing_token
                    )
                    bed_ok, _, _ = self.bed_client.allocate_bed(ctx_prom_bed, target_bed.bed_id, candidate_alloc_id)
                    if bed_ok:
                        ctx_prom_staff = MutationContext(
                            operation_id=f"{candidate_alloc_id}:prom-staff",
                            request_id=request_id,
                            pool_id=pool_id,
                            lock_id=lease.lock_id,
                            owner_id=lease.owner_id,
                            fencing_token=lease.fencing_token
                        )
                        staff_ok, p_asgn_id, _, _ = self.staff_client.assign_staff(ctx_prom_staff, candidate_alloc_id, target_bed.bed_id)
                        if staff_ok:
                            # Remove from waitlist only after successful promotion
                            ctx_rem = MutationContext(
                                operation_id=f"{candidate_alloc_id}:prom-remove",
                                request_id=request_id,
                                pool_id=pool_id,
                                lock_id=lease.lock_id,
                                owner_id=lease.owner_id,
                                fencing_token=lease.fencing_token
                            )
                            self.triage_client.remove_entry(ctx_rem, candidate_alloc_id, "promoted")
                            promoted_allocation_id = candidate_alloc_id

                            # Update promoted allocation record
                            with self.session_factory() as session:
                                p_rec = session.query(AllocationModel).filter_by(allocation_id=candidate_alloc_id).first()
                                if p_rec:
                                    p_rec.status = "ALLOCATED"
                                    p_rec.bed_id = target_bed.bed_id
                                    p_rec.staff_assignment_id = p_asgn_id
                                    p_rec.waitlist_entry_id = None
                                    p_rec.updated_at = datetime.now(timezone.utc)
                                session.commit()
                        else:
                            # Compensate bed
                            self.bed_client.release_bed(ctx_prom_bed, target_bed.bed_id, candidate_alloc_id)
        finally:
            self.lock_client.release(lease.lock_id, lease.owner_id)

        # Update database with release outcome & outbox events
        with self.session_factory() as session:
            alloc_rec = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            alloc_rec.status = "RELEASED"
            alloc_rec.updated_at = datetime.now(timezone.utc)

            now_iso = datetime.now(timezone.utc).isoformat()

            # Pub/Sub BED_RELEASED
            pubsub_event = {
                "schema_version": "1.0.0",
                "event_id": str(uuid.uuid4()),
                "event_type": "BED_RELEASED",
                "occurred_at": now_iso,
                "request_id": request_id,
                "allocation_id": allocation_id,
                "admission_id": admission_id,
                "pool_id": pool_id,
                "bed_id": bed_id,
                "ward": "ICU-A"
            }
            session.add(OutboxEventModel(
                event_id=pubsub_event["event_id"],
                destination="pubsub",
                topic="bed-notifications",
                message_key=pool_id,
                payload=json.dumps(pubsub_event)
            ))

            # Kafka BED_RELEASED (delta = -1)
            kafka_event = {
                "schema_version": "1.0.0",
                "event_id": str(uuid.uuid4()),
                "event_type": "BED_RELEASED",
                "event_time": now_iso,
                "request_id": request_id,
                "allocation_id": allocation_id,
                "admission_id": admission_id,
                "patient_id": f"pat-{admission_id}",
                "pool_id": pool_id,
                "bed_id": bed_id,
                "lock_wait_ms": 0,
                "lock_contended": False,
                "allocation_wait_ms": 0,
                "occupancy_delta": -1
            }
            session.add(OutboxEventModel(
                event_id=kafka_event["event_id"],
                destination="kafka",
                topic="bed-events",
                message_key=pool_id,
                payload=json.dumps(kafka_event)
            ))

            resp_dto = ReleaseResponse(
                allocation_id=allocation_id,
                status="RELEASED",
                promoted_allocation_id=promoted_allocation_id
            )
            self._save_idempotency(session, idempotency_key, f"POST /api/v1/allocations/{allocation_id}/release", {"reason": reason}, 200, resp_dto.model_dump())
            session.commit()

        return resp_dto

    def cancel_allocation(self, allocation_id: str, reason: str, request_id: str, idempotency_key: Optional[str] = None) -> AllocationResponse:
        with self.session_factory() as session:
            cached = self._check_idempotency(session, idempotency_key, f"POST /api/v1/allocations/{allocation_id}/cancel", {"reason": reason})
            if cached:
                return AllocationResponse(**cached)

            alloc = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            if not alloc:
                raise AppException(
                    code="NOT_FOUND",
                    message=f"Allocation '{allocation_id}' does not exist",
                    status_code=status.HTTP_404_NOT_FOUND
                )

            if alloc.status == "CANCELLED":
                return AllocationResponse(
                    allocation_id=alloc.allocation_id,
                    admission_id=alloc.admission_id,
                    pool_id=alloc.pool_id,
                    status=alloc.status,
                    created_at=alloc.created_at.isoformat()
                )

            if alloc.status != "WAITLISTED":
                raise AppException(
                    code="STATE_CONFLICT",
                    message=f"Cannot cancel allocation in '{alloc.status}' status (cancellation applies strictly to WAITLISTED)",
                    status_code=status.HTTP_409_CONFLICT
                )

            pool_id = alloc.pool_id

        # Acquire lock to remove from waitlist
        lease = self.lock_client.acquire(
            pool_id=pool_id,
            owner_id=self.owner_id,
            acquire_id=f"acq-cancel-{allocation_id}",
            lease_ms=10000,
            wait_timeout_ms=5000
        )
        try:
            ctx_cancel = MutationContext(
                operation_id=f"{allocation_id}:cancel-waitlist",
                request_id=request_id,
                pool_id=pool_id,
                lock_id=lease.lock_id,
                owner_id=lease.owner_id,
                fencing_token=lease.fencing_token
            )
            self.triage_client.remove_entry(ctx_cancel, allocation_id, f"cancelled: {reason}")
        finally:
            self.lock_client.release(lease.lock_id, lease.owner_id)

        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            record.status = "CANCELLED"
            record.waitlist_entry_id = None
            record.updated_at = datetime.now(timezone.utc)

            resp_dto = AllocationResponse(
                allocation_id=record.allocation_id,
                admission_id=record.admission_id,
                pool_id=record.pool_id,
                status=record.status,
                bed_id=record.bed_id,
                staff_assignment_id=record.staff_assignment_id,
                waitlist_entry_id=record.waitlist_entry_id,
                created_at=record.created_at.isoformat()
            )
            self._save_idempotency(session, idempotency_key, f"POST /api/v1/allocations/{allocation_id}/cancel", {"reason": reason}, 200, resp_dto.model_dump())
            session.commit()

        return resp_dto

    def get_allocation(self, allocation_id: str) -> AllocationResponse:
        with self.session_factory() as session:
            alloc = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            if not alloc:
                raise AppException(
                    code="NOT_FOUND",
                    message=f"Allocation '{allocation_id}' does not exist",
                    status_code=status.HTTP_404_NOT_FOUND
                )
            return self._allocation_response(alloc)

    def list_allocations(
        self,
        pool_id: Optional[str] = None,
        status_filter: Optional[str] = None,
        admission_id: Optional[str] = None,
        limit: int = 50,
        cursor: Optional[str] = None
    ) -> Tuple[List[AllocationResponse], Optional[str]]:
        with self.session_factory() as session:
            query = session.query(AllocationModel)
            if pool_id:
                query = query.filter(AllocationModel.pool_id == pool_id)
            if status_filter:
                query = query.filter(AllocationModel.status == status_filter)
            if admission_id:
                query = query.filter(AllocationModel.admission_id == admission_id)

            query = query.order_by(AllocationModel.created_at.asc(), AllocationModel.allocation_id.asc())

            if cursor:
                cursor_item = session.query(AllocationModel).filter_by(allocation_id=cursor).first()
                if cursor_item:
                    query = query.filter(
                        (AllocationModel.created_at > cursor_item.created_at) |
                        ((AllocationModel.created_at == cursor_item.created_at) & (AllocationModel.allocation_id > cursor_item.allocation_id))
                    )

            items = query.limit(limit).all()
            results = [
                AllocationResponse(
                    allocation_id=a.allocation_id,
                    admission_id=a.admission_id,
                    pool_id=a.pool_id,
                    status=a.status,
                    bed_id=a.bed_id,
                    staff_assignment_id=a.staff_assignment_id,
                    waitlist_entry_id=a.waitlist_entry_id,
                    created_at=a.created_at.isoformat()
                )
                for a in items
            ]
            next_cursor = results[-1].allocation_id if len(results) == limit else None
            return results, next_cursor
