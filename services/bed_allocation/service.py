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
    BedDto, BedRegistryClient, LockCoordinatorClient, PatientRegistryClient,
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
        outcome_unknown = isinstance(error, AppException) and error.code == "OUTCOME_UNKNOWN"
        durable_action = "RECONCILIATION_REQUIRED" if outcome_unknown else action
        self._record_step(
            allocation_id, durable_action, operation_id, step, "UNCERTAIN",
            {"error_type": type(error).__name__, "error_code": getattr(error, "code", None)}, allocation_status="RECOVERING",
        )
        with self.session_factory() as session:
            allocation = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            if outcome_unknown:
                workflow = json.loads(allocation.workflow_data or "{}")
                workflow["reconciliation_needed"] = True
                workflow["reconciliation_reason"] = "TRIAGE_OUTCOME_UNKNOWN"
                allocation.workflow_data = json.dumps(workflow)
                allocation.recovery_claim_owner = None
                allocation.recovery_claim_until = None
            else:
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
                    AllocationModel.pending_action.isnot(None),
                    AllocationModel.pending_action != "RECONCILIATION_REQUIRED",
                    or_(
                        AllocationModel.status.in_(["PROCESSING", "RECOVERING"]),
                        (AllocationModel.status == "RELEASED") & (AllocationModel.pending_action == "PROMOTE"),
                    ),
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
                if record.status != "RELEASED":
                    record.status = "RECOVERING"
                record.updated_at = now
                claimed_ids.append(record.allocation_id)
            session.commit()

        for allocation_id in claimed_ids:
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                if not record or record.pending_action is None:
                    continue
                data = json.loads(record.workflow_data or "{}")
                action = data.get("action", "CREATE")
                pending_action = record.pending_action
                request_id = data.get("request_id", f"recovery-{uuid.uuid4().hex[:12]}")
                key = data.get("idempotency_key")
                pool_id = record.pool_id
                if not key:
                    operation_name = {"CREATE": "CREATE", "RELEASE": "RELEASE", "CANCEL": "CANCEL"}.get(action)
                    target_id = record.admission_id if action == "CREATE" else allocation_id
                    claim = session.query(IdempotencyModel).filter_by(
                        method="POST", operation=operation_name, target_id=target_id,
                        allocation_id=allocation_id,
                    ).first()
                    key = claim.idempotency_key if claim else None
                if action == "CREATE":
                    payload = {"admission_id": record.admission_id, "pool_id": pool_id}
                else:
                    payload = {"reason": data.get("reason", "recovery")}
            try:
                if action == "CREATE":
                    self._run_create_workflow(allocation_id, request_id, key, payload)
                elif action == "RELEASE" and pending_action == "PROMOTE":
                    self._resume_promotion(allocation_id, request_id)
                elif action == "RELEASE":
                    self._resume_release(allocation_id, request_id, key, payload)
                elif action == "CANCEL":
                    self._resume_cancel(allocation_id, request_id, key, payload)
            except Exception:
                # Leave the durable row claimable on its lease timeout. Reconciliation
                # failures must never be converted into terminal allocation failures.
                continue
        return len(claimed_ids)

    def _resume_promotion(self, allocation_id, request_id):
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            pool_id = record.pool_id
        try:
            lease = self.lock_client.acquire(pool_id, self.owner_id, f"acq-promotion-{uuid.uuid4().hex}", 10000, 5000)
        except Exception:
            return
        try:
            self._promote_next_locked(allocation_id, lease, request_id)
        finally:
            self.lock_client.release(lease.lock_id, lease.owner_id)

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

    def _claim_mutating_action(self, allocation_id, endpoint, payload, state, action, workflow):
        scope = self._idempotency_scope(endpoint, payload)
        key = workflow.get("idempotency_key")
        with self.session_factory() as session:
            cached = self._check_idempotency(session, key, endpoint, payload)
            if cached is not None:
                return cached
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().first()
            if record is None:
                raise AppException("NOT_FOUND", f"Allocation '{allocation_id}' does not exist", status.HTTP_404_NOT_FOUND)
            cached = self._check_idempotency(session, key, endpoint, payload)
            if cached is not None:
                return cached
            allowed_initial = "ALLOCATED" if action == "RELEASE_BED" else "WAITLISTED"
            if record.status != allowed_initial:
                raise AppException("STATE_CONFLICT", f"Cannot start {action.lower()} in '{record.status}' status", status.HTTP_409_CONFLICT)
            if record.pending_action:
                raise AppException("STATE_CONFLICT", f"Allocation has pending action '{record.pending_action}'", status.HTTP_409_CONFLICT)
            record.status = state
            record.pending_action = action
            record.workflow_data = json.dumps(workflow)
            record.recovery_claim_owner = self.owner_id
            record.recovery_claim_until = datetime.now(timezone.utc) + timedelta(seconds=60)
            record.updated_at = datetime.now(timezone.utc)
            if key:
                claim = session.query(IdempotencyModel).filter_by(**scope, idempotency_key=key).first()
                if claim is None:
                    claim = IdempotencyModel(
                        **scope, idempotency_key=key, endpoint=endpoint,
                        request_hash=self._hash_payload(payload), state="IN_PROGRESS",
                        allocation_id=allocation_id, action_id=f"{allocation_id}:{action.lower()}",
                        created_at=datetime.now(timezone.utc), updated_at=datetime.now(timezone.utc),
                    )
                    session.add(claim)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                with self.session_factory() as retry_session:
                    winner = self._check_idempotency(retry_session, key, endpoint, payload)
                    if winner is not None:
                        return winner
                raise AppException("ACTIVE_ALLOCATION_EXISTS", "A competing action claimed this allocation", status.HTTP_409_CONFLICT)
            return None

    def _complete_mutating_claim(self, session, allocation_id, endpoint, payload, key, response):
        self._save_idempotency(session, key, endpoint, payload, 200, response.model_dump())

    def _release_operation(self, allocation_id, step, lease, request_id, remote_call):
        operation_id = f"{allocation_id}:{step.lower().replace('_', '-')}"
        previous = self._read_operation(operation_id)
        if previous and previous["status"] == "SUCCEEDED":
            return previous.get("details_data", {})
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            data = json.loads(record.workflow_data or "{}")
        self._record_step(allocation_id, step, operation_id, step, "INTENDED", allocation_status="RELEASING")
        try:
            result = remote_call(operation_id, lease, data)
            if result is False or (isinstance(result, tuple) and result and result[0] is False):
                raise RuntimeError(f"{step} was not confirmed")
        except Exception as error:
            self._mark_recovering(allocation_id, step, operation_id, step, error)
            return None
        details = result if isinstance(result, dict) else {}
        self._record_step(allocation_id, step, operation_id, step, "SUCCEEDED", details, "RELEASING")
        return details

    def _finalize_release(self, allocation_id, idempotency_key, payload, request_id):
        endpoint = f"POST /api/v1/allocations/{allocation_id}/release"
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().one()
            data = json.loads(record.workflow_data or "{}")
            now = datetime.now(timezone.utc)
            response = ReleaseResponse(allocation_id=allocation_id, status="RELEASED")
            record.status = "RELEASED"
            record.pending_action = "PROMOTE"
            data.update({"stage": "PROMOTE", "request_id": request_id})
            record.workflow_data = json.dumps(data)
            record.recovery_claim_until = now
            record.updated_at = now
            # Stable event IDs make recovery safe if the transaction is retried.
            for destination, topic in (("pubsub", "bed-notifications"), ("kafka", "bed-events")):
                event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"allocation:{allocation_id}:{destination}:BED_RELEASED"))
                if session.query(OutboxEventModel).filter_by(event_id=event_id).first():
                    continue
                event = {
                    "schema_version": "1.0.0", "event_id": event_id, "event_type": "BED_RELEASED",
                    ("occurred_at" if destination == "pubsub" else "event_time"): now.isoformat(),
                    "request_id": request_id, "allocation_id": allocation_id,
                    "admission_id": record.admission_id, "pool_id": record.pool_id,
                    "bed_id": record.bed_id, "occupancy_delta": -1,
                }
                session.add(OutboxEventModel(event_id=event_id, destination=destination, topic=topic,
                    message_key=record.pool_id, payload=json.dumps(event)))
            self._complete_mutating_claim(session, allocation_id, endpoint, payload, idempotency_key, response)
            session.commit()
            return record.pool_id

    def _promotion_context(self, operation_id, request_id, lease):
        return MutationContext(operation_id=operation_id, request_id=request_id, pool_id=lease.pool_id,
            lock_id=lease.lock_id, owner_id=lease.owner_id, fencing_token=lease.fencing_token)

    def _promote_next_locked(self, allocation_id, lease, request_id):
        """Attempt one priority-ordered promotion while holding the already acquired pool lease."""
        with self.session_factory() as session:
            release_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            data = json.loads(release_record.workflow_data or "{}")
            attempt = int(data.get("promotion_attempt", 1))
            candidate_id = data.get("promotion_target")
        compensation_id = f"{candidate_id}:prom-compensate-bed:{attempt}" if candidate_id else None
        compensation = self._read_operation(compensation_id) if compensation_id else None
        if compensation and compensation["status"] in {"INTENDED", "UNCERTAIN"}:
            bed_id = compensation["details"].get("bed_id")
            released, _ = self.bed_client.release_bed(
                self._promotion_context(compensation_id, request_id, lease), bed_id, candidate_id
            )
            if not released:
                raise RuntimeError("Promotion compensation remains unconfirmed")
            self._record_step(allocation_id, "PROMOTE", compensation_id, "PROMOTE_COMPENSATE_BED", "SUCCEEDED", compensation["details"], "RELEASED")
            with self.session_factory() as session:
                release_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                data = json.loads(release_record.workflow_data or "{}")
                data.update({"stage": "PROMOTE", "promotion_attempt": attempt + 1})
                data.pop("promotion_target", None)
                release_record.workflow_data = json.dumps(data)
                release_record.pending_action = "PROMOTE"
                release_record.status = "RELEASED"
                release_record.recovery_claim_until = datetime.now(timezone.utc)
                session.commit()
            return None
        bed_op = f"{candidate_id}:prom-bed:{attempt}" if candidate_id else None
        staff_op = f"{candidate_id}:prom-staff:{attempt}" if candidate_id else None
        remove_op = f"{candidate_id}:prom-remove:{attempt}" if candidate_id else None
        existing_effect = any(self._read_operation(op_id) for op_id in (bed_op, staff_op, remove_op) if op_id)
        peeked = self.triage_client.peek_next(lease.pool_id, request_id)
        if candidate_id and existing_effect:
            # Continue the already-started target even if queue removal may have
            # committed before this process died.
            pass
        elif not peeked:
            with self.session_factory() as session:
                release_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                release_record.pending_action = None
                release_record.recovery_claim_owner = None
                release_record.recovery_claim_until = None
                release_record.workflow_data = json.dumps({**json.loads(release_record.workflow_data or "{}"), "stage": "DONE"})
                session.commit()
            return None
        else:
            candidate_id = peeked.allocation_id
            with self.session_factory() as session:
                release_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                data = json.loads(release_record.workflow_data or "{}")
                data.update({"stage": "PROMOTE", "promotion_target": candidate_id})
                release_record.workflow_data = json.dumps(data)
                release_record.pending_action = "PROMOTE"
                session.commit()
            attempt = int(data.get("promotion_attempt", 1))
            bed_op = f"{candidate_id}:prom-bed:{attempt}"
            staff_op = f"{candidate_id}:prom-staff:{attempt}"
            remove_op = f"{candidate_id}:prom-remove:{attempt}"
            existing_effect = False

        if self._read_operation(remove_op) and self._read_operation(remove_op)["status"] == "SUCCEEDED":
            staff_result = self._read_operation(staff_op)
            details = staff_result["details"] if staff_result else {}
            with self.session_factory() as session:
                candidate = session.query(AllocationModel).filter_by(allocation_id=candidate_id).with_for_update().first()
                if candidate:
                    candidate.status = "ALLOCATED"
                    candidate.bed_id = details.get("bed_id", candidate.bed_id)
                    candidate.staff_assignment_id = details.get("staff_assignment_id", candidate.staff_assignment_id)
                    candidate.waitlist_entry_id = None
                release_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                release_record.pending_action = None
                release_record.recovery_claim_owner = None
                release_record.recovery_claim_until = None
                release_record.workflow_data = json.dumps({**json.loads(release_record.workflow_data or "{}"), "stage": "DONE"})
                session.commit()
            return candidate_id

        if not existing_effect:
            current = self.triage_client.peek_next(lease.pool_id, request_id)
            if not current or current.allocation_id != candidate_id:
                return None

        prior_bed = self._read_operation(bed_op)
        target_bed_id = (prior_bed["details"].get("bed_id") if prior_bed else None)
        available_beds = self.bed_client.list_available_beds(lease.pool_id, request_id)
        if not target_bed_id:
            if not available_beds:
                return None
            target_bed_id = available_beds[0].bed_id
        has_capacity, _ = self.staff_client.check_capacity(lease.pool_id, request_id)
        if not has_capacity and not self._read_operation(staff_op):
            return None
        target_bed = next((bed for bed in available_beds if bed.bed_id == target_bed_id), None)
        if target_bed is None:
            target_bed = BedDto(bed_id=target_bed_id, pool_id=lease.pool_id, ward="ICU-A", status="OCCUPIED", current_allocation_id=candidate_id)
        try:
            self._record_step(allocation_id, "PROMOTE", bed_op, "PROMOTE_BED", "INTENDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
            bed_ok, _, _ = self.bed_client.allocate_bed(self._promotion_context(bed_op, request_id, lease), target_bed.bed_id, candidate_id)
            if not bed_ok:
                self._record_step(allocation_id, "PROMOTE", bed_op, "PROMOTE_BED", "FAILED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
                with self.session_factory() as session:
                    record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                    data = json.loads(record.workflow_data or "{}")
                    data["promotion_attempt"] = attempt + 1
                    data.pop("promotion_target", None)
                    record.workflow_data = json.dumps(data)
                    record.pending_action = "PROMOTE"
                    record.status = "RELEASED"
                    record.recovery_claim_until = datetime.now(timezone.utc)
                    session.commit()
                return None
            self._record_step(allocation_id, "PROMOTE", bed_op, "PROMOTE_BED", "SUCCEEDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
            self._record_step(allocation_id, "PROMOTE", staff_op, "PROMOTE_STAFF", "INTENDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
            staff_ok, assignment_id, _, _ = self.staff_client.assign_staff(self._promotion_context(staff_op, request_id, lease), candidate_id, target_bed.bed_id)
            if not staff_ok:
                # Never remove the queue entry. Persist compensation intent before releasing our bed.
                compensation_id = f"{candidate_id}:prom-compensate-bed:{attempt}"
                self._record_step(allocation_id, "PROMOTE", compensation_id, "PROMOTE_COMPENSATE_BED", "INTENDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
                released, _ = self.bed_client.release_bed(self._promotion_context(compensation_id, request_id, lease), target_bed.bed_id, candidate_id)
                if not released:
                    raise RuntimeError("Promotion compensation was not confirmed")
                self._record_step(allocation_id, "PROMOTE", compensation_id, "PROMOTE_COMPENSATE_BED", "SUCCEEDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id}, "RELEASED")
                with self.session_factory() as session:
                    record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                    data = json.loads(record.workflow_data or "{}")
                    data["promotion_attempt"] = attempt + 1
                    data["stage"] = "PROMOTE"
                    record.workflow_data = json.dumps(data)
                    record.pending_action = "PROMOTE"
                    record.status = "RELEASED"
                    record.recovery_claim_until = datetime.now(timezone.utc)
                    session.commit()
                return None
            self._record_step(allocation_id, "PROMOTE", staff_op, "PROMOTE_STAFF", "SUCCEEDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id, "staff_assignment_id": assignment_id}, "RELEASED")
            self._record_step(allocation_id, "PROMOTE", remove_op, "PROMOTE_REMOVE_QUEUE", "INTENDED", {"candidate_id": candidate_id}, "RELEASED")
            removed = self.triage_client.remove_entry(self._promotion_context(remove_op, request_id, lease), candidate_id, "promoted")
            if not removed:
                raise RuntimeError("Promotion queue removal was not confirmed")
            self._record_step(allocation_id, "PROMOTE", remove_op, "PROMOTE_REMOVE_QUEUE", "SUCCEEDED", {"candidate_id": candidate_id}, "RELEASED")
            self._record_step(allocation_id, "PROMOTE", f"{candidate_id}:promotion-confirmed", "PROMOTION_CONFIRMED", "SUCCEEDED", {"candidate_id": candidate_id, "bed_id": target_bed.bed_id, "staff_assignment_id": assignment_id}, "RELEASED")
            with self.session_factory() as session:
                candidate = session.query(AllocationModel).filter_by(allocation_id=candidate_id).with_for_update().first()
                if candidate:
                    candidate.status = "ALLOCATED"
                    candidate.bed_id = target_bed.bed_id
                    candidate.staff_assignment_id = assignment_id
                    candidate.waitlist_entry_id = None
                    candidate.updated_at = datetime.now(timezone.utc)
                released_record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                released_record.pending_action = None
                released_record.recovery_claim_owner = None
                released_record.recovery_claim_until = None
                released_record.workflow_data = json.dumps({**json.loads(released_record.workflow_data or "{}"), "stage": "DONE", "promotion_target": candidate_id})
                release_claim = session.query(IdempotencyModel).filter_by(
                    method="POST", operation="RELEASE", target_id=allocation_id,
                    allocation_id=allocation_id,
                ).first()
                if release_claim and release_claim.state == "COMPLETED":
                    body = json.loads(release_claim.response_body or "{}")
                    body["promoted_allocation_id"] = candidate_id
                    release_claim.response_body = json.dumps(body)
                    release_claim.updated_at = datetime.now(timezone.utc)
                session.commit()
            return candidate_id
        except Exception as error:
            self._mark_recovering(allocation_id, "PROMOTE", f"{candidate_id}:promotion", "PROMOTION", error)
            # The released allocation stays RELEASED; its durable promotion action remains retryable.
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                record.status = "RELEASED"
                if isinstance(error, AppException) and error.code == "OUTCOME_UNKNOWN":
                    record.pending_action = "RECONCILIATION_REQUIRED"
                    workflow = json.loads(record.workflow_data or "{}")
                    workflow["reconciliation_needed"] = True
                    workflow["reconciliation_reason"] = "TRIAGE_OUTCOME_UNKNOWN"
                    record.workflow_data = json.dumps(workflow)
                    record.recovery_claim_owner = None
                    record.recovery_claim_until = None
                else:
                    record.pending_action = "PROMOTE"
                    record.recovery_claim_until = datetime.now(timezone.utc)
                session.commit()
            return None

    def release_allocation(self, allocation_id: str, reason: str, request_id: str, idempotency_key: Optional[str] = None):
        endpoint = f"POST /api/v1/allocations/{allocation_id}/release"
        payload = {"reason": reason}
        workflow = {"action": "RELEASE", "reason": reason, "request_id": request_id, "idempotency_key": idempotency_key, "stage": "RELEASE_BED"}
        claimed = self._claim_mutating_action(allocation_id, endpoint, payload, "RELEASING", "RELEASE_BED", workflow)
        if claimed is not None:
            return claimed if isinstance(claimed, (AllocationResponse, ReleaseResponse)) else ReleaseResponse(**claimed)
        return self._resume_release(allocation_id, request_id, idempotency_key, payload)

    def _resume_release(self, allocation_id, request_id, idempotency_key, payload):
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            data = json.loads(record.workflow_data or "{}")
            pool_id, bed_id, assignment_id = record.pool_id, record.bed_id, record.staff_assignment_id
        try:
            lease = self.lock_client.acquire(pool_id, self.owner_id, f"acq-release-{uuid.uuid4().hex}", 10000, 5000)
        except Exception as error:
            return self._mark_recovering(allocation_id, "RELEASE_BED", f"{allocation_id}:release-bed", "RELEASE_BED", error)
        released = False
        try:
            if bed_id:
                result = self._release_operation(allocation_id, "RELEASE_BED", lease, request_id,
                    lambda op, current, _: self.bed_client.release_bed(self._promotion_context(op, request_id, current), bed_id, allocation_id))
                if result is None:
                    return self._allocation_snapshot(allocation_id)
            if assignment_id:
                result = self._release_operation(allocation_id, "RELEASE_STAFF", lease, request_id,
                    lambda op, current, _: self.staff_client.release_assignment(self._promotion_context(op, request_id, current), assignment_id, allocation_id))
                if result is None:
                    return self._allocation_snapshot(allocation_id)
            pool_id = self._finalize_release(allocation_id, idempotency_key, payload, request_id)
            released = True
            # Release is durable before promotion; failure can only leave the queue and retry action intact.
            try:
                self._promote_next_locked(allocation_id, lease, request_id)
            except Exception:
                # The release transaction is already committed and its response cached.
                # Keep promotion claimable without changing the confirmed release.
                with self.session_factory() as session:
                    record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                    record.status = "RELEASED"
                    record.pending_action = "PROMOTE"
                    record.recovery_claim_until = datetime.now(timezone.utc)
                    session.commit()
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                data = json.loads(record.workflow_data or "{}")
                candidate = data.get("promotion_target") if record.pending_action is None else None
                pending_action = record.pending_action
            return ReleaseResponse(allocation_id=allocation_id, status="RELEASED", promoted_allocation_id=candidate,
                pending_action=None)
        except Exception as error:
            return self._mark_recovering(allocation_id, "RELEASE_BED" if not released else "PROMOTE", f"{allocation_id}:release-recovery", "RELEASE", error)
        finally:
            self.lock_client.release(lease.lock_id, lease.owner_id)

    def cancel_allocation(self, allocation_id: str, reason: str, request_id: str, idempotency_key: Optional[str] = None):
        endpoint = f"POST /api/v1/allocations/{allocation_id}/cancel"
        payload = {"reason": reason}
        workflow = {"action": "CANCEL", "reason": reason, "request_id": request_id, "idempotency_key": idempotency_key, "stage": "CANCEL_QUEUE"}
        claimed = self._claim_mutating_action(allocation_id, endpoint, payload, "CANCELLING", "CANCEL_QUEUE", workflow)
        if claimed is not None:
            return claimed if isinstance(claimed, AllocationResponse) else AllocationResponse(**claimed)
        return self._resume_cancel(allocation_id, request_id, idempotency_key, payload)

    def _resume_cancel(self, allocation_id, request_id, idempotency_key, payload):
        endpoint = f"POST /api/v1/allocations/{allocation_id}/cancel"
        try:
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                data = json.loads(record.workflow_data or "{}")
                pool_id, reason = record.pool_id, data.get("reason", "cancelled")
            lease = self.lock_client.acquire(pool_id, self.owner_id, f"acq-cancel-{uuid.uuid4().hex}", 10000, 5000)
        except Exception as error:
            return self._mark_recovering(allocation_id, "CANCEL_QUEUE", f"{allocation_id}:cancel-waitlist", "CANCEL_QUEUE", error)
        try:
            op_id = f"{allocation_id}:cancel-waitlist"
            self._record_step(allocation_id, "CANCEL_QUEUE", op_id, "CANCEL_QUEUE", "INTENDED", allocation_status="CANCELLING")
            try:
                removed = self.triage_client.remove_entry(self._promotion_context(op_id, request_id, lease), allocation_id, f"cancelled: {reason}")
                if not removed:
                    raise RuntimeError("Waitlist removal was not confirmed")
            except Exception as error:
                return self._mark_recovering(allocation_id, "CANCEL_QUEUE", op_id, "CANCEL_QUEUE", error)
            self._record_step(allocation_id, "CANCEL_CONFIRMED", op_id, "CANCEL_QUEUE", "SUCCEEDED", allocation_status="CANCELLING")
            with self.session_factory() as session:
                record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).with_for_update().one()
                record.status = "CANCELLED"
                record.pending_action = None
                record.waitlist_entry_id = None
                record.recovery_claim_owner = None
                record.recovery_claim_until = None
                record.updated_at = datetime.now(timezone.utc)
                response = self._allocation_response(record)
                self._complete_mutating_claim(session, allocation_id, endpoint, payload, idempotency_key, response)
                session.commit()
                return response
        finally:
            self.lock_client.release(lease.lock_id, lease.owner_id)

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
