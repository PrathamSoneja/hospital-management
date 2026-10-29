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
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from services.bed_allocation.clients.base import (
    BedRegistryClient, LockCoordinatorClient, PatientRegistryClient,
    StaffSchedulerClient, TriageClient
)
from services.bed_allocation.db import (
    AllocationModel, IdempotencyModel, OperationRecoveryModel, OutboxEventModel
)
from services.bed_allocation.models import (
    AllocationResponse, CreateAllocationRequest, ReleaseResponse
)
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

    def _check_idempotency(self, session: Session, key: Optional[str], endpoint: str, payload: dict) -> Optional[dict]:
        if not key:
            return None
        existing = session.query(IdempotencyModel).filter_by(idempotency_key=key).first()
        if not existing:
            return None
        req_hash = self._hash_payload(payload)
        if existing.request_hash != req_hash:
            raise AppException(
                code="ALREADY_EXISTS",
                message=f"Idempotency key '{key}' was previously used with a different request payload",
                status_code=status.HTTP_409_CONFLICT
            )
        return json.loads(existing.response_body)

    def _save_idempotency(self, session: Session, key: Optional[str], endpoint: str, payload: dict, resp_status: int, response_data: dict):
        if not key:
            return
        model = IdempotencyModel(
            idempotency_key=key,
            endpoint=endpoint,
            request_hash=self._hash_payload(payload),
            response_status=resp_status,
            response_body=json.dumps(response_data)
        )
        session.merge(model)

    def create_allocation(self, req: CreateAllocationRequest, request_id: str, idempotency_key: Optional[str] = None) -> AllocationResponse:
        start_time = time.perf_counter()
        with self.session_factory() as session:
            # 1. Durable Idempotency Check
            cached = self._check_idempotency(session, idempotency_key, "POST /api/v1/allocations", req.model_dump())
            if cached:
                return AllocationResponse(**cached)

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

            # 5. Persist initial allocation in REQUESTED state
            allocation_id = f"alloc-{uuid.uuid4().hex[:10]}"
            alloc_record = AllocationModel(
                allocation_id=allocation_id,
                admission_id=req.admission_id,
                pool_id=req.pool_id,
                status="REQUESTED"
            )
            session.add(alloc_record)
            try:
                session.commit()
            except IntegrityError as error:
                session.rollback()
                if _is_active_admission_conflict(error):
                    raise AppException(
                        code="ACTIVE_ALLOCATION_EXISTS",
                        message=f"An active allocation or waitlist entry already exists for admission '{req.admission_id}'",
                        status_code=status.HTTP_409_CONFLICT
                    ) from error
                raise

        # 6. Acquire distributed pool lock
        t_lock_start = time.perf_counter()
        lease = self.lock_client.acquire(
            pool_id=req.pool_id,
            owner_id=self.owner_id,
            acquire_id=f"acq-{allocation_id}",
            lease_ms=10000,
            wait_timeout_ms=5000
        )
        lock_wait_s = time.perf_counter() - t_lock_start
        LOCK_WAIT_SECONDS.labels(service="bed-allocation", pool_id=req.pool_id).observe(lock_wait_s)
        lock_wait_ms = int(lock_wait_s * 1000)

        chosen_bed_id: Optional[str] = None
        assigned_staff_id: Optional[str] = None
        assigned_staff_assignment_id: Optional[str] = None
        waitlist_entry_id: Optional[str] = None
        final_status = "WAITLISTED"

        try:
            # Under distributed pool lock
            # 7. Check if priority waitlist exists ahead of this patient
            peeked = self.triage_client.peek_next(req.pool_id, request_id)
            # If waitlist has waiting patients, this request is queued to preserve priority
            if peeked and peeked.admission_id != req.admission_id:
                final_status = "WAITLISTED"
            else:
                # 8. Check available beds in pool
                beds = self.bed_client.list_available_beds(req.pool_id, request_id)
                if not beds:
                    final_status = "WAITLISTED"
                else:
                    # 9. Check staff capacity
                    has_cap, cap = self.staff_client.check_capacity(req.pool_id, request_id)
                    if not has_cap:
                        # No-staff capacity policy: waitlist rather than fail
                        final_status = "WAITLISTED"
                    else:
                        # Attempt conditional bed reservation
                        target_bed = beds[0]
                        ctx_bed = MutationContext(
                            operation_id=f"{allocation_id}:reserve-bed",
                            request_id=request_id,
                            pool_id=req.pool_id,
                            lock_id=lease.lock_id,
                            owner_id=lease.owner_id,
                            fencing_token=lease.fencing_token
                        )
                        bed_ok, bed_obj, bed_err = self.bed_client.allocate_bed(ctx_bed, target_bed.bed_id, allocation_id)
                        if not bed_ok:
                            final_status = "WAITLISTED"
                        else:
                            chosen_bed_id = target_bed.bed_id
                            # Bed reserved. Now assign staff
                            ctx_staff = MutationContext(
                                operation_id=f"{allocation_id}:assign-staff",
                                request_id=request_id,
                                pool_id=req.pool_id,
                                lock_id=lease.lock_id,
                                owner_id=lease.owner_id,
                                fencing_token=lease.fencing_token
                            )
                            try:
                                staff_ok, staff_asgn_id, s_id, s_err = self.staff_client.assign_staff(ctx_staff, allocation_id, chosen_bed_id)
                            except AppException as e:
                                # Uncertain outcome: mark recovering
                                with self.session_factory() as session:
                                    rec = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
                                    rec.status = "RECOVERING"
                                    rec.bed_id = chosen_bed_id
                                    session.add(OperationRecoveryModel(
                                        operation_id=ctx_staff.operation_id,
                                        allocation_id=allocation_id,
                                        step="ASSIGN_STAFF",
                                        status="UNCERTAIN",
                                        details=str(e)
                                    ))
                                    session.commit()
                                raise

                            if staff_ok:
                                final_status = "ALLOCATED"
                                assigned_staff_assignment_id = staff_asgn_id
                                assigned_staff_id = s_id
                            else:
                                # Compensation: Release reserved bed with expected allocation_id
                                self.bed_client.release_bed(ctx_bed, chosen_bed_id, allocation_id)
                                chosen_bed_id = None
                                final_status = "WAITLISTED"

            # If outcome is WAITLISTED, enqueue into Triage waitlist
            if final_status == "WAITLISTED":
                ctx_enq = MutationContext(
                    operation_id=f"{allocation_id}:enqueue-waitlist",
                    request_id=request_id,
                    pool_id=req.pool_id,
                    lock_id=lease.lock_id,
                    owner_id=lease.owner_id,
                    fencing_token=lease.fencing_token
                )
                wl_entry = self.triage_client.enqueue(ctx_enq, allocation_id, req.admission_id)
                waitlist_entry_id = wl_entry.entry_id

        finally:
            # 10. Always release pool lock before outbox/network publishing
            self.lock_client.release(lease.lock_id, lease.owner_id)

        # 11. Persist outcome, outbox events, and idempotency in atomic DB transaction
        with self.session_factory() as session:
            record = session.query(AllocationModel).filter_by(allocation_id=allocation_id).first()
            record.status = final_status
            record.bed_id = chosen_bed_id
            record.staff_assignment_id = assigned_staff_assignment_id
            record.waitlist_entry_id = waitlist_entry_id
            record.updated_at = datetime.now(timezone.utc)

            # Durable events in transactional outbox
            now_iso = datetime.now(timezone.utc).isoformat()

            # Pub/Sub Notification Event
            pubsub_payload = {
                "schema_version": "1.0.0",
                "event_id": str(uuid.uuid4()),
                "event_type": "BED_ALLOCATED" if final_status == "ALLOCATED" else "ALLOCATION_WAITLISTED",
                "occurred_at": now_iso,
                "request_id": request_id,
                "allocation_id": allocation_id,
                "admission_id": req.admission_id,
                "pool_id": req.pool_id,
                "bed_id": chosen_bed_id,
                "ward": "ICU-A"
            }
            session.add(OutboxEventModel(
                event_id=pubsub_payload["event_id"],
                destination="pubsub",
                topic="bed-notifications",
                message_key=req.pool_id,
                payload=json.dumps(pubsub_payload)
            ))

            # Kafka Stream Analytics Event
            kafka_payload = {
                "schema_version": "1.0.0",
                "event_id": str(uuid.uuid4()),
                "event_type": "BED_ALLOCATED" if final_status == "ALLOCATED" else "ALLOCATION_WAITLISTED",
                "event_time": now_iso,
                "request_id": request_id,
                "allocation_id": allocation_id,
                "admission_id": req.admission_id,
                "patient_id": adm.patient_id,
                "pool_id": req.pool_id,
                "bed_id": chosen_bed_id,
                "lock_wait_ms": lock_wait_ms,
                "lock_contended": (lock_wait_ms > 0),
                "allocation_wait_ms": 0,
                "occupancy_delta": 1 if final_status == "ALLOCATED" else 0
            }
            session.add(OutboxEventModel(
                event_id=kafka_payload["event_id"],
                destination="kafka",
                topic="bed-events",
                message_key=req.pool_id,
                payload=json.dumps(kafka_payload)
            ))

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

            self._save_idempotency(session, idempotency_key, "POST /api/v1/allocations", req.model_dump(), 201, resp_dto.model_dump())
            session.commit()

        ALLOCATION_REQUESTS_TOTAL.labels(service="bed-allocation", pool_id=req.pool_id, outcome=final_status).inc()
        ALLOCATION_PROCESSING_SECONDS.labels(service="bed-allocation", pool_id=req.pool_id, outcome=final_status).observe(time.perf_counter() - start_time)
        return resp_dto

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
            return AllocationResponse(
                allocation_id=alloc.allocation_id,
                admission_id=alloc.admission_id,
                pool_id=alloc.pool_id,
                status=alloc.status,
                bed_id=alloc.bed_id,
                staff_assignment_id=alloc.staff_assignment_id,
                waitlist_entry_id=alloc.waitlist_entry_id,
                created_at=alloc.created_at.isoformat()
            )

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
