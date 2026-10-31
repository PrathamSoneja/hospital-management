"""
Pure domain business logic for Triage and Waitlist operations.
Decoupled from REST and gRPC transports.
"""
import time
import uuid
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional, Tuple
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.base import TriageRepository
from services.triage.idempotency import build_put_claim
from services.triage.validation import FencingValidator
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import AppException
from shared.observability.metrics import STALE_FENCING_REJECTIONS_TOTAL
from starlette import status

logger = logging.getLogger("triage-service")

_last_ts_ns = 0

def get_monotonic_rfc3339() -> str:
    global _last_ts_ns
    current_ns = time.time_ns()
    if current_ns <= _last_ts_ns:
        current_ns = _last_ts_ns + 1000  # advance at least 1 microsecond
    _last_ts_ns = current_ns
    dt = datetime.fromtimestamp(current_ns / 1e9, tz=timezone.utc)
    return dt.isoformat()

class TriageDomainService:
    def __init__(self, repo: TriageRepository, validator: FencingValidator,
                 lock_client=None, owner_id: str = "triage-service"):
        self.repo = repo
        self.validator = validator
        self.lock_client = lock_client
        self.owner_id = owner_id

    def put_triage(self, admission_id: str, triage_level: int, pool_id: str,
                   idempotency_key: str, request_id: str) -> TriageRecord:
        if triage_level < 1 or triage_level > 5:
            raise AppException("INVALID_ARGUMENT", f"Triage level must be between 1 and 5, got {triage_level}", status.HTTP_400_BAD_REQUEST)
        claim = build_put_claim(admission_id, idempotency_key, triage_level, pool_id)
        replay = self.repo.lookup_triage_idempotency(claim)
        if replay is not None:
            return replay
        if self.lock_client is None:
            raise AppException("SERVICE_UNAVAILABLE", "Lock Coordinator is not configured", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

        lease_ms = int(os.environ.get("TRIAGE_LOCK_LEASE_MS", "10000"))
        wait_timeout_ms = int(os.environ.get("TRIAGE_LOCK_WAIT_TIMEOUT_MS", "5000"))
        lease = self.lock_client.acquire(
            pool_id, self.owner_id, f"triage-put:{claim['claim_id']}",
            lease_ms, wait_timeout_ms, request_id=request_id,
        )
        try:
            context = MutationContext(
                operation_id=f"triage-put:{claim['claim_id']}",
                request_id=request_id,
                pool_id=pool_id,
                lock_id=lease.lock_id,
                owner_id=lease.owner_id,
                fencing_token=lease.fencing_token,
            )
            if not self.validator.validate(context):
                STALE_FENCING_REJECTIONS_TOTAL.labels(service="triage", pool_id=pool_id).inc()
                raise AppException("FAILED_PRECONDITION", "Lock ownership or fencing token is no longer valid", status.HTTP_409_CONFLICT)
            record = TriageRecord(
                admission_id=admission_id, triage_level=triage_level,
                pool_id=pool_id, updated_at=get_monotonic_rfc3339(),
            )
            result = self.repo.set_triage_idempotent(context, record, claim)
            return result.record
        finally:
            try:
                released = self.lock_client.release(lease.lock_id, lease.owner_id, request_id=request_id)
                if not released:
                    logger.warning("coordinator_lease_release_unconfirmed", extra={
                        "event": "coordinator_lease_release_unconfirmed",
                        "request_id": request_id, "pool_id": pool_id,
                    })
            except Exception:
                # The lease has a bounded expiry. Do not mask an already committed
                # Triage result with a cleanup RPC failure.
                logger.warning("coordinator_lease_release_failed", extra={
                    "event": "coordinator_lease_release_failed",
                    "request_id": request_id, "pool_id": pool_id,
                })

    def set_triage(self, admission_id: str, triage_level: int, pool_id: str) -> TriageRecord:
        if triage_level < 1 or triage_level > 5:
            raise AppException(
                code="INVALID_ARGUMENT",
                message=f"Triage level must be between 1 and 5, got {triage_level}",
                status_code=status.HTTP_400_BAD_REQUEST
            )
            
        prior_triage = self.repo.get_triage(admission_id)
        if prior_triage and prior_triage.pool_id != pool_id:
            raise AppException(
                code="POOL_CHANGE_NOT_SUPPORTED",
                message="pool_id is immutable after triage creation",
                status_code=status.HTTP_409_CONFLICT,
            )

        existing_waitlist = self.repo.get_active_waitlist_by_admission(admission_id)
        if existing_waitlist:
            if existing_waitlist.pool_id != pool_id:
                raise AppException(
                    code="STATE_CONFLICT",
                    message=(
                        f"Cannot change pool to '{pool_id}' while active in waitlist for pool "
                        f"'{existing_waitlist.pool_id}'. Must cancel current waitlist entry first."
                    ),
                    status_code=status.HTTP_409_CONFLICT
                )
            if existing_waitlist.triage_level != triage_level:
                self.repo.update_waitlist_urgency(admission_id, triage_level)

        record = TriageRecord(
            admission_id=admission_id,
            triage_level=triage_level,
            pool_id=pool_id,
            updated_at=get_monotonic_rfc3339()
        )
        return self.repo.save_triage(record)

    def get_triage(self, admission_id: str) -> TriageRecord:
        record = self.repo.get_triage(admission_id)
        if not record:
            raise AppException(
                code="NOT_FOUND",
                message=f"No triage assessment found for admission '{admission_id}'",
                status_code=status.HTTP_404_NOT_FOUND
            )
        return record

    def enqueue(self, context: MutationContext, allocation_id: str, admission_id: str) -> Tuple[WaitlistEntry, bool]:
        if not self.validator.validate(context):
            STALE_FENCING_REJECTIONS_TOTAL.labels(service="triage", pool_id=context.pool_id).inc()
            raise AppException(
                code="FAILED_PRECONDITION",
                message="Mutation context invalid or lock fencing token superseded",
                status_code=status.HTTP_409_CONFLICT
            )

        triage = self.repo.get_triage(admission_id)
        if not triage:
            raise AppException(
                code="NOT_FOUND",
                message=f"Cannot enqueue admission '{admission_id}' without an established triage record",
                status_code=status.HTTP_404_NOT_FOUND
            )

        if triage.pool_id != context.pool_id:
            raise AppException(
                code="STATE_CONFLICT",
                message=f"Mutation pool '{context.pool_id}' does not match triage pool '{triage.pool_id}'",
                status_code=status.HTTP_409_CONFLICT
            )

        entry = WaitlistEntry(
            entry_id=f"wl-{uuid.uuid4().hex[:10]}",
            allocation_id=allocation_id,
            admission_id=admission_id,
            pool_id=context.pool_id,
            triage_level=triage.triage_level,
            queued_at=get_monotonic_rfc3339()
        )
        enqueue_result = self.repo.enqueue_waitlist(context, entry)
        saved_entry, already_existed = enqueue_result.entry, enqueue_result.already_existed
        return saved_entry, already_existed

    def peek_next(self, pool_id: str) -> Optional[WaitlistEntry]:
        return self.repo.peek_next_waitlist(pool_id)

    def remove_entry(self, context: MutationContext, allocation_id: str, reason: str = "") -> bool:
        if not self.validator.validate(context):
            STALE_FENCING_REJECTIONS_TOTAL.labels(service="triage", pool_id=context.pool_id).inc()
            raise AppException(
                code="FAILED_PRECONDITION",
                message="Mutation context invalid or lock fencing token superseded",
                status_code=status.HTTP_409_CONFLICT
            )
        removed = self.repo.remove_waitlist_entry(context, allocation_id, reason)
        return removed.success

    def list_waitlist(
        self, pool_id: str, limit: int = 50, cursor: Optional[str] = None
    ) -> Tuple[List[WaitlistEntry], Optional[str]]:
        return self.repo.list_waitlist_entries(pool_id, limit, cursor)
