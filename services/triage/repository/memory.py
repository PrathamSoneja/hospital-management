"""
Thread-safe in-memory repository for Triage and Waitlist entities.
Used for isolated unit tests and local standalone development.
PROHIBITED for multi-replica Kubernetes deployments.
"""
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.base import TriageRepository, QueueEnqueueResult, QueueMutationResult, TriageUpdateResult
from services.triage.idempotency import validate_put_claim
from services.triage.repository.operations import (
    enqueue_fingerprint, fence_for_new_mutation, original_context, remove_entry_fingerprint,
    validate_reused_enqueue, validate_reused_operation,
)
from shared.observability.errors import AppException
from starlette import status

class InMemoryTriageRepository(TriageRepository):
    def __init__(self):
        self._lock = threading.RLock()
        self._triage_records: Dict[str, TriageRecord] = {} # admission_id -> TriageRecord
        self._waitlist_by_allocation: Dict[str, WaitlistEntry] = {} # allocation_id -> WaitlistEntry
        self._waitlist_by_admission: Dict[str, str] = {} # admission_id -> allocation_id
        self._queue_operation_results: Dict[str, dict] = {}
        self._pool_fence_state: Dict[str, dict] = {}
        self._triage_idempotency: Dict[str, dict] = {}

    def get_triage(self, admission_id: str) -> Optional[TriageRecord]:
        with self._lock:
            return self._triage_records.get(admission_id)

    def save_triage(self, triage: TriageRecord) -> TriageRecord:
        with self._lock:
            self._triage_records[triage.admission_id] = triage
            return triage

    def lookup_triage_idempotency(self, claim: dict) -> Optional[TriageRecord]:
        with self._lock:
            existing = self._triage_idempotency.get(claim["claim_id"])
            if not existing:
                return None
            validate_put_claim(existing, claim)
            return TriageRecord(**existing["response"])

    def set_triage_idempotent(self, context, record: TriageRecord, claim: dict) -> TriageUpdateResult:
        with self._lock:
            existing_claim = self._triage_idempotency.get(claim["claim_id"])
            if existing_claim:
                validate_put_claim(existing_claim, claim)
                return TriageUpdateResult(TriageRecord(**existing_claim["response"]), True)

            current = self._triage_records.get(record.admission_id)
            if current and current.pool_id != record.pool_id:
                raise AppException("POOL_CHANGE_NOT_SUPPORTED", "pool_id is immutable after triage creation", status.HTTP_409_CONFLICT)
            active = self.get_active_waitlist_by_admission(record.admission_id)
            if active and (active.pool_id != record.pool_id or active.admission_id != record.admission_id):
                raise AppException("STATE_CONFLICT", "Active queue entry does not match triage target", status.HTTP_409_CONFLICT)
            new_fence = fence_for_new_mutation(context, self._pool_fence_state.get(record.pool_id))

            self._triage_records[record.admission_id] = record
            if active and active.triage_level != record.triage_level:
                updated_entry = WaitlistEntry(
                    entry_id=active.entry_id, allocation_id=active.allocation_id,
                    admission_id=active.admission_id, pool_id=active.pool_id,
                    triage_level=record.triage_level, queued_at=active.queued_at,
                )
                self._waitlist_by_allocation[active.allocation_id] = updated_entry
            self._pool_fence_state[record.pool_id] = new_fence
            self._triage_idempotency[claim["claim_id"]] = {
                **claim, "state": "COMPLETED", "response": record.model_dump(),
                "recorded_at": datetime.now(timezone.utc),
                "completed_at": datetime.now(timezone.utc),
            }
            return TriageUpdateResult(record, False)

    def get_active_waitlist_by_admission(self, admission_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            alloc_id = self._waitlist_by_admission.get(admission_id)
            if alloc_id:
                return self._waitlist_by_allocation.get(alloc_id)
            return None

    def get_waitlist_entry(self, allocation_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            return self._waitlist_by_allocation.get(allocation_id)

    def enqueue_waitlist(self, context, entry: WaitlistEntry) -> QueueEnqueueResult:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        if entry.pool_id != context.pool_id:
            raise AppException("STATE_CONFLICT", "Waitlist entry pool does not match mutation context", status.HTTP_409_CONFLICT)
        with self._lock:
            prior = self._queue_operation_results.get(context.operation_id)
            if prior:
                validate_reused_enqueue(prior, context, entry)
                if prior.get("state") == "OUTCOME_UNKNOWN":
                    raise AppException("OUTCOME_UNKNOWN", prior.get("unknown_reason", "Queue mutation requires reconciliation"), status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
                return QueueEnqueueResult(WaitlistEntry(**prior["result_entry"]), bool(prior.get("result_already_existed")), True)

            triage = self._triage_records.get(entry.admission_id)
            if not triage:
                raise AppException("NOT_FOUND", "Cannot enqueue without an established triage record", status.HTTP_404_NOT_FOUND)
            if triage.pool_id != entry.pool_id or triage.triage_level != entry.triage_level:
                raise AppException("STATE_CONFLICT", "Triage changed before enqueue; retry with current triage", status.HTTP_409_CONFLICT)
            new_fence = fence_for_new_mutation(context, self._pool_fence_state.get(context.pool_id))
            # Check if this allocation is already queued (idempotency)
            if entry.allocation_id in self._waitlist_by_allocation:
                saved, existed = self._waitlist_by_allocation[entry.allocation_id], True
                
            # Check if admission already has an active waitlist entry with a different allocation
            else:
                existing_alloc_id = self._waitlist_by_admission.get(entry.admission_id)
                if existing_alloc_id and existing_alloc_id in self._waitlist_by_allocation:
                    saved, existed = self._waitlist_by_allocation[existing_alloc_id], True
                else:
                    self._waitlist_by_allocation[entry.allocation_id] = entry
                    self._waitlist_by_admission[entry.admission_id] = entry.allocation_id
                    saved, existed = entry, False
            self._pool_fence_state[context.pool_id] = new_fence
            self._queue_operation_results[context.operation_id] = {
                "operation_id": context.operation_id, "action": "ENQUEUE",
                "allocation_id": entry.allocation_id, "admission_id": entry.admission_id,
                "entry_id": saved.entry_id, "pool_id": context.pool_id,
                "input_fingerprint": enqueue_fingerprint(entry.allocation_id, entry.admission_id, context.pool_id, entry.triage_level),
                "state": "COMPLETED", "result_success": True,
                "result_entry": saved.model_dump(), "result_already_existed": existed,
                "recorded_at": datetime.now(timezone.utc), "completed_at": datetime.now(timezone.utc),
                "original_context": original_context(context),
            }
            return QueueEnqueueResult(saved, existed, False)

    def _get_sorted_pool_entries(self, pool_id: str) -> List[WaitlistEntry]:
        pool_entries = [e for e in self._waitlist_by_allocation.values() if e.pool_id == pool_id]
        # Order: triage_level ascending, queued_at ascending, entry_id ascending
        pool_entries.sort(key=lambda e: (e.triage_level, e.queued_at, e.entry_id))
        return pool_entries

    def peek_next_waitlist(self, pool_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            sorted_entries = self._get_sorted_pool_entries(pool_id)
            return sorted_entries[0] if sorted_entries else None

    def remove_waitlist_entry(self, context, allocation_id: str, reason: str) -> QueueMutationResult:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        unknown = False
        with self._lock:
            existing = self._queue_operation_results.get(context.operation_id)
            if existing:
                validate_reused_operation(existing, context, allocation_id, reason)
                current = self._waitlist_by_allocation.get(allocation_id)
                if existing["state"] == "COMPLETED" and current and current.entry_id != existing.get("entry_id"):
                    raise AppException("IDEMPOTENCY_MISMATCH", "Queue entry identity changed for operation_id", status.HTTP_409_CONFLICT)
                if existing["state"] == "OUTCOME_UNKNOWN":
                    unknown = True
                else:
                    return QueueMutationResult(success=existing["result_success"], replayed=True)
            else:
                entry = self._waitlist_by_allocation.get(allocation_id)
                if entry and (entry.allocation_id != allocation_id or entry.pool_id != context.pool_id or not entry.entry_id):
                    raise AppException("STATE_CONFLICT", "Waitlist entry target does not match mutation context", status.HTTP_409_CONFLICT)
                fence = fence_for_new_mutation(context, self._pool_fence_state.get(context.pool_id))
                self._pool_fence_state[context.pool_id] = fence
                if entry is None:
                    self._queue_operation_results[context.operation_id] = {
                        "operation_id": context.operation_id, "action": "REMOVE_ENTRY",
                        "allocation_id": allocation_id, "entry_id": None,
                        "pool_id": context.pool_id, "reason": reason,
                        "input_fingerprint": remove_entry_fingerprint(allocation_id, None, context.pool_id, reason),
                        "state": "OUTCOME_UNKNOWN", "result_success": None,
                        "unknown_reason": "entry_missing_without_operation_record",
                        "recorded_at": datetime.now(timezone.utc), "completed_at": None,
                        "original_context": original_context(context),
                    }
                    unknown = True
                else:
                    del self._waitlist_by_allocation[allocation_id]
                    if self._waitlist_by_admission.get(entry.admission_id) == allocation_id:
                        del self._waitlist_by_admission[entry.admission_id]
                    self._queue_operation_results[context.operation_id] = {
                        "operation_id": context.operation_id, "action": "REMOVE_ENTRY",
                        "allocation_id": allocation_id, "entry_id": entry.entry_id,
                        "pool_id": context.pool_id, "reason": reason,
                        "input_fingerprint": remove_entry_fingerprint(allocation_id, entry.entry_id, context.pool_id, reason),
                        "state": "COMPLETED", "result_success": True,
                        "unknown_reason": None, "recorded_at": datetime.now(timezone.utc),
                        "completed_at": datetime.now(timezone.utc), "original_context": original_context(context),
                    }
                    return QueueMutationResult(success=True, replayed=False)
        if unknown:
            raise AppException(
                "OUTCOME_UNKNOWN", "Waitlist entry is absent without a durable operation result; reconciliation is required",
                status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True,
            )

    def list_waitlist_entries(
        self, pool_id: str, limit: int = 50, cursor: Optional[str] = None
    ) -> Tuple[List[WaitlistEntry], Optional[str]]:
        with self._lock:
            sorted_entries = self._get_sorted_pool_entries(pool_id)
            start_idx = 0
            if cursor:
                # Find index after cursor
                cursor_found = False
                for idx, entry in enumerate(sorted_entries):
                    if entry.entry_id == cursor:
                        start_idx = idx + 1
                        cursor_found = True
                        break
                if not cursor_found:
                    raise AppException("INVALID_ARGUMENT", "Waitlist cursor is not valid for this pool", status.HTTP_400_BAD_REQUEST)
            
            paged = sorted_entries[start_idx : start_idx + limit]
            next_cursor = paged[-1].entry_id if len(paged) == limit and (start_idx + limit) < len(sorted_entries) else None
            return paged, next_cursor

    def update_waitlist_urgency(self, admission_id: str, new_level: int) -> Optional[WaitlistEntry]:
        with self._lock:
            alloc_id = self._waitlist_by_admission.get(admission_id)
            if not alloc_id:
                return None
            entry = self._waitlist_by_allocation.get(alloc_id)
            if not entry:
                return None
            
            updated_entry = WaitlistEntry(
                entry_id=entry.entry_id,
                allocation_id=entry.allocation_id,
                admission_id=entry.admission_id,
                pool_id=entry.pool_id,
                triage_level=new_level,
                queued_at=entry.queued_at # Preserves FIFO timestamp
            )
            self._waitlist_by_allocation[alloc_id] = updated_entry
            return updated_entry
