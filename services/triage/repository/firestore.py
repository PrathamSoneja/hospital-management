"""
Google Cloud Firestore Native implementation of TriageRepository.
Supports local Firestore emulator (via FIRESTORE_EMULATOR_HOST) and production GCP.
"""
import os
import hashlib
from typing import List, Optional, Tuple
from google.cloud import firestore
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.base import TriageRepository, QueueEnqueueResult, QueueMutationResult
from services.triage.repository.operations import (
    enqueue_fingerprint, fence_for_new_mutation, original_context,
    remove_entry_fingerprint, validate_reused_enqueue, validate_reused_operation,
)
from shared.observability.errors import AppException
from starlette import status

class FirestoreTriageRepository(TriageRepository):
    def __init__(self, project_id: Optional[str] = None):
        project = project_id or os.environ.get("FIRESTORE_PROJECT_ID", "hospital-bed-mgmt-dev")
        self.client = firestore.Client(project=project)
        self.triage_col = self.client.collection("triage_records")
        self.waitlist_col = self.client.collection("waitlist_entries")
        self.operation_col = self.client.collection("queue_operation_results")
        self.pool_fence_col = self.client.collection("pool_fence_state")

    def get_triage(self, admission_id: str) -> Optional[TriageRecord]:
        doc = self.triage_col.document(admission_id).get()
        if doc.exists:
            return TriageRecord(**doc.to_dict())
        return None

    def save_triage(self, triage: TriageRecord) -> TriageRecord:
        self.triage_col.document(triage.admission_id).set(triage.model_dump())
        return triage

    def get_active_waitlist_by_admission(self, admission_id: str) -> Optional[WaitlistEntry]:
        docs = list(self.waitlist_col.where("admission_id", "==", admission_id).limit(1).stream())
        if docs:
            return WaitlistEntry(**docs[0].to_dict())
        return None

    def get_waitlist_entry(self, allocation_id: str) -> Optional[WaitlistEntry]:
        doc = self.waitlist_col.document(allocation_id).get()
        if doc.exists:
            return WaitlistEntry(**doc.to_dict())
        return None

    def enqueue_waitlist(self, context, entry: WaitlistEntry) -> QueueEnqueueResult:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        if entry.pool_id != context.pool_id:
            raise AppException("STATE_CONFLICT", "Waitlist entry pool does not match mutation context", status.HTTP_409_CONFLICT)
        doc_ref = self.waitlist_col.document(entry.allocation_id)
        operation_ref = self.operation_col.document(hashlib.sha256(context.operation_id.encode()).hexdigest())
        fence_ref = self.pool_fence_col.document(context.pool_id)
        recorded_at = firestore.SERVER_TIMESTAMP

        @firestore.transactional
        def apply(transaction):
            operation_snapshot = operation_ref.get(transaction=transaction)
            if operation_snapshot.exists:
                operation = operation_snapshot.to_dict()
                validate_reused_enqueue(operation, context, entry)
                if operation.get("state") != "COMPLETED" or not operation.get("result_entry"):
                    raise AppException("OUTCOME_UNKNOWN", "Stored enqueue result requires reconciliation", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
                return WaitlistEntry(**operation["result_entry"]), bool(operation.get("result_already_existed")), True

            fence_snapshot = fence_ref.get(transaction=transaction)
            target_snapshot = doc_ref.get(transaction=transaction)
            admission_query = self.waitlist_col.where("admission_id", "==", entry.admission_id).limit(1)
            admission_snapshots = list(transaction.get(admission_query))
            new_fence = fence_for_new_mutation(context, fence_snapshot.to_dict() if fence_snapshot.exists else None)
            if target_snapshot.exists:
                saved, existed = WaitlistEntry(**target_snapshot.to_dict()), True
            elif admission_snapshots:
                saved, existed = WaitlistEntry(**admission_snapshots[0].to_dict()), True
            else:
                saved, existed = entry, False

            operation = {
                "operation_id": context.operation_id, "action": "ENQUEUE",
                "allocation_id": entry.allocation_id, "admission_id": entry.admission_id,
                "entry_id": saved.entry_id, "pool_id": context.pool_id,
                "input_fingerprint": enqueue_fingerprint(entry.allocation_id, entry.admission_id, context.pool_id, entry.triage_level),
                "state": "COMPLETED", "result_success": True,
                "result_entry": saved.model_dump(), "result_already_existed": existed,
                "recorded_at": recorded_at, "completed_at": recorded_at,
                "original_context": original_context(context),
            }
            if not existed:
                transaction.set(doc_ref, saved.model_dump())
            transaction.set(operation_ref, operation)
            transaction.set(fence_ref, new_fence)
            return saved, existed, False

        saved, existed, replayed = apply(self.client.transaction())
        return QueueEnqueueResult(saved, existed, replayed)

    def peek_next_waitlist(self, pool_id: str) -> Optional[WaitlistEntry]:
        query = (
            self.waitlist_col.where("pool_id", "==", pool_id)
            .order_by("triage_level")
            .order_by("queued_at")
            .order_by("entry_id")
            .limit(1)
        )
        docs = list(query.stream())
        if docs:
            return WaitlistEntry(**docs[0].to_dict())
        return None

    def remove_waitlist_entry(self, context, allocation_id: str, reason: str) -> QueueMutationResult:
        if not context.operation_id:
            raise AppException("INVALID_ARGUMENT", "operation_id is required", status.HTTP_400_BAD_REQUEST)
        operation_ref = self.operation_col.document(hashlib.sha256(context.operation_id.encode()).hexdigest())
        queue_ref = self.waitlist_col.document(allocation_id)
        fence_ref = self.pool_fence_col.document(context.pool_id)
        recorded_at = firestore.SERVER_TIMESTAMP

        @firestore.transactional
        def apply(transaction):
            # A completed record is authoritative even when removal made the queue
            # document absent. Read it before considering queue state.
            operation_snapshot = operation_ref.get(transaction=transaction)
            if operation_snapshot.exists:
                operation = operation_snapshot.to_dict()
                validate_reused_operation(operation, context, allocation_id, reason)
                if operation.get("state") == "OUTCOME_UNKNOWN":
                    return QueueMutationResult(success=False, replayed=True)
                if operation.get("state") != "COMPLETED" or operation.get("result_success") is None:
                    raise AppException("SERVICE_UNAVAILABLE", "Stored queue operation result is invalid", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
                current_queue = queue_ref.get(transaction=transaction)
                if current_queue.exists:
                    current_entry_id = current_queue.to_dict().get("entry_id")
                    if current_entry_id != operation.get("entry_id"):
                        raise AppException("IDEMPOTENCY_MISMATCH", "Queue entry identity changed for operation_id", status.HTTP_409_CONFLICT)
                return QueueMutationResult(success=bool(operation["result_success"]), replayed=True)

            # No result exists: read every document needed for fencing and target
            # validation before any writes in this transaction callback.
            fence_snapshot = fence_ref.get(transaction=transaction)
            queue_snapshot = queue_ref.get(transaction=transaction)
            fence = fence_snapshot.to_dict() if fence_snapshot.exists else None
            new_fence = fence_for_new_mutation(context, fence)
            entry = queue_snapshot.to_dict() if queue_snapshot.exists else None
            if entry is not None and (
                entry.get("allocation_id") != allocation_id
                or entry.get("pool_id") != context.pool_id
                or not entry.get("entry_id")
            ):
                raise AppException("STATE_CONFLICT", "Waitlist entry target does not match mutation context", status.HTTP_409_CONFLICT)

            entry_id = entry.get("entry_id") if entry else None
            fingerprint = remove_entry_fingerprint(allocation_id, entry_id, context.pool_id, reason)
            operation = {
                "operation_id": context.operation_id,
                "action": "REMOVE_ENTRY",
                "allocation_id": allocation_id,
                "entry_id": entry_id,
                "pool_id": context.pool_id,
                "reason": reason,
                "input_fingerprint": fingerprint,
                "result_success": True if entry else None,
                "state": "COMPLETED" if entry else "OUTCOME_UNKNOWN",
                "unknown_reason": None if entry else "entry_missing_without_operation_record",
                "recorded_at": recorded_at,
                "completed_at": recorded_at if entry else None,
                "original_context": original_context(context),
            }
            if entry:
                transaction.delete(queue_ref)
            transaction.set(operation_ref, operation)
            transaction.set(fence_ref, new_fence)
            return QueueMutationResult(success=bool(entry), replayed=False)

        result = apply(self.client.transaction())
        if result.replayed:
            operation_snapshot = operation_ref.get()
            operation = operation_snapshot.to_dict() if operation_snapshot.exists else {}
            if operation.get("state") == "OUTCOME_UNKNOWN":
                raise AppException(
                    "OUTCOME_UNKNOWN",
                    operation.get("unknown_reason", "Queue mutation outcome is unknown; reconciliation is required"),
                    status.HTTP_503_SERVICE_UNAVAILABLE,
                    retryable=True,
                )
        elif not result.success:
            raise AppException(
                "OUTCOME_UNKNOWN",
                "Waitlist entry is absent without a durable operation result; reconciliation is required",
                status.HTTP_503_SERVICE_UNAVAILABLE,
                retryable=True,
            )
        return result

    def list_waitlist_entries(
        self, pool_id: str, limit: int = 50, cursor: Optional[str] = None
    ) -> Tuple[List[WaitlistEntry], Optional[str]]:
        query = (
            self.waitlist_col.where("pool_id", "==", pool_id)
            .order_by("triage_level")
            .order_by("queued_at")
            .order_by("entry_id")
        )
        if cursor:
            cursor_doc = self.waitlist_col.where("entry_id", "==", cursor).limit(1).stream()
            cursor_list = list(cursor_doc)
            if cursor_list:
                query = query.start_after(cursor_list[0])

        docs = list(query.limit(limit).stream())
        entries = [WaitlistEntry(**d.to_dict()) for d in docs]
        next_cursor = entries[-1].entry_id if len(entries) == limit else None
        return entries, next_cursor

    def update_waitlist_urgency(self, admission_id: str, new_level: int) -> Optional[WaitlistEntry]:
        existing = self.get_active_waitlist_by_admission(admission_id)
        if not existing:
            return None
        doc_ref = self.waitlist_col.document(existing.allocation_id)
        doc_ref.update({"triage_level": new_level})
        existing.triage_level = new_level
        return existing
