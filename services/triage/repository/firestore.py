"""
Google Cloud Firestore Native implementation of TriageRepository.
Supports local Firestore emulator (via FIRESTORE_EMULATOR_HOST) and production GCP.
"""
import os
from typing import List, Optional, Tuple
from google.cloud import firestore
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.base import TriageRepository

class FirestoreTriageRepository(TriageRepository):
    def __init__(self, project_id: Optional[str] = None):
        project = project_id or os.environ.get("FIRESTORE_PROJECT_ID", "hospital-bed-mgmt-dev")
        self.client = firestore.Client(project=project)
        self.triage_col = self.client.collection("triage_records")
        self.waitlist_col = self.client.collection("waitlist_entries")

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

    def enqueue_waitlist(self, entry: WaitlistEntry) -> Tuple[WaitlistEntry, bool]:
        doc_ref = self.waitlist_col.document(entry.allocation_id)
        doc = doc_ref.get()
        if doc.exists:
            return WaitlistEntry(**doc.to_dict()), True

        # Check if admission is already queued under another allocation
        existing = self.get_active_waitlist_by_admission(entry.admission_id)
        if existing:
            return existing, True

        doc_ref.set(entry.model_dump())
        return entry, False

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

    def remove_waitlist_entry(self, allocation_id: str) -> bool:
        doc_ref = self.waitlist_col.document(allocation_id)
        doc = doc_ref.get()
        if doc.exists:
            doc_ref.delete()
            return True
        return False

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
