"""
Thread-safe in-memory repository for Triage and Waitlist entities.
Used for isolated unit tests and local standalone development.
PROHIBITED for multi-replica Kubernetes deployments.
"""
import threading
from typing import Dict, List, Optional, Tuple
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.base import TriageRepository

class InMemoryTriageRepository(TriageRepository):
    def __init__(self):
        self._lock = threading.RLock()
        self._triage_records: Dict[str, TriageRecord] = {} # admission_id -> TriageRecord
        self._waitlist_by_allocation: Dict[str, WaitlistEntry] = {} # allocation_id -> WaitlistEntry
        self._waitlist_by_admission: Dict[str, str] = {} # admission_id -> allocation_id

    def get_triage(self, admission_id: str) -> Optional[TriageRecord]:
        with self._lock:
            return self._triage_records.get(admission_id)

    def save_triage(self, triage: TriageRecord) -> TriageRecord:
        with self._lock:
            self._triage_records[triage.admission_id] = triage
            return triage

    def get_active_waitlist_by_admission(self, admission_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            alloc_id = self._waitlist_by_admission.get(admission_id)
            if alloc_id:
                return self._waitlist_by_allocation.get(alloc_id)
            return None

    def get_waitlist_entry(self, allocation_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            return self._waitlist_by_allocation.get(allocation_id)

    def enqueue_waitlist(self, entry: WaitlistEntry) -> Tuple[WaitlistEntry, bool]:
        with self._lock:
            # Check if this allocation is already queued (idempotency)
            if entry.allocation_id in self._waitlist_by_allocation:
                return self._waitlist_by_allocation[entry.allocation_id], True
                
            # Check if admission already has an active waitlist entry with a different allocation
            existing_alloc_id = self._waitlist_by_admission.get(entry.admission_id)
            if existing_alloc_id and existing_alloc_id in self._waitlist_by_allocation:
                return self._waitlist_by_allocation[existing_alloc_id], True

            self._waitlist_by_allocation[entry.allocation_id] = entry
            self._waitlist_by_admission[entry.admission_id] = entry.allocation_id
            return entry, False

    def _get_sorted_pool_entries(self, pool_id: str) -> List[WaitlistEntry]:
        pool_entries = [e for e in self._waitlist_by_allocation.values() if e.pool_id == pool_id]
        # Order: triage_level ascending, queued_at ascending, entry_id ascending
        pool_entries.sort(key=lambda e: (e.triage_level, e.queued_at, e.entry_id))
        return pool_entries

    def peek_next_waitlist(self, pool_id: str) -> Optional[WaitlistEntry]:
        with self._lock:
            sorted_entries = self._get_sorted_pool_entries(pool_id)
            return sorted_entries[0] if sorted_entries else None

    def remove_waitlist_entry(self, allocation_id: str) -> bool:
        with self._lock:
            entry = self._waitlist_by_allocation.pop(allocation_id, None)
            if entry:
                if self._waitlist_by_admission.get(entry.admission_id) == allocation_id:
                    del self._waitlist_by_admission[entry.admission_id]
                return True
            return False

    def list_waitlist_entries(
        self, pool_id: str, limit: int = 50, cursor: Optional[str] = None
    ) -> Tuple[List[WaitlistEntry], Optional[str]]:
        with self._lock:
            sorted_entries = self._get_sorted_pool_entries(pool_id)
            start_idx = 0
            if cursor:
                # Find index after cursor
                for idx, entry in enumerate(sorted_entries):
                    if entry.entry_id == cursor:
                        start_idx = idx + 1
                        break
            
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
