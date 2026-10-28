"""
Abstract repository interface for Triage and Waitlist persistence.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from services.triage.models import TriageRecord, WaitlistEntry

class TriageRepository(ABC):
    @abstractmethod
    def get_triage(self, admission_id: str) -> Optional[TriageRecord]:
        pass

    @abstractmethod
    def save_triage(self, triage: TriageRecord) -> TriageRecord:
        pass

    @abstractmethod
    def get_active_waitlist_by_admission(self, admission_id: str) -> Optional[WaitlistEntry]:
        pass

    @abstractmethod
    def get_waitlist_entry(self, allocation_id: str) -> Optional[WaitlistEntry]:
        pass

    @abstractmethod
    def enqueue_waitlist(self, entry: WaitlistEntry) -> Tuple[WaitlistEntry, bool]:
        """Returns (entry, already_existed)"""
        pass

    @abstractmethod
    def peek_next_waitlist(self, pool_id: str) -> Optional[WaitlistEntry]:
        pass

    @abstractmethod
    def remove_waitlist_entry(self, allocation_id: str) -> bool:
        pass

    @abstractmethod
    def list_waitlist_entries(
        self, pool_id: str, limit: int = 50, cursor: Optional[str] = None
    ) -> Tuple[List[WaitlistEntry], Optional[str]]:
        pass

    @abstractmethod
    def update_waitlist_urgency(self, admission_id: str, new_level: int) -> Optional[WaitlistEntry]:
        """Updates triage_level on active waitlist entry if present, preserving queued_at FIFO time."""
        pass
