"""
Abstract client interfaces for Bed Allocation Service dependencies.
"""
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple
from pydantic import BaseModel

class LockLease(BaseModel):
    lock_id: str
    pool_id: str
    owner_id: str
    fencing_token: int
    expires_at: int
    lock_wait_ms: int

class BedDto(BaseModel):
    bed_id: str
    pool_id: str
    ward: str
    status: str
    current_allocation_id: Optional[str] = None

class PatientAdmissionDto(BaseModel):
    admission_id: str
    patient_id: str
    status: str # OPEN, DISCHARGED, CANCELLED

class TriageDto(BaseModel):
    admission_id: str
    triage_level: int
    pool_id: str
    updated_at: str

class WaitlistEntryDto(BaseModel):
    entry_id: str
    allocation_id: str
    admission_id: str
    pool_id: str
    triage_level: int
    queued_at: str

class PatientRegistryClient(ABC):
    @abstractmethod
    def get_admission(self, admission_id: str, request_id: str) -> Optional[PatientAdmissionDto]:
        pass

class TriageClient(ABC):
    @abstractmethod
    def get_triage(self, admission_id: str, request_id: str) -> Optional[TriageDto]:
        pass

    @abstractmethod
    def enqueue(self, context, allocation_id: str, admission_id: str) -> WaitlistEntryDto:
        pass

    @abstractmethod
    def peek_next(self, pool_id: str, request_id: str) -> Optional[WaitlistEntryDto]:
        pass

    @abstractmethod
    def remove_entry(self, context, allocation_id: str, reason: str) -> bool:
        pass

class LockCoordinatorClient(ABC):
    @abstractmethod
    def acquire(self, pool_id: str, owner_id: str, acquire_id: str, lease_ms: int, wait_timeout_ms: int) -> LockLease:
        pass

    @abstractmethod
    def release(self, lock_id: str, owner_id: str) -> bool:
        pass

    @abstractmethod
    def validate(self, lock_id: str, owner_id: str, pool_id: str, fencing_token: int) -> bool:
        pass

class BedRegistryClient(ABC):
    @abstractmethod
    def list_available_beds(self, pool_id: str, request_id: str) -> List[BedDto]:
        pass

    @abstractmethod
    def allocate_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, Optional[BedDto], str]:
        pass

    @abstractmethod
    def release_bed(self, context, bed_id: str, allocation_id: str) -> Tuple[bool, str]:
        pass

class StaffSchedulerClient(ABC):
    @abstractmethod
    def check_capacity(self, pool_id: str, request_id: str) -> Tuple[bool, int]:
        pass

    @abstractmethod
    def assign_staff(self, context, allocation_id: str, bed_id: str) -> Tuple[bool, Optional[str], Optional[str], str]:
        """Returns (success, staff_assignment_id, staff_id, error_message)"""
        pass

    @abstractmethod
    def release_assignment(self, context, staff_assignment_id: str, allocation_id: str) -> Tuple[bool, str]:
        pass
