"""
Pool contention on real PostgreSQL with an exclusive in-process lock double.

50 distinct eligible admissions race for 10 free beds with ample staff and no
releases. The settled state must be exactly 10 ALLOCATED with 10 distinct beds
and 40 WAITLISTED. Bed/Staff/Triage/Lock are tracked test doubles, so this is
MOCK-TESTED evidence of Allocation's own logic, not of distributed lock safety.
"""
import threading
from collections import Counter

from services.bed_allocation.db import AllocationModel, OutboxEventModel
from services.bed_allocation.models import CreateAllocationRequest
from services.bed_allocation.service import BedAllocationDomainService
from services.triage.repository.memory import InMemoryTriageRepository
from services.triage.service import TriageDomainService
from tests.support.allocation_fakes import (
    DirectTriageClient, FakeBedRegistryClient, FakePatientRegistryClient, FakeStaffSchedulerClient,
)
from tests.support.stateful_lock import InProcessLockClient, StatefulLockCoordinator
from tests.support.triage_fakes import MockFencingValidator

ADMISSIONS = 50
BEDS = 10


def test_fifty_distinct_admissions_settle_to_ten_allocated_and_forty_waitlisted(pg_sessions):
    patients = FakePatientRegistryClient()
    triage = TriageDomainService(InMemoryTriageRepository(), MockFencingValidator(True))
    for n in range(ADMISSIONS):
        patients.seed_admission(f"adm-{n:02d}", f"pat-{n:02d}", "OPEN")
        triage.set_triage(f"adm-{n:02d}", 1 + n % 5, "POOL-A")
    beds = FakeBedRegistryClient()
    for n in range(BEDS):
        beds.seed_bed(f"bed-{n:02d}", "POOL-A", ward="WARD-NORTH")
    coordinator = StatefulLockCoordinator()
    services = [
        BedAllocationDomainService(
            session_factory=pg_sessions, patient_client=patients, triage_client=DirectTriageClient(triage),
            lock_client=InProcessLockClient(coordinator), bed_client=beds,
            staff_client=FakeStaffSchedulerClient(initial_capacity=ADMISSIONS * 2),
            owner_id=f"replica-{r}", lock_wait_timeout_ms=30000,
        )
        for r in range(3)  # three "replicas" sharing PostgreSQL and the lock double
    ]
    barrier = threading.Barrier(ADMISSIONS)
    results, errors = {}, []

    def submit(n):
        try:
            barrier.wait()
            service = services[n % len(services)]
            results[n] = service.create_allocation(
                CreateAllocationRequest(admission_id=f"adm-{n:02d}", pool_id="POOL-A"), f"req-{n}", f"key-{n}")
        except Exception as error:  # pragma: no cover - surfaced below
            errors.append(repr(error))

    threads = [threading.Thread(target=submit, args=(n,)) for n in range(ADMISSIONS)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert errors == []

    with pg_sessions() as session:
        rows = session.query(AllocationModel).all()
        statuses = Counter(row.status for row in rows)
        allocated_beds = [row.bed_id for row in rows if row.status == "ALLOCATED"]
        kafka_deltas = Counter()
        for event in session.query(OutboxEventModel).filter_by(destination="kafka"):
            kafka_deltas[event.stream_sequence] += 1
        sequences = sorted(kafka_deltas)
    assert statuses == {"ALLOCATED": BEDS, "WAITLISTED": ADMISSIONS - BEDS}
    assert len(set(allocated_beds)) == BEDS and None not in allocated_beds
    # Bed Registry ownership agrees with Allocation: each occupied bed belongs to one allocated row.
    owners = {bed.bed_id: bed.current_allocation_id for bed in beds.beds.values()}
    allocated_ids = {row.allocation_id for row in rows if row.status == "ALLOCATED"}
    assert set(owners.values()) == allocated_ids
    assert len({row.admission_id for row in rows}) == ADMISSIONS
    assert sequences == list(range(1, ADMISSIONS + 1))  # one event per admission, gap-free stream
    assert coordinator.acquisitions >= ADMISSIONS
