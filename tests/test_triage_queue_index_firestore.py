"""
Triage queue mutations on the Firestore emulator use key reads only inside transactions.

Regression from the real-services sustained load run: enqueue/set-triage transactions queried
waitlist_entries by admission_id, and on the emulator a query inside a transaction followed by a write
serialized queue mutations across *different* pools (p50 ~2 s, up to 10 s, lock-timeout failures with
three pools; Allocation's 2 s Triage deadline then kept rows RECOVERING). The per-admission index keeps
"one active entry per admission" without range locks.
"""
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from services.triage.repository.firestore import FirestoreTriageRepository
from services.triage.service import TriageDomainService
from shared.generated.common_pb2 import MutationContext
from tests.support.triage_fakes import MockFencingValidator

pytestmark = pytest.mark.skipif(not os.environ.get("FIRESTORE_EMULATOR_HOST"),
                                reason="requires the local Firestore emulator; cloud Firestore is not used")


@pytest.fixture
def triage():
    return TriageDomainService(FirestoreTriageRepository(project_id=f"queue-index-{uuid.uuid4().hex[:8]}"),
                               MockFencingValidator(True))


def _ctx(pool, op, token):
    return MutationContext(operation_id=op, request_id=op, pool_id=pool, lock_id=f"{pool}/lock-{token:010d}",
                           owner_id="alloc", fencing_token=token)


def test_queue_mutations_in_different_pools_do_not_block_each_other(triage):
    pools = ["PA", "PB", "PC"]
    for pool in pools:
        for n in range(10):
            triage.set_triage(f"{pool}-adm-{n}", 3, pool)

    def run(pool):
        durations = []
        for n in range(10):  # sequential per pool, as the pool lease guarantees
            started = time.monotonic()
            triage.enqueue(_ctx(pool, f"{pool}-{n}:enqueue", 100 + n), f"{pool}-alloc-{n}", f"{pool}-adm-{n}")
            durations.append(time.monotonic() - started)
        return durations

    with ThreadPoolExecutor(len(pools)) as executor:
        durations = [d for result in executor.map(run, pools) for d in result]
    assert max(durations) < 1.0, sorted(durations)[-5:]  # was p50 ~2 s / max ~10 s on the emulator


def test_one_active_entry_per_admission_survives_the_index(triage):
    triage.set_triage("adm-1", 2, "PA")
    first, existed = triage.enqueue(_ctx("PA", "a1:enqueue", 1), "a1", "adm-1")
    assert not existed
    # Same stable operation ID: replay of the same entry, nothing new.
    replay, _ = triage.enqueue(_ctx("PA", "a1:enqueue", 1), "a1", "adm-1")
    assert replay.entry_id == first.entry_id
    # A different allocation for the same admission joins the existing entry, never a second one.
    other, existed = triage.enqueue(_ctx("PA", "a2:enqueue", 2), "a2", "adm-1")
    assert existed and other.entry_id == first.entry_id
    assert [e.allocation_id for e in triage.list_waitlist("PA")[0]] == ["a1"]

    triage.set_triage("adm-1", 1, "PA")  # urgency change reaches the indexed entry, identity kept
    entries = triage.list_waitlist("PA")[0]
    assert [(e.entry_id, e.triage_level) for e in entries] == [(first.entry_id, 1)]

    assert triage.remove_entry(_ctx("PA", "a1:remove", 3), "a1", "allocated")
    assert triage.list_waitlist("PA")[0] == []
    again, existed = triage.enqueue(_ctx("PA", "a3:enqueue", 4), "a3", "adm-1")  # index freed by the removal
    assert not existed and again.entry_id != first.entry_id
