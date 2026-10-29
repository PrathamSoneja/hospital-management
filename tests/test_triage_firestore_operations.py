"""Firestore operation-ledger tests; run against the local emulator only."""
import os
import threading
import uuid

import pytest

from google.cloud import firestore
from services.triage.models import TriageRecord, WaitlistEntry
from services.triage.repository.firestore import FirestoreTriageRepository
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import AppException


pytestmark = pytest.mark.skipif(
    not os.environ.get("FIRESTORE_EMULATOR_HOST"),
    reason="requires the Firestore emulator; cloud Firestore is never used by this test",
)


def context(operation_id, token=1, lock="lock", owner="owner", pool="pool-a"):
    return MutationContext(
        operation_id=operation_id, lock_id=lock, owner_id=owner,
        pool_id=pool, fencing_token=token,
    )


@pytest.fixture
def repository():
    repo = FirestoreTriageRepository(project_id=f"triage-test-{uuid.uuid4().hex}")
    yield repo
    # These projects are emulator-only and unique to this test.


def seed_entry(repo, allocation_id="allocation-1", pool_id="pool-a"):
    entry = WaitlistEntry(
        entry_id=f"entry-{uuid.uuid4().hex}", allocation_id=allocation_id,
        admission_id=f"admission-{uuid.uuid4().hex}", pool_id=pool_id,
        triage_level=2, queued_at="2026-10-29T00:00:00+00:00",
    )
    repo.waitlist_col.document(allocation_id).set(entry.model_dump())
    return entry


def operation_doc(repo, operation_id):
    key = __import__("hashlib").sha256(operation_id.encode()).hexdigest()
    return repo.operation_col.document(key).get().to_dict()


def test_remove_commit_lost_response_replays_after_repository_restart(repository):
    entry = seed_entry(repository)
    op = "lost-response"
    result = repository.remove_waitlist_entry(context(op, token=8), entry.allocation_id, "promoted")
    assert result.success and not result.replayed

    restarted = FirestoreTriageRepository(project_id=repository.client.project)
    replay = restarted.remove_waitlist_entry(
        context(op, token=9, lock="fresh-lock", owner="fresh-owner"),
        entry.allocation_id, "promoted",
    )
    assert replay.success and replay.replayed
    assert restarted.get_waitlist_entry(entry.allocation_id) is None
    record = operation_doc(restarted, op)
    assert record["state"] == "COMPLETED" and record["result_success"] is True
    assert record["recorded_at"] is not None and record["completed_at"] is not None
    assert record["entry_id"] == entry.entry_id
    assert record["original_context"]["fencing_token"] == "8"
    assert restarted.pool_fence_col.document("pool-a").get().to_dict()["fencing_token"] == "8"


def test_conflicting_reuse_and_distinct_missing_operation(repository):
    entry = seed_entry(repository)
    repository.remove_waitlist_entry(context("first", token=1), entry.allocation_id, "cancelled")
    with pytest.raises(AppException) as mismatch:
        repository.remove_waitlist_entry(context("first", token=2, lock="new"), entry.allocation_id, "promoted")
    assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"
    with pytest.raises(AppException) as target_mismatch:
        repository.remove_waitlist_entry(context("first", token=2, lock="new"), "another-allocation", "cancelled")
    assert target_mismatch.value.code == "IDEMPOTENCY_MISMATCH"

    with pytest.raises(AppException) as unknown:
        repository.remove_waitlist_entry(context("second", token=2, lock="new"), entry.allocation_id, "cancelled")
    assert unknown.value.code == "OUTCOME_UNKNOWN"
    record = operation_doc(repository, "second")
    assert record["state"] == "OUTCOME_UNKNOWN"
    assert record["result_success"] is None and record["completed_at"] is None
    assert record["recorded_at"] is not None and record["unknown_reason"]


def test_unknown_result_does_not_remove_entry_added_later(repository):
    op = "missing-at-first"
    with pytest.raises(AppException) as first:
        repository.remove_waitlist_entry(context(op, token=1), "legacy-allocation", "cancelled")
    assert first.value.code == "OUTCOME_UNKNOWN"
    entry = seed_entry(repository, "legacy-allocation")
    with pytest.raises(AppException) as retry:
        repository.remove_waitlist_entry(context(op, token=2, lock="new"), "legacy-allocation", "cancelled")
    assert retry.value.code == "OUTCOME_UNKNOWN"
    assert repository.get_waitlist_entry(entry.allocation_id) is not None


def test_stale_fencing_rejected_and_concurrent_same_operation_replays(repository):
    entry = seed_entry(repository)
    repository.pool_fence_col.document("pool-a").set({
        "fencing_token": "10", "lock_id": "current", "owner_id": "owner",
    })
    with pytest.raises(AppException) as stale:
        repository.remove_waitlist_entry(context("stale", token=9), entry.allocation_id, "cancelled")
    assert stale.value.code == "FAILED_PRECONDITION"
    with pytest.raises(AppException) as wrong_lock:
        repository.remove_waitlist_entry(context("equal-token", token=10, lock="different"), entry.allocation_id, "cancelled")
    assert wrong_lock.value.code == "FAILED_PRECONDITION"
    assert repository.get_waitlist_entry(entry.allocation_id) is not None

    errors = []
    def remove():
        try:
            repository.remove_waitlist_entry(context("racing", token=11, lock="new", owner="new"), entry.allocation_id, "cancelled")
        except Exception as exc:  # surfaced after both workers join
            errors.append(exc)
    threads = [threading.Thread(target=remove) for _ in range(2)]
    for thread in threads: thread.start()
    for thread in threads: thread.join()
    assert not errors
    record = operation_doc(repository, "racing")
    assert record["state"] == "COMPLETED" and record["result_success"] is True
    assert repository.get_waitlist_entry(entry.allocation_id) is None


def test_completed_remove_rejects_recreated_target_with_different_entry_id(repository):
    entry = seed_entry(repository)
    repository.remove_waitlist_entry(context("entry-identity", token=1), entry.allocation_id, "cancelled")
    recreated = seed_entry(repository, entry.allocation_id)
    assert recreated.entry_id != entry.entry_id
    with pytest.raises(AppException) as mismatch:
        repository.remove_waitlist_entry(context("entry-identity", token=2, lock="fresh"), entry.allocation_id, "cancelled")
    assert mismatch.value.code == "IDEMPOTENCY_MISMATCH"
    assert repository.get_waitlist_entry(entry.allocation_id).entry_id == recreated.entry_id


def test_enqueue_operation_is_durable_and_conflicts_with_remove_action(repository):
    repository.save_triage(TriageRecord(
        admission_id="enqueue-admission", triage_level=2, pool_id="pool-a",
        updated_at="2026-10-29T00:00:00+00:00",
    ))
    entry = WaitlistEntry(
        entry_id="enqueue-entry", allocation_id="enqueue-allocation",
        admission_id="enqueue-admission", pool_id="pool-a", triage_level=2,
        queued_at="2026-10-29T00:00:00+00:00",
    )
    op = context("shared-action-id", token=1)
    result = repository.enqueue_waitlist(op, entry)
    assert result.entry == entry and not result.already_existed and not result.replayed
    replay = repository.enqueue_waitlist(
        context("shared-action-id", token=2, lock="fresh"), entry,
    )
    assert replay.entry == entry and not replay.already_existed and replay.replayed
    with pytest.raises(AppException) as conflict:
        repository.remove_waitlist_entry(op, entry.allocation_id, "cancelled")
    assert conflict.value.code == "IDEMPOTENCY_MISMATCH"
    assert repository.get_waitlist_entry(entry.allocation_id) == entry
