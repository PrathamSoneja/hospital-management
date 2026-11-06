"""
Shared sandbox fixtures (registered once from tests/conftest.py).

Session sandbox: dependency stand-ins, one Triage, and two Allocation processes started
through their production entrypoints, sharing disposable PostgreSQL (hospital_sandbox_it_test),
a Firestore emulator project, and local Kafka + Pub/Sub emulator topics unique to the run.
Logs are written to .sandbox/pytest/ (ignored; uploaded as a CI artifact).
"""
import os
import time
import uuid
from pathlib import Path

import pytest

from tests.sandbox.runtime import REPO_ROOT, Sandbox, http_json

SANDBOX_ENV = ("TEST_POSTGRES_URL", "FIRESTORE_EMULATOR_HOST", "SANDBOX_KAFKA_BOOTSTRAP", "PUBSUB_EMULATOR_HOST")


@pytest.fixture(scope="session")
def sandbox():
    missing = [name for name in SANDBOX_ENV if not os.environ.get(name, "").strip()]
    if missing:
        pytest.skip(f"sandbox integration needs {', '.join(missing)}")
    sb = Sandbox(
        postgres_url=os.environ["TEST_POSTGRES_URL"], database="hospital_sandbox_it_test",
        firestore_host=os.environ["FIRESTORE_EMULATOR_HOST"], pubsub_host=os.environ["PUBSUB_EMULATOR_HOST"],
        kafka_bootstrap=os.environ["SANDBOX_KAFKA_BOOTSTRAP"], log_dir=REPO_ROOT / ".sandbox" / "pytest",
        allocators=2, recovery_claim_seconds=8,
    )
    sb.start()
    try:
        yield sb
    finally:
        sb.stop()


class Api:
    """Thin REST client over the sandbox processes; every call carries its own request ID."""

    def __init__(self, sandbox: Sandbox):
        self.sb = sandbox

    def _headers(self, key=None):
        headers = {"X-Request-ID": f"it-{uuid.uuid4().hex[:12]}"}
        if key:
            headers["Idempotency-Key"] = key
        return headers

    def create(self, admission_id, pool_id, key, allocator=0):
        return http_json("POST", f"{self.sb.allocator_url(allocator)}/api/v1/allocations",
                         {"admission_id": admission_id, "pool_id": pool_id}, self._headers(key), timeout=30)

    def get(self, allocation_id, allocator=0):
        return http_json("GET", f"{self.sb.allocator_url(allocator)}/api/v1/allocations/{allocation_id}",
                         headers=self._headers(), timeout=10)[1]

    def release(self, allocation_id, key, allocator=0):
        return http_json("POST", f"{self.sb.allocator_url(allocator)}/api/v1/allocations/{allocation_id}/release",
                         {"reason": "discharged"}, self._headers(key), timeout=30)

    def cancel(self, allocation_id, key, allocator=0):
        return http_json("POST", f"{self.sb.allocator_url(allocator)}/api/v1/allocations/{allocation_id}/cancel",
                         {"reason": "withdrawn"}, self._headers(key), timeout=30)

    def triage(self, admission_id, level, pool_id, key=None):
        return http_json("PUT", f"{self.sb.triage_url}/api/v1/triage/{admission_id}",
                         {"triage_level": level, "pool_id": pool_id},
                         self._headers(key or f"triage-{uuid.uuid4().hex}"), timeout=15)

    def waitlist(self, pool_id):
        status, body, _ = http_json("GET", f"{self.sb.triage_url}/api/v1/waitlists/{pool_id}?limit=50",
                                    headers=self._headers(), timeout=10)
        assert status == 200, body
        return body["items"]

    def wait_status(self, allocation_id, statuses, timeout=60):
        deadline = time.monotonic() + timeout
        record = None
        while time.monotonic() < deadline:
            record = self.get(allocation_id)
            if record and record.get("status") in statuses and not record.get("pending_action"):
                return record
            time.sleep(0.5)
        raise AssertionError(f"allocation did not reach {statuses}: {record}")


@pytest.fixture
def api(sandbox):
    return Api(sandbox)


@pytest.fixture
def pool(request, sandbox):
    """A pool name unique to this test, so tests share the sandbox without interfering."""
    name = request.node.name.replace("test_", "")[:24].upper().replace("_", "-")
    return f"P-{name}-{uuid.uuid4().hex[:6]}"


def seed_pool(sandbox, pool_id, beds, admissions, staff=100, levels=None):
    """admissions: list of admission IDs; levels: optional urgency per admission (default 3)."""
    levels = levels or {}
    sandbox.seed({
        "beds": [{"bed_id": f"{pool_id}-bed-{n:02d}", "pool_id": pool_id, "ward": f"WARD-{pool_id}"}
                 for n in range(1, beds + 1)],
        "staff_capacity": {pool_id: staff},
        "admissions": [{"admission_id": a, "patient_id": f"pat-{a}", "status": "OPEN"} for a in admissions],
        "triage": [{"admission_id": a, "triage_level": levels.get(a, 3), "pool_id": pool_id} for a in admissions],
    })
