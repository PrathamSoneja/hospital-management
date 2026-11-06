"""
Dependency contract acceptance: the same tests run against the sandbox stand-ins (default)
or against real endpoints when ACCEPTANCE_* settings are provided (future staging):

  ACCEPTANCE_LOCK_ADDR, ACCEPTANCE_BED_ADDR, ACCEPTANCE_STAFF_ADDR   host:port (gRPC)
  ACCEPTANCE_PATIENT_URL                                            .../api/v1
  ACCEPTANCE_POOL_ID, ACCEPTANCE_ADMISSION_ID, ACCEPTANCE_BED_ID     designated TEST resources
  ACCEPTANCE_ALLOW_MUTATION=1                                       required before any lock/bed/staff mutation

Real targets are read-only unless mutation is explicitly allowed; never point this at
production or at resources holding real patient data.
"""
import os
import uuid

import pytest

from tests.sandbox.fixtures import seed_pool

REAL_SETTINGS = ("LOCK_ADDR", "BED_ADDR", "STAFF_ADDR", "PATIENT_URL", "POOL_ID", "ADMISSION_ID", "BED_ID")


@pytest.fixture(scope="session")
def targets(request):
    if os.environ.get("ACCEPTANCE_LOCK_ADDR"):
        missing = ["ACCEPTANCE_" + n for n in REAL_SETTINGS if not os.environ.get("ACCEPTANCE_" + n)]
        if missing:
            pytest.fail("real-endpoint acceptance needs " + ", ".join(missing))
        values = {n.lower(): os.environ["ACCEPTANCE_" + n] for n in REAL_SETTINGS}
        return {**values, "mode": "real", "mutation": os.environ.get("ACCEPTANCE_ALLOW_MUTATION") == "1"}
    sb = request.getfixturevalue("sandbox")
    pool = f"P-ACCEPT-{uuid.uuid4().hex[:6]}"
    admission = f"{pool}-adm-1"
    seed_pool(sb, pool, beds=1, admissions=[admission], staff=2)
    deps = f"127.0.0.1:{sb.ports['deps_grpc']}"
    return {"lock_addr": deps, "bed_addr": deps, "staff_addr": deps, "patient_url": f"{sb.deps_http}/api/v1",
            "pool_id": pool, "admission_id": admission, "bed_id": f"{pool}-bed-01", "mode": "sandbox", "mutation": True}


@pytest.fixture
def mutating(targets):
    if not targets["mutation"]:
        pytest.skip("set ACCEPTANCE_ALLOW_MUTATION=1 with designated test resources to run mutating checks")
    return targets
