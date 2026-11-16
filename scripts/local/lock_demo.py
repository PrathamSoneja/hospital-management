"""Demo helper (Member C): take a lock from the Lock Coordinator, as Bed Allocation would.

Run from the repository root while lock-coordinator-service is up (compose profile "application"):
    python scripts/local/lock_demo.py alice WARDA-ICU
It asks for a 2-minute lease and waits up to 3 seconds, which is long enough to show contention and recovery.
"""
import os
import sys
import time

sys.path.insert(0, os.getcwd())  # Import services/ and shared/ from the repository root.

from services.bed_allocation.clients.grpc_clients import GrpcLockCoordinatorClient  # noqa: E402
from shared.observability.errors import AppException  # noqa: E402

LEASE_MS = 120_000
WAIT_MS = 3_000


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python scripts/local/lock_demo.py <owner> <pool>")
    owner, pool = sys.argv[1], sys.argv[2]
    address = os.environ.get("LOCK_DEMO_ADDR", "127.0.0.1:50051")
    client = GrpcLockCoordinatorClient(address, default_timeout_s=3.0)
    started = time.time()
    try:
        lease = client.acquire(pool, owner, f"demo-{owner}", LEASE_MS, WAIT_MS)
        print(f"{owner}: GOT {pool}  token={lease.fencing_token}  ({time.time() - started:.1f}s)")
    except AppException as error:
        print(f"{owner}: FAILED {error.status_code} {error.code}  ({time.time() - started:.1f}s)")


if __name__ == "__main__":
    main()
