"""
Background renewal for one Allocation pool lease while a lock-protected workflow runs.

The believed deadline is kept on this process's monotonic clock, measured from *before*
each Acquire/Renew request, so a slow response can only shorten it. Renewal keeps the lease
alive between steps; it does not cancel remote calls already in flight, and it does not close
the documented race between external lease validation and a Firestore commit. Remote fencing
tokens remain the final guard.
"""
import threading
import time

from fastapi import status

from shared.observability.errors import AppException


class LeaseRenewer:
    def __init__(self, lock_client, lease, lease_ms: int, acquired_at: float, enabled: bool = True,
                 clock=time.monotonic):
        self.lock_client = lock_client
        self.lease = lease
        self.lease_ms = lease_ms
        self.enabled = enabled
        self.clock = clock
        self.interval_s = lease_ms / 3000           # three renewal attempts per lease period
        self.guard_s = lease_ms / 10000             # stop new mutations shortly before expiry
        self.deadline = acquired_at + lease_ms / 1000
        self.refs = 1                               # holders sharing this lock_id
        self._lost = threading.Event()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self.enabled:
            self._thread = threading.Thread(target=self._run, name=f"lease-renewal-{self.lease.pool_id}", daemon=True)
            self._thread.start()

    def _run(self):
        while not self._stop.wait(self.interval_s):
            if self.clock() >= self.deadline:
                self._lost.set()
                return
            started = self.clock()
            try:
                renewed = self.lock_client.renew(self.lease.lock_id, self.lease.owner_id, self.lease_ms)
            except Exception:
                continue  # Coordinator unavailable: keep trying until the believed deadline passes
            if self._stop.is_set():
                return
            if not renewed:
                self._lost.set()  # the Coordinator no longer recognises this owner
                return
            self.deadline = started + self.lease_ms / 1000

    def ensure_held(self):
        if self._lost.is_set() or self.clock() >= self.deadline - self.guard_s:
            raise AppException("LEASE_LOST", f"Pool lease for {self.lease.pool_id} is no longer held; "
                               "no new mutation was sent", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)

    def stop(self, timeout: float = 5.0):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
