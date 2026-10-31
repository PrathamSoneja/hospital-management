"""
Stateful, exclusive Lock Coordinator double for tests and explicit local runs.

Models one lease per pool, owner checks, lease expiry, monotonically increasing
fencing tokens, bounded waiting, and Acquire de-duplication by acquire_id.
It is a single-process stand-in for Nisarg's ZooKeeper-backed service and is
not evidence of distributed lock safety.
"""
import threading
import time
import uuid
from dataclasses import dataclass

import grpc
from fastapi import status

from services.bed_allocation.clients.base import LockCoordinatorClient, LockLease
from shared.generated import lock_pb2, lock_pb2_grpc
from shared.observability.errors import AppException


@dataclass
class _Lease:
    lock_id: str
    pool_id: str
    owner_id: str
    acquire_id: str
    fencing_token: int
    expires_at_ms: int
    lock_wait_ms: int


class LockTimeout(Exception):
    pass


class StatefulLockCoordinator:
    def __init__(self, clock=time.time):
        self._clock = clock
        self._cond = threading.Condition()
        self._leases: dict[str, _Lease] = {}        # pool_id -> active lease
        self._tokens: dict[str, int] = {}           # pool_id -> last issued token
        self._by_acquire: dict[str, _Lease] = {}    # acquire_id -> lease (de-duplication)
        self.acquisitions = 0

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def _expire(self, pool_id: str) -> None:
        lease = self._leases.get(pool_id)
        if lease and lease.expires_at_ms <= self._now_ms():
            del self._leases[pool_id]

    def acquire(self, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms) -> _Lease:
        started = self._now_ms()
        deadline = time.monotonic() + wait_timeout_ms / 1000
        with self._cond:
            while True:
                self._expire(pool_id)
                current = self._leases.get(pool_id)
                retried = self._by_acquire.get(acquire_id)
                if retried and current is retried and retried.owner_id == owner_id:
                    return retried  # retry of a granted Acquire returns the same lease
                if current is None:
                    token = self._tokens.get(pool_id, 0) + 1
                    self._tokens[pool_id] = token
                    lease = _Lease(f"lock-{uuid.uuid4().hex}", pool_id, owner_id, acquire_id, token,
                                   self._now_ms() + lease_ms, max(0, self._now_ms() - started))
                    self._leases[pool_id] = lease
                    self._by_acquire[acquire_id] = lease
                    self.acquisitions += 1
                    return lease
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise LockTimeout(pool_id)
                wait_for = min(remaining, max(0.001, (current.expires_at_ms - self._now_ms()) / 1000))
                self._cond.wait(wait_for)

    def release(self, lock_id, owner_id) -> bool:
        with self._cond:
            for pool_id, lease in list(self._leases.items()):
                if lease.lock_id == lock_id:
                    if lease.owner_id != owner_id:
                        return False
                    del self._leases[pool_id]
                    self._cond.notify_all()
                    return True
            return False

    def validate(self, lock_id, owner_id, pool_id, fencing_token) -> tuple[bool, str]:
        with self._cond:
            self._expire(pool_id)
            lease = self._leases.get(pool_id)
            if lease is None:
                return False, "LEASE_NOT_HELD"
            if lease.lock_id != lock_id or lease.owner_id != owner_id:
                return False, "NOT_OWNER"
            if lease.fencing_token != int(fencing_token):
                return False, "STALE_FENCING_TOKEN"
            return True, ""

    def renew(self, lock_id, owner_id, lease_ms) -> tuple[bool, int]:
        with self._cond:
            for pool_id in list(self._leases):
                self._expire(pool_id)
            for lease in self._leases.values():
                if lease.lock_id == lock_id and lease.owner_id == owner_id:
                    lease.expires_at_ms = self._now_ms() + lease_ms
                    return True, lease.expires_at_ms
            return False, 0


class InProcessLockClient(LockCoordinatorClient):
    """Allocation LockCoordinatorClient backed by StatefulLockCoordinator."""

    def __init__(self, coordinator: StatefulLockCoordinator):
        self.coordinator = coordinator

    def acquire(self, pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms) -> LockLease:
        try:
            lease = self.coordinator.acquire(pool_id, owner_id, acquire_id, lease_ms, wait_timeout_ms)
        except LockTimeout:
            raise AppException("DEADLINE_EXCEEDED", f"Lock acquisition timed out on pool {pool_id}",
                               status.HTTP_504_GATEWAY_TIMEOUT, retryable=True)
        return LockLease(lock_id=lease.lock_id, pool_id=pool_id, owner_id=owner_id,
                         fencing_token=lease.fencing_token, expires_at=lease.expires_at_ms,
                         lock_wait_ms=lease.lock_wait_ms)

    def release(self, lock_id, owner_id) -> bool:
        return self.coordinator.release(lock_id, owner_id)

    def validate(self, lock_id, owner_id, pool_id, fencing_token) -> bool:
        return self.coordinator.validate(lock_id, owner_id, pool_id, fencing_token)[0]


class StatefulLockServicer(lock_pb2_grpc.LockCoordinatorServicer):
    """gRPC face of the same model, for Triage/Allocation adapters in local runs."""

    def __init__(self, coordinator: StatefulLockCoordinator):
        self.coordinator = coordinator

    def Acquire(self, request, context):
        try:
            lease = self.coordinator.acquire(request.pool_id, request.owner_id, request.acquire_id,
                                             request.lease_ms, request.wait_timeout_ms)
        except LockTimeout:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "lock wait timeout")
        return lock_pb2.AcquireLockResponse(lock_id=lease.lock_id, pool_id=lease.pool_id, owner_id=lease.owner_id,
                                            fencing_token=lease.fencing_token, expires_at=lease.expires_at_ms,
                                            lock_wait_ms=lease.lock_wait_ms)

    def Renew(self, request, context):
        success, expires_at = self.coordinator.renew(request.lock_id, request.owner_id, request.lease_ms)
        return lock_pb2.RenewLockResponse(success=success, expires_at=expires_at)

    def Release(self, request, context):
        return lock_pb2.ReleaseLockResponse(success=self.coordinator.release(request.lock_id, request.owner_id))

    def Validate(self, request, context):
        valid, reason = self.coordinator.validate(request.lock_id, request.owner_id, request.pool_id, request.fencing_token)
        return lock_pb2.ValidateLockResponse(valid=valid, reason=reason)
