"""gRPC server for hospital.v1.LockCoordinator. All lock state lives in ZooKeeper."""
import logging
import os
import re
import threading
from concurrent import futures

import grpc

from services.lock_coordinator import zk_store
from shared.generated import lock_pb2, lock_pb2_grpc

logger = logging.getLogger("lock-coordinator")

POOL_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]+")
REAPER_INTERVAL_S = 0.5


class LockCoordinatorServicer(lock_pb2_grpc.LockCoordinatorServicer):
    def __init__(self, zk):
        self.zk = zk

    def Acquire(self, request, context):
        if not POOL_ID_PATTERN.fullmatch(request.pool_id) or not request.owner_id or request.lease_ms <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "pool_id, owner_id and lease_ms are required")

        started = zk_store.now_ms()
        path = None
        if request.acquire_id:  # A retry reuses its earlier place in line.
            path = zk_store.find_request(self.zk, request.pool_id, request.owner_id, request.acquire_id)
        if path is None:
            path = zk_store.join_queue(
                self.zk, request.pool_id, request.owner_id, request.acquire_id, request.lease_ms, request.wait_timeout_ms,
            )

        if not zk_store.wait_for_turn(self.zk, request.pool_id, path, request.wait_timeout_ms / 1000):
            zk_store.leave_queue(self.zk, path)  # Never block the requests behind us.
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "lock wait timeout")

        expires_at = zk_store.start_lease(self.zk, path)
        if expires_at is None:
            context.abort(grpc.StatusCode.UNAVAILABLE, "lock was lost before it could be granted")

        return lock_pb2.AcquireLockResponse(
            lock_id=zk_store.lock_id_for(path),
            pool_id=request.pool_id,
            owner_id=request.owner_id,
            fencing_token=zk_store.fencing_token(self.zk, path),
            expires_at=expires_at,
            lock_wait_ms=zk_store.now_ms() - started,
        )

    def Renew(self, request, context):
        path = zk_store.path_for(request.lock_id)
        expires_at = None
        if path is not None and request.lease_ms > 0:
            expires_at = zk_store.renew_lease(self.zk, path, request.owner_id, request.lease_ms)
        return lock_pb2.RenewLockResponse(success=expires_at is not None, expires_at=expires_at or 0)

    def Release(self, request, context):
        path = zk_store.path_for(request.lock_id)
        success = path is not None and zk_store.release(self.zk, path, request.owner_id)
        return lock_pb2.ReleaseLockResponse(success=success)

    def Validate(self, request, context):
        path = zk_store.path_for(request.lock_id)
        if path is None:
            return lock_pb2.ValidateLockResponse(valid=False, reason="LEASE_NOT_HELD")
        if not request.lock_id.startswith(f"{request.pool_id}/"):
            return lock_pb2.ValidateLockResponse(valid=False, reason="NOT_OWNER")
        valid, reason = zk_store.validate(self.zk, path, request.owner_id, request.fencing_token)
        return lock_pb2.ValidateLockResponse(valid=valid, reason=reason)


def reaper_loop(zk, stop):
    """Sweep for expired leases until told to stop."""
    while not stop.wait(REAPER_INTERVAL_S):
        try:
            reaped = zk_store.reap_expired(zk)
            if reaped:
                logger.info("reaped %d expired lease(s)", reaped)
        except Exception:
            logger.exception("reaper pass failed")


def serve():
    logging.basicConfig(level=logging.INFO)
    zk = zk_store.connect()

    # Each waiting Acquire holds a worker, so keep plenty free for Release calls.
    workers = int(os.environ.get("LOCK_GRPC_WORKERS", "128"))
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers))
    lock_pb2_grpc.add_LockCoordinatorServicer_to_server(LockCoordinatorServicer(zk), server)
    port = os.environ.get("LOCK_GRPC_PORT", "50051")
    server.add_insecure_port(f"0.0.0.0:{port}")

    stop = threading.Event()
    threading.Thread(target=reaper_loop, args=(zk, stop), daemon=True).start()

    server.start()
    logger.info("Lock Coordinator listening on port %s", port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.stop(2)
        zk.stop()


if __name__ == "__main__":
    serve()