"""gRPC server for hospital.v1.LockCoordinator. All lock state lives in ZooKeeper."""
import logging
import os
import signal
import threading
from concurrent import futures

import grpc
import uvicorn

from services.lock_coordinator import zk_store
from services.lock_coordinator.metrics import MetricsInterceptor, record_acquire, record_reap
from services.lock_coordinator.rest import build_app
from shared.generated import lock_pb2, lock_pb2_grpc

logger = logging.getLogger("lock-coordinator")

REAPER_INTERVAL_S = 0.5


class LockCoordinatorServicer(lock_pb2_grpc.LockCoordinatorServicer):
    def __init__(self, zk):
        self.zk = zk

    def Acquire(self, request, context):
        if not zk_store.POOL_ID_PATTERN.fullmatch(request.pool_id) or not request.owner_id or request.lease_ms <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "pool_id, owner_id and lease_ms are required")

        started = zk_store.now_ms()
        # A retry reuses its earlier place in line, even when both copies arrive at the same moment.
        path = zk_store.claim_place(
            self.zk, request.pool_id, request.owner_id, request.acquire_id,
            request.lease_ms, request.wait_timeout_ms,
        )

        if not zk_store.wait_for_turn(self.zk, request.pool_id, path, request.wait_timeout_ms / 1000):
            zk_store.leave_queue(self.zk, path)  # Never block the requests behind us.
            record_acquire(request.pool_id, "timeout")
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "lock wait timeout")

        expires_at = zk_store.start_lease(self.zk, path)
        if expires_at is None:
            context.abort(grpc.StatusCode.UNAVAILABLE, "lock was lost before it could be granted")

        lock_wait_ms = zk_store.now_ms() - started
        record_acquire(request.pool_id, "granted", lock_wait_ms)
        return lock_pb2.AcquireLockResponse(
            lock_id=zk_store.lock_id_for(path),
            pool_id=request.pool_id,
            owner_id=request.owner_id,
            fencing_token=zk_store.fencing_token(self.zk, path),
            expires_at=expires_at,
            lock_wait_ms=lock_wait_ms,
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
            reaped = zk_store.reap_expired(zk, on_reap=record_reap)
            zk_store.reap_tombstones(zk)
            if reaped:
                logger.info("reaped %d expired lease(s)", reaped)
        except Exception:
            logger.exception("reaper pass failed")


def create_server(zk, address, workers=128):
    """Build the gRPC server (not started yet). Return it and the port it bound."""
    # Each waiting Acquire holds a worker, so keep plenty free for Release calls.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=workers), interceptors=[MetricsInterceptor()])
    lock_pb2_grpc.add_LockCoordinatorServicer_to_server(LockCoordinatorServicer(zk), server)
    port = server.add_insecure_port(address)
    return server, port


def start_rest(zk):
    """Serve health checks, /metrics and the lock inspector in a background thread."""
    port = int(os.environ.get("LOCK_REST_PORT", "8000"))
    rest = uvicorn.Server(uvicorn.Config(build_app(zk), host="0.0.0.0", port=port, log_level="warning"))
    threading.Thread(target=rest.run, daemon=True).start()
    logger.info("Lock inspector and metrics on port %s", port)
    return rest


def serve():
    logging.basicConfig(level=logging.INFO)
    zk = zk_store.connect()

    workers = int(os.environ.get("LOCK_GRPC_WORKERS", "128"))
    port = os.environ.get("LOCK_GRPC_PORT", "50051")
    server, _ = create_server(zk, f"0.0.0.0:{port}", workers)

    stop = threading.Event()
    threading.Thread(target=reaper_loop, args=(zk, stop), daemon=True).start()
    rest = start_rest(zk)

    # Kubernetes sends SIGTERM first. Stopping the server lets the finally block close ZooKeeper cleanly.
    signal.signal(signal.SIGTERM, lambda signum, frame: server.stop(5))
    server.start()
    logger.info("Lock Coordinator listening on port %s", port)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        rest.should_exit = True
        server.stop(2)
        zk.stop()


if __name__ == "__main__":
    serve()