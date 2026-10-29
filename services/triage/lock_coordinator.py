"""gRPC adapter for the teammate-owned Lock Coordinator contract."""
from dataclasses import dataclass
import logging

import grpc

from shared.generated import lock_pb2, lock_pb2_grpc
from shared.observability.errors import AppException
from starlette import status

logger = logging.getLogger("triage-service")


@dataclass(frozen=True)
class LockLease:
    lock_id: str
    pool_id: str
    owner_id: str
    fencing_token: int
    expires_at: int


class GrpcLockCoordinatorClient:
    """Uses the existing Acquire, Validate, and Release protobuf RPCs."""

    def __init__(self, target: str, timeout_s: float = 2.0):
        if not target:
            raise ValueError("LOCK_COORDINATOR_ADDR is required in coordinator mode")
        self.target = target
        self.timeout_s = timeout_s
        self.channel = grpc.insecure_channel(target)
        self.stub = lock_pb2_grpc.LockCoordinatorStub(self.channel)

    @staticmethod
    def _metadata(request_id: str | None):
        return (("x-request-id", request_id),) if request_id else None

    @staticmethod
    def _raise_rpc(error: grpc.RpcError, operation: str):
        if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise AppException("DEADLINE_EXCEEDED", f"Lock Coordinator {operation} timed out", status.HTTP_504_GATEWAY_TIMEOUT, retryable=True) from error
        raise AppException("SERVICE_UNAVAILABLE", f"Lock Coordinator {operation} is unavailable", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True) from error

    def acquire(self, pool_id: str, owner_id: str, acquire_id: str, lease_ms: int,
                wait_timeout_ms: int, request_id: str = "") -> LockLease:
        request = lock_pb2.AcquireLockRequest(
            pool_id=pool_id, owner_id=owner_id, acquire_id=acquire_id,
            lease_ms=lease_ms, wait_timeout_ms=wait_timeout_ms,
        )
        try:
            response = self.stub.Acquire(
                request, timeout=wait_timeout_ms / 1000 + self.timeout_s,
                metadata=self._metadata(request_id),
            )
        except grpc.RpcError as error:
            self._raise_rpc(error, "acquire")
        if not response.lock_id or response.pool_id != pool_id or response.owner_id != owner_id:
            raise AppException("SERVICE_UNAVAILABLE", "Lock Coordinator returned an invalid lease", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
        logger.info("coordinator_lease_acquired", extra={"event": "coordinator_lease_acquired", "request_id": request_id, "pool_id": pool_id})
        return LockLease(response.lock_id, response.pool_id, response.owner_id,
                         response.fencing_token, response.expires_at)

    def validate(self, lock_id: str, owner_id: str, pool_id: str,
                 fencing_token: int, request_id: str = "") -> bool:
        request = lock_pb2.ValidateLockRequest(
            lock_id=lock_id, owner_id=owner_id, pool_id=pool_id,
            fencing_token=fencing_token,
        )
        try:
            response = self.stub.Validate(
                request, timeout=self.timeout_s,
                metadata=self._metadata(request_id),
            )
        except grpc.RpcError as error:
            self._raise_rpc(error, "validation")
        logger.info("coordinator_lease_validated", extra={"event": "coordinator_lease_validated", "request_id": request_id, "pool_id": pool_id})
        return response.valid

    def release(self, lock_id: str, owner_id: str, request_id: str = "") -> bool:
        request = lock_pb2.ReleaseLockRequest(lock_id=lock_id, owner_id=owner_id)
        try:
            response = self.stub.Release(
                request, timeout=self.timeout_s,
                metadata=self._metadata(request_id),
            )
        except grpc.RpcError as error:
            self._raise_rpc(error, "release")
        logger.info("coordinator_lease_released", extra={"event": "coordinator_lease_released", "request_id": request_id})
        return response.success

    def check_ready(self, timeout_s: float = 0.5) -> bool:
        try:
            grpc.channel_ready_future(self.channel).result(timeout=timeout_s)
            return True
        except grpc.FutureTimeoutError:
            return False

    def close(self):
        self.channel.close()
