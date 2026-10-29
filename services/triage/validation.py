"""
Injectable ownership and fencing token validation interface.
Supports local mock doubles for unit testing and gRPC LockCoordinator client for integration.
"""
from abc import ABC, abstractmethod
from typing import Optional
import grpc
from shared.generated import lock_pb2
from shared.generated.common_pb2 import MutationContext
from shared.observability.errors import AppException
from starlette import status

class FencingValidator(ABC):
    """Abstract interface to validate distributed lock ownership and fencing tokens."""
    @abstractmethod
    def validate(self, context: MutationContext) -> bool:
        """Returns True if lock and token are valid and not superseded; False otherwise."""
        pass

class MockFencingValidator(FencingValidator):
    """
    Test double for unit testing.
    NOTE: A mock validator DOES NOT enforce real distributed safety against concurrent split-brain.
    Real distributed safety in production requires the ZooKeeper Lock Coordinator.
    """
    def __init__(self, accept_all: bool = True):
        self.accept_all = accept_all
        self.last_fencing_token: dict[str, int] = {}

    def validate(self, context: MutationContext) -> bool:
        if not self.accept_all:
            return False
        if not context.lock_id or not context.owner_id:
            return False
        # Monotonic fencing check per pool
        current_max = self.last_fencing_token.get(context.pool_id, 0)
        if context.fencing_token < current_max:
            return False
        self.last_fencing_token[context.pool_id] = context.fencing_token
        return True

class RemoteLockValidator(FencingValidator):
    """Validates lock and fencing token via Lock Coordinator gRPC."""
    def __init__(self, lock_coordinator_stub):
        self.stub = lock_coordinator_stub

    def validate(self, context: MutationContext) -> bool:
        if self.stub is None:
            raise AppException("SERVICE_UNAVAILABLE", "Lock Coordinator is not configured", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True)
        try:
            req = lock_pb2.ValidateLockRequest(
                lock_id=context.lock_id,
                owner_id=context.owner_id,
                pool_id=context.pool_id,
                fencing_token=context.fencing_token
            )
            metadata = (("x-request-id", context.request_id),) if context.request_id else None
            resp = self.stub.Validate(req, timeout=2.0, metadata=metadata)
            return resp.valid
        except grpc.RpcError as error:
            if error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                raise AppException("DEADLINE_EXCEEDED", "Lock Coordinator validation timed out", status.HTTP_504_GATEWAY_TIMEOUT, retryable=True) from error
            raise AppException("SERVICE_UNAVAILABLE", "Lock Coordinator is unavailable", status.HTTP_503_SERVICE_UNAVAILABLE, retryable=True) from error
