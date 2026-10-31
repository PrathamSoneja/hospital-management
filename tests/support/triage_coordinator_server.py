"""Contract-shaped local gRPC server for adapter tests, not a live lock service."""

from concurrent import futures
import time

import grpc
import pytest

from shared.generated import lock_pb2, lock_pb2_grpc


class CoordinatorContractStub(lock_pb2_grpc.LockCoordinatorServicer):
    def __init__(self):
        self.request_ids = []
        self.block_acquire = False

    def Acquire(self, request, context):
        metadata = dict((item.key, item.value) for item in context.invocation_metadata())
        self.request_ids.append(metadata.get("x-request-id"))
        if self.block_acquire:
            time.sleep(0.1)
        return lock_pb2.AcquireLockResponse(
            lock_id="lock-test", pool_id=request.pool_id, owner_id=request.owner_id,
            fencing_token=12, expires_at=12345,
        )

    def Validate(self, request, context):
        metadata = dict((item.key, item.value) for item in context.invocation_metadata())
        self.request_ids.append(metadata.get("x-request-id"))
        return lock_pb2.ValidateLockResponse(valid=request.fencing_token == 12)

    def Release(self, request, context):
        metadata = dict((item.key, item.value) for item in context.invocation_metadata())
        self.request_ids.append(metadata.get("x-request-id"))
        return lock_pb2.ReleaseLockResponse(success=True)


@pytest.fixture
def coordinator_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    implementation = CoordinatorContractStub()
    lock_pb2_grpc.add_LockCoordinatorServicer_to_server(implementation, server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield implementation, f"127.0.0.1:{port}"
    server.stop(0).wait()
