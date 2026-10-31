"""Coordinator adapter contract tests using a local fake gRPC server."""
import grpc
import pytest
from fastapi.testclient import TestClient

from services.triage.lock_coordinator import GrpcLockCoordinatorClient
from shared.observability.errors import AppException
from tests.support.triage_coordinator_server import coordinator_server


def test_adapter_uses_existing_rpc_contract_and_propagates_request_id(coordinator_server):
    implementation, target = coordinator_server
    client = GrpcLockCoordinatorClient(target)
    lease = client.acquire("pool", "triage", "acq-1", 1000, 0, request_id="http-req-1")
    assert lease.lock_id == "lock-test" and lease.fencing_token == 12
    assert client.validate(lease.lock_id, lease.owner_id, lease.pool_id, lease.fencing_token, "http-req-2")
    assert client.release(lease.lock_id, lease.owner_id, "http-req-3")
    assert implementation.request_ids == ["http-req-1", "http-req-2", "http-req-3"]
    assert client.check_ready()
    client.close()


def test_adapter_maps_rpc_deadline_and_unavailable_to_consistent_errors(coordinator_server):
    implementation, target = coordinator_server
    implementation.block_acquire = True
    client = GrpcLockCoordinatorClient(target, timeout_s=0.01)
    with pytest.raises(AppException) as deadline:
        client.acquire("pool", "triage", "acq-timeout", 1000, 0, request_id="deadline-id")
    assert deadline.value.status_code == 504
    client.close()

    unavailable = GrpcLockCoordinatorClient("127.0.0.1:1", timeout_s=0.05)

    class UnavailableRpc(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNAVAILABLE

    class UnavailableStub:
        def Validate(self, *args, **kwargs):
            raise UnavailableRpc()

    unavailable.stub = UnavailableStub()
    with pytest.raises(AppException) as error:
        unavailable.validate("l", "o", "p", 1, "unavailable-id")
    assert error.value.code == "SERVICE_UNAVAILABLE" and error.value.status_code == 503
    unavailable.close()


def test_storage_and_fencing_configuration_never_fall_back_to_fakes(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.setenv("TRIAGE_STORAGE_BACKEND", "memory")
    monkeypatch.setenv("TRIAGE_FENCING_MODE", "coordinator")
    monkeypatch.delenv("LOCK_COORDINATOR_ADDR", raising=False)
    from services.triage import main

    unconfigured = main.build_app()
    with TestClient(unconfigured) as client:
        assert client.get("/healthz").status_code == 200
        assert client.get("/readyz").status_code == 503

    monkeypatch.setenv("TRIAGE_STORAGE_BACKEND", "mystery")
    with pytest.raises(ValueError, match="Unknown TRIAGE_STORAGE_BACKEND"):
        main.build_app()

    monkeypatch.setenv("TRIAGE_STORAGE_BACKEND", "memory")
    monkeypatch.setenv("ENVIRONMENT", "local")
    with pytest.raises(ValueError, match="memory is permitted only"):
        main.build_app()
    monkeypatch.setenv("TRIAGE_STORAGE_BACKEND", "firestore")
    monkeypatch.setenv("FIRESTORE_PROJECT_ID", "test-project")
    monkeypatch.setenv("TRIAGE_FENCING_MODE", "fake")
    with pytest.raises(ValueError, match="only coordinator is supported"):
        main.build_app()
