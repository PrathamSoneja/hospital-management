"""
Unit and integration tests for Bed Registry Service.
Owned by Member D (Jay Patel).
Verifies:
1. Bed inventory query & filtering
2. Bed allocation and release
3. Ownership protection on release
4. Idempotency on operation_id
5. Fencing token safety: STALE FENCING TOKENS ARE REJECTED
6. gRPC servicer and REST endpoints
"""
import pytest
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from services.bed_registry.models import Base
from services.bed_registry.service import BedRegistryDomainService
from services.bed_registry.grpc_server import BedGrpcServicer
from services.bed_registry.main import build_app
from shared.generated import bed_pb2, common_pb2

@pytest.fixture
def in_memory_service():
    """Sets up an in-memory SQLite database and BedRegistryDomainService."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool
    )
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    service = BedRegistryDomainService(session_factory)

    # Seed test beds
    service.create_or_update_bed("bed-icu-01", "WARDA-ICU", "ICU-A", "AVAILABLE")
    service.create_or_update_bed("bed-icu-02", "WARDA-ICU", "ICU-A", "AVAILABLE")
    service.create_or_update_bed("bed-hdu-01", "WARDA-HDU", "HDU-A", "AVAILABLE")
    return service

def test_list_available_beds(in_memory_service):
    beds = in_memory_service.list_available_beds("WARDA-ICU")
    assert len(beds) == 2
    assert {b.bed_id for b in beds} == {"bed-icu-01", "bed-icu-02"}

def test_get_bed(in_memory_service):
    bed = in_memory_service.get_bed("bed-icu-01")
    assert bed is not None
    assert bed.bed_id == "bed-icu-01"
    assert bed.status == "AVAILABLE"

    not_found = in_memory_service.get_bed("non-existent-bed")
    assert not_found is None

def test_allocate_bed_success(in_memory_service):
    success, bed, err = in_memory_service.allocate_bed(
        operation_id="op-alloc-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10,
        request_id="req-1"
    )
    assert success is True
    assert err == ""
    assert bed.status == "OCCUPIED"
    assert bed.current_allocation_id == "alloc-100"

    # Verify no longer returned in available list
    available = in_memory_service.list_available_beds("WARDA-ICU")
    assert len(available) == 1
    assert available[0].bed_id == "bed-icu-02"

def test_allocate_bed_already_occupied(in_memory_service):
    # First allocation succeeds
    in_memory_service.allocate_bed(
        operation_id="op-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10
    )

    # Competing allocation for same bed fails
    success, bed, err = in_memory_service.allocate_bed(
        operation_id="op-2",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-101",
        fencing_token=11
    )
    assert success is False
    assert "already occupied" in err

def test_release_bed_success(in_memory_service):
    # Allocate
    in_memory_service.allocate_bed(
        operation_id="op-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10
    )

    # Release by owner
    success, err = in_memory_service.release_bed(
        operation_id="op-rel-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=11
    )
    assert success is True
    assert err == ""

    # Verify bed is AVAILABLE again
    bed = in_memory_service.get_bed("bed-icu-01")
    assert bed.status == "AVAILABLE"
    assert bed.current_allocation_id is None

def test_release_bed_unauthorized_owner(in_memory_service):
    in_memory_service.allocate_bed(
        operation_id="op-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10
    )

    # Different allocation attempts release -> REJECTED
    success, err = in_memory_service.release_bed(
        operation_id="op-rel-bad",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-999", # wrong owner
        fencing_token=11
    )
    assert success is False
    assert "cannot be released by alloc-999" in err

def test_idempotent_operation_replay(in_memory_service):
    # First allocate
    s1, b1, err1 = in_memory_service.allocate_bed(
        operation_id="op-idempotent-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10
    )
    assert s1 is True

    # Replay same operation_id
    s2, b2, err2 = in_memory_service.allocate_bed(
        operation_id="op-idempotent-1",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-100",
        fencing_token=10
    )
    assert s2 is True
    assert b2.bed_id == b1.bed_id

def test_stale_fencing_token_rejected(in_memory_service):
    """
    CRITICAL VIVA DEMONSTRATION TEST:
    A write with a lower fencing token must be rejected to prevent
    a delayed / paused node from corrupting state.
    """
    # 1. First request arrives with token 50
    s1, b1, err1 = in_memory_service.allocate_bed(
        operation_id="op-token-50",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-1",
        fencing_token=50
    )
    assert s1 is True
    assert in_memory_service.get_highest_fencing_token("WARDA-ICU") == 50

    # 2. Stale request arrives with older token 45 (e.g. from stalled GC pause)
    s2, b2, err2 = in_memory_service.allocate_bed(
        operation_id="op-token-stale",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-02",
        allocation_id="alloc-2",
        fencing_token=45
    )
    assert s2 is False
    assert "Stale fencing token 45" in err2
    assert "highest token is 50" in err2

    # 3. Stale release attempt also rejected
    s3, err3 = in_memory_service.release_bed(
        operation_id="op-release-stale",
        pool_id="WARDA-ICU",
        bed_id="bed-icu-01",
        allocation_id="alloc-1",
        fencing_token=40
    )
    assert s3 is False
    assert "Stale fencing token 40" in err3

def test_grpc_servicer(in_memory_service):
    servicer = BedGrpcServicer(in_memory_service)
    ctx = MagicMock()

    # 1. List Available Beds
    list_req = bed_pb2.ListAvailableBedsRequest(pool_id="WARDA-ICU")
    list_resp = servicer.ListAvailableBeds(list_req, ctx)
    assert len(list_resp.beds) == 2

    # 2. Allocate Bed via gRPC
    mut_ctx = common_pb2.MutationContext(
        operation_id="grpc-op-1",
        request_id="req-grpc-1",
        pool_id="WARDA-ICU",
        fencing_token=100
    )
    alloc_req = bed_pb2.AllocateBedRequest(
        context=mut_ctx,
        bed_id="bed-icu-01",
        allocation_id="alloc-grpc-1"
    )
    alloc_resp = servicer.AllocateBed(alloc_req, ctx)
    assert alloc_resp.success is True
    assert alloc_resp.bed.status == "OCCUPIED"

    # 3. Release Bed via gRPC
    mut_ctx_rel = common_pb2.MutationContext(
        operation_id="grpc-op-rel-1",
        request_id="req-grpc-2",
        pool_id="WARDA-ICU",
        fencing_token=101
    )
    rel_req = bed_pb2.ReleaseBedRequest(
        context=mut_ctx_rel,
        bed_id="bed-icu-01",
        allocation_id="alloc-grpc-1"
    )
    rel_resp = servicer.ReleaseBed(rel_req, ctx)
    assert rel_resp.success is True

def test_rest_api_endpoints():
    app = build_app(start_grpc=False, db_url="sqlite:///:memory:")
    client = TestClient(app)

    # Health check
    health_resp = client.get("/healthz")
    assert health_resp.status_code == 200

    # Ready check
    ready_resp = client.get("/readyz")
    assert ready_resp.status_code == 200

    # List beds for WARDA-ICU (auto-seeded)
    list_resp = client.get("/api/v1/beds?pool_id=WARDA-ICU")
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert data["count"] == 10

    # Inspect fencing token
    token_resp = client.get("/api/v1/pools/WARDA-ICU/fencing-token")
    assert token_resp.status_code == 200
    assert token_resp.json()["highest_fencing_token"] == 0

    # Allocate via REST
    alloc_payload = {
        "operation_id": "rest-op-1",
        "bed_id": "bed-warda-icu-01",
        "allocation_id": "alloc-rest-1",
        "fencing_token": 10,
        "pool_id": "WARDA-ICU"
    }
    alloc_resp = client.post("/api/v1/beds/bed-warda-icu-01/allocate", json=alloc_payload)
    assert alloc_resp.status_code == 200
    assert alloc_resp.json()["success"] is True

    # Stale token via REST -> 409 STALE_FENCING_TOKEN
    stale_payload = {
        "operation_id": "rest-op-stale",
        "bed_id": "bed-warda-icu-02",
        "allocation_id": "alloc-rest-2",
        "fencing_token": 5, # Stale!
        "pool_id": "WARDA-ICU"
    }
    stale_resp = client.post("/api/v1/beds/bed-warda-icu-02/allocate", json=stale_payload)
    assert stale_resp.status_code == 409
    assert stale_resp.json()["error"]["code"] == "STALE_FENCING_TOKEN"

def test_grpc_client_socket_integration(in_memory_service):
    """Verifies that GrpcBedRegistryClient can communicate with BedGrpcServicer over a real socket."""
    from services.bed_registry.grpc_server import create_grpc_server
    from services.bed_allocation.clients.grpc_clients import GrpcBedRegistryClient

    server = create_grpc_server(in_memory_service, port=50059)
    server.start()
    try:
        client = GrpcBedRegistryClient(target="127.0.0.1:50059", default_timeout_s=2.0)
        beds = client.list_available_beds("WARDA-ICU", request_id="socket-test")
        assert len(beds) == 2

        mut_ctx = common_pb2.MutationContext(
            operation_id="socket-op-1",
            request_id="socket-test",
            pool_id="WARDA-ICU",
            fencing_token=200
        )
        success, allocated_bed, err = client.allocate_bed(mut_ctx, "bed-icu-01", "alloc-socket-1")
        assert success is True
        assert allocated_bed.bed_id == "bed-icu-01"
        assert allocated_bed.status == "OCCUPIED"

        # Release
        mut_ctx_rel = common_pb2.MutationContext(
            operation_id="socket-op-2",
            request_id="socket-test",
            pool_id="WARDA-ICU",
            fencing_token=201
        )
        rel_success, rel_err = client.release_bed(mut_ctx_rel, "bed-icu-01", "alloc-socket-1")
        assert rel_success is True
    finally:
        server.stop(grace=None)

