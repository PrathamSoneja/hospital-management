import pytest

from shared.config.runtime import ConfigError, bounded_int, runtime_profile, validate_cloud_runtime

ENDPOINTS = ["LOCK_SERVICE_ADDR", "PATIENT_SERVICE_URL"]
CLOUD_ENV = {
    "ENVIRONMENT": "production",
    "LOCK_SERVICE_ADDR": "lock-coordinator-service:50051",
    "PATIENT_SERVICE_URL": "http://patient-registry-service:8000/api/v1",
    # A Cloud SQL Auth Proxy sidecar listens on loopback; the database URL is not an endpoint check.
    "DATABASE_URL": "postgresql+psycopg2://app@127.0.0.1:5432/allocations",
}


def test_cloud_profile_accepts_cluster_endpoints():
    assert validate_cloud_runtime(ENDPOINTS, CLOUD_ENV) == "production"


@pytest.mark.parametrize("override, fragment", [
    ({"FIRESTORE_EMULATOR_HOST": "localhost:8080"}, "FIRESTORE_EMULATOR_HOST"),
    ({"PUBSUB_EMULATOR_HOST": "localhost:8085"}, "PUBSUB_EMULATOR_HOST"),
    ({"USE_FAKES": "true"}, "USE_FAKES"),
    ({"GOOGLE_APPLICATION_CREDENTIALS": "/keys/sa.json"}, "GOOGLE_APPLICATION_CREDENTIALS"),
    ({"DATABASE_URL": "sqlite:///x.db"}, "DATABASE_URL"),
    ({"LOCK_SERVICE_ADDR": "localhost:50051"}, "LOCK_SERVICE_ADDR"),
    ({"LOCK_SERVICE_ADDR": "dns:///127.0.0.1:50051"}, "LOCK_SERVICE_ADDR"),
    ({"PATIENT_SERVICE_URL": "http://host.docker.internal:8000"}, "PATIENT_SERVICE_URL"),
    ({"LOCK_SERVICE_ADDR": ""}, "LOCK_SERVICE_ADDR (required)"),
])
def test_cloud_profile_rejects_local_only_settings(override, fragment):
    with pytest.raises(ConfigError, match=fragment.replace("(", r"\(").replace(")", r"\)")) as error:
        validate_cloud_runtime(ENDPOINTS, {**CLOUD_ENV, **override})
    # Values (which may embed credentials) never appear in the message.
    assert "sa.json" not in str(error.value) and "127.0.0.1" not in str(error.value)


def test_local_profiles_skip_cloud_rules_and_unknown_profiles_fail():
    assert validate_cloud_runtime(ENDPOINTS, {"ENVIRONMENT": "local", "FIRESTORE_EMULATOR_HOST": "localhost:8080"}) == "local"
    with pytest.raises(ConfigError, match="ENVIRONMENT"):
        runtime_profile({"ENVIRONMENT": "dev"})


def test_bounded_int_validates_range():
    assert bounded_int("RPC_TIMEOUT_MS", 2000, 100, 30000, {}) == 2000
    with pytest.raises(ConfigError, match="between"):
        bounded_int("RPC_TIMEOUT_MS", 2000, 100, 30000, {"RPC_TIMEOUT_MS": "0"})
    with pytest.raises(ConfigError, match="integer"):
        bounded_int("RPC_TIMEOUT_MS", 2000, 100, 30000, {"RPC_TIMEOUT_MS": "fast"})


def test_allocation_rejects_injected_adapters_in_cloud_profile(monkeypatch):
    from services.bed_allocation.main import build_app
    for name in ("FIRESTORE_EMULATOR_HOST", "PUBSUB_EMULATOR_HOST", "GOOGLE_APPLICATION_CREDENTIALS", "USE_FAKES"):
        monkeypatch.delenv(name, raising=False)
    for name, value in CLOUD_ENV.items():
        monkeypatch.setenv(name, value)
    for name in ("BED_SERVICE_ADDR", "STAFF_SERVICE_ADDR", "TRIAGE_SERVICE_ADDR"):
        monkeypatch.setenv(name, f"{name.lower().replace('_', '-')}:50051")
    monkeypatch.setenv("ENABLE_OUTBOX_WORKER", "false")
    with pytest.raises(ConfigError, match="Injected"):
        build_app("postgresql+psycopg2://app@127.0.0.1:5432/allocations",
                  clients={k: object() for k in ("lock", "bed", "staff", "triage", "patient")})
