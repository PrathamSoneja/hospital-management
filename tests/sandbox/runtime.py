"""
Process-level sandbox: real Allocation and Triage entrypoints wired over real gRPC/HTTP to
shared test dependencies, disposable PostgreSQL, the Firestore emulator, and optional local
Kafka + Pub/Sub emulator. TEST-ONLY; production code never imports tests/.

Safety rules enforced here:
  * the database name must end in `_test`; only that database is dropped/recreated;
  * only processes this sandbox started (tracked handles or recorded PIDs) are stopped;
  * Google ADC fallback is disabled for every child (no key file, empty gcloud config,
    unreachable metadata server), so emulator settings can never silently become cloud calls.
"""
import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import create_engine, text

REPO_ROOT = Path(__file__).resolve().parents[2]
LOOPBACK = {"localhost", "127.0.0.1", "::1"}


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_json(method: str, url: str, body=None, headers=None, timeout: float = 5.0):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            try:
                payload = json.loads(raw) if raw else None
            except ValueError:  # emulator readiness endpoints answer with plain text
                payload = raw.decode(errors="replace")
            return response.status, payload, dict(response.headers)
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            payload = raw.decode(errors="replace")
        return error.code, payload, dict(error.headers)


def wait_until(predicate, timeout: float, interval: float = 0.25, what: str = "condition"):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as error:  # readiness probes raise while a process boots
            last_error = error
        time.sleep(interval)
    raise TimeoutError(f"timed out after {timeout}s waiting for {what} (last error: {last_error!r})")


def isolated_env(log_dir: Path, **overrides) -> dict:
    env = {k: v for k, v in os.environ.items() if k not in {"GOOGLE_APPLICATION_CREDENTIALS", "USE_FAKES"}}
    empty_gcloud = log_dir / "gcloud-config-empty"
    empty_gcloud.mkdir(parents=True, exist_ok=True)
    env.update({
        "CLOUDSDK_CONFIG": str(empty_gcloud),       # no user ADC file is visible
        "GCE_METADATA_HOST": "127.0.0.1:9",          # no metadata-server credentials
        "GCE_METADATA_IP": "127.0.0.1:9",
        "NO_GCE_CHECK": "True",
        "PYTHONPATH": str(REPO_ROOT),
        "PYTHONUNBUFFERED": "1",
        "ENVIRONMENT": "local",
    })
    env.update({k: str(v) for k, v in overrides.items() if v is not None})
    return env


def assert_disposable_database_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.hostname not in LOOPBACK:
        raise ValueError("sandbox databases must be on local PostgreSQL")
    name = parts.path.lstrip("/")
    if not name.endswith("_test"):
        raise ValueError(f"database '{name}' is not a disposable *_test database")
    return name


def with_database(url: str, database: str) -> str:
    parts = urlsplit(url)
    return urlunsplit(parts._replace(path=f"/{database}"))


# Local mirrors of the Cloud SQL users created by Terraform (modules/cloudsql): migrations run as the
# schema-owning migrator, the service runs as the DML-only app user. Passwords are sandbox-only.
MIGRATOR_ROLE, APP_ROLE, SANDBOX_ROLE_PASSWORD = "allocation_migrator", "allocation_app", "sandbox-only"


def with_role(url: str, role: str) -> str:
    parts = urlsplit(url)
    host = parts.netloc.rsplit("@", 1)[-1]
    return urlunsplit(parts._replace(netloc=f"{role}:{SANDBOX_ROLE_PASSWORD}@{host}"))


def prepare_allocation_database(admin_url: str, database: str) -> tuple[str, str]:
    """Drop/recreate a disposable *_test database the way the runbook prepares Cloud SQL.

    Ensures the two login roles exist on the local server, recreates the database owned by the
    admin, and grants only USAGE, CREATE on schema public to the migrator. Returns
    (migrator_url, app_url); grants for the app user come from migration 20261031_04.
    """
    assert_disposable_database_url(with_database(admin_url, database))
    admin = create_engine(with_database(admin_url, "postgres"), isolation_level="AUTOCOMMIT")
    try:
        with admin.connect() as connection:
            for role in (MIGRATOR_ROLE, APP_ROLE):
                if not connection.execute(text("SELECT 1 FROM pg_roles WHERE rolname = :r"), {"r": role}).scalar():
                    connection.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD '{SANDBOX_ROLE_PASSWORD}'"))
            connection.execute(text("SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                                    "WHERE datname = :db AND pid <> pg_backend_pid()"), {"db": database})
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database}"'))
            connection.execute(text(f'CREATE DATABASE "{database}"'))
    finally:
        admin.dispose()
    owner = create_engine(with_database(admin_url, database), isolation_level="AUTOCOMMIT")
    try:
        with owner.connect() as connection:
            connection.execute(text(f"GRANT USAGE, CREATE ON SCHEMA public TO {MIGRATOR_ROLE}"))
    finally:
        owner.dispose()
    target = with_database(admin_url, database)
    return with_role(target, MIGRATOR_ROLE), with_role(target, APP_ROLE)


@dataclass
class ManagedProcess:
    name: str
    args: list
    env: dict
    log_path: Path
    ready_url: str
    process: subprocess.Popen | None = None

    def start(self, timeout: float = 60.0):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        log = open(self.log_path, "ab")
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        self.process = subprocess.Popen(self.args, cwd=REPO_ROOT, env=self.env, stdout=log, stderr=subprocess.STDOUT,
                                        creationflags=flags, start_new_session=sys.platform != "win32")
        log.close()

        def ready():
            if self.process.poll() is not None:
                raise RuntimeError(f"{self.name} exited with {self.process.returncode}; see {self.log_path}")
            return http_json("GET", self.ready_url, timeout=2)[0] == 200
        wait_until(ready, timeout, what=f"{self.name} readiness at {self.ready_url}")
        return self

    def stop(self, graceful: bool = True, timeout: float = 15.0):
        if not self.process or self.process.poll() is not None:
            return
        if graceful:
            if sys.platform == "win32":
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout)
                return
            except subprocess.TimeoutExpired:
                pass
        self.process.kill()   # hard kill: used to simulate a crashed allocator
        self.process.wait(timeout)

    @property
    def pid(self):
        return self.process.pid if self.process else None


@dataclass
class Sandbox:
    postgres_url: str                 # any database URL on the local server; the database part is replaced
    database: str                     # must end in _test; dropped and recreated by reset_database()
    firestore_host: str
    log_dir: Path
    firestore_project: str = field(default_factory=lambda: f"sandbox-{uuid.uuid4().hex[:8]}")
    pubsub_host: str | None = None
    kafka_bootstrap: str | None = None
    allocators: int = 2
    bind_host: str = "127.0.0.1"
    ports: dict = field(default_factory=dict)
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    outbox_overrides: dict = field(default_factory=dict)
    recovery_claim_seconds: int = 60

    def __post_init__(self):
        if not self.database.endswith("_test"):
            raise ValueError("sandbox database must end in _test")
        self.log_dir = Path(self.log_dir)
        # Processes and audits use the restricted runtime role; migrations use the migrator role.
        self.database_url = with_role(with_database(self.postgres_url, self.database), APP_ROLE)
        self.migrator_url = with_role(with_database(self.postgres_url, self.database), MIGRATOR_ROLE)
        assert_disposable_database_url(self.database_url)
        for key in ("deps_grpc", "deps_http", "triage_rest", "triage_grpc"):
            self.ports.setdefault(key, free_port())
        for index in range(self.allocators):
            self.ports.setdefault(f"allocator_{index}", free_port())
        self.topics = {"kafka": f"bed-events-{self.run_id}", "pubsub": f"bed-notifications-{self.run_id}"}
        self.processes: dict[str, ManagedProcess] = {}

    # ---------- URLs ----------
    @property
    def deps_http(self):
        return f"http://127.0.0.1:{self.ports['deps_http']}"

    @property
    def triage_url(self):
        return f"http://127.0.0.1:{self.ports['triage_rest']}"

    def allocator_url(self, index: int = 0):
        return f"http://127.0.0.1:{self.ports[f'allocator_{index}']}"

    @property
    def events_enabled(self) -> bool:
        return bool(self.kafka_bootstrap and self.pubsub_host)

    # ---------- storage ----------
    def reset_database(self):
        prepare_allocation_database(self.postgres_url, self.database)
        env = isolated_env(self.log_dir, DATABASE_URL=self.migrator_url)
        result = subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], cwd=REPO_ROOT, env=env,
                                capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}")

    def reset_firestore(self):
        url = (f"http://{self.firestore_host}/emulator/v1/projects/{self.firestore_project}"
               "/databases/(default)/documents")
        status, _, _ = http_json("DELETE", url, timeout=10)
        if status not in (200, 204):
            raise RuntimeError(f"Firestore emulator reset failed with {status}")

    def ensure_brokers(self):
        if not self.events_enabled:
            return
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": self.kafka_bootstrap})
        futures_by_topic = admin.create_topics([NewTopic(self.topics["kafka"], num_partitions=3, replication_factor=1)])
        for future in futures_by_topic.values():
            try:
                future.result(30)
            except Exception as error:
                if "TOPIC_ALREADY_EXISTS" not in str(error):
                    raise
        os.environ["PUBSUB_EMULATOR_HOST"] = self.pubsub_host
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(self.firestore_project, self.topics["pubsub"])
        try:
            publisher.create_topic(name=topic_path)
        except Exception as error:
            if "AlreadyExists" not in type(error).__name__:
                raise
        subscriber = pubsub_v1.SubscriberClient()
        subscription = subscriber.subscription_path(self.firestore_project, f"{self.topics['pubsub']}-audit")
        try:
            subscriber.create_subscription(name=subscription, topic=topic_path, ack_deadline_seconds=30)
        except Exception as error:
            if "AlreadyExists" not in type(error).__name__:
                raise
        self.pubsub_subscription = subscription

    # ---------- processes ----------
    def _deps(self):
        return ManagedProcess(
            "dependencies",
            [sys.executable, "-m", "tests.support.sandbox_dependencies", "--grpc-port", str(self.ports["deps_grpc"]),
             "--http-port", str(self.ports["deps_http"]), "--host", self.bind_host],
            isolated_env(self.log_dir), self.log_dir / "dependencies.log", f"{self.deps_http}/healthz")

    def _triage(self):
        env = isolated_env(
            self.log_dir, TRIAGE_STORAGE_BACKEND="firestore", TRIAGE_FENCING_MODE="coordinator",
            FIRESTORE_PROJECT_ID=self.firestore_project, FIRESTORE_EMULATOR_HOST=self.firestore_host,
            LOCK_COORDINATOR_ADDR=f"127.0.0.1:{self.ports['deps_grpc']}", START_GRPC="true",
            TRIAGE_GRPC_PORT=self.ports["triage_grpc"], TRIAGE_REST_PORT=self.ports["triage_rest"],
            TRIAGE_REST_HOST=self.bind_host, TRIAGE_INSTANCE_ID="sandbox-triage-0")
        return ManagedProcess("triage", [sys.executable, "-m", "services.triage.main"], env,
                              self.log_dir / "triage.log", f"{self.triage_url}/readyz")

    def allocator_env(self, index: int) -> dict:
        deps = f"127.0.0.1:{self.ports['deps_grpc']}"
        env = dict(
            DATABASE_URL=self.database_url, ALLOCATION_REST_HOST=self.bind_host,
            ALLOCATION_REST_PORT=self.ports[f"allocator_{index}"], POD_NAME=f"sandbox-allocator-{index}",
            LOCK_SERVICE_ADDR=deps, BED_SERVICE_ADDR=deps, STAFF_SERVICE_ADDR=deps,
            TRIAGE_SERVICE_ADDR=f"127.0.0.1:{self.ports['triage_grpc']}",
            PATIENT_SERVICE_URL=f"{self.deps_http}/api/v1",
            ENABLE_RECOVERY_WORKER="true", RECOVERY_POLL_SECONDS="1", RECOVERY_CLAIM_SECONDS=self.recovery_claim_seconds,
            ENABLE_OUTBOX_WORKER="false",
        )
        if self.events_enabled:
            env.update(
                ENABLE_OUTBOX_WORKER="true", PUBSUB_EMULATOR_HOST=self.pubsub_host,
                PUBSUB_PROJECT_ID=self.firestore_project, PUBSUB_TOPIC_NOTIFICATIONS=self.topics["pubsub"],
                KAFKA_BOOTSTRAP_SERVERS=self.kafka_bootstrap, KAFKA_SECURITY_PROTOCOL="PLAINTEXT",
                KAFKA_TOPIC_BED_EVENTS=self.topics["kafka"], OUTBOX_POLL_SECONDS="0.2",
                OUTBOX_PUBLISH_TIMEOUT_SECONDS="5", OUTBOX_MAX_BACKOFF_SECONDS="4", OUTBOX_CLAIM_SECONDS="10",
            )
            env.update(self.outbox_overrides)
        return env

    def _allocator(self, index: int):
        return ManagedProcess(f"allocator-{index}", [sys.executable, "-m", "services.bed_allocation.main"],
                              isolated_env(self.log_dir, **self.allocator_env(index)),
                              self.log_dir / f"allocator-{index}.log", f"{self.allocator_url(index)}/readyz")

    def start(self, reset: bool = True):
        self.log_dir.mkdir(parents=True, exist_ok=True)
        if reset:
            self.reset_database()
            self.reset_firestore()
        self.ensure_brokers()
        self.processes["dependencies"] = self._deps().start()
        self.processes["triage"] = self._triage().start()
        for index in range(self.allocators):
            self.processes[f"allocator-{index}"] = self._allocator(index).start()
        return self

    def restart_allocator(self, index: int, graceful: bool = False):
        name = f"allocator-{index}"
        self.processes[name].stop(graceful=graceful)
        self.processes[name] = self._allocator(index).start()

    def stop(self):
        for name in reversed(list(self.processes)):
            self.processes[name].stop()

    def pids(self) -> dict:
        return {name: process.pid for name, process in self.processes.items()}

    # ---------- fixtures ----------
    def seed(self, fixture: dict):
        """Seed shared dependencies and Triage assessments from a deterministic fictional fixture."""
        status, body, _ = http_json("POST", f"{self.deps_http}/admin/seed", fixture)
        if status != 200:
            raise RuntimeError(f"dependency seed failed: {status} {body}")
        for item in fixture.get("triage", []):
            status, body, _ = http_json(
                "PUT", f"{self.triage_url}/api/v1/triage/{item['admission_id']}",
                {"triage_level": item["triage_level"], "pool_id": item["pool_id"]},
                headers={"Idempotency-Key": f"seed-{self.run_id}-{item['admission_id']}"}, timeout=10)
            if status != 200:
                raise RuntimeError(f"triage seed failed for an admission: {status} {body}")

    def fault(self, rpc: str, mode: str, count: int = 1, delay_seconds: float | None = None):
        payload = {"rpc": rpc, "mode": mode, "count": count}
        if delay_seconds is not None:
            payload["delay_seconds"] = delay_seconds
        status, body, _ = http_json("POST", f"{self.deps_http}/admin/faults", payload)
        if status != 200:
            raise RuntimeError(f"fault injection failed: {body}")

    def dependency_state(self) -> dict:
        return http_json("GET", f"{self.deps_http}/admin/state")[1]

    def describe(self) -> dict:
        return {"run_id": self.run_id, "database": self.database, "firestore_project": self.firestore_project,
                "ports": self.ports, "topics": self.topics if self.events_enabled else None,
                "pids": self.pids(), "log_dir": str(self.log_dir)}


def fixture_contention(pool_id: str = "POOL-K6", admissions: int = 50, beds: int = 10, staff: int = 100,
                       prefix: str = "adm-k6") -> dict:
    """Deterministic fictional contention fixture: N eligible admissions, M free beds, ample staff."""
    ids = [f"{prefix}-{n}" for n in range(1, admissions + 1)]
    return {
        "beds": [{"bed_id": f"bed-{pool_id.lower()}-{n:02d}", "pool_id": pool_id, "ward": f"WARD-{pool_id}"}
                 for n in range(1, beds + 1)],
        "staff_capacity": {pool_id: staff},
        "admissions": [{"admission_id": a, "patient_id": f"pat-{a[4:]}", "status": "OPEN"} for a in ids],
        "triage": [{"admission_id": a, "triage_level": 1 + (n % 5), "pool_id": pool_id} for n, a in enumerate(ids)],
    }


def fixture_multipool(pools=("POOL-A", "POOL-B", "POOL-C"), admissions_per_pool: int = 2000, beds: int = 20,
                      prefix: str = "adm-load") -> dict:
    """Admissions exist in the Patient stand-in; k6 performs the Triage PUTs itself."""
    data = {"beds": [], "staff_capacity": {}, "admissions": [], "triage": []}
    for pool in pools:
        data["beds"] += [{"bed_id": f"bed-{pool.lower()}-{n:02d}", "pool_id": pool, "ward": f"WARD-{pool}"}
                         for n in range(1, beds + 1)]
        data["staff_capacity"][pool] = admissions_per_pool + beds
        data["admissions"] += [{"admission_id": f"{prefix}-{pool}-{n}", "patient_id": f"pat-{pool.lower()}-{n}",
                                "status": "OPEN"} for n in range(1, admissions_per_pool + 1)]
    return data
