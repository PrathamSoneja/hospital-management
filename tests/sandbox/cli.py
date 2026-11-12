"""
Local sandbox command line (TEST-ONLY). Used directly, by scripts/local/Sandbox.ps1, and by CI.

    python -m tests.sandbox.cli infra-up        # disposable Postgres, Firestore, Pub/Sub, Kafka (+Prometheus/Grafana)
    python -m tests.sandbox.cli verify          # full pytest suite against the sandbox; skips fail
    python -m tests.sandbox.cli up              # start deps + Triage + 2 allocators, seed, run until Ctrl+C
    python -m tests.sandbox.cli load            # reset -> seed -> readiness -> k6 -> audit -> evidence
    python -m tests.sandbox.cli infra-down      # remove only the hospital-sandbox Compose project

Everything binds to fixed local ports (see PORTS) and uses the disposable database
hospital_sandbox_load_test. Evidence is written to .sandbox/evidence/<timestamp>/ (ignored).
"""
import argparse
import base64
import datetime as dt
import json
import os
import platform
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path

from tests.sandbox.audit import audit
from tests.sandbox.runtime import REPO_ROOT, Sandbox, fixture_contention, fixture_multipool, http_json, wait_until

COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "tests" / "sandbox" / "compose.yaml")]
K6_IMAGE = "grafana/k6:0.54.0"
PORTS = {"deps_grpc": 47051, "deps_http": 47080, "triage_rest": 47101, "triage_grpc": 47151,
         "allocator_0": 47201, "allocator_1": 47202}
ENV = {  # sandbox Compose endpoints (tests/sandbox/compose.yaml)
    "TEST_POSTGRES_URL": "postgresql+psycopg2://postgres:sandbox-only@127.0.0.1:45432/hospital_allocation_unit_test",
    "FIRESTORE_EMULATOR_HOST": "localhost:48080",
    "PUBSUB_EMULATOR_HOST": "localhost:48085",
    "SANDBOX_KAFKA_BOOTSTRAP": "localhost:49092",
    "ZK_TEST_HOSTS": "localhost:42181",
}
PROMETHEUS = "http://localhost:49090"
GRAFANA = "http://localhost:43000"


def run(command, check=True, **kwargs):
    print("+", " ".join(command), flush=True)
    return subprocess.run(command, check=check, cwd=REPO_ROOT, **kwargs)


def infra_up(observability: bool = True):
    services = ["postgres", "firestore", "pubsub", "kafka", "zookeeper"] + (["prometheus", "grafana"] if observability else [])
    run(COMPOSE + ["up", "-d", *services])
    wait_until(lambda: run(COMPOSE + ["exec", "-T", "postgres", "pg_isready", "-U", "postgres"], check=False,
                           capture_output=True).returncode == 0, 120, what="PostgreSQL")
    wait_until(lambda: http_json("GET", "http://localhost:48080/")[0] == 200, 180, what="Firestore emulator")
    wait_until(lambda: http_json("GET", "http://localhost:48085/")[0] == 200, 180, what="Pub/Sub emulator")

    def kafka_ready():
        from confluent_kafka.admin import AdminClient
        return bool(AdminClient({"bootstrap.servers": ENV["SANDBOX_KAFKA_BOOTSTRAP"]}).list_topics(timeout=5).brokers)
    wait_until(kafka_ready, 180, interval=2, what="Kafka")

    def zookeeper_ready():
        from kazoo.client import KazooClient
        client = KazooClient(hosts=ENV["ZK_TEST_HOSTS"])
        try:
            client.start(timeout=5)
            return True
        finally:
            client.stop()
    wait_until(zookeeper_ready, 120, interval=2, what="ZooKeeper")
    # The unit/PostgreSQL tests use temporary schemas inside this disposable database.
    run(COMPOSE + ["exec", "-T", "postgres", "sh", "-c",
                   "psql -U postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='hospital_allocation_unit_test'\" "
                   "| grep -q 1 || createdb -U postgres hospital_allocation_unit_test"])
    print("sandbox infrastructure ready:", json.dumps(ENV))


def infra_down():
    run(COMPOSE + ["down", "--remove-orphans"])  # project-scoped; tmpfs storage only, no shared volumes


def sandbox_env():
    os.environ.update(ENV)


def new_sandbox(log_dir: Path) -> Sandbox:
    sandbox_env()
    return Sandbox(postgres_url=ENV["TEST_POSTGRES_URL"], database="hospital_sandbox_load_test",
                   firestore_host=ENV["FIRESTORE_EMULATOR_HOST"], pubsub_host=ENV["PUBSUB_EMULATOR_HOST"],
                   kafka_bootstrap=ENV["SANDBOX_KAFKA_BOOTSTRAP"], log_dir=log_dir, allocators=2,
                   bind_host="0.0.0.0", ports=dict(PORTS), recovery_claim_seconds=15)


def verify(extra_args):
    sandbox_env()
    env = {**os.environ, "REQUIRE_INTEGRATION": "1"}
    junit = REPO_ROOT / ".sandbox" / "pytest" / "junit.xml"
    junit.parent.mkdir(parents=True, exist_ok=True)
    return run([sys.executable, "-m", "pytest", "-p", "no:cacheprovider", "-q", f"--junitxml={junit}", *extra_args],
               check=False, env=env).returncode


def up(fixture: str):
    sandbox = new_sandbox(REPO_ROOT / ".sandbox" / "interactive")
    sandbox.start()
    try:
        sandbox.seed(fixture_contention() if fixture == "contention" else fixture_multipool(admissions_per_pool=200))
        print(json.dumps({"allocators": [sandbox.allocator_url(i) for i in range(2)], "triage": sandbox.triage_url,
                          "dependencies_admin": f"{sandbox.deps_http}/admin/state", **sandbox.describe()}, indent=2))
        print("Sandbox running; press Ctrl+C to stop the processes (infrastructure keeps running).", flush=True)
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        sandbox.stop()


def _k6(script: str, env: dict, summary: Path, log: Path) -> int:
    args = ["docker", "run", "--rm", "--add-host=host.docker.internal:host-gateway",
            "-v", f"{REPO_ROOT / 'k6'}:/scripts:ro", "-v", f"{summary.parent}:/out"]
    for key, value in env.items():
        args += ["-e", f"{key}={value}"]
    args += [K6_IMAGE, "run", "--quiet", f"--summary-export=/out/{summary.name}", f"/scripts/{script}"]
    with open(log, "w", encoding="utf-8") as handle:
        return run(args, check=False, stdout=handle, stderr=subprocess.STDOUT).returncode


def _settle(sandbox: Sandbox, pools, timeout=180):
    def settled():
        report = audit(sandbox.database_url, sandbox.dependency_state(), pools=pools)
        return report["unsettled"] == 0 and report["outbox_undelivered"] == 0
    try:
        wait_until(settled, timeout, interval=2, what="allocations settled and outbox drained")
    except TimeoutError:
        pass  # the audit below records what did not settle
    return audit(sandbox.database_url, sandbox.dependency_state(), pools=pools)


def _observability(sandbox: Sandbox, out: Path) -> dict:
    auth = {"Authorization": "Basic " + base64.b64encode(b"admin:sandbox-only").decode()}
    result = {}
    status, targets, _ = http_json("GET", f"{PROMETHEUS}/api/v1/targets")
    result["prometheus_targets"] = [{"job": t["labels"]["job"], "instance": t["labels"]["instance"], "health": t["health"]}
                                    for t in (targets or {}).get("data", {}).get("activeTargets", [])]
    rules = http_json("GET", f"{PROMETHEUS}/api/v1/rules")[1] or {}
    result["prometheus_rules"] = [r["name"] for g in rules.get("data", {}).get("groups", []) for r in g["rules"]]
    dashboard = json.loads((REPO_ROOT / "monitoring" / "dashboards" / "hospital_overview.json").read_text())
    queries = {}
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            expr = target["expr"]
            body = http_json("GET", f"{PROMETHEUS}/api/v1/query?query={urllib.parse.quote(expr)}")[1] or {}
            queries[expr] = {"panel": panel["title"], "series": len(body.get("data", {}).get("result", []))}
    result["dashboard_queries"] = queries
    endpoints = http_json("GET", f"{PROMETHEUS}/api/v1/label/endpoint/values")[1] or {}
    result["endpoint_label_values"] = endpoints.get("data", [])
    result["grafana_health"] = http_json("GET", f"{GRAFANA}/api/health")[0]
    result["grafana_datasources"] = [d["uid"] for d in (http_json("GET", f"{GRAFANA}/api/datasources", headers=auth)[1] or [])]
    result["grafana_dashboards"] = [d["title"] for d in (http_json("GET", f"{GRAFANA}/api/search?type=dash-db", headers=auth)[1] or [])]
    proxied = http_json("GET", f"{GRAFANA}/api/datasources/proxy/uid/prometheus/api/v1/query?query=up", headers=auth)
    result["grafana_to_prometheus_up_series"] = len(((proxied[1] or {}).get("data") or {}).get("result", []))
    # Structured logs: JSON lines carrying the k6 request IDs.
    correlated = 0
    for log in sandbox.log_dir.glob("*.log"):
        for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("{") and '"request_id": "k6-' in line:
                correlated += 1
    result["json_log_lines_with_k6_request_id"] = correlated
    (out / "observability.json").write_text(json.dumps(result, indent=2))
    return result


def load(sustained_duration: str, vus: int) -> int:
    started = dt.datetime.now(dt.timezone.utc)
    out = REPO_ROOT / ".sandbox" / "evidence" / started.strftime("%Y%m%dT%H%M%SZ")
    out.mkdir(parents=True, exist_ok=True)
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True).stdout.strip()
    dirty = bool(subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=REPO_ROOT,
                                capture_output=True, text=True).stdout.strip())
    sandbox = new_sandbox(out / "logs")
    manifest = {"started_at": started.isoformat(), "revision": revision, "tree_dirty": dirty,
                "command": " ".join(sys.argv), "platform": platform.platform(), "k6_image": K6_IMAGE, "steps": []}
    failures = []
    targets = ",".join(f"http://host.docker.internal:{PORTS[f'allocator_{i}']}" for i in range(2))
    try:
        sandbox.start(reset=True)
        manifest["sandbox"] = sandbox.describe()

        contention = fixture_contention(pool_id="POOL-K6", admissions=50, beds=10, staff=100)
        sandbox.seed(contention)
        manifest["steps"].append({"step": "contention", "fixture": "POOL-K6: 50 OPEN admissions, 10 beds, staff 100, no releases"})
        code = _k6("contention.js", {"BASE_URLS": targets, "POOL_ID": "POOL-K6", "ADMISSIONS": 50,
                                     "EXPECT_ALLOCATED": 10, "ADMISSION_PREFIX": "adm-k6",
                                     "P95_THRESHOLD_MS": 8000},  # 5 s lock wait + 2 s RPC deadline + margin
                   out / "k6-contention-summary.json", out / "k6-contention.log")
        report = _settle(sandbox, ["POOL-K6"])
        (out / "audit-contention.json").write_text(json.dumps(report, indent=2, default=str))
        expected = {"ALLOCATED": 10, "WAITLISTED": 40}
        if code or report["violations"] or report["statuses"] != expected or report["distinct_allocated_beds"] != 10:
            failures.append(f"contention: k6_exit={code} statuses={report['statuses']} violations={report['violations']}")
        manifest["steps"][-1].update(k6_exit=code, statuses=report["statuses"], violations=report["violations"])

        pools = ["POOL-A", "POOL-B", "POOL-C"]
        sandbox.seed(fixture_multipool(pools=pools, admissions_per_pool=2000, beds=20))
        manifest["steps"].append({"step": "sustained", "fixture": "3 pools x 2000 admissions, 20 beds each; k6 PUTs triage",
                                  "duration": sustained_duration, "vus": vus})
        code = _k6("sustained-multi-pool.js", {"ALLOCATION_URLS": targets, "TRIAGE_URL": "http://host.docker.internal:47101",
                                               "POOL_IDS": ",".join(pools), "ADMISSIONS_PER_POOL": 2000, "VUS": vus,
                                               "RAMP": "15s", "DURATION": sustained_duration,
                                               "P95_THRESHOLD_MS": 8000},  # local emulators; staging target stays 3000
                   out / "k6-sustained-summary.json", out / "k6-sustained.log")
        report = _settle(sandbox, pools)
        (out / "audit-sustained.json").write_text(json.dumps(report, indent=2, default=str))
        if code or report["violations"]:
            failures.append(f"sustained: k6_exit={code} violations={report['violations']}")
        manifest["steps"][-1].update(k6_exit=code, statuses=report["statuses"], violations=report["violations"],
                                     outbox_streams=len(report["outbox_streams"]), p95_threshold_ms=8000)
        time.sleep(12)  # at least two Prometheus scrapes after the run
        manifest["observability"] = _observability(sandbox, out)
        (out / "dependencies-state-summary.json").write_text(json.dumps(
            {k: v for k, v in sandbox.dependency_state().items() if k in ("calls", "fence_rejections", "lock_acquisitions")}, indent=2))
    except Exception as error:
        failures.append(f"procedure error: {error!r}")
    finally:
        sandbox.stop()
        manifest["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        manifest["failures"] = failures
        manifest["limitations"] = [
            "Lock/Bed/Staff/Patient are shared test stand-ins (tests/support/sandbox_dependencies.py), not teammate services.",
            "Single machine, two allocator processes: not GKE, HPA, or distributed-lock evidence.",
            "Kafka and Pub/Sub are local emulators/containers, not the production brokers.",
        ]
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str))
        print(f"evidence: {out}")
        print("FAILURES:" if failures else "load procedure passed", *failures, sep="\n")
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    up_infra = sub.add_parser("infra-up")
    up_infra.add_argument("--no-observability", action="store_true")
    sub.add_parser("infra-down")
    sub.add_parser("env", help="print the sandbox environment variables")
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("pytest_args", nargs="*")
    up_parser = sub.add_parser("up")
    up_parser.add_argument("--fixture", choices=["contention", "multipool"], default="contention")
    load_parser = sub.add_parser("load")
    load_parser.add_argument("--duration", default="2m")
    load_parser.add_argument("--vus", type=int, default=20)
    args = parser.parse_args()
    if args.command == "infra-up":
        infra_up(not args.no_observability)
    elif args.command == "infra-down":
        infra_down()
    elif args.command == "env":
        print(json.dumps(ENV, indent=2))
    elif args.command == "verify":
        sys.exit(verify(args.pytest_args))
    elif args.command == "up":
        up(args.fixture)
    elif args.command == "load":
        sys.exit(load(args.duration, args.vus))


if __name__ == "__main__":
    main()
