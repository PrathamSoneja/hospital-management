"""
Smoke-check a built service image locally. Never contacts cloud services.

    python scripts/smoke_image.py <image> [--service allocation|triage] [--database-url URL] [--firestore HOST:PORT]

allocation: needs a disposable, already-migrated database. Expects /healthz 200, storage-aware
            /readyz 200, a DB-backed 404, then a graceful SIGTERM shutdown (exit 0).
triage:     runs against the Firestore emulator with an unreachable Coordinator. Expects /healthz 200
            and /readyz 503 (fail-closed without its lock dependency), then a graceful shutdown.

Teammate endpoints are deliberately unreachable (gRPC channels connect lazily). Only the
container this script started is removed.
"""
import argparse
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from urllib.parse import urlsplit, urlunsplit

DOCKER_HOST_ALIAS = "host.docker.internal"


def _container_address(value: str) -> str:
    """Rewrite localhost to the Docker host alias (works on Linux via --add-host=host-gateway)."""
    if "://" in value:
        parts = urlsplit(value)
        if parts.hostname in {"localhost", "127.0.0.1"}:
            return urlunsplit(parts._replace(netloc=parts.netloc.replace(parts.hostname, DOCKER_HOST_ALIAS, 1)))
        return value
    host, _, port = value.rpartition(":")
    return f"{DOCKER_HOST_ALIAS}:{port}" if host in {"localhost", "127.0.0.1"} else value


def _get(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("--service", choices=["allocation", "triage"], default="allocation")
    parser.add_argument("--database-url", default=os.environ.get("SMOKE_DATABASE_URL") or os.environ.get("TEST_POSTGRES_URL"))
    parser.add_argument("--firestore", default=os.environ.get("FIRESTORE_EMULATOR_HOST"))
    parser.add_argument("--port", type=int, default=18002)
    args = parser.parse_args()

    if args.service == "allocation":
        if not args.database_url or urlsplit(args.database_url).hostname not in {"localhost", "127.0.0.1"}:
            print("Allocation smoke needs --database-url for a local disposable migrated database", file=sys.stderr)
            return 2
        env = {
            "ALLOCATION_REST_PORT": "8000", "DATABASE_URL": _container_address(args.database_url),
            "LOCK_SERVICE_ADDR": "lock-unavailable.invalid:50051", "BED_SERVICE_ADDR": "bed-unavailable.invalid:50051",
            "STAFF_SERVICE_ADDR": "staff-unavailable.invalid:50051",
            "TRIAGE_SERVICE_ADDR": "triage-unavailable.invalid:50051",
            "PATIENT_SERVICE_URL": "http://patient-unavailable.invalid/api/v1",
        }
        expected = {"healthz": 200, "readyz": 200, "missing_allocation": 404}
    else:
        if not args.firestore:
            print("Triage smoke needs --firestore pointing at a local Firestore emulator", file=sys.stderr)
            return 2
        env = {
            "TRIAGE_REST_PORT": "8000", "START_GRPC": "true", "TRIAGE_GRPC_PORT": "50051",
            "TRIAGE_STORAGE_BACKEND": "firestore", "TRIAGE_FENCING_MODE": "coordinator",
            "FIRESTORE_PROJECT_ID": f"smoke-{uuid.uuid4().hex[:8]}", "FIRESTORE_EMULATOR_HOST": _container_address(args.firestore),
            "LOCK_COORDINATOR_ADDR": "lock-unavailable.invalid:50051",
        }
        expected = {"healthz": 200, "readyz": 503}
    env["ENVIRONMENT"] = "local"

    name = f"{args.service}-smoke-{uuid.uuid4().hex[:8]}"
    command = ["docker", "run", "-d", "--name", name, f"--add-host={DOCKER_HOST_ALIAS}:host-gateway",
               "-p", f"{args.port}:8000"]
    for key, value in env.items():
        command += ["-e", f"{key}={value}"]
    subprocess.run(command + [args.image], check=True, stdout=subprocess.DEVNULL)
    base = f"http://localhost:{args.port}"
    try:
        observed = {"healthz": None}
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                observed["healthz"] = _get(f"{base}/healthz")
                if observed["healthz"] == 200:
                    break
            except OSError:
                pass
            time.sleep(1)
        time.sleep(1)
        observed["readyz"] = _get(f"{base}/readyz")
        if args.service == "allocation":
            observed["missing_allocation"] = _get(f"{base}/api/v1/allocations/smoke-missing-allocation")
        # Graceful shutdown: SIGTERM, lifespan cleanup, exit 0 well inside the 30 s pod grace period.
        started = time.time()
        subprocess.run(["docker", "stop", "-t", "25", name], check=True, stdout=subprocess.DEVNULL)
        observed["stop_seconds"] = round(time.time() - started, 1)
        observed["exit_code"] = int(subprocess.run(["docker", "inspect", "-f", "{{.State.ExitCode}}", name],
                                                   capture_output=True, text=True).stdout.strip())
        logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
        observed["shutdown_logged"] = "Application shutdown complete" in (logs.stdout + logs.stderr)
        print(" ".join(f"{k}={v}" for k, v in observed.items()))
        ok = all(observed[k] == v for k, v in expected.items())
        ok = ok and observed["exit_code"] == 0 and observed["shutdown_logged"] and observed["stop_seconds"] < 25
        return 0 if ok else 1
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


if __name__ == "__main__":
    sys.exit(main())
