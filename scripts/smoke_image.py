"""
Smoke-check a built Allocation image against a disposable, already-migrated database.

    python scripts/smoke_image.py <image> [--database-url URL] [--port 18002]

Starts the container in the local profile with deliberately unreachable teammate
endpoints (gRPC channels connect lazily), checks /healthz, storage-aware /readyz, and a DB-backed 404,
then removes only the container it started. It never contacts cloud services.
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


def _container_db_url(url: str) -> str:
    # Docker Desktop (Windows/macOS) reaches the host through host.docker.internal.
    if sys.platform in {"win32", "darwin"}:
        parts = urlsplit(url)
        if parts.hostname in {"localhost", "127.0.0.1"}:
            netloc = parts.netloc.replace(parts.hostname, "host.docker.internal", 1)
            return urlunsplit(parts._replace(netloc=netloc))
    return url


def _get(url: str) -> int:
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("--database-url", default=os.environ.get("SMOKE_DATABASE_URL") or os.environ.get("TEST_POSTGRES_URL"))
    parser.add_argument("--port", type=int, default=18002)
    args = parser.parse_args()
    if not args.database_url:
        print("Set --database-url or TEST_POSTGRES_URL to a disposable migrated database", file=sys.stderr)
        return 2
    if urlsplit(args.database_url).hostname not in {"localhost", "127.0.0.1"}:
        print("Smoke checks only run against a local disposable database", file=sys.stderr)
        return 2

    name = f"allocation-smoke-{uuid.uuid4().hex[:8]}"
    env = {
        "ENVIRONMENT": "local", "ALLOCATION_REST_PORT": "8000", "DATABASE_URL": _container_db_url(args.database_url),
        "LOCK_SERVICE_ADDR": "lock-unavailable.invalid:50051", "BED_SERVICE_ADDR": "bed-unavailable.invalid:50051",
        "STAFF_SERVICE_ADDR": "staff-unavailable.invalid:50051", "TRIAGE_SERVICE_ADDR": "triage-unavailable.invalid:50051",
        "PATIENT_SERVICE_URL": "http://patient-unavailable.invalid/api/v1",
    }
    network = ["--network", "host"] if sys.platform.startswith("linux") else ["-p", f"{args.port}:8000"]
    port = 8000 if sys.platform.startswith("linux") else args.port
    command = ["docker", "run", "-d", "--name", name, *network]
    for key, value in env.items():
        command += ["-e", f"{key}={value}"]
    subprocess.run(command + [args.image], check=True, stdout=subprocess.DEVNULL)
    try:
        base = f"http://localhost:{port}"
        deadline = time.time() + 60
        health = None
        while time.time() < deadline:
            try:
                health = _get(f"{base}/healthz")
                if health == 200:
                    break
            except OSError:
                pass
            time.sleep(1)
        ready = _get(f"{base}/readyz")
        missing = _get(f"{base}/api/v1/allocations/smoke-missing-allocation")
        print(f"healthz={health} readyz={ready} missing_allocation={missing}")
        return 0 if health == 200 and ready == 200 and missing == 404 else 1
    finally:
        subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    sys.exit(main())
