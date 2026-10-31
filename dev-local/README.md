# Local-only support

`dev-local/` is ignored except for this command guide. Keep machine-specific
mock clients, seed data, emulator helpers, and Compose overrides here. Tests
use the tracked doubles under `tests/support/` and never import this directory.
Production entrypoints use real adapters only; missing dependencies fail
clearly. Docker PostgreSQL is the local relational database, not an emulator.

## Local dependencies and regression

From the repository root in PowerShell:

```powershell
Copy-Item dev-local/local.env.example .env
docker compose up -d postgres firestore-emulator
$env:TEST_POSTGRES_URL = 'postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_pratham_test'
$env:FIRESTORE_EMULATOR_HOST = 'localhost:8080'
.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_allocation_service.py tests\test_allocation_firestore_recovery.py tests\test_triage_service.py tests\test_triage_firestore_operations.py tests\test_triage_put_firestore.py tests\test_triage_lock_coordinator.py tests\contracts\test_event_schemas.py -q
```

The test database must be a disposable PostgreSQL database created separately.
The command above does not run migrations or alter the original
`hospital_allocation` database. Firestore tests use only the emulator. The
Coordinator adapter test runs a local contract-shaped gRPC test server; it is
not evidence of a live teammate service.

Run only Patient Registry client contract tests without its cloud storage SDK:

```powershell
.venv\Scripts\python.exe -m pytest -p no:cacheprovider tests\test_patient_registry_client.py -q
```

`tests/test_patient_registry.py` needs the declared `google-cloud-storage`
dependency installed from `requirements.txt`, even when tests inject in-memory
document storage. It does not contact Cloud Storage.

## Runtime support boundary

- `docker compose up -d postgres firestore-emulator` starts local storage
  dependencies only. Pub/Sub emulator is available as a separate Compose
  service; no local Kafka broker is currently configured.
- Do not use `USE_FAKES`. Allocation and Triage runtime entrypoints select real
  service adapters. Current teammate-owned services are undeployed, so local
  application-to-application integration is blocked until their endpoints are
  supplied.
- `dev-local/` doubles are inspection/manual test support. Tracked CI tests use
  independent `tests/support/` doubles so ignored files are never required.
- Optional explicitly selected Allocation demo (after creating the disposable
  database above):

  ```powershell
  docker exec hospital-postgres psql -U postgres -c "CREATE DATABASE hospital_allocation_demo_test"
  $env:DATABASE_URL = 'postgresql+psycopg2://postgres:postgrespassword@localhost:5432/hospital_allocation_demo_test'
  .venv\Scripts\python.exe -m alembic upgrade head
  .venv\Scripts\python.exe dev-local\allocation_demo.py
  ```

  This uses Docker PostgreSQL as real local storage and in-memory stubs for
  Patient Registry, Triage, Lock Coordinator, Bed Registry, and Staff Scheduler.
  It does not publish outbox events; there is no real publisher worker yet.
- After starting the explicit local Allocation demo above, run the contention
  script against its seeded admission and the read-only sustained script across
  at least two pool IDs:

  ```powershell
  k6 run -e BASE_URL=http://127.0.0.1:8002 -e ADMISSION_ID=adm-101 -e POOL_ID=WARDA-ICU k6\contention.js
  k6 run -e BASE_URL=http://127.0.0.1:8002 -e POOL_IDS=WARDA-ICU,WARDA-STEPDOWN -e VUS=20 -e DURATION=10m k6\sustained-multi-pool.js
  ```

  The first script allows the expected same-admission 409 conflict; the second
  repeatedly reads allocation pages and makes no allocation changes. These
  scripts were prepared but not run in this workspace because k6 is not installed.
- No Compose override existed in the current checkout; the existing root Compose
  file remains the local dependency stack, including real PostgreSQL and optional
  Firestore/Pub/Sub emulators.
- `.env.gcp.example` documents GCP runtime keys without credentials or
  emulator settings. Terraform outputs are configuration outputs only; no
  Terraform apply or deployed resource state is confirmed in this workspace.

## Local k6 load targets (MOCK-TESTED evidence only)

`dev-local/load_target.py` (ignored, machine-local) starts real Allocation code on a disposable
migrated database with explicit stateful doubles from `tests/support/` (exclusive Lock Coordinator,
Bed/Staff/Patient, in-process Triage), and real Triage code on the Firestore emulator with the gRPC
lock double. Use a fresh `*_test` database per contention run.

```powershell
$env:ENVIRONMENT='local'; $env:DATABASE_URL='<disposable *_test URL>'; .venv\Scripts\python.exe dev-local\load_target.py allocation
$env:ENVIRONMENT='local'; $env:FIRESTORE_EMULATOR_HOST='localhost:8080'; .venv\Scripts\python.exe dev-local\load_target.py triage
.\scripts\local\Invoke-LocalLoad.ps1 -Scenario contention
.\scripts\local\Invoke-LocalLoad.ps1 -Scenario sustained -Duration 2m
```

Full local verification: `.\scripts\local\Start-LocalStack.ps1` then `.\scripts\local\Invoke-LocalTests.ps1`
(reads the ignored `.env.local`; created from `.env.local.example`).
