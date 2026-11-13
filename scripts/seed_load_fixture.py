"""
Seed a fictional k6 load fixture into DESIGNATED TEST services through their own APIs.

    python scripts/seed_load_fixture.py --bed-url URL --staff-url URL --patient-url URL \
        --pool P-LOAD-A --pool P-LOAD-B --admissions 2000 --beds 20 --out evidence/admissions.json \
        --i-confirm-designated-test-environment

Creates beds (Bed Registry), on-shift nurses (Staff Scheduler) and fictional patients/admissions
(Patient Registry), then writes {pool_id: [admission_id, ...]} for k6 (ADMISSIONS_FILE). Triage is set
by k6 itself. URLs are base URLs without /api/v1 (for example kubectl port-forward targets).
Never point this at production or at services holding real patient data.
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests.sandbox.runtime import seed_admissions, seed_beds, seed_staff  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for name in ("--bed-url", "--staff-url", "--patient-url", "--out"):
        parser.add_argument(name, required=True)
    parser.add_argument("--pool", action="append", required=True)
    parser.add_argument("--admissions", type=int, required=True)
    parser.add_argument("--beds", type=int, required=True)
    parser.add_argument("--staff", type=int, help="assignable staff per pool (default admissions + beds)")
    parser.add_argument("--i-confirm-designated-test-environment", action="store_true")
    args = parser.parse_args()
    if not args.i_confirm_designated_test_environment:
        parser.error("refusing to write: pass --i-confirm-designated-test-environment for approved test services only")
    result = {}
    for pool in args.pool:
        seed_beds(args.bed_url, [{"bed_id": f"{pool}-bed-{n:02d}", "pool_id": pool, "ward": f"WARD-{pool}"}
                                 for n in range(1, args.beds + 1)])
        seed_staff(args.staff_url, {pool: args.staff or args.admissions + args.beds})
        ids = seed_admissions(args.patient_url, [{"admission_id": f"{pool}-{n}"} for n in range(1, args.admissions + 1)],
                              f"k6-fixture-{pool}")
        result[pool] = [ids[f"{pool}-{n}"] for n in range(1, args.admissions + 1)]
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result))
    print(f"seeded {len(args.pool)} pool(s); admission IDs written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
