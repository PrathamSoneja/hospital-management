"""
Ownership and delivery invariants checked against durable state, not HTTP status counts.

`audit(database_url, dependency_state)` returns a JSON-serializable report; `violations`
lists every broken invariant (empty means all hold).
"""
from collections import Counter

from sqlalchemy import create_engine, text

SETTLED = {"ALLOCATED", "WAITLISTED", "RELEASED", "CANCELLED", "FAILED"}


def audit(database_url: str, dependency_state: dict | None = None, pools=None) -> dict:
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            pool_filter = "WHERE pool_id = ANY(:pools)" if pools else ""
            params = {"pools": list(pools)} if pools else {}
            rows = connection.execute(text(
                f"SELECT allocation_id, admission_id, pool_id, status, bed_id, staff_assignment_id, pending_action "
                f"FROM allocations {pool_filter}"), params).mappings().all()
            streams = connection.execute(text(
                "SELECT destination, pool_id, count(*) AS rows, count(DISTINCT stream_sequence) AS distinct_sequences, "
                "min(stream_sequence) AS min_sequence, max(stream_sequence) AS max_sequence, "
                "count(*) FILTER (WHERE status <> 'DELIVERED') AS undelivered, "
                "count(*) FILTER (WHERE status = 'POISONED') AS poisoned "
                "FROM outbox_events WHERE stream_sequence IS NOT NULL "
                f"{'AND pool_id = ANY(:pools)' if pools else ''} "
                "GROUP BY destination, pool_id ORDER BY destination, pool_id"), params).mappings().all()
    finally:
        engine.dispose()

    violations = []
    statuses = Counter(row["status"] for row in rows)
    unsettled = [row["allocation_id"] for row in rows if row["status"] not in SETTLED or row["pending_action"]]
    if unsettled:
        violations.append(f"{len(unsettled)} allocations not settled (PROCESSING/RECOVERING or pending action)")

    active = [row for row in rows if row["status"] in ("ALLOCATED", "WAITLISTED")]
    duplicate_admissions = [a for a, n in Counter(r["admission_id"] for r in active).items() if n > 1]
    if duplicate_admissions:
        violations.append(f"{len(duplicate_admissions)} admissions with more than one active allocation")

    allocated = [row for row in rows if row["status"] == "ALLOCATED"]
    if any(row["bed_id"] is None for row in allocated):
        violations.append("ALLOCATED rows without a bed")
    duplicate_beds = [b for b, n in Counter(r["bed_id"] for r in allocated).items() if n > 1]
    if duplicate_beds:
        violations.append(f"{len(duplicate_beds)} beds owned by more than one ALLOCATED row")

    if dependency_state is not None:
        allocated_by_bed = {row["bed_id"]: row["allocation_id"] for row in allocated}
        pool_set = set(pools) if pools else None
        occupied = {b["bed_id"]: b["current_allocation_id"] for b in dependency_state["beds"]
                    if b["status"] == "OCCUPIED" and (pool_set is None or b["pool_id"] in pool_set)}
        if occupied != allocated_by_bed:
            violations.append("Bed Registry ownership differs from ALLOCATED rows "
                              f"(registry={len(occupied)}, allocation={len(allocated_by_bed)})")
        active_assignments = {a["allocation_id"] for a in dependency_state["assignments"].values()
                              if a["active"] and (pool_set is None or a["pool_id"] in pool_set)}
        if active_assignments != {row["allocation_id"] for row in allocated}:
            violations.append("active staff assignments differ from ALLOCATED rows")

    stream_report = []
    for stream in streams:
        entry = {k: stream[k] for k in stream.keys()}
        stream_report.append(entry)
        if not (entry["rows"] == entry["distinct_sequences"] == entry["max_sequence"] and entry["min_sequence"] == 1):
            violations.append(f"outbox stream {entry['destination']}/{entry['pool_id']} is not gap-free 1..n")
        if entry["poisoned"]:
            violations.append(f"outbox stream {entry['destination']}/{entry['pool_id']} has poisoned rows")

    return {
        "allocations": len(rows), "statuses": dict(statuses),
        "distinct_allocated_beds": len({r["bed_id"] for r in allocated}),
        "unsettled": len(unsettled), "outbox_streams": stream_report,
        "outbox_undelivered": sum(s["undelivered"] for s in stream_report),
        "violations": violations,
    }
