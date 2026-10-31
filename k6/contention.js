// Pool contention against a DISPOSABLE Allocation target only.
//
// MODE=distinct (default): ADMISSIONS distinct eligible admissions
// (`${ADMISSION_PREFIX}-${n}`, n = 1..ADMISSIONS) each submit one allocation for
// POOL_ID at once. The pool must start with EXPECT_ALLOCATED free beds, enough
// staff, and no releases. teardown() waits for every row to settle and then
// verifies state, not HTTP counts: exactly EXPECT_ALLOCATED ALLOCATED rows with
// distinct bed_ids, the rest WAITLISTED, one row per admission.
//
// MODE=same-admission: many retries of one ADMISSION_ID with distinct
// Idempotency-Keys; at most one active allocation may exist afterwards.
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

const baseUrl = (__ENV.BASE_URL || '').replace(/\/$/, '');
const mode = __ENV.MODE || 'distinct';
const poolId = __ENV.POOL_ID || '';
const admissions = Number(__ENV.ADMISSIONS || 50);
const expectAllocated = Number(__ENV.EXPECT_ALLOCATED || 10);
const prefix = __ENV.ADMISSION_PREFIX || 'adm-k6';
const runId = __ENV.RUN_ID || `${Date.now()}`;
const settleSeconds = Number(__ENV.SETTLE_SECONDS || 120);

if (!baseUrl || !poolId || (mode === 'same-admission' && !__ENV.ADMISSION_ID)) {
  throw new Error('Set BASE_URL and POOL_ID (and ADMISSION_ID for same-admission) to disposable targets');
}

const settleViolations = new Counter('settle_violations');

export const options = {
  scenarios: mode === 'distinct'
    ? { distinct_admissions: { executor: 'per-vu-iterations', vus: admissions, iterations: 1, maxDuration: '2m' } }
    : { same_admission: { executor: 'shared-iterations', vus: Number(__ENV.VUS || 20), iterations: Number(__ENV.ITERATIONS || 200), maxDuration: '2m' } },
  thresholds: {
    checks: ['rate>0.99'],
    settle_violations: ['count==0'],
    http_req_duration: ['p(95)<3000'],
  },
  teardownTimeout: `${settleSeconds + 30}s`,
};

function headers(requestId, key) {
  return { 'Content-Type': 'application/json', 'Idempotency-Key': key, 'X-Request-ID': requestId };
}

export default function () {
  const requestId = `k6-${runId}-${__VU}-${__ITER}`;
  const admissionId = mode === 'distinct' ? `${prefix}-${__VU}` : __ENV.ADMISSION_ID;
  const key = mode === 'distinct' ? `k6-${runId}-${admissionId}` : `k6-${runId}-${requestId}`;
  const response = http.post(`${baseUrl}/api/v1/allocations`,
    JSON.stringify({ admission_id: admissionId, pool_id: poolId }), { headers: headers(requestId, key) });
  const accepted = mode === 'distinct' ? [201, 202] : [201, 202, 409];
  check(response, { 'allocation accepted (or conflict for same admission)': (r) => accepted.includes(r.status) });
}

function listPool() {
  const items = [];
  let cursor = '';
  for (let page = 0; page < 50; page += 1) {
    const query = `pool_id=${encodeURIComponent(poolId)}&limit=200${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
    const response = http.get(`${baseUrl}/api/v1/allocations?${query}`, { headers: { 'X-Request-ID': `k6-${runId}-settle` } });
    if (response.status !== 200) {
      return null;
    }
    const body = response.json();
    items.push(...body.items);
    if (!body.next_cursor) {
      return items;
    }
    cursor = body.next_cursor;
  }
  return items;
}

function violation(message) {
  console.error(`settle violation: ${message}`);
  settleViolations.add(1);
}

export function teardown() {
  const deadline = Date.now() + settleSeconds * 1000;
  let rows = null;
  while (Date.now() < deadline) {
    const all = listPool();
    rows = all && all.filter((row) => mode !== 'distinct' || row.admission_id.startsWith(`${prefix}-`));
    if (rows && rows.every((row) => ['ALLOCATED', 'WAITLISTED', 'CANCELLED', 'RELEASED', 'FAILED'].includes(row.status))) {
      break;
    }
    sleep(2);
  }
  if (!rows) {
    violation('allocation list unavailable');
    return;
  }
  const count = (status) => rows.filter((row) => row.status === status).length;
  const unsettled = rows.filter((row) => !['ALLOCATED', 'WAITLISTED', 'CANCELLED', 'RELEASED', 'FAILED'].includes(row.status));
  if (unsettled.length) violation(`${unsettled.length} rows still PROCESSING/RECOVERING`);
  if (mode === 'distinct') {
    const allocated = rows.filter((row) => row.status === 'ALLOCATED');
    const beds = new Set(allocated.map((row) => row.bed_id).filter(Boolean));
    const admissionsSeen = new Set(rows.map((row) => row.admission_id));
    if (allocated.length !== expectAllocated) violation(`ALLOCATED=${allocated.length}, expected ${expectAllocated}`);
    if (count('WAITLISTED') !== admissions - expectAllocated) violation(`WAITLISTED=${count('WAITLISTED')}, expected ${admissions - expectAllocated}`);
    if (beds.size !== allocated.length) violation(`distinct beds=${beds.size} for ${allocated.length} allocations`);
    if (admissionsSeen.size !== admissions || rows.length !== admissions) violation(`rows=${rows.length}, distinct admissions=${admissionsSeen.size}`);
    console.log(`settled: ALLOCATED=${allocated.length} WAITLISTED=${count('WAITLISTED')} distinct_beds=${beds.size}`);
  } else {
    const active = rows.filter((row) => row.admission_id === __ENV.ADMISSION_ID && ['ALLOCATED', 'WAITLISTED'].includes(row.status));
    if (active.length > 1) violation(`${active.length} active allocations for one admission`);
  }
}
