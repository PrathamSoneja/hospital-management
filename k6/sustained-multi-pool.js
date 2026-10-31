// Sustained multi-pool Allocation + Triage traffic against DISPOSABLE targets only.
//
// Each iteration takes the next pre-seeded admission for one pool
// (`${ADMISSION_PREFIX}-${pool}-${n}`, n = 1..ADMISSIONS_PER_POOL), PUTs its
// triage to TRIAGE_URL, creates an allocation on ALLOCATION_URL, releases it
// when it was ALLOCATED (exercising promotion), and reads the pool waitlist and
// allocation list. Used for HPA observation; correctness is checked by contention.js.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';

const allocationUrl = (__ENV.ALLOCATION_URL || '').replace(/\/$/, '');
const triageUrl = (__ENV.TRIAGE_URL || '').replace(/\/$/, '');
const poolIds = (__ENV.POOL_IDS || '').split(',').map((value) => value.trim()).filter(Boolean);
const perPool = Number(__ENV.ADMISSIONS_PER_POOL || 2000);
const prefix = __ENV.ADMISSION_PREFIX || 'adm-load';
const runId = __ENV.RUN_ID || `${Date.now()}`;
const releaseAllocated = (__ENV.RELEASE_ALLOCATED || 'true') === 'true';

if (!allocationUrl || !triageUrl || poolIds.length < 2) {
  throw new Error('Set ALLOCATION_URL, TRIAGE_URL, and at least two POOL_IDS for disposable targets');
}

export const options = {
  scenarios: {
    sustained_multi_pool: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: __ENV.RAMP || '1m', target: Number(__ENV.VUS || 20) },
        { duration: __ENV.DURATION || '10m', target: Number(__ENV.VUS || 20) },
        { duration: '30s', target: 0 },
      ],
    },
  },
  thresholds: {
    'checks{kind:write}': ['rate>0.98'],
    'checks{kind:read}': ['rate>0.99'],
    http_req_duration: ['p(95)<3000'],
  },
};

function headers(requestId, key) {
  const result = { 'Content-Type': 'application/json', 'X-Request-ID': requestId };
  if (key) result['Idempotency-Key'] = key;
  return result;
}

export default function () {
  const sequence = exec.scenario.iterationInTest;
  const poolId = poolIds[sequence % poolIds.length];
  const n = Math.floor(sequence / poolIds.length) + 1;
  if (n > perPool) {
    sleep(1);  // seeded admissions exhausted; keep reads only
  }
  const admissionId = `${prefix}-${poolId}-${n}`;
  const requestId = `k6-${runId}-${sequence}`;

  if (n <= perPool) {
    const triage = http.put(`${triageUrl}/api/v1/triage/${admissionId}`,
      JSON.stringify({ triage_level: 1 + (n % 5), pool_id: poolId }),
      { headers: headers(`${requestId}-t`, `k6-${runId}-triage-${admissionId}`), tags: { kind: 'write' } });
    check(triage, { 'triage PUT ok': (r) => r.status === 200 }, { kind: 'write' });

    const created = http.post(`${allocationUrl}/api/v1/allocations`,
      JSON.stringify({ admission_id: admissionId, pool_id: poolId }),
      { headers: headers(`${requestId}-a`, `k6-${runId}-alloc-${admissionId}`), tags: { kind: 'write' } });
    check(created, { 'allocation accepted': (r) => [201, 202].includes(r.status) }, { kind: 'write' });

    if (releaseAllocated && created.status === 201 && created.json('status') === 'ALLOCATED') {
      const allocationId = created.json('allocation_id');
      const released = http.post(`${allocationUrl}/api/v1/allocations/${allocationId}/release`,
        JSON.stringify({ reason: 'k6-discharge' }),
        { headers: headers(`${requestId}-r`, `k6-${runId}-release-${allocationId}`), tags: { kind: 'write' } });
      check(released, { 'release accepted': (r) => [200, 202].includes(r.status) }, { kind: 'write' });
    }
  }

  const waitlist = http.get(`${triageUrl}/api/v1/waitlists/${encodeURIComponent(poolId)}?limit=20`,
    { headers: headers(`${requestId}-w`), tags: { kind: 'read' } });
  check(waitlist, { 'waitlist read ok': (r) => r.status === 200 }, { kind: 'read' });
  const listed = http.get(`${allocationUrl}/api/v1/allocations?pool_id=${encodeURIComponent(poolId)}&limit=20`,
    { headers: headers(`${requestId}-l`), tags: { kind: 'read' } });
  check(listed, { 'allocation list ok': (r) => r.status === 200 }, { kind: 'read' });
  sleep(Number(__ENV.THINK_SECONDS || 0.2));
}
