// Sustained multi-pool Allocation + Triage traffic against DISPOSABLE targets only.
//
// Each iteration takes the next pre-seeded admission for one pool
// (`${ADMISSION_PREFIX}-${pool}-${n}`, n = 1..ADMISSIONS_PER_POOL), PUTs its
// triage to TRIAGE_URL, creates an allocation on ALLOCATION_URL(S), discharges an existing
// occupant every other iteration (exercising promotion), and reads the pool waitlist and
// allocation list. Used for HPA observation; correctness is checked by contention.js.
import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep } from 'k6';

// ALLOCATION_URLS (comma-separated) spreads load across allocator replicas.
const allocationUrls = (__ENV.ALLOCATION_URLS || __ENV.ALLOCATION_URL || '').split(',').map((u) => u.trim().replace(/\/$/, '')).filter(Boolean);
const triageUrl = (__ENV.TRIAGE_URL || '').replace(/\/$/, '');
const poolIds = (__ENV.POOL_IDS || '').split(',').map((value) => value.trim()).filter(Boolean);
const perPool = Number(__ENV.ADMISSIONS_PER_POOL || 2000);
const prefix = __ENV.ADMISSION_PREFIX || 'adm-load';
const runId = __ENV.RUN_ID || `${Date.now()}`;
const releaseAllocated = (__ENV.RELEASE_ALLOCATED || 'true') === 'true';

if (!allocationUrls.length || !triageUrl || poolIds.length < 2) {
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

// Retryable responses (503/504, e.g. a pool-lock wait timeout) are retried with the SAME
// Idempotency-Key, as a correct client would; only the final outcome is checked.
function withRetry(send) {
  let response = send();
  for (let attempt = 1; attempt < 4 && [503, 504].includes(response.status); attempt += 1) {
    sleep(0.25 * 2 ** attempt);
    response = send();
  }
  return response;
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
  const allocationUrl = allocationUrls[sequence % allocationUrls.length];

  if (n <= perPool) {
    const triage = withRetry(() => http.put(`${triageUrl}/api/v1/triage/${admissionId}`,
      JSON.stringify({ triage_level: 1 + (n % 5), pool_id: poolId }),
      { headers: headers(`${requestId}-t`, `k6-${runId}-triage-${admissionId}`), tags: { kind: 'write' } }));
    const triaged = check(triage, { 'triage PUT ok': (r) => r.status === 200 }, { kind: 'write' });

    if (triaged) {  // an untriaged admission is correctly refused by Allocation; do not count that twice
      const created = withRetry(() => http.post(`${allocationUrl}/api/v1/allocations`,
        JSON.stringify({ admission_id: admissionId, pool_id: poolId }),
        { headers: headers(`${requestId}-a`, `k6-${runId}-alloc-${admissionId}`), tags: { kind: 'write' } }));
      check(created, { 'allocation accepted': (r) => [201, 202].includes(r.status) }, { kind: 'write' });
    }

    // Discharge an existing occupant every other iteration, so beds fill, waitlists form, and
    // releases promote waiting admissions. A concurrent discharge of the same occupant by another
    // VU is a legitimate 409, not a failure.
    if (releaseAllocated && n % 2 === 0) {
      const occupied = http.get(`${allocationUrl}/api/v1/allocations?pool_id=${encodeURIComponent(poolId)}&status=ALLOCATED&limit=10`,
        { headers: headers(`${requestId}-o`), tags: { kind: 'read' } });
      const items = occupied.status === 200 ? occupied.json('items') : [];
      if (items.length) {
        const allocationId = items[sequence % items.length].allocation_id;
        const released = withRetry(() => http.post(`${allocationUrl}/api/v1/allocations/${allocationId}/release`,
          JSON.stringify({ reason: 'k6-discharge' }),
          { headers: headers(`${requestId}-r`, `k6-${runId}-release-${allocationId}`), tags: { kind: 'write' } }));
        check(released, { 'release accepted or already released': (r) => [200, 202, 409].includes(r.status) }, { kind: 'write' });
      }
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
