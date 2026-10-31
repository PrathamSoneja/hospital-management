import http from 'k6/http';
import { check } from 'k6';

const baseUrl = (__ENV.BASE_URL || '').replace(/\/$/, '');
const admissionId = __ENV.ADMISSION_ID || '';
const poolId = __ENV.POOL_ID || '';

if (!baseUrl || !admissionId || !poolId) {
  throw new Error('Set BASE_URL, ADMISSION_ID, and POOL_ID to disposable local targets');
}

export const options = {
  scenarios: {
    same_admission_contention: {
      executor: 'shared-iterations',
      vus: Number(__ENV.VUS || 20),
      iterations: Number(__ENV.ITERATIONS || 200),
      maxDuration: '2m',
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_duration: ['p(95)<2000'],
  },
};

export default function () {
  const requestId = `k6-${__VU}-${__ITER}`;
  const response = http.post(
    `${baseUrl}/api/v1/allocations`,
    JSON.stringify({ admission_id: admissionId, pool_id: poolId }),
    {
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': `k6-contention-${requestId}`,
        'X-Request-ID': requestId,
      },
    },
  );
  check(response, {
    'request has an expected accepted-or-conflict result': (r) => [201, 202, 409].includes(r.status),
  });
}
