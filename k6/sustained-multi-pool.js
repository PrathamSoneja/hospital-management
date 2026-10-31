import http from 'k6/http';
import { check, sleep } from 'k6';

const baseUrl = (__ENV.BASE_URL || '').replace(/\/$/, '');
const poolIds = (__ENV.POOL_IDS || '').split(',').map((value) => value.trim()).filter(Boolean);
const vus = Number(__ENV.VUS || 20);
const duration = __ENV.DURATION || '10m';

if (!baseUrl || poolIds.length < 2) {
  throw new Error('Set BASE_URL and at least two POOL_IDS for a disposable local target');
}

export const options = {
  scenarios: {
    sustained_multi_pool: {
      executor: 'constant-vus',
      vus,
      duration,
    },
  },
  thresholds: {
    checks: ['rate>0.99'],
    http_req_duration: ['p(95)<3000'],
  },
};

export default function () {
  const index = (__VU + __ITER) % poolIds.length;
  const requestId = `k6-${__VU}-${__ITER}`;
  const response = http.get(`${baseUrl}/api/v1/allocations?pool_id=${encodeURIComponent(poolIds[index])}`, {
    headers: { 'X-Request-ID': requestId },
  });
  check(response, {
    'multi-pool allocation list succeeds': (r) => r.status === 200,
  });
  sleep(0.1);
}
