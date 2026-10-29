
import http from 'k6/http';
import { check } from 'k6';

const runId = __ENV.E2E_RUN_ID || `run-${Date.now()}`;

export const options = {
    scenarios: {
        end_to_end_baseline: {
            executor: 'constant-arrival-rate',
            rate: 1000,
            timeUnit: '1s',
            duration: '60s',
            preAllocatedVUs: 200,
            maxVUs: 500,
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500'],
        checks: ['rate>0.99'],
    },
};

export default function () {
    const eventId =
        `e2e-${runId}-${__VU}-${__ITER}-${Date.now()}`;

    const payload = JSON.stringify({
        eventId,
        eventType: 'PAGE_VIEW',
        userId: `e2e-user-${__VU}`,
        timestamp: new Date().toISOString(),
        source: 'k6-e2e-test',
        metadata: {
            page: '/products',
            testRun: runId,
        },
    });

    const response = http.post(
        'http://localhost:8081/api/v1/events',
        payload,
        {
            headers: {
                'Content-Type': 'application/json',
            },
        }
    );

    check(response, {
        'event accepted (HTTP 202)': (r) => r.status === 202,
    });
}
