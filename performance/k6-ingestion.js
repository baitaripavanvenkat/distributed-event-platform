import http from 'k6/http';
import { check } from 'k6';

export const options = {
    scenarios: {
        baseline_load: {
            executor: 'constant-arrival-rate',
            rate: 100,
            timeUnit: '1s',
            duration: '60s',
            preAllocatedVUs: 20,
            maxVUs: 50,
        },

    },

    thresholds: {
        http_req_failed: ['rate<0.01'],
        http_req_duration: ['p(95)<500'],
    },
};

export default function () {
    const eventId = `load-test-${__VU}-${__ITER}-${Date.now()}`;

    const payload = JSON.stringify({
        eventId: eventId,
        eventType: 'PAGE_VIEW',
        userId: `user-${__VU}`,
        timestamp: new Date().toISOString(),
        source: 'k6-performance-test',
        metadata: {
            page: '/products',
            testRun: 'baseline',
        },
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    const response = http.post(
        'http://localhost:8081/api/v1/events',
        payload,
        params
    );

    check(response, {
        'response is successful': (r) =>
            r.status >= 200 && r.status < 300,
    });


}