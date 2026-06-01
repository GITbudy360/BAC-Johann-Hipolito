import http from 'k6/http';
import { check, sleep } from 'k6';

// --- CONFIGURATION ---
// Replace this with the actual IP/Port exposed by your k3s cluster
const BASE_URL = 'http://192.168.1.101:8080'; 

export const options = {
    // Run a very small test: 5 virtual users for 10 seconds
    vus: 5,
    duration: '10s',
};

export default function () {
    // Target the health check endpoint which has no Redis dependency
    let res = http.get(`${BASE_URL}/healthy`);
    
    // Validate that the request was successful (HTTP 200)
    check(res, {
        'Status is 200': (r) => r.status === 200,
        'Response is healthy': (r) => r.json('status') === 'healthy',
    });

    // Pacing between requests
    sleep(1);
}