import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Configuration for the load test phases
export const options = {
  stages: [
    { duration: '30s', target: 50 },  // Ramp up to 50 virtual users
    { duration: '2m', target: 200 },  // Hold at 200 users to test scaling
    { duration: '30s', target: 0 },   // Ramp down
  ],
};

// Pass the Entrypoint API URL as an environment variable
// Example: k6 run -e API_URL=http://<ENTRYPOINT_NODEPORT_IP> test.js
const API_URL = __ENV.API_URL || 'http://localhost:8000';

export default function () {
  // Simulate an 80/20 split between writes (submitting scores) and reads (viewing leaderboard)
  const isWrite = Math.random() < 0.8;

  if (isWrite) {
    const payload = JSON.stringify({
      player_id: `player_${randomString(8)}`,
      score: randomIntBetween(100, 10000),
    });

    const params = {
      headers: { 'Content-Type': 'application/json' },
    };

    const res = http.post(`${API_URL}/score/`, payload, params);
    
    check(res, {
      'score update status is 200': (r) => r.status === 200,
    });
  } else {
    const res = http.get(`${API_URL}/leaderboard/`);
    
    check(res, {
      'leaderboard fetch status is 200': (r) => r.status === 200,
    });
  }

  // Brief pause to simulate human interaction time
  sleep(0.1);
}