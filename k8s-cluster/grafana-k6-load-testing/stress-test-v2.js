import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    // Step 1: 60 VUs (Warm-up / Safe baseline)
    { duration: '15s', target: 60 },
    { duration: '2m',  target: 60 },

    // Step 2: 80 VUs
    { duration: '15s', target: 80 },
    { duration: '2m',  target: 80 },

    // Step 3: 100 VUs
    { duration: '15s', target: 100 },
    { duration: '2m',  target: 100 },

    // Step 4: 120 VUs
    { duration: '15s', target: 120 },
    { duration: '2m',  target: 120 },

    // Step 5: 140 VUs
    { duration: '15s', target: 140 },
    { duration: '2m',  target: 140 },

    // Step 6: 160 VUs
    { duration: '15s', target: 160 },
    { duration: '2m',  target: 160 },

    // Step 7: 180 VUs (Upper ceiling check)
    { duration: '15s', target: 180 },
    { duration: '2m',  target: 180 },
  ],

  thresholds: {
    // Abort if p(95) breaches 2000ms; delayAbortEval prevents false aborts during initial warm-up
    http_req_duration: [
      { threshold: 'p(95)<2000', abortOnFail: true, delayAbortEval: '30s' },
    ],
    // Abort if error rate exceeds 1% (ensures middleware/Redis aren't dropping connections)
    http_req_failed: [
      { threshold: 'rate<0.01', abortOnFail: true, delayAbortEval: '30s' },
    ],
  },
};

export default function () {
  const BASE_URL = 'http://ca-crs-bak-hipolito-01:31964';
  const playerId = `player_${Math.floor(Math.random() * 5000) + 1}`;
  const rand = Math.random();

  if (rand < 0.20) {
    const payload = JSON.stringify({ score: Math.random() * 10000 });
    const params = {
      headers: { 'Content-Type': 'application/json' },
      tags: {
        endpoint: 'write_score',
        name: '/player/{id}/score',
      },
    };
    const res = http.post(`${BASE_URL}/player/${playerId}/score`, payload, params);
    check(res, { 'write status is 200': (r) => r.status === 200 });

  } else if (rand < 0.90) {
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      tags: {
        endpoint: 'read_rank',
        name: '/player/{id}/rank',
      },
    });
    check(res, { 'rank status is 200 or 404': (r) => r.status === 200 || r.status === 404 });

  } else {
    const res = http.get(`${BASE_URL}/leaderboard?top=100`, {
      tags: {
        endpoint: 'read_leaderboard',
        name: '/leaderboard',
      },
    });
    check(res, { 'leaderboard status is 200': (r) => r.status === 200 });
  }

  sleep(1);
}