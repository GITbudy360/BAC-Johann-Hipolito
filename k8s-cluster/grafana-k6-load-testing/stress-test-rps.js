import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    step_rps: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 250,
      stages: [
        // Step 1: 20 RPS
        { duration: '15s', target: 20 },
        { duration: '2m',  target: 20 },

        // Step 2: 30 RPS
        { duration: '15s', target: 30 },
        { duration: '2m',  target: 30 },

        // Step 3: 40 RPS
        { duration: '15s', target: 40 },
        { duration: '2m',  target: 40 },

        // Step 4: 50 RPS
        { duration: '15s', target: 50 },
        { duration: '2m',  target: 50 },

        // Step 5: 60 RPS
        { duration: '15s', target: 60 },
        { duration: '2m',  target: 60 },
      ],
    },
  },

  thresholds: {
    http_req_duration: [
      { threshold: 'p(95)<2000', abortOnFail: true, delayAbortEval: '30s' },
    ],
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
    const res = http.post(`${BASE_URL}/player/${playerId}/score`, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: '5s',
      tags: { endpoint: 'write_score', name: '/player/{id}/score' },
    });
    check(res, { 'write status is 200': (r) => r.status === 200 });

  } else if (rand < 0.90) {
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      timeout: '5s',
      tags: { endpoint: 'read_rank', name: '/player/{id}/rank' },
    });
    check(res, { 'rank status is 200 or 404': (r) => r.status === 200 || r.status === 404 });

  } else {
    const res = http.get(`${BASE_URL}/leaderboard?top=100`, {
      timeout: '5s',
      tags: { endpoint: 'read_leaderboard', name: '/leaderboard' },
    });
    check(res, { 'leaderboard status is 200': (r) => r.status === 200 });
  }
}