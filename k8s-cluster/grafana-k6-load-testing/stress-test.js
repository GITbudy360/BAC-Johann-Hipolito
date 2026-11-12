import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  // A single, aggressive stage that steadily increases VUs until failure
  stages: [
    { duration: '10m', target: 1000 }, 
  ],
  
  thresholds: {
    // Automatically abort the test if the overall error rate exceeds 5%
    'http_req_failed': [{ threshold: 'rate<0.05', abortOnFail: true }],
    
    // Automatically abort if the 95th percentile latency crosses 2 seconds
    'http_req_duration': [{ threshold: 'p(95)<2000', abortOnFail: true }],
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
      tags: { endpoint: 'write_score' }, 
    };
    const res = http.post(`${BASE_URL}/player/${playerId}/score`, payload, params);
    check(res, { 'write status is 200': (r) => r.status === 200 });

  } else if (rand < 0.90) {
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      tags: { endpoint: 'read_rank' }
    });
    check(res, { 'rank status is 200 or 404': (r) => r.status === 200 || r.status === 404 });

  } else {
    const res = http.get(`${BASE_URL}/leaderboard?top=100`, {
      tags: { endpoint: 'read_leaderboard' }
    });
    check(res, { 'leaderboard status is 200': (r) => r.status === 200 });
  }

  sleep(1);
}