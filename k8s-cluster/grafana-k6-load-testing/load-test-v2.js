import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    // Hours 0-4: Nighttime dip (4 hours = 10m)
    { duration: '10m', target: 70 },

    // Hours 4-10: Morning ramp-up (6 hours = 15m)
    { duration: '15m', target: 160 },

    // Hours 10-18: Daytime steady climb (8 hours = 20m)
    { duration: '20m', target: 240 },

    // Hours 18-21: Evening peak (3 hours = 7m30s)
    { duration: '7m30s', target: 300 },

    // Hours 21-24: Late night drop (3 hours = 7m30s)
    { duration: '7m30s', target: 150 },
  ],

  thresholds: {
    'http_req_duration{endpoint:write_score}': ['p(50)<200', 'p(95)<500', 'p(99)<1000'],
    'http_req_duration{endpoint:read_rank}': ['p(50)<100', 'p(95)<300', 'p(99)<600'],
    'http_req_duration{endpoint:read_leaderboard}': ['p(50)<400', 'p(95)<800', 'p(99)<1500'],  
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
        name: '/player/{id}/score'  // Groups all dynamic IDs into one metric
      }, 
    };
    const res = http.post(`${BASE_URL}/player/${playerId}/score`, payload, params);
    check(res, { 'write status is 200': (r) => r.status === 200 });

  } else if (rand < 0.90) {
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      tags: { 
        endpoint: 'read_rank', 
        name: '/player/{id}/rank'   // Groups all dynamic IDs into one metric
      }
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