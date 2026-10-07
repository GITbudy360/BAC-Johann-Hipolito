import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    // 00:00 - 06:00 (Night Baseline): 5m at 10 VUs (Initializes predictive lookback window)
    { duration: '5m', target: 10 },

    // 06:00 - 11:30 (Morning Ramp): 4m climbing to 80 VUs (Tests linear forecasting)
    { duration: '4m', target: 80 },

    // 11:30 - 13:30 (Lunch Peak): 8m held at 180 VUs (Forces Opstree re-sharding & measures 1a, 1b, 2b)
    { duration: '8m', target: 180 },

    // 13:30 - 16:00 (Afternoon Plateau): 4m stepping down to 60 VUs (Post-scale stabilization)
    { duration: '4m', target: 60 },

    // 16:00 - 17:00 (Flash Spike): 1m spike to 200 VUs immediately followed by a drop
    { duration: '1m', target: 200 },

    // Premature Drop: 10s cliff down to 20 VUs (Tests 3c: Predictive Resource Waste)
    { duration: '10s', target: 20 },

    // Cooldown & Observation: 8m at 20 VUs (Observes idle pod allocation and scale-down)
    { duration: '8m', target: 20 },
    { duration: '1m', target: 0 },
  ],

  thresholds: {
    // Explicitly track p50, p95, and p99 to measure client disturbance (3b)
    // Adding 'delay' ensures we only track the latency of successful 200 OK responses
    // Isolating latency thresholds by endpoint tag to measure specific client disturbance

    'http_req_duration{endpoint:write_score}': ['p(50)<200', 'p(95)<500', 'p(99)<1000'],
    'http_req_duration{endpoint:read_rank}': ['p(50)<100', 'p(95)<300', 'p(99)<600'],
    'http_req_duration{endpoint:read_leaderboard}': ['p(50)<400', 'p(95)<800', 'p(99)<1500'],  
  },
};

export default function () {
  const BASE_URL = 'http://ca-crs-bak-hipolito-01:31964';
  
  // Use a constrained pool (e.g., 1 to 5000) so random reads overlap with random writes
  const playerId = `player_${Math.floor(Math.random() * 5000) + 1}`;
  const rand = Math.random();

  if (rand < 0.20) {
    // 20% Probability: Write a new score
    const payload = JSON.stringify({ score: Math.random() * 10000 });
    const params = {
      headers: { 'Content-Type': 'application/json' },
      tags: { endpoint: 'write_score' }, // Tag for isolated metrics
    };
    const res = http.post(`${BASE_URL}/player/${playerId}/score`, payload, params);
    check(res, { 'write status is 200': (r) => r.status === 200 });
    console.log(res.status, res.body);

  } else if (rand < 0.90) {
    // 70% Probability: Read a single player's rank
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      tags: { endpoint: 'read_rank' }
    });
    // Accept 404 as valid because players might not be written yet in the first few seconds
    check(res, { 'rank status is 200 or 404': (r) => r.status === 200 || r.status === 404 });
    console.log(res.status, res.body);

  } else {
    // 10% Probability: Read the full leaderboard
    const res = http.get(`${BASE_URL}/leaderboard?top=100`, {
      tags: { endpoint: 'read_leaderboard' }
    });
    check(res, { 'leaderboard status is 200': (r) => r.status === 200 });
    console.log(res.status, res.body);
  }

  //sleep(1);
}