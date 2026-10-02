import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    // 1. Baseline: Establish normal operation metrics before scaling triggers
    { duration: '2m', target: 20 }, 
    
    // 2. Load Spike: Trigger HPA/KEDA and force the Opstree operator to re-shard
    { duration: '1m', target: 150 }, 
    
    // 3. Sustained Peak: Hold long enough to measure Data Sync Duration (2b) 
    // and observe Re-sharding CPU Penalty (1a) / Memory Pressure (1b)
    { duration: '7m', target: 150 }, 
    
    // 4. Premature Drop: Abruptly kill the traffic to test Predictive Resource Waste (3c)
    // This abrupt drop tests if the forecasting model over-provisioned.
    { duration: '10s', target: 10 }, 
    
    // 5. Cooldown: Observe control plane overhead (1c) during scale-down
    { duration: '3m', target: 10 },
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
  const res = http.get('http://ca-crs-bak-hipolito-01:31964/'); 

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

  } else if (rand < 0.90) {
    // 70% Probability: Read a single player's rank
    const res = http.get(`${BASE_URL}/player/${playerId}/rank`, {
      tags: { endpoint: 'read_rank' }
    });
    // Accept 404 as valid because players might not be written yet in the first few seconds
    check(res, { 'rank status is 200 or 404': (r) => r.status === 200 || r.status === 404 });

  } else {
    // 10% Probability: Read the full leaderboard
    const res = http.get(`${BASE_URL}/leaderboard?top=100`, {
      tags: { endpoint: 'read_leaderboard' }
    });
    check(res, { 'leaderboard status is 200': (r) => r.status === 200 });
    
  }
  
  check(res, {
    'node resolved and reachable': (r) => r.error_code === 0,
    'status is 200': (r) => r.status === 200,
  });

  console.log(res.status, res.body);

  sleep(1); // Add a 1-second delay between requests
}