import http from 'k6/http';
import { check } from 'k6';

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
    http_req_duration: ['p(50)<200', 'p(95)<500', 'p(99)<1000'], 
  },
};

export default function () {
  const res = http.get('http://ca-crs-bak-hipolito-01:31964/'); 
  
  check(res, {
    'node resolved and reachable': (r) => r.error_code === 0,
    'status is 200': (r) => r.status === 200,
  });
}