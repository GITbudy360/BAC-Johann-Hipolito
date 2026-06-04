import http from 'k6/http';
import { check } from 'k6';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Configuration using the Ramping Arrival Rate executor
export const options = {
  scenarios: {
    redis_hpa_trigger: {
      executor: 'ramping-arrival-rate',
      startRate: 50,
      timeUnit: '1s', // 50 requests per second
      preAllocatedVUs: 50, // Allocate fewer VUs to save Windows VM resources
      maxVUs: 300, // Maximum VUs k6 can scale to if requests get queued
      stages: [
        { duration: '30s', target: 1000 }, 
        { duration: '2m', target: 1000 },
        { duration: '30s', target: 0 },   // Ramp down
      ],
    },
  },
};

const API_URL = __ENV.API_URL || 'http://localhost:8000';

export default function () {
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
}