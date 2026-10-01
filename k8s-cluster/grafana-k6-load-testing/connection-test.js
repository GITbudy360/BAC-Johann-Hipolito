import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 1,
  iterations: 1, // Run exactly one request
};

export default function () {
  // Replace <NODE_PORT> with the actual port exposed by your FastAPI service
  const res = http.get('http://ca-crs-bak-hipolito-01:8000/'); 
  
  check(res, {
    'node resolved and reachable': (r) => r.error_code === 0,
    'status is 200': (r) => r.status === 200,
  });
}