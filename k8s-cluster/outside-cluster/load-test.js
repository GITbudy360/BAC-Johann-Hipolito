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

// --- Target resolution: spread ingress across ALL node IPs ----------------
// Hammering a single NodePort IP saturates that one node's conntrack table /
// SYN backlog under load, which surfaces on the client as `connectex` /
// "connection timed out". Spreading requests across every node removes that
// single-node bottleneck - and keeps the thesis measuring Redis scaling rather
// than ingress saturation on one box.
//
// Precedence:
//   API_HOSTS="ip1,ip2,..." (+ API_PORT, default 30080) -> round-robin these
//   API_URL="http://host:port"                          -> single host (legacy)
//   neither                                             -> all 6 cluster nodes
const API_PORT = __ENV.API_PORT || '30080';
const DEFAULT_HOSTS = [
  '192.168.1.101', '192.168.1.102', '192.168.1.103',
  '192.168.1.104', '192.168.1.105', '192.168.1.106',
];

function resolveBases() {
  if (__ENV.API_HOSTS) {
    return __ENV.API_HOSTS.split(',')
      .map((h) => h.trim())
      .filter(Boolean)
      .map((h) => (h.includes('://') ? h : `http://${h}:${API_PORT}`));
  }
  if (__ENV.API_URL) {
    return [__ENV.API_URL.trim()];
  }
  return DEFAULT_HOSTS.map((h) => `http://${h}:${API_PORT}`);
}

// Resolve once at init and strip any trailing slash so `${base}/score/` never
// becomes `//score/`.
const BASES = resolveBases().map((b) => b.replace(/\/+$/, ''));

// Pick a node per request. Random spread balances connections across nodes
// without VUs needing to share a counter.
function apiBase() {
  return BASES[Math.floor(Math.random() * BASES.length)];
}

// --- Workload shape -------------------------------------------------------
// Keys are spread across many sorted sets so adding/resharding Redis nodes can
// actually relieve load. Format: lb:<region>:<mode>:<window>
// NOTE: no curly braces -> no hash tags -> keys spread across slots/nodes.
// Region popularity is skewed (Zipfian-ish) to create realistic hotspots.
const REGIONS = [
  { name: 'asia', weight: 40 },
  { name: 'eu',   weight: 30 },
  { name: 'na',   weight: 20 },
  { name: 'sa',   weight: 6 },
  { name: 'oce',  weight: 3 },
  { name: 'afr',  weight: 1 },
];
const REGION_TOTAL = REGIONS.reduce((sum, r) => sum + r.weight, 0);

// 'ranked' is the hot mode; casual/blitz are cooler.
const MODES = ['ranked', 'ranked', 'ranked', 'casual', 'blitz'];

const WINDOW_DAYS = 7;          // boards for the last 7 days exist (warm)
const COLD_READ_FRACTION = 0.25; // 25% of reads hit nonexistent boards -> keyspace misses (drives KEDA)

function weightedRegion() {
  let pick = Math.random() * REGION_TOTAL;
  for (const r of REGIONS) {
    pick -= r.weight;
    if (pick <= 0) return r.name;
  }
  return REGIONS[0].name;
}

function dateNDaysFromNow(n) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10); // YYYY-MM-DD
}

// Warm board: a region/mode within the last WINDOW_DAYS.
function warmBoard() {
  const region = weightedRegion();
  const mode = MODES[Math.floor(Math.random() * MODES.length)];
  const day = dateNDaysFromNow(-randomIntBetween(0, WINDOW_DAYS - 1));
  return `lb:${region}:${mode}:${day}`;
}

// Cold board: a future date that was never written -> guaranteed cache miss.
function coldBoard() {
  const region = weightedRegion();
  const mode = MODES[Math.floor(Math.random() * MODES.length)];
  const day = dateNDaysFromNow(randomIntBetween(1, 30));
  return `lb:${region}:${mode}:${day}`;
}

export default function () {
  const base = apiBase();
  const isWrite = Math.random() < 0.8;

  if (isWrite) {
    const payload = JSON.stringify({
      board: warmBoard(),
      player_id: `player_${randomString(8)}`,
      score: randomIntBetween(100, 10000),
    });

    const params = {
      headers: { 'Content-Type': 'application/json' },
    };

    const res = http.post(`${base}/score/`, payload, params);

    check(res, {
      'score update status is 200': (r) => r.status === 200,
    });
  } else {
    // Most reads hit warm boards; a fraction intentionally miss to exercise the
    // KEDA cache-miss-ratio trigger.
    const board = Math.random() < COLD_READ_FRACTION ? coldBoard() : warmBoard();
    const res = http.get(`${base}/leaderboard/?board=${encodeURIComponent(board)}`);

    check(res, {
      'leaderboard fetch status is 200': (r) => r.status === 200,
    });
  }
}
