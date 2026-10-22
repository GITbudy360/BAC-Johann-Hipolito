#!/usr/bin/env python3
import json
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime

# --- Autoscaling Configuration ---
NAMESPACE = "redis-cluster"
REDIS_CLUSTER_NAME = "redis-master"
MIN_REPLICAS = 3
MAX_REPLICAS = 4
THRESHOLD_RPS = 65.0
POLL_INTERVAL_SECONDS = 10
COOLDOWN_SCALE_DOWN_SECONDS = 300  # 5 minutes stabilization window

PROMQL_QUERY = (
    'sum(rate(http_request_duration_seconds_count'
    '{namespace="fastapi-leaderboard"}[2m])) or vector(0)'
)


def log(msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def get_prometheus_endpoint() -> str:
    """Find the active Prometheus Pod IP directly."""
    try:
        cmd = [
            "sudo","k3s","kubectl","get","pods","-n","monitoring","-l","app.kubernetes.io/name=prometheus","-o","jsonpath={.items[0].status.podIP}",
        ]
        ip = subprocess.check_output(cmd, text=True).strip()
        if ip and ip.lower() != "none":
            return f"http://{ip}:9090"
    except Exception:
        pass
    return "http://127.0.0.1:9090"


def get_current_cluster_size() -> int:
    cmd = [
        "sudo","k3s","kubectl","get","rediscluster",REDIS_CLUSTER_NAME,"-n",NAMESPACE,"-o","jsonpath={.spec.clusterSize}",
    ]
    try:
        size_str = subprocess.check_output(cmd, text=True).strip()
        return int(size_str)
    except Exception as e:
        log(f"Error fetching current cluster size: {e}")
        return MIN_REPLICAS


def patch_cluster_size(new_size: int) -> bool:
    cmd = [
        "sudo",
        "k3s",
        "kubectl",
        "patch",
        "rediscluster",
        REDIS_CLUSTER_NAME,
        "-n",
        NAMESPACE,
        "--type=merge",
        "-p",
        json.dumps({"spec": {"clusterSize": new_size}}),
    ]
    try:
        out = subprocess.check_output(cmd, text=True).strip()
        log(f"Scaled RedisCluster to {new_size} nodes: {out}")
        return True
    except subprocess.CalledProcessError as e:
        log(f"Failed to patch RedisCluster: {e}")
        return False


def query_prometheus(base_url: str, query: str) -> float:
    encoded_query = urllib.parse.urlencode({"query": query})
    url = f"{base_url}/api/v1/query?{encoded_query}"
    req = urllib.request.Request(url, headers={"User-Agent": "Redis-Poller-Daemon"})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            payload = json.loads(response.read().decode())
            results = payload.get("data", {}).get("result", [])
            if results and "value" in results[0]:
                return float(results[0]["value"][1])
    except Exception as err:
        log(f"Prometheus query failed ({url}): {err}")
    return 0.0


def main():
    prom_url = get_prometheus_endpoint()
    log(f"Starting Redis Autoscaler Poller Daemon targeting Prometheus at {prom_url}")
    log(
        f"Config: Min={MIN_REPLICAS}, Max={MAX_REPLICAS}, Threshold={THRESHOLD_RPS} RPS, Cooldown={COOLDOWN_SCALE_DOWN_SECONDS}s"
    )

    below_threshold_start_time = None

    while True:
        current_size = get_current_cluster_size()
        current_rps = query_prometheus(prom_url, PROMQL_QUERY)

        log(f"Telemetry: {current_rps:.2f} RPS | Active Cluster Size: {current_size}")

        if current_rps > THRESHOLD_RPS:
            below_threshold_start_time = None
            if current_size < MAX_REPLICAS:
                log(
                    f"Threshold breached ({current_rps:.2f} > {THRESHOLD_RPS} RPS). Scaling up to {MAX_REPLICAS}."
                )
                patch_cluster_size(MAX_REPLICAS)
        else:
            if current_size > MIN_REPLICAS:
                if below_threshold_start_time is None:
                    below_threshold_start_time = time.time()
                    log(
                        f"Traffic dropped below threshold. Entering {COOLDOWN_SCALE_DOWN_SECONDS}s stabilization cooldown."
                    )
                else:
                    elapsed = time.time() - below_threshold_start_time
                    remaining = max(0, int(COOLDOWN_SCALE_DOWN_SECONDS - elapsed))
                    log(
                        f"Cooldown active: {remaining}s remaining before scaling down to {MIN_REPLICAS}."
                    )

                    if elapsed >= COOLDOWN_SCALE_DOWN_SECONDS:
                        log(
                            f"Cooldown period elapsed. Scaling down RedisCluster to {MIN_REPLICAS}."
                        )
                        patch_cluster_size(MIN_REPLICAS)
                        below_threshold_start_time = None
            else:
                below_threshold_start_time = None

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()