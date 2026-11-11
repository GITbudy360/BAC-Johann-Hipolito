import time, requests, urllib.parse, datetime
from kubernetes import client, config

THRESHOLD = 65.0
COOLDOWN_SEC = 300
# Uses internal k8s DNS instead of pod IP routing
PROM_URL = "http://prometheus-operated.monitoring.svc.cluster.local:9090"
QUERY = 'sum(rate(http_request_duration_seconds_count{namespace="fastapi-leaderboard", handler!="/metrics"}[2m])) or vector(0)'


def log(msg):
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_rps():
    try:
        resp = requests.get(f"{PROM_URL}/api/v1/query?query={urllib.parse.quote(QUERY)}", timeout=5)
        return float(resp.json()['data']['result'][0]['value'][1])
    except Exception as e:
        log(f"Prometheus query failed: {e}")
        return 0.0


def get_cluster_size(api):
    try:
        res = api.get_namespaced_custom_object(
            group="redis.redis.opstreelabs.in", version="v1beta1", namespace="redis-cluster", plural="redisclusters", name="redis-master"
        )
        return res.get("spec", {}).get("clusterSize", 3)
    except Exception:
        return 3


def patch_cluster(api, size):
    try:
        api.patch_namespaced_custom_object(
            group="redis.redis.opstreelabs.in", version="v1beta1", namespace="redis-cluster", plural="redisclusters", name="redis-master",
            body={"spec": {"clusterSize": size}}
        )
        log(f"Scaled RedisCluster to {size} nodes")
    except Exception as e:
        log(f"Failed to scale: {e}")


def main():
    log("Starting In-Cluster Redis Autoscaler Controller...")
    # Authenticates securely using the Pod's mounted ServiceAccount token
    config.load_incluster_config()
    api = client.CustomObjectsApi()
    
    cooldown = 0
    while True:
        rps = get_rps()
        size = get_cluster_size(api)
        log(f"Telemetry: {rps:.2f} RPS | Active Cluster Size: {size}")

        if rps > THRESHOLD:
            cooldown = 0
            if size < 4:
                log(f"Threshold breached ({rps} > {THRESHOLD} RPS). Scaling up.")
                patch_cluster(api, 4)
        else:
            if size > 3:
                cooldown += 10
                if cooldown >= COOLDOWN_SEC:
                    log("Cooldown complete. Scaling down to 3 nodes.")
                    patch_cluster(api, 3)
                    cooldown = 0
        time.sleep(10)


if __name__ == "__main__":
    main()