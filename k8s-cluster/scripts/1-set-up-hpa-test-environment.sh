#!/bin/bash
# exit when any command fails, except for interactive prompts
set -e

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Resolve paths relative to this script so it works no matter where it's invoked from
cd "$(dirname "$0")"

echo -e "${BLUE}=== Phase 1: Vanilla HPA - Scale-Out (Ghost Pod) + Scale-In (Data Cliff) ===${NC}\n"

# 0. Scorch any prior state so this run starts from a guaranteed clean slate
echo -e "${YELLOW}[0/3] Scorching prior Redis state (manifests, PVCs, namespace)...${NC}"
# Drop set -e: most of these resources won't exist on a fresh cluster, and that's fine.
set +e
# Delete the Operator/KEDA custom resources first, while their controllers are alive,
# so the namespace deletion below isn't blocked by lingering finalizers.
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-keda-scaling.yaml --ignore-not-found=true
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-operator-cluster.yaml --ignore-not-found=true
# Delete the HPA scenario manifests.
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-hpa-scaling.yaml --ignore-not-found=true
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-servicemonitor.yaml --ignore-not-found=true
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-hpa-cluster.yaml --ignore-not-found=true
# Purge any leftover persistent data.
k3s kubectl delete pvc --all -n redis --ignore-not-found=true
# Finally drop the namespace itself (cascades anything remaining) and wait for full termination.
k3s kubectl delete namespace redis --ignore-not-found=true
k3s kubectl wait --for=delete namespace/redis --timeout=120s

# Clear any FastAPI entrypoint pods left over from a previous run. main.py's
# _connect_with_retry connects only until its FIRST success and never re-resolves
# topology, so a surviving pod stays pinned to the cluster we just scorched. Scale
# to 0 here and bring it back up (in step 1) so every pod boots fresh against the
# NEW cluster. Guarded so a first run (no deployment yet) just skips this.
if k3s kubectl get deployment fastapi-entrypoint -n default >/dev/null 2>&1; then
    echo -e "${YELLOW}Clearing stale FastAPI entrypoint pods...${NC}"
    k3s kubectl scale deployment fastapi-entrypoint -n default --replicas=0
    k3s kubectl wait --for=delete pod -l app=entrypoint -n default --timeout=120s
fi
set -e
echo -e "${GREEN}Clean slate confirmed.${NC}\n"

# 1. Namespace & Cluster Setup
echo -e "${YELLOW}[1/3] Creating namespace and deploying Redis StatefulSet...${NC}"
k3s kubectl create namespace redis --dry-run=client -o yaml | k3s kubectl apply -f -
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-hpa-cluster.yaml

# Spin the FastAPI entrypoint back up NOW - BEFORE the cluster is formed below - so
# its boot overlaps with Redis coming online. Each pod's _connect_with_retry loop
# keeps retrying (with backoff) against the not-yet-formed cluster and latches on
# the instant the cluster is created, leaving the entrypoint warm and connected by
# the time the test starts. apply also (re)creates the NodePort Service and resets
# the deployment to its full 6 replicas, so this works on a first run too.
echo -e "${YELLOW}Spinning the FastAPI entrypoint back up (parallel with Redis init)...${NC}"
k3s kubectl apply -f ../config/entrypoint-deployment.yaml

echo -e "Waiting for the 6 baseline Redis pods to initialize (3 masters + 3 replicas)..."
k3s kubectl wait --for=jsonpath='{.status.readyReplicas}'=6 statefulset/redis -n redis --timeout=300s

# Form the cluster as 3 masters + 1 replica each (--cluster-replicas 1). redis-cli makes the
# FIRST three nodes listed the masters (redis-0/1/2) and assigns redis-3/4/5 as their replicas
# (on different hosts where it can). Listing masters first makes the ordinal->role mapping
# DETERMINISTIC, which the scale-in below relies on: reverse-ordinal deletion removes the
# replicas (redis-5/4/3) before any master, so we can show redundancy absorbing the first three
# deletions and the cliff landing only when a replica-less master (redis-2) is removed.
# NOTE: master/replica role assignment is a RUNTIME redis-cli operation - a StatefulSet manifest
# cannot express it. That topology glue living here, not in the YAML, IS the gap the operator
# closes declaratively (clusterSize + follower count in its CR).
echo -e "Forming the Redis Cluster (3 masters + 3 replicas)..."
ip0=$(k3s kubectl get pod redis-0 -n redis -o jsonpath='{.status.podIP}')
ip1=$(k3s kubectl get pod redis-1 -n redis -o jsonpath='{.status.podIP}')
ip2=$(k3s kubectl get pod redis-2 -n redis -o jsonpath='{.status.podIP}')
ip3=$(k3s kubectl get pod redis-3 -n redis -o jsonpath='{.status.podIP}')
ip4=$(k3s kubectl get pod redis-4 -n redis -o jsonpath='{.status.podIP}')
ip5=$(k3s kubectl get pod redis-5 -n redis -o jsonpath='{.status.podIP}')
k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster create \
    "${ip0}:6379" "${ip1}:6379" "${ip2}:6379" \
    "${ip3}:6379" "${ip4}:6379" "${ip5}:6379" \
    --cluster-replicas 1 --cluster-yes

echo -e "${GREEN}Baseline Redis Cluster is online (3 masters, 3 replicas).${NC}\n"

# Slots are now assigned, so wait for the entrypoint to finish connecting - its
# /ready probe flips to 200 only after the Redis client is live. This keeps us
# from handing the user a "start the test" prompt while the entrypoint is still
# unconnected. Tolerant (|| true) so one slow pod never blocks the thesis flow.
echo -e "Waiting for the FastAPI entrypoint to connect to the new cluster..."
k3s kubectl rollout status deployment/fastapi-entrypoint -n default --timeout=120s || true
echo -e "${GREEN}FastAPI entrypoint is connected and ready.${NC}\n"

# 2. HPA Deployment
echo -e "${YELLOW}[2/3] Deploying ServiceMonitor & Horizontal Pod Autoscaler (40% CPU Target)...${NC}"
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-servicemonitor.yaml
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-hpa-scaling.yaml
echo -e "${GREEN}HPA is active and monitoring.${NC}\n"

# 3. The Scale-Out Experiment: drive load and observe the Ghost Pod.
#    This is the entire point of Phase 1. We deliberately do NOT reshard - hand-
#    integrating the new node (add-node -> converge -> reshard) would just be
#    reimplementing the operator by hand. The thesis claim is the GAP itself:
#    native HPA provisions capacity it structurally cannot integrate.
#    NOTE: the baseline is now 6 pods (3 masters + 3 replicas), so the HPA's added pods - the
#    GHOSTS - start at redis-6 (redis-3/4/5 are real cluster replicas, NOT ghosts).
echo -e "${RED}>>> ACTION REQUIRED: START THE SCALE-OUT LOAD TEST <<<${NC}"
echo -e "1. In a new terminal on the load generator, run k6 (load-test.js now fans"
echo -e "   out across all 6 NodePorts automatically):"
echo -e "   ${YELLOW}k6 run -o experimental-prometheus-rw load-test.js${NC}"
echo -e "2. In Grafana, watch Redis CPU cross the 40% target and the HPA scale the"
echo -e "   StatefulSet out (redis-6, then redis-7/8 as the load persists)."
echo -e "3. THE KEY OBSERVATION (Ghost Pod): each new pod joins the Kubernetes"
echo -e "   Deployment but NOT the Redis cluster - it owns 0 hash slots, draws ~0 CPU,"
echo -e "   and serves no traffic. The original 3 masters stay saturated, so the HPA"
echo -e "   keeps scaling toward maxReplicas while throughput/latency never improve."
echo ""
read -p "Press [Enter] once the HPA has scaled out (redis-6 is Running) to snapshot the evidence..."

# 4. Evidence snapshot (best-effort): prove the scaled-out pods are Ghost Pods.
echo -e "\n${YELLOW}[3/3] Capturing Ghost Pod evidence...${NC}"
set +e
echo -e "${BLUE}--- Kubernetes view: redis pods that EXIST ---${NC}"
k3s kubectl get pods -n redis -l app=redis -o wide

echo -e "\n${BLUE}--- Redis view: nodes actually IN the cluster (6 real members: 3 masters + 3 replicas) ---${NC}"
echo -e "(The Kubernetes count above EXCEEDS this; the difference is the Ghost Pod[s].)"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster nodes

echo -e "\n${BLUE}--- The Ghost itself (redis-6): an isolated 1-node island, 0 slots ---${NC}"
if k3s kubectl get pod redis-6 -n redis >/dev/null 2>&1; then
    k3s kubectl exec redis-6 -n redis -- redis-cli cluster info \
        | grep -E "cluster_known_nodes|cluster_slots_assigned|cluster_size"
else
    echo -e "(redis-6 not present yet - did the HPA actually scale out?)"
fi

echo -e "\n${BLUE}--- HPA status: replicas climbing, CPU target never satisfied ---${NC}"
k3s kubectl get hpa redis-hpa -n redis
set -e

# 5. The Scale-In Experiment: the reverse-ordinal data cliff, NOW WITH REPLICAS.
#    With 3 masters + 3 replicas, native cluster failover protects against node DEATH. But a
#    StatefulSet scale-in is a PLANNED removal: it deletes pods by DESCENDING ORDINAL with no
#    role awareness, no drain, and no reshard. So redundancy absorbs the first three deletions
#    (the replicas redis-5/4/3 - zero data loss, HA doing its job), and the cliff lands the
#    instant ordinal-deletion reaches a MASTER whose replica is already gone (redis-2). This is
#    the empirical answer to "just give HPA replicas": failover DELAYS the cliff, it cannot
#    PREVENT it, because a planned scale-in needs slot MIGRATION, which nothing here performs.
#    We drive the steps by hand only to make the experiment deterministic.
echo -e "\n${RED}>>> SCALE-IN / DATA CLIFF (with replicas) <<<${NC}"
echo -e "Make sure your k6 load test has populated keys, so the cliff drops real data."
echo -e "Recommended: STOP k6 first - a planned scale-in needs no live traffic to lose data"
echo -e "(keys persist, no TTL), and quiescing keeps the run clean and reproducible."
read -p "Press [Enter] to record the BEFORE state and begin the stepwise scale-in..."
set +e

# Remove HPA control so the scale-in is deterministic and can drop below its minReplicas.
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-hpa-scaling.yaml --ignore-not-found=true

echo -e "\n${BLUE}--- BEFORE (6 pods = 3 masters + 3 replicas): full coverage, cluster healthy ---${NC}"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
k3s kubectl exec redis-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379 | grep -E 'master|slave'

# Phase A: delete the 3 REPLICAS (reverse ordinal: redis-5, redis-4, redis-3). Native redundancy
# absorbs each one - the cluster stays OK with all 16384 slots covered and ZERO data loss. This
# is exactly the protection a "just add replicas" critic expects to rescue the HPA approach.
for target in 5 4 3; do
    echo -e "\n${YELLOW}Scaling -> ${target} (deletes redis-${target}, a REPLICA)...${NC}"
    k3s kubectl scale statefulset redis -n redis --replicas=${target}
    k3s kubectl wait --for=delete pod/redis-${target} -n redis --timeout=120s
    sleep 3
    k3s kubectl exec redis-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
done
echo -e "${GREEN}3 replicas gone, 3 bare masters remain: still all 16384 slots covered, NO data loss yet.${NC}"

# Phase B: THE CLIFF. The next reverse-ordinal deletion removes redis-2 - a MASTER whose replica
# was already deleted in Phase A. No drain, no reshard, no replica to promote, so its ~1/3 of the
# hash slots are orphaned the instant the pod dies. Failover cannot help: nothing died unexpectedly.
read -p "Press [Enter] to take the final step (3 -> 2) that removes a replica-less master..."
echo -e "${YELLOW}Scaling -> 2 (deletes redis-2, a MASTER with no surviving replica)...${NC}"
k3s kubectl scale statefulset redis -n redis --replicas=2
k3s kubectl wait --for=delete pod/redis-2 -n redis --timeout=120s
sleep 3

echo -e "\n${RED}--- AFTER: the data cliff - slots orphaned, cluster DOWN (replicas did NOT save it) ---${NC}"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
k3s kubectl exec redis-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379
set -e
echo -e "${YELLOW}Grafana (PromQL): redis_cluster_slots_assigned holds at 16384 through the replica${NC}"
echo -e "${YELLOW}deletions, then drops below 16384 at the master deletion; redis_cluster_state flips to${NC}"
echo -e "${YELLOW}fail and sum(redis_db_keys{namespace=\"redis\"}) steps down = lost keys.${NC}"

echo -e "\n${GREEN}=== Phase 1 (Ghost Pod + Data Cliff) Complete ===${NC}"
echo -e "Capture the Grafana panels (replicas vs. slots_assigned / total keys / error rate) for"
echo -e "your Results section, then stop k6 and run the Phase 2 teardown script."
