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

echo -e "Waiting for the 3 baseline Redis pods to initialize..."
k3s kubectl wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/redis -n redis --timeout=300s

# Form the cluster (3 masters splitting all 16384 slots) using the pods' IPs.
echo -e "Forming the Redis Cluster..."
ip0=$(k3s kubectl get pod redis-0 -n redis -o jsonpath='{.status.podIP}')
ip1=$(k3s kubectl get pod redis-1 -n redis -o jsonpath='{.status.podIP}')
ip2=$(k3s kubectl get pod redis-2 -n redis -o jsonpath='{.status.podIP}')
k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster create "${ip0}:6379" "${ip1}:6379" "${ip2}:6379" --cluster-yes

echo -e "${GREEN}Baseline Redis Cluster is online.${NC}\n"

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
echo -e "${RED}>>> ACTION REQUIRED: START THE SCALE-OUT LOAD TEST <<<${NC}"
echo -e "1. In a new terminal on the load generator, run k6 (load-test.js now fans"
echo -e "   out across all 6 NodePorts automatically):"
echo -e "   ${YELLOW}k6 run -o experimental-prometheus-rw load-test.js${NC}"
echo -e "2. In Grafana, watch Redis CPU cross the 40% target and the HPA scale the"
echo -e "   StatefulSet out (redis-3, then redis-4/5 as the load persists)."
echo -e "3. THE KEY OBSERVATION (Ghost Pod): each new pod joins the Kubernetes"
echo -e "   Deployment but NOT the Redis cluster - it owns 0 hash slots, draws ~0 CPU,"
echo -e "   and serves no traffic. The original 3 masters stay saturated, so the HPA"
echo -e "   keeps scaling toward maxReplicas while throughput/latency never improve."
echo ""
read -p "Press [Enter] once the HPA has scaled out (redis-3 is Running) to snapshot the evidence..."

# 4. Evidence snapshot (best-effort): prove the scaled-out pods are Ghost Pods.
echo -e "\n${YELLOW}[3/3] Capturing Ghost Pod evidence...${NC}"
set +e
echo -e "${BLUE}--- Kubernetes view: redis pods that EXIST ---${NC}"
k3s kubectl get pods -n redis -l app=redis -o wide

echo -e "\n${BLUE}--- Redis view: nodes actually IN the cluster ---${NC}"
echo -e "(The Kubernetes count above EXCEEDS this; the difference is the Ghost Pod[s].)"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster nodes

echo -e "\n${BLUE}--- The Ghost itself (redis-3): an isolated 1-node island, 0 slots ---${NC}"
if k3s kubectl get pod redis-3 -n redis >/dev/null 2>&1; then
    k3s kubectl exec redis-3 -n redis -- redis-cli cluster info \
        | grep -E "cluster_known_nodes|cluster_slots_assigned|cluster_size"
else
    echo -e "(redis-3 not present yet - did the HPA actually scale out?)"
fi

echo -e "\n${BLUE}--- HPA status: replicas climbing, CPU target never satisfied ---${NC}"
k3s kubectl get hpa redis-hpa -n redis
set -e

# 5. The Scale-In Experiment: the reverse-ordinal data cliff.
#    The cliff only appears when a SLOT-OWNING master is removed - HPA's scaled-out pods
#    are ghosts (0 slots), so removing those loses nothing. We instead scale the StatefulSet
#    down to 2, which (reverse-ordinal) deletes redis-2 - an ORIGINAL master owning ~1/3 of
#    the hash slots - with NO drain and no replica. That is the exact StatefulSet mechanism
#    HPA scale-in uses; we drive it by hand only to make the experiment deterministic.
echo -e "\n${RED}>>> SCALE-IN / DATA CLIFF <<<${NC}"
echo -e "Make sure your k6 load test has populated keys, so the cliff drops real data."
read -p "Press [Enter] to record the BEFORE state and trigger the scale-in..."
set +e

echo -e "\n${BLUE}--- BEFORE: full slot coverage, cluster healthy ---${NC}"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'

# Remove HPA control so the scale-in is deterministic and can drop below its minReplicas.
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-hpa-scaling.yaml --ignore-not-found=true

echo -e "${YELLOW}Scaling the StatefulSet 3 -> 2 (deletes redis-2, a slot-owning master)...${NC}"
k3s kubectl scale statefulset redis -n redis --replicas=2
k3s kubectl wait --for=delete pod/redis-2 -n redis --timeout=120s

echo -e "\n${RED}--- AFTER: the data cliff - slots orphaned, cluster DOWN ---${NC}"
k3s kubectl exec redis-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
k3s kubectl exec redis-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379
set -e
echo -e "${YELLOW}Grafana (PromQL): redis_cluster_slots_assigned drops below 16384, redis_cluster_state${NC}"
echo -e "${YELLOW}flips to fail, and sum(redis_db_keys{namespace=\"redis\"}) steps down = lost keys.${NC}"

echo -e "\n${GREEN}=== Phase 1 (Ghost Pod + Data Cliff) Complete ===${NC}"
echo -e "Capture the Grafana panels (replicas vs. slots_assigned / total keys / error rate) for"
echo -e "your Results section, then stop k6 and run the Phase 2 teardown script."
