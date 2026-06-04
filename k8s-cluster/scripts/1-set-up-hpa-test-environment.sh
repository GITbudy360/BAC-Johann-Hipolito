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

echo -e "${BLUE}=== Phase 1: Vanilla HPA Scenario Setup ===${NC}\n"

# 0. Scorch any prior state so this run starts from a guaranteed clean slate
echo -e "${YELLOW}[0/4] Scorching prior Redis state (manifests, PVCs, namespace)...${NC}"
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
echo -e "${YELLOW}[1/4] Creating namespace and deploying Redis StatefulSet...${NC}"
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
echo -e "${YELLOW}[2/4] Deploying ServiceMonitor & Horizontal Pod Autoscaler (40% CPU Target)...${NC}"
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-servicemonitor.yaml
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-hpa-scaling.yaml
echo -e "${GREEN}HPA is active and monitoring.${NC}\n"

# 3. The Scale-Out Phase & Ghost Pod Observation
echo -e "${RED}>>> ACTION REQUIRED: START SCALE-OUT TEST <<<${NC}"
echo -e "1. Open a new terminal and run your k6 test using the provided script:"
echo -e "   ${YELLOW}k6 run -e API_URL=http://<YOUR_ENTRYPOINT_IP>:30080 load-test.js${NC}"
echo -e "2. Watch your Grafana dashboard. Wait for the HPA to spawn 'redis-3'."
echo -e "3. Observe the 'Ghost Pod' phenomenon (0 CPU, 0 Hash Slots)."
read -p "Press [Enter] ONLY AFTER 'redis-3' is running to proceed with manual intervention..."

# 4. Manual Intervention: graft the Ghost Pod into the cluster, THEN reshard onto it.
echo -e "\n${YELLOW}[3/4] Executing Manual Resharding Intervention...${NC}"
# Tolerant block: a missing pod or a user Ctrl-C shouldn't abort the whole script.
set +e

NEW_NODE="redis-3"   # the pod the HPA scaled out; repeat this block for redis-4/5 if more appeared

# 4a. JOIN the node. The StatefulSet started redis-3, but a vanilla Redis node never
#     issues CLUSTER MEET on its own, so it sits isolated with 0 slots and no ID the
#     cluster recognizes - the "Ghost Pod". add-node grafts it in as a master. Without
#     this the reshard has no valid receiving node: the prompt wants a 40-char node ID,
#     so typing the pod name 'redis-3' is rejected ("node is not known or not a master").
new_ip=$(k3s kubectl get pod "${NEW_NODE}" -n redis -o jsonpath='{.status.podIP}' 2>/dev/null)
anchor_ip=$(k3s kubectl get pod redis-0 -n redis -o jsonpath='{.status.podIP}')

if [[ -z "${new_ip}" ]]; then
    echo -e "${RED}Could not find pod ${NEW_NODE}. Did the scale-out happen? Skipping intervention.${NC}"
else
    # Idempotent: only MEET the node if the cluster doesn't already know its IP.
    if k3s kubectl exec redis-0 -n redis -- redis-cli cluster nodes | grep -q "${new_ip}:6379"; then
        echo -e "${GREEN}${NEW_NODE} (${new_ip}) is already a cluster member; skipping add-node.${NC}"
    else
        echo -e "Joining Ghost Pod ${NEW_NODE} (${new_ip}) into the cluster as a master..."
        k3s kubectl exec -it redis-0 -n redis -- \
            redis-cli --cluster add-node "${new_ip}:6379" "${anchor_ip}:6379"
    fi

    # 4b. Resolve the node's real 40-char cluster ID (NOT the pod name). Note: no -t on
    #     this exec - a TTY would append \r and corrupt the captured ID.
    new_id=$(k3s kubectl exec redis-0 -n redis -- redis-cli cluster nodes \
        | grep "${new_ip}:6379" | awk '{print $1}')

    if [[ -z "${new_id}" ]]; then
        echo -e "${RED}Could not resolve ${NEW_NODE}'s node ID after add-node. Skipping reshard.${NC}"
    else
        # Move ~4096 slots (16384 / 4 masters) onto the new node, taken evenly from all
        # existing masters. Non-interactive (--cluster-yes) so there are no prompts.
        echo -e "Receiving node: ${NEW_NODE} -> ID ${YELLOW}${new_id}${NC}"
        echo -e "Resharding 4096 slots from all masters onto ${NEW_NODE}..."
        k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster reshard 127.0.0.1:6379 \
            --cluster-from all \
            --cluster-to "${new_id}" \
            --cluster-slots 4096 \
            --cluster-yes
        echo -e "${GREEN}Reshard complete. Verify with:${NC}"
        echo -e "   k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379"
    fi
fi

set -e

# 5. The Scale-In Phase & Data Cliff Observation
echo -e "\n${RED}>>> ACTION REQUIRED: START SCALE-IN TEST <<<${NC}"
echo -e "1. Stop your k6 load test in the other terminal."
echo -e "2. Keep an eye on Grafana. The HPA has a 5-minute stabilization window."
echo -e "3. Watch for 'redis-3' to be terminated and observe the HTTP 500 errors and Data Cliff."
read -p "Press [Enter] once you have captured the failure metrics in Grafana to finish Phase 1..."

echo -e "\n${GREEN}Phase 1 Complete. You are ready to run the Phase 2 teardown script.${NC}"