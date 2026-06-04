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
set -e
echo -e "${GREEN}Clean slate confirmed.${NC}\n"

# 1. Namespace & Cluster Setup
echo -e "${YELLOW}[1/4] Creating namespace and deploying Redis StatefulSet...${NC}"
k3s kubectl create namespace redis --dry-run=client -o yaml | k3s kubectl apply -f -
k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-hpa-cluster.yaml

echo -e "Waiting for the 3 baseline Redis pods to initialize..."
k3s kubectl wait --for=jsonpath='{.status.readyReplicas}'=3 statefulset/redis -n redis --timeout=300s
echo -e "${GREEN}Baseline Redis Cluster is online.${NC}\n"

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

# 4. Manual Intervention
echo -e "\n${YELLOW}[3/4] Executing Manual Resharding Intervention...${NC}"
echo -e "You will now enter the interactive Redis Cluster resharding prompt."
echo -e "You need to move ~4096 slots to the new node."
echo -e "Running command: k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster reshard 127.0.0.1:6379"
# Dropping the set -e temporarily so a user cancelling the reshard doesn't break the script
set +e 
k3s kubectl exec -it redis-0 -n redis -- redis-cli --cluster reshard 127.0.0.1:6379
set -e

# 5. The Scale-In Phase & Data Cliff Observation
echo -e "\n${RED}>>> ACTION REQUIRED: START SCALE-IN TEST <<<${NC}"
echo -e "1. Stop your k6 load test in the other terminal."
echo -e "2. Keep an eye on Grafana. The HPA has a 5-minute stabilization window."
echo -e "3. Watch for 'redis-3' to be terminated and observe the HTTP 500 errors and Data Cliff."
read -p "Press [Enter] once you have captured the failure metrics in Grafana to finish Phase 1..."

echo -e "\n${GREEN}Phase 1 Complete. You are ready to run the Phase 2 teardown script.${NC}"