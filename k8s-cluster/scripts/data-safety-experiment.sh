#!/bin/bash
# Data-safety experiment: does a single, QUIESCENT scale-in preserve all 16384 slots and all
# keys? Run once per scenario, then compare the BEFORE/AFTER tables.
#
#   Usage:  ./data-safety-experiment.sh [hpa|operator]
#   Env:    KEYS=5000  START_MASTERS=4  END_MASTERS=3
#
# This is the MINIMAL, bulletproof proof. It deliberately uses NO autoscaler, NO FastAPI
# entrypoint and NO k6 - keys are written directly with redis-cli, so the run is quiescent by
# construction (zero workload traffic) and the result is deterministic. The single variable is
# the scaling MECHANISM:
#   * hpa      -> remove the highest-ordinal pod (the exact action HPA performs). No reshard.
#   * operator -> lower clusterSize (the operator drains the master FIRST, then removes it).
# The thesis metric is DATA LOSS (keys lost / slots permanently uncovered), NOT whether the
# transition was perfectly autonomous.
set -e

GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
cd "$(dirname "$0")"
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

SCENARIO="${1:-}"
KEYS="${KEYS:-5000}"            # exact number of keys to populate
START_MASTERS="${START_MASTERS:-4}"
END_MASTERS="${END_MASTERS:-3}"

if [[ "$SCENARIO" != "hpa" && "$SCENARIO" != "operator" ]]; then
    echo -e "${RED}Usage: $0 [hpa|operator]   (env: KEYS, START_MASTERS, END_MASTERS)${NC}"
    exit 1
fi
# Leader-0 pod name differs per scenario; every redis-cli call execs through it (ordinal 0 is
# never removed, so it survives the scale-in).
if [[ "$SCENARIO" == "hpa" ]]; then LEAD=redis-0; else LEAD=redis-cluster-leader-0; fi

echo -e "${BLUE}=== Data-Safety Experiment: ${SCENARIO} (${START_MASTERS} -> ${END_MASTERS} masters, ${KEYS} keys) ===${NC}\n"

# --- helper: read authoritative cluster state from leader-0 -> SNAP_STATE/SNAP_SLOTS/SNAP_KEYS
snapshot() {
    local info chk
    info=$(k3s kubectl exec "$LEAD" -n redis -- redis-cli cluster info 2>/dev/null | tr -d '\r')
    chk=$(k3s kubectl exec "$LEAD" -n redis -- redis-cli --cluster check 127.0.0.1:6379 2>/dev/null)
    SNAP_STATE=$(echo "$info" | awk -F: '/^cluster_state:/{print $2}')
    SNAP_SLOTS=$(echo "$info" | awk -F: '/^cluster_slots_ok:/{print $2}')
    SNAP_KEYS=$(echo "$chk" | grep -oE '[0-9]+ keys in' | grep -oE '^[0-9]+' | head -1)
}

# 1. Clean slate.
echo -e "${YELLOW}[1/5] Scorching the redis namespace...${NC}"
set +e
helm uninstall redis-cluster -n redis >/dev/null 2>&1
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-hpa-cluster.yaml --ignore-not-found=true >/dev/null 2>&1
k3s kubectl delete -f ../config/redis/scaling-scenario-hpa/redis-servicemonitor.yaml --ignore-not-found=true >/dev/null 2>&1
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-servicemonitor.yaml --ignore-not-found=true >/dev/null 2>&1
k3s kubectl delete pvc --all -n redis --ignore-not-found=true >/dev/null 2>&1
k3s kubectl delete namespace redis --ignore-not-found=true
k3s kubectl wait --for=delete namespace/redis --timeout=120s
set -e
k3s kubectl create namespace redis --dry-run=client -o yaml | k3s kubectl apply -f -

# 2. Deploy the data tier (NO autoscaler, NO entrypoint, NO load).
echo -e "${YELLOW}[2/5] Deploying the ${SCENARIO} data tier...${NC}"
if [[ "$SCENARIO" == "hpa" ]]; then
    # Self-managed cluster: StatefulSet + exporter, then hand-form START_MASTERS masters (no replicas).
    k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-hpa-cluster.yaml
    k3s kubectl scale statefulset redis -n redis --replicas="${START_MASTERS}"
    k3s kubectl rollout status statefulset redis -n redis --timeout=300s
    k3s kubectl apply -f ../config/redis/scaling-scenario-hpa/redis-servicemonitor.yaml
    echo -e "Forming a ${START_MASTERS}-master cluster (no replicas)..."
    ips=()
    for i in $(seq 0 $((START_MASTERS-1))); do
        ips+=("$(k3s kubectl get pod redis-$i -n redis -o jsonpath='{.status.podIP}'):6379")
    done
    k3s kubectl exec redis-0 -n redis -- redis-cli --cluster create "${ips[@]}" --cluster-yes
else
    # Operator-managed cluster: deploy DIRECTLY at START_MASTERS (clean initial formation avoids
    # the flaky incremental scale-out reshard). No KEDA - we drive clusterSize by hand.
    helm repo add ot-helm https://ot-container-kit.github.io/helm-charts/ >/dev/null 2>&1 || true
    helm repo update ot-helm >/dev/null
    helm upgrade --install redis-cluster ot-helm/redis-cluster -n redis \
        -f ../config/redis/scaling-scenario-operator-keda/redis-operator-cluster-values.yaml \
        --set redisCluster.clusterSize="${START_MASTERS}"
    k3s kubectl apply -f ../config/redis/scaling-scenario-operator-keda/redis-servicemonitor.yaml
    echo -e "Waiting for the operator to form the cluster..."
    for _ in $(seq 1 60); do
        k3s kubectl get statefulset redis-cluster-leader -n redis >/dev/null 2>&1 && break; sleep 2
    done
    k3s kubectl rollout status statefulset redis-cluster-leader -n redis --timeout=300s
    k3s kubectl rollout status statefulset redis-cluster-follower -n redis --timeout=300s 2>/dev/null || true
fi

# Wait until the cluster reports healthy (all 16384 slots) before populating.
echo -e "Waiting for the cluster to report healthy (cluster_state:ok, all 16384 slots)..."
for _ in $(seq 1 60); do
    snapshot
    [[ "$SNAP_STATE" == "ok" && "$SNAP_SLOTS" == "16384" ]] && break
    sleep 5
done
echo -e "${GREEN}Cluster online: state=${SNAP_STATE} slots_ok=${SNAP_SLOTS}.${NC}\n"

# 3. Populate a known, exact dataset (deterministic; spread across all slots, no hash tags).
echo -e "${YELLOW}[3/5] Populating ${KEYS} keys via redis-cli (quiescent - this is the only writer)...${NC}"
k3s kubectl exec "$LEAD" -n redis -- sh -c \
    "for i in \$(seq 1 ${KEYS}); do echo set key:\$i \$i; done | redis-cli -c -h 127.0.0.1 >/dev/null"

snapshot
BEFORE_STATE="$SNAP_STATE"; BEFORE_SLOTS="$SNAP_SLOTS"; BEFORE_KEYS="${SNAP_KEYS:-0}"
echo -e "${BLUE}BEFORE: state=${BEFORE_STATE} slots_ok=${BEFORE_SLOTS} keys=${BEFORE_KEYS}${NC}\n"

# 4. The scale-in. From here on, don't abort on a wedge - capture and report it.
set +e
echo -e "${YELLOW}[4/5] Scaling in ${START_MASTERS} -> ${END_MASTERS} (removing one slot-owning master)...${NC}"
if [[ "$SCENARIO" == "hpa" ]]; then
    # The EXACT action HPA performs on scale-down: drop the highest-ordinal pod. No drain, no reshard.
    k3s kubectl scale statefulset redis -n redis --replicas="${END_MASTERS}"
    k3s kubectl wait --for=delete pod/redis-"${END_MASTERS}" -n redis --timeout=120s
else
    # The operator's mechanism: lower clusterSize; it migrates the departing master's slots to the
    # survivors FIRST, then removes the pod.
    k3s kubectl patch rediscluster redis-cluster -n redis --type merge \
        -p "{\"spec\":{\"clusterSize\":${END_MASTERS}}}"
    k3s kubectl wait --for=delete pod/redis-cluster-leader-"${END_MASTERS}" -n redis --timeout=600s
fi
sleep 5
snapshot
AFTER_STATE="$SNAP_STATE"; AFTER_SLOTS="$SNAP_SLOTS"; AFTER_KEYS="${SNAP_KEYS:-0}"

# Read probe: with cluster-require-full-coverage (the default), ANY uncovered slot makes the WHOLE
# cluster reject reads with CLUSTERDOWN. So a healthy cluster serves all; a cliffed one serves none.
errs=0
for k in 1 1000 2500 4000 4999; do
    out=$(k3s kubectl exec "$LEAD" -n redis -- redis-cli -c -h 127.0.0.1 get "key:$k" 2>&1)
    echo "$out" | grep -qiE "CLUSTERDOWN|error|not served" && errs=$((errs+1))
done

# 5. Report.
echo -e "\n${BLUE}===================== DATA-SAFETY RESULT (${SCENARIO}) =====================${NC}"
printf "%-10s %-10s %-12s %-8s\n" "" "state" "slots_ok" "keys"
printf "%-10s %-10s %-12s %-8s\n" "BEFORE" "${BEFORE_STATE}" "${BEFORE_SLOTS}" "${BEFORE_KEYS}"
printf "%-10s %-10s %-12s %-8s\n" "AFTER"  "${AFTER_STATE}"  "${AFTER_SLOTS}"  "${AFTER_KEYS}"
lost=$(( ${BEFORE_KEYS:-0} - ${AFTER_KEYS:-0} ))
echo -e "Read probe after scale-in: ${errs}/5 keys returned a cluster error (CLUSTERDOWN)."
echo ""
if [[ "$AFTER_STATE" == "ok" && "$AFTER_SLOTS" == "16384" && "$lost" -le 0 ]]; then
    echo -e "${GREEN}VERDICT: GRACEFUL scale-in - all 16384 slots covered, 0 keys lost.${NC}"
else
    echo -e "${RED}VERDICT: DATA CLIFF - state=${AFTER_STATE} slots_ok=${AFTER_SLOTS}, ${lost} keys lost.${NC}"
fi

# Distinguish a recoverable operator STALL (no data lost) from a true cliff.
if [[ "$SCENARIO" == "operator" ]] && k3s kubectl get pod redis-cluster-leader-"${END_MASTERS}" -n redis >/dev/null 2>&1; then
    echo -e "${YELLOW}NOTE: leader-${END_MASTERS} is still present - the drain STALLED (open slot). Data is NOT lost"
    echo -e "      (slots are mid-migration, recoverable). Complete it with:"
    echo -e "      ${YELLOW}k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli --cluster fix 127.0.0.1:6379${NC}"
fi
set -e

echo -e "\n${GREEN}Done. The cluster is left in its post-scale-in state for Grafana capture.${NC}"
echo -e "Import grafana/data-safety-dashboard.json and screenshot the panels for this run, then"
echo -e "run the other scenario: ./data-safety-experiment.sh $([[ "$SCENARIO" == hpa ]] && echo operator || echo hpa)"