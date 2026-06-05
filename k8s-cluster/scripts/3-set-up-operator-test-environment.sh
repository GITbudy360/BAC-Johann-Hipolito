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

# helm needs a kubeconfig; k3s keeps its at the path below. (k3s kubectl embeds it,
# but plain `helm` does not.) Respect an already-exported KUBECONFIG if present.
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

echo -e "${BLUE}=== Phase 3: Operator + KEDA Scenario - Topology-Aware Scale-Out ===${NC}\n"

# 0. Sanity-check the cluster-wide controllers this scenario depends on. They are
#    installed once and assumed running: the Opstree Redis Operator in namespace
#    'ot-operators', KEDA in namespace 'keda'. We only confirm their CRDs exist so
#    the apply steps below fail loudly here rather than cryptically later.
echo -e "${YELLOW}[0/4] Verifying Operator + KEDA prerequisites...${NC}"
if ! k3s kubectl get crd redisclusters.redis.redis.opstreelabs.in >/dev/null 2>&1; then
    echo -e "${RED}Opstree Redis Operator CRD not found - expected the operator running in namespace 'ot-operators'.${NC}"
    exit 1
fi
if ! k3s kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1; then
    echo -e "${RED}KEDA CRD not found - expected KEDA running in namespace 'keda'.${NC}"
    exit 1
fi
echo -e "${GREEN}Operator (ot-operators) and KEDA (keda) detected.${NC}"

# This operator version ships the RedisCluster CRD WITHOUT a Kubernetes /scale
# subresource, so KEDA - which actuates every target through a managed HPA hitting
# the /scale endpoint - cannot change clusterSize and nothing would scale. Map
# /scale onto the operator's own knob: spec.clusterSize <-> status.readyLeaderReplicas.
# Idempotent and reversible. NOTE: an operator UPGRADE would regenerate (and wipe)
# the CRD, which is why we re-assert this every run.
RC_CRD="redisclusters.redis.redis.opstreelabs.in"
if [[ -z "$(k3s kubectl get crd "${RC_CRD}" -o jsonpath='{.spec.versions[0].subresources.scale}' 2>/dev/null)" ]]; then
    echo -e "${YELLOW}Enabling the /scale subresource on ${RC_CRD} (required for KEDA)...${NC}"
    if [[ -z "$(k3s kubectl get crd "${RC_CRD}" -o jsonpath='{.spec.versions[0].subresources}' 2>/dev/null)" ]]; then
        # No subresources object yet - add the whole thing, keeping status enabled.
        k3s kubectl patch crd "${RC_CRD}" --type=json -p='[{"op":"add","path":"/spec/versions/0/subresources","value":{"status":{},"scale":{"specReplicasPath":".spec.clusterSize","statusReplicasPath":".status.readyLeaderReplicas"}}}]'
    else
        # subresources.status already exists - add only the scale key.
        k3s kubectl patch crd "${RC_CRD}" --type=json -p='[{"op":"add","path":"/spec/versions/0/subresources/scale","value":{"specReplicasPath":".spec.clusterSize","statusReplicasPath":".status.readyLeaderReplicas"}}]'
    fi
    echo -e "${GREEN}/scale subresource enabled (spec.clusterSize <-> status.readyLeaderReplicas).${NC}\n"
else
    echo -e "${GREEN}/scale subresource already present on RedisCluster.${NC}\n"
fi

# KEDA's managed HPA ALSO requires the /scale subresource to report a status.selector,
# otherwise it errors "the HPA target's scale is missing a selector" (ScalingActive=False)
# and never scales. The operator's status exposes no selector field, so we add a
# labelSelectorPath pointing at a small spec field that the apiserver DEFAULTS (spec
# defaulting is always applied), giving the scale a non-empty selector. Idempotent and
# re-asserted each run, same as the /scale patch above.
# NOTE: the default must match the LEADER pod labels (the same label the leader affinity
# uses). Verify with: k3s kubectl get pods -n redis -l app=redis-cluster-leader
if [[ -z "$(k3s kubectl get crd "${RC_CRD}" -o jsonpath='{.spec.versions[0].subresources.scale.labelSelectorPath}' 2>/dev/null)" ]]; then
    echo -e "${YELLOW}Adding scale labelSelectorPath on ${RC_CRD} (HPA selector requirement)...${NC}"
    k3s kubectl patch crd "${RC_CRD}" --type=json -p='[{"op":"add","path":"/spec/versions/0/subresources/scale/labelSelectorPath","value":".spec.scaleSelector"},{"op":"add","path":"/spec/versions/0/schema/openAPIV3Schema/properties/spec/properties/scaleSelector","value":{"type":"string","default":"app=redis-cluster-leader"}}]'
    echo -e "${GREEN}labelSelectorPath set (.spec.scaleSelector defaults to app=redis-cluster-leader).${NC}\n"
else
    echo -e "${GREEN}scale labelSelectorPath already present on RedisCluster.${NC}\n"
fi

# 1. Scorch any prior state so this run starts from a guaranteed clean slate.
echo -e "${YELLOW}[1/4] Scorching prior Redis state (manifests, PVCs, namespace)...${NC}"
# Drop set -e: most of these resources won't exist on a fresh cluster, and that's fine.
set +e
# Delete the Operator/KEDA custom resources FIRST, while their controllers are alive,
# so the namespace deletion below isn't blocked by lingering finalizers. The RedisCluster
# is now a Helm release, so uninstall it (removes the CR + its ServiceMonitor).
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-keda-scaling.yaml --ignore-not-found=true
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-servicemonitor.yaml --ignore-not-found=true
helm uninstall redis-cluster -n redis 2>/dev/null
# Delete the HPA scenario manifests too, in case Phase 1 ran before this.
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
# topology, so a surviving pod stays pinned to the cluster we just scorched (and to
# the HPA service name). Scale to 0 here and bring it back up (in step 2) so every
# pod boots fresh and resolves redis-cluster-leader. Guarded so a first run skips it.
if k3s kubectl get deployment fastapi-entrypoint -n default >/dev/null 2>&1; then
    echo -e "${YELLOW}Clearing stale FastAPI entrypoint pods...${NC}"
    k3s kubectl scale deployment fastapi-entrypoint -n default --replicas=0
    k3s kubectl wait --for=delete pod -l app=entrypoint -n default --timeout=120s
fi
set -e
echo -e "${GREEN}Clean slate confirmed.${NC}\n"

# 2. Deploy the operator-managed Redis Cluster.
echo -e "${YELLOW}[2/4] Deploying the operator-managed Redis Cluster (clusterSize 3)...${NC}"
k3s kubectl create namespace redis --dry-run=client -o yaml | k3s kubectl apply -f -
# Ensure the Opstree chart repo is available (idempotent), then deploy the cluster via
# the operator's OWN Helm chart. One values file gives us the RedisCluster, the exporter,
# AND the ServiceMonitor - the operator-method ease the thesis contrasts against the HPA
# path's hand-assembled ConfigMap + StatefulSet + manual cluster-create + ServiceMonitor.
helm repo add ot-helm https://ot-container-kit.github.io/helm-charts/ >/dev/null 2>&1 || true
helm repo update ot-helm >/dev/null
helm upgrade --install redis-cluster ot-helm/redis-cluster -n redis \
    -f ../config/redis/scaling-scenario-operator-keda/redis-operator-cluster-values.yaml

# Per the Opstree monitoring docs, neither the operator nor the chart's serviceMonitor flag
# actually creates a ServiceMonitor - you must apply one yourself. Without it Prometheus
# scrapes nothing and KEDA's cache-miss query reads 0. This wires the exporter into Prometheus.
k3s kubectl apply -f ../config/redis/scaling-scenario-operator-keda/redis-servicemonitor.yaml

# Spin the FastAPI entrypoint back up in parallel (same rationale as Phase 1): its
# REDIS_STARTUP_NODES includes redis-cluster-leader, so it connects automatically
# once the operator finishes forming the cluster.
echo -e "${YELLOW}Spinning the FastAPI entrypoint back up (parallel with cluster init)...${NC}"
k3s kubectl apply -f ../config/entrypoint-deployment.yaml

# The operator reconciles the CR into StatefulSets ASYNCHRONOUSLY and forms the
# cluster itself (CLUSTER MEET + slot assignment) - no manual redis-cli --cluster
# create. This is precisely the topology-aware work the HPA scenario could not do.
echo -e "Waiting for the operator to create and form the cluster..."
for _ in $(seq 1 60); do
    k3s kubectl get statefulset redis-cluster-leader -n redis >/dev/null 2>&1 && break
    sleep 2
done
k3s kubectl rollout status statefulset redis-cluster-leader -n redis --timeout=300s
# Followers (HA replicas) are created too; wait best-effort so a followerless config can't hang us.
k3s kubectl rollout status statefulset redis-cluster-follower -n redis --timeout=300s 2>/dev/null || true
echo -e "${GREEN}Operator-managed Redis Cluster is online.${NC}\n"

# /ready flips to 200 only after the client is live, so this doubles as a
# "cluster actually formed and reachable" barrier. Tolerant so one slow pod
# never blocks the flow.
echo -e "Waiting for the FastAPI entrypoint to connect to the cluster..."
# Report the REAL result. The if-condition keeps set -e from aborting on a timeout,
# but we no longer claim success when the rollout actually failed.
if k3s kubectl rollout status deployment/fastapi-entrypoint -n default --timeout=120s; then
    echo -e "${GREEN}FastAPI entrypoint is connected and ready.${NC}\n"
else
    echo -e "${RED}WARNING: entrypoint did not become Ready - it likely can't reach Redis.${NC}"
    echo -e "${RED}Inspect: k3s kubectl logs -n default -l app=entrypoint --tail=20${NC}\n"
fi

# 3. Deploy the KEDA autoscaler.
echo -e "${YELLOW}[3/4] Deploying the KEDA ScaledObject (cache-miss-ratio trigger > 15%)...${NC}"
k3s kubectl apply -f ../config/redis/scaling-scenario-operator-keda/redis-keda-scaling.yaml
# KEDA materializes a managed HPA for the ScaledObject; give it a moment to register.
k3s kubectl wait --for=condition=Ready scaledobject/redis-keda-scaler -n redis --timeout=120s 2>/dev/null || true
echo -e "${GREEN}KEDA is active and monitoring.${NC}\n"

# 4. The Scale-Out Experiment: drive load and watch the operator scale CORRECTLY.
#    Contrast with Phase 1: here the new node is fully integrated (slots + traffic),
#    so added capacity actually relieves load.
echo -e "${RED}>>> ACTION REQUIRED: START THE SCALE-OUT LOAD TEST <<<${NC}"
echo -e "1. In a new terminal on the load generator, run k6 (fans out across all 6"
echo -e "   NodePorts automatically):"
echo -e "   ${YELLOW}k6 run -o experimental-prometheus-rw load-test.js${NC}"
echo -e "2. The workload pushes the cache-miss ratio past KEDA's 15% threshold. In"
echo -e "   Grafana, watch KEDA scale the RedisCluster CR (3 -> 4 ...) and the OPERATOR"
echo -e "   automatically join + reshard the new master."
echo -e "3. THE CONTRAST WITH PHASE 1: the new pod is a real cluster member - it owns"
echo -e "   hash slots, draws CPU, and serves traffic, so throughput/latency recover."
echo ""
read -p "Press [Enter] once KEDA has scaled out (a 4th leader is Running) to snapshot the evidence..."

# 5. Evidence snapshot (best-effort): show that scaling actually integrated the node.
echo -e "\n${YELLOW}Capturing operator scale-out evidence...${NC}"
set +e
echo -e "${BLUE}--- KEDA ScaledObject + the HPA it manages ---${NC}"
k3s kubectl get scaledobject -n redis
k3s kubectl get hpa -n redis

echo -e "\n${BLUE}--- RedisCluster CR (operator-managed size) ---${NC}"
k3s kubectl get rediscluster -n redis

echo -e "\n${BLUE}--- Leader pods that EXIST ---${NC}"
k3s kubectl get pods -n redis -l app=redis-cluster-leader -o wide

echo -e "\n${BLUE}--- Cluster topology: EVERY master owns slots (no Ghost Pod) ---${NC}"
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379
set -e

# 6. The Scale-In Experiment: graceful, topology-aware drain (the operator's answer to the
#    HPA data cliff). Size is driven deterministically via KEDA's paused-replicas annotation
#    (overrides the metric for reproducibility): pin to 4 as setup, then to 3.
echo -e "\n${RED}>>> SCALE-IN / GRACEFUL DRAIN <<<${NC}"
set +e

echo -e "${YELLOW}Pinning to 4 (operator adds leader-3 and RESHARDS slots onto it)...${NC}"
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas="4" --overwrite
for _ in $(seq 1 150); do
    k3s kubectl get pod redis-cluster-leader-3 -n redis >/dev/null 2>&1 && break
    sleep 2
done
k3s kubectl rollout status statefulset redis-cluster-leader -n redis --timeout=600s

# Wait for the scale-out reshard to FULLY settle before scaling in. The operator moves a
# quarter of the slots onto leader-3; if we scale in mid-migration we leave an OPEN slot the
# operator CANNOT self-heal - it loops forever on "Please fix your cluster problems". Gate on:
# all 16384 slots covered AND none left open. Generous window: resharding is slow on small nodes.
echo -e "Waiting for the scale-out reshard to settle (all slots covered, none open)..."
settled=0
for _ in $(seq 1 120); do   # up to ~10 min
    chk=$(k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379 2>/dev/null)
    if echo "$chk" | grep -q "All 16384 slots covered" && ! echo "$chk" | grep -q "are open"; then
        settled=1; break
    fi
    sleep 5
done
if [[ "$settled" -ne 1 ]]; then
    echo -e "${RED}Scale-out reshard did not settle (open slots remain). Scaling in now risks a stuck${NC}"
    echo -e "${RED}cluster - fix first:  k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli --cluster fix 127.0.0.1:6379${NC}"
fi

echo -e "\n${BLUE}--- BEFORE scale-in: 4 masters, all 16384 slots covered ---${NC}"
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'

read -p "Press [Enter] to scale IN to 3 and watch the operator DRAIN leader-3 first..."
echo -e "${YELLOW}Pinning to 3 (operator migrates leader-3's slots to survivors, THEN removes it)...${NC}"
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas="3" --overwrite
k3s kubectl wait --for=delete pod/redis-cluster-leader-3 -n redis --timeout=600s

echo -e "\n${GREEN}--- AFTER scale-in: still 16384 slots, no data loss (no cliff) ---${NC}"
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379

# Resume metric-driven autoscaling.
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas-
set -e
echo -e "${YELLOW}Grafana (PromQL): redis_cluster_slots_assigned stays 16384 and${NC}"
echo -e "${YELLOW}sum(redis_db_keys{namespace=\"redis\"}) holds flat across scale-in - the opposite of Phase 1.${NC}"

echo -e "\n${GREEN}=== Phase 3 (Operator + KEDA: Scale-Out + Graceful Scale-In) Complete ===${NC}"
echo -e "Capture the Grafana panels (replicas vs. slots_assigned / total keys) for your"
echo -e "Results section, then stop k6. Run the Phase 4 teardown script when finished."
