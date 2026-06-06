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
#    HPA data cliff). To keep this controlled and reproducible we do NOT scale in from
#    wherever step 4's metric-driven load happened to leave the cluster. Instead we first
#    RESET to a known, settled 3-master baseline, then do SINGLE-STEP transitions only:
#    3->4 to set up the node we will drain, then 4->3 to measure the drain itself.
#    WHY single-step: the paused-replicas annotation pins clusterSize DIRECTLY and bypasses
#    the behavior block's 1-pod-per-step rate limit, so a multi-master jump (e.g. a churned
#    6->3) asks the operator to remove several masters at once - the uncontrolled collapse
#    that orphaned ~5000 slots in earlier runs. One master at a time lets each reshard finish.
echo -e "\n${RED}>>> SCALE-IN / GRACEFUL DRAIN <<<${NC}"
set +e

# Gate every transition on a fully settled cluster: exactly N ready leaders, all 16384 slots
# covered, and no slot left "open" (migrating/importing). Returns 1 on timeout so the caller
# can surface a wedge instead of charging ahead on a half-resharded cluster. NOTE: the Opstree
# operator has NO self-healing - an interrupted reshard leaves an open slot it cannot repair
# (redis-cli then refuses every reshard with "Please fix your cluster problems"); we never
# auto-repair, so if this never settles, that wedge IS the finding.
wait_settled() {
    local want="$1" tries="${2:-120}" chk masters   # default ~10 min at 5s/iteration
    for _ in $(seq 1 "$tries"); do
        chk=$(k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379 2>/dev/null)
        # Count REAL Redis masters from the check summary ("[OK] N keys in M masters."), NOT k8s
        # readyReplicas - a pod can be Ready but not yet MEET'd/resharded into the cluster.
        masters=$(echo "$chk" | grep -oE '[0-9]+ masters' | grep -oE '^[0-9]+' | head -1)
        if [[ "$masters" == "$want" ]] \
           && echo "$chk" | grep -q "All 16384 slots covered" \
           && echo "$chk" | grep -q "All nodes agree about slots configuration" \
           && ! echo "$chk" | grep -q "are open"; then
            return 0
        fi
        sleep 5
    done
    return 1
}

# Human-in-the-loop settle gate. Drives the cluster to a CLEAN state at <want> Redis masters
# (all 16384 slots covered, none open, AND all nodes agreeing) and HARD-STOPS until it is
# reached - never plowing ahead onto an unsettled cluster, which is what compounds churn into a
# configEpoch split. The operator has NO self-healing, so on a wedge it surfaces the SPECIFIC
# variant and the MATCHING manual recovery, then loops until a human has cleared it:
#   * open slot (migrating/importing)            -> redis-cli --cluster fix
#   * "Nodes don't agree about configuration!"   -> CLUSTER BUMPEPOCH ('--cluster fix' CANNOT fix this; it exits 1)
gate_settled() {
    local want="$1" chk
    if wait_settled "$want"; then return 0; fi
    while true; do
        chk=$(k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379 2>/dev/null)
        echo -e "\n${RED}>>> ACTION REQUIRED: operator wedged, cannot self-heal (need ${want} settled masters) <<<${NC}"
        if echo "$chk" | grep -q "are open"; then
            echo -e "An interrupted reshard left an OPEN slot. OPEN A NEW TERMINAL and run:"
            echo -e "   ${YELLOW}k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli --cluster fix 127.0.0.1:6379${NC}"
        elif echo "$chk" | grep -q "Nodes don't agree"; then
            echo -e "Masters DISAGREE about the slot map (a configEpoch collision). '--cluster fix' CANNOT"
            echo -e "repair this and exits 1. OPEN A NEW TERMINAL, list the nodes and their config-epochs:"
            echo -e "   ${YELLOW}k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli cluster nodes${NC}"
            echo -e "find two masters sharing an epoch, then bump the newer/importing one so one claim wins:"
            echo -e "   ${YELLOW}k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli -h <MASTER_IP> -p 6379 cluster bumpepoch${NC}"
        else
            echo -e "Cluster not settled at ${want} masters yet. Inspect:"
            echo -e "   ${YELLOW}k3s kubectl exec -it redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379${NC}"
        fi
        read -p "Press [Enter] AFTER recovering it in the OTHER terminal..."
        if wait_settled "$want" 12; then   # quick ~60s re-check
            echo -e "${GREEN}Cluster settled at ${want} masters - continuing.${NC}"
            return 0
        fi
        echo -e "${RED}Still not settled - repeating the guidance.${NC}"
    done
}

# Quiesce BEFORE any resharding below. Live writes landing on slots that are mid-migration are
# the dominant cause of an INTERRUPTED reshard (the open-slot wedge), and BOTH the reset (6a)
# and the scale-out (6b) below reshard. Stopping k6 first is the single most effective way to
# get a reproducible, graceful scale-in. Data loss stays fully measurable with no traffic
# (keys have no TTL), so quiescing costs us nothing for the data-safety result.
echo -e "\n${RED}>>> STOP k6 NOW <<<${NC}"
echo -e "Terminate the k6 load test and wait for the request rate to fall to 0 before continuing"
echo -e "(watch it flatline in Grafana - ideally confirm the redis write-command rate is ~0 too)."
echo -e "Resharding under live writes is what interrupts the operator; quiescing avoids the wedge."
read -p "Press [Enter] once k6 is stopped and traffic has drained to 0..."

# 6a-pre. Best-effort: let any in-flight load-driven reshard from step 4 FINISH before we touch
# clusterSize. Pinning to 3 ON TOP of an in-flight reshard is exactly what produced the
# configEpoch split ("Nodes don't agree about configuration!") in earlier runs. Not human-gated
# here: if it can't settle we reset anyway, and the HARD gate on 3 (below) catches any wedge.
cur_leaders=$(k3s kubectl get statefulset redis-cluster-leader -n redis -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
echo -e "${YELLOW}[settle] Letting the load-driven scale-out finish at its current ${cur_leaders:-?} master(s) before resetting...${NC}"
wait_settled "${cur_leaders:-3}" 60 || echo -e "${YELLOW}  (incoming state did not fully settle; resetting to 3 anyway)${NC}"

# 6a. RESET to a settled 3-master baseline (single step), then HARD-gate on it.
echo -e "${YELLOW}[reset] Pinning to a settled 3-master baseline before the controlled scale-in...${NC}"
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas="3" --overwrite
gate_settled 3
echo -e "${GREEN}Baseline reached: 3 masters, all 16384 slots covered, all nodes agree.${NC}\n"

# 6b. SINGLE-STEP scale-OUT 3 -> 4 (creates leader-3, the master we will then drain).
echo -e "${YELLOW}Pinning to 4 (operator adds leader-3 and RESHARDS slots onto it)...${NC}"
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas="4" --overwrite
for _ in $(seq 1 150); do
    k3s kubectl get pod redis-cluster-leader-3 -n redis >/dev/null 2>&1 && break
    sleep 2
done
k3s kubectl rollout status statefulset redis-cluster-leader -n redis --timeout=600s

echo -e "Waiting for the scale-out reshard to settle (4 masters, all slots covered, all agree)..."
gate_settled 4
echo -e "${GREEN}Scale-out settled: leader-3 integrated, all 16384 slots covered, all nodes agree.${NC}"

echo -e "\n${BLUE}--- BEFORE scale-in: 4 masters, all 16384 slots covered ---${NC}"
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'

read -p "Press [Enter] to scale IN to 3 and watch the operator DRAIN leader-3 first..."
# 6c. SINGLE-STEP scale-IN 4 -> 3 (the measurement: graceful drain of exactly one master).
echo -e "${YELLOW}Pinning to 3 (operator migrates leader-3's slots to survivors, THEN removes it)...${NC}"
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas="3" --overwrite

# The operator drains leader-3 then removes it. CRITICAL FINDING: it has NO self-healing. If the
# drain reshard is interrupted, a slot is left "open" (migrating/importing) that the operator
# CANNOT repair on its own - it loops forever on "Please fix your cluster problems" and never
# removes leader-3. There is no in-cluster automation for this: recovery is a MANUAL operation
# performed by a HUMAN OPERATOR. This script does NOT auto-repair; instead, when the operator
# wedges, the human must OPEN A NEW TERMINAL and manually trigger `redis-cli --cluster fix`. That
# manual toil is precisely the operational cost the thesis is documenting, so we make it an
# explicit, required, verified step rather than hiding it behind an automated fix.
# The operator should drain leader-3 (migrate its slots to survivors) then remove it, leaving a
# clean 3-master cluster. CRITICAL FINDING: it has NO self-healing - if the drain reshard is
# interrupted it wedges (an open slot, OR a configEpoch "nodes don't agree" split) and never
# removes leader-3. gate_settled waits for the clean 3-master end state and, on a wedge, HARD-
# STOPS with the matching MANUAL recovery (--cluster fix for an open slot, CLUSTER BUMPEPOCH for
# a disagreement) until a human clears it. That manual toil is the operational cost the thesis documents.
echo -e "Waiting for the operator to drain and remove leader-3 (clean 3-master end state)..."
gate_settled 3

# 6d. HONEST after-check: only claim "no cliff" if the cluster is genuinely whole. Read the
#     real numbers and compare - never assert zero data loss we did not verify.
echo -e "\n${BLUE}--- AFTER scale-in ---${NC}"
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info | grep -E 'cluster_state|cluster_slots_assigned|cluster_slots_ok'
state=$(k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info 2>/dev/null | tr -d '\r' | awk -F: '/^cluster_state:/{print $2}')
slots_ok=$(k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli cluster info 2>/dev/null | tr -d '\r' | awk -F: '/^cluster_slots_ok:/{print $2}')
if [[ "$state" == "ok" && "$slots_ok" == "16384" ]]; then
    echo -e "${GREEN}Graceful scale-in CONFIRMED: cluster_state=ok, all 16384 slots present - no data cliff.${NC}"
else
    echo -e "${RED}Scale-in did NOT complete cleanly: cluster_state=${state:-?} slots_ok=${slots_ok:-?} (expected ok / 16384).${NC}"
    echo -e "${RED}That is an operator-side data cliff - report it honestly; do not claim zero data loss.${NC}"
fi
k3s kubectl exec redis-cluster-leader-0 -n redis -- redis-cli --cluster check 127.0.0.1:6379

# Resume metric-driven autoscaling.
k3s kubectl annotate scaledobject redis-keda-scaler -n redis autoscaling.keda.sh/paused-replicas-
set -e
echo -e "${YELLOW}Grafana (PromQL): redis_cluster_slots_assigned stays 16384 and${NC}"
echo -e "${YELLOW}sum(redis_db_keys{namespace=\"redis\"}) holds flat across scale-in - the opposite of Phase 1.${NC}"

echo -e "\n${GREEN}=== Phase 3 (Operator + KEDA: Scale-Out + Graceful Scale-In) Complete ===${NC}"
echo -e "Capture the Grafana panels (replicas vs. slots_assigned / total keys) for your"
echo -e "Results section, then stop k6. Run the Phase 4 teardown script when finished."
