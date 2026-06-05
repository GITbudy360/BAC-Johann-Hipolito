#!/bin/bash
# Phase 4: Final Teardown for the Operator + KEDA scenario.
# Returns the cluster to a clean state once the operator telemetry is captured.
# Leaves the shared cluster-wide controllers installed (Redis Operator in 'ot-operators',
# KEDA in 'keda') - they are one-time infra, not part of the per-experiment teardown.
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Resolve paths relative to this script so it works no matter where it's invoked from.
cd "$(dirname "$0")"

# helm needs a kubeconfig; k3s keeps its here (k3s kubectl embeds it, plain `helm` does not).
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"

echo -e "${RED}=== Phase 4: Operator + KEDA Final Teardown ===${NC}\n"

# 1. Drop FastAPI connections FIRST, so the entrypoints stop hammering the cluster while we
#    tear it down (and aren't left pinned to a now-deleted topology). Scaled to 0 rather than
#    deleted, mirroring Phase 2 - the Deployment object is harmless and a future run reuses it.
echo -e "${YELLOW}[1/3] Scaling FastAPI entrypoint to 0...${NC}"
if k3s kubectl get deployment fastapi-entrypoint -n default >/dev/null 2>&1; then
    k3s kubectl scale deployment fastapi-entrypoint -n default --replicas=0
    echo -e "${GREEN}FastAPI scaled to 0.${NC}\n"
else
    echo -e "${GREEN}No fastapi-entrypoint deployment found; nothing to scale.${NC}\n"
fi

# 2. Remove the experiment's custom resources while their controllers are still alive, so
#    finalizers clear cleanly: the KEDA ScaledObject, our standalone ServiceMonitor, and the
#    Helm release. `helm uninstall` lets the operator process the RedisCluster finalizer and
#    keeps Helm's bookkeeping consistent (cleaner than relying on the namespace cascade alone).
#    Tolerant block: on a partially-torn-down cluster, some of these won't exist.
echo -e "${YELLOW}[2/3] Removing KEDA ScaledObject, ServiceMonitor, and the Helm release...${NC}"
set +e
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-keda-scaling.yaml --ignore-not-found=true
k3s kubectl delete -f ../config/redis/scaling-scenario-operator-keda/redis-servicemonitor.yaml --ignore-not-found=true
helm uninstall redis-cluster -n redis 2>/dev/null
set -e
echo -e "${GREEN}Custom resources and Helm release removed.${NC}\n"

# 3. Scorch the namespace to sweep up anything remaining (the RedisCluster CR, Services,
#    ConfigMaps, the Helm release secret, PVCs) in one shot. --wait (the default) blocks
#    until the namespace and all its dependents are fully terminated.
echo -e "${YELLOW}[3/3] Scorching the 'redis' namespace...${NC}"
k3s kubectl delete namespace redis --ignore-not-found=true
echo -e "${GREEN}The 'redis' namespace is gone. The slate is clean.${NC}\n"

echo -e "${GREEN}=== Phase 4 Complete ===${NC}"
echo -e "The Operator + KEDA environment is fully torn down. The Redis Operator (ot-operators)"
echo -e "and KEDA (keda) remain installed as shared infra; the CRD /scale patches are left in"
echo -e "place (harmless, and re-asserted by the Phase 3 setup script)."
