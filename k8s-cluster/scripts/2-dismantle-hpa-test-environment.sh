#!/bin/bash
# Phase 2: Critical Teardown (Reset to Zero) for the HPA scenario.
# Wipes everything the HPA test created so the Operator + KEDA run starts from a
# guaranteed clean slate - no inherited data, no fragmented hash slots.
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Resolve paths relative to this script so it works no matter where it's invoked from.
cd "$(dirname "$0")"

echo -e "${RED}=== Phase 2: Aggressive HPA Teardown (Reset to Zero) ===${NC}\n"

# 1. Drop FastAPI connections FIRST.
#    Each entrypoint pod holds a Redis cluster client pinned to the HPA topology
#    (redis-headless + the master pod IPs). _connect_with_retry only reconnects
#    until its first success, so a pod that already latched onto the HPA cluster
#    will NOT migrate to the Operator cluster on its own. Scaling to 0 now means
#    Phase 3 brings the pods up fresh against redis-cluster-leader, and it also
#    stops them from spamming errors at the cluster while we delete it.
echo -e "${YELLOW}[1/2] Scaling FastAPI entrypoint to 0...${NC}"
if k3s kubectl get deployment fastapi-entrypoint -n default >/dev/null 2>&1; then
    k3s kubectl scale deployment fastapi-entrypoint -n default --replicas=0
    echo -e "${GREEN}FastAPI scaled to 0.${NC}\n"
else
    echo -e "${GREEN}No fastapi-entrypoint deployment found; nothing to scale.${NC}\n"
fi

# 2. Scorch the entire 'redis' namespace in one shot.
#    Deleting the namespace cascades EVERYTHING the HPA scenario created -
#    StatefulSet, redis-headless Service, ConfigMap, HorizontalPodAutoscaler and
#    ServiceMonitor (plus any PVCs) - with no manifest-path fragility or
#    delete-ordering concerns. --wait (the default) blocks until the namespace
#    and all its dependents are fully terminated, so "clean" really means clean.
echo -e "${YELLOW}[2/2] Scorching the 'redis' namespace (all HPA resources + data)...${NC}"
k3s kubectl delete namespace redis --ignore-not-found=true
echo -e "${GREEN}The 'redis' namespace is gone. The slate is clean.${NC}\n"

echo -e "${GREEN}=== Phase 2 Complete ===${NC}"
echo -e "The HPA environment is completely reset. You are ready to begin the Operator + KEDA scenario."
