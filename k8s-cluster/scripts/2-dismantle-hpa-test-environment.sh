#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${RED}=== Phase 2: Aggressive HPA Teardown ===${NC}\n"

# 1. Delete HPA and Cluster Resources using the provided manifests
echo -e "${YELLOW}[1/3] Deleting Autoscaler, StatefulSet, Services, and ConfigMaps...${NC}"
k delete -f ../config/redis/redis-hpa-scaling.yaml --ignore-not-found=true
k delete -f redis-servicemonitor.yaml --ignore-not-found=true
k delete -f ../config/redis/redis-hpa-cluster.yaml --ignore-not-found=true

# Wait for the pods to actually terminate before deleting PVCs
echo -e "Waiting for Redis pods to terminate completely..."
while [[ $(k get pods -n redis -l app=redis --no-headers 2>/dev/null | wc -l) -gt 0 ]]; do
    sleep 2
done
echo -e "${GREEN}All Redis pods terminated.${NC}\n"

# 2. Destroy the Data
echo -e "${YELLOW}[2/3] Purging Persistent Volume Claims (Destroying Data)...${NC}"
k delete pvc --all -n redis
echo -e "${GREEN}All stateful data destroyed. The slate is clean.${NC}\n"

# 3. Clean up application connections
echo -e "${YELLOW}[3/3] Dropping FastAPI connections...${NC}"
# Assuming your entrypoint deployment is named 'fastapi-entrypoint' in the default namespace
k scale deployment fastapi-entrypoint -n default --replicas=0
echo -e "${GREEN}FastAPI scaled to 0.${NC}\n"

echo -e "${GREEN}=== Phase 2 Complete ===${NC}"
echo -e "The environment is completely reset. You are now ready to begin the Operator + KEDA scenario."