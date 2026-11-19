#!/bin/bash

# Enforce strict blocking: exit immediately if any command returns a non-zero status
set -e

NAMESPACE="redis-cluster"
MASTERS=("redis-master-leader-0" "redis-master-leader-1" "redis-master-leader-2")

echo "1. Verifying Redis Cluster health before flush..."
CLUSTER_STATE=$(sudo k3s kubectl exec redis-master-leader-0 -n "$NAMESPACE" -c redis-master-leader -- redis-cli cluster info | grep "cluster_state:" | tr -d '\r')
echo "   Current state: $CLUSTER_STATE"

if [ "$CLUSTER_STATE" != "cluster_state:ok" ]; then
  echo "ERROR: Redis cluster is not healthy ($CLUSTER_STATE). Aborting."
  exit 1
fi

echo "2. Flushing all data from Redis master pods (keeping topology intact)..."
for POD in "${MASTERS[@]}"; do
  echo -n "   Flushing $POD... "
  sudo k3s kubectl exec "$POD" -n "$NAMESPACE" -c redis-master-leader -- redis-cli FLUSHALL
done

echo "3. Verifying all master pods are empty (DBSIZE == 0)..."
for POD in "${MASTERS[@]}"; do
  KEYS=$(sudo k3s kubectl exec "$POD" -n "$NAMESPACE" -c redis-master-leader -- redis-cli DBSIZE | tr -d '\r')
  echo "   $POD key count: $KEYS"
  if [ "$KEYS" != "0" ]; then
    echo "ERROR: $POD still has $KEYS keys remaining. Aborting."
    exit 1
  fi
done

echo "4. Confirming cluster state is ready for seed-db.sh..."
sudo k3s kubectl exec redis-master-leader-0 -n "$NAMESPACE" -c redis-master-leader -- redis-cli cluster info | grep -E "cluster_state|cluster_slots_ok|cluster_known_nodes"

echo "Done! Cluster topology is preserved and ready for seeding."