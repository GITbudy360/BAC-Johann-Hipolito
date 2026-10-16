#!/bin/bash

# Enforce strict blocking: exit immediately if any command returns a non-zero status
set -e

echo "1. Deleting existing Redis Cluster..."
sudo k3s kubectl delete rediscluster redis-master -n redis-cluster --ignore-not-found

echo "2. Deleting existing stale Persistent Volume Claims (PVCs)..."
sudo k3s kubectl delete pvc --all -n redis-cluster --ignore-not-found

echo "3. Applying redis-healthy-deployment.yaml..."
sudo k3s kubectl apply -f /home/itpadm/BAC-Johann-Hipolito/k8s-cluster/rediscluster/redis-deployment.yaml

echo "4. Monitoring the Redis Cluster pods (after waiting for 30s)..."
# Wait for the Redis pods to be ready before proceeding
sleep 30
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info
echo "Another 10s"
sleep 10
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info
echo "Another 10s"
sleep 10
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info