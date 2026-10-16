#!/bin/bash

# Enforce strict blocking: exit immediately if any command returns a non-zero status
set -e

echo "1. Rollout restart of the FastApi Pods..."
sudo k3s kubectl rollout restart deployment fastapi-leaderboard -n fastapi-leaderboard

echo "2. Deleting existing Redis Cluster..."
sudo k3s kubectl delete rediscluster redis-master -n redis-cluster --ignore-not-found

echo "3. Deleting existing stale Persistent Volume Claims (PVCs)..."
sudo k3s kubectl delete pvc --all -n redis-cluster --ignore-not-found

echo "4. Applying redis-deployment.yaml..."
sudo k3s kubectl apply -f /home/itpadm/BAC-Johann-Hipolito/k8s-cluster/rediscluster/redis-deployment.yaml

echo "5. Monitoring the Redis Cluster pods (after waiting for 30s)..."
# Wait for the Redis pods to be ready before proceeding
sleep 30
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info
echo "Another 10s"
sleep 10
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info
echo "Another 10s"
sleep 10
sudo k3s kubectl exec -it redis-master-leader-0 -n redis-cluster -- redis-cli cluster info