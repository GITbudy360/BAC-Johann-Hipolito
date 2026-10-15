#!/bin/bash

# Enforce strict blocking: exit immediately if any command returns a non-zero status
set -e

echo "1. Deleting existing configmap..."
# Added --ignore-not-found so the script doesn't fail on the first execution
sudo k3s kubectl delete configmap seed-script -n redis-cluster --ignore-not-found

echo "2. Creating new configmap..."
sudo k3s kubectl create configmap seed-script --from-file=/home/itpadm/BAC-Johann-Hipolito/k8s-cluster/setup-and-access-scripts/seed-job/seed.py --from-file=/home/itpadm/BAC-Johann-Hipolito/k8s-cluster/setup-and-access-scripts/seed-job/requirements.txt -n redis-cluster

echo "3. Deploying the Job..."
# Deleting the existing job guarantees a fresh run instead of Kubernetes reporting "unchanged"
sudo k3s kubectl delete job seed-redis -n redis-cluster --ignore-not-found
sudo k3s kubectl apply -f /home/itpadm/BAC-Johann-Hipolito/k8s-cluster/setup-and-access-scripts/seed-job/seed-job.yaml

echo "4. Executing and monitoring..."
# Pause briefly to allow the k3s control plane to schedule the pod
sleep 3
sudo k3s kubectl logs -f job/seed-redis -n redis-cluster

echo "5. Applying starved Redis cluster setup..."
sudo k3s kubectl apply -f /home/itpadm/BAC-Johann-Hipolito/k8s-cluster/rediscluster/redis-starved-deployment.yaml