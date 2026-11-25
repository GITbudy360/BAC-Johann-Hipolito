#!/bin/bash

# Exit immediately if any command fails
set -e

NAMESPACE="fastapi-leaderboard"
DEPLOYMENT="fastapi-leaderboard"
DEPLOYMENT_MANIFEST="/home/itpadm/BAC-Johann-Hipolito/k8s-cluster/fastapi-leaderboard-deployment/fastapi-deployment.yaml"
SERVICEMONITOR_MANIFEST="/home/itpadm/BAC-Johann-Hipolito/k8s-cluster/fastapi-leaderboard-deployment/fastapi-servicemonitor.yaml"

echo "1. Applying FastAPI Deployment and ServiceMonitor manifests..."
sudo k3s kubectl apply -f "$DEPLOYMENT_MANIFEST"
sudo k3s kubectl apply -f "$SERVICEMONITOR_MANIFEST"

echo "2. Triggering rolling restart to pull latest image (imagePullPolicy: Always)..."
sudo k3s kubectl rollout restart deployment/"$DEPLOYMENT" -n "$NAMESPACE"

echo "3. Waiting for all 10 replicas to become Ready..."
sudo k3s kubectl rollout status deployment/"$DEPLOYMENT" -n "$NAMESPACE" --timeout=180s

echo "4. Verifying pod distribution and status..."
sudo k3s kubectl get pods -n "$NAMESPACE" -l app="$DEPLOYMENT" -o wide

echo "5. Verifying ServiceMonitor..."
sudo k3s kubectl get servicemonitor fastapi-monitor -n "$NAMESPACE"

echo "Done! FastAPI cleanly redeployed."