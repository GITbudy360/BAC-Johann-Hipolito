#!/bin/bash
set -e

echo "=== Wiping Thesis Test Environments ==="

# 1. Wipe the Operator/KEDA Environment
echo "[1/4] Destroying Operator-managed Redis Cluster and Autoscaler..."
k delete ScaledObject redis-keda-scaler -n redis-op --ignore-not-found=true
k delete RedisCluster redis-cluster -n redis-op --ignore-not-found=true

# 2. Wipe the HPA Environment
echo "[2/4] Destroying Vanilla HPA and StatefulSet..."
k delete hpa redis-hpa -n redis-hpa --ignore-not-found=true
k delete statefulset redis -n redis-hpa --ignore-not-found=true
k delete svc redis-headless -n redis-hpa --ignore-not-found=true

# 3. The Most Critical Step: Wiping the Hard Drives
# If you don't delete the PVCs, the next test will boot up with the old data!
echo "[3/4] Purging residual Persistent Volume Claims (Destroying Data)..."
k delete pvc --all -n redis-hpa
k delete pvc --all -n redis-op

# 4. Wipe the Entrypoints
echo "[4/4] Scaling down FastAPI entrypoints to drop all active connections..."
k scale deployment fastapi-entrypoint -n default --replicas=0

echo "=== Environment Reset Complete ==="
echo "The Operator and KEDA controllers are still running, but the databases are gone."
echo "You are ready to deploy a fresh architecture for your next test run."