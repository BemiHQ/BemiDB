#!/bin/bash

set -euo pipefail

echo "[MinIO] Waiting for server to be ready..."
until curl -f http://localhost:9000/minio/health/live 2>/dev/null; do
  echo "MinIO not ready yet, waiting..."
  sleep 2
done
echo "[MinIO] Ready"

echo "[MinIO] Creating $AWS_S3_BUCKET bucket..."
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/$AWS_S3_BUCKET --ignore-existing
