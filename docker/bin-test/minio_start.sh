#!/bin/bash

set -euo pipefail

echo "[MinIO] Starting..."
MINIO_ROOT_USER=minioadmin \
  MINIO_ROOT_PASSWORD=minioadmin123 \
  minio server /data/minio --address ":9000" --console-address ":9001" 2>&1 | sed 's/^/[MinIO] /'
