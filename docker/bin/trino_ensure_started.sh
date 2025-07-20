#!/bin/bash

set -euo pipefail

export TRINO_DATABASE_URL="http://user@localhost:8080"
while ! curl -sf "${TRINO_DATABASE_URL}/v1/info" > /dev/null; do
  echo "[Trino] Waiting for server to be ready..."
  sleep 15
done
echo "[Trino] Server is ready."
