#!/bin/bash

set -euo pipefail

case "${1:-}" in
  bash)
    exec /bin/bash
    ;;
  syncer-postgres)
    : "${SOURCE_POSTGRES_DATABASE_URL:?Environment variable SOURCE_POSTGRES_DATABASE_URL must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    /app/bin/trino_configure_and_start.sh &
    trino_pid=$!
    trap 'kill -s TERM $trino_pid 2>/dev/null' EXIT # kill children on exit

    /app/bin/trino_ensure_started.sh

    echo "Starting Syncer for PostgreSQL..."
    TRINO_DATABASE_URL=http://user@localhost:8080 \
      TRINO_CATALOG_NAME=iceberg \
      SOURCE_POSTGRES_SYNC_MODE=FULL_REFRESH \
      ./bin/syncer-postgres 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for PostgreSQL finished."

    kill -s TERM $trino_pid 2>/dev/null
    ;;
  syncer-amplitude)
    : "${SOURCE_AMPLITUDE_API_KEY:?Environment variable SOURCE_AMPLITUDE_API_KEY must be set}"
    : "${SOURCE_AMPLITUDE_SECRET_KEY:?Environment variable SOURCE_AMPLITUDE_SECRET_KEY must be set}"
    : "${DESTINATION_SCHEMA_NAME:?Environment variable DESTINATION_SCHEMA_NAME must be set}"

    /app/bin/trino_configure_and_start.sh &
    trino_pid=$!
    trap 'kill -s TERM $trino_pid 2>/dev/null' EXIT # kill children on exit

    /app/bin/trino_ensure_started.sh

    echo "Starting Syncer for Amplitude..."
    TRINO_DATABASE_URL=http://user@localhost:8080 \
      TRINO_CATALOG_NAME=iceberg \
      ./bin/syncer-amplitude 2>&1 | sed 's/^/[Syncer] /'
    echo "Syncer for Amplitude finished."

    kill -s TERM $trino_pid 2>/dev/null
    ;;
  server)
    : "${AWS_REGION:?Environment variable AWS_REGION must be set}"
    : "${AWS_S3_BUCKET:?Environment variable AWS_S3_BUCKET must be set}"
    : "${AWS_ACCESS_KEY_ID:?Environment variable AWS_ACCESS_KEY_ID must be set}"
    : "${AWS_SECRET_ACCESS_KEY:?Environment variable AWS_SECRET_ACCESS_KEY must be set}"
    : "${CATALOG_DATABASE_URL:?Environment variable CATALOG_DATABASE_URL must be set}"

    echo "Starting server..."
    ./bin/server
    ;;
  *)
    echo "Unknown argument: ${1:-}"
    echo "Available options: syncer-postgres, bash"
    exit 1
    ;;
esac
