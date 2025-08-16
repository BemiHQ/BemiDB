#!/bin/bash

set -euo pipefail

# Set common environment variables
export CATALOG_DATABASE_URL=postgres://postgres:postgres@localhost:5432/catalog
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin123
export AWS_S3_ENDPOINT=localhost:9000
export AWS_S3_BUCKET=bemidb-bucket

/app/bin/postgres_start.sh &
postgres_pid=$!
/app/bin/postgres_configure.sh

psql $CATALOG_DATABASE_URL -f /app/scripts/catalog.sql

/app/bin/minio_start.sh &
minio_pid=$!
trap 'kill -s TERM $postgres_pid $minio_pid 2>/dev/null' EXIT # kill children on exit
/app/bin/minio_configure.sh

# Seed data
cd /app/src/syncer-postgres
SOURCE_POSTGRES_SYNC_MODE=FULL_REFRESH \
  DESTINATION_SCHEMA_NAME=postgres \
  go test -v ./...

# Run tests
cd /app/src/syncer-common
go test -v -count=1 ./...
cd /app/src/server
BEMIDB_USER=user \
  BEMIDB_PASSWORD=password \
  BEMIDB_LOG_LEVEL=ERROR \
  go test -v -count=1 ./...

kill -s TERM $postgres_pid $minio_pid 2>/dev/null
