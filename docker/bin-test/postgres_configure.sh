#!/bin/bash

set -euo pipefail

until /usr/lib/postgresql/16/bin/pg_isready -h localhost -p 5432 2>/dev/null; do
    echo "[Postgres] Waiting for server to be ready..."
    sleep 2
done

psql -d postgres -c "CREATE USER postgres WITH PASSWORD 'postgres' SUPERUSER;" 2>&1 | sed 's/^/[Postgres] /'
psql -d postgres -c "CREATE DATABASE catalog OWNER postgres;" 2>&1 | sed 's/^/[Postgres] /'

echo "[Postgres] Configured successfully"
