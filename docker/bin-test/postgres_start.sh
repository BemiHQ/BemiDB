#!/bin/bash

set -euo pipefail

export PGDATA="/var/lib/postgresql/data"

/usr/lib/postgresql/16/bin/initdb -D "$PGDATA" 2>&1 | sed 's/^/[Postgres] /'

/usr/lib/postgresql/16/bin/pg_ctl -D "$PGDATA" -l "$PGDATA/postgresql.log" start 2>&1 | sed 's/^/[Postgres] /'

echo "[Postgres] started successfully"
