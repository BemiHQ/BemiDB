#!/bin/bash

set -xeuo pipefail

launcher_opts=(--etc-dir /etc/trino)
if ! grep -s -q 'node.id' /etc/trino/node.properties; then
    launcher_opts+=("-Dnode.id=${HOSTNAME}")
fi

exec /app/trino/bin/launcher run "${launcher_opts[@]}" "$@"
