#!/bin/bash

set -euo pipefail

: "${AWS_REGION:?Environment variable AWS_REGION must be set}"
: "${AWS_S3_BUCKET:?Environment variable AWS_S3_BUCKET must be set}"
: "${AWS_ACCESS_KEY_ID:?Environment variable AWS_ACCESS_KEY_ID must be set}"
: "${AWS_SECRET_ACCESS_KEY:?Environment variable AWS_SECRET_ACCESS_KEY must be set}"
: "${CATALOG_DATABASE_URL:?Environment variable CATALOG_DATABASE_URL must be set}"

echo "[Trino] Configuring catalog..."

psql $CATALOG_DATABASE_URL -f /app/trino/configure.sql

if [[ -z "${AWS_S3_ENDPOINT:-}" || "${AWS_S3_ENDPOINT:-}" == "s3.amazonaws.com" ]]; then
  AWS_S3_ENDPOINT="https://s3-${AWS_REGION}.amazonaws.com"
else
  if [[ "${AWS_S3_ENDPOINT}" == localhost* ]]; then
    AWS_S3_ENDPOINT="http://${AWS_S3_ENDPOINT}"
  else
    AWS_S3_ENDPOINT="https://${AWS_S3_ENDPOINT}"
  fi
fi

sed -i \
  -e "s#{{AWS_S3_BUCKET}}#${AWS_S3_BUCKET}#g" \
  -e "s#{{AWS_REGION}}#${AWS_REGION}#g" \
  -e "s#{{AWS_ACCESS_KEY_ID}}#${AWS_ACCESS_KEY_ID}#g" \
  -e "s#{{AWS_SECRET_ACCESS_KEY}}#${AWS_SECRET_ACCESS_KEY}#g" \
  -e "s#{{AWS_S3_ENDPOINT}}#${AWS_S3_ENDPOINT}#g" \
  -e "s#{{CATALOG_DATABASE_URL}}#postgresql://$(echo $CATALOG_DATABASE_URL | cut -d'@' -f2)#g" \
  -e "s#{{TRINO_CATALOG_DATABASE_USER}}#$(echo $CATALOG_DATABASE_URL | cut -d'/' -f3 | cut -d':' -f1)#g" \
  -e "s#{{TRINO_CATALOG_DATABASE_PASSWORD}}#$(echo $CATALOG_DATABASE_URL | cut -d'/' -f3 | cut -d':' -f2 | cut -d'@' -f1)#g" \
  /etc/trino/catalog/iceberg.properties

echo "[Trino] Starting server..."
CATALOG_MANAGEMENT=static \
  ./trino/bin/run.sh >/dev/null 2> >(sed 's/^/[Trino] /')
