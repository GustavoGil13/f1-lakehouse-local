#!/usr/bin/env bash
set -e

echo "[init] Ensuring connection exists: minio_logs"

if airflow connections get minio_logs >/dev/null 2>&1; then
  echo "[init] Connection minio_logs already exists - skipping"
  exit 0
fi

airflow connections add minio_logs \
  --conn-type aws \
  --conn-login "${S3A_ACCESS_KEY}" \
  --conn-password "${S3A_SECRET_KEY}" \
  --conn-extra "{\"region_name\":\"us-east-1\",\"endpoint_url\":\"${S3A_ENDPOINT}\",\"verify\":false,\"config_kwargs\":{\"s3\":{\"addressing_style\":\"path\"}}}"

echo "[init] Connection created."
