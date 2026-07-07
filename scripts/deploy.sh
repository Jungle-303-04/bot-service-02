#!/usr/bin/env bash
set -euo pipefail

CONTEXT="${1:-cluster-1}"
RELEASE="${2:-stable}"
NAMESPACE="sandbox"
SERVICE="bot-service-02"
DB_NAME="bot_service_02"
DB_USER="bot"
DB_PASSWORD="${DB_PASSWORD:-}"

if [[ -z "$DB_PASSWORD" ]]; then
  DB_PASSWORD="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom | head -c 32)"
fi

kubectl --context "$CONTEXT" -n "$NAMESPACE" create secret generic "${SERVICE}-db-secret" \
  --from-literal=password="$DB_PASSWORD" \
  --from-literal=database-url="postgresql://${DB_USER}:${DB_PASSWORD}@${SERVICE}-db:5432/${DB_NAME}" \
  --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -

kubectl --context "$CONTEXT" apply -k "deploy/releases/${RELEASE}"
kubectl --context "$CONTEXT" -n "$NAMESPACE" rollout status "deployment/${SERVICE}-db" --timeout=180s
kubectl --context "$CONTEXT" -n "$NAMESPACE" rollout status "deployment/${SERVICE}-api" --timeout=240s
kubectl --context "$CONTEXT" -n "$NAMESPACE" rollout status "deployment/${SERVICE}-web" --timeout=240s
kubectl --context "$CONTEXT" -n "$NAMESPACE" get svc "${SERVICE}-web"

