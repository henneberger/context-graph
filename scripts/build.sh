#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tag="${IMAGE_TAG:-}"
for service in identity check-proxy ingestion processor query dashboard; do
  service_tag="$tag"
  if [[ -z "$service_tag" ]]; then
    case "$service" in query|processor) service_tag=lakehouse-v3 ;; ingestion) service_tag=lakehouse-v2 ;; *) service_tag=secure-v6 ;; esac
  fi
  docker build -t "context-graph/$service:$service_tag" -f "services/$service/Dockerfile" .
done
