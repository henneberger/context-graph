#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tag="${IMAGE_TAG:-}"
for service in identity check-proxy ingestion processor query dashboard control kafka; do
  service_tag="$tag"
  if [[ -z "$service_tag" ]]; then
    service_tag=observability-v1
    if [[ "$service" == control || "$service" == dashboard ]]; then service_tag=observability-v2; fi
  fi
  docker build -t "context-graph/$service:$service_tag" -f "services/$service/Dockerfile" .
done
