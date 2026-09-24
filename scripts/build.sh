#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
tag="${IMAGE_TAG:-}"
for service in identity check-proxy ingestion processor query dashboard control kafka connectors search-api search-ui control-ui; do
  service_tag="$tag"
  if [[ -z "$service_tag" ]]; then
    service_tag=observability-v1
    case "$service" in
      query|dashboard|control|search-api|search-ui|control-ui) service_tag=search-v1 ;;
      connectors) service_tag=search-v2 ;;
    esac
  fi
  docker build -t "context-graph/$service:$service_tag" -f "services/$service/Dockerfile" .
done
