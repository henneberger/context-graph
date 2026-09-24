#!/usr/bin/env bash
# A bounded mixed-media demo; stopping the script also stops its child generators.
set -euo pipefail
cd "$(dirname "$0")/.."
url="${DASHBOARD_URL:-http://localhost:18088}"
duration="${DURATION:-60}"
token_file="${TOKEN_FILE:?Set TOKEN_FILE to a private verified JWT/token response file}"
workspace="${WORKSPACE_ID:-demo}"
entity="${ENTITY_ID:-alpha}"
auth=(--token-file "$token_file" --workspace "$workspace" --entity "$entity")
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM
python3 generators/load.py json --url "$url" "${auth[@]}" --rate 10 --concurrency 2 --entities 6 --duration "$duration" & pids+=("$!")
python3 generators/load.py images --url "$url" "${auth[@]}" --rate 0.2 --concurrency 1 --entities 2 --duration "$duration" & pids+=("$!")
python3 generators/load.py video --url "$url" "${auth[@]}" --concurrency 1 --duration "$duration" --stream-seconds 30 & pids+=("$!")
for pid in "${pids[@]}"; do wait "$pid"; done
