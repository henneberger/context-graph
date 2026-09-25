#!/usr/bin/env bash
# Local trial only: keep both connections on the host loopback interface.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
pids=()
cleanup(){ for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM
# A service forward still attaches to one pod. Reconnect after pod replacement
# or a cluster restart, independently for each side of the bridge.
forward(){
  local child=''
  trap 'if [[ -n "$child" ]]; then kill "$child" 2>/dev/null || true; wait "$child" 2>/dev/null || true; fi; exit 0' INT TERM
  while true; do
    kubectl "$@" &
    child=$!
    wait "$child" || true
    child=''
    sleep 2 &
    child=$!
    wait "$child" || true
    child=''
  done
}
forward --kubeconfig "$root/.runtime/ax-kubeconfig" --context kind-context-ax -n ax-system port-forward service/ax-server 28080:8080 &
pids+=("$!")
forward --context docker-desktop -n context-graph port-forward service/investigation-api 28443:8443 &
pids+=("$!")
wait
