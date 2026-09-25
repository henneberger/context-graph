#!/usr/bin/env bash
# Local trial only: keep both connections on the host loopback interface.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
pids=()
cleanup(){ for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT INT TERM
kubectl --kubeconfig "$root/.runtime/ax-kubeconfig" --context kind-context-ax -n ax-system port-forward service/ax-server 28080:8080 &
pids+=("$!")
kubectl --context docker-desktop -n context-graph port-forward service/investigation-api 28443:8443 &
pids+=("$!")
wait
