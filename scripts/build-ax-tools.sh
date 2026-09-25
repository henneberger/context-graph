#!/usr/bin/env bash
set -euo pipefail
# No deployment or current-context mutation. Source and binaries stay outside Git.
root="$(cd "$(dirname "$0")/.." && pwd)"
source_dir="$root/.runtime/upstream-ax"
revision=0b5427c4bb3d79889f5d536538a46421190ada9d
if [[ ! -d "$source_dir/.git" ]]; then git clone --filter=blob:none https://github.com/google/ax "$source_dir"; fi
git -C "$source_dir" checkout --detach "$revision"
if git -C "$source_dir" apply --check "$root/deploy/ax/task-limits.patch" 2>/dev/null; then
  git -C "$source_dir" apply "$root/deploy/ax/task-limits.patch"
else
  git -C "$source_dir" apply --reverse --check "$root/deploy/ax/task-limits.patch"
fi
mkdir -p "$root/.build"
cd "$source_dir"
GOTOOLCHAIN=auto go build -o "$root/.runtime/ax" ./cmd/ax
GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=linux GOARCH="${AX_TARGET_ARCH:-arm64}" go build -o "$root/.build/ax" ./cmd/ax
GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=linux GOARCH="${AX_TARGET_ARCH:-arm64}" go build -o "$root/.build/ax-task-runner" ./cmd/ax-task-runner

GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=linux GOARCH="${AX_TARGET_ARCH:-arm64}" go build -o "$root/.build/ax-server" ./cmd/ax-server
GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=linux GOARCH="${AX_TARGET_ARCH:-arm64}" go build -o "$root/.build/ax-controller" ./cmd/ax-controller
