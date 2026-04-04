#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Building dag-l0 JARs ==="
cd "$REPO_DIR"
sbt "dagL0/stage"

echo ""
echo "=== Building Go sidecar ==="
cd "$REPO_DIR/p2p"
CGO_ENABLED=0 go build -o sidecar ./cmd/sidecar

echo ""
echo "=== Starting Nakamoto cluster (3 nodes + 3 sidecars) ==="
cd "$SCRIPT_DIR"
docker compose up --build "$@"
