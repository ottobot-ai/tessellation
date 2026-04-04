#!/bin/bash
# Build and start the Nakamoto PoC local cluster.
# Sets a shared genesis time 45s in the future so all nodes start VRF production
# at the same wall clock moment.

set -e
cd "$(dirname "$0")/.."

echo "=== Staging dag-l0 ==="
source ~/.sdkman/bin/sdkman-init.sh
sbt -J-Xmx4g "dagL0/stage"

echo "=== Computing shared genesis time (now + 45s) ==="
GENESIS_MS=$(( ($(date +%s) + 45) * 1000 ))
export NAKAMOTO_GENESIS_TIME_MS=$GENESIS_MS
echo "Genesis time: $GENESIS_MS ($(date -d @$((GENESIS_MS / 1000)) '+%Y-%m-%d %H:%M:%S'))"

echo "=== Starting cluster ==="
cd nakamoto-test
docker compose down -v 2>/dev/null || true
docker compose up --build -d

echo "=== Cluster started ==="
echo "Genesis in ~45s. Nodes will idle until then."
echo "Monitor: docker compose logs -f"
