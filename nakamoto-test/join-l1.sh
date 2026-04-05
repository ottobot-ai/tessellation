#!/usr/bin/env bash
# Join L1 nodes 1 and 2 to node 0's cluster
set -euo pipefail

L1_0_ID="1b4b9f98190ede0d26ec1a2ce736638ffa556b08135403256d47d3c405e08e3bb272fb9172c7bd9f96008dd380ceb6201e965498bc9115bbdf905ad9781dbf18"

echo "Waiting for L1 node-0 to be ready..."
for i in $(seq 1 30); do
  state=$(curl -sf http://localhost:9100/node/info 2>/dev/null | jq -r '.state' 2>/dev/null || echo "unavailable")
  echo "  L1 node-0 state: $state (attempt $i)"
  if [ "$state" = "Ready" ]; then
    break
  fi
  sleep 5
done

echo ""
echo "Joining L1 node-1 to node-0..."
docker exec nakamoto-l1-1 curl -sf -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$L1_0_ID\",\"ip\":\"l1-node-0\",\"p2pPort\":9101}" \
  http://localhost:9112/cluster/join || echo "Join node-1 failed"

echo ""
echo "Joining L1 node-2 to node-0..."
docker exec nakamoto-l1-2 curl -sf -X POST \
  -H 'Content-Type: application/json' \
  -d "{\"id\":\"$L1_0_ID\",\"ip\":\"l1-node-0\",\"p2pPort\":9101}" \
  http://localhost:9122/cluster/join || echo "Join node-2 failed"

echo ""
echo "Checking cluster state..."
sleep 5
curl -sf http://localhost:9100/cluster/info | jq '.[].state' 2>/dev/null || echo "Cluster info unavailable"
