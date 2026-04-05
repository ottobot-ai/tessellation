#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Building and starting 3-node cluster ==="
GENESIS_TIME=$(($(date +%s%3N) + 45000))
export NAKAMOTO_GENESIS_TIME_MS=$GENESIS_TIME
docker compose build --no-cache > /dev/null 2>&1
docker compose up -d
echo "Genesis at $(date -d @$((GENESIS_TIME/1000)))"

echo "=== Waiting 55s for convergence ==="
sleep 55

echo "=== 3-Node Status ==="
for n in 0 1 2; do
  echo -n "node-$n: "
  docker logs nakamoto-node-$n 2>&1 | grep "stateProof:" | tail -1 | grep -oP 'ordinal=\S+ stateProof.*mptConsistency=\S+'
done

echo "=== Launching validator (node-3) ==="
docker compose --profile validator up -d sidecar-3 node-3

echo "=== Waiting 25s for validator to sync ==="
sleep 25

echo "=== Validator Status ==="
docker logs nakamoto-node-3 2>&1 | grep -E "genesisTime|📦|✅|Ready|WON|stateProof|FINALIZED|Error|error|InvalidNodeState" | head -15

echo "=== All 4 Nodes Latest ==="
for n in 0 1 2 3; do
  echo -n "node-$n: "
  docker logs nakamoto-node-$n 2>&1 | grep "stateProof:" | tail -1 | grep -oP 'ordinal=\S+ stateProof.*mptConsistency=\S+'
done

echo "=== Wins per Node ==="
for n in 0 1 2 3; do
  echo -n "node-$n: "
  docker logs nakamoto-node-$n 2>&1 | grep -c "WON slot"
done

echo "=== Done ==="
