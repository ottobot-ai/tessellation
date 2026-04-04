#!/bin/bash
# Compare LDD vs Praos fill rates
# Usage: ./compare-ldd-praos.sh [duration_seconds]
set -e

DURATION=${1:-300}  # 5 minutes default
COMPOSE="docker compose"
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

run_test() {
  local mode="$1"
  local logfile="$DIR/results-${mode}.log"
  local cutoff="${2:-15}"
  
  echo "=== Running $mode mode (cutoff=$cutoff) for ${DURATION}s ==="
  
  # Set genesis time 30s in the future
  export NAKAMOTO_GENESIS_TIME_MS=$(( ($(date +%s) + 30) * 1000 ))
  export NAKAMOTO_LDD_CUTOFF="$cutoff"
  
  # Clean start
  $COMPOSE down -v 2>/dev/null || true
  $COMPOSE up -d --build --force-recreate 2>/dev/null
  
  echo "Waiting ${DURATION}s for data collection..."
  sleep "$DURATION"
  
  # Collect results
  echo "=== $mode results ===" > "$logfile"
  for node in node-0 node-1 node-2; do
    echo "--- $node ---" >> "$logfile"
    docker logs "nakamoto-test-${node}-1" 2>&1 | grep -E "🎰 WON|📥 Received|✅ FINALIZED" >> "$logfile" 2>/dev/null || true
  done
  
  # Count wins per node
  echo "" >> "$logfile"
  echo "=== Win Summary ===" >> "$logfile"
  for node in node-0 node-1 node-2; do
    wins=$(docker logs "nakamoto-test-${node}-1" 2>&1 | grep -c "🎰 WON" || echo 0)
    echo "$node: $wins wins" >> "$logfile"
  done
  
  # Extract all slot wins with timestamps for interval analysis
  echo "" >> "$logfile"
  echo "=== Slot Wins (chronological) ===" >> "$logfile"
  for node in node-0 node-1 node-2; do
    docker logs "nakamoto-test-${node}-1" 2>&1 | grep "🎰 WON" | sed "s/^/${node}: /" >> "$logfile" 2>/dev/null || true
  done
  
  $COMPOSE down -v 2>/dev/null || true
  
  echo "Results saved to $logfile"
}

# Run LDD test (default params)
run_test "ldd" 15

# Run Praos test (cutoff=1)
run_test "praos" 1

# Summary
echo ""
echo "=== COMPARISON ==="
echo "--- LDD mode ---"
grep "wins$" "$DIR/results-ldd.log"
echo "--- Praos mode ---"
grep "wins$" "$DIR/results-praos.log"
echo ""
echo "Full results in results-ldd.log and results-praos.log"
