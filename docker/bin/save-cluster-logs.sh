#!/bin/bash
# Save docker logs from all cluster containers before teardown.
# Usage: ./save-cluster-logs.sh [output-dir]
# Default output: /tmp/iter-logs-<timestamp>/

set -u

OUT_DIR="${1:-/tmp/iter-logs-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"

echo "Saving cluster logs to: $OUT_DIR"

# Hypergraph nodes (gl0/gl1 use indexed names like gl0-0, gl0-1, gl1-0, etc.)
# Plus metagraph nodes ml0-m0-N, cl1-m0-N, dl1-m0-N (per metagraph)
# Use docker ps to discover what's actually running.
for c in $(docker ps --format '{{.Names}}'); do
  echo "  $c"
  docker logs "$c" > "$OUT_DIR/${c}.log" 2>&1 || echo "    [failed to capture]"
done

# Save a summary of cluster state at capture time
{
  echo "=== Capture timestamp: $(date) ==="
  echo "=== Running containers ==="
  docker ps --format 'table {{.Names}}\t{{.Status}}'
  echo ""
  echo "=== gl0-0 latest snapshot ==="
  curl -sS --max-time 5 http://localhost:9000/global-snapshots/latest 2>/dev/null | python3 -m json.tool 2>/dev/null | head -20
  echo ""
  echo "=== gl0 nakamoto finalized ordinal (per node) ==="
  for i in 0 1 2 3 4 5 6 7; do
    port=$((9000 + i*10))
    fin=$(curl -sS --max-time 2 "http://localhost:${port}/global-snapshots/latest/finalized-ordinal" 2>/dev/null)
    echo "  gl0-$i (port $port): $fin"
  done
} > "$OUT_DIR/_capture-summary.txt" 2>&1

echo "Done. Logs saved to: $OUT_DIR"
echo "Run: ls -la $OUT_DIR"
