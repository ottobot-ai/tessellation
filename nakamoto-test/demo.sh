#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  Nakamoto Consensus Demo
#  Brings up GL0 (3 nodes) + DAG-L1 (3 nodes) + Grafana/Prometheus
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE_DIR="$(dirname "$0")"
NODES=${NODES:-3}           # GL0 node count (3 or 8)
WITH_L1=${WITH_L1:-true}    # bring up L1 layer
WITH_MON=${WITH_MON:-true}  # bring up monitoring

echo "╔══════════════════════════════════════╗"
echo "║   Tessellation Nakamoto Demo         ║"
echo "║   GL0: ${NODES} nodes                       ║"
echo "║   L1:  $([ "$WITH_L1" = true ] && echo "3 nodes" || echo "off")                      ║"
echo "║   Monitoring: $([ "$WITH_MON" = true ] && echo "on " || echo "off")                  ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── Step 1: Build JARs ──────────────────────────────────
echo "=== [1/5] Building dag-l0 JARs ==="
source ~/.sdkman/bin/sdkman-init.sh 2>/dev/null || true
sbt -J-Xmx4g "dagL0/clean" "dagL0/stage"

if [ "$WITH_L1" = true ]; then
  echo "=== [1b/5] Building dag-l1 JARs ==="
  sbt -J-Xmx4g "dagL1/stage"
fi

# ── Step 2: Tear down any existing cluster ───────────────
echo ""
echo "=== [2/5] Cleaning up previous containers ==="
cd "$COMPOSE_DIR"
docker compose down -v 2>/dev/null || true
docker compose -f docker-compose-l1.yml down -v 2>/dev/null || true
docker compose -f docker-compose-monitoring.yml down -v 2>/dev/null || true

# ── Step 3: Start GL0 ───────────────────────────────────
echo ""
echo "=== [3/5] Starting GL0 cluster (${NODES} nodes) ==="
GENESIS_MS=$(( ($(date +%s) + 60) * 1000 ))
export NAKAMOTO_GENESIS_TIME_MS=$GENESIS_MS
echo "Genesis time: $(date -d @$((GENESIS_MS / 1000)) '+%H:%M:%S') (60s from now)"

COMPOSE_FILE="docker-compose.yml"
[ "$NODES" -eq 8 ] && COMPOSE_FILE="docker-compose-8node.yml"
docker compose -f "$COMPOSE_FILE" up --build -d

# ── Step 4: Start monitoring ────────────────────────────
if [ "$WITH_MON" = true ]; then
  echo ""
  echo "=== [4/5] Starting Prometheus + Grafana ==="
  docker compose -f docker-compose-monitoring.yml up -d
fi

# ── Step 5: Wait for GL0 ready, then start L1 ───────────
echo ""
echo "=== [5/5] Waiting for GL0 nodes to reach Ready ==="
for port in 9000 9010 9020; do
  for attempt in $(seq 1 90); do
    state=$(curl -sf "http://localhost:${port}/node/info" 2>/dev/null | jq -r '.state' 2>/dev/null || echo "starting")
    if [ "$state" = "Ready" ]; then
      echo "  ✓ GL0 :${port} Ready"
      break
    fi
    [ $((attempt % 10)) -eq 0 ] && echo "  … GL0 :${port} state=$state (${attempt}s)"
    sleep 1
  done
done

if [ "$WITH_L1" = true ]; then
  echo ""
  echo "=== Starting DAG-L1 layer ==="
  docker compose -f docker-compose-l1.yml up --build -d
  sleep 10
  bash join-l1.sh
fi

# ── Summary ─────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  Demo Running!                                      ║"
echo "╟──────────────────────────────────────────────────────╢"
echo "║  GL0 nodes:                                         ║"
echo "║    http://localhost:9000/node/info   (node-0)       ║"
echo "║    http://localhost:9010/node/info   (node-1)       ║"  
echo "║    http://localhost:9020/node/info   (node-2)       ║"
if [ "$WITH_MON" = true ]; then
echo "║                                                     ║"
echo "║  Dashboards:                                        ║"
echo "║    Grafana:     http://localhost:3000                ║"
echo "║                 (admin/admin, no login required)     ║"
echo "║    Prometheus:  http://localhost:9090                ║"
fi
if [ "$WITH_L1" = true ]; then
echo "║                                                     ║"
echo "║  L1 nodes:                                          ║"
echo "║    http://localhost:9100/node/info   (l1-0)         ║"
fi
echo "║                                                     ║"
echo "║  Useful commands:                                   ║"
echo "║    docker compose logs -f node-0    # GL0 logs      ║"
echo "║    docker compose logs -f           # all logs      ║"
echo "║    bash test-validator.sh           # add 4th node  ║"
echo "║    bash demo.sh stop                # tear down     ║"
echo "╚══════════════════════════════════════════════════════╝"

# ── Stop command ─────────────────────────────────────────
if [ "${1:-}" = "stop" ]; then
  echo "Stopping all containers..."
  docker compose -f docker-compose-monitoring.yml down -v 2>/dev/null || true
  docker compose -f docker-compose-l1.yml down -v 2>/dev/null || true
  docker compose down -v 2>/dev/null || true
  echo "Done."
fi
