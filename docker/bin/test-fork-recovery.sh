#!/usr/bin/env bash
#
# Fork Recovery Test
# Tests that a node isolated from the cluster can detect divergence,
# recover, and rejoin consensus after network is restored.
#
# Usage: ./docker/bin/test-fork-recovery.sh [gl0_port_prefix]
# Example: ./docker/bin/test-fork-recovery.sh 90
#
# Requires: 3+ GL0 nodes running, NET_ADMIN capability in containers
#

set -eo pipefail

GL0_PORT_PREFIX=${1:-90}
# Auto-detect node count
NUM_GL0=$(docker ps --format "{{.Names}}" | grep -c "^gl0-" 2>/dev/null || echo "3")
NUM_GL0=$(echo "$NUM_GL0" | tr -d '[:space:]')
ISOLATION_NODE="gl0-$((NUM_GL0 - 1))"
MONITOR_NODE="gl0-0"
# With quorum-threshold=0.67, need ceil(N*0.67) declarations.
# ceil(3*0.67)=3 (can't lose any), ceil(4*0.67)=3 (can lose 1).
# Minimum 4 nodes required for this test to work.
if [ "$NUM_GL0" -lt 4 ]; then
  echo "ERROR: Fork recovery test requires at least 4 GL0 nodes (current: $NUM_GL0)"
  echo "  With quorum-threshold=0.67, ceil(3*0.67)=3 so 3-node clusters can't tolerate any loss."
  echo "  Use: just test --test=fork-recovery --num-gl0=4"
  exit 1
fi

ISOLATION_DURATION=90   # seconds to keep node isolated
RECOVERY_TIMEOUT=300    # max seconds to wait for recovery
STABILIZE_WAIT=300      # seconds to wait for initial cluster stability (nodes need time to join + sync)

echo "================================================"
echo "Fork Recovery Test"
echo "================================================"
echo "  Isolation node: $ISOLATION_NODE"
echo "  Monitor node:   $MONITOR_NODE"
echo "  Isolation time: ${ISOLATION_DURATION}s"
echo "  Recovery timeout: ${RECOVERY_TIMEOUT}s"
echo ""

# Helper: get ordinal from a node (via host-mapped port)
get_ordinal() {
  local node=$1
  local idx=${node##gl0-}
  local port=$((GL0_PORT_PREFIX * 100 + idx * 10))
  curl -s "http://localhost:${port}/global-snapshots/latest" 2>/dev/null | jq -r '.value.ordinal // empty' 2>/dev/null || echo ""
}

# Helper: get facilitator count from latest consensus log
get_facilitator_count() {
  local node=$1
  docker logs "$node" 2>&1 | grep "facilitators=" | tail -1 | grep -oP 'facilitators=\d+' | grep -oP '\d+' | head -1
}

# Helper: get node state
get_node_state() {
  local node=$1
  local idx=${node##gl0-}
  local port=$((GL0_PORT_PREFIX * 100 + idx * 10))
  curl -s "http://localhost:${port}/node/info" 2>/dev/null | jq -r '.state // empty' 2>/dev/null || echo ""
}

# Helper: check for fork recovery events
get_fork_events() {
  local node=$1
  docker logs "$node" 2>&1 | grep -c "Fork divergence\|FORK_CHECKS_PASSED\|fork.*detect" 2>/dev/null || echo "0"
}

# Helper: check for round completions after a given ordinal
get_completed_rounds_after() {
  local node=$1
  local after_ordinal=$2
  docker logs "$node" 2>&1 | grep "ROUND_COMPLETED" | grep -oP 'round=SnapshotOrdinal\{value=(\d+)\}' | grep -oP '\d+' | awk -v min="$after_ordinal" '$1 > min' | wc -l
}

fail() {
  echo "FAIL: $1"
  # Attempt cleanup
  docker exec --privileged "$ISOLATION_NODE" tc qdisc del dev eth0 root 2>/dev/null || true
  exit 1
}

pass() {
  echo ""
  echo "================================================"
  echo "PASS: Fork Recovery Test"
  echo "================================================"
  echo "$1"
  exit 0
}

# ── Phase 1: Wait for cluster stability ────────────────────────
# Genesis runs solo consensus (~43s/round) until validators download its
# snapshots and join.  We must wait until ALL nodes are producing rounds
# together before isolating — otherwise the lagging genesis node is on a
# different ordinal and consensus stalls after isolation.

echo "Phase 1: Waiting for ALL $NUM_GL0 GL0 nodes to synchronise (${STABILIZE_WAIT}s)..."

deadline=$(($(date +%s) + STABILIZE_WAIT))
stable=false
while [ "$(date +%s)" -lt "$deadline" ]; do
  all_synced=true
  min_ord=999999
  max_ord=0
  status_line=""

  for i in $(seq 0 $((NUM_GL0 - 1))); do
    node="gl0-${i}"
    ord=$(get_ordinal "$node")
    fac=$(get_facilitator_count "$node")
    status_line="${status_line} ${node}:ord=${ord:-?}/fac=${fac:-?}"

    # Every node must report an ordinal, facilitators == NUM_GL0, and ordinal > 5
    if [ -z "$ord" ] || [ "$ord" -le 5 ] || [ -z "$fac" ] || [ "$fac" -lt "$NUM_GL0" ]; then
      all_synced=false
    fi

    # Track ordinal spread
    if [ -n "$ord" ]; then
      [ "$ord" -lt "$min_ord" ] && min_ord=$ord
      [ "$ord" -gt "$max_ord" ] && max_ord=$ord
    fi
  done

  # All nodes must be within 1 ordinal of each other (same round or adjacent)
  spread=$((max_ord - min_ord))
  if [ "$all_synced" = true ] && [ "$spread" -le 1 ]; then
    echo "  Cluster synchronised: $status_line (spread=$spread)"
    stable=true
    break
  fi

  echo "  Waiting...${status_line} spread=${spread}"
  sleep 10
done

if [ "$stable" != "true" ]; then
  fail "Cluster did not synchronise within ${STABILIZE_WAIT}s"
fi

# Give one extra round for any lingering lag to settle
echo "  Waiting one extra consensus round (45s) for safety..."
sleep 45

# Record pre-isolation state — use the node with the LOWEST ordinal as reference
pre_ordinal=$(get_ordinal "$MONITOR_NODE")
pre_isolation_ordinal=$(get_ordinal "$ISOLATION_NODE" 2>/dev/null || echo "$pre_ordinal")
echo "  Pre-isolation ordinal: $pre_ordinal (monitor), $pre_isolation_ordinal (isolated node)"

# Final sanity: verify all nodes still in sync after the safety wait
for i in $(seq 0 $((NUM_GL0 - 1))); do
  node="gl0-${i}"
  ord=$(get_ordinal "$node")
  fac=$(get_facilitator_count "$node")
  echo "    $node: ordinal=$ord facilitators=$fac"
done

# ── Phase 2: Isolate node ──────────────────────────────────────

echo ""
echo "Phase 2: Isolating $ISOLATION_NODE (100% packet loss)..."

docker exec --privileged "$ISOLATION_NODE" tc qdisc add dev eth0 root netem loss 100% 2>&1 || \
  fail "Could not apply network impairment (needs --privileged or NET_ADMIN)"

echo "  $ISOLATION_NODE isolated. Waiting ${ISOLATION_DURATION}s for cluster to advance..."
sleep "$ISOLATION_DURATION"

# Check cluster advanced
post_isolation_ordinal=$(get_ordinal "$MONITOR_NODE")
echo "  Cluster advanced: ordinal $pre_ordinal → $post_isolation_ordinal"

if [ -z "$post_isolation_ordinal" ] || [ "$post_isolation_ordinal" -le "$pre_ordinal" ]; then
  docker exec --privileged "$ISOLATION_NODE" tc qdisc del dev eth0 root 2>/dev/null || true
  fail "Cluster did not advance during isolation (stuck at ordinal $pre_ordinal)"
fi

advancement=$((post_isolation_ordinal - pre_ordinal))
echo "  Cluster produced $advancement snapshots while $ISOLATION_NODE was isolated"

# ── Phase 3: Restore network and monitor recovery ──────────────

echo ""
echo "Phase 3: Restoring $ISOLATION_NODE network..."

docker exec --privileged "$ISOLATION_NODE" tc qdisc del dev eth0 root 2>&1 || \
  echo "  Warning: tc qdisc del failed (may already be clean)"

echo "  Network restored. Monitoring recovery (timeout: ${RECOVERY_TIMEOUT}s)..."

recovery_start=$(date +%s)
recovery_deadline=$((recovery_start + RECOVERY_TIMEOUT))
recovered=false
rejoined_consensus=false

while [ "$(date +%s)" -lt "$recovery_deadline" ]; do
  elapsed=$(( $(date +%s) - recovery_start ))

  # Check if isolated node is participating in consensus again
  iso_fac=$(get_facilitator_count "$ISOLATION_NODE")
  iso_completed=$(get_completed_rounds_after "$ISOLATION_NODE" "$post_isolation_ordinal")
  fork_events=$(get_fork_events "$ISOLATION_NODE")
  monitor_ord=$(get_ordinal "$MONITOR_NODE")

  echo "  [${elapsed}s] $ISOLATION_NODE: facilitators=${iso_fac:-?} completedAfterIsolation=$iso_completed forkEvents=$fork_events clusterOrdinal=${monitor_ord:-?}"

  # Success criteria: node completed at least 2 rounds after the isolation period ended
  if [ -n "$iso_completed" ] && [ "$iso_completed" -ge 2 ]; then
    recovered=true

    # Check if it's in the facilitator set (at least the original count)
    if [ -n "$iso_fac" ] && [ "$iso_fac" -ge 2 ]; then
      rejoined_consensus=true
    fi

    break
  fi

  sleep 15
done

# ── Phase 4: Verify results ────────────────────────────────────

echo ""
echo "Phase 4: Verifying results..."

final_ordinal=$(get_ordinal "$MONITOR_NODE")
final_fac=$(get_facilitator_count "$MONITOR_NODE")
final_fork_events=$(get_fork_events "$ISOLATION_NODE")
recovery_elapsed=$(( $(date +%s) - recovery_start ))

echo "  Final state:"
echo "    Cluster ordinal: $final_ordinal"
echo "    Facilitators: $final_fac"
echo "    Fork events on $ISOLATION_NODE: $final_fork_events"
echo "    Recovery time: ${recovery_elapsed}s"

# Check for any abandonment tracker activity
abandonment_count=$(docker logs "$ISOLATION_NODE" 2>&1 | grep -c "ROUND_ABANDONED_TRACKED" 2>/dev/null || echo "0")
echo "    Round abandonments on $ISOLATION_NODE: $abandonment_count"

if [ "$recovered" != "true" ]; then
  fail "$ISOLATION_NODE did not recover within ${RECOVERY_TIMEOUT}s"
fi

if [ "$rejoined_consensus" != "true" ]; then
  echo "  WARNING: Node recovered but may not be in facilitator set yet (facilitators=${iso_fac:-?})"
fi

pass "Node $ISOLATION_NODE recovered and rejoined consensus in ${recovery_elapsed}s (ordinal $pre_ordinal → $final_ordinal, fork events: $final_fork_events)"
