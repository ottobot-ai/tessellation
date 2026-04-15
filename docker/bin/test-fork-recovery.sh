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
MONITOR_NODE="gl0-1"

# ── Mode detection ────────────────────────────────────────────
# Nakamoto mode is detected by the presence of SnapshotLeaderLoop logs.
NAKAMOTO_MODE=false
_nk_count=$(docker logs gl0-0 2>&1 | grep -c "SnapshotLeaderLoop" || true)
if [ "$_nk_count" -gt 0 ] 2>/dev/null; then
  NAKAMOTO_MODE=true
fi

if [ "$NAKAMOTO_MODE" = "true" ]; then
  # Nakamoto: 3 nodes is fine (longest chain + GRANDPA finality, no BFT quorum).
  if [ "$NUM_GL0" -lt 3 ]; then
    echo "ERROR: Fork recovery test requires at least 3 GL0 nodes (current: $NUM_GL0)"
    exit 1
  fi
  # Nakamoto slots are ~10s; 90s isolation = ~9 ordinals of divergence.
  ISOLATION_DURATION=90
  RECOVERY_TIMEOUT=300
  STABILIZE_WAIT=300
else
  # BFT: need 5 nodes for quorum safety.
  if [ "$NUM_GL0" -lt 5 ]; then
    echo "ERROR: Fork recovery test (BFT) requires at least 5 GL0 nodes (current: $NUM_GL0)"
    echo "  With quorum-threshold=0.67, isolating 1 of 4 leaves ceil(3*0.67)=3 (zero fault tolerance)."
    echo "  With 5 nodes, isolating 1 leaves 4 and ceil(4*0.67)=3 (tolerates 1 additional failure)."
    echo "  Use: just test --test=fork-recovery --num-gl0=5"
    exit 1
  fi
  ISOLATION_DURATION=270
  RECOVERY_TIMEOUT=900
  STABILIZE_WAIT=480
fi

echo "================================================"
echo "Fork Recovery Test ($( [ "$NAKAMOTO_MODE" = "true" ] && echo "Nakamoto" || echo "BFT" ))"
echo "================================================"
echo "  Isolation node: $ISOLATION_NODE"
echo "  Monitor node:   $MONITOR_NODE"
echo "  Isolation time: ${ISOLATION_DURATION}s"
echo "  Recovery timeout: ${RECOVERY_TIMEOUT}s"
echo ""

# Helper: get finalized ordinal from a node (via host-mapped port)
get_ordinal() {
  local node=$1
  local idx=${node##gl0-}
  local port=$((GL0_PORT_PREFIX * 100 + idx * 10))
  curl -s "http://localhost:${port}/global-snapshots/latest" 2>/dev/null | jq -r '.value.ordinal // empty' 2>/dev/null || echo ""
}

# Helper: get facilitator count from latest consensus log (BFT only)
get_facilitator_count() {
  local node=$1
  local result
  result=$(docker logs "$node" 2>&1 | grep "facilitators=" | tail -1 | grep -oP 'facilitators=\d+' | grep -oP '\d+' | head -1 || true)
  echo "${result:-0}"
}

# Helper: get last finalized ordinal from Nakamoto chain store logs
get_nakamoto_finalized() {
  local node=$1
  local result
  result=$(docker logs "$node" 2>&1 | grep "Finalized ordinal=" | tail -1 | grep -oP 'ordinal=\d+' | grep -oP '\d+' | head -1 || true)
  echo "${result:-0}"
}

# Helper: count Nakamoto finalizations after a given ordinal
get_finalizations_after() {
  local node=$1
  local after_ordinal=$2
  local result
  result=$(docker logs "$node" 2>&1 | grep "Finalized ordinal=" | grep -oP 'ordinal=\d+' | grep -oP '\d+' | awk -v min="$after_ordinal" '$1 > min' | wc -l || true)
  echo "${result:-0}"
}

# Helper: get node state
get_node_state() {
  local node=$1
  local idx=${node##gl0-}
  local port=$((GL0_PORT_PREFIX * 100 + idx * 10))
  curl -s "http://localhost:${port}/node/info" 2>/dev/null | jq -r '.state // empty' 2>/dev/null || echo ""
}

# Helper: check for fork recovery events (BFT only)
get_fork_events() {
  local node=$1
  local result
  result=$(docker logs "$node" 2>&1 | grep -c "Fork divergence\|FORK_CHECKS_PASSED\|fork.*detect" 2>/dev/null || true)
  echo "${result:-0}"
}

# Helper: check for round completions after a given ordinal (BFT only)
get_completed_rounds_after() {
  local node=$1
  local after_ordinal=$2
  local result
  result=$(docker logs "$node" 2>&1 | grep "Round finished ordinal=" | grep -oP 'ordinal=\d+' | grep -oP '\d+' | awk -v min="$after_ordinal" '$1 > min' | wc -l || true)
  echo "${result:-0}"
}

fail() {
  echo "FAIL: $1"
  # Attempt cleanup
  docker exec --privileged "$ISOLATION_NODE" tc qdisc del dev eth0 root 2>/dev/null || true
  if [ "$NAKAMOTO_MODE" = "true" ]; then
    local net
    net=$(docker network ls --format '{{.Name}}' | grep tessellation | head -1)
    [ -n "$net" ] && docker network connect "$net" "sidecar-${ISOLATION_NODE##gl0-}" 2>/dev/null || true
  fi
  exit 1
}

pass() {
  echo ""
  echo "================================================"
  echo "PASS: Fork Recovery Test ($( [ "$NAKAMOTO_MODE" = "true" ] && echo "Nakamoto" || echo "BFT" ))"
  echo "================================================"
  echo "$1"
  exit 0
}

# ── Phase 1: Wait for cluster stability ────────────────────────

echo "Phase 1: Waiting for cluster to synchronise (${STABILIZE_WAIT}s)..."

deadline=$(($(date +%s) + STABILIZE_WAIT))
stable=false

if [ "$NAKAMOTO_MODE" = "true" ]; then
  # Nakamoto: wait until all nodes are finalizing at the same ordinal (within 2).
  while [ "$(date +%s)" -lt "$deadline" ]; do
    all_synced=true
    min_ord=999999
    max_ord=0
    status_line=""

    for i in $(seq 0 $((NUM_GL0 - 1))); do
      node="gl0-${i}"
      ord=$(get_ordinal "$node")
      fin=$(get_nakamoto_finalized "$node")
      status_line="${status_line} ${node}:ord=${ord:-?}/fin=${fin:-?}"

      if [ -z "$ord" ] || [ "$ord" -lt 5 ]; then
        all_synced=false
      fi

      if [ -n "$ord" ]; then
        [ "$ord" -lt "$min_ord" ] && min_ord=$ord
        [ "$ord" -gt "$max_ord" ] && max_ord=$ord
      fi
    done

    spread=$((max_ord - min_ord))
    if [ "$all_synced" = true ] && [ "$spread" -le 2 ]; then
      echo "  Nodes synchronised: $status_line (spread=$spread)"
      stable=true
      break
    fi

    echo "  Waiting...${status_line} spread=${spread}"
    sleep 10
  done

else
  # BFT: original facilitator-based checks.
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

      # Skip genesis (gl0-0) for sync checks — it often falls behind
      if [ "$i" -eq 0 ]; then
        continue
      fi

      if [ -z "$ord" ] || [ "$ord" -lt 5 ] || [ "${fac:-0}" -lt "$NUM_GL0" ]; then
        all_synced=false
      fi

      if [ -n "$ord" ]; then
        [ "$ord" -lt "$min_ord" ] && min_ord=$ord
        [ "$ord" -gt "$max_ord" ] && max_ord=$ord
      fi
    done

    spread=$((max_ord - min_ord))
    if [ "$all_synced" = true ] && [ "$spread" -le 1 ]; then
      echo "  Validators synchronised: $status_line (spread=$spread)"
      stable=true
      break
    fi

    echo "  Waiting...${status_line} spread=${spread}"
    sleep 10
  done

  # BFT: additional wait for facilitator stability.
  if [ "$stable" = "true" ]; then
    echo "  Waiting for ${NUM_GL0}-node consensus to stabilize (3+ rounds)..."
    stable_rounds=0
    stab_deadline=$(($(date +%s) + 300))
    while [ "$(date +%s)" -lt "$stab_deadline" ] && [ "$stable_rounds" -lt 3 ]; do
      fac=$(get_facilitator_count "$MONITOR_NODE")
      ord=$(get_ordinal "$MONITOR_NODE")
      if [ "${fac:-0}" -eq "$NUM_GL0" ]; then
        stable_rounds=$((stable_rounds + 1))
        echo "    Round $stable_rounds/3 with fac=$fac at ordinal $ord"
      else
        stable_rounds=0
        echo "    Waiting... fac=${fac:-?} at ordinal ${ord:-?} (need $NUM_GL0)"
      fi
      sleep 45
    done
    if [ "$stable_rounds" -lt 3 ]; then
      echo "  WARNING: Only $stable_rounds/3 stable rounds achieved, proceeding anyway"
    fi
  fi
fi

if [ "$stable" != "true" ]; then
  fail "Cluster did not synchronise within ${STABILIZE_WAIT}s"
fi

# Record pre-isolation state from monitor
pre_ordinal=$(get_ordinal "$MONITOR_NODE")
pre_isolation_ordinal=$(get_ordinal "$ISOLATION_NODE" 2>/dev/null || echo "$pre_ordinal")
echo "  Pre-isolation ordinal: $pre_ordinal (monitor), $pre_isolation_ordinal (isolated node)"

for i in $(seq 0 $((NUM_GL0 - 1))); do
  node="gl0-${i}"
  ord=$(get_ordinal "$node")
  if [ "$NAKAMOTO_MODE" = "true" ]; then
    fin=$(get_nakamoto_finalized "$node")
    echo "    $node: ordinal=$ord finalized=$fin"
  else
    fac=$(get_facilitator_count "$node")
    echo "    $node: ordinal=$ord facilitators=$fac"
  fi
done

# ── Phase 2: Isolate node ──────────────────────────────────────

echo ""
echo "Phase 2: Isolating $ISOLATION_NODE (100% packet loss)..."

docker exec --privileged "$ISOLATION_NODE" tc qdisc add dev eth0 root netem loss 100% 2>&1 || \
  fail "Could not apply network impairment on $ISOLATION_NODE (needs --privileged or NET_ADMIN)"

# In Nakamoto mode, also isolate the sidecar — it's a separate container with
# its own network namespace. The sidecar image is minimal (no tc binary), so
# we use docker network disconnect instead.
if [ "$NAKAMOTO_MODE" = "true" ]; then
  SIDECAR_NAME="sidecar-${ISOLATION_NODE##gl0-}"
  DOCKER_NETWORK=$(docker network ls --format '{{.Name}}' | grep tessellation | head -1)
  if [ -n "$DOCKER_NETWORK" ]; then
    docker network disconnect "$DOCKER_NETWORK" "$SIDECAR_NAME" 2>&1 || \
      echo "  Warning: could not disconnect $SIDECAR_NAME from $DOCKER_NETWORK"
    echo "  $ISOLATION_NODE (tc) + $SIDECAR_NAME (network disconnect) isolated."
  else
    echo "  Warning: could not find tessellation Docker network for sidecar isolation"
  fi
fi

echo "  Waiting ${ISOLATION_DURATION}s for cluster to advance..."
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
  echo "  Warning: tc qdisc del on $ISOLATION_NODE failed (may already be clean)"

if [ "$NAKAMOTO_MODE" = "true" ]; then
  SIDECAR_NAME="sidecar-${ISOLATION_NODE##gl0-}"
  DOCKER_NETWORK=$(docker network ls --format '{{.Name}}' | grep tessellation | head -1)
  if [ -n "$DOCKER_NETWORK" ]; then
    docker network connect "$DOCKER_NETWORK" "$SIDECAR_NAME" 2>&1 || \
      echo "  Warning: could not reconnect $SIDECAR_NAME to $DOCKER_NETWORK"
    echo "  $SIDECAR_NAME reconnected to $DOCKER_NETWORK"
  fi
fi

echo "  Network restored. Monitoring recovery (timeout: ${RECOVERY_TIMEOUT}s)..."
if [ "$NAKAMOTO_MODE" = "true" ]; then
  echo "  (Sidecar mesh health monitor should auto-reconnect within ~60s)"
fi

recovery_start=$(date +%s)
recovery_deadline=$((recovery_start + RECOVERY_TIMEOUT))
recovered=false

if [ "$NAKAMOTO_MODE" = "true" ]; then
  # Nakamoto recovery: the isolated node receives blocks via GossipSub,
  # backfills missing blocks via ChainSync, and resumes finalization.
  # Success: isolated node's finalized ordinal catches up to within 2 of the cluster.
  while [ "$(date +%s)" -lt "$recovery_deadline" ]; do
    elapsed=$(( $(date +%s) - recovery_start ))

    iso_ord=$(get_ordinal "$ISOLATION_NODE")
    iso_fin=$(get_nakamoto_finalized "$ISOLATION_NODE")
    monitor_ord=$(get_ordinal "$MONITOR_NODE")
    new_finalizations=$(get_finalizations_after "$ISOLATION_NODE" "$post_isolation_ordinal")

    echo "  [${elapsed}s] $ISOLATION_NODE: ordinal=${iso_ord:-?} finalized=${iso_fin:-?} newFinalizations=$new_finalizations clusterOrdinal=${monitor_ord:-?}"

    # Success: isolated node's ordinal is within 2 of cluster AND
    # has finalized blocks beyond the isolation point.
    if [ -n "$iso_ord" ] && [ -n "$monitor_ord" ] && [ "$iso_ord" -ge "$((monitor_ord - 2))" ] && \
       [ -n "$new_finalizations" ] && [ "$new_finalizations" -ge 1 ]; then
      recovered=true
      break
    fi

    sleep 15
  done

else
  # BFT recovery: original facilitator + round-based checks.
  while [ "$(date +%s)" -lt "$recovery_deadline" ]; do
    elapsed=$(( $(date +%s) - recovery_start ))

    iso_fac=$(get_facilitator_count "$ISOLATION_NODE")
    iso_completed=$(get_completed_rounds_after "$ISOLATION_NODE" "$post_isolation_ordinal")
    fork_events=$(get_fork_events "$ISOLATION_NODE")
    monitor_ord=$(get_ordinal "$MONITOR_NODE")

    echo "  [${elapsed}s] $ISOLATION_NODE: facilitators=${iso_fac:-?} completedAfterIsolation=$iso_completed forkEvents=$fork_events clusterOrdinal=${monitor_ord:-?}"

    iso_ord=$(get_ordinal "$ISOLATION_NODE")
    cluster_advanced=false
    if [ -n "$monitor_ord" ] && [ -n "$post_isolation_ordinal" ] && [ "$monitor_ord" -ge "$((post_isolation_ordinal + 5))" ]; then
      cluster_advanced=true
    fi
    iso_caught_up=false
    if [ -n "$iso_ord" ] && [ -n "$monitor_ord" ] && [ "$iso_ord" -ge "$((monitor_ord - 1))" ]; then
      iso_caught_up=true
    fi

    if [ -n "$iso_completed" ] && [ "$iso_completed" -ge 1 ]; then
      recovered=true
      break
    fi
    if [ "$cluster_advanced" = "true" ] && [ "$iso_caught_up" = "true" ]; then
      recovered=true
      echo "  Criterion B: cluster advanced >=5 ordinals and $ISOLATION_NODE caught up (iso_ord=$iso_ord cluster_ord=$monitor_ord)"
      break
    fi

    sleep 15
  done
fi

# ── Phase 4: Verify results ────────────────────────────────────

echo ""
echo "Phase 4: Verifying results..."

final_ordinal=$(get_ordinal "$MONITOR_NODE")
recovery_elapsed=$(( $(date +%s) - recovery_start ))

if [ "$NAKAMOTO_MODE" = "true" ]; then
  iso_fin=$(get_nakamoto_finalized "$ISOLATION_NODE")
  iso_ord=$(get_ordinal "$ISOLATION_NODE")
  new_finalizations=$(get_finalizations_after "$ISOLATION_NODE" "$post_isolation_ordinal")

  echo "  Final state:"
  echo "    Cluster ordinal: $final_ordinal"
  echo "    $ISOLATION_NODE ordinal: $iso_ord"
  echo "    $ISOLATION_NODE finalized: $iso_fin"
  echo "    New finalizations after isolation: $new_finalizations"
  echo "    Recovery time: ${recovery_elapsed}s"

  if [ "$recovered" != "true" ]; then
    fail "$ISOLATION_NODE did not recover within ${RECOVERY_TIMEOUT}s"
  fi

  pass "Node $ISOLATION_NODE recovered in ${recovery_elapsed}s (ordinal $pre_isolation_ordinal → $iso_ord, finalized=$iso_fin, cluster=$final_ordinal)"

else
  final_fac=$(get_facilitator_count "$MONITOR_NODE")
  final_fork_events=$(get_fork_events "$ISOLATION_NODE")
  iso_completed=$(get_completed_rounds_after "$ISOLATION_NODE" "$post_isolation_ordinal")

  echo "  Final state:"
  echo "    Cluster ordinal: $final_ordinal"
  echo "    Facilitators: $final_fac"
  echo "    Fork events on $ISOLATION_NODE: $final_fork_events"
  echo "    Recovery time: ${recovery_elapsed}s"

  abandonment_count=$(docker logs "$ISOLATION_NODE" 2>&1 | grep -c "ROUND_ABANDONED_TRACKED" 2>/dev/null || echo "0")
  echo "    Round abandonments on $ISOLATION_NODE: $abandonment_count"

  if [ "$recovered" != "true" ]; then
    fail "$ISOLATION_NODE did not recover within ${RECOVERY_TIMEOUT}s"
  fi

  pass "Node $ISOLATION_NODE recovered and rejoined consensus in ${recovery_elapsed}s (ordinal $pre_ordinal → $final_ordinal, completedAfterIsolation=$iso_completed fork events: $final_fork_events)"
fi
