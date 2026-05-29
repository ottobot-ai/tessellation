

check_health() {
  local url=$1
  local service=$2
  local num_expected_nodes=$3
  local max_retries=${4:-$MAX_RETRIES}
  # Poll cluster until it has enough nodes
  local retry_count=0
  while [ $retry_count -lt $max_retries ]; do
      CLUSTER_INFO=$(curl -s --connect-timeout 5 --max-time 10 ${url}/cluster/info 2>/dev/null) || CLUSTER_INFO=""

      # Check if curl returned valid JSON before using jq
      if [ -z "$CLUSTER_INFO" ] || [ "$CLUSTER_INFO" = "null" ]; then
        if [ "$((retry_count % 10))" -eq 0 ]; then
          echo "Waiting for $service at $url to come online (attempt $((retry_count+1))/$max_retries)"
        fi
        sleep 5
        retry_count=$((retry_count+1))
        continue
      fi

      # Use jq with error handling
      CLUSTER_INFO_LEN=$(echo "$CLUSTER_INFO" | jq 'length' 2>/dev/null || echo "0")

      if [ "$CLUSTER_INFO_LEN" -ge "$num_expected_nodes" ]; then
        echo "Success: cluster $service has $CLUSTER_INFO_LEN nodes (>= $num_expected_nodes expected) at $url"
        return 0
      else
        if [ "$((retry_count % 5))" -eq 0 ]; then
          echo "Waiting for $service at $url to have >= $num_expected_nodes nodes, currently $CLUSTER_INFO_LEN nodes (attempt $((retry_count+1))/$max_retries)"
        fi
        sleep 4
        retry_count=$((retry_count+1))
      fi
    done

  echo "ERROR: $service cluster doesn't have >= $num_expected_nodes nodes at $url after $max_retries attempts"
  # Dump container logs for diagnostics
  docker logs "$service" 2>&1 | tail -30 || true
  return 1
}

# Check all nodes of a given layer in parallel, fail if any fails
check_layer_parallel() {
  local layer_name=$1
  local port_prefix=$2
  local num_nodes=$3
  local num_expected=$4
  local host=$5

  local pids=()
  local nodes=()
  for i in $(seq 0 $((num_nodes - 1))); do
    local url="${host}:${port_prefix}${i}0"
    local service="${layer_name}-${i}"
    check_health "$url" "$service" "$num_expected" &
    pids+=($!)
    nodes+=("$service")
  done

  local failed=false
  for idx in "${!pids[@]}"; do
    if ! wait "${pids[$idx]}"; then
      echo "FAILED: ${nodes[$idx]} health check timed out"
      failed=true
    fi
  done

  if [ "$failed" = "true" ]; then
    return 1
  fi
  return 0
}

verify_healthy() {
  echo "Sending cluster poll health request for cluster info to check joined."
  MAX_RETRIES=200
  local host=${TEST_HOST:-http://localhost}

  if [ "$host" != "http://localhost" ]; then
    # Remote host: check one endpoint per layer using configured URLs
    if [ "$NUM_GL0_NODES" -gt 0 ]; then
      check_health "$GL0_URL" "gl0" 1
    fi
    if [ "$NUM_GL1_NODES" -gt 0 ]; then
      check_health "$GL1_URL" "gl1" 1
    fi
    if [ "$NUM_ML0_NODES" -gt 0 ]; then
      check_health "$ML0_URL" "ml0" 1
    fi
    if [ "$NUM_CL1_NODES" -gt 0 ]; then
      check_health "$CL1_URL" "cl1" 1
    fi
    if [ "$NUM_DL1_NODES" -gt 0 ]; then
      check_health "$DL1_URL" "dl1" 1
    fi
  else
    # Local docker: check node-0 of each layer for full cluster size.
    # Node-0 (genesis) is authoritative — if its /cluster/info reports N nodes,
    # all N nodes have joined the consensus cluster. Checking every node's HTTP
    # port individually is flaky under CI load (non-genesis nodes may have slow
    # Docker port mapping even while actively participating in consensus).
    local any_failed=false

    if [ "$NUM_GL0_NODES" -gt 0 ]; then
      check_health "${host}:${DAG_L0_PORT_PREFIX}00" "gl0-0" "$NUM_GL0_NODES" || any_failed=true

      # Nakamoto GL0: also wait for finality before proceeding. Non-GL0 consumers
      # can ONLY see finalized snapshots, so starting metagraphs/L1s before GL0 has
      # finalized anything would leave them stuck on NotFound.
      echo "Waiting for GL0 to reach finality before starting other layers..."
      local gl0_finality_url="${host}:${DAG_L0_PORT_PREFIX}00"
      for finality_attempt in $(seq 1 120); do
        finalized=$(curl -s --connect-timeout 3 --max-time 5 "${gl0_finality_url}/global-snapshots/latest/finalized-ordinal" 2>/dev/null || echo "")
        if [ -n "$finalized" ] && echo "$finalized" | jq -e '.value > 0' >/dev/null 2>&1; then
          fin_val=$(echo "$finalized" | jq '.value')
          echo "GL0 finality reached: ordinal=$fin_val"
          break
        fi
        if [ "$((finality_attempt % 10))" -eq 0 ]; then
          echo "GL0 not yet finalized (attempt $finality_attempt/120)..."
        fi
        sleep 3
      done
    fi

    if [ "$NUM_GL1_NODES" -gt 0 ]; then
      # gl1-0's EXTERNAL host port is GL1_EXT_PORT_BASE (9600), NOT the
      # DAG_L1_PORT_PREFIX (9100) band — that's the gl1 container-internal port.
      # GL1_URL is set by set-env.sh to the gl1 external base for local runs.
      local gl1_health_url="${GL1_URL:-${host}:${GL1_EXT_PORT_BASE}}"
      check_health "$gl1_health_url" "gl1-0" "$NUM_GL1_NODES" || any_failed=true
    fi

    # Per-metagraph health: each metagraph k has its own ml0/cl1/dl1 cluster
    # at port-prefix shifted -10 per k (m0=92/93/94, m1=82/83/84, ...) and
    # container names ml0-m${k}-0, cl1-m${k}-0, dl1-m${k}-0.
    for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
      local m_ml0_pp=$((ML0_PORT_PREFIX - k*10))
      local m_cl1_pp=$((CL1_PORT_PREFIX - k*10))
      local m_dl1_pp=$((DL1_PORT_PREFIX - k*10))

      if [ "$NUM_ML0_NODES" -gt 0 ]; then
        check_health "${host}:${m_ml0_pp}00" "ml0-m${k}-0" "$NUM_ML0_NODES" || any_failed=true
      fi

      if [ "$NUM_CL1_NODES" -gt 0 ]; then
        check_health "${host}:${m_cl1_pp}00" "cl1-m${k}-0" "$NUM_CL1_NODES" || any_failed=true
      fi

      if [ "$NUM_DL1_NODES" -gt 0 ]; then
        check_health "${host}:${m_dl1_pp}00" "dl1-m${k}-0" "$NUM_DL1_NODES" || any_failed=true
      fi

      # Operational readiness probe: peer-count alone isn't enough. DL1 joins
      # the cluster before it has received its first currency snapshot from
      # ML0, and /data POSTs return 500 during that window. Probe proxy: ML0
      # ordinal >= 1 (past genesis 0).
      if [ "$NUM_ML0_NODES" -gt 0 ]; then
        local ml0_ordinal_url="${host}:${m_ml0_pp}00"
        echo "Waiting for metagraph $k ML0 to produce its first snapshot..."
        for op_attempt in $(seq 1 120); do
          ordinal_resp=$(curl -s --connect-timeout 3 --max-time 5 "${ml0_ordinal_url}/snapshots/latest/ordinal" 2>/dev/null || echo "")
          if [ -n "$ordinal_resp" ] && echo "$ordinal_resp" | jq -e '.value >= 1' >/dev/null 2>&1; then
            ord_val=$(echo "$ordinal_resp" | jq '.value')
            echo "Metagraph $k ML0 operational: snapshot ordinal=$ord_val"
            break
          fi
          if [ "$((op_attempt % 10))" -eq 0 ]; then
            echo "Metagraph $k ML0 not yet producing snapshots (attempt $op_attempt/120)..."
          fi
          sleep 3
        done
      fi
    done

    if [ "$any_failed" = "true" ]; then
      echo "ERROR: One or more cluster health checks failed"
      return 1
    fi
  fi

}
