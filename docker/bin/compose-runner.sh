#!/usr/bin/env bash

set -e 

if [ "$BASH_DEBUG_MODE" = "true" ]; then
  set -x             # Print each command before executing (verbose)
  set -eo pipefail  # Exit on error, pipe failures
fi


export START_TIME=$(date +%s)
export LATEST_TIME=$START_TIME

show_time() {
  local stage=$1
  export PREV_TIME=$LATEST_TIME
  export LATEST_TIME=$(date +%s)
  export DELTA_SECONDS_TOTAL=$((LATEST_TIME - START_TIME))
  export DELTA_SECONDS=$((LATEST_TIME - PREV_TIME))
  echo "$stage took: $DELTA_SECONDS seconds - total time: $DELTA_SECONDS_TOTAL seconds"
}

cleanup_end() {
  docker rm -f tx-sender prometheus nakamoto-grafana 2>/dev/null || true
  if [ "$CLEANUP_DOCKER_AT_END" == "true" ] && { [ -z "$TEST_HOST" ] || [ "$TEST_HOST" = "http://localhost" ]; }; then
    ./docker/bin/tessellation-docker-cleanup.sh
  fi
}

trap cleanup_end EXIT

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cur_dir=$(pwd)
echo "Script started in $cur_dir with script directory $SCRIPT_DIR"

cd "$SCRIPT_DIR/../../"
cur_dir=$(pwd)
export PROJECT_ROOT=$cur_dir
echo "Running in top level directory $cur_dir"


source ./docker/bin/set-env.sh "$@"

if [ "$LIST_TESTS" = "true" ]; then
  echo "================================================"
  echo "Available tests:"
  echo "================================================"
  echo ""
  echo "DAG tests (no metagraph required):"
  echo "  dag-cluster              DAG cluster check"
  echo "  delegated-staking        Delegated staking tests"
  echo "  fork-recovery            Fork recovery test (needs --num-gl0=5 for BFT, 3 for Nakamoto)"
  echo "  token-lock-replacement   Token lock replacement edge case tests"
  echo "  snapshot-streaming       Snapshot streaming indexer E2E test"
  echo ""
  echo "Metagraph tests (require --use-test-metagraph):"
  echo "  currency                 Metagraph currency transaction tests"
  echo "  rewards                  Metagraph rewards tests"
  echo "  token-locks              Token lock tests"
  echo "  allow-spends             Allow-spend tests"
  echo "  spend                    Spend transaction tests"
  echo "  data-without-fee         Data transaction tests (without fee, requires CI_PRIVATE_KEY)"
  echo "  data-with-fee            Data transaction tests (with fee, requires CI_PRIVATE_KEY)"
  echo ""
  echo "Usage: just test --test=dag-cluster --test=delegated-staking"
  echo "       just test --test=dag-cluster,rewards    (comma-separated)"
  exit 0
fi

# If tests are selected, check if any require metagraph. If not, skip metagraph setup.
METAGRAPH_TESTS="currency,rewards,token-locks,allow-spends,spend,data-without-fee,data-with-fee"
if [ -n "$SELECTED_TESTS" ] && [ -n "$METAGRAPH" ]; then
  needs_metagraph=false
  for t in $(echo "$SELECTED_TESTS" | tr ',' ' '); do
    if echo "$METAGRAPH_TESTS" | tr ',' '\n' | grep -qx "$t"; then
      needs_metagraph=true
      break
    fi
  done
  if [ "$needs_metagraph" = "false" ]; then
    echo "Selected tests do not require metagraph, skipping metagraph setup"
    unset METAGRAPH
    export NUM_ML0_NODES=0
    export NUM_CL1_NODES=0
    export NUM_DL1_NODES=0
  fi
fi

# snapshot-streaming build-from-source needs sdk/publishLocal
if [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; then
  export PUBLISH=${PUBLISH:-true}
fi

REMOTE_HOST=${TEST_HOST:-}

if [ -n "$REMOTE_HOST" ] && [ "$REMOTE_HOST" != "http://localhost" ]; then
  echo "------------------------------------------------"
  echo "Remote host provided ($REMOTE_HOST), skipping docker setup"
  echo "------------------------------------------------"
else
  ./docker/bin/tessellation-docker-cleanup.sh &
  CLEANUP_PID=$!

  echo "Starting assembly"
  source ./docker/bin/assembly.sh

  export TESSELLATION_DOCKER_VERSION=test

  echo "Finished assembly, building docker image"
  docker build -t constellationnetwork/tessellation:$TESSELLATION_DOCKER_VERSION -f docker/Dockerfile .

  # Nakamoto-mode GL0: also build the Go libp2p sidecar image. The same JVM
  # JAR runs both BFT and Nakamoto modes — the sidecar provides the GossipSub
  # transport that Nakamoto-mode GL0 publishes snapshots/attestations/rumors on.
  if [ "$NAKAMOTO_GL0" = "true" ]; then
    echo "Building Nakamoto sidecar image (nakamoto-sidecar:test)"
    docker build -t nakamoto-sidecar:test -f p2p/Dockerfile p2p/
  fi


  # Wait for cleanup PID to finish
  wait $CLEANUP_PID

  if [ "$PURGE_CONFIG" = "true" ]; then
    echo "Purging config, removing $PROJECT_ROOT/nodes"
    sleep 1
    # strange issue with docker mount persistence, so we sleep and try twice, this may be removable now?
    ./docker/bin/clean-configs.sh
    sleep 1
    ./docker/bin/clean-configs.sh
    ls -la $PROJECT_ROOT/nodes || true
    echo "removed config, $PROJECT_ROOT/nodes"
  fi

  for i in $(seq 0 $((MAX_NODES - 1))); do
    mkdir -p ./nodes/$i
  done

  # Copy keytool and wallet jars to nodes directory for key generation (needed for nodes 3+)
  cp ./docker/jars/keytool.jar ./docker/jars/wallet.jar ./nodes/ 2>/dev/null || true

  source ./docker/bin/node-key-env-setup.sh
  source ./docker/bin/docker-env-setup.sh


  echo "------------------------------------------------"
  echo "All deployment configurations now generated, proceeding to run cluster"
  echo "------------------------------------------------"


  # Build snapshot-streaming JAR (needed before BUILD_ONLY exit so `just build` produces it)
  if [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; then
    SS_DIR="$PROJECT_ROOT/docker/snapshot-streaming"
    source "$SS_DIR/build-snapshot-streaming.sh"
    cd "$PROJECT_ROOT"
  fi

  if [ "$BUILD_ONLY" = "true" ]; then
    echo "Build only mode, skipping container startup and end-to-end tests"
    exit 0
  fi


  docker network create \
    --driver=bridge \
    --subnet=${NET_PREFIX}.0/24 \
    tessellation_common

  # Phase 1: Setup compose files and start GL0 nodes
  for i in $(seq 0 $((MAX_NODES - 1))); do
    cd ./nodes/$i/

    docker compose -f docker-compose.test.yaml \
    -f docker-compose.yaml \
    -f docker-compose.volumes.yaml \
    down --remove-orphans --volumes > /dev/null 2>&1 || true;

    cp ../../docker/docker-compose.yaml . ; \
    cp ../../docker/docker-compose.test.yaml . ; \
    cp ../../docker/docker-compose.volumes.yaml . ; \
    cp ../../docker/docker-compose.metagraph.yaml . ;
    cp ../../docker/docker-compose.metagraph-test.yaml . ;
    cp ../../docker/docker-compose.metagraph-genesis.yaml . ;

    if [ "$NAKAMOTO_GL0" = "true" ]; then
      cp ../../docker/docker-compose.nakamoto-sidecar.yaml . ;
      cp ../../docker/docker-compose.nakamoto-overlay.yaml . ;
    fi

    cd ../../
  done

  # Nakamoto mode: write shared genesis time, sidecar peer list, JVM seedlist,
  # and genesis CSV into each gl0 node's .env + node dir. All gl0 nodes MUST
  # agree on the same genesis time and seedlist for VRF consensus to work.
  if [ "$NAKAMOTO_GL0" = "true" ]; then
    # 90s in the future — gives all containers time to start before slot 0
    NAKAMOTO_GENESIS_MS=$(( ($(date +%s) + 90) * 1000 ))
    echo "Nakamoto genesis time: $(date -d @$((NAKAMOTO_GENESIS_MS / 1000)) '+%H:%M:%S') (90s from now)"

    # Derive Ed25519 sidecar identity keys from each node's ECDSA key so the
    # sidecar's libp2p peer ID is deterministic from the node identity. This
    # lets us include /p2p/<id> in the seedlist multiaddrs — required for
    # DHT-only discovery (no mDNS) where peers must be dialed by identity.
    echo "Deriving sidecar identity keys from node ECDSA keys..."
    for j in $(seq 0 $((NUM_GL0_NODES - 1))); do
      SIDECAR_PEER_ID=$(docker run --rm \
        -v "$(pwd)/nodes/$j:/keys" \
        -v "$(pwd)/docker/config/local-test-keys/$j:/ecdsa:ro" \
        nakamoto-sidecar:test \
        -derive-from-ecdsa /ecdsa/id_ecdsa.hex -key /keys/sidecar.key)
      echo "$SIDECAR_PEER_ID" > "./nodes/$j/sidecar_peer_id"
      echo "  sidecar-$j: $SIDECAR_PEER_ID"
    done

    # Build the sidecar seedlist with full /p2p/<peer-id> multiaddrs
    NAKAMOTO_SEEDLIST=""
    for j in $(seq 0 $((NUM_GL0_NODES - 1))); do
      SIDECAR_PID=$(cat "./nodes/$j/sidecar_peer_id")
      [ -n "$NAKAMOTO_SEEDLIST" ] && NAKAMOTO_SEEDLIST="${NAKAMOTO_SEEDLIST},"
      NAKAMOTO_SEEDLIST="${NAKAMOTO_SEEDLIST}/dns4/sidecar-${j}/tcp/9500/p2p/${SIDECAR_PID}"
    done
    echo "Nakamoto sidecar seedlist: $NAKAMOTO_SEEDLIST"

    # Build the JVM seedlist (5-field CSV: peerId,ip,p2pPort,alias,bias).
    # Used by StakeRegistry for validator set, by Joining.scala for peer
    # allowance, and by ClusterStorage population so /cluster/info returns
    # the full validator set. Same file works for ALL layers since each node
    # shares one key across gl0/gl1/ml0/cl1/dl1 in the test environment.
    echo "Generating Nakamoto seedlist from gl0 peer IDs..."
    NAKAMOTO_JVM_SEEDLIST=""
    for j in $(seq 0 $((NUM_GL0_NODES - 1))); do
      PEER_ID=$(cat ./nodes/$j/peer_id 2>/dev/null || echo "")
      if [ -z "$PEER_ID" ]; then
        echo "ERROR: missing peer_id for node $j (expected at ./nodes/$j/peer_id)"
        exit 1
      fi
      # Compute per-node IP and P2P port from the test cluster layout
      NODE_IP="${NET_PREFIX}.1${j}"
      NODE_P2P_PORT="${DAG_L0_PORT_PREFIX}${j}1"
      NAKAMOTO_JVM_SEEDLIST="${NAKAMOTO_JVM_SEEDLIST}${PEER_ID},${NODE_IP},${NODE_P2P_PORT},,\n"
    done

    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      # Write the JVM seedlist file into each node directory
      printf "$NAKAMOTO_JVM_SEEDLIST" > ./nodes/$i/seedlist.csv
      # Copy genesis.csv to each gl0 node (run-nakamoto needs it on every node,
      # not just node 0 — all nodes derive the same genesis state independently)
      cp ./nodes/0/genesis.csv ./nodes/$i/genesis.csv 2>/dev/null || true

      {
        echo ""
        echo "# Nakamoto GL0 mode (set by compose-runner.sh --nakamoto-gl0)"
        echo "NAKAMOTO_GENESIS_TIME_MS=$NAKAMOTO_GENESIS_MS"
        echo "NAKAMOTO_SIDECAR_SEEDLIST=$NAKAMOTO_SEEDLIST"
        echo "CL_DOCKER_SEEDLIST=./seedlist.csv"
        # Ensure genesis.csv is mounted into the container (docker-env-setup.sh
        # only sets this for node 0; Nakamoto needs it on every node)
        echo "CL_GENESIS_FILE=./genesis.csv"
        # Override join to false for ALL gl0 nodes — Nakamoto has no BFT cluster
        # join protocol. Validators discover each other via seedlist + sidecar.
        echo "CL_DOCKER_GL0_JOIN=false"
      } >> ./nodes/$i/.env
    done
    echo "Nakamoto seedlist written to nodes/*/seedlist.csv ($(wc -l < ./nodes/0/seedlist.csv) peers)"
  fi

  # Start all GL0 nodes together
  for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
    cd ./nodes/$i/
    nakamoto_compose_args=""
    if [ "$NAKAMOTO_GL0" = "true" ]; then
      nakamoto_compose_args="-f docker-compose.nakamoto-sidecar.yaml -f docker-compose.nakamoto-overlay.yaml"
    fi
    docker compose -f docker-compose.test.yaml \
      -f docker-compose.yaml \
      -f docker-compose.volumes.yaml \
      $nakamoto_compose_args \
      --profile l0 \
      up -d
    cd ../../
  done

  # Wait for GL0 cluster to be ready before starting GL1
  # GL1 needs GL0 for L0PeerDiscovery; without this, GL1's join state machine
  # gets stuck at SessionStarted.
  if [ "$NUM_GL0_NODES" -gt 0 ] && [ "$NUM_GL1_NODES" -gt 0 ]; then
    echo "Waiting for GL0 cluster to be ready before starting GL1..."
    gl0_url="${TEST_HOST:-http://localhost}:${DAG_L0_PORT_PREFIX}00"
    gl0_ready=false
    for attempt in $(seq 1 120); do
      cluster_info=$(curl -s "${gl0_url}/cluster/info" 2>/dev/null || echo "")
      if [ -n "$cluster_info" ] && echo "$cluster_info" | jq 'length' >/dev/null 2>&1; then
        node_count=$(echo "$cluster_info" | jq 'length')
        if [ "$node_count" -ge 1 ]; then
          echo "GL0 cluster ready with $node_count node(s)"
          gl0_ready=true
          break
        fi
      fi
      echo "GL0 not ready yet (attempt $attempt/120), waiting..."
      sleep 5
    done
    if [ "$gl0_ready" = "false" ]; then
      echo "ERROR: GL0 did not become ready in time"
      docker logs gl0-0 || true
      exit 1
    fi
  fi

  # Phase 2: Start GL1 nodes (GL0 is now ready for peer discovery)
  for i in $(seq 0 $((MAX_NODES - 1))); do
    cd ./nodes/$i/

    if [ "$i" -lt "$NUM_GL1_NODES" ]; then
      docker compose -f docker-compose.test.yaml \
      -f docker-compose.yaml \
      -f docker-compose.volumes.yaml \
      --profile l1 \
      up -d
    fi

    cd ../../
  done

  # Wait for GL0 to be ready before starting metagraph nodes
  if [ -n "$METAGRAPH" ] && [ "$NUM_GL0_NODES" -gt 0 ]; then
    echo "Waiting for GL0 to be ready before starting metagraph..."
    gl0_url="${TEST_HOST:-http://localhost}:${DAG_L0_PORT_PREFIX}00"
    gl0_ready=false
    for attempt in $(seq 1 60); do
      cluster_info=$(curl -s "${gl0_url}/cluster/info" 2>/dev/null || echo "")
      if [ -n "$cluster_info" ] && echo "$cluster_info" | jq 'length' >/dev/null 2>&1; then
        node_count=$(echo "$cluster_info" | jq 'length')
        if [ "$node_count" -ge 1 ]; then
          echo "GL0 is ready with $node_count node(s)"
          gl0_ready=true
          break
        fi
      fi
      echo "GL0 not ready yet (attempt $attempt/60), waiting..."
      sleep 5
    done
    if [ "$gl0_ready" = "false" ]; then
      echo "ERROR: GL0 did not become ready in time"
      docker logs gl0-0 || true
      exit 1
    fi
  fi

  if [ -n "$METAGRAPH" ]; then
    metagraph_args="-f docker-compose.metagraph.yaml -f docker-compose.metagraph-test.yaml"

    # Phase 1: Genesis creation, set METAGRAPH_ID, and start ML0
    for i in $(seq 0 $((MAX_NODES - 1))); do
      cd ./nodes/$i/

      if [ ! -f "./genesis.snapshot" ] && [ "$i" -eq 0 ]; then
        echo "Generating metagraph genesis snapshot"
        cp .env .env.bak
        echo "CL_ML0_GENERATE_GENESIS=true" >> .env
        docker compose $metagraph_args -f docker-compose.metagraph-genesis.yaml --profile ml0 up
        docker stop ml0-0
        docker rm ml0-0
        cp ml0-data/genesis.snapshot .
        cp ml0-data/genesis.address .
        mv .env.bak .env
      fi
      # Ensure genesis.snapshot is in ml0-data (clean-data wipes ml0-data/ but
      # leaves ./genesis.snapshot in the node root, so regeneration is skipped)
      if [ -f "./genesis.snapshot" ] && [ ! -f "./ml0-data/genesis.snapshot" ]; then
        mkdir -p ./ml0-data
        cp ./genesis.snapshot ./ml0-data/genesis.snapshot
      fi
      # Set METAGRAPH_ID from genesis.address (may already be exported from
      # genesis generation above, or read from a previous run's file)
      if [ -z "$METAGRAPH_ID" ] && [ -f "./genesis.address" ]; then
        export METAGRAPH_ID=$(head -n 1 genesis.address)
      fi
      echo "METAGRAPH_ID=$METAGRAPH_ID" >> .env
      echo "CL_L0_TOKEN_IDENTIFIER=$METAGRAPH_ID" >> .env

      if [ "$i" -lt "$NUM_ML0_NODES" ]; then
        echo "Starting ML0 for node $i"
        docker compose $metagraph_args --profile ml0 up -d
      fi

      cd ../../
    done

    # Wait for ML0 to be ready before starting metagraph L1 layers (CL1/DL1)
    if [ "$NUM_ML0_NODES" -gt 0 ]; then
      echo "Waiting for ML0 to be ready before starting metagraph L1 layers..."
      ml0_url="${TEST_HOST:-http://localhost}:${ML0_PORT_PREFIX}00"
      ml0_ready=false
      for attempt in $(seq 1 120); do
        cluster_info=$(curl -s "${ml0_url}/cluster/info" 2>/dev/null || echo "")
        if [ -n "$cluster_info" ] && echo "$cluster_info" | jq 'length' >/dev/null 2>&1; then
          node_count=$(echo "$cluster_info" | jq 'length')
          if [ "$node_count" -ge 1 ]; then
            echo "ML0 is ready with $node_count node(s)"
            ml0_ready=true
            break
          fi
        fi
        echo "ML0 not ready yet (attempt $attempt/120), waiting..."
        sleep 5
      done
      if [ "$ml0_ready" = "false" ]; then
        echo "ERROR: ML0 did not become ready in time"
        docker logs ml0-0 || true
        exit 1
      fi
    fi

    # Phase 2: Start CL1/DL1 services (ML0 is now ready)
    for i in $(seq 0 $((MAX_NODES - 1))); do
      cd ./nodes/$i/

      l1_profile_args=""
      if [ "$i" -lt "$NUM_CL1_NODES" ]; then
        l1_profile_args="$l1_profile_args --profile cl1"
      fi
      if [ "$i" -lt "$NUM_DL1_NODES" ]; then
        l1_profile_args="$l1_profile_args --profile dl1"
      fi
      l1_profile_args=$(echo $l1_profile_args | xargs)

      if [ -n "$l1_profile_args" ]; then
        echo "Starting CL1/DL1 for node $i"
        docker compose $metagraph_args $l1_profile_args up -d
      fi

      cd ../../
    done
  fi


  # --- Snapshot-streaming infrastructure ---
  if [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; then
    echo "================================================"
    echo "Setting up snapshot-streaming infrastructure"
    echo "================================================"

    SS_DIR="$PROJECT_ROOT/docker/snapshot-streaming"

    # Build/obtain JAR + SQL
    source "$SS_DIR/build-snapshot-streaming.sh"

    # Generate application.conf
    source "$SS_DIR/generate-config.sh"

    export SS_JAR_PATH="$SS_DIR/snapshot-streaming.jar"
    export SS_CONFIG_PATH="$SS_DIR/application.conf"
    export SS_DATA_PATH="$SS_DIR/data"
    mkdir -p "$SS_DATA_PATH"

    # Start postgres
    echo "Starting snapshot-streaming-postgres..."
    docker compose -f "$SS_DIR/docker-compose.yaml" up -d snapshot-streaming-postgres

    # Wait for postgres healthy
    echo "Waiting for snapshot-streaming-postgres to be healthy..."
    for attempt in $(seq 1 60); do
      if docker exec snapshot-streaming-postgres pg_isready -U snapshot_streaming >/dev/null 2>&1; then
        echo "snapshot-streaming-postgres is ready"
        break
      fi
      if [ "$attempt" -eq 60 ]; then
        echo "ERROR: snapshot-streaming-postgres did not become ready"
        docker logs snapshot-streaming-postgres || true
        exit 1
      fi
      sleep 2
    done

    # Apply database schema via block_explorer prisma migrations
    echo "Applying database schema via block_explorer prisma..."
    BE_DIR="$SS_DIR/block-explorer"
    SS_PG_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' snapshot-streaming-postgres)
    DATABASE_URL="postgresql://snapshot_streaming:snapshot_streaming@${SS_PG_IP}:5432/snapshot_streaming"
    # Reset schema to ensure clean state
    docker exec snapshot-streaming-postgres psql -U snapshot_streaming -d snapshot_streaming \
      -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
    docker run --rm \
      --network tessellation_common \
      -v "$BE_DIR/prisma:/app/prisma" \
      -w /app \
      -e DATABASE_URL="$DATABASE_URL" \
      node:20-alpine \
      sh -c "npx prisma@6.2.1 db push --accept-data-loss --force-reset"
    echo "Database schema applied"

    # Seed snapshot-streaming with initial snapshot from GL0
    echo "Seeding snapshot-streaming with initial snapshot from GL0..."
    gl0_seed_url="${TEST_HOST:-http://localhost}:${DAG_L0_PORT_PREFIX}00"

    # Wait for GL0 to have a few snapshots
    for attempt in $(seq 1 120); do
      ordinal_resp=$(curl -sf "$gl0_seed_url/global-snapshots/latest/ordinal" 2>/dev/null || echo "")
      if [ -n "$ordinal_resp" ]; then
        latest_ord=$(echo "$ordinal_resp" | jq -r 'if type == "object" then .value else . end' 2>/dev/null || echo "0")
        if [ "$latest_ord" -ge 2 ] 2>/dev/null; then
          echo "GL0 has snapshots up to ordinal $latest_ord"
          break
        fi
      fi
      if [ "$attempt" -eq 120 ]; then
        echo "ERROR: GL0 did not produce enough snapshots for seeding"
        exit 1
      fi
      sleep 3
    done

    # Fetch latest combined snapshot + state
    combined_json=$(curl -sf "$gl0_seed_url/global-snapshots/latest/combined")
    seed_ordinal=$(echo "$combined_json" | jq '.[0].value.ordinal')
    echo "Fetched combined snapshot at ordinal $seed_ordinal"

    # Fetch the correct hash for this ordinal
    hash_resp=$(curl -sf "$gl0_seed_url/global-snapshots/$seed_ordinal/hash")
    snapshot_hash=$(echo "$hash_resp" | jq -r 'if type == "string" then . else .value // . end' 2>/dev/null || echo "$hash_resp")
    # Strip any surrounding quotes
    snapshot_hash=$(echo "$snapshot_hash" | tr -d '"')
    echo "Snapshot hash: ${snapshot_hash:0:16}..."

    # Compute proofsHash: SHA256 of the sorted proofs JSON (compact, sorted keys, no nulls).
    # Note: The node uses Brotli-compressed JSON for hashing, which we cannot replicate here.
    # This simplified hash is sufficient for the E2E test since snapshot-streaming does not
    # cryptographically verify the seed proofsHash.
    proofs_hash=$(echo "$combined_json" | jq -cS '.[0].proofs' | shasum -a 256 | awk '{print $1}')
    echo "Proofs hash: ${proofs_hash:0:16}..."

    # Create SnapshotWithState seed file (gzipped JSON)
    echo "$combined_json" | jq --arg h "$snapshot_hash" --arg ph "$proofs_hash" '{
      snapshot: {
        signed: .[0],
        hash: $h,
        proofsHash: $ph
      },
      state: .[1]
    }' | gzip > "$SS_DATA_PATH/seed-snapshot.json.gz"
    echo "Seed file created at $SS_DATA_PATH/seed-snapshot.json.gz"

    # Start snapshot-streaming
    echo "Starting snapshot-streaming..."
    docker compose -f "$SS_DIR/docker-compose.yaml" up -d snapshot-streaming

    show_time "Snapshot-streaming infrastructure started"
  fi

  show_time "Started docker compose"

  # Optional Prometheus + Grafana monitoring (--grafana flag)
  if [ "$ENABLE_GRAFANA" = "true" ]; then
    echo "Starting Prometheus + Grafana monitoring..."

    # Generate Prometheus config with per-node targets
    PROM_CFG="$PROJECT_ROOT/nodes/prometheus.yml"
    cat > "$PROM_CFG" <<PROMEOF
global:
  scrape_interval: 5s
  evaluation_interval: 5s
scrape_configs:
PROMEOF

    # GL0 targets
    echo "  - job_name: 'nakamoto-gl0'" >> "$PROM_CFG"
    echo "    metrics_path: '/metrics'" >> "$PROM_CFG"
    echo "    static_configs:" >> "$PROM_CFG"
    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      port=$((${DAG_L0_PORT_PREFIX}${i}0))
      echo "      - targets: ['gl0-$i:$port']" >> "$PROM_CFG"
      echo "        labels: { layer: 'gl0', node: 'gl0-$i' }" >> "$PROM_CFG"
    done

    # Sidecar targets (if Nakamoto)
    if [ "$NAKAMOTO_GL0" = "true" ]; then
      echo "  - job_name: 'sidecar'" >> "$PROM_CFG"
      echo "    metrics_path: '/metrics'" >> "$PROM_CFG"
      echo "    static_configs:" >> "$PROM_CFG"
      for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
        echo "      - targets: ['sidecar-$i:9501']" >> "$PROM_CFG"
        echo "        labels: { layer: 'sidecar', node: 'sidecar-$i' }" >> "$PROM_CFG"
      done
    fi

    docker rm -f prometheus nakamoto-grafana 2>/dev/null || true
    docker run -d --name prometheus --network tessellation_common \
      -v "$PROM_CFG:/etc/prometheus/prometheus.yml:ro" \
      -p 9090:9090 --restart unless-stopped prom/prometheus:latest >/dev/null 2>&1

    docker run -d --name nakamoto-grafana --network tessellation_common \
      -e GF_SECURITY_ADMIN_USER=admin -e GF_SECURITY_ADMIN_PASSWORD=admin \
      -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
      -p 3000:3000 \
      -v "$PROJECT_ROOT/nakamoto-test/grafana/provisioning:/etc/grafana/provisioning:ro" \
      -v "$PROJECT_ROOT/nakamoto-test/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
      --restart unless-stopped grafana/grafana:latest >/dev/null 2>&1

    echo "  Grafana: http://localhost:3000 (admin/admin)"
    echo "  Prometheus: http://localhost:9090"
    show_time "Monitoring started"
  fi

  if [ "$DOCKER_UP" = "true" ]; then
    echo "Docker up mode, skipping end-to-end tests"
    exit 0
  fi
fi


# ------------------------------------------------
# Test selection helpers
# ------------------------------------------------
should_run_test() {
  local test_name=$1
  if [ -z "$SELECTED_TESTS" ]; then
    return 0
  fi
  echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "$test_name"
}

if [ -n "$SELECTED_TESTS" ]; then
  echo "------------------------------------------------"
  echo "Running selected tests: $SELECTED_TESTS"
  echo "------------------------------------------------"
fi

echo "------------------------------------------------"
echo "Running end-to-end tests from .github/action_scripts"
echo "------------------------------------------------"

# Install dependencies
cd $PROJECT_ROOT/.github/action_scripts
echo "Installing Node.js dependencies..."
npm i @stardust-collective/dag4 js-sha256 axios brotli zod elliptic

if [ -z "$REMOTE_HOST" ] || [ "$REMOTE_HOST" = "http://localhost" ]; then
  sleep 10
  docker logs gl0-0
  echo "GL0-0 logs above, now continuing with cluster health check."
fi

source ../../docker/bin/cluster-health-check.sh
verify_healthy
show_time "Cluster became healthy"

# ------------------------------------------------
# Start background transaction sender (keeps EventTrigger flowing)
# ------------------------------------------------
TX_SENDER_JAR="$PROJECT_ROOT/docker/jars/tools.jar"
TX_SENDER_CONF="$PROJECT_ROOT/docker/config/tx-sender.conf"
if [ -f "$TX_SENDER_JAR" ] && [ -f "$TX_SENDER_CONF" ] && [ "$NAKAMOTO_GL0" != "true" ]; then
  echo "Starting background transaction sender..."
  docker rm -f tx-sender 2>/dev/null || true
  docker run -d --name tx-sender \
    --network tessellation_common \
    --restart unless-stopped \
    -v "$TX_SENDER_JAR:/app/tools.jar:ro" \
    -v "$TX_SENDER_CONF:/app/tx-sender.conf:ro" \
    eclipse-temurin:11-jre \
    java -jar /app/tools.jar tx-sender --config /app/tx-sender.conf \
    > /dev/null 2>&1 && echo "  tx-sender started" || echo "  tx-sender failed to start (non-fatal)"
else
  echo "Skipping background tx-sender (tools.jar or tx-sender.conf not found)"
fi

# ------------------------------------------------
# GL0/GL1 tests (no metagraph required)
# ------------------------------------------------

if should_run_test "dag-cluster"; then
  echo "================================================"
  echo "Running DAG cluster check"
  echo "================================================"
  cd $PROJECT_ROOT/.github/action_scripts/check_clusters
  node dag.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX
  show_time "DAG cluster check completed"
fi

if should_run_test "delegated-staking"; then
  echo "================================================"
  echo "Running delegated staking tests"
  echo "================================================"
  cd $PROJECT_ROOT/.github/action_scripts/delegated_staking
  node delegated-staking.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX testDelegatedStaking
  show_time "Delegated staking tests completed"
fi

if should_run_test "token-lock-replacement"; then
  echo "================================================"
  echo "Running token lock replacement edge case tests"
  echo "================================================"
  cd $PROJECT_ROOT/.github/action_scripts/delegated_staking
  node token-lock-replacement-edge-cases.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX testTokenLockReplacementEdgeCases
  show_time "Token lock replacement edge case tests completed"
fi

if should_run_test "fork-recovery"; then
  echo "================================================"
  echo "Running fork-recovery test"
  echo "================================================"
  cd $PROJECT_ROOT
  bash docker/bin/test-fork-recovery.sh $DAG_L0_PORT_PREFIX
  show_time "Fork recovery test completed"
fi

if should_run_test "snapshot-streaming"; then
  echo "================================================"
  echo "Running snapshot-streaming E2E test"
  echo "================================================"
  # Stop tx-sender before snapshot-streaming: the Prisma schema requires
  # dag_transactions.snapshot_ordinal NOT NULL but the trigger that populates it
  # from global_snapshots.hash is racy — if tx lands before the snapshot row
  # exists, snapshot_ordinal stays NULL and the insert fails.
  docker rm -f tx-sender 2>/dev/null || true
  cd $PROJECT_ROOT

  ss_test_passed=false
  echo "Waiting for snapshot-streaming to index snapshots..."
  for attempt in $(seq 1 120); do
    count=$(docker exec snapshot-streaming-postgres psql -U snapshot_streaming -d snapshot_streaming -t -A -c \
      "SELECT COUNT(*) FROM global_snapshots;" 2>/dev/null || echo "0")
    count=$(echo "$count" | tr -d '[:space:]')

    if [ "$count" -ge 3 ]; then
      echo "snapshot-streaming indexed $count global snapshots"
      max_ordinal=$(docker exec snapshot-streaming-postgres psql -U snapshot_streaming -d snapshot_streaming -t -A -c \
        "SELECT MAX(ordinal) FROM global_snapshots;" 2>/dev/null || echo "0")
      max_ordinal=$(echo "$max_ordinal" | tr -d '[:space:]')
      echo "Max ordinal: $max_ordinal"
      if [ "$max_ordinal" -gt 0 ]; then
        ss_test_passed=true
        break
      fi
    fi

    if [ "$((attempt % 10))" -eq 0 ]; then
      echo "  snapshot count: ${count:-0} (attempt $attempt/120)"
    fi
    sleep 5
  done

  if [ "$ss_test_passed" = "true" ]; then
    echo "snapshot-streaming E2E test PASSED"
  else
    echo "snapshot-streaming E2E test FAILED"
    echo "--- snapshot-streaming logs ---"
    docker logs snapshot-streaming 2>&1 | tail -100 || true
    echo "--- postgres tables ---"
    docker exec snapshot-streaming-postgres psql -U snapshot_streaming -d snapshot_streaming -c '\dt' || true
    exit 1
  fi
  show_time "Snapshot-streaming E2E test completed"
fi

# ------------------------------------------------
# Metagraph tests (require --use-test-metagraph or --metagraph=...)
# ------------------------------------------------

if [ -n "$METAGRAPH" ]; then

  if should_run_test "currency"; then
    echo "================================================"
    echo "Running metagraph currency transaction tests"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node send_transactions/currency.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX
    show_time "Currency transaction tests completed"
  fi

  if should_run_test "rewards"; then
    echo "================================================"
    echo "Running metagraph rewards tests"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node rewards.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX
    show_time "Metagraph rewards tests completed"
  fi

  if should_run_test "token-locks"; then
    echo "================================================"
    echo "Running token lock tests"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node send_transactions/token-locks.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX
    show_time "Token lock tests completed"
  fi

  if should_run_test "allow-spends"; then
    echo "================================================"
    echo "Running allow-spend tests"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    # Note: double-spend scenario requires extended DAG L1 (6 nodes) and is skipped here
    for scenario in dag currency exceeding-balance invalid-parent invalid-epoch invalid-signature expired-allow-spend double-use-allow-spend invalid-currency-destination invalid-approver; do
      echo "--- Allow-spend scenario: $scenario ---"
      node send_transactions/allow-spends-and-spend-transactions.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $scenario
    done
    show_time "Allow-spend tests completed"
  fi

  if should_run_test "spend"; then
    echo "================================================"
    echo "Running spend transaction tests"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    for scenario in spend full-spend unauthorized unauthorized-currency exceeding-amount-spend; do
      echo "--- Spend scenario: $scenario ---"
      node send_transactions/allow-spends-and-spend-transactions.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $scenario
    done
    show_time "Spend transaction tests completed"
  fi

  if [ -n "$CI_PRIVATE_KEY" ]; then
    if should_run_test "data-without-fee"; then
      echo "================================================"
      echo "Running data transaction tests (without fee)"
      echo "================================================"
      cd $PROJECT_ROOT/.github/action_scripts
      node send_transactions/data-without-fee.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $CI_PRIVATE_KEY
      show_time "Data transaction tests (without fee) completed"
    fi

    if should_run_test "data-with-fee"; then
      echo "================================================"
      echo "Running data transaction tests (with fee)"
      echo "================================================"
      cd $PROJECT_ROOT/.github/action_scripts
      node send_transactions/data-with-fee.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $CI_PRIVATE_KEY
      show_time "Data transaction tests (with fee) completed"
    fi
  else
    echo "================================================"
    echo "Skipping data transaction tests (CI_PRIVATE_KEY not set)"
    echo "================================================"
  fi

else
  echo "================================================"
  echo "Skipping metagraph tests (no --metagraph or --use-test-metagraph flag)"
  echo "================================================"
fi

echo "------------------------------------------------"
echo "End-to-end tests completed"
echo "------------------------------------------------"

cd $PROJECT_ROOT




