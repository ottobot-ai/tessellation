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

# --- Runner safety: global hard timeout (watchdog) + exit-code preservation ---
# A background watchdog (started just after set-env.sh is sourced) SIGTERMs this
# script if the whole run exceeds RUN_TIMEOUT_SECONDS, so a stuck/looping test
# can never run all night and leak containers. The SIGTERM handler records the
# timeout and exits, which fires the single EXIT trap (cleanup_end) below.
#
# Marker file is set by the watchdog/handler; cleanup_end reads it to force a
# non-zero (124) exit on timeout. mktemp here is best-effort: if it fails the
# handler still exits non-zero, just without the explicit 124 normalization.
RUNNER_TIMEOUT_MARKER="$(mktemp 2>/dev/null || echo /tmp/compose-runner-timeout.$$)"
WATCHDOG_PID=""

on_timeout_signal() {
  # Invoked when the watchdog SIGTERMs us. Convert the signal into a controlled,
  # non-zero exit so cleanup_end runs teardown deterministically (a bare signal
  # would otherwise exit 143 and skip the 124 normalization).
  echo "" >&2
  echo "============================================================" >&2
  echo "RUN TIMEOUT: exceeded ${RUN_TIMEOUT_SECONDS:-?}s hard ceiling — killing run + tearing down." >&2
  echo "============================================================" >&2
  echo "timeout" > "$RUNNER_TIMEOUT_MARKER" 2>/dev/null || true
  exit 124
}

# cleanup_end is the SINGLE teardown point for EVERY exit path (normal end,
# mid-flow `exit N`, set -e failure, or watchdog timeout). It is status-
# PRESERVING: it captures the pending exit code FIRST and re-exits with it LAST,
# so the teardown (which itself returns 0) can never clobber the true result.
cleanup_end() {
  local code=$?

  # Timeout normalization: a watchdog kill becomes a definite non-zero (124).
  if [ -s "$RUNNER_TIMEOUT_MARKER" ]; then
    code=124
  fi

  # Stop the watchdog so it can't fire after we've already exited. Reap its
  # child `sleep` first (pkill -P) so the multi-hour timer doesn't linger as an
  # orphan after the subshell dies.
  if [ -n "$WATCHDOG_PID" ]; then
    pkill -P "$WATCHDOG_PID" 2>/dev/null || true
    kill "$WATCHDOG_PID" 2>/dev/null || true
  fi
  rm -f "$RUNNER_TIMEOUT_MARKER" 2>/dev/null || true

  # tx-sender is a test fixture — always remove when the run ends.
  docker rm -f tx-sender 2>/dev/null || true

  # Auto-teardown decision. Default ON. Skip only when:
  #   * --keep-alive          : operator wants the cluster left up for debugging
  #   * --up (DOCKER_UP)      : `just up` intentionally leaves a long-lived cluster
  #   * --build / --list-tests: no cluster was ever started (nothing to tear down)
  #   * remote host           : we don't own the cluster, never touch it
  # PRESERVES nodes/ logs: tessellation-docker-cleanup.sh (== `just down` ==
  # clean-docker) only removes containers/volumes/network + snapshot-streaming
  # scratch — it NEVER touches nodes/, so post-mortem logs survive. (Do NOT use
  # clean-data here.)
  local is_local="false"
  if [ -z "${TEST_HOST:-}" ] || [ "${TEST_HOST:-}" = "http://localhost" ]; then
    is_local="true"
  fi

  if [ "${KEEP_ALIVE:-false}" = "true" ]; then
    echo "[cleanup] --keep-alive set: leaving cluster UP (logs in nodes/; run 'just down' when done). exit=$code"
  elif [ "${DOCKER_UP:-false}" = "true" ]; then
    echo "[cleanup] --up mode: leaving cluster UP. exit=$code"
  elif [ "${BUILD_ONLY:-false}" = "true" ] || [ "${LIST_TESTS:-false}" = "true" ]; then
    : # no cluster was started; nothing to tear down
  elif [ "$is_local" != "true" ]; then
    echo "[cleanup] remote host (${TEST_HOST:-}): not tearing down a cluster we don't own. exit=$code"
  else
    local grace="${TEARDOWN_GRACE_SECONDS:-45}"
    echo "============================================================"
    echo "[cleanup] e2e run finished (exit=$code). Auto-teardown in ${grace}s"
    echo "          (--keep-alive to skip). nodes/ logs are PRESERVED."
    echo "============================================================"
    if [ "$grace" -gt 0 ] 2>/dev/null; then
      sleep "$grace" || true
    fi
    echo "[cleanup] tearing down cluster via clean-docker (just down)..."
    # Mirror `just down` exactly: clean-docker == tessellation-docker-cleanup.sh.
    # Wrapped in `|| true` so a teardown hiccup can never overwrite $code.
    ./docker/bin/tessellation-docker-cleanup.sh || true
    echo "[cleanup] teardown complete. nodes/ preserved for post-mortem."
  fi

  # Remove our own EXIT trap and re-exit with the TRUE captured status so the
  # caller (just / CI) sees 0 only on a real pass, non-zero on failure/timeout.
  trap - EXIT
  exit "$code"
}

trap on_timeout_signal TERM
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

# --- Start the global hard-timeout watchdog ---
# Bounds the WHOLE run (assembly + bringup + workflows). If RUN_TIMEOUT_SECONDS
# elapses before the runner exits, the watchdog SIGTERMs us; on_timeout_signal
# then converts that into a controlled exit, and cleanup_end tears the cluster
# down. This makes an all-night stuck-test container leak impossible. The
# watchdog is reaped by cleanup_end on every exit path (including the fast
# --list-tests/--build/--up exits), so it never outlives the run.
RUNNER_MAIN_PID=$$
(
  sleep "${RUN_TIMEOUT_SECONDS:-10800}"
  echo "[watchdog] run exceeded ${RUN_TIMEOUT_SECONDS:-10800}s — sending SIGTERM to runner (pid $RUNNER_MAIN_PID)" >&2
  kill -TERM "$RUNNER_MAIN_PID" 2>/dev/null || true
) &
WATCHDOG_PID=$!
echo "[watchdog] armed: global run timeout = ${RUN_TIMEOUT_SECONDS:-10800}s (pid $WATCHDOG_PID)"

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
  echo "  data-without-fee         Data transaction tests (without fee; override signer with CI_PRIVATE_KEY)"
  echo "  data-with-fee            Data transaction tests (with fee; override signer with CI_PRIVATE_KEY)"
  echo ""
  echo "Multi-metagraph tests (run by default under \`just test\` since NUM_METAGRAPHS>=2; require K>=2):"
  echo "  multi-metagraph          Sanity check K parallel metagraphs share gl0 with distinct IDs"
  echo ""
  echo "Usage: just test --test=dag-cluster --test=delegated-staking"
  echo "       just test --test=dag-cluster,rewards    (comma-separated)"
  echo "       just test --metagraphs=1                (single-metagraph, skips multi-metagraph)"
  exit 0
fi

# If tests are selected, check if any require metagraph. If not, skip metagraph setup.
METAGRAPH_TESTS="currency,rewards,token-locks,allow-spends,spend,data-without-fee,data-with-fee,multi-metagraph"
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

# Metagraph builds and snapshot-streaming need sdk/publishLocal
if [ -n "$METAGRAPH" ] || { [ "$SKIP_STREAMING" != "true" ] && { [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; }; }; then
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

  # Build the Go libp2p sidecar image. The sidecar provides the GossipSub
  # transport that Nakamoto-mode GL0 publishes snapshots/attestations/rumors on.
  echo "Building Nakamoto sidecar image (nakamoto-sidecar:test)"
  docker build -t nakamoto-sidecar:test -f p2p/Dockerfile p2p/


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

  # Hypergraph operator dirs (gl0+gl1)
  for i in $(seq 0 $((MAX_HG_NODES - 1))); do
    mkdir -p ./nodes/$i
  done
  # Per-metagraph operator dirs (ml0+cl1+dl1, distinct keystore per metagraph)
  if [ -n "$METAGRAPH" ]; then
    for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
      for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
        mkdir -p ./nodes/m${k}-${i}
      done
    done
  fi

  # Copy keytool and wallet jars to nodes directory for key generation (needed for nodes 3+)
  cp ./docker/jars/keytool.jar ./docker/jars/wallet.jar ./nodes/ 2>/dev/null || true

  source ./docker/bin/node-key-env-setup.sh
  source ./docker/bin/docker-env-setup.sh

  # Tier-1 test-vectors hook (§1.1 — `project_test_vector_pattern`). When
  # NAKAMOTO_STAKE_DISTRIBUTION is set, generate a Tier-1 l0-genesis.json via the tools-jar
  # and overlay it onto each gl0 node so the cluster boots from a stake-weighted genesis
  # instead of the equal-weight CSV. Validation, generator invocation, per-node copy +
  # CL_GENESIS_FILE wire-up are all gated by this env var being non-empty.
  if [ -n "$NAKAMOTO_STAKE_DISTRIBUTION" ]; then
    echo "------------------------------------------------"
    echo "Tier-1 test-vector path: NAKAMOTO_STAKE_DISTRIBUTION='$NAKAMOTO_STAKE_DISTRIBUTION'"
    echo "------------------------------------------------"

    # Validate: comma-separated, length matches NUM_GL0_NODES, sum ≈ 1.0
    STAKE_COUNT=$(echo "$NAKAMOTO_STAKE_DISTRIBUTION" | tr ',' '\n' | grep -c .)
    if [ "$STAKE_COUNT" -ne "$NUM_GL0_NODES" ]; then
      echo "ERROR: NAKAMOTO_STAKE_DISTRIBUTION has $STAKE_COUNT entries but NUM_GL0_NODES=$NUM_GL0_NODES"
      exit 1
    fi
    STAKE_SUM=$(echo "$NAKAMOTO_STAKE_DISTRIBUTION" | tr ',' '\n' | awk '{s+=$1} END{printf "%.4f", s}')
    awk -v s="$STAKE_SUM" 'BEGIN{ if (s < 0.99 || s > 1.01) exit 1 }' || {
      echo "ERROR: NAKAMOTO_STAKE_DISTRIBUTION sum=$STAKE_SUM, must be ≈ 1.0"
      exit 1
    }

    # Invoke the generator. --keys-from ./nodes lets the generator REUSE the existing
    # `nodes/N/key.p12` operator credentials (rather than synthesizing fresh keys) so the
    # peerIds + addresses in `l0-genesis.json` match the runtime credentials downstream.
    GENESIS_SEED=${NAKAMOTO_GENESIS_SEED:-42}
    STAKE_BUDGET=${NAKAMOTO_STAKE_BUDGET_DATUM:-1000000000000}
    COLLATERAL=${NAKAMOTO_COLLATERAL_PER_OPERATOR:-0}
    mkdir -p ./nodes/_genesis-out
    java --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
         --add-opens=java.base/java.util=ALL-UNNAMED \
         --add-opens=java.base/java.security=ALL-UNNAMED \
         -jar ./docker/jars/tools.jar generate-genesis \
         --output-dir ./nodes/_genesis-out \
         --num-operators "$NUM_GL0_NODES" \
         --keys-from ./nodes \
         --stake-distribution "$NAKAMOTO_STAKE_DISTRIBUTION" \
         --stake-budget-datum "$STAKE_BUDGET" \
         --collateral-per-operator "$COLLATERAL" \
         --initial-balances-csv ./.github/config/genesis.csv \
         --seed "$GENESIS_SEED" \
         --network-magic test-cluster

    if [ ! -f ./nodes/_genesis-out/l0-genesis.json ]; then
      echo "ERROR: generator did not produce l0-genesis.json"
      exit 1
    fi

    # Per-node copy + opt-in env vars. Each gl0 node sees the same l0-genesis.json — the
    # `loadL0Genesis` reader is deterministic w.r.t. its input file, so all nodes derive the
    # same seeded `GlobalSnapshotInfo`.
    # - CL_GENESIS_JSON_HOST: docker-compose mounts ./genesis.json → /tessellation/genesis.json
    # - CL_GENESIS_CONTAINER_PATH: entrypoint.sh passes this to run-nakamoto (dispatches JSON branch)
    #
    # §1.2 Slice 3d: distribute per-operator KES SK files. Slice 3b writes them to
    # `_genesis-out/keys/operator-<N>/kes-sk.bin`. Each container gets a PER-NODE copy
    # (operator N → node N) so the disk-backed SecureStore inside the container loads
    # exactly the SK whose master VK is registered in genesis for that node's PeerId.
    # Mount is RW (not :ro) because OperationalKeyMaker.evolveTo writes the evolved
    # key back to disk after each period rotation — the read-once scrub semantics
    # require the SecureStore to mutate the backing file.
    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      cp ./nodes/_genesis-out/l0-genesis.json ./nodes/$i/genesis.json
      # Append rather than overwrite — node-key-env-setup already wrote envrc + .env entries.
      echo "CL_GENESIS_JSON_HOST=./genesis.json" >> ./nodes/$i/.env
      echo "CL_GENESIS_CONTAINER_PATH=/tessellation/genesis.json" >> ./nodes/$i/.env

      # §1.2 Slice 3d: stage the per-operator KES SK file. Operator N maps to gl0 node N
      # (1:1 by the generator's per-operator-key allocation). If the file is missing the
      # generator didn't write KES SKs (older tools.jar) — log + skip; the gl0 will fall
      # back to the in-memory fresh-bootstrap path (no genesis match for KES verify, but
      # the cluster still boots and runs Ed25519-only).
      KES_SK_SRC="./nodes/_genesis-out/keys/operator-$i/kes-sk.bin"
      KES_SK_DST_DIR="./nodes/$i/kes"
      if [ -f "$KES_SK_SRC" ]; then
        mkdir -p "$KES_SK_DST_DIR"
        cp "$KES_SK_SRC" "$KES_SK_DST_DIR/kes-sk.bin"
        chmod 0600 "$KES_SK_DST_DIR/kes-sk.bin"
        echo "CL_KES_SECURE_STORE_HOST=./kes" >> ./nodes/$i/.env
        echo "CL_KES_SECURE_STORE_DIR=/tessellation/data/kes" >> ./nodes/$i/.env
      else
        echo "  WARN: $KES_SK_SRC missing; gl0-$i will fall back to in-memory KES bootstrap"
      fi
    done
    echo "Tier-1 l0-genesis.json + per-operator KES SK files propagated to $NUM_GL0_NODES gl0 nodes"
  fi


  echo "------------------------------------------------"
  echo "All deployment configurations now generated, proceeding to run cluster"
  echo "------------------------------------------------"


  # Build snapshot-streaming JAR (needed before BUILD_ONLY exit so `just build` produces it)
  if [ "$SKIP_STREAMING" != "true" ]; then
    if [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; then
      SS_DIR="$PROJECT_ROOT/docker/snapshot-streaming"
      source "$SS_DIR/build-snapshot-streaming.sh"
      cd "$PROJECT_ROOT"
    fi
  fi

  if [ "$BUILD_ONLY" = "true" ]; then
    echo "Build only mode, skipping container startup and end-to-end tests"
    exit 0
  fi


  # /16 subnet — hypergraph at NET_BASE.0.0/24, metagraph k at NET_BASE.${k+1}.0/24.
  # Allows K parallel metagraph clusters with disjoint /24s, no IP collisions.
  NET_BASE_SUBNET=${NET_PREFIX%.*}
  # --ip-range confines Docker's DYNAMIC IP allocation to the UPPER half of the /16
  # (NET_BASE.128.0/17 = .128.0 .. .255.254) so it can NEVER overlap the STATIC
  # assignments, which all live in the LOWER half (NET_BASE.0..127.x):
  #   gl0  NET_BASE.0.(GL0_IP_BASE+i)   = .0.10..   (docker-env-setup.sh)
  #   gl1  NET_BASE.0.(GL1_IP_BASE+i)   = .0.30..
  #   snapshot-streaming               = .0.60 / .0.61
  #   metagraph k {ml0,cl1,dl1}        = NET_BASE.(k+1).{30,40,50}+i  (k>=0 ⇒ <.128.0)
  # Containers with NO ipv4_address — the Go sidecars (one per gl0, addressed by
  # container DNS name so a dynamic IP is fine), plus prometheus/nakamoto-grafana/
  # grafana-renderer/tx-sender — used to climb from .0.2 upward and collided with
  # gl0's static band once the 9th sidecar reached .0.10+. Pinning the dynamic pool
  # to .128.0/17 (~32k addrs, far more than the ~25 unpinned containers at N=16)
  # removes that collision class entirely while leaving every static IP outside the
  # range (Docker requires static IPs to be in the subnet but NOT in --ip-range).
  docker network create \
    --driver=bridge \
    --subnet=${NET_BASE_SUBNET}.0.0/16 \
    --ip-range=${NET_BASE_SUBNET}.128.0/17 \
    tessellation_common

  # Phase 1a: Seed compose files into hypergraph operator dirs (gl0+gl1)
  for i in $(seq 0 $((MAX_HG_NODES - 1))); do
    cd ./nodes/$i/

    docker compose -f docker-compose.test.yaml \
    -f docker-compose.yaml \
    -f docker-compose.volumes.yaml \
    down --remove-orphans --volumes > /dev/null 2>&1 || true;

    cp ../../docker/docker-compose.yaml . ; \
    cp ../../docker/docker-compose.test.yaml . ; \
    cp ../../docker/docker-compose.volumes.yaml . ; \

    cp ../../docker/docker-compose.nakamoto-sidecar.yaml . ;
    cp ../../docker/docker-compose.nakamoto-overlay.yaml . ;

    cd ../../
  done

  # Phase 1b: Seed compose files into per-metagraph operator dirs (ml0+cl1+dl1)
  if [ -n "$METAGRAPH" ]; then
    for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
      for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
        if [ ! -d "./nodes/m${k}-${i}" ]; then
          continue
        fi
        cd ./nodes/m${k}-${i}/

        docker compose -f docker-compose.metagraph.yaml \
        -f docker-compose.metagraph-test.yaml \
        down --remove-orphans --volumes > /dev/null 2>&1 || true;

        cp ../../docker/docker-compose.metagraph.yaml . ;
        cp ../../docker/docker-compose.metagraph-test.yaml . ;
        cp ../../docker/docker-compose.metagraph-genesis.yaml . ;

        cd ../../
      done
    done
  fi

  # Nakamoto mode: write shared genesis time, sidecar peer list, JVM seedlist,
  # and genesis CSV into each gl0 node's .env + node dir. All gl0 nodes MUST
  # agree on the same genesis time and seedlist for VRF consensus to work.
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
      # Compute per-node IP and P2P port from the test cluster layout. ARITHMETIC
      # (bases exported by set-env.sh) — MUST match the bound IP/internal-P2P port
      # written by docker-env-setup.sh (CL_DOCKER_GL0_IPV4 / CL_DOCKER_INTERNAL_GL0_P2P),
      # since gl0 nodes dial each other at this address. Byte-identical to the legacy
      # "${NET_PREFIX}.1${j}" / "${DAG_L0_PORT_PREFIX}${j}1" string-concat for j<10.
      NODE_IP="${NET_PREFIX}.$((GL0_IP_BASE + j))"
      NODE_P2P_PORT="$((GL0_PORT_BASE + j*10 + 1))"
      NAKAMOTO_JVM_SEEDLIST="${NAKAMOTO_JVM_SEEDLIST}${PEER_ID},${NODE_IP},${NODE_P2P_PORT},,\n"
    done

    # Append per-metagraph operator peer IDs with alias=metagraph-op so gl0 will
    # accept their state-channel binary signatures (validateSignaturesWithSeedlist
    # requires at least one signer to be in the seedlist). The "metagraph-op"
    # marker is filtered out of StakeRegistry validators in
    # GlobalSnapshotConsensus.scala so VRF stake stays at 1/N over hg validators
    # only — adding metagraph signers would otherwise dilute it.
    if [ -n "$METAGRAPH" ]; then
      for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
        M_PREFIX="${NET_BASE}.$((k + 1))"
        M_ML0_PORT_PREFIX=$((ML0_PORT_PREFIX - k*10))
        for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
          M_PEER_FILE="./nodes/m${k}-${i}/peer_id"
          if [ -f "$M_PEER_FILE" ]; then
            M_PEER_ID=$(cat "$M_PEER_FILE")
            M_NODE_IP="${M_PREFIX}.3${i}"
            M_NODE_P2P_PORT="${M_ML0_PORT_PREFIX}${i}1"
            NAKAMOTO_JVM_SEEDLIST="${NAKAMOTO_JVM_SEEDLIST}${M_PEER_ID},${M_NODE_IP},${M_NODE_P2P_PORT},metagraph-op,\n"
          fi
        done
      done
    fi

    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      # Write the JVM seedlist file into each node directory
      printf "$NAKAMOTO_JVM_SEEDLIST" > ./nodes/$i/seedlist.csv
      # Copy genesis.csv to each gl0 node (run-nakamoto needs it on every node,
      # not just node 0 — all nodes derive the same genesis state independently)
      cp ./nodes/0/genesis.csv ./nodes/$i/genesis.csv 2>/dev/null || true

      {
        echo ""
        echo "# Nakamoto GL0 mode"
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

  # Start all GL0 nodes together
  for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
    cd ./nodes/$i/
    nakamoto_compose_args="-f docker-compose.nakamoto-sidecar.yaml -f docker-compose.nakamoto-overlay.yaml"
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

  # Phase 2: Start GL1 nodes (GL0 is now ready for peer discovery).
  # GL1 lives in the hypergraph operator dirs alongside GL0.
  for i in $(seq 0 $((MAX_HG_NODES - 1))); do
    cd ./nodes/$i/

    if [ "$i" -lt "$NUM_GL1_NODES" ]; then
      # Apply nakamoto overlays for gl1 too — gl1 forwards DAG/AllowSpend/TokenLock
      # blocks to gl0 via the sidecar (#196/#197), which requires the SIDECAR_HOST
      # env defined in docker-compose.nakamoto-overlay.yaml's gl1 service block.
      # Without these `-f` flags the overlay is silently skipped and gl1 falls back
      # to 127.0.0.1:50051 with nothing listening → publishes silently drop.
      docker compose -f docker-compose.test.yaml \
      -f docker-compose.yaml \
      -f docker-compose.volumes.yaml \
      $nakamoto_compose_args \
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

    # Per-metagraph startup. Each metagraph k has its own operator dirs at
    # nodes/m${k}-${i} with distinct keystore. Genesis is m${k}-0; its
    # genesis.address gives the metagraph's address (METAGRAPH_ID).
    declare -a METAGRAPH_IDS=()
    for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
      M_PREFIX="m${k}"
      echo "================================================"
      echo "Bringing up metagraph $k (operators: nodes/${M_PREFIX}-*/)"
      echo "================================================"

      # Phase 1: Genesis creation + ML0 start
      for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
        if [ ! -d "./nodes/${M_PREFIX}-${i}" ]; then
          continue
        fi
        cd ./nodes/${M_PREFIX}-${i}/

        if [ ! -f "./genesis.snapshot" ] && [ "$i" -eq 0 ]; then
          echo "Generating metagraph $k genesis snapshot"
          cp .env .env.bak
          echo "CL_ML0_GENERATE_GENESIS=true" >> .env
          docker compose $metagraph_args -f docker-compose.metagraph-genesis.yaml --profile ml0 up
          docker stop ml0-${M_PREFIX}-0
          docker rm ml0-${M_PREFIX}-0
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
        # Capture this metagraph's ID from m${k}-0/genesis.address; reuse for
        # validators. METAGRAPH_IDS[k] holds the address; written into every
        # operator dir's .env so cl1/dl1/ml0 share the L0_TOKEN_IDENTIFIER.
        if [ "$i" -eq 0 ] && [ -f "./genesis.address" ]; then
          MK_ID=$(head -n 1 genesis.address)
          METAGRAPH_IDS[$k]="$MK_ID"
          export "M${k}_METAGRAPH_ID=$MK_ID"
          # Back-compat: METAGRAPH_ID without prefix == metagraph 0's ID.
          # JS tests today read process.env.METAGRAPH_ID expecting m0.
          if [ "$k" -eq 0 ]; then
            export METAGRAPH_ID="$MK_ID"
          fi
        fi
        MK_ID="${METAGRAPH_IDS[$k]:-${MK_ID:-}}"
        echo "METAGRAPH_ID=$MK_ID" >> .env
        echo "CL_L0_TOKEN_IDENTIFIER=$MK_ID" >> .env

        if [ "$i" -lt "$NUM_ML0_NODES" ]; then
          echo "Starting ML0 for metagraph $k node $i (container ml0-${M_PREFIX}-${i})"
          docker compose $metagraph_args --profile ml0 up -d
        fi

        cd ../../
      done

      # Wait for THIS metagraph's ML0 to be ready before starting its CL1/DL1
      if [ "$NUM_ML0_NODES" -gt 0 ]; then
        echo "Waiting for metagraph $k ML0 to be ready before starting CL1/DL1..."
        # Per-metagraph external ml0 port: M_ML0_PORT_PREFIX shifts -10 per k
        # to avoid host-port collisions across metagraphs (m0=92xx, m1=82xx, ...).
        M_ML0_PORT_PREFIX=$((ML0_PORT_PREFIX - k*10))
        ml0_url="${TEST_HOST:-http://localhost}:${M_ML0_PORT_PREFIX}00"
        ml0_ready=false
        for attempt in $(seq 1 120); do
          cluster_info=$(curl -s "${ml0_url}/cluster/info" 2>/dev/null || echo "")
          if [ -n "$cluster_info" ] && echo "$cluster_info" | jq 'length' >/dev/null 2>&1; then
            node_count=$(echo "$cluster_info" | jq 'length')
            if [ "$node_count" -ge 1 ]; then
              echo "Metagraph $k ML0 is ready with $node_count node(s)"
              ml0_ready=true
              break
            fi
          fi
          echo "Metagraph $k ML0 not ready yet (attempt $attempt/120), waiting..."
          sleep 5
        done
        if [ "$ml0_ready" = "false" ]; then
          echo "ERROR: Metagraph $k ML0 did not become ready in time"
          docker logs ml0-${M_PREFIX}-0 || true
          exit 1
        fi
      fi

      # Phase 2: Start CL1/DL1 for this metagraph
      for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
        if [ ! -d "./nodes/${M_PREFIX}-${i}" ]; then
          continue
        fi
        cd ./nodes/${M_PREFIX}-${i}/

        l1_profile_args=""
        if [ "$i" -lt "$NUM_CL1_NODES" ]; then
          l1_profile_args="$l1_profile_args --profile cl1"
        fi
        if [ "$i" -lt "$NUM_DL1_NODES" ]; then
          l1_profile_args="$l1_profile_args --profile dl1"
        fi
        l1_profile_args=$(echo $l1_profile_args | xargs)

        if [ -n "$l1_profile_args" ]; then
          echo "Starting CL1/DL1 for metagraph $k node $i"
          docker compose $metagraph_args $l1_profile_args up -d
        fi

        cd ../../
      done
    done

    # Export aggregate METAGRAPH_IDS_CSV so JS tests can iterate K metagraphs
    METAGRAPH_IDS_CSV=""
    for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
      [ -n "$METAGRAPH_IDS_CSV" ] && METAGRAPH_IDS_CSV="${METAGRAPH_IDS_CSV},"
      METAGRAPH_IDS_CSV="${METAGRAPH_IDS_CSV}${METAGRAPH_IDS[$k]:-}"
    done
    export METAGRAPH_IDS_CSV
    echo "METAGRAPH_IDS_CSV=${METAGRAPH_IDS_CSV}"
  fi


  # --- Snapshot-streaming infrastructure ---
  if [ "$SKIP_STREAMING" != "true" ] && { [ -z "$SELECTED_TESTS" ] || echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "snapshot-streaming"; }; then
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
    echo "  - job_name: 'gl0'" >> "$PROM_CFG"
    echo "    metrics_path: '/metrics'" >> "$PROM_CFG"
    echo "    static_configs:" >> "$PROM_CFG"
    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      # gl0 metrics = its public HTTP port (internal == external for gl0).
      # ARITHMETIC, matching docker-env-setup.sh; byte-identical to the legacy
      # "${DAG_L0_PORT_PREFIX}${i}0" string-concat for i<10. (Prometheus scrapes
      # by container DNS name + INTERNAL port, so the lifted host band is irrelevant.)
      port=$((GL0_PORT_BASE + i*10))
      echo "      - targets: ['gl0-$i:$port']" >> "$PROM_CFG"
      echo "        labels: { layer: 'gl0', node: 'gl0-$i' }" >> "$PROM_CFG"
    done

    # Sidecar targets
    echo "  - job_name: 'sidecar'" >> "$PROM_CFG"
    echo "    metrics_path: '/metrics'" >> "$PROM_CFG"
    echo "    static_configs:" >> "$PROM_CFG"
    for i in $(seq 0 $((NUM_GL0_NODES - 1))); do
      echo "      - targets: ['sidecar-$i:9501']" >> "$PROM_CFG"
      echo "        labels: { layer: 'sidecar', node: 'sidecar-$i' }" >> "$PROM_CFG"
    done

    # Monitoring trio: Prometheus (scraper), Grafana (UI), and grafana-image-renderer
    # (server-side PNG rendering for /render/ endpoints so Grafana can serve dashboard
    # panels as images — required for headless snapshots, screenshots in reports, etc).
    # All three live on tessellation_common so Grafana can reach both Prometheus
    # (http://prometheus:9090) and the renderer (http://grafana-renderer:8081) by DNS.
    docker rm -f prometheus nakamoto-grafana grafana-renderer 2>/dev/null || true

    docker run -d --name grafana-renderer --network tessellation_common \
      -e ENABLE_METRICS=true \
      --restart unless-stopped grafana/grafana-image-renderer:latest >/dev/null 2>&1

    # Prometheus HOST port: gl0-9's public port is 9090 (GL0_PORT_BASE + 9*10), so
    # the legacy `-p 9090:9090` collides with the gl0 host band once N>9. Lift the
    # host mapping to PROMETHEUS_HOST_PORT (default 19090); the container port stays
    # 9090 so Grafana's `http://prometheus:9090` datasource is unaffected.
    PROMETHEUS_HOST_PORT=${PROMETHEUS_HOST_PORT:-19090}
    docker run -d --name prometheus --network tessellation_common \
      -v "$PROM_CFG:/etc/prometheus/prometheus.yml:ro" \
      -p ${PROMETHEUS_HOST_PORT}:9090 --restart unless-stopped prom/prometheus:latest >/dev/null 2>&1

    docker run -d --name nakamoto-grafana --network tessellation_common \
      -e GF_SECURITY_ADMIN_USER=admin -e GF_SECURITY_ADMIN_PASSWORD=admin \
      -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer \
      -e GF_RENDERING_SERVER_URL=http://grafana-renderer:8081/render \
      -e GF_RENDERING_CALLBACK_URL=http://nakamoto-grafana:3000/ \
      -p 3000:3000 \
      -v "$PROJECT_ROOT/nakamoto-test/grafana/provisioning:/etc/grafana/provisioning:ro" \
      -v "$PROJECT_ROOT/nakamoto-test/grafana/dashboards:/var/lib/grafana/dashboards:ro" \
      --restart unless-stopped grafana/grafana:latest >/dev/null 2>&1

    echo "  Grafana: http://localhost:3000 (admin/admin)"
    echo "  Prometheus: http://localhost:${PROMETHEUS_HOST_PORT}"
    echo "  Renderer: http://grafana-renderer:8081 (internal)"
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
  # --skip-streaming excludes snapshot-streaming from default "run all" mode
  if [ "$SKIP_STREAMING" = "true" ] && [ "$test_name" = "snapshot-streaming" ]; then
    return 1
  fi
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
npm i @stardust-collective/dag4 js-sha256 axios zod elliptic

if [ -z "$REMOTE_HOST" ] || [ "$REMOTE_HOST" = "http://localhost" ]; then
  sleep 10
  docker logs gl0-0
  echo "GL0-0 logs above, now continuing with cluster health check."
fi

source ../../docker/bin/cluster-health-check.sh
verify_healthy
show_time "Cluster became healthy"

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

  # Multi-metagraph sanity test. Runs by default when NUM_METAGRAPHS>=2 (default
  # under `just test` since 2026-05-08), or explicitly when --test=multi-metagraph
  # is selected. With NUM_METAGRAPHS=1 (single-metagraph runs) it's skipped to
  # avoid silent passes — the test refuses K<2 by design.
  if should_run_test "multi-metagraph" && [ "${NUM_METAGRAPHS:-1}" -ge 2 ]; then
    echo "================================================"
    echo "Running multi-metagraph sanity test (K=${NUM_METAGRAPHS})"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node check_clusters/multi-metagraph.js
    show_time "Multi-metagraph sanity test completed"
  elif [ -n "$SELECTED_TESTS" ] && echo "$SELECTED_TESTS" | tr ',' '\n' | grep -qx "multi-metagraph"; then
    # Explicit --test=multi-metagraph with NUM_METAGRAPHS<2: hard error so the
    # caller notices the misconfiguration instead of silently no-op'ing.
    echo "ERROR: multi-metagraph test requires --metagraphs=K with K>=2 (got NUM_METAGRAPHS=${NUM_METAGRAPHS:-1})"
    exit 1
  fi

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

  # Data-transaction tests sign a UsageUpdateWithFee with an account that has DAG balance.
  # Default to PRIVATE_KEYS.key1 from shared/constants.js (funded via genesis.csv) so local
  # and CI runs exercise these by default. Override with CI_PRIVATE_KEY=... if a test needs
  # a specific signer (e.g. a metagraph whose data app enforces owner-only updates).
  TEST_DATA_PRIVATE_KEY="${CI_PRIVATE_KEY:-595a30ab6c62ae48a23414951e2703f49f8c0040b9801738ad3550475389d811}"

  if should_run_test "data-without-fee"; then
    echo "================================================"
    echo "Running data transaction tests (without fee)"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node send_transactions/data-without-fee.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $TEST_DATA_PRIVATE_KEY
    show_time "Data transaction tests (without fee) completed"
  fi

  if should_run_test "data-with-fee"; then
    echo "================================================"
    echo "Running data transaction tests (with fee)"
    echo "================================================"
    cd $PROJECT_ROOT/.github/action_scripts
    node send_transactions/data-with-fee.js $DAG_L0_PORT_PREFIX $DAG_L1_PORT_PREFIX $ML0_PORT_PREFIX $CL1_PORT_PREFIX $DL1_PORT_PREFIX $TEST_DATA_PRIVATE_KEY
    show_time "Data transaction tests (with fee) completed"
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




