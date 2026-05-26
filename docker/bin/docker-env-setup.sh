
# Hypergraph operator identity (nodes/0 is the gl0 genesis node)
export GL0_GENERATED_WALLET_PEER_ID=$(cat ./nodes/0/peer_id)
echo "Generated GL0 wallet peer id $GL0_GENERATED_WALLET_PEER_ID"
export ADDRESS=$(cat ./nodes/0/address)
echo "Generated GL0 wallet address $ADDRESS"

# /16 base for per-metagraph subnets: NET_PREFIX="172.32.0" → NET_BASE="172.32"
# Hypergraph stays at ${NET_PREFIX}.* (172.32.0.{10,20}+i). Metagraph k lives
# at ${NET_BASE}.${k+1}.{30,40,50}+i so each metagraph has its own /24 within
# the shared /16 — no IP collisions across K metagraphs.
export NET_BASE=${NET_PREFIX%.*}

#CL_GLOBAL_L0_PEER_ID", help = "Global L0 peer Id"))
#CL_GLOBAL_L0_PEER_HTTP_HOST", help = "Global L0 peer HTTP host"))
# CL_GLOBAL_L0_PEER_HTTP_PORT", help = "Global L0 peer HTTP port

# Shared base .env — hypergraph globals + gl0 connection info needed by every
# layer (gl0/gl1 + every metagraph's ml0/cl1/dl1 connect to this same gl0).
# Per-metagraph CL_DOCKER_ML0/CL1/DL1_* entries are NOT in this base — they
# get written into nodes/m${k}-${i}/.env per-metagraph in the loop below.
cat << EOF > ./nodes/.env
CL_APP_ENV="dev"
CL_COLLATERAL=0
TESSELLATION_DOCKER_VERSION=test
CL_DOCKER_METAGRAPH_IMAGE=test
CL_TEST_MODE=true
CL_LOCAL_MODE=true
NET_PREFIX=${NET_PREFIX}
NET_BASE=${NET_BASE}
CL_DOCKER_BIND_INTERFACE=${CL_DOCKER_BIND_INTERFACE}

CL_GLOBAL_L0_PEER_ID=${GL0_GENERATED_WALLET_PEER_ID}
CL_GLOBAL_L0_PEER_HTTP_HOST=${NET_PREFIX}.10
CL_GLOBAL_L0_PEER_HTTP_PORT=${DAG_L0_PORT_PREFIX}00

# These must be unique for each service, this is used by GL1 for example to hit GL0
# CL_L0_PEER_ID=$GL0_GENERATED_WALLET_PEER_ID
CL_DOCKER_GL0_PEER_ID=$GL0_GENERATED_WALLET_PEER_ID
# CL_L0_PEER_HTTP_HOST=${NET_PREFIX}.10
CL_DOCKER_GL0_PEER_HTTP_HOST=${NET_PREFIX}.10
# CL_L0_PEER_HTTP_PORT=${DAG_L0_PORT_PREFIX}00
CL_DOCKER_GL0_PEER_HTTP_PORT=${DAG_L0_PORT_PREFIX}00

# Hypergraph-internal join IDs (gl0/gl1 piggyback on hg node 0)
CL_DOCKER_GL0_JOIN_ID=$GL0_GENERATED_WALLET_PEER_ID
CL_DOCKER_GL1_JOIN_ID=$GL0_GENERATED_WALLET_PEER_ID

CL_DOCKER_GL0_JOIN_IP=${NET_PREFIX}.10
# gl1-0 base moved 20→60 (matches CL_DOCKER_GL1_IPV4 arithmetic in the per-node
# loop) so gl0 (.10..) and gl1 (.60..) IP ranges stay disjoint at >9 nodes.
CL_DOCKER_GL1_JOIN_IP=${NET_PREFIX}.60

CL_DOCKER_GL0_JOIN_PORT=${DAG_L0_PORT_PREFIX}01
CL_DOCKER_GL1_JOIN_PORT=${DAG_L1_PORT_PREFIX}01

EOF


if [ -f "${EXTRA_ENV_PATH:-/dev/null}" ]; then
  EXTRA_ENV=$(cat $EXTRA_ENV_PATH)
  echo "Extra env: $EXTRA_ENV"
  echo "$EXTRA_ENV" >> ./nodes/.env
fi

# maybe re-enable later -- these are the current mainnet defaults
# CL_DOCKER_JAVA_OPTS="-Xms512M -Xss256K -Xmx8192M"

# Append any CL_TEST_* environment variables from the current bash environment
echo "" >> ./nodes/.env
echo "# Test environment variables from host" >> ./nodes/.env
if env | grep -q "^CL_TEST_"; then
  env | grep "^CL_TEST_" | while IFS= read -r line; do
    echo "$line" >> ./nodes/.env
  done
fi

# Seed every hypergraph and metagraph operator dir with the shared base .env
for i in $(seq 0 $((MAX_HG_NODES - 1))); do
  cp ./nodes/.env ./nodes/$i/.env
  cp ./nodes/.envrc ./nodes/$i/.envrc
done
for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
  for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
    if [ -d "./nodes/m${k}-${i}" ]; then
      cp ./nodes/.env ./nodes/m${k}-${i}/.env
      cp ./nodes/.envrc ./nodes/m${k}-${i}/.envrc
    fi
  done
done

if [ "$SET_FAILURE_BREAKPOINT_TIME" == "true" ]; then
  # Default failure time +100 seconds, adjust if needed.
  FAILURE_TIME=$(($(date +%s) + 100))
  echo "CL_TEST_SIMULATE_GOSSIP_FAIL_TIME=$FAILURE_TIME" >> ./nodes/1/.env
  echo "Setting gossip failure simulation to trigger at $(date -d @$FAILURE_TIME 2>/dev/null || date -r $FAILURE_TIME) (100 seconds from now) $FAILURE_TIME"
fi


cp ./.github/config/genesis.csv ./nodes/0/genesis.csv

cd ./nodes/0/
# 1000000 * 1e8
# Ensure the file ends with a newline before appending
echo "" >> genesis.csv
echo "$ADDRESS,100000000000000" >> genesis.csv
echo "Generated genesis file:"
cat genesis.csv
# sed -i.bak '${\n/^$/d\n}' genesis.csv && rm -f genesis.csv.bak
echo "CL_GENESIS_FILE=./genesis.csv" >> .env

cd ../../

# Each metagraph genesis node (m${k}-0) needs a genesis.csv to feed its
# create-genesis bootstrap (entrypoint.sh:167 gates on CL_GENESIS_FILE).
# We reuse the hg genesis CSV so metagraph initial currency supply tracks
# the test address allocations. Copy into every operator dir so validators
# also have the file present (harmless — they don't enter the create-genesis
# branch since CL_DOCKER_GENESIS=false there).
for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
  for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
    if [ -d "./nodes/m${k}-${i}" ]; then
      cp ./nodes/0/genesis.csv ./nodes/m${k}-${i}/genesis.csv
      echo "CL_GENESIS_FILE=./genesis.csv" >> ./nodes/m${k}-${i}/.env
    fi
  done
done


# gl0/gl1 port bases. GL1_EXT_BASE (gl1 HOST port base) is computed once in
# set-env.sh and inherited here (shard-sortition Slice S7); recompute as a
# fallback if this script is sourced standalone. For ≤9 gl0 nodes GL1_EXT_BASE
# == ${DAG_L1_PORT_PREFIX}00 (9100, legacy). For larger N it lifts above the gl0
# external band so gl0/gl1 never share a host port. gl1's INTERNAL/container port
# stays at the legacy 9100 band (GL1_PORT_BASE), so the `gl1-0:9100` container
# alias (tx-sender.conf, in-network clients) is unaffected.
GL0_PORT_BASE=$((DAG_L0_PORT_PREFIX * 100))
GL1_PORT_BASE=$((DAG_L1_PORT_PREFIX * 100))
if [ -z "${GL1_EXT_BASE:-}" ]; then
  GL0_EXT_TOP=$((GL0_PORT_BASE + (${NUM_GL0_NODES:-MAX_HG_NODES} - 1) * 10 + 2))
  GL0_EXT_TOP_ROUNDED=$(( (GL0_EXT_TOP / 100 + 1) * 100 ))
  GL1_EXT_BASE=$GL1_PORT_BASE
  [ "$GL0_EXT_TOP_ROUNDED" -gt "$GL1_EXT_BASE" ] && GL1_EXT_BASE=$GL0_EXT_TOP_ROUNDED
  export GL1_EXT_BASE
fi
echo "[docker-env] gl0 ext base ${GL0_PORT_BASE}; gl1 ext base ${GL1_EXT_BASE}; gl1 internal base ${GL1_PORT_BASE}"

# === Hypergraph operators (nodes/$i — gl0 + gl1 only) ===
for i in $(seq 0 $((MAX_HG_NODES - 1))); do
  cd ./nodes/$i

  if [ "$i" != "0" ]; then
    echo "CL_DOCKER_GL1_JOIN=true" >> .env
    echo "CL_DOCKER_GL0_JOIN=true" >> .env
  else
    echo "CL_DOCKER_GL1_JOIN=false" >> .env
    echo "CL_DOCKER_GL0_JOIN=false" >> .env
    echo "CL_DOCKER_GL0_GENESIS=true" >> .env
    echo "CL_DOCKER_GL1_GENESIS=true" >> .env
    # Rollback settings only for genesis node - it does run-rollback
    # Followers use run-validator and join/download from the leader
    if [ "$ROLLBACK_MODE" == "true" ]; then
      echo "CL_DOCKER_ROLLBACK=true" >> .env
      if [ -n "$ROLLBACK_HASH" ]; then
        echo "CL_DOCKER_ROLLBACK_HASH=$ROLLBACK_HASH" >> .env
      fi
    fi
  fi

  echo "CONTAINER_NAME_SUFFIX=-$i" >> .env
  echo "CONTAINER_OFFSET=$i" >> .env

  # Per-node ports — ARITHMETIC allocation (shard-sortition Slice S7).
  # stride = 10 ⇒ node i owns the 10-port window [base + i*10 .. +2].
  #   * gl0 external==internal at ${PREFIX0}00 + i*10 — byte-identical to the
  #     legacy "${PREFIX0}${i}{0,1,2}" string-concat for i<10 (gl0-0=9000/1/2 ..
  #     gl0-9=9090/1/2) and extending past 9 (32 gl0 → 9000..9312, < 65536).
  #   * gl1 INTERNAL stays at ${PREFIX1}00 + i*10 (legacy 9100 band) so the
  #     `gl1-0:9100` container alias keeps working.
  #   * gl1 EXTERNAL uses GL1_EXT_BASE (==9100 for ≤9 gl0, lifted above the gl0
  #     band for larger N) so gl0 and gl1 never collide on a host port.
  L0_PUBLIC=$((GL0_PORT_BASE + i*10))
  L1_PUBLIC_INT=$((GL1_PORT_BASE + i*10))
  L1_PUBLIC_EXT=$((GL1_EXT_BASE + i*10))

  # Per-node IP octet — ARITHMETIC so the hypergraph scales past 9 nodes.
  # gl0 lives at ${NET_PREFIX}.(10+i), gl1 at ${NET_PREFIX}.(60+i): disjoint
  # ranges (gl0 .10.., gl1 .60..) up to 49 nodes/tier, and byte-identical to the
  # legacy ".1${i}" gl0 layout for i<10 (.10..19). gl1's base moves 20→60 to keep
  # the ranges disjoint at scale; the matching join IP below is updated in lockstep.
  echo "CL_DOCKER_GL0_IPV4=${NET_PREFIX}.$((10 + i))" >> .env
  echo "CL_DOCKER_GL1_IPV4=${NET_PREFIX}.$((60 + i))" >> .env

  # External (host) ports
  echo "CL_DOCKER_EXTERNAL_GL0_PUBLIC=${L0_PUBLIC}" >> .env
  echo "CL_DOCKER_EXTERNAL_GL0_P2P=$((L0_PUBLIC + 1))" >> .env
  echo "CL_DOCKER_EXTERNAL_GL0_CLI=$((L0_PUBLIC + 2))" >> .env

  echo "CL_DOCKER_EXTERNAL_GL1_PUBLIC=${L1_PUBLIC_EXT}" >> .env
  echo "CL_DOCKER_EXTERNAL_GL1_P2P=$((L1_PUBLIC_EXT + 1))" >> .env
  echo "CL_DOCKER_EXTERNAL_GL1_CLI=$((L1_PUBLIC_EXT + 2))" >> .env

  # LocalEvents reactive gRPC stream — bind:50054 inside the container (set by
  # application.conf via NAKAMOTO_LOCAL_EVENTS_PORT). Host-side port stripes by
  # node index using the existing pattern: external port = 50054 + i*10 (50054 for
  # gl0-0, 50064 for gl0-1, etc.) so concurrent nodes don't collide on the host.
  # Tests reach gl0-0 at host port 50054 via TEST_HOST resolution.
  echo "CL_DOCKER_EXTERNAL_GL0_LOCAL_EVENTS=$((50054 + i*10))" >> .env

  # These are only required on systems that implement docker with a host networking bridge
  # Port conflicts cause it to fail with external networks that re-use ports
  # internal ports (same arithmetic window as external — preserves the existing
  # external==internal invariant for the test cluster)
  echo "CL_DOCKER_INTERNAL_GL0_PUBLIC=${L0_PUBLIC}" >> .env
  echo "CL_DOCKER_INTERNAL_GL0_P2P=$((L0_PUBLIC + 1))" >> .env
  echo "CL_DOCKER_INTERNAL_GL0_CLI=$((L0_PUBLIC + 2))" >> .env

  echo "CL_DOCKER_INTERNAL_GL1_PUBLIC=${L1_PUBLIC_INT}" >> .env
  echo "CL_DOCKER_INTERNAL_GL1_P2P=$((L1_PUBLIC_INT + 1))" >> .env
  echo "CL_DOCKER_INTERNAL_GL1_CLI=$((L1_PUBLIC_INT + 2))" >> .env

  echo "CL_DOCKER_GL1_JOIN_INITIAL_DELAY=$((i*12 + 30))" >> .env

  cd ../../
done


# === Metagraph operators (nodes/m${k}-${i} — ml0 + cl1 + dl1, per metagraph k) ===
# Each metagraph has its own operator set with distinct keystore. genesis is
# m${k}-0 (its peer_id is the join target for ml0/cl1/dl1 validators within
# this metagraph). IP layout: ${NET_BASE}.${k+1}.{30,40,50}+i — per-metagraph
# /24 inside the shared /16 subnet, avoiding cross-metagraph IP collisions.
for k in $(seq 0 $((${NUM_METAGRAPHS:-1} - 1))); do
  M_PREFIX="${NET_BASE}.$((k + 1))"
  M_GENESIS_PEER_ID=""
  if [ -f "./nodes/m${k}-0/peer_id" ]; then
    M_GENESIS_PEER_ID=$(cat ./nodes/m${k}-0/peer_id)
  fi
  # Per-metagraph port prefix shift to avoid host-port collisions across K
  # metagraphs. m0 uses defaults (92/93/94); m1 uses 82/83/84; m2 = 72/73/74,
  # ... K=10 wraps but is well past the test cap.
  M_ML0_PORT_PREFIX=$((ML0_PORT_PREFIX - k*10))
  M_CL1_PORT_PREFIX=$((CL1_PORT_PREFIX - k*10))
  M_DL1_PORT_PREFIX=$((DL1_PORT_PREFIX - k*10))

  for i in $(seq 0 $((${MAX_METAGRAPH_NODES:-$MAX_NODES} - 1))); do
    if [ ! -d "./nodes/m${k}-${i}" ]; then
      continue
    fi
    cd ./nodes/m${k}-${i}

    if [ "$i" != "0" ]; then
      echo "CL_DOCKER_ML0_JOIN=true" >> .env
      echo "CL_DOCKER_CL1_JOIN=true" >> .env
      echo "CL_DOCKER_DL1_JOIN=true" >> .env
    else
      echo "CL_DOCKER_ML0_JOIN=false" >> .env
      echo "CL_DOCKER_CL1_JOIN=false" >> .env
      echo "CL_DOCKER_DL1_JOIN=false" >> .env
      echo "CL_DOCKER_ML0_GENESIS=true" >> .env
      echo "CL_DOCKER_CL1_GENESIS=true" >> .env
      echo "CL_DOCKER_DL1_GENESIS=true" >> .env
    fi

    # Two-dimensional suffix: container becomes ml0-m${k}-${i}, etc. Keeps
    # docker compose's container_name=ml0${CONTAINER_NAME_SUFFIX} working
    # (just expanded suffix). CONTAINER_OFFSET drives the per-node IP octet.
    echo "CONTAINER_NAME_SUFFIX=-m${k}-${i}" >> .env
    echo "CONTAINER_OFFSET=$i" >> .env
    # Per-metagraph IP base, consumed by docker-compose.metagraph-test.yaml
    # (ipv4_address aliases swapped from NET_PREFIX → METAGRAPH_NET_PREFIX).
    echo "METAGRAPH_NET_PREFIX=${M_PREFIX}" >> .env
    echo "METAGRAPH_INDEX=$k" >> .env

    # Layer ports — same per-i pattern as today, but applied within the
    # metagraph's own port-prefix range (single-digit i; 10 nodes max).
    # Use the per-k port prefix so multiple metagraphs don't fight for host ports.
    ML0_PORT="${M_ML0_PORT_PREFIX}$i"
    CL1_PORT="${M_CL1_PORT_PREFIX}$i"
    DL1_PORT="${M_DL1_PORT_PREFIX}$i"

    echo "CL_DOCKER_EXTERNAL_ML0_PUBLIC=${ML0_PORT}0" >> .env
    echo "CL_DOCKER_EXTERNAL_ML0_P2P=${ML0_PORT}1" >> .env
    echo "CL_DOCKER_EXTERNAL_ML0_CLI=${ML0_PORT}2" >> .env

    echo "CL_DOCKER_EXTERNAL_CL1_PUBLIC=${CL1_PORT}0" >> .env
    echo "CL_DOCKER_EXTERNAL_CL1_P2P=${CL1_PORT}1" >> .env
    echo "CL_DOCKER_EXTERNAL_CL1_CLI=${CL1_PORT}2" >> .env

    echo "CL_DOCKER_EXTERNAL_DL1_PUBLIC=${DL1_PORT}0" >> .env
    echo "CL_DOCKER_EXTERNAL_DL1_P2P=${DL1_PORT}1" >> .env
    echo "CL_DOCKER_EXTERNAL_DL1_CLI=${DL1_PORT}2" >> .env

    echo "CL_DOCKER_INTERNAL_ML0_PUBLIC=${ML0_PORT}0" >> .env
    echo "CL_DOCKER_INTERNAL_ML0_P2P=${ML0_PORT}1" >> .env
    echo "CL_DOCKER_INTERNAL_ML0_CLI=${ML0_PORT}2" >> .env

    echo "CL_DOCKER_INTERNAL_CL1_PUBLIC=${CL1_PORT}0" >> .env
    echo "CL_DOCKER_INTERNAL_CL1_P2P=${CL1_PORT}1" >> .env
    echo "CL_DOCKER_INTERNAL_CL1_CLI=${CL1_PORT}2" >> .env

    echo "CL_DOCKER_INTERNAL_DL1_PUBLIC=${DL1_PORT}0" >> .env
    echo "CL_DOCKER_INTERNAL_DL1_P2P=${DL1_PORT}1" >> .env
    echo "CL_DOCKER_INTERNAL_DL1_CLI=${DL1_PORT}2" >> .env

    # ml0 cluster connection (cl1/dl1 within this metagraph dial m${k}-0)
    echo "CL_DOCKER_ML0_PEER_ID=${M_GENESIS_PEER_ID}" >> .env
    echo "CL_DOCKER_ML0_PEER_HTTP_HOST=ml0-m${k}-0" >> .env
    echo "CL_DOCKER_ML0_PEER_HTTP_PORT=${M_ML0_PORT_PREFIX}00" >> .env

    # Layer join targets — all within this metagraph
    echo "CL_DOCKER_ML0_JOIN_ID=${M_GENESIS_PEER_ID}" >> .env
    echo "CL_DOCKER_CL1_JOIN_ID=${M_GENESIS_PEER_ID}" >> .env
    echo "CL_DOCKER_DL1_JOIN_ID=${M_GENESIS_PEER_ID}" >> .env

    echo "CL_DOCKER_ML0_JOIN_IP=${M_PREFIX}.30" >> .env
    echo "CL_DOCKER_CL1_JOIN_IP=${M_PREFIX}.40" >> .env
    echo "CL_DOCKER_DL1_JOIN_IP=${M_PREFIX}.50" >> .env

    echo "CL_DOCKER_ML0_JOIN_PORT=${M_ML0_PORT_PREFIX}01" >> .env
    echo "CL_DOCKER_CL1_JOIN_PORT=${M_CL1_PORT_PREFIX}01" >> .env
    echo "CL_DOCKER_DL1_JOIN_PORT=${M_DL1_PORT_PREFIX}01" >> .env

    # Surface per-metagraph port prefixes for compose substitutions or scripts
    echo "M_ML0_PORT_PREFIX=${M_ML0_PORT_PREFIX}" >> .env
    echo "M_CL1_PORT_PREFIX=${M_CL1_PORT_PREFIX}" >> .env
    echo "M_DL1_PORT_PREFIX=${M_DL1_PORT_PREFIX}" >> .env

    echo "CL_DOCKER_GL1_JOIN_INITIAL_DELAY=$((i*12 + 30))" >> .env
    echo "CL_DOCKER_DL1_JOIN_INITIAL_DELAY=$((i*12 + 30))" >> .env
    echo "CL_DOCKER_CL1_JOIN_INITIAL_DELAY=$((i*12 + 30))" >> .env

    cd ../../
  done
done
