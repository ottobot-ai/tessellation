
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

CL_DOCKER_GL0_JOIN_IP=${NET_PREFIX}.$((GL0_IP_BASE + 0))
# gl1-0's IP moved with GL1_IP_BASE (.20 → .30) so gl0's arithmetic IP band can't
# overlap gl1. The BFT join target (gl1-0) is updated here in lockstep with the
# per-node CL_DOCKER_GL1_IPV4 written in the loop below.
CL_DOCKER_GL1_JOIN_IP=${NET_PREFIX}.$((GL1_IP_BASE + 0))

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

  # Per-node IP + ports — ARITHMETIC allocation (bases exported by set-env.sh) so
  # the hypergraph scales past a single digit. Byte-identical to the legacy
  # string-concat scheme for i<10. The seedlist builders in compose-runner.sh use
  # the SAME bases, so the dial targets stay consistent with these bound ports/IPs.
  #
  #   gl0 IP   .(GL0_IP_BASE + i)            (legacy .1${i})
  #   gl1 IP   .(GL1_IP_BASE + i)            (legacy .2${i}; base moved to .30)
  #   gl0 ports  GL0_PORT_BASE + i*10        external==internal (legacy 90${i}{0,1,2})
  #   gl1 internal ports  GL1_INT_PORT_BASE + i*10   (legacy 9100 band — gl1-0:9100 alias)
  #   gl1 external ports  GL1_EXT_PORT_BASE + i*10   (lifted to avoid gl0/metagraph bands)
  L0_PUBLIC=$((GL0_PORT_BASE + i*10))
  L1_PUBLIC_EXT=$((GL1_EXT_PORT_BASE + i*10))
  L1_PUBLIC_INT=$((GL1_INT_PORT_BASE + i*10))

  echo "CL_DOCKER_GL0_IPV4=${NET_PREFIX}.$((GL0_IP_BASE + i))" >> .env
  echo "CL_DOCKER_GL1_IPV4=${NET_PREFIX}.$((GL1_IP_BASE + i))" >> .env

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
  # internal (container) ports — gl0 keeps external==internal; gl1 internal stays
  # in the legacy 9100 band (so gl1-0:9100 + the BFT join port 9101 still resolve)
  # while gl1 external is lifted to GL1_EXT_PORT_BASE above.
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
