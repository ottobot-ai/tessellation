#!/usr/bin/env bash
# Generate a docker-compose.yml for an N-node Nakamoto cluster.
# Usage: ./gen-cluster.sh <num_nodes> [output_file]
#   e.g., ./gen-cluster.sh 8
#   e.g., ./gen-cluster.sh 3 docker-compose-3node.yml

set -euo pipefail

NUM_NODES=${1:?Usage: gen-cluster.sh <num_nodes> [output_file]}
OUTPUT=${2:-docker-compose.yml}

if (( NUM_NODES < 1 || NUM_NODES > 8 )); then
  echo "Error: num_nodes must be 1-8 (we have keys 0-7)" >&2
  exit 1
fi

# Peer IDs (keys 0-7)
PEER_IDS=(
  "1b4b9f98190ede0d26ec1a2ce736638ffa556b08135403256d47d3c405e08e3bb272fb9172c7bd9f96008dd380ceb6201e965498bc9115bbdf905ad9781dbf18"
  "8dc987df37287cbfeb177e1593878a74db4e658506c491022187cb5c9cd840b8d788c938fd03e69d33f48d093d4cfc64591d04e1cf5553d8d7f65381ec93011e"
  "96410098053654f15a6db714c7457264c3b0358f04f86d4dc636cb7d549461c76a0ce948d1458acfa07a24e806fbda0635ded465d867e521c8d8d14fc0ac9c47"
  "b5534065956ff40c731ff0ee1a718016e864b9ded55c7b8b2d61d57c91f5d7b861d4a38dfa1b2e6b01ac141dff712932165fc4bd1d138b6e1d58b3ceb6f64cdf"
  "345647c83ed6ddfd3c1fd43ee6f75f6e107f10979b730e841eba230a004adb8d9b794e17a635f05810016ec5e7001cf2c452073a928a3baf8f0a8b1bb20c5dff"
  "2798cc2af90d515c21ec1400635f694952086fe38b2b25e8dd0f79497f4aa337d46a59ca636dfb9475424ad4d53158dfb483791438340275e8062a136ebe2829"
  "dbb4066bb1d2211309882ed3fac6121519cdd2956c20976920eb1ca704b43024eff9e37e6fadbc42f04c53280b74dee0422cc142bf21d7196c6e6707d3524442"
  "a0fddf2d5e042c8c8bfffe8c72b4699e73936de16532b7c9d23898b2e8c36a989090a67b549d96ddb7f53ba58ef9bebd46f07a8d5d7678ee78c07c2e5af4d923"
)

ADDRESSES=(
  "DAG3yG9CRoYd4XF4PTBtLo95h8uiGNWYXXrASJGg"
  "DAG5mDaWGvPK5XXe7tnYur7jFkdHdUWvJ4czeAyW"
  "DAG851Tzp3YBHVHEJk7drRP1gzMTscGfcMsQeGLe"
  "DAG7BMY7dU4sYtViV14xX4pxCbmGcz6i6Tm3d8qq"
  "DAG3MzhA2jhmcD5eVM4HAgxuEuMJas5e2MACrS3i"
  "DAG13wvY5G5ZixWVr9GqdnHUJhGxikA5g1zMxjff"
  "DAG8JEZNodV6ZuRwSfGg5PNRMiCyfzDnoyfbY6pG"
  "DAG83JJ2dfwuai4zvPsKFffN7Z2AanFuXpHwMw8e"
)

# Generate seedlist and genesis CSV
SEEDLIST_FILE="seedlist-${NUM_NODES}.csv"
GENESIS_FILE="genesis-${NUM_NODES}.csv"

> "$SEEDLIST_FILE"
> "$GENESIS_FILE"
for (( i=0; i<NUM_NODES; i++ )); do
  echo "${PEER_IDS[$i]}" >> "$SEEDLIST_FILE"
  echo "${ADDRESSES[$i]},1000000000000" >> "$GENESIS_FILE"
done

echo "Generated $SEEDLIST_FILE ($NUM_NODES entries)"
echo "Generated $GENESIS_FILE ($NUM_NODES entries)"

# Build docker-compose
cat > "$OUTPUT" <<'HEADER'
# Auto-generated Nakamoto cluster — do not edit manually
# Regenerate: ./gen-cluster.sh NUM_NODES

x-jvm-common: &jvm-common
  build:
    context: ..
    dockerfile: nakamoto-test/Dockerfile.jvm
  environment: &jvm-env
    CL_APP_ENV: dev
    NAKAMOTO_ENABLED: "true"
    NAKAMOTO_PURE: "true"
    NAKAMOTO_GENESIS_ETA: "tessellation-nakamoto-genesis"
    NAKAMOTO_SLOTS_PER_EPOCH: "60"
    NAKAMOTO_ETA_ROTATION_SLOTS: "${NAKAMOTO_ETA_ROTATION_SLOTS:-600}"
    # Test-only LDD overrides — production defaults are in LddConfig.Default (15/1/0.05/0.5).
    # Doubled here to roughly 4× chain throughput for faster e2e cycles. Cluster network
    # delay is ~0ms in docker so fA=1.0 + 8 nodes is fine despite the higher concurrent-winner rate.
    NAKAMOTO_LDD_CUTOFF: "${NAKAMOTO_LDD_CUTOFF:-32}"
    NAKAMOTO_LDD_OFFSET: "${NAKAMOTO_LDD_OFFSET:-2}"
    NAKAMOTO_LDD_BASELINE: "${NAKAMOTO_LDD_BASELINE:-0.1}"
    NAKAMOTO_LDD_AMPLITUDE: "${NAKAMOTO_LDD_AMPLITUDE:-1.0}"
    # Test-only slot duration. Production default is 1000ms (set in SnapshotLeaderLoop).
    NAKAMOTO_SLOT_DURATION_MS: "${NAKAMOTO_SLOT_DURATION_MS:-500}"
    NAKAMOTO_GENESIS_TIME_MS: "${NAKAMOTO_GENESIS_TIME_MS:-0}"
    CL_KEYALIAS: alias
    CL_PASSWORD: password
    JAVA_OPTS: "-Xmx1g -Xss256k --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED"

x-sidecar-common: &sidecar-common
  build:
    context: ../p2p
    dockerfile: Dockerfile

services:
HEADER

# Generate sidecar + node pairs
for (( i=0; i<NUM_NODES; i++ )); do
  # Build sidecar seedlist (all sidecars except self)
  SIDECAR_SEEDS=""
  for (( j=0; j<NUM_NODES; j++ )); do
    if (( j != i )); then
      if [[ -n "$SIDECAR_SEEDS" ]]; then
        SIDECAR_SEEDS="${SIDECAR_SEEDS},"
      fi
      SIDECAR_SEEDS="${SIDECAR_SEEDS}/dns4/nakamoto-sidecar-${j}/tcp/9500"
    fi
  done

  PUBLIC_PORT=$(( 9000 + i * 10 ))
  P2P_PORT=$(( 9001 + i * 10 ))
  CLI_PORT=$(( 9002 + i * 10 ))

  cat >> "$OUTPUT" <<EOF
  # ── Node $i ────────────────────────────────────────────
  sidecar-${i}:
    <<: *sidecar-common
    container_name: nakamoto-sidecar-${i}
    command: [
      "-grpc", "0.0.0.0:50051",
      "-listen", "/ip4/0.0.0.0/tcp/9500",
      "-seedlist", "${SIDECAR_SEEDS}"
    ]
    networks:
      nakamoto:
        aliases: [sidecar-${i}]

  node-${i}:
    <<: *jvm-common
    container_name: nakamoto-node-${i}
    environment:
      <<: *jvm-env
      CL_KEYSTORE: /opt/keys/key.p12
      SIDECAR_HOST: sidecar-${i}
      SIDECAR_GRPC_PORT: "50051"
      CL_PUBLIC_HTTP_PORT: "${PUBLIC_PORT}"
      CL_P2P_HTTP_PORT: "${P2P_PORT}"
      CL_CLI_HTTP_PORT: "${CLI_PORT}"
    volumes:
      - ./${GENESIS_FILE}:/opt/genesis.csv:ro
      - ./${SEEDLIST_FILE}:/opt/seedlist.csv:ro
      - ../docker/config/local-test-keys/${i}:/opt/keys:ro
    command: >
      run-nakamoto
      /opt/genesis.csv
      --ip node-${i}
      --public-port ${PUBLIC_PORT}
      --p2p-port ${P2P_PORT}
      --cli-port ${CLI_PORT}
      --seedlist /opt/seedlist.csv
      --collateral 0
EOF

  # Only expose ports on node-0
  if (( i == 0 )); then
    cat >> "$OUTPUT" <<EOF
    ports:
      - "${PUBLIC_PORT}:${PUBLIC_PORT}"
      - "${CLI_PORT}:${CLI_PORT}"
EOF
  fi

  cat >> "$OUTPUT" <<EOF
    depends_on:
      - sidecar-${i}
    networks:
      nakamoto:
        aliases: [node-${i}]

EOF
done

# Network definition
cat >> "$OUTPUT" <<'FOOTER'
networks:
  nakamoto:
    driver: bridge
FOOTER

echo "Generated $OUTPUT ($NUM_NODES nodes)"
echo ""
echo "Usage:"
echo "  GENESIS_TIME=\$(((\$(date +%s%3N) + 25000))) \\"
echo "  NAKAMOTO_GENESIS_TIME_MS=\$GENESIS_TIME \\"
echo "  docker compose -f $OUTPUT up --build -d"
