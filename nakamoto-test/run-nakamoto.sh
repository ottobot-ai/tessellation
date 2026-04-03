#!/usr/bin/env bash
# Quick test: run a single dag-l0 node with Nakamoto slot loop.
# The slot loop runs alongside the regular node — it doesn't replace BFT yet.
# We just want to see VRF eligibility evaluation and slot wins in logs.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
JAR="$REPO_DIR/modules/dag-l0/target/scala-2.13/$(ls "$REPO_DIR/modules/dag-l0/target/scala-2.13/" | grep assembly | head -1)"

if [ ! -f "$JAR" ]; then
  echo "ERROR: Assembly JAR not found. Run: sbt dagL0/assembly"
  exit 1
fi

echo "JAR: $JAR"
echo "Size: $(du -h "$JAR" | cut -f1)"

# Set genesis time to NOW so we start at slot 0
export NAKAMOTO_GENESIS_MS=$(date +%s%3N)

echo ""
echo "═══════════════════════════════════════════════"
echo "  Nakamoto PoC — Single Node Test"
echo "  Genesis: $NAKAMOTO_GENESIS_MS"
echo "═══════════════════════════════════════════════"
echo ""

# Run the Nakamoto entry point
# NakamotoMain extends TessellationIOApp just like Main, so it expects
# the same CLI args. For a quick local test, we use run-genesis.
#
# However, NakamotoMain doesn't currently use the standard Services/Storages
# since it's a PoC stub. It just starts the slot loop.
# For the real test, we need to verify the loop ticks and evaluates VRF.

java -cp "$JAR" io.constellationnetwork.dag.l0.NakamotoMain \
  run-genesis \
  --ip 127.0.0.1 \
  --public-port 9000 \
  --p2p-port 9001 \
  --cli-port 9002 \
  --collateral 0 \
  2>&1 | head -100
