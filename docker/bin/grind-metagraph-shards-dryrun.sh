#!/usr/bin/env bash
#
# Shard-sortition Slice S7 — metagraph key-grinding DRY RUN.
#
# Validates the grinding routine + shardId oracle used by compose-runner.sh
# WITHOUT standing up a cluster. For each of K metagraphs it consumes freshly
# minted candidate addresses until one whose ShardAssignment.shardId == the
# target shard (k mod M) is found, then prints the per-metagraph result and the
# aggregate distribution. The selection loop mirrors compose-runner.sh's live
# grind exactly (draw a candidate → compute its shard via the production oracle →
# accept on match, else draw again); only the candidate SOURCE differs (see the
# caveat below).
#
# Speed: candidate generation + shardId computation are batched into a SINGLE JVM
# via `tools.jar shard-scan` (mints N keypairs, prints "address shardId" per
# line). The bash loop then selects/consumes from that pool — so the whole dry
# run is ~1-2 JVM launches instead of one-per-candidate.
#
# CAVEAT — operator address vs genesis address:
#   The LIVE grind (compose-runner.sh) checks shardIdFor(GENESIS address) =
#   Address.fromBytes(serialize(signed genesis snapshot)), which depends on the
#   m${k}-0 SIGNING key AND gl0's embedded GlobalSyncView, so it must run live
#   (gl0 up), checking the real genesis.address each attempt. This dry run cannot
#   produce a genesis snapshot offline, so it grinds OPERATOR addresses
#   (KeyPair.getPublic.toAddress) as a faithful stand-in for the routine's
#   control flow + oracle. Both paths mint a keypair, route the address through
#   the identical production ShardAssignment, and loop on a miss — only the
#   address derivation differs. The distribution math is identical (each shard is
#   reachable; expected ≈ M draws per metagraph).
#
# Usage:
#   docker/bin/grind-metagraph-shards-dryrun.sh [NUM_METAGRAPHS] [NUM_SHARDS] [POOL_SIZE]
# Defaults: 8 metagraphs, 4 shards, pool of 64 candidates (auto-extends if short).
#
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../../"
PROJECT_ROOT=$(pwd)

NUM_METAGRAPHS=${1:-8}
NUM_SHARDS=${2:-4}
POOL_SIZE=${3:-64}

TOOLS_JAR="$PROJECT_ROOT/docker/jars/tools.jar"
if [ ! -f "$TOOLS_JAR" ]; then
  echo "ERROR: $TOOLS_JAR missing (run 'just build' or 'sbt tools/assembly')"
  exit 1
fi

# Batch oracle: mint `n` keypairs and emit "address shardId" per line, all in ONE
# JVM. Identical ShardAssignment code path as the live grind's per-candidate
# `tools.jar shard-id` check.
scan_pool() {
  local n="$1"
  java -jar "$TOOLS_JAR" shard-scan --count "$n" --num-shards "$NUM_SHARDS" 2>/dev/null
}

echo "============================================================"
echo "Slice S7 metagraph-shard grind DRY RUN"
echo "  metagraphs = $NUM_METAGRAPHS   shards = $NUM_SHARDS   pool = $POOL_SIZE"
echo "  oracle     = tools.jar shard-scan (production ShardAssignment)"
echo "============================================================"

# Build the candidate pool once (auto-extend if a target is starved).
mapfile -t POOL < <(scan_pool "$POOL_SIZE")
pool_idx=0
extra_scans=0

# draw_next → sets DRAW_LINE to the next "address shardId" entry, scanning more
# on exhaustion. Sets a global (NOT echo + $()) so the pool index advances in the
# current shell rather than a command-substitution subshell.
DRAW_LINE=""
draw_next() {
  if [ "$pool_idx" -ge "${#POOL[@]}" ]; then
    extra_scans=$((extra_scans + 1))
    mapfile -t -O "${#POOL[@]}" POOL < <(scan_pool "$POOL_SIZE")
  fi
  DRAW_LINE="${POOL[$pool_idx]}"
  pool_idx=$((pool_idx + 1))
}

declare -a RESULT_SHARD=()
declare -a RESULT_ADDR=()
declare -a RESULT_TRIES=()
total_draws=0

for k in $(seq 0 $((NUM_METAGRAPHS - 1))); do
  target=$(( k % NUM_SHARDS ))
  attempt=0
  landed=""
  addr=""
  while :; do
    attempt=$((attempt + 1))
    total_draws=$((total_draws + 1))
    draw_next
    addr=${DRAW_LINE%% *}
    shard=${DRAW_LINE##* }
    if [ "$shard" = "$target" ]; then
      landed=$shard
      break
    fi
    if [ "$attempt" -ge 1000 ]; then
      echo "ERROR: metagraph $k failed to hit shard $target in 1000 draws"
      exit 1
    fi
  done
  RESULT_SHARD[$k]=$landed
  RESULT_ADDR[$k]=$addr
  RESULT_TRIES[$k]=$attempt
  printf "  metagraph %d → target shard %d : landed shard %s  (%d draw(s))  addr=%s\n" \
    "$k" "$target" "$landed" "$attempt" "$addr"
done

echo "------------------------------------------------------------"
echo "Per-shard occupancy:"
declare -a SHARD_COUNT=()
for s in $(seq 0 $((NUM_SHARDS - 1))); do SHARD_COUNT[$s]=0; done
for k in $(seq 0 $((NUM_METAGRAPHS - 1))); do
  s=${RESULT_SHARD[$k]}
  SHARD_COUNT[$s]=$(( ${SHARD_COUNT[$s]} + 1 ))
done

# k mod M distributes as evenly as possible: first (K mod M) shards get one extra.
expected_per_shard=$(( NUM_METAGRAPHS / NUM_SHARDS ))
remainder=$(( NUM_METAGRAPHS % NUM_SHARDS ))
all_ok=true
for s in $(seq 0 $((NUM_SHARDS - 1))); do
  want=$expected_per_shard
  [ "$s" -lt "$remainder" ] && want=$(( want + 1 ))
  got=${SHARD_COUNT[$s]}
  status="OK"
  if [ "$got" -ne "$want" ]; then status="MISMATCH (want $want)"; all_ok=false; fi
  printf "  shard %d : %d metagraph(s)   [%s]\n" "$s" "$got" "$status"
done

echo "------------------------------------------------------------"
avg=$(awk -v t="$total_draws" -v k="$NUM_METAGRAPHS" 'BEGIN{printf "%.1f", t/k}')
echo "Total candidate draws: $total_draws (avg ${avg}/metagraph; ideal ≈ $NUM_SHARDS). Extra pool scans: $extra_scans"
if [ "$all_ok" = "true" ]; then
  echo "RESULT: PASS — $NUM_METAGRAPHS metagraphs spread evenly across $NUM_SHARDS shards"
  exit 0
else
  echo "RESULT: FAIL — distribution did not match the k mod M target"
  exit 1
fi
