#!/usr/bin/env bash
#
# §1.1 stake-weighted VRF validation gate.
#
# Parses gl0 logs for `WON slot N (...)` lines per validator and asserts the
# per-validator block-production rate matches the expected stake fraction
# within ±20% (per docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md §1.1
# line 171). The log line comes from
# modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/SnapshotLeaderLoop.scala:890.
#
# Usage:
#   bash test-scripts/analyze-block-production-by-stake.sh '0.40,0.20,0.10,0.10,0.05,0.05,0.05,0.05'
#
# Reads ./nodes/0/gl0-logs/*.log .. ./nodes/N-1/gl0-logs/*.log where N = weight count.
# Exit 0 if all validators within ±20% of expected; exit 1 otherwise.

set -euo pipefail

if [ $# -ne 1 ]; then
  echo "Usage: $0 '<comma-separated-weights>'"
  echo "Example: $0 '0.40,0.20,0.10,0.10,0.05,0.05,0.05,0.05'"
  exit 2
fi

WEIGHTS="$1"
IFS=',' read -ra W_ARR <<< "$WEIGHTS"
N=${#W_ARR[@]}

if [ "$N" -lt 2 ]; then
  echo "ERROR: need at least 2 weights, got $N"
  exit 2
fi

# Sanity-check sum ≈ 1.0
SUM=$(echo "$WEIGHTS" | tr ',' '\n' | awk '{s+=$1} END{printf "%.4f", s}')
SUM_OK=$(awk -v s="$SUM" 'BEGIN { print (s > 0.99 && s < 1.01) ? "ok" : "no" }')
if [ "$SUM_OK" != "ok" ]; then
  echo "WARNING: weights sum=$SUM (not ≈ 1.0); proceeding anyway"
fi

declare -a WINS
TOTAL=0

for i in $(seq 0 $((N-1))); do
  LOG_DIR="./nodes/$i/gl0-logs"
  if [ ! -d "$LOG_DIR" ]; then
    echo "ERROR: $LOG_DIR not found — has the cluster been booted with --num-gl0=$N?"
    exit 1
  fi
  COUNT=$(grep -h "WON slot" "$LOG_DIR"/*.log 2>/dev/null | wc -l | tr -d ' ')
  WINS[$i]=${COUNT:-0}
  TOTAL=$((TOTAL + WINS[i]))
done

if [ "$TOTAL" -eq 0 ]; then
  echo "ERROR: zero WON-slot lines across $N validators in ./nodes/*/gl0-logs/"
  echo "       check that the cluster has been running long enough"
  exit 1
fi

echo "=================================================="
echo "§1.1 stake-weighted VRF block-production analysis"
echo "=================================================="
echo "Validators: $N    Total wins observed: $TOTAL"
echo "Tolerance:  ±20%  (per IMPLEMENTATION-PLAN §1.1)"
echo
printf "%-3s %-7s %-10s %-10s %-7s %-15s %-6s\n" \
       "i" "wins" "expected" "observed" "ratio" "tolerance" "pass"
printf "%-3s %-7s %-10s %-10s %-7s %-15s %-6s\n" \
       "--" "------" "--------" "--------" "------" "-----------" "----"

FAILED=0
for i in $(seq 0 $((N-1))); do
  WIN=${WINS[$i]}
  EXP_W=${W_ARR[$i]}
  OBS_F=$(awk -v w="$WIN" -v t="$TOTAL" 'BEGIN { printf "%.4f", w/t }')
  RATIO=$(awk -v o="$OBS_F" -v e="$EXP_W" 'BEGIN { if (e==0) printf "n/a"; else printf "%.3f", o/e }')
  LO=$(awk -v e="$EXP_W" 'BEGIN { printf "%.4f", e * 0.8 }')
  HI=$(awk -v e="$EXP_W" 'BEGIN { printf "%.4f", e * 1.2 }')
  RES=$(awk -v o="$OBS_F" -v lo="$LO" -v hi="$HI" 'BEGIN { print (o+0 >= lo+0 && o+0 <= hi+0) ? "PASS" : "FAIL" }')
  printf "%-3s %-7s %-10s %-10s %-7s [%s,%s] %-6s\n" \
         "$i" "$WIN" "$EXP_W" "$OBS_F" "$RATIO" "$LO" "$HI" "$RES"
  if [ "$RES" = "FAIL" ]; then
    FAILED=1
  fi
done

echo

# Binomial-CI hint at p=min and given N — gives a sanity-check window
P_MIN=$(echo "$WEIGHTS" | tr ',' '\n' | sort -n | head -1)
CI_HALFWIDTH=$(awk -v t="$TOTAL" -v p="$P_MIN" 'BEGIN { if (t==0 || p==0) printf "n/a"; else printf "%.3f", 1.96*sqrt(p*(1-p)/t) }')
echo "At $TOTAL total slots, 95% CI half-width for p=$P_MIN ≈ ±$CI_HALFWIDTH"
echo "(if this is larger than 0.2*p_min, consider running longer for tighter validation)"
echo

if [ $FAILED -eq 1 ]; then
  echo "❌ FAIL — one or more validators outside ±20% tolerance band"
  exit 1
else
  echo "✅ PASS — all validators within ±20% of expected stake fraction"
  exit 0
fi
