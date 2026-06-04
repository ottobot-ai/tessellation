

export BASH_DEBUG_MODE=${BASH_DEBUG_MODE:-false}
export DATA_ONLY_METAGRAPH=${DATA_ONLY_METAGRAPH:-false}

# Hypergraph release JAR support
# When set, downloads pre-built JARs from GitHub releases instead of building from source
export HYPERGRAPH_RELEASE=${HYPERGRAPH_RELEASE:-""}

# Release tag (set via --version flag or RELEASE_TAG env var)
# Note: only export if non-empty, as sbt's sys.env.get treats "" as Some("")
# which bypasses dynver version resolution
export RELEASE_TAG=${RELEASE_TAG:-""}

export EXTRA_ENV_PATH=${EXTRA_ENV_PATH:-""}
export EXIT_CODE=${EXIT_CODE:-0}
export SNAPSHOT_STREAMING_JAR=${SNAPSHOT_STREAMING_JAR:-""}
export SNAPSHOT_STREAMING_BRANCH=${SNAPSHOT_STREAMING_BRANCH:-"develop"}
export BLOCK_EXPLORER_BRANCH=${BLOCK_EXPLORER_BRANCH:-"develop"}
export CL_DOCKER_BIND_INTERFACE=${CL_DOCKER_BIND_INTERFACE:-""}
export CLEAN_ASSEMBLY=${CLEAN_ASSEMBLY:-false}
export DO_EXIT=${DO_EXIT:-false}
export INCLUDE_L0=${INCLUDE_L0:-true}
export INCLUDE_L1=${INCLUDE_L1:-true}
export INCLUDE_ALL=${INCLUDE_ALL:-false}
export PURGE_CONFIG=${PURGE_CONFIG:-true}
export ROLLBACK_MODE=${ROLLBACK_MODE:-false}
export ROLLBACK_HASH=${ROLLBACK_HASH:-""}
export SKIP_ASSEMBLY=${SKIP_ASSEMBLY:-false}
export NET_PREFIX=${NET_PREFIX:-"172.32.0"}
export TESSELLATION_DOCKER_VERSION=${TESSELLATION_DOCKER_VERSION:-"test"}
export CLEANUP_DOCKER_AT_END=${CLEANUP_DOCKER_AT_END:-false}
export REGENERATE_TEST_KEYS=${REGENERATE_TEST_KEYS:-false}
export BUILD_ONLY=${BUILD_ONLY:-false}

# --- Runner safety / control (auto-teardown + global hard timeout) ---
# KEEP_ALIVE: when true, the cluster is LEFT UP after the e2e suite finishes
#   (debugging). Default false ⇒ compose-runner.sh auto-tears-down the cluster
#   via `just down` (clean-docker; PRESERVES nodes/ logs) once the suite ends,
#   pass OR fail. Set by --keep-alive.
export KEEP_ALIVE=${KEEP_ALIVE:-false}
# TEARDOWN_GRACE_SECONDS: seconds to wait after the suite concludes before the
#   auto-teardown fires (gives an operator a moment to attach / inspect a fresh
#   failure before containers vanish; logs in nodes/ survive regardless).
export TEARDOWN_GRACE_SECONDS=${TEARDOWN_GRACE_SECONDS:-45}
# RUN_TIMEOUT_SECONDS: global hard ceiling on the WHOLE run. A background
#   watchdog force-kills the runner (and triggers teardown) if exceeded, so a
#   stuck/looping test can never run all night and leak containers. Default 3h.
export RUN_TIMEOUT_SECONDS=${RUN_TIMEOUT_SECONDS:-10800}


export DAG_L0_PORT_PREFIX=${DAG_L0_PORT_PREFIX:-90}
export DAG_L1_PORT_PREFIX=${DAG_L1_PORT_PREFIX:-91}
export ML0_PORT_PREFIX=${ML0_PORT_PREFIX:-92}
export CL1_PORT_PREFIX=${CL1_PORT_PREFIX:-93}
export DL1_PORT_PREFIX=${DL1_PORT_PREFIX:-94}

# Configurable base host for test URLs (used with port prefixes)
export TEST_HOST=${TEST_HOST:-"http://localhost"}

# Metagraph specific settings
export METAGRAPH_ML0=${METAGRAPH_ML0:-true}
export METAGRAPH_CL1=${METAGRAPH_CL1:-true}
export METAGRAPH_DL1=${METAGRAPH_DL1:-true}
export METAGRAPH_ML0_RELATIVE_PATH=${METAGRAPH_ML0_RELATIVE_PATH:-"l0"}
export METAGRAPH_CL1_RELATIVE_PATH=${METAGRAPH_CL1_RELATIVE_PATH:-"l1"}
export METAGRAPH_DL1_RELATIVE_PATH=${METAGRAPH_DL1_RELATIVE_PATH:-"data_l1"}
export USE_TESSELLATION_VERSION=${USE_TESSELLATION_VERSION:-true}

# Common docker profile addons:
export DOCKER_PROFILES=${DOCKER_PROFILES:-""}

# Test specific settings
export USE_TEST_METAGRAPH=${USE_TEST_METAGRAPH:-false}
export SELECTED_TESTS=${SELECTED_TESTS:-""}
export LIST_TESTS=${LIST_TESTS:-false}

# Number of independent metagraphs to spin up against a single hypergraph.
# K=1 (default) preserves the legacy single-metagraph behaviour. K>=2 spawns
# additional metagraph clusters with their own keystore + genesis, sharing
# the same GL0/GL1 hypergraph. Required for sharding-related e2e — see
# .workspace/sharding-design-notes.md §3 (Deliverable A — multi-metagraph e2e).
export NUM_METAGRAPHS=${NUM_METAGRAPHS:-1}


# Store any explicitly-set TESSELLATION_VERSION from environment
# This will be used for precedence after args are parsed
if [ -n "${TESSELLATION_VERSION:-}" ]; then
    export EXPLICIT_TESSELLATION_VERSION="$TESSELLATION_VERSION"
fi


echo "processing args: $@"

# Process command-line arguments
for arg in "$@"; do
  case "$arg" in
    --data)
      export DATA_ONLY_METAGRAPH=true
      ;;
    --env=*)
      export EXTRA_ENV_PATH="${arg#*=}"
      ;;
    --exit-code)
      export EXIT_CODE=1
      ;;
    --bind-interface)
      export CL_DOCKER_BIND_INTERFACE=""
      ;;
    --clean-assembly)
      export CLEAN_ASSEMBLY=true
      ;;
    --do-exit)
      export DO_EXIT=true
      ;;
    --l1)
      export INCLUDE_L1=true
      ;;
    --include-all)
      export INCLUDE_ALL=true
      ;;
    --purge-config)
      export PURGE_CONFIG=true
      ;;
    --rollback)
      export ROLLBACK_MODE=true
      export PURGE_CONFIG=false
      ;;
    --rollback-hash=*)
      export ROLLBACK_HASH="${arg#*=}"
      ;;
    --skip-assembly)
      export SKIP_ASSEMBLY=true
      ;;
    --net-prefix=*)
      export NET_PREFIX="${arg#*=}"
      ;;
    --dag-l0-port-prefix=*)
      export DAG_L0_PORT_PREFIX="${arg#*=}"
      ;;
    --gl1-port-prefix=*)
      export DAG_L1_PORT_PREFIX="${arg#*=}"
      ;;
    --cleanup)
      export CLEANUP_DOCKER_AT_END=true
      ;;
    --tessellation-docker-version=*)
      export TESSELLATION_DOCKER_VERSION="${arg#*=}"
      ;;
    --regenerate-test-keys)
      export REGENERATE_TEST_KEYS=true
      ;;
    --build)
      export BUILD_ONLY=true
      ;;
    --publish)
      export PUBLISH=true
      ;;
    --version=*)
      export RELEASE_TAG="${arg#*=}"
      ;;
    --metagraph=*)
      export METAGRAPH="${arg#*=}"
      ;;
    --hypergraph-release=*)
      export HYPERGRAPH_RELEASE="${arg#*=}"
      ;;
    --snapshot-streaming-jar=*)
      export SNAPSHOT_STREAMING_JAR="${arg#*=}"
      ;;
    --snapshot-streaming-branch=*)
      export SNAPSHOT_STREAMING_BRANCH="${arg#*=}"
      ;;
    --block-explorer-branch=*)
      export BLOCK_EXPLORER_BRANCH="${arg#*=}"
      ;;
    --ml0-path=*)
      export METAGRAPH_ML0_RELATIVE_PATH="${arg#*=}"
      ;;
    --cl1-path=*)
      export METAGRAPH_CL1_RELATIVE_PATH="${arg#*=}"
      ;;
    --dl1-path=*)
      export METAGRAPH_DL1_RELATIVE_PATH="${arg#*=}"
      ;;
    --ml0)
      export METAGRAPH_ML0=true
      ;;
    --cl1)
      export METAGRAPH_CL1=true
      ;;
    --dl1)
      export METAGRAPH_DL1=true
      ;;  
    --num-gl0=*)
      export NUM_GL0_NODES="${arg#*=}"
      export NUM_GL0_NODES_EXPLICIT="${arg#*=}"
      ;;
    --num-gl1=*)
      export NUM_GL1_NODES="${arg#*=}"
      export NUM_GL1_NODES_EXPLICIT="${arg#*=}"
      ;;
    --num-ml0=*)
      export NUM_ML0_NODES="${arg#*=}"
      export NUM_ML0_NODES_EXPLICIT="${arg#*=}"
      ;;
    --num-cl1=*)
      export NUM_CL1_NODES="${arg#*=}"
      export NUM_CL1_NODES_EXPLICIT="${arg#*=}"
      ;;
    --num-dl1=*)
      export NUM_DL1_NODES="${arg#*=}"
      export NUM_DL1_NODES_EXPLICIT="${arg#*=}"
      ;;
    --skip-metagraph-assembly)
      export SKIP_METAGRAPH_ASSEMBLY=true
      ;;
    --use-test-metagraph)
      export USE_TEST_METAGRAPH=true
      ;;
    --metagraphs=*)
      export NUM_METAGRAPHS="${arg#*=}"
      if ! [[ "$NUM_METAGRAPHS" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: --metagraphs must be a positive integer (got: $NUM_METAGRAPHS)"
        exit 1
      fi
      ;;
    --skip-streaming)
      export SKIP_STREAMING=true
      ;;
    --fail)
      export SET_FAILURE_BREAKPOINT_TIME=true
      ;;
    --up)
      export DOCKER_UP=true
      ;;
    --grafana)
      export ENABLE_GRAFANA=true
      ;;
    --keep-alive)
      # Skip the default post-suite auto-teardown — leave the cluster UP for
      # debugging. (Logs are preserved either way; this keeps the containers.)
      export KEEP_ALIVE=true
      ;;
    --teardown-grace=*)
      export TEARDOWN_GRACE_SECONDS="${arg#*=}"
      if ! [[ "$TEARDOWN_GRACE_SECONDS" =~ ^[0-9]+$ ]]; then
        echo "Error: --teardown-grace must be a non-negative integer (got: $TEARDOWN_GRACE_SECONDS)"
        exit 1
      fi
      ;;
    --timeout=*)
      # Global hard ceiling (seconds) on the whole run; watchdog kills + tears
      # down if exceeded. Guards against an all-night stuck-test container leak.
      export RUN_TIMEOUT_SECONDS="${arg#*=}"
      if ! [[ "$RUN_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: --timeout must be a positive integer number of seconds (got: $RUN_TIMEOUT_SECONDS)"
        exit 1
      fi
      ;;
    --gl0-url=*)
      export GL0_URL="${arg#*=}"
      ;;
    --gl1-url=*)
      export GL1_URL="${arg#*=}"
      ;;
    --ml0-url=*)
      export ML0_URL="${arg#*=}"
      ;;
    --cl1-url=*)
      export CL1_URL="${arg#*=}"
      ;;
    --dl1-url=*)
      export DL1_URL="${arg#*=}"
      ;;
    --host=*)
      export TEST_HOST="${arg#*=}"
      ;;
    --test=*)
      test_val="${arg#*=}"
      if [ -n "$SELECTED_TESTS" ]; then
        export SELECTED_TESTS="$SELECTED_TESTS,$test_val"
      else
        export SELECTED_TESTS="$test_val"
      fi
      ;;
    --list-tests)
      export LIST_TESTS=true
      ;;
    --stake-dist=*)
      # §1.1 stake-weighted VRF e2e: pre-genesis stake distribution.
      # Accepts "uniform" | "rand" | "<comma-separated weights>". Resolved into
      # NAKAMOTO_STAKE_DISTRIBUTION after all args parse (needs NUM_GL0_NODES).
      export STAKE_DIST_SPEC="${arg#*=}"
      ;;
    --shards=*)
      # Committee-sortition target size K. Exports NAKAMOTO_COMMITTEE_K_TARGET,
      # which docker-compose.nakamoto-overlay.yaml forwards to gl0 containers.
      # Validated after NUM_GL0_NODES is known (must be 1 ≤ K ≤ N).
      export NAKAMOTO_COMMITTEE_K_TARGET="${arg#*=}"
      if ! [[ "$NAKAMOTO_COMMITTEE_K_TARGET" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: --shards must be a positive integer (got: $NAKAMOTO_COMMITTEE_K_TARGET)"
        exit 1
      fi
      ;;
    --num-shards=*)
      # Hierarchical shard count M (HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md).
      # Exports NAKAMOTO_NUM_SHARDS, which docker-compose.nakamoto-overlay.yaml
      # forwards to gl0 containers and application.conf reads via ${?...}.
      # num-shards = 1 (default/unset) keeps the shard path inert (byte-identical
      # to pre-sharding). num-shards > 1 activates per-shard ShardCheckpoint
      # production + gl0 aggregation. Distinct from --shards (committee K target).
      export NAKAMOTO_NUM_SHARDS="${arg#*=}"
      if ! [[ "$NAKAMOTO_NUM_SHARDS" =~ ^[1-9][0-9]*$ ]]; then
        echo "Error: --num-shards must be a positive integer (got: $NAKAMOTO_NUM_SHARDS)"
        exit 1
      fi
      ;;
    *)
      echo "Unknown argument: $arg"
      exit 1
      ;;
  esac
done

# §1.1 stake-weighted VRF: resolve --stake-dist=<spec> into NAKAMOTO_STAKE_DISTRIBUTION
# now that NUM_GL0_NODES is known. compose-runner.sh forwards the env var to the Tier-1
# genesis generator path (test-vectors/genesis fixtures); when unset, default CSV path is used.
#
# Default to --stake-dist=harmonic when the flag is omitted AND we have ≥ 2 gl0 nodes.
# Harmonic gives a deterministic Zipf-style skewed distribution (w_i = 1/(i+1), normalized)
# so every `just test` exercises the Tier-1 / stake-weighted-VRF / KES path AND keeps the
# top operator's stake share ≥ ~0.37 for any N. The skew is load-bearing: under more-
# uniform stake the gl0 acceptance validators (TokenLockBlockAcceptanceLogic +
# GlobalSnapshotStateChannelAcceptanceManager) race against MultiBranch overlay branch
# switches and reject chain-linked TXs with ParentHashNotEqLastTxHash / Chain-link
# rejection — see task #186 (same family as #117 / #118). Concentrating leadership on
# a single heavy operator keeps the branch view stable enough for the existing
# validators to converge. Harmonic matches the shape used in iter37/iter36 validation
# (0.4/0.2/0.1/0.1/0.05x4 for N=8 → harmonic gives 0.37/0.18/0.12/0.09/0.07/0.06/0.05/0.05).
#
# Uniform and rand stakes both expose #186 and are kept reachable via --stake-dist=
# uniform / --stake-dist=rand for explicit stress-testing. CSV-genesis fallback via
# --stake-dist=csv.
if [ -z "${STAKE_DIST_SPEC:-}" ] && [ -n "${NUM_GL0_NODES:-}" ] && [ "$NUM_GL0_NODES" -ge 2 ]; then
  export STAKE_DIST_SPEC="harmonic"
fi

if [ "${STAKE_DIST_SPEC:-}" = "csv" ]; then
  # Explicit opt-out: caller wants the legacy CSV-genesis path (no KES registry, no
  # stake-weighted VRF). Surfaces as `STAKE_DIST_SPEC` set but `NAKAMOTO_STAKE_DISTRIBUTION`
  # unset, which compose-runner.sh treats as "skip Tier-1 generator".
  unset STAKE_DIST_SPEC
fi

if [ -n "${STAKE_DIST_SPEC:-}" ]; then
  case "$STAKE_DIST_SPEC" in
    uniform)
      if [ -z "${NUM_GL0_NODES:-}" ] || [ "$NUM_GL0_NODES" -lt 2 ]; then
        echo "ERROR: --stake-dist=uniform requires --num-gl0=N with N >= 2 (got: '${NUM_GL0_NODES:-}')"
        exit 1
      fi
      export NAKAMOTO_STAKE_DISTRIBUTION=$(awk -v n="$NUM_GL0_NODES" 'BEGIN {
        for (i = 0; i < n; i++) { if (i > 0) printf ","; printf "%.6f", 1.0/n }
      }')
      ;;
    rand)
      if [ -z "${NUM_GL0_NODES:-}" ] || [ "$NUM_GL0_NODES" -lt 2 ]; then
        echo "ERROR: --stake-dist=rand requires --num-gl0=N with N >= 2 (got: '${NUM_GL0_NODES:-}')"
        exit 1
      fi
      seed=${NAKAMOTO_GENESIS_SEED:-$(date +%s)}
      export NAKAMOTO_STAKE_DISTRIBUTION=$(awk -v n="$NUM_GL0_NODES" -v seed="$seed" 'BEGIN {
        srand(seed)
        total = 0
        for (i = 0; i < n; i++) { w[i] = rand() + 0.01; total += w[i] }
        for (i = 0; i < n; i++) { if (i > 0) printf ","; printf "%.6f", w[i] / total }
      }')
      ;;
    harmonic)
      # Zipf-style skewed distribution: w_i = 1/(i+1), normalized. Deterministic (no
      # seed), scales naturally to any N≥2, top-operator share decreases gradually
      # from 0.67 (N=2) → 0.55 (N=3) → 0.44 (N=5) → 0.37 (N=8) → 0.32 (N=10).
      # Top share stays well above 1/N for all N, which keeps the gl0 acceptance
      # validators' branch view stable under leader churn (see #186).
      if [ -z "${NUM_GL0_NODES:-}" ] || [ "$NUM_GL0_NODES" -lt 2 ]; then
        echo "ERROR: --stake-dist=harmonic requires --num-gl0=N with N >= 2 (got: '${NUM_GL0_NODES:-}')"
        exit 1
      fi
      export NAKAMOTO_STAKE_DISTRIBUTION=$(awk -v n="$NUM_GL0_NODES" 'BEGIN {
        total = 0
        for (i = 0; i < n; i++) { w[i] = 1.0 / (i + 1); total += w[i] }
        for (i = 0; i < n; i++) { if (i > 0) printf ","; printf "%.6f", w[i] / total }
      }')
      ;;
    *)
      # Pass-through CSV; compose-runner.sh validates length + sum.
      export NAKAMOTO_STAKE_DISTRIBUTION="$STAKE_DIST_SPEC"
      ;;
  esac
  echo "[set-env] --stake-dist=$STAKE_DIST_SPEC → NAKAMOTO_STAKE_DISTRIBUTION=$NAKAMOTO_STAKE_DISTRIBUTION"
fi

exit_func() {
  if [ "$DO_EXIT" = "true" ]; then
    exit $EXIT_CODE
  fi
  return 0
}

echo "BUILD_ONLY: $BUILD_ONLY"
echo "RELEASE_TAG: $RELEASE_TAG"

# Set TESSELLATION_VERSION with explicit precedence (after args are parsed):
# 1. TESSELLATION_VERSION env var (explicit override) - highest priority
# 2. --hypergraph-release flag
# 3. --version / RELEASE_TAG
# 4. Git describe (auto-derive, matches sbt dynver format)
# 5. 99.99.99-SNAPSHOT (default, build from source)
if [ -n "${EXPLICIT_TESSELLATION_VERSION:-}" ]; then
    export TESSELLATION_VERSION="$EXPLICIT_TESSELLATION_VERSION"
    echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (from environment)"
elif [ -n "$HYPERGRAPH_RELEASE" ]; then
    export TESSELLATION_VERSION="${HYPERGRAPH_RELEASE#v}"
    echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (from --hypergraph-release)"
elif [ -n "$RELEASE_TAG" ]; then
    export TESSELLATION_VERSION="${RELEASE_TAG#v}"
    echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (from --version/RELEASE_TAG)"
elif command -v git &> /dev/null && git rev-parse --git-dir &> /dev/null; then
    # Get version from git describe (fast, matches dynver format)
    # Use --match 'v*' to only consider version tags (consistent with dynver)
    # Output: v4.1.0 (on tag) or v4.1.0-3-gabc1234 (3 commits after tag)
    GIT_DESC=$(git describe --tags --match 'v*' --abbrev=7 2>/dev/null || echo "")
    if [ -n "$GIT_DESC" ]; then
        # Strip leading 'v' and convert to dynver-like format
        # v4.1.0-3-gabc1234 -> 4.1.0+3.abc1234.local (or .buildN in CI)
        BASE_VERSION=$(echo "$GIT_DESC" | sed 's/^v//; s/-\([0-9]*\)-g\([a-f0-9]*\)$/+\1.\2/')
        # Append buildId suffix to match sbt dynver format
        if [ -n "${GITHUB_RUN_NUMBER:-}" ]; then
            BUILD_ID="build${GITHUB_RUN_NUMBER}"
        else
            BUILD_ID="local"
        fi
        # Only append buildId if not on exact tag (version contains +)
        if [[ "$BASE_VERSION" == *"+"* ]]; then
            export TESSELLATION_VERSION="${BASE_VERSION}.${BUILD_ID}"
        else
            export TESSELLATION_VERSION="$BASE_VERSION"
        fi
        echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (from git describe)"
    else
        export TESSELLATION_VERSION="99.99.99-SNAPSHOT"
        echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (fallback - no git tags)"
    fi
else
    export TESSELLATION_VERSION="99.99.99-SNAPSHOT"
    echo "Setting TESSELLATION_VERSION=$TESSELLATION_VERSION (default snapshot)"
fi

if [ "$DATA_ONLY_METAGRAPH" = "true" ]; then
    export NUM_GL0_NODES=1
    export NUM_GL1_NODES=0
    export NUM_ML0_NODES=1
    export NUM_CL1_NODES=0
    export NUM_DL1_NODES=3

fi


# If a specific metagraph is provided, set sensible defaults
if [ -n "$METAGRAPH" ]; then
  if [ -z "$NUM_GL0_NODES" ]; then
    export NUM_GL0_NODES=1
  fi
  if [ -z "$NUM_GL1_NODES" ]; then
    export NUM_GL1_NODES=3
  fi
  if [ -z "$NUM_ML0_NODES" ]; then
    export NUM_ML0_NODES=1
  fi
  if [ -z "$NUM_CL1_NODES" ]; then
    export NUM_CL1_NODES=3
  fi
  if [ -z "$NUM_DL1_NODES" ]; then
    export NUM_DL1_NODES=3
  fi
fi

# Set more complex defaults below

if [ "$USE_TEST_METAGRAPH" = "true" ] && [ -z "$METAGRAPH" ]; then
    export METAGRAPH=".github/templates/metagraphs/project_template"
fi

# Auto-skip metagraph assembly only when the main --skip-assembly is set.
# Without this guard, the test metagraph JAR goes stale and doesn't pick up
# changes to tessellation SDK code (e.g., Download.scala fixes) on re-runs.
if [ "$METAGRAPH" = ".github/templates/metagraphs/project_template" ] && [ -z "$SKIP_METAGRAPH_ASSEMBLY" ] && [ "$SKIP_ASSEMBLY" = "true" ]; then
    export SKIP_METAGRAPH_ASSEMBLY=true
fi


# Defaults which must be declared after, such that the complex ones won't override them

if [ -z "$SKIP_METAGRAPH_ASSEMBLY" ]; then
    export SKIP_METAGRAPH_ASSEMBLY=false
fi


if [ -z $NUM_GL0_NODES ]; then
    if [ -z "$METAGRAPH" ]; then
        export NUM_GL0_NODES=3
    else
        export NUM_GL0_NODES=2
    fi
fi

if [ -z $NUM_GL1_NODES ]; then
    export NUM_GL1_NODES=3
fi

if [ -z $NUM_ML0_NODES ]; then
    export NUM_ML0_NODES=2
fi

if [ -z $NUM_CL1_NODES ]; then
    export NUM_CL1_NODES=3
fi

if [ -z $NUM_DL1_NODES ]; then
    export NUM_DL1_NODES=3
fi


if [ -z "$METAGRAPH" ]; then
    export NUM_ML0_NODES="0"
    export NUM_CL1_NODES="0"
    export NUM_DL1_NODES="0"
fi

# --shards=K validation now that NUM_GL0_NODES is known. K=N is the degenerate
# committee (sortition no-op). K<N gives genuine per-metagraph sortition; the
# gate requires ceil(2K/3) attestations from a K-sized committee.
if [ -n "${NAKAMOTO_COMMITTEE_K_TARGET:-}" ]; then
    if [ "$NAKAMOTO_COMMITTEE_K_TARGET" -gt "$NUM_GL0_NODES" ]; then
        echo "Error: --shards=$NAKAMOTO_COMMITTEE_K_TARGET exceeds --num-gl0=$NUM_GL0_NODES"
        exit 1
    fi
    echo "[set-env] --shards=$NAKAMOTO_COMMITTEE_K_TARGET → NAKAMOTO_COMMITTEE_K_TARGET=$NAKAMOTO_COMMITTEE_K_TARGET (gate is real sortition since K<N=$NUM_GL0_NODES is $([ "$NAKAMOTO_COMMITTEE_K_TARGET" -lt "$NUM_GL0_NODES" ] && echo true || echo "false — gate is degenerate K=N"))"
fi

# Remote host: default to 1 gl0 node, 1 gl1 node, 0 metagraph nodes for health check
# unless explicitly overridden via --num-* args
if [ "$TEST_HOST" != "http://localhost" ]; then
    export NUM_GL0_NODES=${NUM_GL0_NODES_EXPLICIT:-1}
    export NUM_GL1_NODES=${NUM_GL1_NODES_EXPLICIT:-1}
    export NUM_ML0_NODES=${NUM_ML0_NODES_EXPLICIT:-0}
    export NUM_CL1_NODES=${NUM_CL1_NODES_EXPLICIT:-0}
    export NUM_DL1_NODES=${NUM_DL1_NODES_EXPLICIT:-0}
fi

if [ -n "$METAGRAPH" ]; then
    if [ -z "$PUBLISH" ]; then
        export PUBLISH=true
    fi
fi

# Compute MAX_NODES as the maximum of all NUM_*_NODES values.
# Hypergraph operators live in nodes/$i/ and run gl0+gl1; their count comes
# from NUM_GL0_NODES / NUM_GL1_NODES.
#
# Metagraph operators (per metagraph k in [0, NUM_METAGRAPHS)) live in
# nodes/m${k}-${i}/ and run ml0+cl1+dl1; their count is MAX_METAGRAPH_NODES,
# the max of the per-layer NUM_ML0/CL1/DL1_NODES (each metagraph has its own
# independent operator set with its own keystore).
#
# MAX_NODES is the unified ceiling — drives directory creation for both
# hypergraph and metagraph operators (the latter via K parallel m${k}-* dirs).
#
# Node-count ceilings (HG vs metagraph differ, because their IP/port allocation
# schemes differ):
#   * Hypergraph (gl0+gl1) uses ARITHMETIC IP/port allocation (docker-env-setup.sh
#     + docker-compose.test.yaml) so it scales past a single digit. The ceiling is
#     NAKAMOTO_MAX_HG_NODES (default 20). 20 keeps gl0 at IP octets .10..29 and gl1
#     at .30..49 within the shared /24, disjoint from each other and from
#     snapshot-streaming (.60/.61); gl0 host ports stay 9000..9192 and gl1 host
#     ports 9600..9792 — all well under 65535. Override via the env var if you grow
#     the layout, but re-verify the IP/port arithmetic in docker-env-setup.sh first.
#   * Metagraph (ml0/cl1/dl1) still uses single-digit STRING-CONCAT IP/port
#     allocation (.3${i}/.4${i}/.5${i}, 92${i}0/93${i}0/94${i}0) on its own per-k
#     /24, so it remains capped at 9. Metagraphs only ever run ~3 nodes in e2e, so
#     this is not a practical limit; lifting it would require the same arithmetic
#     refactor applied to the metagraph compose files.
# --- Hypergraph IP / host-port bases (single source of truth) ---
# Consumed by docker-env-setup.sh (per-node .env) AND the JVM/sidecar seedlist
# builders in compose-runner.sh. They MUST agree byte-for-byte or gl0 consensus
# peering breaks (the seedlist tells each gl0 node where to dial its peers, and
# the dial target is the container's bound IP + internal P2P port). All values
# are ARITHMETIC (base + i*stride) so the hypergraph scales past a single digit;
# for i<10 every value below is byte-identical to the legacy string-concat scheme
# (gl0 IP .10..19, gl0 ports 9000..9092), so small-N runs are unchanged.
#
# IP layout (shared /24 ${NET_PREFIX}.x — gl0/gl1 + snapshot-streaming .60/.61):
#   gl0  .(GL0_IP_BASE + i)   = .10..  (legacy .1${i})
#   gl1  .(GL1_IP_BASE + i)   = .30..  (legacy was .2${i}; moved to .30 so gl0's
#                                       arithmetic band can't overlap gl1)
# Host-port layout:
#   gl0  external==internal = GL0_PORT_BASE + i*10  {+0 public,+1 p2p,+2 cli}
#   gl1  internal           = GL1_INT_PORT_BASE + i*10   (legacy 9100 band — keeps
#                                                         the `gl1-0:9100` DNS alias
#                                                         and BFT join port working)
#   gl1  external (host)    = GL1_EXT_PORT_BASE + i*10   (lifted to 9600 so it never
#                                                         collides with the gl0 host
#                                                         band 9000.. or the m0
#                                                         metagraph band 9200..9492)
export GL0_IP_BASE=${GL0_IP_BASE:-10}
export GL1_IP_BASE=${GL1_IP_BASE:-30}
export GL0_PORT_BASE=${GL0_PORT_BASE:-$((DAG_L0_PORT_PREFIX * 100))}
export GL1_INT_PORT_BASE=${GL1_INT_PORT_BASE:-$((DAG_L1_PORT_PREFIX * 100))}
export GL1_EXT_PORT_BASE=${GL1_EXT_PORT_BASE:-9600}

# snapshot-streaming reserves these two octets on the shared /24 (its compose
# pins .60 postgres / .61 streaming). gl0's IP band must stop before .60.
SS_IP_FLOOR=60

# Hypergraph node-count ceiling. Default 20 — the largest N the default IP layout
# can host without collision (see derivation below), giving the 16-gl0 scale-up
# target comfortable headroom. Override via NAKAMOTO_MAX_HG_NODES, but we clamp it
# to the IP-safe maximum so an over-eager override can't silently produce
# overlapping gl0/gl1/streaming IPs.
#
# Safe-N derivation (octet bands, gl0 below gl1, both ≤254, gl0 clear of streaming):
#   gl0 occupies [GL0_IP_BASE, GL0_IP_BASE+N-1]; must stay < min(GL1_IP_BASE, SS_IP_FLOOR)
#   gl1 occupies [GL1_IP_BASE, GL1_IP_BASE+N-1]; must stay ≤ 254
# With defaults (gl0=.10, gl1=.30, streaming=.60): gl0 limit ⇒ N≤20 (.10..29),
# gl1 limit ⇒ N≤225. So the IP-safe ceiling is 20. Going beyond 20 means shifting
# GL1_IP_BASE up (clear of streaming .60/.61) or moving to a wider NET_PREFIX.
export NAKAMOTO_MAX_HG_NODES=${NAKAMOTO_MAX_HG_NODES:-20}
_gl0_ip_room=$(( (GL1_IP_BASE < SS_IP_FLOOR ? GL1_IP_BASE : SS_IP_FLOOR) - GL0_IP_BASE ))
_gl1_ip_room=$(( 255 - GL1_IP_BASE ))
HG_IP_SAFE_MAX=$(( _gl0_ip_room < _gl1_ip_room ? _gl0_ip_room : _gl1_ip_room ))
if [ "$NAKAMOTO_MAX_HG_NODES" -gt "$HG_IP_SAFE_MAX" ]; then
  echo "WARN: NAKAMOTO_MAX_HG_NODES=$NAKAMOTO_MAX_HG_NODES exceeds the IP-safe ceiling $HG_IP_SAFE_MAX"
  echo "      (gl0 base .$GL0_IP_BASE / gl1 base .$GL1_IP_BASE / streaming floor .$SS_IP_FLOOR); clamping to $HG_IP_SAFE_MAX."
  NAKAMOTO_MAX_HG_NODES=$HG_IP_SAFE_MAX
fi

_max_of() { [ "$1" -gt "$2" ] && echo "$1" || echo "$2"; }
MAX_HG_NODES=$(_max_of ${NUM_GL0_NODES:-0} ${NUM_GL1_NODES:-0})
MAX_METAGRAPH_NODES=$(_max_of ${NUM_ML0_NODES:-0} ${NUM_CL1_NODES:-0})
MAX_METAGRAPH_NODES=$(_max_of $MAX_METAGRAPH_NODES ${NUM_DL1_NODES:-0})
MAX_NODES=$(_max_of $MAX_HG_NODES $MAX_METAGRAPH_NODES)
# Ensure at least 3 (legacy default).
MAX_NODES=$(_max_of $MAX_NODES 3)
# Hypergraph ceiling = NAKAMOTO_MAX_HG_NODES (arithmetic allocation).
if [ "$MAX_HG_NODES" -gt "$NAKAMOTO_MAX_HG_NODES" ]; then
  echo "ERROR: hypergraph node count ($MAX_HG_NODES) exceeds NAKAMOTO_MAX_HG_NODES=$NAKAMOTO_MAX_HG_NODES."
  echo "       Raise NAKAMOTO_MAX_HG_NODES (up to the IP-safe ceiling $HG_IP_SAFE_MAX) to go higher;"
  echo "       beyond that, shift GL1_IP_BASE / NET_PREFIX and re-verify docker-env-setup.sh arithmetic."
  exit 1
fi
# Metagraph ceiling stays single-digit (string-concat allocation).
[ "$MAX_METAGRAPH_NODES" -gt 9 ] && MAX_METAGRAPH_NODES=9
# MAX_NODES is the unified directory-creation ceiling; bound it by the HG ceiling.
[ "$MAX_NODES" -gt "$NAKAMOTO_MAX_HG_NODES" ] && MAX_NODES=$NAKAMOTO_MAX_HG_NODES
export MAX_NODES MAX_HG_NODES MAX_METAGRAPH_NODES HG_IP_SAFE_MAX

# Layer URLs: explicit overrides take priority, otherwise built from TEST_HOST + port prefix
# When using a remote host, GL1 defaults to port 9010 instead of the local band.
# Locally, gl1's EXTERNAL host port for node 0 is GL1_EXT_PORT_BASE (9600) — the
# gl1 container's internal port stays in the DAG_L1_PORT_PREFIX (9100) band, but
# the host reaches it via the lifted external port. This is the single source of
# truth shared with docker-env-setup.sh and the JS test client.
if [ "$TEST_HOST" != "http://localhost" ]; then
  GL1_DEFAULT_PORT=9010
else
  GL1_DEFAULT_PORT="${GL1_EXT_PORT_BASE}"
fi
export GL0_URL=${GL0_URL:-"${TEST_HOST}:${DAG_L0_PORT_PREFIX}00"}
export GL1_URL=${GL1_URL:-"${TEST_HOST}:${GL1_DEFAULT_PORT}"}
export ML0_URL=${ML0_URL:-"${TEST_HOST}:${ML0_PORT_PREFIX}00"}
export CL1_URL=${CL1_URL:-"${TEST_HOST}:${CL1_PORT_PREFIX}00"}
export DL1_URL=${DL1_URL:-"${TEST_HOST}:${DL1_PORT_PREFIX}00"}
