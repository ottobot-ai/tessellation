#!/bin/bash
# MPT Rollback E2E Test
# Tests that StateProof remains valid after rollback scenarios with delegated stakes
#
# Usage: ./test-scripts/mpt_rollback_e2e_test.sh [test_name]
#   test_name: basic | between_ds | replacement | all (default: all)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"
cd "$REPO_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Config
ADDRESS="DAG3yG9CRoYd4XF4PTBtLo95h8uiGNWYXXrASJGg"
WALLET="docker/jars/wallet.jar"
WAIT_ORDINALS=30  # seconds to wait for transactions to be in snapshot

log() { echo -e "${GREEN}[TEST]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

wait_for_ordinal() {
    local target=$1
    local timeout=${2:-120}
    local start=$(date +%s)
    while true; do
        local current=$(curl -s http://localhost:9000/global-snapshots/latest 2>/dev/null | jq -r '.value.ordinal // 0')
        if [ "$current" -ge "$target" ]; then
            echo "$current"
            return 0
        fi
        if [ $(($(date +%s) - start)) -gt $timeout ]; then
            fail "Timeout waiting for ordinal $target (current: $current)"
        fi
        sleep 2
    done
}

wait_for_cluster() {
    log "Waiting for cluster to be healthy..."
    local timeout=180
    local start=$(date +%s)
    while true; do
        local healthy=$(docker ps --filter "health=healthy" --format "{{.Names}}" 2>/dev/null | wc -l)
        if [ "$healthy" -ge 6 ]; then
            log "Cluster healthy ($healthy nodes)"
            return 0
        fi
        if [ $(($(date +%s) - start)) -gt $timeout ]; then
            fail "Timeout waiting for cluster (healthy: $healthy)"
        fi
        sleep 5
    done
}

cleanup_cluster() {
    log "Cleaning up cluster..."
    docker stop $(docker ps -q) 2>/dev/null || true
    docker rm $(docker ps -aq) 2>/dev/null || true
    docker network rm tessellation_common 0_default 2>/dev/null || true
    for i in 0 1 2; do
        docker run --rm -v "$REPO_DIR/nodes/$i:/node" alpine sh -c "rm -rf /node/*" 2>/dev/null || true
    done
}

start_cluster() {
    log "Starting fresh cluster..."
    cleanup_cluster
    SKIP_ASSEMBLY=true bash ./docker/bin/compose-runner.sh >/dev/null 2>&1 &
    wait_for_cluster
    sleep 10  # Extra time for stability
}

create_token_lock() {
    local amount=$1
    local parent_file=$2
    local replace_hash=$3
    
    cd nodes/0
    source .envrc
    
    local cmd="java -jar ../../$WALLET create-token-lock --amount $amount"
    [ -n "$parent_file" ] && cmd="$cmd --parent $parent_file"
    [ -n "$replace_hash" ] && cmd="$cmd --replace $replace_hash"
    
    local hash=$($cmd)
    curl -s -X POST -H 'Content-Type: application/json' -d @event http://localhost:9100/token-locks >/dev/null
    cd "$REPO_DIR"
    echo "$hash"
}

create_delegated_stake() {
    local amount=$1
    local tl_hash=$2
    local node_id=$3
    local parent_file=$4
    
    cd nodes/0
    source .envrc
    
    local cmd="java -jar ../../$WALLET create-delegated-stake --amount $amount --token-lock $tl_hash"
    [ -n "$node_id" ] && cmd="$cmd --nodeId $node_id"
    [ -n "$parent_file" ] && cmd="$cmd --parent $parent_file"
    
    local hash=$($cmd)
    curl -s -X POST -H 'Content-Type: application/json' -d @event http://localhost:9000/delegated-stakes >/dev/null
    cd "$REPO_DIR"
    echo "$hash"
}

get_last_ref() {
    local endpoint=$1
    local address=$2
    local output=$3
    curl -s "http://localhost:$endpoint/last-reference/$address" -o "$output"
}

delete_ordinals() {
    local start=$1
    local end=$2
    log "Deleting ordinals $start to $end from gl0-0..."
    local cmd="for i in \$(seq $start $end); do rm -f /data/snapshot_info/\$i /data/mpt_snapshot_info/\$i /data/incremental_snapshot/ordinal/0/\$i 2>/dev/null; done"
    docker run --rm -v "$REPO_DIR/nodes/0/gl0-data:/data" alpine sh -c "$cmd"
}

setup_rollback() {
    log "Setting up rollback mode..."
    local hash=$(curl -s http://localhost:9010/global-snapshots/latest | jq -r '.value.lastSnapshotHash')
    sed -i '/CL_DOCKER_ROLLBACK/d' nodes/0/.env 2>/dev/null || true
    echo "CL_DOCKER_ROLLBACK=true" >> nodes/0/.env
    echo "CL_DOCKER_ROLLBACK_HASH=$hash" >> nodes/0/.env
    log "Rollback hash: $hash"
}

restart_gl0() {
    log "Restarting gl0-0..."
    docker rm gl0-0 2>/dev/null || true
    docker compose -f nodes/0/docker-compose.yaml up -d gl0
    sleep 60  # Wait for rollback/traverse
}

check_stateproof_errors() {
    log "Checking for StateProof errors..."
    local errors=$(docker logs gl0-0 2>&1 | grep -iE "stateproof.*broken|broken.*stateproof" | head -5)
    if [ -n "$errors" ]; then
        echo "$errors"
        return 1
    fi
    return 0
}

get_ds_ordinals() {
    curl -s http://localhost:9000/global-snapshots/latest/combined | \
        jq -r ".[1].activeDelegatedStakes[\"$ADDRESS\"] | map(.createdAt) | sort | @csv" 2>/dev/null
}

# =============================================================================
# TEST: Basic Rollback with 2 Delegated Stakes
# =============================================================================
test_basic_rollback() {
    log "=========================================="
    log "TEST: Basic Rollback with 2 Delegated Stakes"
    log "=========================================="
    
    start_cluster
    
    # Create TL#1 + DS#1
    log "Creating TL#1 + DS#1..."
    local tl1=$(create_token_lock 6000)
    log "TL#1: $tl1"
    sleep $WAIT_ORDINALS
    
    local ds1=$(create_delegated_stake 6000 "$tl1")
    log "DS#1: $ds1"
    sleep $WAIT_ORDINALS
    
    # Create TL#2 + DS#2 (different node)
    log "Creating TL#2 + DS#2..."
    cd nodes/0 && source .envrc
    get_last_ref "9100/token-locks" "$ADDRESS" "tl-ref.json"
    cd "$REPO_DIR"
    
    local tl2=$(create_token_lock 6000 "nodes/0/tl-ref.json")
    log "TL#2: $tl2"
    sleep $WAIT_ORDINALS
    
    local node1_id=$(cat nodes/1/peer_id)
    cd nodes/0 && source .envrc
    get_last_ref "9000/delegated-stakes" "$ADDRESS" "ds-ref.json"
    cd "$REPO_DIR"
    
    local ds2=$(create_delegated_stake 6000 "$tl2" "$node1_id" "nodes/0/ds-ref.json")
    log "DS#2: $ds2"
    sleep $WAIT_ORDINALS
    
    # Verify 2 DS exist
    local ds_ordinals=$(get_ds_ordinals)
    log "DS ordinals: $ds_ordinals"
    
    # Get current ordinal
    local current=$(curl -s http://localhost:9000/global-snapshots/latest | jq '.value.ordinal')
    log "Current ordinal: $current"
    
    # Stop and clear MPT data
    log "Stopping gl0-0 and clearing MPT data..."
    docker stop gl0-0
    docker run --rm -v "$REPO_DIR/nodes/0/gl0-data:/data" alpine sh -c "rm -rf /data/mpt_snapshot_info/*"
    
    # Setup rollback and restart
    setup_rollback
    restart_gl0
    
    # Check for errors
    if check_stateproof_errors; then
        log "${GREEN}✅ TEST PASSED: Basic Rollback${NC}"
        return 0
    else
        fail "StateProof errors found!"
        return 1
    fi
}

# =============================================================================
# TEST: Rollback Between DS Creations
# =============================================================================
test_between_ds() {
    log "=========================================="
    log "TEST: Rollback Between DS Creations"
    log "=========================================="
    
    start_cluster
    
    # Create TL#1 + DS#1
    log "Creating TL#1 + DS#1..."
    local tl1=$(create_token_lock 6000)
    sleep $WAIT_ORDINALS
    local ds1=$(create_delegated_stake 6000 "$tl1")
    sleep $WAIT_ORDINALS
    
    # Note DS#1 ordinal
    local ds1_ordinal=$(curl -s http://localhost:9000/global-snapshots/latest/combined | \
        jq ".[1].activeDelegatedStakes[\"$ADDRESS\"][0].createdAt")
    log "DS#1 created at ordinal: $ds1_ordinal"
    
    # Wait a few ordinals
    local gap_start=$(curl -s http://localhost:9000/global-snapshots/latest | jq '.value.ordinal')
    log "Gap starts at ordinal: $gap_start"
    sleep 20
    
    # Create TL#2 + DS#2
    log "Creating TL#2 + DS#2..."
    cd nodes/0 && source .envrc
    get_last_ref "9100/token-locks" "$ADDRESS" "tl-ref.json"
    cd "$REPO_DIR"
    local tl2=$(create_token_lock 6000 "nodes/0/tl-ref.json")
    sleep $WAIT_ORDINALS
    
    local node1_id=$(cat nodes/1/peer_id)
    cd nodes/0 && source .envrc
    get_last_ref "9000/delegated-stakes" "$ADDRESS" "ds-ref.json"
    cd "$REPO_DIR"
    local ds2=$(create_delegated_stake 6000 "$tl2" "$node1_id" "nodes/0/ds-ref.json")
    sleep $WAIT_ORDINALS
    
    # Note DS#2 ordinal
    local ds2_ordinal=$(curl -s http://localhost:9000/global-snapshots/latest/combined | \
        jq ".[1].activeDelegatedStakes[\"$ADDRESS\"][1].createdAt")
    log "DS#2 created at ordinal: $ds2_ordinal"
    
    local current=$(curl -s http://localhost:9000/global-snapshots/latest | jq '.value.ordinal')
    log "Current ordinal: $current"
    
    # Stop and delete ordinals BETWEEN DS#1 and DS#2
    log "Stopping gl0-0..."
    docker stop gl0-0
    
    local delete_start=$((ds1_ordinal + 2))
    delete_ordinals $delete_start $current
    
    # Setup rollback and restart
    setup_rollback
    restart_gl0
    
    # Check for errors
    if check_stateproof_errors; then
        log "${GREEN}✅ TEST PASSED: Rollback Between DS${NC}"
        return 0
    else
        fail "StateProof errors found!"
        return 1
    fi
}

# =============================================================================
# TEST: Token Lock Replacement
# =============================================================================
test_replacement() {
    log "=========================================="
    log "TEST: Token Lock Replacement"
    log "=========================================="
    
    start_cluster
    
    # Create TL#1 + DS#1
    log "Creating TL#1 + DS#1..."
    local tl1=$(create_token_lock 6000)
    sleep $WAIT_ORDINALS
    local ds1=$(create_delegated_stake 6000 "$tl1")
    sleep $WAIT_ORDINALS
    
    # Create TL#2 + DS#2
    log "Creating TL#2 + DS#2..."
    cd nodes/0 && source .envrc
    get_last_ref "9100/token-locks" "$ADDRESS" "tl-ref.json"
    cd "$REPO_DIR"
    local tl2=$(create_token_lock 6000 "nodes/0/tl-ref.json")
    sleep $WAIT_ORDINALS
    
    local node1_id=$(cat nodes/1/peer_id)
    cd nodes/0 && source .envrc
    get_last_ref "9000/delegated-stakes" "$ADDRESS" "ds-ref.json"
    cd "$REPO_DIR"
    local ds2=$(create_delegated_stake 6000 "$tl2" "$node1_id" "nodes/0/ds-ref.json")
    sleep $WAIT_ORDINALS
    
    local before_replace=$(curl -s http://localhost:9000/global-snapshots/latest | jq '.value.ordinal')
    log "Before replacement, ordinal: $before_replace"
    
    # Create replacement token lock
    log "Creating replacement token lock..."
    cd nodes/0 && source .envrc
    get_last_ref "9100/token-locks" "$ADDRESS" "tl-ref.json"
    cd "$REPO_DIR"
    local tl_replace=$(create_token_lock 7000 "nodes/0/tl-ref.json" "$tl1")
    log "TL replacement: $tl_replace"
    sleep $WAIT_ORDINALS
    
    local current=$(curl -s http://localhost:9000/global-snapshots/latest | jq '.value.ordinal')
    log "After replacement, ordinal: $current"
    
    # Verify replacement in state
    local tl_amounts=$(curl -s http://localhost:9000/global-snapshots/latest/combined | \
        jq ".[1].activeTokenLocks[\"$ADDRESS\"] | map(.value.amount)")
    log "Token lock amounts: $tl_amounts"
    
    # Stop and delete ordinals after DS#2 but before replacement
    log "Stopping gl0-0..."
    docker stop gl0-0
    
    delete_ordinals $((before_replace + 1)) $current
    
    # Setup rollback and restart
    setup_rollback
    restart_gl0
    
    # Check for errors
    if check_stateproof_errors; then
        log "${GREEN}✅ TEST PASSED: Token Lock Replacement${NC}"
        return 0
    else
        fail "StateProof errors found!"
        return 1
    fi
}

# =============================================================================
# Main
# =============================================================================
main() {
    local test_name=${1:-all}
    local failed=0
    
    log "MPT Rollback E2E Test Suite"
    log "Test: $test_name"
    log ""
    
    case $test_name in
        basic)
            test_basic_rollback || failed=1
            ;;
        between_ds)
            test_between_ds || failed=1
            ;;
        replacement)
            test_replacement || failed=1
            ;;
        all)
            test_basic_rollback || failed=1
            test_between_ds || failed=1
            test_replacement || failed=1
            ;;
        *)
            echo "Usage: $0 [basic|between_ds|replacement|all]"
            exit 1
            ;;
    esac
    
    echo ""
    if [ $failed -eq 0 ]; then
        log "${GREEN}=========================================="
        log "ALL TESTS PASSED"
        log "==========================================${NC}"
    else
        log "${RED}=========================================="
        log "SOME TESTS FAILED"
        log "==========================================${NC}"
        exit 1
    fi
}

main "$@"
