#!/usr/bin/env bash

set -e


# 2. More thorough container cleanup with proper error handling
echo "Stopping and removing gl0 containers..."


cleanup_container() {
    local name=$1
    local vol=$2
    docker stop $name 2>/dev/null || true
    docker rm -f $name 2>/dev/null || true
    docker volume rm ${vol} 2>/dev/null || true
}

cleanup() {
    for i in $(seq 0 9); do
        cleanup_container gl0-$i gl0-data-$i &
        cleanup_container gl1-$i gl1-data-$i &
        # Nakamoto sidecar containers (only present when --nakamoto-gl0 was used)
        cleanup_container sidecar-$i "" &
        # Legacy single-metagraph layout (pre multi-metagraph)
        cleanup_container dl1-$i dl1-data-$i &
        cleanup_container ml0-$i ml0-data-$i &
        cleanup_container cl1-$i cl1-data-$i &
        # Multi-metagraph layout: ml0-m${k}-${i}, cl1-m${k}-${i}, dl1-m${k}-${i}
        # for k in 0..9 (matches NUM_METAGRAPHS cap from set-env.sh).
        for k in $(seq 0 9); do
            cleanup_container ml0-m${k}-$i ml0-data-m${k}-$i &
            cleanup_container cl1-m${k}-$i cl1-data-m${k}-$i &
            cleanup_container dl1-m${k}-$i dl1-data-m${k}-$i &
        done
    done
    cleanup_container snapshot-streaming-postgres ss-pgdata &
    cleanup_container snapshot-streaming "" &
    # Monitoring stack — attached to tessellation_common. If any of these are
    # left running, the network-removal loop below spins forever on "has active
    # endpoints". The compose-runner cleanup trap deliberately leaves them up
    # across test runs (see compose-runner.sh:cleanup_end) so `just clean-data`
    # doesn't disturb them; full docker teardown (this script, invoked by
    # `just down` / `just clean-docker` / `just clean` / `just nuke`) takes them
    # down. grafana-renderer also attaches to tessellation_common, so include it.
    cleanup_container prometheus "" &
    cleanup_container nakamoto-grafana "" &
    cleanup_container grafana-renderer "" &
    rm -rf "$(dirname "$0")/../snapshot-streaming/data" 2>/dev/null || true &
    LAST_PID=$!
    wait $LAST_PID
}

cleanup &
export CLEANUP_PID=$!
# 8. Remove the network with better error handling and retry logic
echo "Removing tessellation_common network..."
while true; do
  output=$(docker network rm tessellation_common 2>&1) || true
  if [[ $output == *"not found"* ]]; then
    echo "Network removed successfully"
    break
  elif [[ $output != *"has active endpoints"* ]]; then
    # If the error message is not present, break the loop
    echo "Network removed successfully or encountered a different error. Output below"
    echo $output
    break
  fi
  echo "Network has active endpoints, retrying in 1 second..."
  sleep 1
done

echo "Waiting for cleanup to finish..."
wait $CLEANUP_PID

# 3. Find and kill any lingering processes binding to tessellation ports

# TODO: Update this to use new port offsets
check_port_binds() {
    if [ "$CHECK_PORT_BINDS" == "true" ]; then
        echo "Checking for lingering processes on common ports... -- this requires sudo"
        for base_port in 9000 9001 9002 9010 9011 9012; do
            for prefix in "" "1" "2"; do
                port="${prefix}${base_port}"
                pid=$(sudo lsof -i:$port -t 2>/dev/null || true)
                if [ -n "$pid" ]; then
                    echo "Found process $pid on port $port"
                    return 0
                fi
            done
        done
    fi
}

check_port_binds