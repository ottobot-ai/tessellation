#!/usr/bin/env bash
#
# Consensus Recovery Test Script
# 
# Simulates network conditions that can cause consensus stalls:
# - High latency between nodes
# - Packet loss
# - Node unresponsiveness
#
# Prerequisites:
# - Docker environment running (just up --num-gl0=5)
# - tc (traffic control) available in containers
#
# Usage:
#   ./docker/bin/test-consensus-recovery.sh [scenario]
#
# Scenarios:
#   latency     - Add 500ms latency to 2 nodes
#   packet-loss - Add 20% packet loss to 2 nodes  
#   partition   - Isolate 2 nodes from the rest
#   slow-peer   - Make 1 node extremely slow (2s latency)
#   restore     - Remove all network impairments
#   monitor     - Watch consensus metrics in real-time

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../../"

# Get list of running gl0 containers
get_gl0_containers() {
    docker ps --filter "name=gl0" --format "{{.Names}}" | sort
}

# Apply network latency to a container
apply_latency() {
    local container=$1
    local delay_ms=$2
    echo "Applying ${delay_ms}ms latency to $container"
    docker exec "$container" tc qdisc add dev eth0 root netem delay "${delay_ms}ms" 2>/dev/null || \
    docker exec "$container" tc qdisc change dev eth0 root netem delay "${delay_ms}ms"
}

# Apply packet loss to a container
apply_packet_loss() {
    local container=$1
    local loss_pct=$2
    echo "Applying ${loss_pct}% packet loss to $container"
    docker exec "$container" tc qdisc add dev eth0 root netem loss "${loss_pct}%" 2>/dev/null || \
    docker exec "$container" tc qdisc change dev eth0 root netem loss "${loss_pct}%"
}

# Remove network impairments from a container
restore_network() {
    local container=$1
    echo "Restoring network for $container"
    docker exec "$container" tc qdisc del dev eth0 root 2>/dev/null || true
}

# Monitor consensus metrics
monitor_consensus() {
    echo "Monitoring consensus metrics (Ctrl+C to stop)..."
    echo ""
    while true; do
        clear
        echo "=== Consensus Recovery Test Monitor ==="
        echo "Time: $(date)"
        echo ""
        
        for container in $(get_gl0_containers); do
            echo "--- $container ---"
            # Get last few consensus-related log lines
            docker logs "$container" 2>&1 | grep -E "(Stall|LOCKED|REOPENED|abandoned|unlock|Round)" | tail -3
            echo ""
        done
        
        sleep 5
    done
}

# Check ordinals across nodes
check_ordinals() {
    echo "=== Node Ordinals ==="
    for container in $(get_gl0_containers); do
        local port=$(docker port "$container" 9000 | cut -d: -f2)
        local ordinal=$(curl -s "http://localhost:$port/global-snapshots/latest" 2>/dev/null | jq -r '.value.ordinal // "N/A"')
        echo "$container: ordinal=$ordinal"
    done
}

# Main scenario handler
case "${1:-help}" in
    latency)
        echo "=== Latency Scenario ==="
        echo "Adding 500ms latency to 2 nodes to simulate high-latency peers"
        containers=($(get_gl0_containers))
        if [ ${#containers[@]} -lt 3 ]; then
            echo "Error: Need at least 3 gl0 nodes. Start with: just up --num-gl0=5"
            exit 1
        fi
        apply_latency "${containers[0]}" 500
        apply_latency "${containers[1]}" 500
        echo ""
        echo "Latency applied. Monitor with: $0 monitor"
        echo "Restore with: $0 restore"
        ;;
        
    packet-loss)
        echo "=== Packet Loss Scenario ==="
        echo "Adding 20% packet loss to 2 nodes"
        containers=($(get_gl0_containers))
        if [ ${#containers[@]} -lt 3 ]; then
            echo "Error: Need at least 3 gl0 nodes"
            exit 1
        fi
        apply_packet_loss "${containers[0]}" 20
        apply_packet_loss "${containers[1]}" 20
        echo ""
        echo "Packet loss applied. Monitor with: $0 monitor"
        ;;
        
    partition)
        echo "=== Network Partition Scenario ==="
        echo "Isolating 2 nodes from the cluster"
        containers=($(get_gl0_containers))
        if [ ${#containers[@]} -lt 4 ]; then
            echo "Error: Need at least 4 gl0 nodes for partition test"
            exit 1
        fi
        # Drop all traffic to simulate complete partition
        apply_packet_loss "${containers[0]}" 100
        apply_packet_loss "${containers[1]}" 100
        echo ""
        echo "Partition created. Nodes ${containers[0]} and ${containers[1]} are isolated."
        echo "Watch for fork detection. Restore with: $0 restore"
        ;;
        
    slow-peer)
        echo "=== Slow Peer Scenario ==="
        echo "Making 1 node extremely slow (2000ms latency)"
        containers=($(get_gl0_containers))
        apply_latency "${containers[0]}" 2000
        echo ""
        echo "Slow peer created: ${containers[0]}"
        echo "This should trigger stall detection and ACK spreading"
        ;;
        
    restore)
        echo "=== Restoring All Networks ==="
        for container in $(get_gl0_containers); do
            restore_network "$container"
        done
        echo "All network impairments removed"
        ;;
        
    monitor)
        monitor_consensus
        ;;
        
    ordinals)
        check_ordinals
        ;;
        
    stress)
        echo "=== Stress Test Scenario ==="
        echo "Cycling through impairments every 30 seconds"
        while true; do
            echo "[$(date)] Applying latency..."
            $0 latency
            sleep 30
            
            echo "[$(date)] Restoring..."
            $0 restore
            sleep 10
            
            echo "[$(date)] Applying packet loss..."
            $0 packet-loss
            sleep 30
            
            echo "[$(date)] Restoring..."
            $0 restore
            sleep 10
            
            check_ordinals
        done
        ;;
        
    *)
        echo "Consensus Recovery Test Script"
        echo ""
        echo "Usage: $0 <scenario>"
        echo ""
        echo "Scenarios:"
        echo "  latency      Add 500ms latency to 2 nodes"
        echo "  packet-loss  Add 20% packet loss to 2 nodes"
        echo "  partition    Isolate 2 nodes completely"
        echo "  slow-peer    Make 1 node extremely slow (2s latency)"
        echo "  restore      Remove all network impairments"
        echo "  monitor      Watch consensus logs in real-time"
        echo "  ordinals     Check ordinals across all nodes"
        echo "  stress       Cycle through impairments continuously"
        echo ""
        echo "Example workflow:"
        echo "  1. just up --num-gl0=5"
        echo "  2. $0 latency"
        echo "  3. $0 monitor    # watch for stalls and recovery"
        echo "  4. $0 ordinals   # check for forks"
        echo "  5. $0 restore"
        ;;
esac
