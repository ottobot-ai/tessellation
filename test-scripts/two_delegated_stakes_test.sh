#!/bin/bash
# Test script for TWO independent delegated stakes to reproduce MPT mismatch bug
# This creates two token locks and two delegated stakes from the same address

set -e

function exit_func() {
  echo "Test failed!"
  exit 1
}

echo "Waiting for docker containers to come online"
sleep 45

### CLUSTER SPECIFIC TESTING BELOW
echo "Starting TWO delegated stakes test"

export DAG_L0_URL="http://localhost:9000"
export DAG_L1_URL="http://localhost:9100"

# Get the address from node 0
cd ./nodes/0/
source .envrc
export ADDRESS=$(java -jar ../../wallet.jar show-address | grep "DAG" | head -1)
echo "Test address: $ADDRESS"
cd ../../

# Create node update params for gl0 kp (node 0)
echo "=== Creating node params for node 0 ==="
cd ./nodes/0/
out=$(
  source .envrc
  java -jar ../../wallet.jar create-node-params
)
echo "Create node params output $out"
cat event
cp event initial-node-params.json
curl -i -X POST --header 'Content-Type: application/json' --data @initial-node-params.json "$DAG_L0_URL"/node-params
sleep 30
cd ../../

curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq -e '.[1].updateNodeParameters | length > 0' > /dev/null || \
{ echo "ERROR: updateNodeParameters is empty in snapshot combined"; exit_func; }

# Create node update params for node 1
echo "=== Creating node params for node 1 ==="
cd ./nodes/1/
out=$(
  source .envrc
  java -jar ../../wallet.jar create-node-params
)
echo "Create node params output $out"
cat event
cp event initial-node-params.json
curl -i -X POST --header 'Content-Type: application/json' --data @initial-node-params.json "$DAG_L0_URL"/node-params
sleep 30
cd ../../

curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq -e '.[1].updateNodeParameters | length > 1' > /dev/null || \
{ echo "ERROR: updateNodeParameters count should be > 1"; exit_func; }


# ============================================
# Create FIRST token lock (3000 DAG)
# ============================================
echo "=== Creating FIRST token lock (3000 DAG) ==="
cd ./nodes/0/
out=$(
  source .envrc
  java -jar ../../wallet.jar create-token-lock --amount 3000
)
echo "Create token lock 1 output hash $out"
cat event
cp event token-lock-1.json
export TOKEN_LOCK_HASH_1=$out
echo "Token lock 1 hash: $TOKEN_LOCK_HASH_1"
curl -i -X POST --header 'Content-Type: application/json' --data @token-lock-1.json "$DAG_L1_URL"/token-locks
sleep 40
cd ../../

curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq -e '.[1].activeTokenLocks | length == 1' > /dev/null || \
{ echo "ERROR: activeTokenLocks should have 1 entry"; exit_func; }


# ============================================
# Create FIRST delegated stake (using token lock 1, delegating to node 0)
# ============================================
echo "=== Creating FIRST delegated stake ==="
cd ./nodes/0/
source .envrc
export NODE_0_ID=$(cat peer_id)
echo "Node 0 ID: $NODE_0_ID"

out=$(
  source .envrc
  java -jar ../../wallet.jar create-delegated-stake --amount 3000 --token-lock $TOKEN_LOCK_HASH_1
)
echo "Create delegated stake 1 hash $out"
export DELEGATED_STAKE_HASH_1=$out
cat event
cp event delegated-stake-1.json
curl -i -X POST --header 'Content-Type: application/json' --data @delegated-stake-1.json "$DAG_L0_URL"/delegated-stakes
sleep 30
cd ../../

curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq -e '.[1].activeDelegatedStakes | length == 1' > /dev/null || \
{ echo "ERROR: activeDelegatedStakes should have 1 entry"; exit_func; }

echo "First delegated stake created. Hash: $DELEGATED_STAKE_HASH_1"


# ============================================
# Create SECOND token lock (3000 DAG)
# ============================================
echo "=== Creating SECOND token lock (3000 DAG) ==="
cd ./nodes/0/
out=$(
  source .envrc
  java -jar ../../wallet.jar create-token-lock --amount 3000
)
echo "Create token lock 2 output hash $out"
cat event
cp event token-lock-2.json
export TOKEN_LOCK_HASH_2=$out
echo "Token lock 2 hash: $TOKEN_LOCK_HASH_2"
curl -i -X POST --header 'Content-Type: application/json' --data @token-lock-2.json "$DAG_L1_URL"/token-locks
sleep 40
cd ../../

curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq -e '.[1].activeTokenLocks | length == 2' > /dev/null || \
{ echo "ERROR: activeTokenLocks should have 2 entries"; exit_func; }


# ============================================
# Create SECOND delegated stake (using token lock 2, delegating to node 1)
# ============================================
echo "=== Creating SECOND delegated stake ==="

# Get node 1's peer ID
export NODE_1_ID=$(cat ./nodes/1/peer_id)
echo "Node 1 ID: $NODE_1_ID"

cd ./nodes/0/

# Get the last reference for the delegated stake chain
wget -q "$DAG_L0_URL/delegated-stakes/last-reference/$ADDRESS" -O ds-last-ref.json
echo "Last DS reference:"
cat ds-last-ref.json

out=$(
  source .envrc
  java -jar ../../wallet.jar create-delegated-stake --amount 3000 --token-lock $TOKEN_LOCK_HASH_2 --nodeId $NODE_1_ID --parent ds-last-ref.json
)
echo "Create delegated stake 2 hash $out"
export DELEGATED_STAKE_HASH_2=$out
cat event
cp event delegated-stake-2.json
curl -i -X POST --header 'Content-Type: application/json' --data @delegated-stake-2.json "$DAG_L0_URL"/delegated-stakes
sleep 30
cd ../../

# ============================================
# Verify TWO delegated stakes exist
# ============================================
echo "=== Verifying TWO delegated stakes ==="

# Check the snapshot combined endpoint
DS_COUNT=$(curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | \
jq '.[1].activeDelegatedStakes | to_entries | map(.value | length) | add')
echo "Total delegated stake records in snapshot: $DS_COUNT"

if [ "$DS_COUNT" != "2" ]; then
  echo "ERROR: Expected 2 delegated stakes, got $DS_COUNT"
  # Don't exit - let's see what we have
  curl -s "$DAG_L0_URL"/global-snapshots/latest/combined | jq '.[1].activeDelegatedStakes'
fi

# Check via the info endpoint
curl -s "$DAG_L0_URL/delegated-stakes/$ADDRESS/info" | jq '.'

DS_INFO_COUNT=$(curl -s "$DAG_L0_URL/delegated-stakes/$ADDRESS/info" | \
jq '.activeDelegatedStakes | length')
echo "Delegated stakes from info endpoint: $DS_INFO_COUNT"

if [ "$DS_INFO_COUNT" != "2" ]; then
  echo "ERROR: Expected 2 delegated stakes in info, got $DS_INFO_COUNT"
  exit_func
fi

# Record the current ordinal
CURRENT_ORDINAL=$(curl -s "$DAG_L0_URL"/global-snapshots/latest | jq '.value.ordinal')
echo "Current ordinal with 2 delegated stakes: $CURRENT_ORDINAL"

# Wait a few more snapshots to let rewards accumulate
echo "Waiting for a few more snapshots..."
sleep 60

FINAL_ORDINAL=$(curl -s "$DAG_L0_URL"/global-snapshots/latest | jq '.value.ordinal')
echo "Final ordinal: $FINAL_ORDINAL"

echo ""
echo "============================================"
echo "TWO DELEGATED STAKES TEST COMPLETED"
echo "============================================"
echo "Token Lock 1: $TOKEN_LOCK_HASH_1"
echo "Token Lock 2: $TOKEN_LOCK_HASH_2"
echo "Delegated Stake 1: $DELEGATED_STAKE_HASH_1 (to node 0)"
echo "Delegated Stake 2: $DELEGATED_STAKE_HASH_2 (to node 1)"
echo "Ordinal range with 2 stakes: $CURRENT_ORDINAL - $FINAL_ORDINAL"
echo ""
echo "Now test RETRAVERSAL by restarting a node and checking for StateProof errors"
echo "============================================"

echo "success"
