# Tessellation Consensus Recovery Test Report

**Date:** 2026-03-11  
**Branch:** `scas/all-postmortem-fixes` (commit `0c7abd51`)  
**Testers:** Work Agent + James A  
**Environment:** 8-node GL0 cluster on Euler (v3.5.12 JARs)

---

## Executive Summary

We conducted distributed systems testing on the post-mortem fixes branch. The recovery mechanism works well for partial failures but **has a critical bug that can remove ALL facilitators under quorum loss scenarios**.

### Results Overview

| Test | Status | Severity |
|------|--------|----------|
| Cascading Failure | ✅ PASS | - |
| Flapping Node | ✅ PASS | - |
| Node Rejoin | ⚠️ OBSERVE | Low |
| Quorum Loss | 🐛 BUG | **CRITICAL** |

---

## Test Details

### TEST 1: Cascading Failure ✅

**Scenario:** Isolate nodes one-by-one to simulate sequential failures.

**Steps:**
1. Start with 8 nodes, 6 active facilitators
2. Isolate gl0-1 → wait → isolate gl0-2 → wait → isolate gl0-3

**Results:**
```
Ordinal 11: facilitatorCount=6
Ordinal 12: facilitatorCount=5, removedFacilitators=Set(8dc987df)
Ordinal 12: facilitatorCount=4, removedFacilitators=Set(8dc987df, 96410098)
Ordinal 12: facilitatorCount=3, removedFacilitators=Set(8dc987df, 96410098, ad677fd8)
Status: Finished ✅
```

**Conclusion:** System correctly handles cascading failures with proper ACK spreading through multiple consensus phases (Facility → Proposal → Signature).

---

### TEST 2: Flapping Node ✅

**Scenario:** Rapidly toggle a node's network (isolate/restore every 20s).

**Steps:**
1. Start with stable cluster
2. 3 cycles of: isolate gl0-4 for 20s → restore for 20s

**Results:**
- Ordinals progressed normally: 12→13→14→15
- No stalls triggered
- `lockStatus=Open` maintained throughout

**Conclusion:** System is resilient to brief network interruptions. The 20s isolation period is shorter than declaration timeout, so no stalls occur.

---

### TEST 3: Node Rejoin ⚠️

**Scenario:** After nodes are removed as facilitators, can they rejoin?

**Observation:**
- After cascading failure test, cluster had 3 facilitators
- Restored network to all 8 nodes
- Waited 3+ minutes
- Cluster stayed at 3 facilitators

**Conclusion:** Removed nodes do NOT automatically rejoin the facilitator set. They need to go through the candidacy process. This is expected behavior but worth noting for operational awareness.

---

### TEST 4: Quorum Loss 🐛 CRITICAL BUG

**Scenario:** With only 3 facilitators, isolate 2 to cause quorum loss.

**Steps:**
1. Start with 3 facilitators: gl0-0 (1b4b9f98), gl0-4 (07722c42), gl0-5 (5195a7c1)
2. Isolate gl0-4 and gl0-5
3. Only gl0-0 can participate

**Results:**
```
Ordinal 21: facilitatorCount=3, lockStatus=Open (last successful)
Ordinal 22: facilitatorCount=3, lockStatus=Closed (stall detected)
Ordinal 22: facilitatorCount=3, lockStatus=Closed, spreadAckKinds=Set(Facility{})
Ordinal 22: facilitatorCount=0, lockStatus=Reopened, 
            removedFacilitators=Set(07722c42, 1b4b9f98, 5195a7c1)
```

**BUG: ALL FACILITATORS REMOVED**

The unlock mechanism fired and removed everyone, including the only responsive node!

#### Root Cause Analysis

1. gl0-0 was the only node able to send ACKs
2. gl0-0's ACK declared only itself as "present" (it couldn't see the isolated nodes)
3. Unlock logic requires majority ACKs to determine who to keep
4. With only 1/3 nodes ACKing, no node achieved majority "present" votes
5. Result: everyone marked for removal, including gl0-0 itself

#### Impact

- **Cluster death:** 0 facilitators = no consensus possible
- **No recovery:** Even restoring network doesn't help; facilitator set is empty
- **Mainnet risk:** Could occur during severe network partition

#### Suggested Fix

In `UnlockConsensusUpdate.scala`, add minimum facilitator check:

```scala
// Current logic (simplified)
val (removedFacilitators, keptFacilitators) = 
  facilitators.value.partition(peerId => presentVotes(peerId) < majorityThreshold)

// Proposed fix
val minFacilitators = 2  // Or configurable

val safeKeptFacilitators = 
  if (keptFacilitators.size < minFacilitators) {
    // Abort unlock if it would remove too many facilitators
    // Either keep everyone, or keep the top N by vote count
    logger.warn(s"Unlock would reduce facilitators below minimum ($minFacilitators), aborting")
    facilitators.value
  } else {
    keptFacilitators
  }
```

Alternative approaches:
1. **Abort unlock** if result would have < N facilitators
2. **Keep top N by vote count** even if below threshold
3. **Self-preservation:** A node should never vote to remove itself if it would leave 0 facilitators

---

## Recommendations

### Critical (Before v4 retry)
1. **Fix minimum facilitator bug** - Add safety check in UnlockConsensusUpdate

### High Priority
2. Add metrics/alerting for low facilitator count
3. Consider self-preservation logic in ACK voting

### Medium Priority
4. Document facilitator rejoin process for operators
5. Add integration test for quorum loss scenario

---

## Appendix: Branch Contents

### Commits in `scas/all-postmortem-fixes`
- `0c7abd51` fix: copy keytool/wallet jars to nodes dir before key generation
- `cde01474` feat: expand cap on local clusters to 10
- `f3e8ff74` fix: comprehensive post-mortem action items

### Post-Mortem Items Addressed
- ✅ maxRoundDuration (5 min hard cap)
- ✅ Error recovery in consensus event loop
- ✅ Unlock metrics (dag_consensus_unlock_succeeded)
- ✅ Graduated timeout (+50% when >75% declared)
- ✅ SnapshotStorage info write safety
- ✅ Rollback prepend error handling

### New Issue Found
- 🐛 facilitatorCount=0 possible under quorum loss

---

## UPDATE: Recovery Testing (11:40)

### Single Node Restart
- **Result:** Does NOT recover
- State briefly shows `facilitatorCount=3` (loaded from snapshot)
- Then recomputes back to `facilitatorCount=0`
- The unlock decision is persisted

### Full Cluster Restart (all 8 nodes)
- **Result:** Does NOT recover
- Same behavior - state replays to `facilitatorCount=0`
- API stops responding
- **CLUSTER IS IRRECOVERABLE**

### Bug Severity Upgrade: CRITICAL → CATASTROPHIC

This is not just a stall - it's **permanent cluster death**:

| Recovery Attempt | Result |
|------------------|--------|
| Wait | ❌ No recovery |
| Restore network | ❌ No recovery |
| Restart single node | ❌ No recovery |
| Restart all nodes | ❌ No recovery |

The only recovery would be:
1. Manual state surgery, or
2. Rollback to pre-bug snapshot ordinal

### Operational Impact

If this bug occurs on mainnet:
- **Immediate:** Network halts, no new snapshots
- **Recovery:** Requires coordinated manual intervention across all validators
- **Downtime:** Hours to days depending on coordination speed

### MUST FIX BEFORE v4 RETRY
