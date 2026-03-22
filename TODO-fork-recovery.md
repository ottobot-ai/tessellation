# Fork Recovery TODOs

Branch: scas/event-mempool-fork-recovery

## Open Issues

### 1. Healthy nodes trigger unnecessary recovery download during quorum stall
**Severity:** Medium — recovered cleanly but wasteful
**Found in:** Kill4 test (8N, 4 isolated)
**Symptom:** gl0-0 and gl0-3 (never isolated) hit 5 consecutive `QUORUM_INFEASIBLE` abandonments → triggered recovery download against their own healthy peers.
**Root cause:** AbandonmentTracker counts all abandonments equally. `QUORUM_INFEASIBLE` rounds (where the node's data is fine, it just can't form a quorum) count the same as rounds where the node is actually stuck/behind.
**Impact:** Unnecessary recovery download on healthy nodes, ~4 min delay before they rejoin.
**Possible fixes:**
  - Don't count `QUORUM_INFEASIBLE` abandonments toward recovery threshold
  - Separate counter for quorum-infeasible vs data-stale abandonments
  - Check if local ordinal matches peers before triggering recovery (if peers are at same ordinal, no recovery needed)
  - Reset abandonment counter when quorum is infeasible but network is healthy

### 2. Fork detection / chain tip sampling not firing
**Severity:** Low — abandonment tracker handles recovery, but fork detector is dead weight
**Found in:** Both kill3 and kill4 tests
**Symptom:** Zero fork detection logs, zero chain tip sampling logs. Recovery was entirely through abandonment tracker.
**Root cause:** Needs investigation. Possible causes:
  - `maybeForkRecoveryDetector` is None (not wired up correctly in Main.scala)
  - Chain tip sampling code in `runHeartbeat` not reached
  - `getLocalChainTip` returning None during stall
  - heartbeat interval too long relative to recovery timing
**Action:** Add explicit logging at entry point of chain tip sampling to confirm it runs. Verify `maybeForkRecoveryDetector` is Some in EventGossipDaemon.
