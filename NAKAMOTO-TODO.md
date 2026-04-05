# Nakamoto Consensus — TODO

## Priority

### Production Abandonment on Better Gossip
When a node is mid-production and receives a gossip snapshot that would beat its in-progress snapshot via ChainSelection, it should abandon local production and adopt the received one instead of completing and creating a competing fork.

**Key constraint (James):** Don't abandon on *any* gossip — only if the incoming snapshot passes the threshold where fork choice would adopt it over what we're currently building. Effectively: run `ChainSelection.compare` against the in-progress tip, and if the gossip snapshot wins, cancel production.

**Implementation sketch:**
- SnapshotLeaderLoop.onSlotWon checks a `Deferred[F, Unit]` or `Signal[F, Boolean]` for cancellation
- NakamotoSyncDaemon, on receiving a snapshot that beats current tip, sets the signal
- SnapshotLeaderLoop checks the signal at key points (before signing, before publishing)
- If cancelled, skip publish + self-attestation, adopt the gossip snapshot instead

### Incremental MPT Undo Journal (Replace Full-Resync)
Current approach: self-healing via full-resync on fork boundary (O(state_size) per fork switch).
Target: undo journal per ordinal in MptStore's stateRef (O(delta_size × fork_depth) per switch).
See Bifrost EventSourcedState pattern. Needed for testnet scale (100MB state, 80k trie entries).

### Enforce Content Validation
Currently advisory (log warn, don't reject). Must become enforced per James.
Blocked on: MPT determinism fix (done ✅), genesis seeding for validator (done ✅).

### Genesis Time Discovery
Validator currently requires NAKAMOTO_GENESIS_TIME_MS env var. Should derive from chain
(e.g., slot certificate + known slot duration, or expose via peer API endpoint).

## Done
- [x] VRF crypto (PR #4)
- [x] Slot clock + LDD eligibility (PR #5)
- [x] StakeRegistry, EpochState, SlotCertificate, TipTracker (PR #6)
- [x] Go libp2p sidecar + GossipSub
- [x] NakamotoProposer + Attestation + Finality
- [x] Fork choice / ChainSelection (Bifrost-style density fallback)
- [x] ParentChildTree + reorg support
- [x] Self-healing incremental MPT (21cec6de)
- [x] /latest/info endpoint + RunNakamotoValidator (5a09af8f)
- [x] 4th node joins running cluster — validated
- [x] Chain-derived eta (replaces SharedEpochState accumulator)

## Future
- Stake-proportional VRF (replace equal-weight)
- Configurable finality mode (attestation for large clusters, depth-only for small)
- Content-addressed MPT (Ethereum-style, Option A — long-term)
- Disable BFT daemons in Nakamoto mode
- Orphaned state channel event recycling on finalize
- Clean up debug logging
