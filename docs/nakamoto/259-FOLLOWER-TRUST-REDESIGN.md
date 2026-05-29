# #259 redesign — metagraph followers trust finalized gl0 root + inclusion-prove reads

**Status:** implementation in progress (slices 1–2 built on worktree branches, slice 3 gated for review).
**Date:** 2026-05-27. Supersedes the band-aid options in [259-HISTORICALSTAKE-VERIFIER-DIVERGENCE.md](259-HISTORICALSTAKE-VERIFIER-DIVERGENCE.md).

## ⚠️ DIAGNOSIS CORRECTED 2026-05-27 — the prior model below was INVERTED
Live forensics on the running 8gl0+4mg cluster (run `numshards4-etafix-noargmax-v2`, eta-fix in place) show the **opposite** of what this doc originally claimed:
- The SPM diff is `historicalStakeSnapshots(c=9515a584ab4f,l=000000000000)` where `perFieldRootDiffs(computed, claimed)` ⇒ **`c=` is the METAGRAPH's recompute (POPULATED, correct) and `l=` is GL0's claimed proof (EMPTY, `Some(Hash.empty)`)**. (GlobalSnapshotContextFunctions.scala:299,449.)
- gl0's **GSI is populated** — `/global-snapshots/latest/combined` shows `historicalStakeSnapshots = {7,8,9,10}` (correct retention window). gl0's **data is right; only its committed stateProof sub-trie root is empty.**
- All 8 gl0 nodes agree with each other (0 internal SPM). R=100 on every container (no R-asymmetry). The metagraph is at ord 1117 vs gl0 1118 — **near lockstep, NOT throttled 0.2x** (so the "throttle → epoch never reaches unlockEpoch" story is also wrong; the metagraph recovers each SPM fast).
- ∴ This is a **gl0 PRODUCER-side proof-construction bug** (GSI populated, MPT/proof sub-trie empty), *not* a follower-can't-reproduce-leader-state problem. The eta-adopt fix (`19502c3a3`) targeted the wrong thing (it changes *which eta*, not populated-vs-empty). The follower-trust redesign below is **not required for #259** — the right fix is to make gl0's producer commit a proof that reflects its GSI; the metagraph already computes the correct populated value and will then match by construction. (Producer root-cause + minimal-fix investigation dispatched — agent `aa6561a7`.)
- The follower-trust + inclusion-proof redesign may still be worth pursuing later as an architectural simplification (it removes a redundant recompute), but it is **decoupled from unblocking 8/4/4** now.

## Problem (one line) — ORIGINAL, SUPERSEDED, kept for history
Metagraph followers (ml0/cl1/dl1) re-run `GlobalSnapshotAcceptanceManager.accept` via `GlobalSnapshotContextFunctions.createContext` to reconstruct gl0's full GSI **and** recompute+compare its `mptRoot`. That forces them to reproduce `historicalStakeSnapshots` — gl0 leader-election state they structurally cannot reproduce (no gl0 VRF-output chain) — which diverges (empty `c=000000`), trips `StateProofMismatch` every boundary → `recoverFromOrphan` loop → metagraph throttled ~0.2x → token-lock-expiration fails (#259). Six in-paradigm patches failed (R-config, eta-adopt, …); the eta-adopt can't work because only the `eta` Hash crosses the wire, not the full field value.

## Decisive grounding (why this is low-risk)
Followers **already** trust a gl0-served full GSI at bootstrap/recovery: `GlobalL0Service.getLatest` returns `(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)`, validated with **only** majority-ordinal + majority-hash checks — **no state-proof recompute** — and `mptStore.syncFromGlobalSnapshotInfo` rebuilds the whole local store from it. The trust model is already in production. #259 is caused *only* by the incremental **forward** path re-deriving instead of trusting. This redesign removes a redundant recompute; it does not add a new trust surface.

## Design
On the **follower** verify-replay path (NOT gl0 producers):
1. **Trust the finalized claimed root.** Stop recomputing the global `mptRoot` for comparison; trust `signedArtifact.stateProof.mptRoot` (the snapshot is gl0-majority-attested + phase-2 finalized; followers cast zero gl0 attestation votes).
2. **Cross-check only consumed fields.** Verify the per-field proofs already in `GlobalSnapshotStateProof` (`balancesProof`, `lastAllowSpendRefs`, …) for the partitions the follower actually reads (`followerConsumedFieldIds`: balances, lastTxRefs, lastStateChannelSnapshotHashes, active allow-spends/token-locks, tokenLockBalances, last allow-spend/token-lock refs, lastCurrencySnapshots family, metagraphSyncData) against the follower's locally-rebuilt sub-trie roots. A mismatch on a *consumed* field still raises (real divergence). `historicalStakeSnapshots` (and other gl0 leader-election / SystemNamespace partitions) are **not** in the follower's obligation → no more empty-field mismatch.
3. **Keep the local store via the existing delta-apply** (`syncFromStateChanges`) — the GSI value the storage interfaces need is still maintained; only the *root comparison* changes.
4. **Cold-cache reads** for a key absent locally: fetch `{value, proof}` from a gl0 peer via a new route, verify against the trusted finalized root, cache. Steady-state = zero proof round-trips (local reads).

## Resolved decisions
- **No full recompute; split the root obligation** — trust the top-level root, cross-check only consumed per-field roots.
- **Read source = local MPT store** (delta-applied), remote fetch is cold-cache fallback only. Perf: removes the ~800K-entry per-ordinal full-GSI re-encode.
- **New gl0 route `GlobalStateProofRoutes`**, anchored at the **finalized** snapshot's `mptRoot` (a finalized snapshot can't be orphaned → no proof staleness). Don't overload `ShardProofRoutes` (different anchor/ownership).
- **Migration: era-gate via typed HOCON** (`FieldsAddedOrdinals.followerTrustFinalizedRoot`, no `sys.env`). Below the gate = current recompute (byte-identical); at/after = trust+prove. Greenfield, but gated for safe A/B + rollback.
- **No A4 hard fork** — changes follower verification only, not what gl0 commits. Orthogonal twin of A4 (which is gl0-stops-re-executing-metagraphs).
- **Complementary to ActiveAddressIndex `353dcabfb`** — that's a gl0-vs-gl0 sidecar-root divergence; still needed on the gl0 side, doesn't conflict.

## Slices
1. **`TrustedGlobalStateReader` + `followerConsumedFieldIds`** (node-shared, pure add) — ✅ built (`4421bd506`, worktree). `verifyAgainst` binds the proof to the key (`proof.path === toHex(key)`).
2. **gl0 `GlobalStateProofRoutes` + `GlobalStateProofService` + `GlobalStateProofClient` + `GlobalStateInclusionProof`** (dag-l0 + node-shared, pure add, finalized-anchored via a `ResolveFinalizedAnchor` callback → `FinalityGate.finalizedOrdinal`) — ✅ built (`3aef072ac`, worktree).
3. **Gated follower trust-branch in `createContext`** (the core, consensus-critical) — ⏸ prep + bring for explicit sign-off before enabling. Removes the `19502c3a3` eta-adopt workaround.
4. **Cold-cache fallback wiring** (followers, gated). ⚠️ **Blocker found in slice 2:** the `GlobalStateKey` Circe codec is **lossy for address-keyed keys** — `PartitionNamespace`'s decoder does `Address.fromBytes(s.getBytes)` (re-hashes the encoded address string rather than recovering it), so `AddressNamespace`/`MetagraphNamespace` keys (incl. **balances**) don't survive a JSON round-trip; `Hash`/`Hypergraph`/`Empty` namespaces round-trip cleanly. Remote balance proof-fetch needs a codec fix (or a restricted wire-key shape) first. The slice-3 core (per-field-proof cross-check + local reads) and steady-state reads are unaffected — this only gates remote *cold-cache* fetches of address-keyed entries.
5. **Flip the gate (test env ordinal 0) + 8gl0+4mg+4shards e2e.**
6. **(Independent) Re-land ActiveAddressIndex `353dcabfb`** on the gl0 side.

## Risks
- *Loss of follower independent global-root recompute* — acceptable: still verifies every consumed field; trusts only finalized (phase-2) snapshots; already the production bootstrap/recovery model. Net trust surface is reduced.
- *Performance* — net positive (removes the recompute); only new latency is cold-cache proof fetch (rare in steady state; fan-out + reject-on-timeout).
- *Two-path drift during the gated window* — mitigated by a below-gate parity suite + reusing the same `accept()` write path (only the root-comparison differs).
- *Per-field coverage edge* — `lastCurrencySnapshots` is a Merkle tree not an MPT sub-trie; `mptRoot` is `None` for `LegacyFormat` ordinals (no-op the comparison there, as today).

## Test plan
Per-slice units + a below-gate parity suite + a property test (follower per-field roots == gl0 claimed proofs over `followerConsumedFieldIds`) + the 8gl0+4mg+4shards e2e (expect metagraph `StateProofMismatch`→0, no throttle, token-lock expiration passes; gl0 self-validation unchanged).

## Critical files
`GlobalSnapshotContextFunctions.scala` (the recompute+compare; add gated trust-branch — divergence at `:296`, eta-workaround at `:239`), `domain/nakamoto/overlay/AcceptanceMpt.scala` (slice-1 primitive), `domain/nakamoto/sharding/ShardSubtreeProofService.scala` + `HistoricalMptProofService.scala` (proof template/generation), `dag-l0/http/routes/ShardProofRoutes.scala` (route template), `config/types.scala` (era-gate), follower call sites `currency-l0/StateChannel.scala` · `currency-l1/CurrencySnapshotProcessor.scala` · `dag-l1/DAGSnapshotProcessor.scala`.

See [[project-s1-s6-validated-259-blocker]] (memory) for the full saga + the argmax-revert + the eta-adopt failure that drove this pivot.
