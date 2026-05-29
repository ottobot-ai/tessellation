# gl1 Inclusion-Proof Follow (Axis 2)

**Status:** design, 2026-05-28. Owner: orchestrator. Replaces gl1's full re-execution of gl0 global snapshots with proof-verified state.

## Locked decisions (2026-05-28) — these supersede any earlier prose below

1. **gl1 holds ONLY its slice** — the five consumed-field subtrees (`Balances`, `LastTxRefs`, `LastAllowSpendRefs`, `LastTokenLockRefs`, `ActiveTokenLocks`). Bootstrap **downloads only that slice**, not the full GSI; gl1 no longer maintains a full GSI/MPT. Global ordinal/epoch context comes free from the signed snapshot header.
   - **Slice = exactly the GSI fields gl1's validators read (enumeration method, not a guess).** The set is derived by sweeping every field of the followed `GlobalSnapshotInfo` that gl1's runtime DAG-tx / allow-spend / token-lock submission + acceptance path reads (`ContextualTransactionValidator`, `ContextualAllowSpendValidator`, `ContextualTokenLockValidator`, `TransactionService`, `AllowSpendService`, `TokenLockService`, `Collateral`/`CollateralDaemon`, and the `consumedFieldsToGlobalSnapshotInfo` consumer in `DAGSnapshotProcessor`). `activeTokenLocks` is in-slice because it is read by the token-lock-replacement validator (`ContextualTokenLockValidator.validateReplaceTokenLockRef` / `getBalanceAffectedByTxs`, fed from `TokenLockService.getActiveTokenLocks`); omitting it left gl1's mirror permanently empty so every token-lock replacement failed `NothingToReplace`. Re-run the sweep before adding/removing a consumed field so the set never silently drifts.
   - **Explicitly out-of-slice: the two-part-key active-sets** `activeAllowSpends` (`hypergraph(_, Option[Address], Address)`) and `tokenLockBalances` (`hypergraph(_, tokenAddr, holder)`). These are read only by the currency-scoped `SpendActionValidator` (currency-l0/l1 SpendAction path), are NOT single-`Address`-keyed, and are NOT uniform extensions of this Address-keyed slice. The gl1 DAG path does not read them; admitting one would need a design decision, not a mechanical add.
   - **Transfer model (S2a finding):** the `MptOverlay` can't read value-bytes at a historical ordinal (`allEntriesAsBytes` returns the branch tip, ignoring the ordinal), so per-ordinal historical-diff deltas aren't available from the overlay. So gl1 fetches the **latest-finalized full slice** and recompute-matches it — gl1 only needs the latest finalized state for tx validation, no per-ordinal replay. O(slice) per poll now; **accept-time delta-capture** (from GSAM's `StateChangesAccumulator`) for O(changes) transfer is the tracked optimization (#287), and doesn't change the verify path.
2. **Completeness via recompute-and-match (field-root equality)** — gl1 applies gl0's claimed slice deltas to its slice subtrees, recomputes each field's MPT subtree root, and asserts `== signedSnapshot.stateProof.<field>Proof`. Root equality ⟺ identical content — correct-by-design. gl1 does **not** use range/inclusion proofs.
3. **Prover extension (genuine range completeness + absence proofs) = deferred (#286)** — only the *stateless* consumers (light clients, cross-shard *set* queries) need it; the range verifier is unsound for sets today. Cross-shard *point* reads use single-key inclusion (complete by itself).
4. **Cross-shard state = explicit proof-carrying txs** (extend #274/#275); a metagraph follows only its own slice, never another's live state.
5. **One trust model, two access patterns**: holders recompute-and-match the signed Merkle roots; stateless verifiers prove against the same roots.

## Problem

gl1 (DAG-L1) follows gl0's finalized global snapshots. Today it **fully re-executes** each one via `GlobalSnapshotContextFunctions.createContext → GSAM.accept`, re-deriving the entire global MPT, and raises `StateProofMismatch` when its re-derivation disagrees with gl0's claimed `mptRoot`. This is the source of a recurring follower-divergence bug class (#54, #70, #113, #115, and the 2026-05-28 gl1-formation blocker: gl1 re-derives `historicalStakeSnapshots` — a gl0 leader-election field **it never reads** — diverges on the carry-forward at the ordinal after an eta boundary, and storms). gl1 is brought down by failing to re-derive state it doesn't use.

## What gl1 actually consumes (the whole surface)

From the grounded map — gl1 reads, for DAG-tx / allow-spend / token-lock validation, only:
- `balances` (by `Address`) — `TransactionService`, `AllowSpendService`, `TokenLockService`, and `Collateral`/`CollateralDaemon` via `getLatestBalances`
- `lastTxRefs` (by `Address`) — via `replaceByRefs`/`advanceMajorityRefs`, source `SnapshotProcessor.extractMajorityTxRefs`
- `lastAllowSpendRefs`, `lastTokenLockRefs` (by `Address`) — `DAGSnapshotProcessor` (`allowSpendStorage.initByRefs` / `tokenLockStorage.initByRefs`)
- `activeTokenLocks` (by `Address`, value `SortedSet[Signed[TokenLock]]`) — `TokenLockService.getActiveTokenLocks`, fed to `ContextualTokenLockValidator.validateReplaceTokenLockRef` / `getBalanceAffectedByTxs` for token-lock replacement

All read from the in-memory `GlobalSnapshotInfo` in `LastNGlobalSnapshotStorage.combinedSnapshotsR`. It uses **none** of the other ~12 GSI fields (incl. the two-part-key `activeAllowSpends` / `tokenLockBalances`, which are SpendActionValidator/currency-scoped — see Locked decision 1). It uses no `GlobalStateReader`/`OverlayReader`.

## Model: partial verified mirror + pushed range proofs

gl1 stops re-deriving. It maintains a **verified mirror of only the consumed fields**, kept current each finalized ordinal by gl0-pushed proofs, and reads it locally (no per-tx gl0 round-trip).

```
gl0 (prover)                          wire (lastN sync)              gl1 (verify + apply)
─────────────                         ──────────────────              ───────────────────
for finalized ordinal N:              GlobalFollowProof {             verify against attestedRoot =
  for each consumed field:              ordinal: N                      signedSnapshot.stateProof.mptRoot:
    range proof over field's            committedRoot: Hash           1. committedRoot == attestedRoot
    full key-range vs mptRoot(N)        fields: Map[FieldId,          2. each field's range proof valid vs root
    (incl exclusion boundaries)                  RangeProof+values]   3. hash(value)==leaf.dataDigest (bound)
                                      }                               4. assemble Verified[ConsumedFieldState]
                                                                      5. apply to local mirror; reads stay local
```

`createContext` re-execution on the gl1 path is **deleted**.

## Correct-by-design contract (the bar — see [[feedback_correct_by_design_not_implementation]])

1. **`Verified[A]` gate.** `sealed abstract case class Verified[A] private (value: A)` — constructed *only* by the verifier. gl1's validator signature consumes `Verified[ConsumedFieldState]`. There is no path to use unproven state; the re-derivation path is removed, not bypassed.
2. **Value-binding inside verify.** Verify takes `(attestedRoot, key, value, proof)` and checks `Hasher.hash(value) == leaf.dataDigest` internally — not caller-optional. (Today's `MerklePatriciaInclusionVerifier.confirm(root, proof)` omits this; we wrap it so the value is mandatory.)
3. **Completeness + absence are cryptographic.** Consumed fields proven with **`MerklePatriciaRangeProof`** (inclusion + exclusion boundaries) over each field's full key-range vs `mptRoot`. A queried address has either a proven leaf or a proven gap ⇒ provably `Balance.empty`. No omitted update can hide; no trusted default.
4. **Sealed ADTs, no string control flow.** `FollowVerificationError` = `CommittedRootMismatch | RangeProofInvalid(field, cause) | ValueBindingFailed(key)`; sync-response variants sealed; exhaustive match; never `getMessage`/string branching.
5. **Trusted root = consensus output.** Verify against `signedArtifact.stateProof.mptRoot` from the signed, finality-gated snapshot. Today backed by signer-sig + majority-hash + finalized watermark; hardening with `slotCertificate`/attestation verification on the follower path is a tracked follow-up (followers don't check it yet).

## Types (target)

```scala
final case class GlobalFollowProof(
  ordinal: SnapshotOrdinal,
  committedRoot: Hash,                                  // must equal snapshot.stateProof.mptRoot
  fields: SortedMap[GlobalStateFieldId, MerklePatriciaRangeProof]   // range proof carries leaves+values+boundaries
)
sealed trait FollowVerificationError
// CommittedRootMismatch(expected, got) | RangeProofInvalid(field, MerklePatriciaVerificationError) | ValueBindingFailed(GlobalStateKey)
final class ConsumedFieldState private (val balances: SortedMap[Address, Balance], val lastTxRefs: …, …)
sealed abstract case class Verified[A] private (value: A)
def verifyConsumedFields[F[_]: Async: Hasher](attestedRoot: Hash, proof: GlobalFollowProof): F[Either[FollowVerificationError, Verified[ConsumedFieldState]]]
```

## Reuses

- `HistoricalMptProofService.proofAtBranch` pattern (`overlay.buildRoot(branch, ord) → trie → prover`) for gl0-side proof generation.
- `MerklePatriciaRangeProver` / `MerklePatriciaRangeVerifier` (inclusion + exclusion boundaries).
- `ShardSubtreeProofService` + `ShardProofRoutes` as the prove→serve→verify template (the fetch-client half is a stub today — net-new).

## Slicing (delegated; orchestrator reviews each; bug clears at S3–S4)

- **S1 (additive, safe):** gl0-side `GlobalFollowProofService` (range proofs over consumed fields vs `mptRoot`) + shared correct-by-design verify core (`Verified`, value-binding, ADT errors) + tests: round-trip, value-tamper→`ValueBindingFailed`, omitted-leaf→range-verify-fail, absence-proven. No wiring, no removal.
- **S2:** wire `GlobalFollowProof` into the lastN sync (payload + HTTP route mirroring `ShardProofRoutes` + the real fetch client — currently a stub).
- **S3:** gl1 verify+apply into `GlobalSnapshotAlignment`/`DAGSnapshotProcessor`, maintaining the verified partial mirror; tx-validation reads consume `Verified[ConsumedFieldState]`.
- **S4:** delete `createContext` re-execution + `StateProofMismatch` raise/recovery on the gl1 path; stop full GSI/MPT maintenance on gl1.
- **S5:** parity test (verified-apply mirror == legacy re-execution mirror for the consumed fields over a snapshot sequence) + 3gl0+2mg e2e (gl1 forms, workflows run); then 8gl0+4mg+4shards.

## Open / deferred

- Efficiency: S1 proves each field's **full** range per ordinal; a delta-scoped range (re-prove only changed sub-ranges + a subtree-root commitment) is a later optimization. v1 = correct-by-design first.
- Follower-side `slotCertificate`/attestation verification (harden the trusted-root basis).
- **Generalization to metagraph followers (the real scalability payoff).** The verify core must be built **generic** — `verify((key→value) set for a namespace/subtree, attestedRoot) → Verified[Slice]` — so gl1's `ConsumedFieldState` is one instantiation and a metagraph follower (cl1/dl1/ml0) is the second. A metagraph's relevant slice is a well-defined MPT subtree: its `Metagraph(addr)` namespace + the `Address(addr)` entries for its participants + a little global context (epoch/ordinal/pricing) + cross-shard receipts it consumes. The per-metagraph subtree prover **already exists** — `ShardSubtreeProofService.generateProofForMetagraph(shardId, metagraphAddr, key)` (built for the sharding path) — so metagraph follow reuses the gl1 fetch+verify half over a metagraph-namespace subtree proof. Wins: (a) kills the same follower-divergence class on cl1/dl1/ml0 (#54/#70/#113/#210), (b) a metagraph's verify cost becomes proportional to its OWN footprint, not global-state size (the actual point of execution sharding), (c) cross-shard reads become "verify B's state via inclusion proof vs gl0's root, don't re-execute B" — the cross-shard DA story for free. Sequence: prove the pipeline on gl1 first (simplest consumer, 5 consumed fields), then map cl1/dl1/ml0's follow path with the same rigor and generalize. The same proofs serve external light clients (metakit-sdk TS inclusion verifier) as a third consumer of one primitive.
- **Cross-metagraph state — explicit message-passing (user direction 2026-05-28, refined).** A metagraph never *reads* another's live state; it follows only its OWN slice (field-root match) and *consumes* explicit proof-carrying inputs. Cross-shard dependency = a transaction carrying `(externalValue + inclusion proof vs gl0's finalized root)`; the validator verifies the proof at admission; the manager writes the value into local state keyed by `(sourceMetagraph, key, sourceOrdinal)`; the transition reads it locally ⇒ self-contained, deterministic, correct-by-design (can't depend on un-referenced external state). Verification rides the PROOF (checked against the gl0 root the metagraph already follows), not on holding the other shard's state — mg-A's committee (gl0 operators) can also check gl0 directly. Follows the owner/stake/node-collateral tx trail (schema→manager→MPT partition→validator→route→gossip) and extends the existing `CrossShardReceipt` + cross-shard `SpendActionValidator` scaffold (#274/#275). This **subsumes the earlier "subscribe later / all-get-all now"** framing — strictly more scalable + more correct-by-design than mirroring everyone. Relaying (who submits the tx) = network-layer/incentive concern (IBC-relayer-style, can be permissionless), NOT protocol state. Tradeoffs: staleness = value as-of-injection at a named ordinal (correct for finalized-fact references; relayer re-injects for liveness); arbitrary live cross-mg reads remain an anti-pattern. This is **one trust model, two access patterns** (not two mechanisms): everything verifies against the same signed Merkle roots in `stateProof`; a node that **holds** the subtree (gl1, a metagraph's own slice) recomputes its root and checks equality (`recomputedRoot == signedRoot ⟺ identical content` — a commitment match, correct-by-design, no proof shipped); a node that **holds nothing** (light client, cross-shard *set* query) verifies a proof against the same root. Forcing a holder to verify proofs for data it already has = over-engineering. Cross-shard *point* reads use single-key inclusion (complete by itself). TODO: confirm #274/#275 exact shape before building the cross-shard tx.
- **Design constraint for S1 review:** ensure the proof-verify + `Verified` + value-binding core is generic over the verified payload type (not coupled to `ConsumedFieldState`) — so gl1 (Phase 0), metagraph followers (Phase 1, full `Metagraph(*)`), and subscribe-narrowed followers (Phase 2) are all instantiations of one core.
