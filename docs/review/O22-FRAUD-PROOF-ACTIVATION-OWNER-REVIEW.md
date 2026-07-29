# O-22 Fraud-Proof Activation and Adjudication Owner Review

**Status:** OWNER-RATIFIED DIRECTION; FINAL LAUNCH IMPLEMENTATION OPEN. On
2026-07-29 the owner rejected the remove-now/re-add-in-a-later-era framing and
ratified a direct greenfield replacement: the sole ordinal-zero ScodecV1 launch
schema contains the final bounded InvalidStateProof V1 contract. There is no
pre-activation fraud-proof wire era and no compatibility decoder. Atomic
candidate semantics and per-new-signer, actual-debit-funded bounty ownership are
also ratified.

**Current runtime authority:** Unsafe and unqualified. The provisional
`fraudProofs` path remains source evidence only and must be disconnected while
the final launch path is built. That development interlock is not an on-chain
era, launch schema, or future activation mechanism.

**Primary gates:** O-03, O-11, O-16, O-17, O-18, O-20, WT-001..010,
O-23, SLASH-03..06, ECO-06, and MPT-05

**Updated:** 2026-07-29

## 1. Ratified decisions

The owner dispositions are:

1. `O22-01`: replace the provisional path once with the final bounded
   `InvalidStateProofEvidenceV1` collection in the sole ordinal-zero ScodecV1
   launch schema. Do not remove and later re-add it, introduce an activation
   ordinal, retain the provisional shape, or add compatibility.
2. `O22-02`: any invalid, not-upheld, stale, or unavailable proof rejects or
   defers the complete candidate, rather than being filtered while the candidate
   continues.
3. `O22-03`: each newly proven signer funds the winning claimant's bounty only
   from principal actually debited from that signer.

This does not alter the consensus architecture. GL0 remains a
Nakamoto/Taktikos/LDD chain. A fraud proof is exceptional evidence about one
already replay-certified sharded-CL1 checkpoint. It is not a proposal vote,
lock, quorum certificate, view change, fork-choice input, optimistic-finality
attestation, or substitute for normal execution.

## 2. Source-proven current state

| Surface | Current behavior | Consequence |
|---|---|---|
| Snapshot schema | `fraudProofs` is a canonical `GlobalIncrementalSnapshot` field and participates in its Scodec bytes (`modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala:134-142`; `modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotCodecs.scala:231-270`). | It is consensus data, not merely a sidecar message. |
| Proposal construction | A GL0 producer reads its node-local fraud pool and copies the result into the proposal (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala:869-877,1032-1037`). | Local staged work can enter the globally signed artifact. |
| Follower replay | Followers thread the producer-carried set through recreation (`GlobalSnapshotConsensusFunctions.scala:401-403,406-422`). | Artifact equality detects some divergent outcomes, but the validity function still depends on local adjudication inputs and policy. |
| Adjudication | GSAM maps every validator `Left` to an empty slash request and continues (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:2044-2077`). | Missing history, malformed evidence, a replay match, and a genuinely not-upheld claim are not represented by a consensus-safe result algebra. |
| Rooted effects | Locally upheld results can change stake/collateral, bounty/cooldown state, and field 34 (`GlobalSnapshotAcceptanceManager.scala:2647-2673,2959-2984`). | Fraud-proof handling already affects the MPT root and economics. |
| Active-era guard | `GlobalSnapshotActiveEraValidator` rejects only a populated historical `smtRoot` (`modules/shared/src/main/scala/io/constellationnetwork/validator/GlobalSnapshotActiveEraValidator.scala:10-30`). | Any nonempty `fraudProofs` shape passes the era gate today. |
| Policy | Watchtower enablement, slash/bounty fractions, and cooldown inputs are local configuration (`modules/node-shared/src/main/resources/application.conf:556-588`; `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala:702-707`). | Honest nodes with different local policy can derive different rooted results from one proposal. |
| Legacy JSON authority | The manual snapshot decoder consumes known fields but does not reject unknown keys (`GlobalIncrementalSnapshot.scala:153-228`), `Signed` delegates its nested value to that decoder (`security/signature/Signed.scala:62`), and live signing still hashes through `Hasher.forJson` (`security/Hasher.scala:111-126`). | This is a confirmed SER-005 cutover gap. Strict unknown-key rejection is useful only as a temporary development interlock; the launch fix is to remove authoritative JSON ingress and sign/verify complete ScodecV1 bytes. |

The current path is therefore not dark. It must not remain active while the
ratified final replacement is incomplete.

## 3. O22-01 final launch representation

This greenfield network has one launch representation:

- `GlobalIncrementalSnapshot` uses the sole ordinal-zero ScodecV1 schema.
- Its fraud-proof field is a bounded canonical collection of the final
  `InvalidStateProofEvidenceV1`, not the provisional
  `InvalidStateProofEvidence`.
- The evidence binds the final checkpoint body, namespace-confined diff/root,
  canonical distinct execution signatures, challenger and signature domain,
  exact historical Phase-2 qualification, historical committee/KES/VRF/eta and
  rooted policy, complete offense-time bond liability, and exact replay inputs
  or a mandatory-available bounded content-addressed bundle.
- The logical identity preserves claimant and newly covered signer distinctions.
  It cannot coalesce a later proof that establishes additional culpable signers.
- Count, byte, replay-work, historical-proof, and total economic-effect limits
  are fixed consensus parameters.

The provisional shape is not frozen. In particular, redundant untrusted
`attestedRoot`, `claimedDerivation`, `challengerDerivation`, and free-form
`reexecutionWitness` values do not become authority. Universal exceptional replay
derives the complete verdict.

There is no remove/re-add sequence, activation transaction, optional launch
feature, or fork-only compatibility decoder. While the replacement is under
construction, a source-level fail-closed interlock disconnects provisional pool
sourcing, rejects nonempty provisional evidence, prevents the provisional GSAM
sink, and prevents provisional field-34 records from affecting committee
selection. The interlock is deleted when the final launch implementation passes
qualification; it never appears in consensus bytes.

## 4. O22-02 launch verdict algebra

**Selected:** atomic candidate semantics. Every carried proof must
produce exactly one of these results:

| Result | Candidate behavior |
|---|---|
| `Upheld(newlyCulpableSigners)` | Apply effects only for those newly proven signers. |
| `EvidenceInvalid` | Reject the complete GL0 candidate with zero mutation. |
| `NotUpheld` | Reject the complete GL0 candidate with zero mutation. |
| `HistoryUnavailable` | Defer the complete candidate with zero mutation. |
| `NoNewlyCulpableSigner` | Reject as stale or duplicate evidence with zero mutation. |

There is no filter-invalid-and-continue path. A producer chooses whether to carry
the evidence; once carried, every validator must reach the same verdict from the
exact proposal-parent context.

`HistoryUnavailable` includes an unavailable exact replay base/input, historical
atomic KES+VRF registration, roster, eta, Phase-2 qualification, proposal-parent
slash state, or authenticated branch history. It never means no-slash acceptance
and can never slash an honest signer.

## 5. O22-03 signer coverage and bounty ownership

**Selected:** debit and reward per newly proven signer. The evidence and
deduplication identity is `(peerId, shardId, checkpointHash)`, not merely
`(shardId, checkpointHash)`.

- Each authenticated execution signature proves only that signer's culpability.
- A bounty is funded only from principal actually debited from a newly culpable
  signer.
- After evidence for `A/B/C`, later evidence for `A/D/E` can debit and reward
  only `D/E`.
- Duplicate `A` creates no debit, bounty, cooldown extension, or second record.
- Competing claims in one candidate use one frozen canonical evidence order. The
  first valid claimant causing a signer's actual debit receives the bounty from
  that debit.
- A zero-debit record/cooldown is permitted only under O23-05/O23-06 when a
  rooted consumed-bond tombstone proves prior debit or portable old-branch
  evidence proves the liable bond is genuinely absent after density replacement.
  Unexplained missing or divergent backing rejects/defer atomically and never
  creates a synthetic bounty.

Per-signer once-only accountability and debit conservation are mandatory. The
selected claimant-reward rule applies when evidence discovers signer subsets
incrementally.

Current checkpoint-wide behavior is insufficient: each committee signature
covers `ShardCheckpoint.signingPreimage`, but the signature collection itself is
excluded, so the same signed state claim can be carried with different signer
subsets (`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:77,87-107`),
evidence ordering coalesces signer-list variants by checkpoint
(`modules/shared/src/main/scala/io/constellationnetwork/schema/slashing/InvalidStateProofEvidence.scala:76-85`),
and both durable and same-ordinal deduplication can suppress remaining guilty
signers (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashedReader.scala:70-92`;
`GlobalSnapshotAcceptanceManager.scala:392-405`).

## 6. Rooted policy and economic gates

The following are exact proposal-parent protocol state, never local HOCON:

- protocol-era/schema/parameter identity;
- slash fraction, bounty fraction, cooldown, and roster-effect boundary;
- challenge and retention horizon;
- challenger bond and false-claim consequence;
- maximum proof count, bytes, replay work, and total economic effect.

Local configuration may control only publication attempts, retry/backoff,
logging, metrics, caches, dark-era local replay, and storage above a protocol
minimum. Local resource shortage causes defer/recovery, never no-slash
acceptance.

Launch qualification remains blocked until all of these are implemented and
tested:

1. Exact proposal-parent and historical adjudication, including Phase-2 lineage
   and density-reorg behavior.
2. Accountability for authentic signers on structurally invalid checkpoints
   (`SLASH-06`) and signer-specific deduplication (`SLASH-05`).
3. Actual bonded-principal debit conservation and a bounty no greater than the
   principal debited (`ECO-06`). O-23 separately freezes the offense-time bond
   identity, full-slash V1, pending/release horizon, and same-candidate order.
4. Frozen Scodec field-34 value/key grammar, accumulation, and physical
   key/value binding (`O-20`, `MPT-05`, `WT-010`).
5. Positive assigned-watchtower coverage, bonded/rate-limited challenges, and
   deterministic resource bounds (`O-03`).
6. Historical atomic KES+VRF/roster/eta verification and O-18 transport bounds.

The current slash manager receives delegated-stake and collateral records,
removes metadata, and derives bounty from their nominal total
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala:96-197`).
It does not prove that a corresponding locked principal was debited. Saturating
bounty credit does not repair that conservation gap.

## 7. Launch and reorg rule

1. New-chain ordinal zero commits the sole ScodecV1 schema and rooted policy.
   There is no protocol activation transaction or earlier target-chain era.
2. Every proof-bearing candidate is evaluated against its exact proposal parent.
   Local pool state, an ambient live head, and a receiver's current configuration
   are never inputs.
3. A Phase-2 density reorg rolls back the complete proof verdict, field-34
   record, exclusion, liability, debit, bounty/burn, balance, and supply delta.
4. Orphaned evidence must be fully revalidated against the replacement exact
   context. An old local upheld result is never grandfathered and an unavailable
   old base cannot slash.
5. O-20 defines the launch Scodec field-34 record; no fork-only JSON leaf,
   provisional evidence shape, or compatibility decoder is retained.
6. O-19 independently governs future upstream-v4 import. Source-chain
   statements are evidence, not target-chain slash authority.

## 8. Atomic implementation sequence

The direct final implementation is split into reviewable build slices, followed
by one consensus integration replacement:

1. **Development interlock.** Stop provisional pool sourcing, reject nonempty
   provisional evidence, reject rather than filter provisional adjudication
   failures, and disconnect provisional field-34 committee effects. Do not
   introduce a temporary target wire schema.
2. **Consensus-byte cutover.** Complete SER-005 with a Scodec hasher API that
   requires one audited `ConsensusHashSchema[A]` binding the exact byte-aligned
   codec, unique static domain, and frozen bound; migrate producer/verifier
   pairs, signatures, hashes, storage, GossipSub/ChainSync/HTTP consensus ingress,
   checkpoint preimages, and evidence preimages together. JSON is API/debug
   projection only.
3. **Final grammar.** Freeze the bounded final checkpoint/evidence, O-20 field-34,
   `BondId`, liability, hold/release, consumed-bond tombstone, and rooted-policy
   types and Scodec vectors. Allocate separate rooted O-23 economic partitions.
4. **Liability propagation.** Carry stable `BondId` through active and pending
   delegation/collateral state, enforce exact-amount lifecycle rules, and commit
   complete E-2 tranche liability.
5. **Portable adjudication.** Authenticate signers independently of ordinary
   structural checkpoint validity and return exactly `Upheld`,
   `EvidenceInvalid`, `NotUpheld`, `NoNewlyCulpableSigner`, or
   `HistoryUnavailable`.
6. **Atomic sink.** Preflight all evidence against one immutable proposal-parent
   capture, then apply all or none of locks, lifecycle maps, release indices,
   tombstones, field 34, exclusion, rewards, checked bounty/burn, balances, and
   supply.
7. **Replay and recovery.** Extend `StateChangesAccumulator`, change sets,
   follower replay, restart, catch-up, and density-reorg rollback with the same
   complete slash delta.
8. **Final integration replacement.** Replace the provisional field/path once
   with `InvalidStateProofEvidenceV1`, remove the development interlock, and pass
   the complete RED/model/multi-node qualification corpus.

No slice creates an on-chain dormant era. Producer and verifier migrations for a
given consensus object cannot be split, and dual JSON/Scodec signature
acceptance is forbidden. The isolated upstream-v4 reader belongs only in the
offline importer and is not current runtime compatibility.

## 9. Required tests

1. During implementation, the development interlock proves that a populated
   provisional pool cannot reach proposal, GSAM, MPT, overlay, storage, serving,
   finality, or committee selection. This test is deleted or converted to a
   source guard when the final path replaces it.
2. Final direct, wrapped, disk, HTTP, GossipSub, ChainSync, restart, catch-up,
   checkpoint, and ML0 envelope paths carry or retain exact bounded ScodecV1
   bytes. Old provisional shapes, unknown tags, trailing bytes, malformed
   refinements, duplicate identities, unsorted collections, and over-limit
   values reject. Upstream-v4 Kryo/Brotli fixtures are readable only by the
   offline importer.
3. Different local watchtower/slash/bounty/cooldown HOCON values produce
   identical launch artifacts and roots at shard counts 1, 2, and K.
4. Upheld, invalid, not-upheld, unavailable, and stale results exercise the exact
   verdict algebra. Asymmetric history/cache/restart yields universal defer,
   never slash versus no-slash.
5. Overlapping `A/B/C` then `A/D/E` evidence debits each signer once and rewards
   only actual new debit.
6. Backing locks, stake metadata, collateral, expiry/replacement indices,
   bounty, burn, and supply conserve atomically under same-ordinal operation
   permutations and near-maximum arithmetic.
7. Field-34 key/value mismatch, duplicate identity, reorg, restart, and
   change-set replay fail closed.
8. Phase-2 base/parent replacement invalidates proof-bearing descendants and all
   old-lineage local capabilities.
9. Local QoS variation never changes artifact validity or rooted output.

Existing W3a component tests use identical stub replay and policy across nodes.
They remain useful component tests, but do not prove launch safety.

The focused legacy/interlock oracle
`O22FraudProofPreActivationContainmentRedSuite` compiles and fails all four
legacy/interlock gates with zero errors: active producer/GSAM authority,
permissive legacy JSON member handling, acceptance of the provisional Scodec
shape, and provisional field-34 cooldown influence on committee selection. It
does not specify the final launch schema. Its SHA-256 is
`e21fe7bc5f3a98ebd1c97a0ad7d9384fc0397d5b1440be91e0b613a31dfd5cdc`.

## 10. Owner disposition

- `O22-01`: final ordinal-zero ScodecV1 launch schema; no remove/re-add era,
  activation ordinal, provisional-shape retention, or compatibility decoder.
- `O22-02`: accepted atomic candidate semantics.
- `O22-03`: accepted per-new-signer, actual-debit-funded bounty ownership.

These answers select design only. They do not close the listed engineering,
schema, economics, resource, recovery, or adversarial-test gates. Fraud proofs
become launch-authoritative only when the final implementation passes them.
