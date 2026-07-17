# O-22 Fraud-Proof Activation and Adjudication Owner Review

**Status:** OWNER RESPONSE REQUIRED. The current implementation gives nonempty
`fraudProofs` live proposal and rooted economic authority before portable
adjudication has been established. This packet does not ratify that behavior.

**Runtime authority after containment:** None. Transport, local replay, bounded
pooling, and RED tests may remain dark, but no current-era fraud proof may enter a
GL0 proposal or change canonical state.

**Primary gates:** O-03, O-11, O-16, O-17, O-18, O-20, WT-001..010,
O-23, SLASH-03..06, ECO-06, and MPT-05

**Updated:** 2026-07-16

## 1. Decision required

O-22 asks three bounded questions:

1. Before activation, should the current snapshot schema retain a
   mandatory-empty `fraudProofs` field, or remove the field until a later
   protocol era?
2. After activation, does any invalid, not-upheld, stale, or unavailable proof
   reject/defer the complete candidate, rather than being filtered while the
   candidate continues?
3. When later evidence proves additional signers on an already-disputed
   checkpoint, which claimant receives the bounty from each newly debited
   signer?

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
| Retired JSON member | The manual snapshot decoder consumes known fields but does not reject unknown keys (`GlobalIncrementalSnapshot.scala:153-228`), and `Signed` delegates its nested value to that decoder (`security/signature/Signed.scala:56`). | After field removal, an attacker can add a retired `fraudProofs` member to a correctly signed new-shape JSON artifact. Circe can discard the key, then signature verification hashes the clean decoded value and succeeds unless key presence is rejected explicitly. |

The current path is therefore not dark. It must not remain active while this
decision is pending.

## 3. O22-01 pre-activation representation

### Option A - mandatory-empty current-era field

Retain the current ScodecV1 field temporarily, but make the
active-era validator reject every nonempty value. Honest construction always
emits `SortedSet.empty`, independently of local pool contents or HOCON. This is
the same containment pattern already used for dark `smtRoot` activation.

Local transport, emitter, replay, and pool code may continue for bounded testing
and diagnostics. Their outputs cannot enter a GL0 proposal, change an MPT root,
create a portable upheld capability, or be grandfathered into a future era.

### Option B - remove the field until an activating era

**Recommendation.** Delete `fraudProofs` from the current snapshot schema and add a versioned field
only in the protocol era that activates the complete adjudication contract. This
requires no compatibility preservation for this greenfield fork and avoids
freezing an evidence collection whose per-signer identity, ordering, resource
bounds, and field-34 relationship are not yet ratified. It causes broader codec
and constructor churn now, but follows the project's rule against retaining
undeployed compatibility or abandoned authority shapes.

The containment is end-to-end, not a case-class-only deletion. Current producer,
follower, and GSAM acceptance lose the proof input, slash fold, bounty/burn path,
and field-34 writer. Current committee selection must not consume pre-activation
field-34 records as cooldown authority. Local transport/pool/replay components may
remain only behind an explicitly dark boundary. O-20 owns the future typed field
and the disposition of fork-only test data; no existing JSON leaf or interpolated
key is grandfathered into that era.

The current JSON decoder must explicitly reject the retired key at every nesting
level; merely deleting the case-class field is permissive because Circe ignores
unconsumed members. The guard covers direct snapshots, `Signed.value`, combined
checkpoint files, ML0 global-state wrappers, HTTP responses, and GossipSub/
ChainSync payloads. Scodec decoding remains complete and rejects the retired
trailing field. The existing Kryo fallback reads only upstream
`GlobalIncrementalSnapshotV1`; it is retained for prior upstream-v4 disk access
and is not a compatibility decoder for this fork's retired 27-field shape.

Accepting nonempty proofs as inert is not a valid third option: it signs bytes
whose future interpretation is ambiguous. Retaining the current conditional
slash path is also not valid.

## 4. O22-02 activated verdict algebra

**Recommendation:** accept atomic candidate semantics. Every carried proof must
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

**Recommendation:** debit and reward per newly proven signer. The evidence and
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
owner choice is the claimant-reward rule when evidence discovers signer subsets
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

- activation status and protocol-era/schema identity;
- slash fraction, bounty fraction, cooldown, and roster-effect boundary;
- challenge and retention horizon;
- challenger bond and false-claim consequence;
- maximum proof count, bytes, replay work, and total economic effect.

Local configuration may control only publication attempts, retry/backoff,
logging, metrics, caches, dark-era local replay, and storage above a protocol
minimum. Local resource shortage causes defer/recovery, never no-slash
acceptance.

Activation remains blocked until all of these are implemented and tested:

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

## 7. Activation and reorg rule

1. Current-era snapshots require an empty field under Option A, or have no field
   under Option B.
2. The activating protocol transition commits the complete policy/schema and
   cannot itself carry fraud proofs.
3. The first proof-bearing snapshot is a child whose exact proposal parent
   already contains the active policy.
4. A density reorg removing the activation parent restores pre-activation
   semantics.
5. Dark pool entries and local upheld results are discarded or fully
   revalidated against the new exact context. They are never grandfathered.
6. Pre-activation nonempty artifacts remain invalid forever.
7. O-20 selects the target Scodec record; no fork-only JSON compatibility leaf
   is retained.
8. O-19 independently governs any future upstream-v4 import. Source-chain
   statements are evidence, not target-chain slash authority.

## 8. Atomic containment sequence

Under recommended Option B, the active removal is one atomic consensus commit:

1. remove `fraudProofs` from the current snapshot case class, strict JSON
   decoder, and complete Scodec shape;
2. disconnect local pool input and carried-proof threading from producer,
   follower recreation, and context reconstruction;
3. remove proof/config/validator inputs and the slash/bounty/field-34 sink from
   GSAM while preserving the ordinary no-proof economic result exactly;
4. remove production field-34 cooldown consumption and hard-bind current-era
   committee construction to no exclusion;
5. retain bounded transport, replay, and pool components only as an explicitly
   dark diagnostic path; and
6. update every direct, wrapped, disk, HTTP, GossipSub, ChainSync, restart, and
   catch-up decoder/test in the same change.

These cannot be split across runtime commits. A schema/codec-only change leaves
hidden state authority or mixed bytes, while a sink-only change signs a retired
field with undefined future meaning. Mixed old/new fork nodes are intentionally
incompatible; this greenfield branch restarts from the new current schema rather
than adding a dual decoder. The isolated upstream-v4 Kryo reader remains an
import/disk-read path, not current consensus compatibility.

## 9. Required tests

1. Under Option A, the current-era field is present-empty, every nonempty value
   rejects before GSAM/MPT/overlay/finality mutation, and a populated local pool
   cannot change the emitted empty value. Under recommended Option B, the field
   is absent, the producer never consults/copies the pool, and strict JSON/other
   object decoders reject an explicitly supplied retired `fraudProofs` member
   when empty, populated, `null`, or malformed, including inside `Signed.value`;
   Scodec rejects every old-shape or trailing-field byte vector.
2. Brotli snapshot storage, combined-checkpoint storage, ML0 global-state
   wrappers, direct/streaming HTTP download, GossipSub, ChainSync, traversal,
   serving, restart, and catch-up enforce the same era boundary. Legitimate
   upstream `GlobalIncrementalSnapshotV1` Kryo disk promotion remains readable,
   while no fork 27-field fallback exists.
3. Different local watchtower/slash/bounty/cooldown HOCON values produce
   identical current-era artifacts and roots at shard counts 1, 2, and K.
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
8. Activation-parent replacement invalidates proof-bearing descendants and all
   old-lineage local capabilities.
9. Local QoS variation never changes artifact validity or rooted output.

Existing W3a component tests use identical stub replay and policy across nodes.
They remain useful dark/future-era tests, but do not prove activation safety.

The focused pre-activation oracle
`O22FraudProofPreActivationContainmentRedSuite` compiles and fails all four
intended gates with zero errors: active producer/GSAM authority, acceptance of a
retired JSON member, acceptance of the pinned retired Scodec shape, and current
field-34 cooldown influence on committee selection. Its SHA-256 is
`e21fe7bc5f3a98ebd1c97a0ad7d9384fc0397d5b1440be91e0b613a31dfd5cdc`.

## 10. Owner response format

Please answer all three:

- `O22-01: accept recommendation (Option B)` or `O22-01: Option A`;
- `O22-02: accept atomic candidate semantics` or provide a different exact
  invalid/not-upheld/unavailable rule; and
- `O22-03: accept per-new-signer debit-funded bounty ownership` or provide a
  different deterministic claimant rule.

An answer selects the design only. It does not close the listed engineering,
schema, economics, resource, recovery, or adversarial-test gates and does not
activate fraud proofs.
