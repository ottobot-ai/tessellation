# O-21 Optimistic Decision Evidence Owner Review

**Status:** OWNER RESPONSE REQUIRED. This packet does not activate optimistic
finality or select an evidence codec. The already-ratified Phase-2 rule remains
`decided-attestation T_weight OR canonical k1 depth`; canonical `k1` depth is the
only live state-changing rail today.

**Runtime authority:** None. `DecidedAttestationEvidence` remains an opaque
content pointer. The live `TipAttestation`, `SnowballAccumulator`, and
`TWeightTrigger` remain telemetry and cannot advance Phase 2.

**Primary gates:** O-01, O-11, O-12, O-15, O-16, O-18, E1, E3, E4,
KEYREG-*, PSIG, FIN-M, FIN-S

**Updated:** 2026-07-16

## 1. Decision required

O-21 asks one narrow question:

> Are the required emit-once signed decision statements sufficient alone, or
> must additional sampling-transcript material also be qualification authority?

This does **not** reopen the finality architecture. GL0 remains a
Nakamoto/Taktikos/LDD chain. The evidence cannot choose a tine, validate a state
transition, create a lock, prevent a later density reorg, or act as a quorum
certificate. It can only qualify an exact locally authenticated and executed
snapshot, or its authenticated ancestor closure, for reversible Phase 2 after
objective fork choice has selected that branch.

The owner has already selected distinct emit-once decided attestations plus
`T_weight`, not `T_count`. What remains unspecified is whether the qualifying
artifact contains those exact signed local decision statements alone or must
also carry authoritative sampling transcripts. Under Option A, the aggregate
authenticates that `T_weight` of delayed-canonical stake signed a claim of local
cascade completion;
it cannot publicly reproduce that execution and depends on the calibrated
Byzantine-stake bound plus an enforced cascade-before-sign boundary.

## 2. Source-proven current state

| Surface | Current behavior | Consequence |
|---|---|---|
| Attestation body | `TipAttestation` contains only `tipHash`, `tipSlot`, `tipOrdinal`, and receiver-compared wall-clock `attestedAt` (`modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/attestation.scala:10-32`). | It does not bind parent/root, network/genesis/era/parameter identity, delayed roster/stake/key view, eta, cascade parameters, or a completed local decision. It is not the target statement. |
| Ingress | The receiver verifies the long-term and active KES signatures, then records the decoded claim (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala:2157-2208`). | This path does not prove that the exact target was locally authenticated/executed or that the signer completed a sampled cascade. |
| Untrusted hash | Protobuf decode wraps arbitrary UTF-8 `tipHash` bytes as `Hash` without enforcing the canonical hash length/encoding (`NakamotoSyncDaemon.scala:2232-2247`). | The live wire object cannot be promoted into consensus evidence by adding a threshold around it. |
| Decision algorithm | `SnowballAccumulator` explicitly has no K-peer query loop or alpha cascade (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/SnowballAccumulator.scala:13-25`). Its sticky leader-minus-runner-up rule decides on first crossing (`SnowballAccumulator.scala:159-177`). | Different receipt orders can produce different sticky decisions from the same eventual claims. It is not portable Snowball evidence. |
| Weight trigger | `TWeightTrigger` reads that transitional sticky decision and explicitly ignores its `threshold` argument (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/FinalityTrigger.scala:176-207`). | No executable `T_weight` decision verifier exists. |
| Stake denominator | Optimistic weight currently renormalizes over receiver-observed active peers (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/StakeRegistry.scala:20-22,158-169`). | A partition can give honest receivers different denominators. Target weight must be derived from one signed historical N-2 view. |
| Qualification wrapper | The dark coordinator distinguishes only `DecidedAttestationTWeight` and `CanonicalDepthK1`, and stores qualification evidence through an immutable pointer (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityCore.scala:189-255`). | The two-rail architecture is represented, but decided-attestation payload semantics are absent. |
| Payload inventory | `DecidedAttestationEvidence` is named, while `OpaquePointerOnly` explicitly means no canonical payload schema or semantic verifier (`modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusFinalityPayloadKind.scala:15,44-65`). | Length/digest commitment alone cannot prove optimistic qualification. |
| Release gate | Live `FinalityGate` is an ordinal watermark and cannot represent same-ordinal replacement or per-hash phase (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityGate.scala:9-30,44-50`). | Optimistic evidence cannot activate until exact-hash, branch-revision-bound release and rollback land. |

## 3. Non-negotiable semantics under every option

1. For each `(network, genesis, protocol era, ordinal, decision epoch)`, a signer
   durably records and emits exactly one decision statement, for the first
   beta-clear target. The domain is not target-, branch-, receiver-, or
   restart-dependent. A signature without a completed local cascade is Byzantine
   behavior; no receipt, current tip, cumulative gossip count, or threshold alone
   authorizes signing.
2. Before signing, the signer authenticates and executes the exact snapshot
   under the layer rules: universal direct `GL1 -> GL0` execution, replay-backed
   sharded-CL1 certificate/diff verification plus root reproduction, and the
   universal global conflict/settlement kernel. Optimistic finality never repairs
   or substitutes for economic validation.
3. One canonical statement binds at least:
   - network, genesis, protocol era, and exact consensus-parameter hash;
   - exact qualifying `GlobalSnapshotStateRef` `(ordinal, hash, parentHash, mptRoot)`
     plus its slot/period;
   - an explicit decision epoch equal to the canonical epoch derived from the
     target slot under the statement's exact protocol-era parameter schedule;
   - signer identity and the exact active historical atomic KES+VRF registration
     and derived KES step;
   - the N-2 roster/stake/key-view commitment and N-1 eta commitment used for
     epoch N;
   - sampler/cascade purpose and exact K, alpha, beta, delta, minimum-population,
     and `T_weight` identities; and
   - an explicit statement version and decision-context/domain tag.
4. Wall-clock `attestedAt`, receiver arrival order, peer liveness, current
   seedlist, local HOCON, current head, and local finalized ordinal are never in
   the statement identity or weight computation.
5. Aggregate verification requires canonically sorted, unique signers, exact
   statement equality except signer/key/signature fields, valid long-term and KES
   signatures at the historical step, exact historical membership, and weight
   derived from the committed N-2 distribution. Any duplicate, unknown,
   inactive, future-effective, stale or mismatched atomic KES+VRF pair, bad
   signature, noncanonical order, unavailable historical record, or context
   mismatch rejects the entire aggregate. There is no filter-then-sum path.
6. The verifier derives the exact aggregate weight and requires the frozen
   `T_weight`. There is no independent `T_count` qualification path and no
   receiver-observed-active renormalization.
7. The target snapshot must already be locally authenticated and executed and be
   on the currently objectively selected tine at release. The evidence cannot
   select that tine. An authenticated path may transfer qualification only to an
   ancestor of the exact qualifying snapshot, never to a same-ordinal replacement
   or unrelated branch.
8. Phase 2 remains reversible. A density reorg increments the lineage/release
   revision and invalidates every old-lineage lease and derivative, including
   those for retained ancestors. Orphaned targets cannot reacquire; a retained
   ancestor reacquires only after current-lineage evidence and readback
   verification. No signer promise or aggregate prohibits changing preference
   later.
9. Missing evidence/history/data means defer or use the independent canonical
   `k1` rail. It never means accept, slash, or fall back to local state.
10. The payload uses one strict, bounded ScodecV1 schema with complete vectors,
    independent reproduction, resource limits, and O-18-compliant transport and
    retention before activation.

## 4. O21-01 options

### Option A - exact signed local decision statements

**Recommendation.** The qualifying payload is a canonical aggregate of exact
signed `OptimisticDecisionStatementV1` values. Each statement attests that its
signer locally authenticated/executed the target and completed the registered
sampled cascade. The verifier checks the statement, historical membership/keys,
signatures, unique signer set, common target/context, and derived `T_weight`.
This authenticates the signers' claims; it does not let a public verifier
reproduce each signer's local cascade execution.

Per-query requests/responses may be retained for diagnostics, adversarial
research, or a separately designed fraud proof, but they are not qualification
authority and are not required to verify the aggregate. Absence of a transcript
cannot turn a structurally and cryptographically valid statement aggregate into
an invalid snapshot or slash its signer.

**Consequences:**

- The safety assumption is explicit: less than the tolerated delayed-canonical
  stake signs false decision statements, and honest signers run the frozen
  cascade before signing.
- Evidence size is proportional to the number of decision signers rather than
  K times beta rounds for every signer.
- Any verifier with the exact historical state can reproduce membership, keys,
  statement bytes, and aggregate weight without trusting the collecting peer.
- The statement aggregate is an optimistic-phase attestation, not a BFT QC: it
  has no proposal round, lock, view, commit rule, fork-choice authority, or
  irreversibility effect.
- A false-signature detector or slash rule cannot be inferred from this choice.
  Slashing requires a separate portable contradiction/proof design that cannot
  punish a signer merely because another receiver observed different samples.

### Option B - decision statements plus complete signed sampling transcripts

Each decision statement must carry every selected peer query and signed response
for every successful/failed cascade round needed to reproduce beta-clear. The
verifier recomputes each sample selection and cascade transition before counting
the signer.

**Consequences:**

- It gives stronger post-hoc evidence of the messages the signer claims to have
  observed, and may expose implementation divergence.
- It does not by itself prove transcript completeness, honest timeout handling,
  or that omitted/adversarially delayed messages did not exist. Those properties
  require a fully specified public sampler, round schedule, nonresponse rule, and
  signed response protocol.
- Qualification bytes and validation work grow with signers, K, and rounds;
  bounded descriptor/chunk/retention and anti-amplification rules become part of
  consensus activation.
- Missing one historical response can make otherwise honest decisions
  unverifiable, coupling optimistic finality availability to transcript DA.

### Option C - decision statements plus transcript roots and mandatory chunks

The aggregate carries decision statements plus a canonical commitment to each
signer's complete transcript. Bounded chunks remain retrievable through the
retention horizon, and qualification is usable only after the verifier obtains
and checks all required chunks.

**Consequences:**

- It reduces the aggregate envelope size but retains Option B's semantic and
  availability requirements.
- A root alone proves no cascade. If chunks are optional or sampled, the scheme
  needs a separately proved availability/sampling security bound and cannot claim
  complete transcript verification.
- This option directly depends on O-18 descriptor, chunk, fetch, compression,
  retention, and aggregate-resource decisions.

## 5. Recommendation rationale

Option A is the narrow interpretation of the already-ratified
`decided-attestation T_weight`. It makes the honest-protocol assumption visible
instead of implying that a finite network transcript proves asynchronous message
completeness. It avoids adding transcript-replay authority or a second
chain-selection/commit protocol; the sampled cascade remains the optimistic
finality component of GL0 consensus.

Options B and C are defensible only if transcript reproducibility is itself a
required security property and the project is willing to specify and carry the
additional signed query protocol, timeouts, DA, bounds, and retention. Neither
option eliminates the need for the exact decision statement or historical
weight verification.

## 6. Activation evidence after an owner answer

The selected evidence shape remains dark until all of the following pass:

1. A pure reference model for K-without-replacement/alpha/beta decisions,
   including partition, churn, equivocation, delayed delivery, undersized
   population, and conflicting-color schedules.
2. A frozen parameter object and independent security analysis/calibration for
   K, alpha, beta, delta, minimum population, and `T_weight` under the intended
   delayed-stake adversary.
3. Exact statement, aggregate, historical-context, signature, and payload vectors
   with duplicate/order/key-step/context/target/decision-epoch mutation tests,
   including rejection when the encoded decision epoch differs from the epoch
   canonically derived from the target slot and exact parameter schedule.
4. Convergence tests showing that evidence qualification never changes objective
   fork choice and that two nodes with the same selected branch and evidence
   compute the same exact-hash phase.
5. Signer-boundary tests proving no statement can be emitted before exact local
   authentication/execution and completed cascade decision. A durable emit-once
   journal/outbox is persisted before publication; crash, cancellation, restart,
   density reorg, and duplicate delivery cannot emit a second statement for
   another target in the same decision domain.
6. Exact-hash `FinalityGate` integration tests for same-ordinal replacement,
   density rollback, restart, inherited ancestor qualification, consumer-lease
   invalidation, and canonical `k1` fallback.
7. O-18 resource/transport limits and retained-history behavior sufficient to
   validate the chosen payload without an unbounded allocation or single-peer
   authority.
8. An independent audit confirming there is no global proposal/vote/lock/QC,
   view-change, or attestation-fed fork-choice path.

## 7. Owner response format

Please answer one of:

- `O21-01: accept recommendation (Option A)`;
- `O21-01: Option B`; or
- `O21-01: Option C`, with any required transcript availability threshold.

An answer selects the evidence shape only. It does not ratify provisional
parameters, close O-01/O-11/O-12/O-15/O-16/O-18, or authorize runtime
activation.
