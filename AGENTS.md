# Architecture Invariants

This repository is a greenfield fork of upstream Tessellation `v4.0.0`. Fork-only
wire shapes have never been deployed. Do not retain abandoned fork-only schemas,
codecs, fields, or execution paths for backwards compatibility.

## Layer Map

| Name | Source/build identity | Responsibility |
| --- | --- | --- |
| GL0 | Global L0, DAG L0, `dag-l0`, `gl0.jar` | Global snapshot consensus, economic settlement, canonical MPT, and finality. |
| GL1 | Global L1, DAG L1, `dag-l1`, `gl1.jar` | Native DAG-token edge application. Sends blocks directly to GL0. |
| ML0 / CL0 | Metagraph L0, Currency L0, `currency-l0`, `ml0.jar` | Metagraph snapshot consensus. Aggregates CL1 framework economics and DL1 custom data, then submits a state-channel binary to GL0. |
| CL1 | Currency L1, `currency-l1`, `cl1.jar` | Framework-defined metagraph economic operations. Sends blocks to ML0. |
| DL1 | Data L1, custom Data Application L1, `dl1.jar` | Arbitrary metagraph application logic. Sends data blocks to ML0. |
| ML1 | Conceptual umbrella for CL1 and DL1 | Not a distinct core module or formal runtime layer. |

The formal `Layer` type contains only `DagL0`, `DagL1`, `CurrencyL0`, and
`CurrencyL1`. A DL1 runtime is a `CurrencyL1App` with a data-application service
injected; it is not `dag-l1`.

## Topology

```text
Native DAG path:
client -> GL1 -> GL0

Metagraph path:
economic client -> CL1 --+
                         +-> ML0 -> GL0
custom data client -> DL1+

Canonical-state return:
exact Phase-2 GL0 state -> GL1, ML0, CL1, DL1
```

## Economic Authority

- GL0 is the canonical economic state machine.
- Every GL0 validator independently executes and validates every direct native
  GL1/DAG-token transition against the exact global proposal parent. A shard
  checkpoint, execution certificate, replay receipt, CL1 diff, or signature
  threshold can never authorize, replace, or bypass that native execution. The
  target also requires every GL0 validator to execute the deterministic global
  conflict/nullifier/settlement kernel. That kernel is not complete today: E9 is
  planned, and the live `numShards <= 1` branch still bypasses shard processing
  (`GlobalSnapshotAcceptanceManager.scala:2372-2384`).
- GL0 enforces framework-defined CL1 transitions through execution sharding:
  the checkpoint producer and every execution-committee signer independently
  re-execute the exact ordered currency inputs at the same Phase-2 GL0 base.
  A signer signs only when its byte-identical canonical diff and resulting root
  match the checkpoint. A noncommittee GL0 node verifies the execution
  certificate, applies the namespace-bounded diff to the signed base, and
  recomputes the root; it does not replay ordinary CL1 checkpoints.
- A state-validity signature is never emitted on receipt, best-tip selection, or
  signature count. It is emitted only from a locally reproduced result. Missing
  replay inputs mean defer/no-sign, never blind sign or false slash.
- The rule is layer-independent: a GL0 snapshot/optimistic signature, shard
  execution signature, ML0 state-validity vote/signature, or GL1 state-validity
  signature requires the signer to run that layer's complete deterministic
  validation/reproduction first. Intake acknowledgements and DA custody proofs
  must use separate non-validity types if the owner retains them.
- An ML0 signature or binary-admission attestation authenticates the metagraph
  input. It cannot replace the execution committee's replay. An execution
  checkpoint root and diff are claims until enough independently replaying
  committee members sign them. Watchtower replay is the noncommittee collusion
  backstop. Positive assigned replay coverage is required before GL0 inclusion;
  its population, deadline, retry, bond, and resource parameters remain open.
- Downstream validation flows toward GL0. After the exact containing GL0 hash
  reaches Phase 2, upstream followers adopt that reversible operational state;
  CL1 must not independently override or re-execute it on the return path.
- DL1 custom application bytes are commitment/data-availability carriage because
  GL0 does not know their schema or run arbitrary metagraph code. GL0 certifies
  authenticated inclusion/availability, not custom semantic correctness. A future
  proof-verifier lane requires an explicit deterministic active-era verifier ADR;
  it is not implicit in a state-channel binary and never extends to CL1 economics.
- The target state-channel envelope has an explicit signed lane. Framework
  currency is replayed by the execution committee. Framework currency with a
  data application replays the framework part and carries only an isolated
  custom-data commitment/availability payload. Decoder success is never a lane
  selector. Custom application output cannot authorize or synthesize a framework
  effect; an independently signed framework fee/intent may explicitly bind the
  exact custom-data commitment and rejects if it changes. The owner-retained
  standalone opaque/data-only lane provides authenticated custody, availability,
  and ordering only. It has no framework-economic effect and cannot import,
  authorize, or synthesize framework state.
- `authoritative*`, `AdoptFromSignedFields`, roots-only economic adoption, and
  fork-only V1/V2 compatibility schemas are forbidden. The canonical checkpoint
  byte diff is not an authoritative override: it is the exact output reproduced
  by every execution signer and root-checked by every adopter.

This section is the target invariant, not a claim that the current transition
rules are production-safe. The source audit at
`docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md` identifies open global
authorization, conservation, stake-backing, replay, and finality violations.
Committee replay does not make a defective transition function correct. The
authorization, conservation, replay-protection, and deterministic-execution
findings in the audit still have to be repaired in that shared function.

## Global Finality

- GL0 consensus is Nakamoto/Taktikos/LDD chain consensus. Do not add global
  proposal/vote/lock/QC/view-change BFT machinery. ML0 consensus may remain BFT.
- `FinalityGate` is intended to be the hash-bound global finality gadget, not
  merely an HTTP ordinal watermark:
  - Phase 0 `Pending`: valid stored branch candidate.
  - Phase 1 `Provisional`: on the current `maxvalid-tk` canonical tine.
  - Phase 2 `Operational`: selected by the ratified Avalanche/Snowball optimistic
    trigger or Nakamoto `k1` depth fallback. This is the only phase metagraphs and
    downstream layers may reference. It remains density-reorgable.
- There is no protocol Phase 3 and `k2` is not a finality or fork-choice floor.
  `k2 = 100 * k1` is a recommended local retention, proof-service, and automatic
  rollback horizon. If objective comparison reaches older than retained state,
  the node enters `RecoveryRequired`, stops production/mutation, fetches exact
  authenticated history/state, and resumes only after verified reconstruction
  and ordinary valid-chain comparison. An operator cannot choose the winner.
- A Phase-2 density reorg orphans the old `(ordinal, hash)` and installs a new
  canonical hash. All downstream followers and checkpoint windows must receive
  the reorg and roll back/re-follow. Ordinal-only monotone finality is invalid.
- Avalanche contributes the optimistic Phase-2 finality rail. It does not validate
  economics, replace replay, or introduce BFT commits. A global attestation may
  be emitted only for a locally authenticated and executed snapshot.

The active dependency order is `NAKAMOTO-PLAN.md`; normative artifact states
and authority are in `docs/review/CONSENSUS-ARTIFACT-LIFECYCLE.md`; delegable
RED/oracle/integration gates are in
`docs/review/CONSENSUS-PROTOCOL-TEST-PLAN.md`; locked/open protocol choices are in
`docs/review/CONSENSUS-OWNER-DECISIONS.md`. The partial upstream-v4 source
inventory in `docs/review/V4-ECONOMIC-GRAMMAR-AUDIT.md` is audit evidence for
the economic-grammar gate, not a completed grammar or protocol specification.
Historical roots-only,
bounded-history, or committee-authority task lists are not implementation plans.

## Operator Consensus Keys

- KES and VRF are one preregistered operator identity, never independent
  authority maps. One canonical record binds `PeerId`, the KES master
  verification key and activation offset, and the 32-byte VRF verification key
  under the operator's established long-term identity.
- Every VRF use is covered: GL0 leadership, metagraph-binary admission,
  execution-shard membership and possession proofs, staircase producer duty,
  execution signing, assigned watchtower selection, optimistic-finality
  sampling, NiPoPoW/tower eligibility, and slashing/evidence verification. A
  future VRF consumer is invalid until it uses the same canonical registry API.
- Runtime registration or rotation is included in rooted GL0 state before it can
  become active. Eligibility in eta period `N` resolves the operator/key view
  from the exact canonical `N-2` prefix and eta evidence from `N-1`; a runtime
  pair cannot activate earlier than its signed effective period or appear unless
  it is present in that `N-2` view. This is a two-period index lookback, not an
  unstated `N+3` elapsed-time rule. Every rotation rebinds the complete KES+VRF
  pair atomically; neither half may change through an independent authority map.
- Key registration proves ownership and fixes key material; it never grants
  validator, committee, or watchtower eligibility. Every draw intersects the
  active paired-key view with the independently authorized, delayed canonical
  operator/stake roster. A self-signed registration outside that roster has zero
  consensus weight and cannot enter any VRF population.
- A candidate, certificate, checkpoint, attestation, tower proof, gossip
  message, or sender may carry a VRF key or KES step only as comparison
  evidence. The verifier resolves the exact active pair at the candidate's
  historical parent, derives `expectedKesStep = artifactPeriod -
  registeredOffset`, and requires equality. Missing, pending, stale, duplicate,
  mismatched, or historically unavailable registration state fails closed;
  missing history defers/recovery and cannot slash.
- The complete unique genesis pair is the only period-zero exception. It must be
  committed by the canonical genesis identity/root, and startup fails if local
  key material does not match it. A local JSON map by itself is not the final
  trust anchor.
- Consensus tests obey the same rule. Snapshot, admission, committee,
  execution, watchtower, optimistic-finality, tower, and evidence fixtures must
  first place a canonical paired registration in the exact `N-2` view used for
  eligibility, or use a complete committed genesis pair. Arbitrary unregistered
  keys are allowed only in isolated cryptographic primitive tests.

The normative contract and current implementation gaps are in
`docs/nakamoto/OPERATOR-CONSENSUS-KEY-REGISTRY.md`.

## Committee Identity

- ML0 operators run metagraph snapshot consensus and sign the binary submitted to
  GL0. They are not either GL0 committee.
- The per-metagraph binary-admission committee and the execution-shard committee
  are both selected from eligible GL0 operators. ML0 operators are not members by
  virtue of operating ML0; they produce and sign the state-channel binary.
- Binary admission is a real secret-key VRF self-sortition for each
  `(eta, metagraph, parentHash)`. Its live threshold weight is uniform `1/N` via
  `StakeRegistry.committeeStake`, not stake weighted.
- Execution-shard membership is a separate public deterministic hash of
  `(eta, shardId, epoch, registeredVrfVk)`, with uniform `1/N` over the
  post-slash eligible pool. Admission's `N` is the full eligible GL0 validator
  set; execution's denominator is independently recomputed after cooldown
  exclusion. Execution membership is enumerable and predictable; it is not a
  secret VRF draw. The checkpoint VRF proves possession of the registered key
  over `(shardEta, slot)`.
- Shard committees partition intake and execution. Every committee signature
  means replay matched the checkpoint diff and root. Noncommittee GL0 nodes
  verify the execution threshold and adopt the diff after namespace, base,
  continuity, and root checks. Selected noncommittee watchtowers replay to detect
  a colluding execution threshold.

## Shard Lifecycle

- Metagraph assignment is static in v1:
  `unsignedBigEndian(SHA-256(metagraphAddress)) mod numShards`. Every GL0 node
  computes the same assignment; ML0 does not choose its shard.
- The metagraph-binary admission committee and execution-shard committee are
  separate draws over eligible GL0 operators. They may share configured draw
  and quorum sizes, but one committee's attestation never substitutes for the
  other committee's work.
- Admission membership is independently self-sortitioned for each
  `(metagraph, parentHash, eta)` input. In practice, advancing the metagraph
  parent redraws the admission committee; an eta rotation also changes it.
- An honest producer derives execution-shard membership once per
  `(shardId, etaPeriod)` from the checkpoint's finalized GL0 anchor, and the
  draw is cached only when the epoch's slash-exclusion anchor is settled.
  Current worktree verifiers recompute the wire-carried epoch from the signed
  anchor ordinal with the same pure function as the producer and reject a
  mismatch before committee lookup. This rejects only inconsistent pairs: a
  producer can still choose an older admissible anchor ordinal and its matching
  favorable epoch/committee because the ordinal is not an exact canonical
  Phase-2 hash/freshness proof. `R` also remains node-local configuration; see
  audit findings SHARD-03 and SHARD-09.
- An eta period contains `R = round(3.1 * k1)` GL0 snapshot ordinals: 3174 on
  mainnet, 794 on testnet/integrationnet, and 99 on dev under current defaults.
- Within an execution committee, producer duty rotates independently of
  membership. Members are deterministically hash-ordered for the next shard
  ordinal; one rank owns each five-slot window by default, wrapping across the
  committee. The genesis duty window is widened by 12x while gossip forms.
- ML0 operators produce and sign metagraph binaries. Eligible GL0 operators
  admit those binaries, buffer them by deterministic shard assignment,
  pre-execute checkpoint windows, attest only after replay, and submit the
  replayable checkpoint claim for GL0 inclusion.
- A producer collects a deterministic, bounded, per-metagraph-ordered binary
  window, executes its framework currency portion at an exact Phase-2 GL0 base,
  and commits the full inputs, canonical per-metagraph diff, and complete
  post-state root. Every committee signer independently reproduces both diff and
  root before signing.
- `kQuorum` execution signatures make a checkpoint diff-adoptable. Shard depth
  chooses/qualifies a Nakamoto shard chain; it must not silently substitute for
  missing execution signatures unless a separately specified mechanism proves
  that the required independent executions occurred.
- Ordinary GL0 adoption validates committee identity, execution signatures,
  metagraph-to-shard ownership, exact base, parent/ordinal continuity, diff key
  namespace, and recomputed root. It then applies the diff without currency
  recreation. A containing tentative GL0 snapshot does not hard-anchor the shard;
  the anchor advances when that GL0 snapshot reaches Phase 2.

## Cross-Shard Economics

- Metagraph shards do not directly settle with or trust one another. A
  cross-shard framework operation is carried to GL0, where it reads the owner
  state from an exact canonical Phase-2 `(ordinal, hash, root)` and is serialized
  through global nullifier/settlement state. Per-metagraph shard diffs may not
  directly write another metagraph's partition or global nullifier partition.
- Cross-shard allow-spend consumption uses the referenced allow-spend as the
  authorization and records a permanent single-use nullifier in canonical GL0
  state. ML0 reads exact canonical Phase-2 GL0 pending entries, fetches and replays the
  SpendActions, applies them to its next currency snapshot, and emits
  `GlobalSnapshotsProcessed`; the appropriate execution committee re-executes
  and certifies the resulting framework diff before GL0 adoption. CL1
  subsequently adopts the exact Phase-2 GL0 MPT view without re-execution. None of
  those downstream steps is a second source of authority.
- A nullifier, settlement adjustment, or owner acknowledgement that affects
  spendability must be consensus state. Node-local caches, peer responses,
  wall-clock state, and committee-only receipts may not affect economic roots.
- Current `numShards = 1` gates disable the execution-shard path; that is a
  production-security gap, not target semantics. In an economic deployment,
  `numShards = 1` means one execution shard using the same committee replay,
  diff adoption, and watchtower rules as K shards. Any direct/unsharded shortcut
  is an explicitly unsafe development profile and cannot change the economic
  transition function.
