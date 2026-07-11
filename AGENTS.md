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
finalized GL0 -> GL1, ML0, CL1, DL1
```

## Economic Authority

- GL0 is the canonical economic state machine.
- Every framework-defined CL1 transition carried toward GL0 must be independently
  re-executed by GL0 before it can affect canonical state or value.
- An ML0 signature, admission attestation, shard-committee root, state diff, or
  watchtower promise is a claim, never economic authority.
- Downstream validation flows toward GL0. After GL0 finalizes the result, upstream
  followers adopt that canonical state; CL1 must not independently override or
  re-execute finalized GL0 state on the return path.
- DL1 custom application state may be proof-carried because GL0 does not run
  arbitrary metagraph code. That exception never extends to CL1 economics.

This section is the target invariant, not a claim that the current transition
rules are production-safe. The source audit at
`docs/review/CORRECTNESS-SECURITY-AUDIT-2026-07-11.md` identifies open global
authorization, conservation, stake-backing, replay, and finality violations.
Repeating an invalid transition on every GL0 node is deterministic invalidity,
not economic enforcement.

The active dependency order for closing these blockers is
`docs/review/CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md`. Historical roots-only,
bounded-history, or committee-authority task lists are not implementation plans.

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
- Shard committees partition execution work, not trust. GL0 adoption still
  re-executes included CL1 inputs.

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
  Current verifiers do not bind the wire-carried epoch back to that anchor, so
  this rotation policy is not yet enforced against a Byzantine producer; see
  audit finding SHARD-03.
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
- Every GL0 adopter validates committee identity and signatures, verifies
  metagraph-to-shard ownership, and recreates every included CL1 transition
  before updating the canonical MPT. A checkpoint root is never directly
  applied as economic state.

## Cross-Shard Economics

- Metagraph shards do not directly settle with or trust one another. A
  cross-shard framework operation is carried to GL0, where the consuming
  transition reads the owner metagraph's state from a consensus-pinned GL0 MPT
  view and is re-executed by every GL0 validator.
- Cross-shard allow-spend consumption uses the referenced allow-spend as the
  authorization and records a permanent single-use nullifier in canonical GL0
  state. ML0 reads the finalized GL0 pending ordinals, fetches and replays the
  SpendActions, applies them to its next currency snapshot, and emits
  `GlobalSnapshotsProcessed`; GL0 re-executes and verifies that snapshot. CL1
  subsequently adopts the finalized GL0 MPT view without re-execution. None of
  those downstream steps is a second source of authority.
- A nullifier, settlement adjustment, or owner acknowledgement that affects
  spendability must be consensus state. Node-local caches, peer responses,
  wall-clock state, and committee-only receipts may not affect economic roots.
- The production default `numShards = 1` disables the execution-shard path.
  Multi-shard guarantees must not be claimed for a deployment until the
  cluster-uniform configuration explicitly enables more than one shard.
