# 17. Committee re-execution is the primary economic-validity gate

Date: 2026-07-10

## Status

Accepted

Extends ADR-0016 (execution-sharding re-execution model) and the two-tier finality model. Supersedes the *implemented* behavior in which a shard-checkpoint reaches quorum on committee signatures that do not attest a re-execution.

## Context

**The core principle this ADR restates as binding: a node adds its signature only to something it has verified itself. You never sign state you did not re-execute.** A committee signature must mean *"I independently re-ran this metagraph's derivation and got this exact root."* The re-execution test suite already states this intent (`ShardCommitteeReExecutionSuite` §"an attestation must mean 'I independently re-ran…and got result R'").

The implementation does not do this on the happy path. Verified at source (HEAD `5557ee084`):

- **Committee members sign on best-tip, without re-executing.** `ShardCheckpointAttestationEmitter` emits an attestation when a received checkpoint *becomes the node's best tip* (chain selection), signing Ed25519+KES over the checkpoint **hash**. There is no re-execution in the emit path; the attestation-receive handler (`NakamotoSyncDaemon.scala:2933-2953`) only verifies the hash-signature and records it. So the quorum encodes **chain agreement, not economic re-validation**.
- **gl0 adopts on signature count, without re-executing.** `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` admits on `distinctSigners >= kQuorum` (`:388-396`, default 6-of-8). Its own scaladoc (`:156-158`) states the exposure: *"admits on `kQuorum` distinct committee signatures WITHOUT re-execution … a corrupt committee that reaches quorum can attest a WRONG root and have it adopted."*
- **The only happy-path re-execution is the producer's** (one node). Re-execution DOES run, and with a real base-pinned closure, in exactly two places — **neither gates the happy path**: the sub-quorum failover `reExecPath` (`:397-406`, reachable only when `distinctSigners < kQuorum`) and `watchtowerReExec` (`:411-441`), which runs **after adoption**, fire-and-forget (`NakamotoSyncDaemon.scala:2784-2787`), producing a fraud proof for *later* slashing.

The re-execution machinery is real and already wired: production `SharedServices.scala:321` injects `reExecuteDerivation = Some { reExecDerivationWithDiff(…, pinnedReaderAt) }` — the same base-pinned derivation the producer runs. (`noReExecDerivation` is only the `None` default for tests/legacy, `ShardCheckpointWiring.scala:503`.) So both the sub-quorum failover and the watchtower re-execute for real; the gap is only that the **happy-path attestation and adoption skip it**.

**How this happened (git trace, not a designed decision).** `6c7746aec feat(shard): emit ShardCheckpointAttestation on best-tip (T_count quorum)` built the emitter as a chain-attestation/quorum-liveness seam. `8a9d54836` (2026-05-29) introduced the `distinctSigners >= quorum → Accepted` adoption. `396ca81b0` (2026-06-04) "decouple kDraw from kQuorum — throughput" tuned the quorum during the committee-gate throughput fight. `8ac7ce04f` (2026-06-29) *documented* the hole and added the watchtower as a **post-hoc** check rather than fixing the source. Net: re-exec-before-sign was never wired into the attestation path; the gap was patched with an after-the-fact watchtower.

**Why post-hoc slashing is insufficient.** On the happy path the economic guarantee reduces to *producer honesty + slash-after-the-fact*. A malicious slot-leader can get a wrong root adopted and optimistically attestation-finalized before any watchtower dispute lands; if extractable value in that window exceeds the slashable stake, slashing does not deter it. That violates the project's #1 rule (CL1 economic ops MUST be re-executed) — accountability is not prevention.

## Decision

**1. The committee quorum IS the re-execution consensus. A committee member MUST re-execute and MUST sign only its own verified root.**

On receiving a shard checkpoint that would become its best tip, each committee member re-runs the SAME base-pinned derivation the producer ran (`ShardCheckpointWiring.reExecDerivationWithDiff` at the wire-carried `diffBaseOrdinal`) over the checkpoint's `includedSnapshots`, and:
- **Full match** of every recomputed `perMetagraphMptRoots(mg)` against the checkpoint's → sign + emit the attestation (as today).
- **Mismatch** → **do NOT sign**, and **emit a `FraudProofEnvelope`** (reuse the existing watchtower emit path). The honest committee becomes the *first* line of fraud detection, at sign-time, not the watchtower post-adoption.
- **Cannot-derive sentinel** (`Hash.empty` — pinned base unresolvable / contiguity gap) → **defer** (do not sign yet, do not fraud-proof; retry when the base catches up), exactly as `reExecPath`/`watchtowerReExec` already bucket it.

A quorum of `kQuorum` signatures then means `kQuorum` independent re-executions that agree. gl0 `verifyEmbedded` adopting on quorum is then adopting on `kQuorum` re-executions — economic validity established **at quorum formation**, before adoption, before finality.

**2. Corrected finality tiers.**

| Tier | Mechanism | Establishes |
|---|---|---|
| **Economic validity — PRIMARY** | Each of `kQuorum` committee members re-executes; signs only its own verified root; refuses + fraud-proofs on mismatch | A checkpoint is not adoptable until `kQuorum` members independently re-executed **and agree** (honest-majority BFT). |
| Attestation finality (unchanged) | Avalanche `T_count` over the global snapshot | chain growth / provisional reads — fast; safe because embedded checkpoints carry a re-exec quorum. |
| Nakamoto depth-k (unchanged) | chain depth | reorg / chain-safety fallback. |
| **Watchtower — BACKSTOP (demoted)** | non-committee re-exec → fraud proof → slash | catches a **fully-colluding committee** (≥`kQuorum` Byzantine members, i.e. honest-majority already broken); may additionally gate value-out. |

The watchtower is no longer the primary (or only) re-execution check — it is defense-in-depth for the case the honest-majority assumption itself fails.

**3. Determinism of the re-execution is now LIVENESS-critical, not only safety-critical.**

Because a committee member refuses to sign on any root mismatch, ANY non-determinism across honest members means they compute different roots, refuse to sign, and **quorum never forms → the checkpoint stalls**. The `producer-root == verifier-root` property and the base-pinning (diff-base-pin, PIN-1 component roots, and the empty-prior boundary — see ADR-0016 / handoff Q4) become the **liveness bar**. This must be airtight before enabling the sign-gate.

## Consequences

- **Latency: ≈ one re-execution window, incurred in parallel.** Committee members re-execute concurrently on their own nodes, so added time-to-quorum ≈ the slowest member's single re-exec (a diff over a pinned base bounded by the included-chain window), not `kQuorum`× serial. Chain growth (soft attestation) can still proceed on the chain-agreement signal; only economic adoption waits for the re-exec quorum.
- **Aggregate CPU:** `kQuorum` committee nodes re-execute each checkpoint (plus a few watchtowers). This is the intended cost of a committee, not a cost to avoid.
- **Sequencing (mandatory):** (a) the re-execution must be deterministic across honest members — close the empty-prior boundary and keep the base-pin airtight; (b) the current committee-gate parent-ordinal wedge (a liveness bug) must be fixed first. Enabling the sign-gate before (a)/(b) re-introduces the committee-gate freeze the throughput campaign fought. The base-pin discipline is already documented as slash-safety-critical (`SharedServices.scala:323-334`); it now also gates liveness.
- **False-stall vs false-slash:** the same base-skew that would have caused a false 100% committee slash on the sub-quorum path now causes a false *stall* on the sign path. Both are prevented by the identical fix (pin the derivation at the wire-carried `diffBaseOrdinal`); the `Hash.empty` defer path prevents a can't-derive from becoming either.
- **Watchtower stays.** Keep the fraud-proof + `InvalidStateProof` slash (consensus-load-bearing, `GlobalSnapshotAcceptanceManager.scala:3183-3184`) as the backstop for ≥`kQuorum` collusion. The slash now also punishes any member who signs a root the committee/watchtower refutes.
- **`numShards = 1`:** none of this exists at the default single-shard config (no committees, no watchtowers). That posture is out of scope here and needs its own decision.

## Implementation scope (the concrete change)

Primary change — wire re-exec into the attestation sign path:
- **`ShardCheckpointAttestationEmitter`** (and its caller `NakamotoSyncDaemon.handleShardCheckpoint`, ~`:2607`, the became-best-tip seam): inject the base-pinned re-exec closure already built at `SharedServices.scala:321` (or a derived `verifyCheckpointRoots(checkpoint): F[VerifyResult]`). Before signing, re-exec every `mg` in `checkpoint.derivedStateDelta.includedSnapshots` via `reExecDerivationWithDiff` at `checkpoint.diffBaseOrdinal`; compare to `checkpoint.derivedStateDelta.perMetagraphMptRoots`.
  - all-match → sign + emit (unchanged emit).
  - any mismatch → do NOT emit; call the existing `WatchtowerFraudProofEmitter.emit` (or its `InvalidStateProofEvidence` builder) to raise the dispute.
  - any `Hash.empty` (can't-derive) → defer + retry (do not sign, do not dispute).
- **`ShardCheckpointGl0AcceptanceManager.verifyEmbedded`**: the quorum branch (`:388-396`) may remain as-is *once* attestation ⇒ re-exec (quorum now means `kQuorum` re-execs). Optionally add a `reExec-on-adopt` belt-and-suspenders, but it is redundant with committee re-exec + watchtower; do not add if it costs adoption latency.
- **Fraud-proof-at-sign** reuses `InvalidStateProofValidator` / `FraudProofEnvelope` / the pool→embed→`applySlash`→`Slashings` chain unchanged.

Tests:
- Extend `ShardCommitteeReExecutionSuite`: a committee member handed a checkpoint with a tampered `perMetagraphMptRoots` **refuses to sign** and **emits a fraud proof**; an honest checkpoint is signed.
- Determinism regression: producer-root == every-member-verifier-root over the same wire-carried `diffBaseOrdinal` (the liveness bar), including an incremental-only-window case (empty-prior boundary).
- e2e: with the sign-gate on, (i) quorum still forms for honest producers (liveness), (ii) a malicious producer's checkpoint fails to reach quorum and is disputed.

## References

- `ShardCheckpointAttestationEmitter.scala` (best-tip sign, no re-exec) — the defect site.
- `ShardCheckpointGl0AcceptanceManager.scala:156-158,388-396,397-406,411-441` — quorum-adopt, sub-quorum re-exec, watchtower re-exec.
- `ShardCheckpointWiring.scala:268,355-363,404,411,503,561` + `SharedServices.scala:321-357` — the real base-pinned re-exec closure and its production wiring.
- `NakamotoSyncDaemon.scala:2784-2787,2933-2953` — watchtower fire-and-forget; attestation-receive hash-verify.
- ADR-0016; `docs/review/NAKAMOTO-ECONOMIC-SECURITY-HANDOFF.md`.
