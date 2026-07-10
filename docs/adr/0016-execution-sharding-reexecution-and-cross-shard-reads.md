# 16. Execution-sharding re-execution model and cross-shard finality-first reads

Date: 2026-07-10

## Status

Accepted

## Context

Tessellation runs a hierarchical DAG. Under execution sharding, the pieces and
their trust relationships are frequently mis-stated (including in prior internal
notes), so this ADR fixes the model as the canonical record.

The dividing question for **every** piece of state is: **does the global
hypergraph have the code to re-execute this?**

**Global layer — universal, every GL0 node re-executes:**

- **dag-l0 / GL0** — global snapshot producer + verifier. The snapshot every
  shard embeds into and every metagraph reads back through a consensus-pinned
  `globalSyncView`.
- **dag-l1 / GL1** — the global DAG-token edge application. Packages DAG
  transactions into blocks, submits them to dag-l0 for inclusion. This global
  currency movement is re-executed by **all** GL0 nodes.

**Metagraph layer — L2, execution-sharded (only *processing* is sharded; the
global layer stays universal):**

- **CL1 (currency-L1)** — the **L2 framework economic layer**: allow-spend,
  spend, token-lock, transfer, fee, balance, supply. GL0 ships this framework
  code — it is identical for every metagraph.
- **DL1 (data-L1)** — arbitrary, per-metagraph data-application logic. GL0 does
  **not** have and **cannot** have this code.
- **ml0** — produces the metagraph snapshot aggregating CL1 + DL1.

Because CL1 is framework code GL0 possesses, it **can** and **must** be
re-executed. Because DL1 is custom code GL0 does not possess, it **cannot** be
re-executed by GL0 — its authoritative state must be carried as state + proof.

The open question this ADR settles: when a CL1 op in shard A depends on state
owned by shard B (cross-shard framework functionality), what does shard A read,
and does it wait for global finality or act on shard B's committee attestation?
See the sequence and analysis in
`docs/nakamoto/CROSS-SHARD-PROTOCOL-RESEARCH.md`.

## Decision

**1. Re-execution split — the economic guarantee is RE-EXECUTION.**

- Global DAG-token movement (dag-l1 / GL1): re-executed by **every GL0 node**.
- Metagraph economic ops (CL1): re-executed by the assigned **shard committee**,
  which produces the re-executed state (byteDiff) and ships **both** the
  checkpoint and the state. **Watchtowers re-exec-check** → mismatch ⇒
  `InvalidStateProof` slash. Other GL0 nodes adopt + verify the stateProof.
- Metagraph data-app logic (DL1): **proof-carried** — authoritative at the
  metagraph, adopted-and-verified by GL0 against a proof/root. Re-execution is
  *impossible* here, so a proof is the only mechanism and is correct **for DL1
  only**.

"Roots-only" / "proof-carrying" describes the **storage commitment** and the
**DL1** path. It is **never** a licence to skip re-executing a CL1-processed
economic operation. Relocating CL1 re-execution to the committee (from a
hypothetical every-GL0 re-exec) **moves** the re-execution; it does not
eliminate it.

**2. Cross-shard framework reads are finality-first (Option A).**

Shard committees do not share state through a direct channel — they share it
**through the global snapshot** every shard embeds into. A cross-shard CL1 read
resolves against gl0's **consensus-pinned, finalized** global mirror:

- On the gl0 consensus accept path, the cross-shard read source is
  `ShardSubtreeProofClient.gl0Local(branchAwareReader)`, where `branchAwareReader`
  is the same accept-`parentTip`-bound, cluster-uniform finalized reader every
  per-manager prior-state read uses. Every gl0 node reads the byte-identical
  value ⇒ no fork.
- If the needed cross-shard value is not yet embedded in gl0's finalized mirror,
  the read returns absent → the consuming op is rejected this round and retried
  on the next gl0 ord (it **waits** for finality; bounded ~one global cadence).

The alternative — admitting a cross-shard input on shard B's committee
attestation *before* global finality (attestation-first) — is **rejected** for
now in favour of safety. It is implemented nowhere and must not be added to the
consensus path.

**3. The invariant: a proof/read supplies an INPUT, never a substitute for
re-execution.**

In every cross-shard case the consuming CL1 op is **still re-executed** by the
committee / gl0 accept. The cross-shard read (or, on the committee-to-committee
P2P path, an inclusion proof) supplies only the *input value* the local shard
cannot compute — it never replaces executing the op.

## Consequences

- **Safety over latency.** Every cross-shard input sits behind global finality,
  so a corrupted, thinned shard committee cannot poison a consuming shard: the
  bad state must survive *global* consensus + watchtowers, not just its own
  committee. This is what keeps the cross-shard chain-quality collapse bound
  (α_total > 1/(2S)) from biting. The cost is up to ~one global-snapshot cadence
  of cross-shard latency and a retry loop on the consuming op.

- **The committee-to-committee P2P proof path is also finality-anchored.**
  `ShardSubtreeProofClient.http` verifies every proof against gl0's
  **last-finalized** checkpoint (`verifyProof` requires the proof's `perMgMptRoot`
  and `shardCheckpointHash` to match what gl0 finalized) and fails closed when
  there is no finalized anchor. The serve side may generate over `bestTip`, but a
  too-fresh proof simply fails the consumer's finalized cross-check and is retried
  — never trusted early.

- **Determinism guard — do NOT wire a node-local reader into consensus.** A live
  `ShardSubtreeProofClient.http` (peer fetch: network / peer-pick / cooldown) or
  any best-tip / pending / live-store reader on the gl0 accept path feeds the
  consensus `mptRoot` non-deterministically and forks the cluster. The accept
  path must always use `gl0Local` off the consensus-pinned finalized reader.

- **Anti-drift rule (load-bearing).** Do not propose roots-only / proof-carrying
  / attestation-only as a *replacement* for re-executing a CL1 economic op, do
  not call the mirror/fold "settled and deletable," and do not frame the
  store-fidelity / byteDiff work as legacy. That work is how GL0 carries and
  adopts the committee's re-executed state; it stays. Fix sharding/mirror bugs
  **within** the re-execution model.

- **Revisiting Option B** (attestation-first cross-shard admission) requires a
  new ADR that supersedes this one, with an explicit analysis of the CQ-collapse
  exposure it reintroduces.

## References

- `modules/node-shared/.../domain/swap/SpendActionValidator.scala` — same-shard
  vs cross-shard classification and read paths.
- `modules/node-shared/.../domain/nakamoto/sharding/ShardSubtreeProofClient.scala`
  — `gl0Local` (deterministic consensus read source) and `http` (finality-anchored
  committee P2P path).
- `modules/node-shared/.../infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala`
  (~L2507–2548) — the gl0 accept wiring that defaults to `gl0Local(branchAwareReader)`.
- `docs/nakamoto/CROSS-SHARD-PROTOCOL-RESEARCH.md`,
  `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.
