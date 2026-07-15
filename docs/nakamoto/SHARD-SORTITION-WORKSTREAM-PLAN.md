# Shard Sortition And Duty

**Status:** current v1 behavior. The prior secret, stake-weighted shard-VRF proposal was abandoned.

## Two Independent GL0 Committees

ML0 operators produce and sign metagraph binaries. They are not the admission or execution committee.

1. **Binary admission:** each eligible GL0 operator privately evaluates a real VRF for
   `(eta, metagraphAddress, parentHash)`. Advancing the metagraph parent or eta redraws membership.
2. **Execution shard:** every GL0 node publicly enumerates membership from
   `H(eta, shardId, etaPeriod, registeredVrfVk) < threshold`. Honest producers select the eta period from the exact canonical Phase-2 GL0 anchor.

Current worktree adopters recompute the only admissible wire period as `floor(gl0AnchorOrdinal/R)` before committee lookup, using the same
pure function as the producer. This rejects only an inconsistent ordinal/period pair. It does not stop a producer from choosing an older
admissible ordinal and its matching favorable period because GL0 selection currently applies only an upper bound. The checkpoint carries
no exact canonical anchor hash/freshness evidence and R remains node-local configuration. SHARD-03/SHARD-09 remain RED until the anchor and
parameters are proposal-parent/genesis/era bound.

The two committees may share `kDraw` and `kQuorum` configuration, but one committee's work never substitutes for the other's.

## Weight

Both live draws use uniform `1/N` weights, but not the same denominator or implementation. Admission calls
`StakeRegistry.committeeStake` over the full eligible GL0 validator set. Execution computes `1/N` independently over the post-cooldown
eligible pool in `ShardCheckpointWiring`. Neither is stake weighted. `StakeRegistry.relativeStake` is used by global Nakamoto slot
leadership and is a different mechanism.

The execution draw is not a secret VRF: registered verification keys and eta are public, so future committees are enumerable and
predictable. A checkpoint VRF proof demonstrates possession of the registered key over `(shardEta, slot)`; it does not hide membership.

## Rotation Cadence

An eta period contains `R = round(3.1 * k1)` GL0 snapshot ordinals under the current typed configuration:

- mainnet: 3174
- testnet/integrationnet: 794
- dev: 99

Execution membership is cached per `(shardId, etaPeriod)` only after the epoch's slash-exclusion anchor is settled. Honest production does
not redraw it per checkpoint. The worktree binds the claimed period to the signed anchor ordinal; exact hash-bound Phase-2 ancestry and a
canonical R parameter commitment remain open.

## Producer Duty

Committee members are hash-ordered for the next shard ordinal. One rank owns each five-slot window by default, wrapping through the
committee; the genesis window is widened by 12x. This deterministic staircase replaced per-slot LDD shard leadership.

Duty determines who may propose. It never determines validity. The producer and
every execution signer replay the included CL1 window before signing. Target
ordinary noncommittee GL0 adopters require distinct execution quorum, compare the
exact Phase-2 base/pre-root, apply the canonical scoped diff, and recompute its
root; assigned watchtowers replay as the collusion backstop. Current ordinary
noncommittee GL0 replay of sharded CL1 checkpoints is a regression, not the
target. Universal native GL1 execution and the global kernel remain.

## Enforcement Sites

- `CommitteeSortition.scala`: admission VRF and public execution draw.
- `StakeRegistry.scala`: uniform `committeeStake` versus global-leadership `relativeStake`.
- `ShardCheckpointWiring.scala`: eligible GL0 pool, sorted VK registry, per-period committee cache.
- `ShardCheckpointProducer.scala`: deterministic duty order and window.
- `ShardCheckpointGl0AcceptanceManager.scala`: membership, key-possession, signature, and mandatory replay checks.
