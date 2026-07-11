# Shard Sortition And Duty

**Status:** current v1 behavior. The prior secret, stake-weighted shard-VRF proposal was abandoned.

## Two Independent GL0 Committees

ML0 operators produce and sign metagraph binaries. They are not the admission or execution committee.

1. **Binary admission:** each eligible GL0 operator privately evaluates a real VRF for
   `(eta, metagraphAddress, parentHash)`. Advancing the metagraph parent or eta redraws membership.
2. **Execution shard:** every GL0 node publicly enumerates membership from
   `H(eta, shardId, etaPeriod, registeredVrfVk) < threshold`. Honest producers select the eta period from the finalized GL0 anchor.

Current adopters validate membership against the checkpoint's wire-carried eta period but do not bind that period back to the finalized
anchor. A Byzantine producer can therefore grind resolvable periods for a favorable public committee. Rotation is intended once per eta
period but is not adversarially enforced until audit finding SHARD-03 is fixed.

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
not redraw it per checkpoint; SHARD-03 tracks the missing verifier-side epoch binding.

## Producer Duty

Committee members are hash-ordered for the next shard ordinal. One rank owns each five-slot window by default, wrapping through the
committee; the genesis window is widened by 12x. This deterministic staircase replaced per-slot LDD shard leadership.

Duty determines who may propose. It never determines validity. The producer and every attester replay the included CL1 window before
signing, and every GL0 adopter replays it again before inclusion can affect canonical state.

## Enforcement Sites

- `CommitteeSortition.scala`: admission VRF and public execution draw.
- `StakeRegistry.scala`: uniform `committeeStake` versus global-leadership `relativeStake`.
- `ShardCheckpointWiring.scala`: eligible GL0 pool, sorted VK registry, per-period committee cache.
- `ShardCheckpointProducer.scala`: deterministic duty order and window.
- `ShardCheckpointGl0AcceptanceManager.scala`: membership, key-possession, signature, and mandatory replay checks.
