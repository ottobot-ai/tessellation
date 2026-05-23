package io.constellationnetwork.schema.sharding

import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Per-(shard, peer, epoch) non-participation accumulator (slice 17 — see
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.3).
  *
  * Each entry is the running tally for a single committee member's participation duties during one eta-period. The tally splits into two
  * complementary obligations:
  *
  *   - '''Slot-leader duty.''' When the per-shard `EligibilityChecker` elects this peer as a slot leader for a slot inside `epoch`,
  *     `totalSlotsAsLeader` increments. If the peer fails to emit a checkpoint for that slot before the next slot rolls, `missedSlotsAsLeader`
  *     also increments.
  *
  *   - '''Attestation duty.''' When this peer receives a checkpoint authored by some other slot leader inside `epoch`,
  *     `totalCheckpointsReceived` increments. If the peer fails to attest within the per-shard attestation window,
  *     `missedAttestationWindows` also increments.
  *
  * '''Why two denominators (not one).''' Slot-leader duties and attestation duties have different cadences: a peer with a small stake share
  * is elected leader rarely (so its `totalSlotsAsLeader` may be 0 or 1 in a whole epoch) but receives every other checkpoint (so its
  * `totalCheckpointsReceived` is high). Folding both into a single missed-count would conflate "elected once, missed once" (1/1 = 100%,
  * suspicious) with "received 50 checkpoints, missed 25" (50%, similarly suspicious). The two-denominator shape lets the
  * [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardNonParticipationSlasher]] apply the threshold
  * separately to each obligation (and apply the min-denominator floor only where it matters).
  *
  * '''Why per (shardId, peerId, epoch) rather than per (peerId, epoch).''' A peer may be a member of multiple shard committees in the same
  * epoch (sortition is per-shard); attributing missed duties to the wrong shard would dilute the rate and let a peer escape slashing by
  * being present in one shard while absent from another. Per-shard scoping keeps the rate honest.
  *
  * '''Why `Long` for the counts.''' Per-epoch counts are bounded by `etaRotationSnapshots` (≈ 100 in v1) × `numShards` ≈ 400 in the headline
  * topology — well inside `Int`. We choose `Long` defensively against future epoch length changes and to match the wire shapes of
  * `EtaPeriod` and `Slot` (both `Long`); a single field-shape across the consensus surface keeps the codecs uniform and the in-memory math
  * promotion-free.
  *
  * @param shardId
  *   shard this accumulator pertains to
  * @param peerId
  *   committee member whose participation is being tallied
  * @param epoch
  *   eta-period the tally covers; written at epoch boundary into the
  *   [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.ShardNonParticipation]] partition
  * @param missedSlotsAsLeader
  *   number of slots inside `epoch` where this peer was elected slot leader but did not emit a checkpoint
  * @param missedAttestationWindows
  *   number of checkpoints inside `epoch` (from other leaders) this peer received but did not attest within the attestation window
  * @param totalSlotsAsLeader
  *   denominator for the missed-slot-leader rate; the count of slots where this peer was elected leader
  * @param totalCheckpointsReceived
  *   denominator for the missed-attestation rate; the count of checkpoints this peer received from other leaders
  */
@derive(encoder, decoder, eqv, show)
final case class ShardNonParticipationCounter(
  shardId: ShardId,
  peerId: PeerId,
  epoch: EtaPeriod,
  missedSlotsAsLeader: Long,
  missedAttestationWindows: Long,
  totalSlotsAsLeader: Long,
  totalCheckpointsReceived: Long
)

object ShardNonParticipationCounter {

  /** Zero counter for a fresh `(shardId, peerId, epoch)` triple. Used by the state manager as the read-side fallback when the partition has
    * no entry yet (i.e., this is the first increment for that key).
    */
  def empty(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod): ShardNonParticipationCounter =
    ShardNonParticipationCounter(shardId, peerId, epoch, 0L, 0L, 0L, 0L)
}
