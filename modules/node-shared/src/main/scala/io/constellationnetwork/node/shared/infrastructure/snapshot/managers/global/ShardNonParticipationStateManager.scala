package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.config.types.ShardSlashingConfig
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardNonParticipationCounter}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

/** Per-(shard, peer, epoch) non-participation accumulator manager — slice 17 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`
  * §10.3.
  *
  * The state manager owns four mutating `record*` entry points (one per duty/total combination) and two read-side `materialize*` entry
  * points. Increments are read-modify-write against the [[GlobalStateFieldId.ShardNonParticipation]] MPT partition: a missing entry is
  * treated as zero, the `+= 1` is applied, and the resulting [[ShardNonParticipationCounter]] is written back under the same key. The
  * partition's entries are then read by [[ShardNonParticipationSlasher]] at epoch boundary.
  *
  * '''Slashing-safety bar — determinism.''' All honest nodes that observe the same sequence of `record*` calls in the same order produce
  * byte-equivalent partition state: integer addition is associative, the codec is byte-canonical, and the MPT key is deterministic from
  * `(shardId, peerId, epoch)`. The only non-determinism risk is interleaving across concurrent writers — call sites MUST serialize the
  * shard's increments through the same path the shard's `accept()` loop serializes through (consensus FSM + checkpoint semaphore already
  * provide this). See `feedback_slashing_safety_bar`.
  *
  * '''Idempotency.''' The `record*` methods are NOT idempotent — each call increments the corresponding counter by 1. Callers that may
  * retry must guard the call with their own dedup (e.g., "already recorded this slot's outcome"). Idempotency at the manager layer would
  * require carrying a per-event ID, which inflates the partition write-amplification with no benefit over caller-side dedup. Tested as
  * "Multiple increments accumulate" in `ShardNonParticipationStateManagerSuite`.
  */
trait ShardNonParticipationStateManager[F[_]] {

  /** Increment `missedSlotsAsLeader` by 1 for `(shardId, peerId, epoch)`. Call when the peer was elected slot leader (so
    * `totalSlotsAsLeader` was previously bumped via [[recordSlotEligibility]]) but the per-shard `accept()` window closed without seeing a
    * checkpoint signed by `peerId`.
    */
  def recordMissedSlot(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit]

  /** Increment `missedAttestationWindows` by 1 for `(shardId, peerId, epoch)`. Call when the peer received a checkpoint authored by some
    * other slot leader (so `totalCheckpointsReceived` was previously bumped via [[recordCheckpointReceived]]) but the per-shard attestation
    * window closed without seeing an attestation signature from `peerId`.
    */
  def recordMissedAttestation(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit]

  /** Increment `totalSlotsAsLeader` by 1 for `(shardId, peerId, epoch)`. Call when the per-shard `EligibilityChecker` elects `peerId` as
    * slot leader for a slot within `epoch`. Always paired with a later [[recordMissedSlot]] if the peer fails to produce.
    */
  def recordSlotEligibility(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit]

  /** Increment `totalCheckpointsReceived` by 1 for `(shardId, peerId, epoch)`. Call when the shard's gossip pipeline delivers a checkpoint
    * to `peerId` from some other slot leader within `epoch`. Always paired with a later [[recordMissedAttestation]] if the peer fails to
    * attest within the window.
    */
  def recordCheckpointReceived(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit]

  /** Point-read the counter for one `(shardId, peerId, epoch)` triple. Returns `None` if no record* call has touched the triple yet (i.e.,
    * the peer has not participated in `epoch` for `shardId`).
    */
  def materializeFromMpt(
    shardId: ShardId,
    peerId: PeerId,
    epoch: EtaPeriod
  )(implicit hasher: Hasher[F]): F[Option[ShardNonParticipationCounter]]

  /** Materialize every counter that pertains to `epoch`. Implementation: prefix-scan the [[GlobalStateFieldId.ShardNonParticipation]]
    * partition, decode each entry, then filter by `epoch`. The resulting map is keyed by the counter's `(ShardId, PeerId)` because the
    * epoch is already constant across the result. Used by [[ShardNonParticipationSlasher.evaluateEpochBoundary]] which needs every
    * peer/shard pair to evaluate the threshold.
    */
  def materializeAllForEpoch(
    epoch: EtaPeriod
  )(implicit hasher: Hasher[F]): F[Map[(ShardId, PeerId), ShardNonParticipationCounter]]
}

object ShardNonParticipationStateManager {

  /** Construct against an [[MptStore]] of [[GlobalStateKey]]. We take the full `MptStore` (not just `GlobalStateReader`) because every
    * `record*` is an RMW — read current counter, increment, write back. Production sites construct one instance per `(shard,
    * gl0-accept-cycle)` and dispose at cycle boundary; tests construct against the in-memory `InMemoryMerklePatriciaProducer`-backed
    * `MptStore.make`.
    */
  def make[F[_]: Async](store: MptStore[F, GlobalStateKey]): ShardNonParticipationStateManager[F] =
    new ShardNonParticipationStateManager[F] {

      private def rmw(
        shardId: ShardId,
        peerId: PeerId,
        epoch: EtaPeriod
      )(modify: ShardNonParticipationCounter => ShardNonParticipationCounter)(implicit hasher: Hasher[F]): F[Unit] =
        for {
          key <- GlobalStateKey.shardNonParticipationKey[F](shardId, peerId, epoch)
          currentOpt <- store.get[ShardNonParticipationCounter](key)
          current = currentOpt.getOrElse(ShardNonParticipationCounter.empty(shardId, peerId, epoch))
          next = modify(current)
          _ <- store.insert[ShardNonParticipationCounter](key, next)
        } yield ()

      def recordMissedSlot(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit] =
        rmw(shardId, peerId, epoch)(c => c.copy(missedSlotsAsLeader = c.missedSlotsAsLeader + 1L))

      def recordMissedAttestation(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit] =
        rmw(shardId, peerId, epoch)(c => c.copy(missedAttestationWindows = c.missedAttestationWindows + 1L))

      def recordSlotEligibility(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit] =
        rmw(shardId, peerId, epoch)(c => c.copy(totalSlotsAsLeader = c.totalSlotsAsLeader + 1L))

      def recordCheckpointReceived(shardId: ShardId, peerId: PeerId, epoch: EtaPeriod)(implicit hasher: Hasher[F]): F[Unit] =
        rmw(shardId, peerId, epoch)(c => c.copy(totalCheckpointsReceived = c.totalCheckpointsReceived + 1L))

      def materializeFromMpt(
        shardId: ShardId,
        peerId: PeerId,
        epoch: EtaPeriod
      )(implicit hasher: Hasher[F]): F[Option[ShardNonParticipationCounter]] =
        GlobalStateKey.shardNonParticipationKey[F](shardId, peerId, epoch).flatMap { key =>
          store.get[ShardNonParticipationCounter](key)
        }

      def materializeAllForEpoch(
        epoch: EtaPeriod
      )(implicit hasher: Hasher[F]): F[Map[(ShardId, PeerId), ShardNonParticipationCounter]] =
        for {
          prefix <- GlobalStateKey.hypergraphFieldPrefix[F](GlobalStateFieldId.ShardNonParticipation)
          all <- store.getAllForPrefix[ShardNonParticipationCounter](prefix)
        } yield
          all.values.iterator
            .filter(_.epoch === epoch)
            .map(c => (c.shardId, c.peerId) -> c)
            .toMap
    }
}

/** Per-epoch boundary slash evaluator — slice 17 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.3.
  *
  * Given the just-closed `EtaPeriod`, materialize every non-participation counter and emit the slash list — distinct `PeerId`s whose
  * missed-rate on either duty exceeds the configured threshold AND whose denominator on that duty meets the minimum sample floor. The slash
  * list is what the gl0 epoch-boundary hook will eventually feed into the standard `SLASHING-DESIGN.md` §5 ledger-effect pipeline; v1
  * returns it for the calling integration slice to wire.
  *
  * '''Determinism (slashing-safety bar).''' Inputs: the partition state at epoch close + the static [[ShardSlashingConfig]]. Outputs: a
  * deterministic `List[PeerId]` sorted by `PeerId.value.value` so two honest nodes compute the same list byte-for-byte. The fold over the
  * MPT prefix-scan results is order-independent (we sort at emit time); the percent comparison uses integer arithmetic (`missed * 100 >
  * total * threshold`) to avoid float drift.
  *
  * '''Why per-peer not per-(peer, shard) in the output.''' The slash list is consumed by the ledger-effect pipeline which acts on peerIds.
  * If a peer is non-participating in multiple shards in the same epoch we don't double-slash (the §10.4 severity tier is per-epoch
  * per-peer). The set semantics in the implementation (toSet → sort) collapse duplicates.
  */
trait ShardNonParticipationSlasher[F[_]] {

  /** Returns the sorted, deduped list of `PeerId`s to slash for non-participation in the just-closed `epoch`. */
  def evaluateEpochBoundary(closedEpoch: EtaPeriod)(implicit hasher: Hasher[F]): F[List[PeerId]]
}

object ShardNonParticipationSlasher {

  /** Construct against a [[ShardNonParticipationStateManager]] reader path + the typed [[ShardSlashingConfig]]. The slasher does NOT take a
    * writer — it's pure observation. Production sites construct one per gl0 epoch-boundary tick; the same instance can be reused across
    * ticks because all of the per-call state is the `closedEpoch` argument.
    */
  def make[F[_]: Async](
    stateManager: ShardNonParticipationStateManager[F],
    config: ShardSlashingConfig
  ): ShardNonParticipationSlasher[F] = new ShardNonParticipationSlasher[F] {

    def evaluateEpochBoundary(closedEpoch: EtaPeriod)(implicit hasher: Hasher[F]): F[List[PeerId]] =
      stateManager.materializeAllForEpoch(closedEpoch).map { byShardPeer =>
        val threshold = config.maxMissedPctPerEpoch
        val minDen = config.minDenominatorPerEpoch
        // Set semantics + sort at the end: collapse per-(shard, peer) duplicates and emit a deterministic ordering. Sort uses PeerId's
        // canonical `Ordering` (lexicographic over the underlying hex). Two honest nodes reading the same partition state produce the same
        // ordered list — the determinism contract for the slash list.
        val slashed: Set[PeerId] = byShardPeer.values.collect {
          case c if shouldSlash(c, threshold, minDen) => c.peerId
        }.toSet
        slashed.toList.sorted
      }

    /** Slash predicate: a peer is slashed if EITHER the slot-leader missed-rate or the attestation-window missed-rate exceeds `threshold`
      * percent AND the corresponding denominator meets the minimum sample floor. Integer-arithmetic comparison (`missed * 100 > total *
      * threshold`) keeps the algebra deterministic and overflow-safe for the per-epoch tally sizes expected (≤ etaRotationSnapshots).
      */
    private def shouldSlash(c: ShardNonParticipationCounter, threshold: Int, minDen: Long): Boolean = {
      val slotRateExceeded =
        c.totalSlotsAsLeader >= minDen &&
          c.missedSlotsAsLeader * 100L > c.totalSlotsAsLeader * threshold.toLong
      val attRateExceeded =
        c.totalCheckpointsReceived >= minDen &&
          c.missedAttestationWindows * 100L > c.totalCheckpointsReceived * threshold.toLong
      slotRateExceeded || attRateExceeded
    }
  }
}
