package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardBinaryBuffer, ShardChainStore}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.Logger

/** Shared shard-checkpoint producer fan-out — the per-slot seam that drives each per-shard [[ShardCheckpointProducer]], decoupled from the
  * global GL0 leader win.
  *
  * Production is called from the ready branch of `SnapshotLeaderLoop` on every slot. Each eligible GL0 operator checks public execution
  * membership and the producer's staircase duty; a shard with no content emits nothing. The GL0 base is a complete retained state claim
  * accepted by the current transitional finality resolver; the target hash-bound Phase-2 lease remains open. Several slots may attempt
  * against the same base while the single-outstanding checkpoint rule and chain store prevent duplicate advancement. There is no checkpoint
  * pipeline-depth parameter.
  *
  * '''numShards = 1 regression bar.''' Both call sites gate this on `shardProducers.nonEmpty && shardAssignment.isDefined`. At numShards=1
  * those are `Map.empty` / `None` ⇒ the call site is `whenA(false)` ⇒ this helper is never entered (no allocation, no log). Even if entered
  * with an empty producer map, `traverse_` over Nil is a no-op (the empty-producer-map short-circuit is preserved).
  *
  * '''§15.5 — content-only production.''' Each producer is handed only the binaries buffered for ITS shard. A shard with no buffered
  * content receives an empty `SortedMap`, and [[ShardCheckpointProducer.produce]] returns `None` on empty input — so an empty shard
  * produces nothing and no empty checkpoint reaches gl0.
  *
  * '''EXECUTION-SHARDING R-1 — the inversion (input source).''' The fan-out NO LONGER partitions gl0's post-chain-link
  * `stateChannelSnapshots` map (which the `CHANGE-3` Axis-1a filter empties at `numShards > 1`, and which inherits gl0's #259 freeze).
  * Instead each producer is fed `shardBinaryBuffers(sid).snapshotPending` — the complete signed binaries that passed metagraph admission
  * and were buffered for this shard off the global binaries gossip topic. The producer chain-link-orders them from the exact pinned GL0
  * execution-base tips it will replay. Production is driven by what the shard has buffered and is decoupled from GL0's post-chain-link
  * event selection, but the economic lineage remains anchored to retained global state. The buffer is already shard-scoped (the daemon
  * buffers by `ShardAssignment.shardIdFor`), so no partition step is needed here.
  */
object ShardCheckpointFanOut {

  /** Drive every per-shard producer for the single gl0 ord `producedOrd`.
    *
    * @param shardBinaryBuffers
    *   per-shard admission-approved binary accumulators keyed by `ShardId`. For each producer, `snapshotPending` on its shard's buffer is
    *   the input, not GL0's post-chain-link map. A shard with an absent / empty buffer feeds the producer an empty map ⇒ `produce` returns
    *   `None`. SAME instances the daemon's gossip-intake writes into.
    * @param producedOrd
    *   the Phase-2 GL0 ordinal currently used both as the legacy `gl0AnchorOrdinal` scheduling hint and the ordinal component of the pinned
    *   replay context. The target checkpoint binds its exact hash and root as well. The independent `currentSlot` drives staircase duty
    * @param epoch
    *   eta-rotation period for `producedOrd` (`EtaPeriod(EtaCalculation.rotationPeriod(producedOrd, etaRotationSnapshots))`). Threaded onto
    *   the produced checkpoint's `epoch` field so verifiers look up the right active set.
    * @param shardProducers
    *   per-shard producers keyed by `ShardId`. Empty ⇒ no-op `traverse_` (the short-circuit).
    * @param shardChainStores
    *   the SAME per-shard chain stores the producers + the acceptance side share. On `Some(checkpoint)` the producing node stores its own
    *   checkpoint here so local execution-quorum collection can begin without waiting for its own gossip echo.
    * @param selfPeerId
    *   this GL0 operator's PeerId. A producer runs for shard `s` only when `selfPeerId` is in the public deterministic
    *   `committeeMembership(s, epoch)` (Task 2 membership gate) — a non-member's checkpoint can never reach committee quorum at any
    *   verifier's `verifyEmbedded`, so producing it is wasted work / liveness drag.
    * @param committeeMembership
    *   the SAME deterministic `committeeFor(shardId, epoch)` draw the acceptance manager uses (from `AcceptanceDeps.committeeMembership`).
    *   Gating produce through it keeps "who may produce for shard s" consistent with "whose signature counts toward quorum for shard s".
    */
  def run[F[_]: Async: Hasher](
    shardBinaryBuffers: Map[ShardId, ShardBinaryBuffer[F]],
    producedOrd: SnapshotOrdinal,
    epoch: EtaPeriod,
    currentSlot: Slot,
    shardProducers: Map[ShardId, ShardCheckpointProducer[F]],
    shardChainStores: Map[ShardId, ShardChainStore[F]],
    selfPeerId: PeerId,
    committeeMembership: (ShardId, EtaPeriod) => F[Set[PeerId]],
    logger: Logger[F]
  ): F[Unit] =
    // Drive each producer from ITS shard's buffered binaries (the inversion). `traverse_` over an empty
    // `shardProducers` map is a no-op — the regression-bar short-circuit.
    shardProducers.toList.traverse_ {
      case (sid, producer) =>
        // EXECUTION-SHARDING committee gate: only run the producer for shard
        // `sid` if THIS node is in `committeeMembership(sid, epoch)`. A non-member's checkpoint can never reach committee quorum at
        // any verifier's `verifyEmbedded` (its signer fails the membership pre-check), so producing it is pure wasted work (and a
        // non-member taking a producer duty would emit a checkpoint no quorum can attest → liveness drag). Gate the whole
        // produce path on membership; `committeeMembership` is the SAME deterministic draw the acceptance manager uses, so the gate
        // is consistent with admission cluster-wide.
        committeeMembership(sid, epoch).flatMap { committee =>
          if (!committee.contains(selfPeerId))
            // INFO (not debug): produce-skips were invisible during the 2026-06-10 silent shard-chain stall —
            // every skip path must say WHY at a visible level (one line per ord per shard; drop to a metric
            // when task #25 lands). committeeSize=0 here would mean the (shard, epoch) draw came up EMPTY —
            // the epoch-rotation failure mode that stalls all producers at once.
            logger.info(
              s"🧩 produce-skip shard=${sid.value.value} epoch=${epoch.value} reason=not-in-committee " +
                s"committeeSize=${committee.size}"
            )
          else {
            // Read this shard's buffered raw binaries (non-destructive). Absent buffer ⇒ empty input ⇒ producer returns None.
            val pendingF: F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] =
              shardBinaryBuffers.get(sid) match {
                case Some(buffer) => buffer.snapshotPending
                case None =>
                  Async[F].pure(
                    SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]](Address.OrderingInstance)
                  )
              }
            pendingF.flatMap { forShard =>
              producer
                .produce(forShard, producedOrd, epoch, currentSlot, committee)
                .flatMap {
                  case None             => Async[F].unit
                  case Some(checkpoint) =>
                    // Chain-store write keyed on the envelope's WIRE slot (design §5.7) — the same value every
                    // receiver stores, so `maxvalid-tk` tiebreaks agree byte-for-byte. vrfOutput is derived from
                    // the producer's own committee VRF proof (first committee signature), mirroring the
                    // receiver-side `vrfOutputFromProof` recovery so both writers store byte-identical vrfOutput
                    // for the same checkpoint.
                    val cp = checkpoint.value
                    shardChainStores.get(sid) match {
                      case Some(store) =>
                        ShardCheckpointChainStoreRecovery.ingestValidated(cp, store).flatMap { recovered =>
                          logger.info(
                            s"🧩 Shard producer: stored own checkpoint shard=${sid.value.value} " +
                              s"shardOrd=${cp.shardOrdinal.value} gl0Anchor=${cp.gl0AnchorOrdinal.value.value} " +
                              s"new=${recovered.inserted} mgs=${forShard.size}"
                          )
                        }
                      case None =>
                        logger.warn(
                          s"🧩 Shard producer won shard=${sid.value.value} but no chain store registered; checkpoint not stored locally"
                        )
                    }
                }
            }
          }
        }
    }
}
