package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardBinaryBuffer, ShardChainStore}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.Logger

/** Shared shard-checkpoint producer fan-out — the seam that drives each per-shard [[ShardCheckpointProducer]] for ONE canonical gl0
  * ordinal, decoupled from the gl0-leader win.
  *
  * '''Why this exists (decoupling per `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.1).''' Production used to live only
  * inside `SnapshotLeaderLoop.onSlotWon`, which runs only on the node that won the gl0 slot. That made checkpoint production require a
  * triple-coincidence (win gl0 slot + win shard slot + content present), so most shards stayed silent. The design intent is one checkpoint
  * per shard per gl0 ord, produced by the shard committee's slot leader — independent of who produced the gl0 snapshot. This helper is the
  * shared body invoked from BOTH seams:
  *   - `SnapshotLeaderLoop.onSlotWon` — the gl0 leader fans out for its OWN produced ord (GossipSub does not echo a publisher its own
  *     message, so the leader never sees its own ord on the gossip path — this self-call is required, not redundant).
  *   - `NakamotoSyncDaemon.processValidSnapshotInner` (inside the `becameBestTip` branch) — every node fans out for each canonical
  *     gossip-received ord.
  *
  * Together these fire the fan-out '''exactly once per canonical ord per node''' (leader via onSlotWon for its own ord; everyone else via
  * the daemon for gossip-received ords). `ShardChainStore.store` is idempotent by hash, so an accidental double on the same checkpoint
  * dedups — but the design is exactly-once.
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
  * Instead each producer is fed `shardBinaryBuffers(sid).snapshotPending` — the RAW metagraph binaries the committee buffered for ITS shard
  * off the global binaries gossip topic. The producer then chain-link-orders them off the SHARD's own prior-checkpoint tip
  * (`ShardChainStore.perMgTip`). This is the centerpiece of the inversion: production is driven by what the shard has buffered, fully
  * decoupled from gl0's chain-link admission. The buffer is already shard-scoped (the daemon buffers by `ShardAssignment.shardIdFor`), so
  * no partition step is needed here.
  */
object ShardCheckpointFanOut {

  /** Recover the VRF output (beta) from VRF proof bytes.
    *
    * '''Why duplicated here (module layering).''' `NakamotoSyncDaemon.vrfOutputFromProof` is the canonical copy, but it lives in `dag-l0`;
    * `node-shared` (this module) cannot reference it without inverting the dependency direction. Both copies are the SAME one-liner over
    * the SAME `EcVrf25519.default.vrfProofToHash`, so producer + receiver store byte-identical `vrfOutput` for the same checkpoint. If a
    * future refactor lifts the daemon helper into `shared`/`node-shared`, both sites should re-route through it.
    */
  private val vrf = EcVrf25519.default

  private[sharding] def vrfOutputFromProof(proofBytes: Array[Byte]): Array[Byte] =
    vrf.vrfProofToHash(proofBytes).getOrElse(proofBytes) // fallback to raw proof if derivation fails

  /** Drive every per-shard producer for the single gl0 ord `producedOrd`.
    *
    * @param shardBinaryBuffers
    *   per-shard raw-binary accumulators keyed by `ShardId` (EXECUTION-SHARDING R-1). For each producer, `snapshotPending` on its shard's
    *   buffer is the input — the RAW metagraph binaries the committee buffered for that shard, NOT gl0's post-chain-link map. A shard with
    *   an absent / empty buffer feeds the producer an empty map ⇒ `produce` returns `None`. SAME instances the daemon's gossip-intake
    *   writes into.
    * @param producedOrd
    *   the gl0 ordinal this fan-out is for. Passed to each producer as `gl0AnchorOrdinal` and used to recompute the shard-local slot.
    * @param epoch
    *   eta-rotation period for `producedOrd` (`EtaPeriod(EtaCalculation.rotationPeriod(producedOrd, etaRotationSnapshots))`). Threaded onto
    *   the produced checkpoint's `epoch` field so verifiers look up the right active set.
    * @param shardProducers
    *   per-shard producers keyed by `ShardId`. Empty ⇒ no-op `traverse_` (the short-circuit).
    * @param shardChainStores
    *   the SAME per-shard chain stores the producers + the acceptance side share. On `Some(checkpoint)` the producing node stores its own
    *   checkpoint here so the local chain advances toward finality without waiting for its own gossip echo.
    * @param selfPeerId
    *   this gl0 operator's PeerId. With real VRF-VK committees a producer runs for shard `s` only when `selfPeerId` is in
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
        // EXECUTION-SHARDING committee gate (Task 2): with real VRF-VK-sortitioned committees, only run the producer for shard
        // `sid` if THIS node is in `committeeMembership(sid, epoch)`. A non-member's checkpoint can never reach committee quorum at
        // any verifier's `verifyEmbedded` (its signer fails the membership pre-check), so producing it is pure wasted work (and a
        // non-member winning the LEADER lottery would emit a checkpoint no quorum can attest → liveness drag). Gate the whole
        // produce path on membership; `committeeMembership` is the SAME deterministic draw the acceptance manager uses, so the gate
        // is consistent with admission cluster-wide.
        committeeMembership(sid, epoch).flatMap { committee =>
          if (!committee.contains(selfPeerId))
            logger.debug(
              s"🧩 Shard producer: self not in committee for shard=${sid.value.value} epoch=${epoch.value}; skipping produce"
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
                .produce(forShard, producedOrd, epoch)
                .flatMap {
                  case None             => Async[F].unit
                  case Some(checkpoint) =>
                    // Recompute the producer's own (slot, vrfOutput) for the chain-store write.
                    // `slotForGl0Anchor(gl0AnchorOrdinal) = Slot(gl0AnchorOrdinal.value)` — identical
                    // pure mapping on producer + receiver so `maxvalid-tk` tiebreaks agree. vrfOutput is
                    // derived from the producer's own committee VRF proof (first committee signature),
                    // mirroring the receiver-side `vrfOutputFromProof` recovery so both writers store
                    // byte-identical vrfOutput for the same checkpoint.
                    val cp = checkpoint.value
                    val localSlot = cp.gl0AnchorOrdinal.value.value
                    val vrfProofBytes = cp.committeeSignatures.head.vrfProof.toBytes
                    val vrfOut = vrfOutputFromProof(vrfProofBytes)
                    shardChainStores.get(sid) match {
                      case Some(store) =>
                        store
                          .store(checkpoint, cp.parentCheckpointHash, cp.shardOrdinal, localSlot, vrfOut)
                          .flatMap { stored =>
                            logger.info(
                              s"🧩 Shard producer: stored own checkpoint shard=${sid.value.value} " +
                                s"shardOrd=${cp.shardOrdinal.value} gl0Anchor=${cp.gl0AnchorOrdinal.value.value} " +
                                s"new=$stored mgs=${forShard.size}"
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
