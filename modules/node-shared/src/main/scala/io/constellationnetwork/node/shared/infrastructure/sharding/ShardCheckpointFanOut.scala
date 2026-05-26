package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.Logger

/** Shared shard-checkpoint producer fan-out — the seam that drives each per-shard [[ShardCheckpointProducer]] for ONE canonical gl0
  * ordinal, decoupled from the gl0-leader win.
  *
  * '''Why this exists (decoupling per `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.1).''' Production used to live only inside
  * `SnapshotLeaderLoop.onSlotWon`, which runs only on the node that won the gl0 slot. That made checkpoint production require a
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
  * '''§15.5 — content-only production.''' Each producer is handed only the SC binaries partitioned to ITS shard. A shard with no content
  * this ord receives an empty `SortedMap`, and [[ShardCheckpointProducer.produce]] returns `None` on empty input — so an empty shard
  * produces nothing and no empty checkpoint reaches gl0.
  */
object ShardCheckpointFanOut {

  /** Recover the VRF output (beta) from VRF proof bytes.
    *
    * '''Why duplicated here (module layering).''' `NakamotoSyncDaemon.vrfOutputFromProof` is the canonical copy, but it lives in `dag-l0`;
    * `node-shared` (this module) cannot reference it without inverting the dependency direction. Both copies are the SAME one-liner over the
    * SAME `EcVrf25519.default.vrfProofToHash`, so producer + receiver store byte-identical `vrfOutput` for the same checkpoint. If a future
    * refactor lifts the daemon helper into `shared`/`node-shared`, both sites should re-route through it.
    */
  private val vrf = EcVrf25519.default

  private[sharding] def vrfOutputFromProof(proofBytes: Array[Byte]): Array[Byte] =
    vrf.vrfProofToHash(proofBytes).getOrElse(proofBytes) // fallback to raw proof if derivation fails

  /** Drive every per-shard producer for the single gl0 ord `producedOrd`.
    *
    * @param stateChannelSnapshots
    *   the canonical SC binaries for THIS gl0 ord (the produced/gossip-received snapshot's `stateChannelSnapshots`). Partitioned per shard
    *   via `shardAssignment.shardIdFor` — the SAME deterministic mapping every gl0 operator computes.
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
    * @param shardAssignment
    *   static metagraph → shard mapping used to partition `stateChannelSnapshots`.
    */
  def run[F[_]: Async: Hasher](
    stateChannelSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    producedOrd: SnapshotOrdinal,
    epoch: EtaPeriod,
    shardProducers: Map[ShardId, ShardCheckpointProducer[F]],
    shardChainStores: Map[ShardId, ShardChainStore[F]],
    shardAssignment: ShardAssignment[F],
    logger: Logger[F]
  ): F[Unit] =
    // Group MG addresses by their shard once, then drive each producer. `traverse_` over an empty
    // `shardProducers` map is a no-op — the regression-bar short-circuit.
    stateChannelSnapshots.toList.traverse {
      case (mgAddr, binaries) => shardAssignment.shardIdFor(mgAddr).map(sid => sid -> (mgAddr, binaries))
    }.map { tagged =>
      tagged.groupBy(_._1).map {
        case (sid, entries) =>
          sid -> SortedMap.from(entries.map(_._2))(Address.OrderingInstance)
      }
    }.flatMap { perShard =>
      shardProducers.toList.traverse_ {
        case (sid, producer) =>
          val forShard = perShard.getOrElse(
            sid,
            SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]](Address.OrderingInstance)
          )
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
