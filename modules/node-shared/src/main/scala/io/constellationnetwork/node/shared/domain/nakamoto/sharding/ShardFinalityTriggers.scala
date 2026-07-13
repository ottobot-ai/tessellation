package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.Functor
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.FinalityTrigger
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong

/** Per-shard execution-quorum qualification.
  *
  * A checkpoint qualifies only after at least `kQuorum` distinct execution-committee members have signed its exact signing preimage. Shard
  * chain depth is not an alternative validity or liveness path: depth cannot prove that any committee member reproduced the economic
  * transition.
  *
  * This component is producer-side selection state. The consensus-critical embedded checkpoint verifier independently checks the distinct
  * carried signatures against the same `kQuorum`; a node-local tracker can therefore delay selection but can never make an under-signed
  * artifact valid.
  */
final case class ShardFinalityTriggers[F[_]](
  shardId: ShardId,
  tCountShard: FinalityTrigger[F],
  advance: F[ShardOrdinal],
  currentQualifyingCheckpoint: F[Option[Hashed[ShardCheckpoint]]]
) {

  /** Highest ordinal observed with execution quorum. Diagnostic only; consensus selection uses the hash-bound
    * [[currentQualifyingCheckpoint]].
    */
  def latestQualifyingOrdinal(implicit F: Functor[F]): F[ShardOrdinal] =
    tCountShard.latestQualifyingOrdinal.map(ShardFinalityTriggers.snapshotOrdinalToShardOrdinal)
}

object ShardFinalityTriggers {

  def make[F[_]: Async](
    shardId: ShardId,
    kQuorum: Int,
    chainStore: ShardChainStore[F],
    tipTracker: ShardTipTracker[F]
  ): F[ShardFinalityTriggers[F]] = {
    val requiredCount = BigInt(kQuorum)

    // A stored checkpoint already carries the producer's replay-backed signature and may carry other signatures gathered before this node
    // received it. The local tracker contains later gossip contributions. Selection must count the union: relying on tracker delivery alone
    // makes a complete certificate appear one short on the producer when the sidecar does not loop the producer's own publication back.
    // External checkpoints enter the chain store only after signature/replay admission; embedded verification independently validates the
    // spliced certificate, so this union is a liveness selector rather than an artifact-validity shortcut.
    def certificateSignerCount(checkpoint: Hashed[ShardCheckpoint]): F[Int] = {
      val carried = checkpoint.signed.value.committeeSignatures.toList.iterator.map(_.peerId).toSet
      tipTracker.signaturesFor(checkpoint.hash).map(collected => (carried ++ collected.keySet).size)
    }

    val countEval: FinalityTrigger.ConsensusState[F] => F[SnapshotOrdinal] = _ =>
      chainStore.bestTip.flatMap {
        case None => SnapshotOrdinal.MinValue.pure[F]
        case Some(tip) =>
          certificateSignerCount(tip).map { count =>
            if (BigInt(count) >= requiredCount)
              shardOrdinalToSnapshotOrdinal(tip.signed.value.shardOrdinal)
            else SnapshotOrdinal.MinValue
          }
      }

    // Recompute against one captured best-tip ancestry. A monotone ordinal observed for branch A must never qualify branch B at the same
    // ordinal after a shard-chain reorg.
    val currentQualifyingCheckpoint: F[Option[Hashed[ShardCheckpoint]]] =
      chainStore.bestTip.flatMap {
        case None => none[Hashed[ShardCheckpoint]].pure[F]
        case Some(tip) =>
          val bestOrd = tip.signed.value.shardOrdinal.value
          val walkDepth = if (bestOrd == Long.MaxValue) Long.MaxValue else math.max(1L, bestOrd + 1L)

          chainStore.walkBackTo(tip.hash, walkDepth).flatMap {
            _.findM { checkpoint =>
              certificateSignerCount(checkpoint).map(count => BigInt(count) >= requiredCount)
            }
          }
      }

    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TCount, SnapshotOrdinal.MinValue)(countEval).map { tCount =>
      val dummyState = FinalityTrigger.ConsensusState[F](
        selfId = dummySelfId,
        bestTipOrdinal = SnapshotOrdinal.MinValue,
        bestTipHash = dummyHash,
        canonicalHashAt = _ => none[Hash].pure[F]
      )

      ShardFinalityTriggers(
        shardId = shardId,
        tCountShard = tCount,
        advance = tCount.evaluateAndAdvance(dummyState).map(snapshotOrdinalToShardOrdinal),
        currentQualifyingCheckpoint = currentQualifyingCheckpoint
      )
    }
  }

  private[sharding] def shardOrdinalToSnapshotOrdinal(o: ShardOrdinal): SnapshotOrdinal =
    if (o.value <= 0L) SnapshotOrdinal.MinValue
    else SnapshotOrdinal(NonNegLong.unsafeFrom(o.value))

  private[sharding] def snapshotOrdinalToShardOrdinal(o: SnapshotOrdinal): ShardOrdinal =
    ShardOrdinal(o.value.value)

  private val dummySelfId: PeerId =
    PeerId(io.constellationnetwork.security.hex.Hex("00" * 64))

  private val dummyHash: Hash = Hash("0" * 64)
}
