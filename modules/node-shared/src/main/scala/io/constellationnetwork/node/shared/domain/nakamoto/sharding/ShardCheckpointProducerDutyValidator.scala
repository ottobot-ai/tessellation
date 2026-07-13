package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

/** Receive-side proof that the retained producer signature belongs to the shuffled-staircase owner for the checkpoint's exact slot.
  *
  * `ShardCheckpoint.committeeSignatures.head` is interpreted as the producer under the transitional wire convention. Honest quorum
  * aggregation preserves that head and only appends replay attestations. Although list order is not signed, reordering another member into
  * the head fails this validator because only the unique scheduled signer passes. Explicit producer-role binding remains useful for
  * accountability. A checkpoint whose parent is unavailable cannot prove parent-relative duty and therefore fails closed before replay,
  * storage, or countersigning.
  */
trait ShardCheckpointProducerDutyValidator[F[_]] {
  def validate(checkpoint: ShardCheckpoint, committee: Set[PeerId]): F[Either[String, Unit]]
}

object ShardCheckpointProducerDutyValidator {

  def make[F[_]: Sync: Hasher](
    staircaseDeltaSlots: Int,
    shardEtaFor: (ShardId, EtaPeriod) => F[Option[Array[Byte]]],
    parentCheckpoint: (ShardId, Hash) => F[Option[ShardCheckpoint]]
  ): ShardCheckpointProducerDutyValidator[F] =
    new ShardCheckpointProducerDutyValidator[F] {

      def validate(checkpoint: ShardCheckpoint, committee: Set[PeerId]): F[Either[String, Unit]] = {
        val parentContextF: F[Either[String, Option[ShardCheckpoint]]] =
          if (checkpoint.parentCheckpointHash === Hash.empty) {
            if (checkpoint.shardOrdinal === ShardOrdinal.Root.next)
              Sync[F].pure(Right(None))
            else
              Sync[F].pure(
                Left(
                  s"producer duty invalid genesis lineage: shardOrdinal=${checkpoint.shardOrdinal.value} required=${ShardOrdinal.Root.next.value}"
                )
              )
          } else
            parentCheckpoint(checkpoint.shardId, checkpoint.parentCheckpointHash).flatMap {
              case None =>
                Sync[F].pure(
                  Left(s"producer duty parent unavailable: parentHash=${checkpoint.parentCheckpointHash.value.take(16)}")
                )
              case Some(parent) if parent.shardId =!= checkpoint.shardId =>
                Sync[F].pure(
                  Left(s"producer duty parent shard mismatch: parent=${parent.shardId} checkpoint=${checkpoint.shardId}")
                )
              case Some(parent) if checkpoint.shardOrdinal =!= parent.shardOrdinal.next =>
                Sync[F].pure(
                  Left(
                    s"producer duty non-contiguous ordinal: parent=${parent.shardOrdinal.value} checkpoint=${checkpoint.shardOrdinal.value}"
                  )
                )
              case Some(parent) =>
                Hasher[F].hash(parent.signingPreimage).map { actualParentHash =>
                  Either.cond(
                    actualParentHash === checkpoint.parentCheckpointHash,
                    Some(parent),
                    s"producer duty parent hash mismatch: expected=${checkpoint.parentCheckpointHash.value.take(16)} " +
                      s"actual=${actualParentHash.value.take(16)}"
                  )
                }
            }

        parentContextF.flatMap {
          case Left(reason) => Sync[F].pure(Left(reason))
          case Right(parentOpt) =>
            shardEtaFor(checkpoint.shardId, checkpoint.epoch).flatMap {
              case None => Sync[F].pure(Left(s"producer duty shard eta unavailable for epoch=${checkpoint.epoch.value}"))
              case Some(shardEta) =>
                ShardSlotLeader
                  .dutyOrder[F](committee.toList.sortBy(_.value.value), shardEta, checkpoint.shardOrdinal)
                  .map { ordered =>
                    ShardSlotLeader
                      .scheduledDuty(ordered, checkpoint.slot, parentOpt.map(_.slot), staircaseDeltaSlots)
                      .leftMap(_.diagnostic)
                      .flatMap { duty =>
                        val retainedProducer = checkpoint.producerSignature.peerId
                        Either.cond(
                          retainedProducer === duty.peerId,
                          (),
                          s"off-duty checkpoint producer: retained=${retainedProducer.value.value.take(16)} " +
                            s"scheduled=${duty.peerId.value.value.take(16)} rank=${duty.rank} gap=${duty.slotGap} " +
                            s"delta=${duty.effectiveDeltaSlots}"
                        )
                      }
                  }
            }
        }
      }
    }
}
