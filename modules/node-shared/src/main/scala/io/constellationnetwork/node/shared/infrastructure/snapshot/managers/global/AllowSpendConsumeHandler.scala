package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.CrossShardMessageOrderings._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendAction
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec.{immutableCodec => consumedAllowSpendImmutableCodec}

/** Instance 1 of [[CrossShardMessageHandler]] — the cross-shard ALLOW-SPEND consume over the `ConsumedAllowSpends` nullifier partition
  * (fieldId 33). It owns the NULLIFIER half of I-ONCE for allow-spends: classify cross-shard consumes, reject double-consume / replay /
  * no-such-reservation, and emit the `ConsumedAllowSpend` markers.
  *
  * The guts are the existing [[ConsumedAllowSpendStateManager]] (`materializeConsumedAllowSpendsFromMpt` / `classifyCrossShardConsumes` /
  * `settleCrossShardConsumes`) — this handler adapts them into the generic [[CrossShardMessageHandler.settle]] shape (prior-set
  * materialization + classify + settle + encode markers to `GlobalStateKey → bytes`). The allow-spend STATE EFFECT — the read-side
  * effective-CURRENCY-balance overlay ([[ConsumedAllowSpendStateManager.effectiveCurrencyBalances]]) applied at the `SpendActionValidator`
  * and the `GL0CurrencyBalanceRoutes` read sites — stays DELIBERATELY outside this handler (it is type-specific and safety-critical; a
  * future message type defines its own effect at its own read sites).
  */
final case class AllowSpendConsumeHandler[F[_]: Async](
  manager: ConsumedAllowSpendStateManager[F]
) extends CrossShardMessageHandler[F] {

  val nullifierFieldId: GlobalStateFieldId = GlobalStateFieldId.ConsumedAllowSpends

  def settle(
    acceptedSpendActions: SortedMap[Address, List[SpendAction]],
    shardAssignment: ShardAssignment[F],
    ordinal: SnapshotOrdinal
  )(implicit hasher: Hasher[F]): F[CrossShardSettlementWrite] =
    for {
      candidates <- manager.classifyCrossShardConsumes(acceptedSpendActions, shardAssignment)
      result <-
        if (candidates.isEmpty) CrossShardSettlementWrite.empty.pure[F]
        else
          for {
            // The include-check needs M's active-allow-spend mirror; materialize it alongside the prior nullifier set.
            lastActiveAllowSpends <- manager.materializeActiveAllowSpendsFromMpt
            priorNullifierSet <- manager.materializeConsumedAllowSpendsFromMpt
            settlement <- manager.settleCrossShardConsumes(candidates, priorNullifierSet, lastActiveAllowSpends, ordinal)
            markers = settlement.newMarkers.toList.foldLeft(SortedMap.empty[GlobalStateKey, Array[Byte]]) {
              case (acc, (h, marker)) =>
                acc.updated(
                  GlobalStateKey.consumedAllowSpendKey(h),
                  consumedAllowSpendImmutableCodec.immutableBytes(marker).toArray
                )
            }
          } yield CrossShardSettlementWrite(markers, settlement.rejected)
    } yield result
}
