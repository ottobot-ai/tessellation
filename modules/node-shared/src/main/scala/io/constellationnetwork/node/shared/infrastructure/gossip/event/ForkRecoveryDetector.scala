package io.constellationnetwork.node.shared.infrastructure.gossip.event

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Information about a detected fork divergence.
  *
  * @param majorityOrdinal
  *   The ordinal that the majority of peers agree on
  * @param majorityHash
  *   The snapshot hash at the majority ordinal
  * @param majorityPeers
  *   The set of peers on the majority chain
  * @param localOrdinal
  *   This node's current ordinal
  * @param lag
  *   How far behind the majority this node is
  */
case class ForkRecoveryInfo(
  majorityOrdinal: SnapshotOrdinal,
  majorityHash: Hash,
  majorityPeers: Set[PeerId],
  localOrdinal: SnapshotOrdinal,
  lag: Long
)

/** Detects fork divergence by comparing local chain tip against peer chain tips collected via gossip.
  *
  * When a node is on a fork, its ordinal will fall behind the majority of peers. This detector identifies that
  * situation by comparing the local ordinal against the majority ordinal reported by peers through IHave chain tip
  * metadata.
  */
trait ForkRecoveryDetector[F[_]] {
  def detectForkDivergence: F[Option[ForkRecoveryInfo]]
}

object ForkRecoveryDetector {

  def make[F[_]: Async](
    meshState: MeshState[F],
    getLocalOrdinal: F[Option[SnapshotOrdinal]],
    forkLagThreshold: Long = 5
  ): ForkRecoveryDetector[F] = new ForkRecoveryDetector[F] {

    private val logger = Slf4jLogger.getLogger[F]

    def detectForkDivergence: F[Option[ForkRecoveryInfo]] =
      for {
        chainTips <- meshState.getChainTips
        localOrdinalOpt <- getLocalOrdinal
        result <- (localOrdinalOpt, chainTips.nonEmpty).pure[F].flatMap {
          case (Some(localOrdinal), true) =>
            val ordinalGroups = chainTips.groupBy(_._2.ordinal)
            val (majorityOrdinal, majorityGroup) = ordinalGroups.maxBy(_._2.size)
            val lag = majorityOrdinal.value.value - localOrdinal.value.value

            if (lag > forkLagThreshold && majorityGroup.size > chainTips.size / 2) {
              val majorityHash = majorityGroup.values.head.snapshotHash
              val info = ForkRecoveryInfo(
                majorityOrdinal = majorityOrdinal,
                majorityHash = majorityHash,
                majorityPeers = majorityGroup.keySet,
                localOrdinal = localOrdinal,
                lag = lag
              )
              logger.warn(
                s"Fork divergence detected: local=${localOrdinal.value.value} " +
                  s"majority=${majorityOrdinal.value.value} lag=$lag " +
                  s"majorityPeers=${majorityGroup.size}/${chainTips.size}"
              ).as(info.some)
            } else none[ForkRecoveryInfo].pure[F]
          case _ => none[ForkRecoveryInfo].pure[F]
        }
      } yield result
  }
}
