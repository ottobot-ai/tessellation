package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.data.NonEmptyList
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

/** Deterministically classifies a checkpoint window against GL0's committed per-metagraph state-channel tip.
  *
  * Only an exact continuation may introduce new state. If the window tail is already the committed tip, the window is a no-op. Every other
  * lineage is deferred until its actual parent is adopted; GL0 never reconstructs a sibling lineage by ordinal or replaces prior state with
  * committee-carried data.
  */
object ShardWindowContinuation {

  sealed trait Decision

  /** A window binary's parent is GL0's tip. Adopt the suffix beginning at `idx`. */
  final case class Continue(idx: Int) extends Decision

  /** The window tail is GL0's committed tip. Nothing in this window remains to adopt. */
  case object AlreadyAdopted extends Decision

  /** The window neither continues nor ends at GL0's tip. It cannot be replayed from the committed pre-state. */
  case object Defer extends Decision

  /** The window tail's value hash, in the same hash space used by state-channel parent links and GL0's committed tip. */
  def windowTipHashF[F[_]](
    nel: NonEmptyList[Signed[StateChannelSnapshotBinary]]
  )(implicit hasher: Hasher[F]): F[Hash] =
    hasher.hash(nel.last.value)

  def classify(
    nel: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    windowTipHash: Hash,
    tipHash: Hash
  ): Decision = {
    val idx = nel.toList.indexWhere(_.value.lastSnapshotHash === tipHash)
    if (idx >= 0) Continue(idx)
    else if (windowTipHash === tipHash) AlreadyAdopted
    else Defer
  }

  /** A checkpoint is eligible for atomic adoption only when at least one window continues GL0 state and no window is deferred. */
  def isAtomicallyContinuableF[F[_]: Monad](
    outerShardId: ShardId,
    checkpoint: ShardCheckpoint,
    priorTips: SortedMap[Address, Hash]
  )(implicit hasher: Hasher[F]): F[Boolean] =
    if (outerShardId =!= checkpoint.shardId) false.pure[F]
    else
      classifyAllF(checkpoint, priorTips).map { decisions =>
        decisions.exists { case (_, Continue(_)) => true; case _ => false } &&
        decisions.forall { case (_, Defer) => false; case _ => true }
      }

  /** Proves that a checkpoint embedded in a GL0 snapshot was applied atomically.
    *
    * Every continuing window must appear as the exact accepted suffix in that GL0 artifact. Already-applied windows are no-ops. A deferred,
    * missing, shortened, or otherwise rejected suffix makes the whole checkpoint ineligible for a Phase-2 shard anchor.
    */
  def wasFullyAppliedF[F[_]: Monad](
    outerShardId: ShardId,
    checkpoint: ShardCheckpoint,
    priorTips: SortedMap[Address, Hash],
    acceptedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  )(implicit hasher: Hasher[F]): F[Boolean] =
    if (outerShardId =!= checkpoint.shardId) false.pure[F]
    else
      classifyAllF(checkpoint, priorTips).flatMap { decisions =>
        val hasContinuation = decisions.exists { case (_, Continue(_)) => true; case _ => false }
        if (!hasContinuation) false.pure[F]
        else
          decisions.forallM {
            case (mg, Continue(idx)) =>
              val expected = NonEmptyList.fromList(checkpoint.derivedStateDelta.includedSnapshots(mg).toList.drop(idx))
              (expected, acceptedSnapshots.get(mg)) match {
                case (Some(suffix), Some(actual)) =>
                  (
                    suffix.toList.traverse(hasher.hash(_)),
                    actual.toList.traverse(hasher.hash(_))
                  ).mapN(_ === _)
                case _ => false.pure[F]
              }
            case (_, AlreadyAdopted) => true.pure[F]
            case (_, Defer)          => false.pure[F]
          }
      }

  private def classifyAllF[F[_]: Monad](
    checkpoint: ShardCheckpoint,
    priorTips: SortedMap[Address, Hash]
  )(implicit hasher: Hasher[F]): F[List[(Address, Decision)]] =
    checkpoint.derivedStateDelta.includedSnapshots.toList.traverse {
      case (mg, nel) =>
        windowTipHashF(nel).map { windowTipHash =>
          mg -> classify(nel, windowTipHash, priorTips.getOrElse(mg, Hash.empty))
        }
    }
}
