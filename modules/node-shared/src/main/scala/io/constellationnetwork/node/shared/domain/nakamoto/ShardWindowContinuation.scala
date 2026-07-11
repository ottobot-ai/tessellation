package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptyList
import cats.syntax.eq._

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
}
