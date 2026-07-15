package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.Functor
import cats.syntax.functor._

import io.constellationnetwork.node.shared.domain.nakamoto.{HistoricalOperatorRegistryView, HistoricalOperatorRegistryViewSource}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash

/** Exact-hash hot-chain source for historical operator-registry views.
  *
  * The historical key resolver needs the authenticated `GlobalSnapshotInfo` reproduced for one exact candidate-parent snapshot. Only the
  * in-memory `NakamotoChainStore.get(hash)` entry has that context. An ordinal/disk fallback can return another branch or a placeholder
  * context, while a best-tip/current-registry fallback would answer a different consensus question.
  */
object NakamotoChainStoreHistoricalOperatorRegistryViewSource {

  def make[F[_]: Functor](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F]
  ): HistoricalOperatorRegistryViewSource[F] = new HistoricalOperatorRegistryViewSource[F] {
    def get(candidateParent: Hash): F[Option[HistoricalOperatorRegistryView]] =
      chainStore.get(candidateParent).map(_.flatMap(stored => Option(stored).flatMap(toView(candidateParent, _))))
  }

  private[nakamoto] def toView(
    requestedHash: Hash,
    stored: NakamotoChainStore.StoredSnapshot
  ): Option[HistoricalOperatorRegistryView] =
    for {
      signedSnapshot <- Option(stored.signedSnapshot)
      signed <- Option(signedSnapshot.value)
      context <- Option(stored.context)
      ordinal <- SnapshotOrdinal(stored.ordinal)
      if ordinal == signed.ordinal
      if stored.hash == requestedHash
      if stored.parentHash == signed.lastSnapshotHash
    } yield
      HistoricalOperatorRegistryView(
        parentHash = requestedHash,
        parentOrdinal = ordinal,
        info = context
      )
}
