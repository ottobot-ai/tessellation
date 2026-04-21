package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.effect.kernel.Ref
import cats.syntax.all._
import cats.{Applicative, Monad}

import io.constellationnetwork.schema.SnapshotOrdinal

/** Single point of truth for "is this snapshot ordinal allowed to leave this node?"
  *
  * The invariant: only finalized data exits GL0 over HTTP. Tentative (pre-finality) snapshots live inside the node during attestation
  * collection and propagate between GL0 peers only via the sidecar GossipSub transport — never via the public/p2p HTTP routes.
  *
  * Two implementations, both in the companion object:
  *
  *   - `passThrough` — BFT mode or non-GL0 layers. Every snapshot in storage is immediately final, so the finalized ordinal is just the
  *     head ordinal and every served ordinal is considered servable.
  *   - `fromRef` — Nakamoto GL0 mode. Backed by a `Ref[F, Long]` updated by the attestation/finality daemon after GRANDPA-style depth-k
  *     finality. Head may run ahead of finalized by a small number of slots; only ordinals at-or-below finalized are servable.
  *
  * Routes take `FinalityGate[F]` as an implicit so gating is uniform across the codebase rather than hand-coded at each call site.
  */
trait FinalityGate[F[_]] {

  /** Latest finalized snapshot ordinal visible to external consumers. None if the node isn't initialized enough to answer yet. */
  def finalizedOrdinal: F[Option[SnapshotOrdinal]]

  /** Whether the given ordinal is within the finalized range (<= finalized ordinal). BFT always true; Nakamoto compares against the
    * finalized ordinal.
    */
  def isServable(ordinal: SnapshotOrdinal): F[Boolean]
}

object FinalityGate {
  def apply[F[_]](implicit F: FinalityGate[F]): FinalityGate[F] = F

  /** BFT / non-GL0 instance: head equals finalized. Caller provides how to read the head so this module doesn't depend on SnapshotStorage.
    */
  def passThrough[F[_]: Applicative](readHeadOrdinal: F[Option[SnapshotOrdinal]]): FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = readHeadOrdinal
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] = true.pure[F]
    }

  /** Nakamoto instance: finalized ordinal is a mutable `SnapshotOrdinal` tracked by the attestation daemon. Seeded with
    * `SnapshotOrdinal.MinIncrementalValue` (ordinal 1 = genesis); grows monotonically.
    */
  def fromRef[F[_]: Monad](ref: Ref[F, SnapshotOrdinal]): FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = ref.get.map(Some(_))
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] =
        ref.get.map(finalized => ordinal <= finalized)
    }

  /** Trivial unconditional instance — everything is servable, finalized is None. Use only in test fixtures where finality doesn't apply. */
  def unrestricted[F[_]: Applicative]: FinalityGate[F] =
    new FinalityGate[F] {
      def finalizedOrdinal: F[Option[SnapshotOrdinal]] = Option.empty[SnapshotOrdinal].pure[F]
      def isServable(ordinal: SnapshotOrdinal): F[Boolean] = true.pure[F]
    }
}
