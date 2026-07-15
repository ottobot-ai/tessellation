package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.Logger

/** Post-bootstrap base-consistency guard (#56.10 Phase H).
  *
  * After restart/rollback/genesis has loaded and root-verified a local snapshot head, assert that the on-disk MPT base ordinal is `≤` that
  * exact recovery anchor. A base ahead of the restored head indicates a crash or rollback that left later trie files behind. Prune those
  * files back to the authenticated local anchor via `MptStore.deleteAbove`. The empty in-memory Nakamoto chain store is deliberately not
  * used as authority here; its finality watermark is not restored yet.
  *
  * Note: `MptStore.deleteAbove` only prunes on-disk state of the producer's storage layer; it does NOT reset `lastSyncedOrdinalRef`. That
  * ref starts at `None` for a freshly-constructed store, so we don't need to clear it — the next `commit` / `sync` advances it normally.
  */
object MptBaseConsistency {

  /** The caller supplies the exact root-verified local recovery anchor. Tests can pass any `F[Long]` without constructing snapshot storage.
    */
  def assertBaseConsistentOrPrune[F[_]: Async: Logger](
    mptStore: MptStore[F, GlobalStateKey],
    recoveryAnchorOrdinal: F[Long]
  ): F[Unit] =
    for {
      basePersisted <- mptStore.lastPersistedOrdinal
      recoveryAnchor <- recoveryAnchorOrdinal
      _ <- basePersisted match {
        case Some(b) if b.value.value > recoveryAnchor && recoveryAnchor > 0L =>
          Logger[F].warn(
            s"[MptBaseConsistency] MPT base ordinal=${b.value.value} is ahead of restored head=$recoveryAnchor — " +
              s"pruning trie back to the root-verified recovery anchor"
          ) >>
            mptStore.deleteAbove(SnapshotOrdinal(NonNegLong.unsafeFrom(recoveryAnchor)))

        case Some(b) if b.value.value > recoveryAnchor && recoveryAnchor === 0L =>
          Logger[F].error(
            s"[MptBaseConsistency] MPT base ordinal=${b.value.value} present but restored recovery anchor=0. " +
              s"Cannot reconcile automatically without an authenticated non-zero snapshot head."
          ) >>
            Async[F].raiseError[Unit](
              new RuntimeException(
                s"MPT base/recovery mismatch: base=${b.value.value} anchor=$recoveryAnchor — authenticated resync required"
              )
            )

        case _ =>
          Async[F].unit
      }
    } yield ()
}
