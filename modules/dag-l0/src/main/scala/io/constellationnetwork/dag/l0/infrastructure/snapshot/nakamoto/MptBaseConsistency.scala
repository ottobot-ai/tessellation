package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.Logger

/** Startup base-consistency guard (#56.10 Phase H).
  *
  * On node start, before serving any requests, assert that the on-disk MPT base ordinal is `≤` `chainStore`'s finalized head. A base that is
  * AHEAD of finalized indicates a crash mid-fold-forward (e.g. the trie's deltas were persisted but the chain store hadn't recorded the
  * matching finality yet). Recover by pruning the trie back to `finalized` via `MptStore.deleteAbove`. If pruning isn't applicable (e.g.
  * `chainStore.lastFinalizedOrdinal == 0` while MPT base is non-empty — fresh node with stale `mpt-data/`), fail loudly with a clear
  * operator-actionable error rather than silently rebuilding.
  *
  * Note: `MptStore.deleteAbove` only prunes on-disk state of the producer's storage layer; it does NOT reset `lastSyncedOrdinalRef`. That
  * ref starts at `None` for a freshly-constructed store, so we don't need to clear it — the next `commit` / `sync` advances it normally.
  */
object MptBaseConsistency {

  /** Narrowed dependency on `chainStore`: only `lastFinalizedOrdinal` is read, so the function takes that effect directly. Production
    * callers pass `chainStore.lastFinalizedOrdinal`; tests can pass any `F[Long]` without stubbing the full chain-store algebra.
    */
  def assertBaseConsistentOrPrune[F[_]: Async: Logger](
    mptStore: MptStore[F, GlobalStateKey],
    finalizedOrdinal: F[Long]
  ): F[Unit] =
    for {
      basePersisted <- mptStore.lastPersistedOrdinal
      finalized <- finalizedOrdinal
      _ <- basePersisted match {
        case Some(b) if b.value.value > finalized && finalized > 0L =>
          Logger[F].warn(
            s"[MptBaseConsistency] MPT base ordinal=${b.value.value} is ahead of chainStore finalized=$finalized — " +
              s"pruning trie back to finalized (recovery from crash mid-fold-forward)"
          ) >>
            mptStore.deleteAbove(SnapshotOrdinal(NonNegLong.unsafeFrom(finalized)))

        case Some(b) if b.value.value > finalized && finalized === 0L =>
          // Pathological: persisted MPT base exists but chainStore reports zero finality. We
          // cannot safely prune (no finalized anchor to align to) and we will not silently
          // rebuild from a snapshot file — that's an operator decision. Fail loudly.
          Logger[F].error(
            s"[MptBaseConsistency] MPT base ordinal=${b.value.value} present but chainStore finalized=0. " +
              s"Cannot reconcile automatically — operator must nuke `mpt-data/` and resync from a peer or genesis."
          ) >>
            Async[F].raiseError[Unit](
              new RuntimeException(
                s"MPT base/chain mismatch: base=${b.value.value} finalized=$finalized — operator-triggered resync required"
              )
            )

        case _ =>
          Async[F].unit
      }
    } yield ()
}
