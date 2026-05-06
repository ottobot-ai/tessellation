package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, MptTxAction}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.mpt.prover.{MerklePatriciaProofError, MerklePatriciaSingleInclusionProver}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Proof generation at a specific ordinal, including historical ordinals within the undo-journal window.
  *
  * Strategy (legacy paths — kept until #56.10 retires `MptUndoJournal`):
  *   - **Current ordinal**: direct traversal of the in-memory trie (fast path).
  *   - **Historical within the journal window**: take a savepoint, `unapplyTo(ordinal)` to roll the flat state back, rebuild the trie at
  *     that ordinal, generate the proof, restore the savepoint. This uses the `MptUndoJournal` infrastructure that was designed for exactly
  *     this case (`MptUndoJournal.scala:46-51` comment — "deferred until inclusion-proof features land that require reconstructing the trie
  *     root at a historical ordinal").
  *   - **Beyond the journal window**: returns `OutOfWindow` — the caller can fall back to snapshot-file replay if they need deep history
  *     (deferred future work).
  *
  * The savepoint/unapply path serializes through `MptStore.withExclusiveLock` so concurrent mutations can't interleave.
  *
  * Branch-aware path (#56.7 — additive): `proofAtBranch(branch, ordinal, key)` materializes the trie at a specific branch view via
  * `MptOverlay.buildRoot`, no journal walk. Used for branch-isolated stateProofs once accept() migrates to the overlay (#56.10). Keeps the
  * `withExclusiveLock` defensive wrap until accept() stops mutating `MptStore` directly — at that point the lock can drop because finalized
  * state on disk is immutable.
  */
trait HistoricalMptProofService[F[_]] {

  /** Legacy ordinal-based proof. Walks the journal for historical ordinals; uses the live trie for current ordinal. Replaced by
    * `proofAtBranch` once accept() migrates (#56.10).
    */
  def proofAt(
    ordinal: SnapshotOrdinal,
    key: GlobalStateKey
  ): F[Either[HistoricalMptProofService.ProofError, MerklePatriciaInclusionProof]]

  /** Branch-aware proof (#56.7). Materializes the trie at the requested branch view via `MptOverlay.buildRoot(branch, ordinal)` and runs a
    * stateless inclusion prover on the resulting `MerklePatriciaTrie`. No journal walk, no rollback — branch composition handles
    * historical-state reconstruction structurally.
    *
    * Lock pattern: keeps `MptStore.withExclusiveLock` for now since `overlay.buildRoot` calls `underlying.build(ordinal)` which mutates
    * producer state and could race with concurrent `accept()` writes. Once #56.10 migrates `accept()` to the overlay, finalized base
    * becomes immutable on disk and this lock can drop.
    */
  def proofAtBranch(
    branch: BranchId,
    ordinal: SnapshotOrdinal,
    key: GlobalStateKey
  ): F[Either[HistoricalMptProofService.ProofError, MerklePatriciaInclusionProof]]
}

object HistoricalMptProofService {

  sealed trait ProofError extends Product with Serializable
  final case class OutOfWindow(ordinal: SnapshotOrdinal, journalFloor: Option[Long]) extends ProofError
  final case class JournalError(message: String) extends ProofError
  final case class TrieBuildFailed(message: String) extends ProofError
  final case class Underlying(cause: MerklePatriciaProofError) extends ProofError

  def make[F[_]: Async: Hasher](
    store: MptStore[F, GlobalStateKey],
    undoJournal: MptUndoJournal[F],
    overlay: MptOverlay[F, GlobalStateKey]
  ): HistoricalMptProofService[F] = new HistoricalMptProofService[F] {

    private val logger = Slf4jLogger.getLoggerFromName[F]("HistoricalMptProofService")

    def proofAt(
      ordinal: SnapshotOrdinal,
      key: GlobalStateKey
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      GlobalStateKey.toHex(key).flatMap { hex =>
        proofAtHex(ordinal, hex)
      }

    private def proofAtHex(
      ordinal: SnapshotOrdinal,
      hex: Hex
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      for {
        tipOpt <- undoJournal.currentTipOrdinal
        target = ordinal.value.value
        result <- tipOpt match {
          case Some(tip) if tip === target =>
            // Current ordinal — direct traversal of the live trie.
            directProof(ordinal, hex)

          case Some(tip) if target < tip =>
            // Historical — walk back via the journal.
            historicalProof(ordinal, hex, tip)

          case Some(tip) =>
            // Requested ordinal is beyond the current tip (future).
            logger.warn(s"[HistoricalMptProofService] Requested proof at ordinal=$target but tip is $tip") >>
              (OutOfWindow(ordinal, Some(tip)): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]

          case None =>
            logger.warn("[HistoricalMptProofService] Journal has no tip — store not synced?") >>
              (OutOfWindow(ordinal, None): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
        }
      } yield result

    private def directProof(
      ordinal: SnapshotOrdinal,
      hex: Hex
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      for {
        trieE <- store.build(ordinal)
        result <- trieE match {
          case Left(err) =>
            (TrieBuildFailed(err.toString): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
          case Right(_) =>
            // After `build(ordinal)`, the underlying producer's current trie IS the one at
            // `ordinal`. `getProver` (no-arg) returns a prover for that current trie.
            store.underlying.getProver.flatMap { prover =>
              prover.attestPath(hex).map(_.leftMap(Underlying(_): ProofError))
            }
        }
      } yield result

    private def historicalProof(
      ordinal: SnapshotOrdinal,
      hex: Hex,
      tipOrdinal: Long
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      // The journal-rewind path is read-only by intent: we mutate the MPT to inspect a past
      // ordinal, then always roll back. `withTransaction` with Rollback expresses that
      // intent at the bracket boundary; restore is automatic on body completion or error.
      // `withTransaction` itself is caller-serialized (it doesn't take the exclusive lock to
      // avoid nested-acquisition deadlocks with `accept()`'s internal MPT writes). HTTP-served
      // proof requests are NOT protected by `snapshotSemaphore`, so we wrap explicitly here.
      store.withExclusiveLock {
        store.withTransaction {
          attemptHistorical(ordinal, hex, tipOrdinal).handleErrorWith { err =>
            logger.error(err)(s"[HistoricalMptProofService] Historical proof at $ordinal failed") >>
              (TrieBuildFailed(err.getMessage): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
          }.map(_ -> (MptTxAction.Rollback: MptTxAction))
        }
      }

    private def attemptHistorical(
      ordinal: SnapshotOrdinal,
      hex: Hex,
      tipOrdinal: Long
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      for {
        journalSize <- undoJournal.size
        target = ordinal.value.value
        _ <- logger.info(
          s"[HistoricalMptProofService] Rolling back trie: tip=$tipOrdinal → target=$target (journal size=$journalSize)"
        )
        _ <- undoJournal.unapplyTo(target)
        trieE <- store.build(ordinal)
        result <- trieE match {
          case Left(err) =>
            (TrieBuildFailed(err.toString): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
          case Right(_) =>
            // After `build(ordinal)`, the underlying producer's current trie IS the one at
            // `ordinal`. `getProver` (no-arg) returns a prover for that current trie.
            store.underlying.getProver.flatMap { prover =>
              prover.attestPath(hex).map(_.leftMap(Underlying(_): ProofError))
            }
        }
      } yield result

    /** Branch-aware proof (#56.7). Asks the overlay to materialize the trie at the requested branch view, then runs a stateless inclusion
      * prover on the resulting `MerklePatriciaTrie`.
      *
      * For branches not registered with the overlay (the typical "give me the finalized base" case), `overlay.buildRoot` falls through to
      * `underlying.build(ordinal)` and returns the on-disk trie unchanged — equivalent to `directProof` above but without the side-effect
      * of leaving the producer's "current" pointer at `ordinal`.
      *
      * For pending branches, the trie is composed in-memory via `MerklePatriciaTrie.withChanges` from the on-disk base + chain-of-deltas;
      * the prover operates on this fresh in-memory trie without touching the producer's mutable state.
      */
    def proofAtBranch(
      branch: BranchId,
      ordinal: SnapshotOrdinal,
      key: GlobalStateKey
    ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
      GlobalStateKey.toHex(key).flatMap { hex =>
        store.withExclusiveLock {
          overlay.buildRoot(branch, ordinal).flatMap {
            case Left(err) =>
              (TrieBuildFailed(err.toString): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
            case Right(trie) =>
              val prover = MerklePatriciaSingleInclusionProver.make[F](trie)
              prover.attestPath(hex).map(_.leftMap(Underlying(_): ProofError))
          }
        }
      }
  }
}
