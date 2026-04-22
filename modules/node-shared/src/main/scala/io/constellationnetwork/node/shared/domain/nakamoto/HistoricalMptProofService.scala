package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaProofError
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Proof generation at a specific ordinal, including historical ordinals within the undo-journal window.
  *
  * Strategy:
  *   - **Current ordinal**: direct traversal of the in-memory trie (fast path).
  *   - **Historical within the journal window**: take a savepoint, `unapplyTo(ordinal)` to roll the flat state back, rebuild the trie at
  *     that ordinal, generate the proof, restore the savepoint. This uses the `MptUndoJournal` infrastructure that was designed for exactly
  *     this case (`MptUndoJournal.scala:46-51` comment — "deferred until inclusion-proof features land that require reconstructing the trie
  *     root at a historical ordinal").
  *   - **Beyond the journal window**: returns `OutOfWindow` — the caller can fall back to snapshot-file replay if they need deep history
  *     (deferred future work).
  *
  * The savepoint/unapply path serializes through `MptStore.withExclusiveLock` so concurrent mutations can't interleave.
  */
trait HistoricalMptProofService[F[_]] {

  def proofAt(
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
    undoJournal: MptUndoJournal[F]
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
      store.withExclusiveLock {
        for {
          savepoint <- store.savepoint
          result <- attemptHistorical(ordinal, hex, tipOrdinal).handleErrorWith { err =>
            logger.error(err)(s"[HistoricalMptProofService] Historical proof at $ordinal failed") >>
              (TrieBuildFailed(err.getMessage): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
          }
          _ <- savepoint.restore
        } yield result
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
  }
}
