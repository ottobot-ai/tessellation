package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.mpt.prover.{MerklePatriciaProofError, MerklePatriciaSingleInclusionProver}

/** Branch-aware inclusion-proof generation (#56.7).
  *
  * `proofAtBranch(branch, ordinal, key)` captures one branch image via `MptOverlay.captureBranchImage` and runs a stateless inclusion
  * prover on its trie. No journal walk, no rollback — branch composition handles historical-state reconstruction structurally.
  *
  * The service consumes one defensively owned [[io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptBranchImage]]. Trie
  * construction is read-only, so proof generation does not take the store mutation lock before the overlay lock. ROOT-005B still has to
  * make direct base mutation share one owner before this local capture can be treated as an exact-parent session.
  */
trait HistoricalMptProofService[F[_]] {

  def proofAtBranch(
    branch: BranchId,
    ordinal: SnapshotOrdinal,
    key: GlobalStateKey
  ): F[Either[HistoricalMptProofService.ProofError, MerklePatriciaInclusionProof]]
}

object HistoricalMptProofService {

  sealed trait ProofError extends Product with Serializable
  final case class TrieBuildFailed(message: String) extends ProofError
  final case class Underlying(cause: MerklePatriciaProofError) extends ProofError

  def make[F[_]: Async: Hasher](overlay: MptOverlay[F, GlobalStateKey]): HistoricalMptProofService[F] =
    new HistoricalMptProofService[F] {

      def proofAtBranch(
        branch: BranchId,
        ordinal: SnapshotOrdinal,
        key: GlobalStateKey
      ): F[Either[ProofError, MerklePatriciaInclusionProof]] =
        GlobalStateKey.toHex(key).flatMap { hex =>
          overlay.captureBranchImage(branch).flatMap {
            case Left(err) =>
              (TrieBuildFailed(err.toString): ProofError).asLeft[MerklePatriciaInclusionProof].pure[F]
            case Right(image) =>
              val prover = MerklePatriciaSingleInclusionProver.make[F](image.trie)
              prover.attestPath(hex).map(_.leftMap(Underlying(_): ProofError))
          }
        }
    }
}
