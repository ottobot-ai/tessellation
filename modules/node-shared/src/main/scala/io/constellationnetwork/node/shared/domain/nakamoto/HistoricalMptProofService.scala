package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.mpt.prover.{MerklePatriciaProofError, MerklePatriciaSingleInclusionProver}

/** Branch-aware inclusion-proof generation (#56.7).
  *
  * `proofAtBranch(branch, ordinal, key)` materializes the trie at a specific branch view via `MptOverlay.buildRoot` and runs a stateless
  * inclusion prover on the resulting `MerklePatriciaTrie`. No journal walk, no rollback — branch composition handles historical-state
  * reconstruction structurally.
  *
  * Lock pattern: keeps `MptStore.withExclusiveLock` defensively because `overlay.buildRoot` calls `underlying.build(ordinal)` which mutates
  * the producer's "current" pointer and could race with concurrent writes from `accept()`. Phase 2 remains density-reorgable, so dropping
  * this lock requires a replacement store API that provides an equivalent exact-branch atomic read; age alone never makes the base
  * immutable.
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

  def make[F[_]: Async: Hasher](
    store: MptStore[F, GlobalStateKey],
    overlay: MptOverlay[F, GlobalStateKey]
  ): HistoricalMptProofService[F] = new HistoricalMptProofService[F] {

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
