package io.constellationnetwork.security.mpt.prover

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.prover.attestation.{AbsenceTermination, MerklePatriciaAbsenceProof}

import io.circe.syntax._

/** Generates non-membership (absence) proofs for the hexary Merkle Patricia Trie — task #286.
  *
  * Mirrors [[MerklePatriciaSingleInclusionProver]]'s walk: it descends from the root following the target key's nibble path, accumulating
  * the same [[MerklePatriciaCommitment]] witness chain (terminal-first ordering). Instead of terminating at a matching leaf, it terminates
  * at the first node that proves the key cannot be present — an empty branch slot, a divergent extension, or a leaf bound to a different
  * key (see [[AbsenceTermination]]).
  *
  * If the walk would actually reach a leaf bound to the target key, the key is PRESENT and `attestAbsence` returns [[KeyIsPresent]] —
  * callers must use [[MerklePatriciaSingleInclusionProver]] for present keys.
  */
trait MerklePatriciaAbsenceProver[F[_]] {

  def attestAbsence(path: Hex): F[Either[MerklePatriciaProofError, MerklePatriciaAbsenceProof]]
}

object MerklePatriciaAbsenceProver {
  def apply[F[_]](implicit prover: MerklePatriciaAbsenceProver[F]): MerklePatriciaAbsenceProver[F] = prover

  def make[F[_]: Async: Hasher](
    trie: MerklePatriciaTrie
  ): MerklePatriciaAbsenceProver[F] =
    new MerklePatriciaAbsenceProver[F] {

      private def computeNodeHash(node: MerklePatriciaNode): F[Hash] =
        node match {
          case leaf: MerklePatriciaNode.Leaf =>
            val commitment = MerklePatriciaCommitment.Leaf(leaf.remaining, leaf.dataDigest)
            Hasher[F].prefixedHash(commitment.asJson, MerklePatriciaNode.LeafPrefix)
          case branch: MerklePatriciaNode.Branch =>
            computeBranchHash(branch)
          case ext: MerklePatriciaNode.Extension =>
            computeExtensionHash(ext)
        }

      private def computeBranchHash(branch: MerklePatriciaNode.Branch): F[Hash] =
        for {
          pathDigests <- branch.paths.toList.traverse {
            case (nibble, child) =>
              computeNodeHash(child).map(nibble -> _)
          }.map(_.toMap)
          commitment = MerklePatriciaCommitment.Branch(pathDigests)
          hash <- Hasher[F].prefixedHash(commitment.asJson, MerklePatriciaNode.BranchPrefix)
        } yield hash

      private def computeExtensionHash(ext: MerklePatriciaNode.Extension): F[Hash] =
        for {
          childHash <- computeBranchHash(ext.child)
          commitment = MerklePatriciaCommitment.Extension(ext.shared, childHash)
          hash <- Hasher[F].prefixedHash(commitment.asJson, MerklePatriciaNode.ExtensionPrefix)
        } yield hash

      private def branchCommitment(branch: MerklePatriciaNode.Branch): F[MerklePatriciaCommitment.Branch] =
        branch.paths.toList.traverse {
          case (k, v) => computeNodeHash(v).map(k -> _)
        }.map(pds => MerklePatriciaCommitment.Branch(pds.toMap))

      def attestAbsence(path: Hex): F[Either[MerklePatriciaProofError, MerklePatriciaAbsenceProof]] = {
        // Accumulator holds commitments terminal-first (head = most-recent), matching the inclusion prover.
        type Continue = (MerklePatriciaNode, Seq[Nibble], List[MerklePatriciaCommitment])
        type Outcome = Either[MerklePatriciaProofError, (List[MerklePatriciaCommitment], AbsenceTermination)]

        Async[F]
          .tailRecM[Continue, Outcome]((trie.rootNode, Nibble(path), List.empty[MerklePatriciaCommitment])) {
            case (currentNode, remainingPath, acc) =>
              currentNode match {
                case leaf: MerklePatriciaNode.Leaf =>
                  // Terminal leaf. If it binds the exact remaining path the key is PRESENT — refuse to fabricate
                  // an absence proof. Otherwise it is a different key occupying this terminal position ⇒ absence.
                  val commitment = MerklePatriciaCommitment.Leaf(leaf.remaining, leaf.dataDigest)
                  if (leaf.remaining == remainingPath)
                    Async[F].pure(
                      (KeyIsPresent(s"Key is present, cannot prove absence: ${path.value}"): MerklePatriciaProofError)
                        .asLeft[(List[MerklePatriciaCommitment], AbsenceTermination)]
                        .asRight[Continue]
                    )
                  else
                    Async[F].pure(
                      (commitment :: acc, AbsenceTermination.LeafMismatch: AbsenceTermination)
                        .asRight[MerklePatriciaProofError]
                        .asRight[Continue]
                    )

                case extension: MerklePatriciaNode.Extension if remainingPath.startsWith(extension.shared) =>
                  // Key continues along the extension — descend into the child branch.
                  computeBranchHash(extension.child).map { childDigest =>
                    (
                      extension.child,
                      remainingPath.drop(extension.shared.length),
                      MerklePatriciaCommitment.Extension(extension.shared, childDigest) :: acc
                    ).asLeft[Outcome]
                  }

                case extension: MerklePatriciaNode.Extension =>
                  // Key reached the extension but its remaining nibbles diverge from the extension's shared
                  // segment ⇒ the key's subtree does not exist under this extension ⇒ absence.
                  computeBranchHash(extension.child).map { childDigest =>
                    (
                      MerklePatriciaCommitment.Extension(extension.shared, childDigest) :: acc,
                      AbsenceTermination.ExtensionDivergence: AbsenceTermination
                    ).asRight[MerklePatriciaProofError].asRight[Continue]
                  }

                case branch: MerklePatriciaNode.Branch =>
                  remainingPath.headOption match {
                    case Some(nextNibble) =>
                      branch.paths.get(nextNibble) match {
                        case Some(child) =>
                          // Slot occupied — descend following the key.
                          branchCommitment(branch).map { branchCommit =>
                            (child, remainingPath.tail, branchCommit :: acc).asLeft[Outcome]
                          }
                        case None =>
                          // The slot for the key's next nibble is empty ⇒ absence.
                          branchCommitment(branch).map { branchCommit =>
                            (
                              branchCommit :: acc,
                              AbsenceTermination.BranchEmptySlot(nextNibble): AbsenceTermination
                            ).asRight[MerklePatriciaProofError].asRight[Continue]
                          }
                      }
                    case None =>
                      // The key's path is exhausted at a branch. A branch never holds a value for an
                      // exhausted path in this trie encoding, so the key is absent at this branch.
                      // Represent as an empty-slot termination on the sentinel "empty" nibble: the verifier
                      // confirms structurally that the path is exhausted here (no value at the branch).
                      Async[F].pure(
                        (PathTerminatesAtBranch(
                          s"Key path exhausted at a branch (unsupported value-at-branch trie): ${path.value}"
                        ): MerklePatriciaProofError)
                          .asLeft[(List[MerklePatriciaCommitment], AbsenceTermination)]
                          .asRight[Continue]
                      )
                  }
              }
          }
          .map(_.map { case (witness, termination) => MerklePatriciaAbsenceProof(path, witness, termination) })
          .handleError(e => ProofGenerationError(e.getMessage).asLeft[MerklePatriciaAbsenceProof])
      }
    }

  object syntax {

    implicit class MerklePatriciaAbsencePathOps(private val path: Hex) extends AnyVal {

      def attestAbsence[F[_]](
        implicit P: MerklePatriciaAbsenceProver[F]
      ): F[Either[MerklePatriciaProofError, MerklePatriciaAbsenceProof]] =
        P.attestAbsence(path)
    }
  }
}
