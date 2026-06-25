package io.constellationnetwork.security.mpt.verifier

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.prover.attestation.{AbsenceTermination, MerklePatriciaAbsenceProof}

import io.circe.syntax.EncoderOps

/** Verifies non-membership (absence) proofs against a trusted MPT root — task #286.
  *
  * The walk mirrors [[MerklePatriciaInclusionVerifier]]: it reconstructs the root from the witness chain (root→terminal after reversing the
  * terminal-first `witness`), recomputing each node commitment's digest and descending along the proven key's own nibble path. The terminal
  * node must structurally witness that the key cannot be present, and the claimed [[AbsenceTermination]] kind is '''independently
  * re-derived''' here — a forged tag is rejected.
  *
  * On success the result is `Right(())` (the key is proven absent under `root`). Any structural failure — a recomputed digest that does not
  * match the expected one (so the root would differ), a non-terminal leaf, a terminal node that does NOT witness absence, or a claimed
  * termination kind that does not match the actual structure — yields `Left(...)`, preserving the same `Either`-of-`Unit` discipline as the
  * sibling MPT verifiers.
  */
trait MerklePatriciaAbsenceVerifier[F[_]] {

  def confirmAbsence(proof: MerklePatriciaAbsenceProof): F[Either[MerklePatriciaVerificationError, Unit]]
}

object MerklePatriciaAbsenceVerifier {
  def apply[F[_]](implicit verifier: MerklePatriciaAbsenceVerifier[F]): MerklePatriciaAbsenceVerifier[F] = verifier

  def make[F[_]: Async: Hasher](root: Hash): MerklePatriciaAbsenceVerifier[F] =
    new MerklePatriciaAbsenceVerifier[F] {

      def confirmAbsence(proof: MerklePatriciaAbsenceProof): F[Either[MerklePatriciaVerificationError, Unit]] = {
        type Continue = (List[MerklePatriciaCommitment], Hash, Seq[Nibble])
        type Return = Either[MerklePatriciaVerificationError, Unit]

        def fail(msg: String): F[Either[Continue, Return]] =
          (InvalidWitness(msg): MerklePatriciaVerificationError).asLeft[Unit].asRight[Continue].pure[F]

        // ---- Terminal-node confirmations (also re-derive + cross-check the claimed termination kind) ----

        def confirmTerminalLeaf(
          leaf: MerklePatriciaCommitment.Leaf,
          currentDigest: Hash,
          remainingPath: Seq[Nibble]
        ): F[Either[Continue, Return]] =
          Hasher[F]
            .prefixedHash(leaf.asJson, MerklePatriciaNode.LeafPrefix)
            .map { digest =>
              if (digest =!= currentDigest)
                InvalidNodeCommitment("Invalid terminal leaf commitment").asLeft[Unit].asRight[Continue]
              else if (leaf.remaining == remainingPath)
                // The terminal leaf binds the exact key — the key is PRESENT, so this is not a valid absence proof.
                InvalidWitness("Terminal leaf matches the key — key is present, absence disproven").asLeft[Unit].asRight[Continue]
              else
                proof.termination match {
                  case AbsenceTermination.LeafMismatch =>
                    ().asRight[MerklePatriciaVerificationError].asRight[Continue]
                  case other =>
                    InvalidWitness(s"Termination kind $other does not match a terminal leaf").asLeft[Unit].asRight[Continue]
                }
            }
            .handleError(e => InvalidNodeCommitment(s"Hash computation error: ${e.getMessage}").asLeft[Unit].asRight[Continue])

        def confirmTerminalExtension(
          ext: MerklePatriciaCommitment.Extension,
          currentDigest: Hash,
          remainingPath: Seq[Nibble]
        ): F[Either[Continue, Return]] =
          Hasher[F]
            .prefixedHash(ext.asJson, MerklePatriciaNode.ExtensionPrefix)
            .map { digest =>
              if (digest =!= currentDigest)
                InvalidNodeCommitment("Invalid terminal extension commitment").asLeft[Unit].asRight[Continue]
              else if (remainingPath.startsWith(ext.shared))
                // The path continues along the extension — this is NOT a divergence; the witness should have descended.
                InvalidWitness("Terminal extension does not diverge from the key path").asLeft[Unit].asRight[Continue]
              else
                proof.termination match {
                  case AbsenceTermination.ExtensionDivergence =>
                    ().asRight[MerklePatriciaVerificationError].asRight[Continue]
                  case other =>
                    InvalidWitness(s"Termination kind $other does not match a terminal extension").asLeft[Unit].asRight[Continue]
                }
            }
            .handleError(e => InvalidNodeCommitment(s"Hash computation error: ${e.getMessage}").asLeft[Unit].asRight[Continue])

        def confirmTerminalBranch(
          branch: MerklePatriciaCommitment.Branch,
          currentDigest: Hash,
          remainingPath: Seq[Nibble]
        ): F[Either[Continue, Return]] =
          Hasher[F]
            .prefixedHash(branch.asJson, MerklePatriciaNode.BranchPrefix)
            .map { digest =>
              if (digest =!= currentDigest)
                InvalidNodeCommitment("Invalid terminal branch commitment").asLeft[Unit].asRight[Continue]
              else
                remainingPath.headOption match {
                  case None =>
                    // Path exhausted exactly at a branch — degenerate; not a supported absence shape.
                    InvalidPath("Key path exhausted at terminal branch").asLeft[Unit].asRight[Continue]
                  case Some(nextNibble) =>
                    if (branch.pathsDigest.contains(nextNibble))
                      // The slot the key descends into is occupied — the witness should have descended further.
                      InvalidWitness("Terminal branch has an occupied slot for the key's next nibble").asLeft[Unit].asRight[Continue]
                    else
                      proof.termination match {
                        case AbsenceTermination.BranchEmptySlot(claimed) if claimed.value == nextNibble.value =>
                          ().asRight[MerklePatriciaVerificationError].asRight[Continue]
                        case AbsenceTermination.BranchEmptySlot(claimed) =>
                          InvalidWitness(
                            s"Claimed missing nibble $claimed does not match the key's next nibble $nextNibble"
                          ).asLeft[Unit].asRight[Continue]
                        case other =>
                          InvalidWitness(s"Termination kind $other does not match a terminal branch").asLeft[Unit].asRight[Continue]
                      }
                }
            }
            .handleError(e => InvalidNodeCommitment(s"Hash computation error: ${e.getMessage}").asLeft[Unit].asRight[Continue])

        // ---- Non-terminal descent (identical authenticated-descent rules as the inclusion verifier) ----

        def descendExtension(
          ext: MerklePatriciaCommitment.Extension,
          tail: List[MerklePatriciaCommitment],
          currentDigest: Hash,
          remainingPath: Seq[Nibble]
        ): F[Either[Continue, Return]] =
          Hasher[F]
            .prefixedHash(ext.asJson, MerklePatriciaNode.ExtensionPrefix)
            .map { digest =>
              if (digest =!= currentDigest)
                InvalidNodeCommitment("Invalid extension commitment").asLeft[Unit].asRight[Continue]
              else if (!remainingPath.startsWith(ext.shared))
                // A non-terminal extension MUST be followed by the key; if it diverges here the witness is malformed
                // (a divergence can only legitimately appear as the terminal node).
                InvalidWitness("Non-terminal extension diverges from the key path").asLeft[Unit].asRight[Continue]
              else
                (tail, ext.childDigest, remainingPath.drop(ext.shared.length)).asLeft[Return]
            }
            .handleError(e => InvalidNodeCommitment(s"Hash computation error: ${e.getMessage}").asLeft[Unit].asRight[Continue])

        def descendBranch(
          branch: MerklePatriciaCommitment.Branch,
          tail: List[MerklePatriciaCommitment],
          currentDigest: Hash,
          remainingPath: Seq[Nibble]
        ): F[Either[Continue, Return]] =
          remainingPath.headOption match {
            case None =>
              fail("Empty path at non-terminal branch")
            case Some(nextNibble) =>
              branch.pathsDigest.get(nextNibble) match {
                case None =>
                  // A non-terminal branch MUST contain the key's next nibble; an empty slot can only legitimately
                  // appear as the terminal node.
                  fail("Non-terminal branch missing the key's next nibble")
                case Some(childDigest) =>
                  Hasher[F]
                    .prefixedHash(branch.asJson, MerklePatriciaNode.BranchPrefix)
                    .map { digest =>
                      if (digest =!= currentDigest)
                        InvalidNodeCommitment("Invalid branch commitment").asLeft[Unit].asRight[Continue]
                      else
                        (tail, childDigest, remainingPath.tail).asLeft[Return]
                    }
                    .handleError(e => InvalidNodeCommitment(s"Hash computation error: ${e.getMessage}").asLeft[Unit].asRight[Continue])
              }
          }

        proof.witness match {
          case Nil =>
            (InvalidWitness("Empty absence witness"): MerklePatriciaVerificationError).asLeft[Unit].pure[F]
          case _ =>
            Async[F]
              .tailRecM[Continue, Return]((proof.witness.reverse, root, Nibble(proof.path))) {
                case (commitments, currentDigest, remainingPath) =>
                  commitments match {
                    // Terminal node — the last (deepest) commitment in root→terminal order.
                    case (leaf: MerklePatriciaCommitment.Leaf) :: Nil =>
                      confirmTerminalLeaf(leaf, currentDigest, remainingPath)
                    case (ext: MerklePatriciaCommitment.Extension) :: Nil =>
                      confirmTerminalExtension(ext, currentDigest, remainingPath)
                    case (branch: MerklePatriciaCommitment.Branch) :: Nil =>
                      confirmTerminalBranch(branch, currentDigest, remainingPath)

                    // Non-terminal descent.
                    case (_: MerklePatriciaCommitment.Leaf) :: _ =>
                      fail("Leaf commitment in non-terminal position of absence witness")
                    case (ext: MerklePatriciaCommitment.Extension) :: tail =>
                      descendExtension(ext, tail, currentDigest, remainingPath)
                    case (branch: MerklePatriciaCommitment.Branch) :: tail =>
                      descendBranch(branch, tail, currentDigest, remainingPath)

                    case Nil =>
                      fail("Exhausted absence witness without reaching a terminal node")
                  }
              }
              .handleError(e => InvalidWitness(s"Absence verification failed with error: ${e.getMessage}").asLeft[Unit])
        }
      }
    }

  object syntax {

    implicit class MerklePatriciaAbsenceProofOps(private val proof: MerklePatriciaAbsenceProof) extends AnyVal {

      def confirmAbsence[F[_]](implicit V: MerklePatriciaAbsenceVerifier[F]): F[Either[MerklePatriciaVerificationError, Unit]] =
        V.confirmAbsence(proof)
    }
  }
}
