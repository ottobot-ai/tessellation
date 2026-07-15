package io.constellationnetwork.security.mpt.verifier

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.Nibble.nibbleSeqOrdering
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.prover.attestation.{MerklePatriciaInclusionProof, MerklePatriciaRangeProof}

import io.circe.syntax.EncoderOps

trait MerklePatriciaRangeVerifier[F[_]] {

  def confirmRange(proof: MerklePatriciaRangeProof): F[Either[MerklePatriciaVerificationError, Unit]]
}

object MerklePatriciaRangeVerifier {
  def apply[F[_]](implicit verifier: MerklePatriciaRangeVerifier[F]): MerklePatriciaRangeVerifier[F] = verifier

  def make[F[_]: Async: Hasher](root: Hash): MerklePatriciaRangeVerifier[F] =
    new MerklePatriciaRangeVerifier[F] {

      def confirmRange(proof: MerklePatriciaRangeProof): F[Either[MerklePatriciaVerificationError, Unit]] = {

        def hexOrdering: Ordering[Hex] = Ordering.by[Hex, String](_.value)

        def verifyInclusionProofs: F[Either[MerklePatriciaVerificationError, Unit]] = {
          val singleVerifier = MerklePatriciaInclusionVerifier.make[F](root)

          proof.inclusionProofs
            .traverse(inclusionProof => singleVerifier.confirm(inclusionProof))
            .map(results => results.sequence.map(_ => ()))
        }

        def verifyPathsInRange: F[Either[MerklePatriciaVerificationError, Unit]] = {
          val allPathsInRange = proof.inclusionProofs.forall { inclusionProof =>
            hexOrdering.compare(inclusionProof.path, proof.startPath) >= 0 &&
            hexOrdering.compare(inclusionProof.path, proof.endPath) <= 0
          }

          allPathsInRange
            .pure[F]
            .ifM(
              ifTrue = ().asRight[MerklePatriciaVerificationError].pure[F],
              ifFalse = (InvalidPath("Some paths in range proof are outside the specified range"): MerklePatriciaVerificationError)
                .asLeft[Unit]
                .pure[F]
            )
        }

        def verifyPathsOrdered: F[Either[MerklePatriciaVerificationError, Unit]] = {
          val paths = proof.inclusionProofs.map(_.path)
          val sortedPaths = paths.sorted(hexOrdering)

          (paths == sortedPaths)
            .pure[F]
            .ifM(
              ifTrue = ().asRight[MerklePatriciaVerificationError].pure[F],
              ifFalse = (InvalidWitness("Paths in range proof are not in sorted order"): MerklePatriciaVerificationError)
                .asLeft[Unit]
                .pure[F]
            )
        }

        def verifyBoundaries: F[Either[MerklePatriciaVerificationError, Unit]] =
          proof.exclusionBoundaries match {
            case Some(boundaries) =>
              val singleVerifier = MerklePatriciaInclusionVerifier.make[F](root)

              for {
                leftResult <- boundaries.leftBoundary match {
                  case Some(leftProof) =>
                    (hexOrdering.compare(leftProof.path, proof.startPath) < 0)
                      .pure[F]
                      .ifM(
                        ifTrue = singleVerifier.confirm(leftProof),
                        ifFalse = (InvalidPath(
                          s"Left boundary ${leftProof.path.value} must be < startPath ${proof.startPath.value}"
                        ): MerklePatriciaVerificationError)
                          .asLeft[Unit]
                          .pure[F]
                      )
                  case None =>
                    ().asRight[MerklePatriciaVerificationError].pure[F]
                }

                rightResult <- boundaries.rightBoundary match {
                  case Some(rightProof) =>
                    (hexOrdering.compare(rightProof.path, proof.endPath) > 0)
                      .pure[F]
                      .ifM(
                        ifTrue = singleVerifier.confirm(rightProof),
                        ifFalse = (InvalidPath(
                          s"Right boundary ${rightProof.path.value} must be > endPath ${proof.endPath.value}"
                        ): MerklePatriciaVerificationError)
                          .asLeft[Unit]
                          .pure[F]
                      )
                  case None =>
                    ().asRight[MerklePatriciaVerificationError].pure[F]
                }

                consecutiveResult <- (boundaries.leftBoundary, proof.inclusionProofs.headOption) match {
                  case (Some(left), Some(firstInclusion)) =>
                    val noGap = hexOrdering.compare(left.path, firstInclusion.path) < 0
                    noGap
                      .pure[F]
                      .ifM(
                        ifTrue = ().asRight[MerklePatriciaVerificationError].pure[F],
                        ifFalse = (InvalidWitness(s"Gap between left boundary and first inclusion"): MerklePatriciaVerificationError)
                          .asLeft[Unit]
                          .pure[F]
                      )
                  case _ =>
                    ().asRight[MerklePatriciaVerificationError].pure[F]
                }

                consecutiveRightResult <- (proof.inclusionProofs.lastOption, boundaries.rightBoundary) match {
                  case (Some(lastInclusion), Some(right)) =>
                    val noGap = hexOrdering.compare(lastInclusion.path, right.path) < 0
                    noGap
                      .pure[F]
                      .ifM(
                        ifTrue = ().asRight[MerklePatriciaVerificationError].pure[F],
                        ifFalse = (InvalidWitness(s"Gap between last inclusion and right boundary"): MerklePatriciaVerificationError)
                          .asLeft[Unit]
                          .pure[F]
                      )
                  case _ =>
                    ().asRight[MerklePatriciaVerificationError].pure[F]
                }

              } yield
                for {
                  _ <- leftResult
                  _ <- rightResult
                  _ <- consecutiveResult
                  _ <- consecutiveRightResult
                } yield ()

            case None =>
              ().asRight[MerklePatriciaVerificationError].pure[F]
          }

        /** Completeness (set-soundness) check — the #286 strengthening.
          *
          * The inclusion proofs + ordering + exclusion boundaries prove every returned key IS in `[lo, hi]` and that the boundaries bracket
          * the range, but they do NOT, on their own, prove the returned set is COMPLETE: an adversary could omit a key strictly between two
          * returned keys (or inside an "empty" range) and the boundary checks would still pass.
          *
          * This is closed WITHOUT any wire-format change, because each [[MerklePatriciaInclusionProof]] already carries, for every
          * [[MerklePatriciaCommitment.Branch]] on its path, the digests of ALL of that branch's children (`pathsDigest` is the full child
          * map, not just the on-path child). The union of all inclusion proofs (plus the boundary proofs) therefore authenticates the trie
          * structure across the whole `[lo, hi]` region. We reconstruct that partial trie keyed by recomputed node digest and traverse the
          * sub-region overlapping `[lo, hi]`:
          *
          *   - every occupied branch slot whose key-prefix overlaps `[lo, hi]` MUST resolve to a commitment we hold (else a subtree — i.e.
          *     at least one leaf — was omitted ⇒ INCOMPLETE), and
          *   - every leaf reached whose full path lies in `[lo, hi]` MUST be one of the returned inclusion-proof paths (else an in-range
          *     leaf was reached but not returned ⇒ INCOMPLETE).
          *
          * Soundness: the reconstructed structure is authenticated by `root` (the inclusion proofs already verified to it), and the descent
          * follows only `[lo, hi]`-overlapping prefixes, so any omitted in-range leaf diverges from the returned/boundary leaves at a
          * branch we DO hold, exposing a child digest we do NOT hold. Genuinely-complete proofs traverse with every overlapping child
          * present and every in-range leaf returned, so they still verify (additive strengthening — no previously-valid complete proof
          * newly fails).
          *
          * Degenerate case: a range with no inclusion or boundary commitments carries no authenticated structure. It is accepted only when
          * the trusted root equals the canonical empty-branch root; under any nonempty root it cannot prove that an in-range leaf was not
          * omitted.
          */
        def verifyCompleteness: F[Either[MerklePatriciaVerificationError, Unit]] = {
          val keyLen = math.max(Nibble(proof.startPath).length, Nibble(proof.endPath).length)
          val lo = Nibble(proof.startPath)
          val hi = Nibble(proof.endPath)

          val minNibble = Nibble.unsafe(0: Byte)
          val maxNibble = Nibble.unsafe(15: Byte)

          // A subtree at nibble-prefix `p` overlaps the range iff some full-length key with that prefix lies in [lo, hi].
          def subtreeOverlapsRange(p: Seq[Nibble]): Boolean = {
            val padLen = math.max(0, keyLen - p.length)
            val pMin = p ++ Seq.fill(padLen)(minNibble)
            val pMax = p ++ Seq.fill(padLen)(maxNibble)
            nibbleSeqOrdering.gteq(pMax, lo) && nibbleSeqOrdering.lteq(pMin, hi)
          }

          def inRange(path: Seq[Nibble]): Boolean =
            nibbleSeqOrdering.gteq(path, lo) && nibbleSeqOrdering.lteq(path, hi)

          val allProofs: List[MerklePatriciaInclusionProof] =
            proof.inclusionProofs ++
              proof.exclusionBoundaries.toList.flatMap(b => b.leftBoundary.toList ++ b.rightBoundary.toList)

          val returnedInRangePaths: Set[Seq[Nibble]] =
            proof.inclusionProofs.map(p => Nibble(p.path)).toSet

          def digestOf(commit: MerklePatriciaCommitment): F[Hash] =
            commit match {
              case l: MerklePatriciaCommitment.Leaf      => Hasher[F].prefixedHash(l.asJson, MerklePatriciaNode.LeafPrefix)
              case b: MerklePatriciaCommitment.Branch    => Hasher[F].prefixedHash(b.asJson, MerklePatriciaNode.BranchPrefix)
              case e: MerklePatriciaCommitment.Extension => Hasher[F].prefixedHash(e.asJson, MerklePatriciaNode.ExtensionPrefix)
            }

          // Index every commitment across every (verified) inclusion + boundary proof by its recomputed digest.
          val indexF: F[Map[Hash, MerklePatriciaCommitment]] =
            allProofs
              .flatMap(_.witness)
              .traverse(c => digestOf(c).map(_ -> c))
              .map(_.toMap)

          indexF.flatMap { index =>
            // DFS over the partial trie, restricted to the range-overlapping region.
            type Frame = (Hash, Seq[Nibble]) // (expected node digest, nibble prefix of this node)
            type Return = Either[MerklePatriciaVerificationError, Unit]

            def step(stack: List[Frame]): F[Either[List[Frame], Return]] =
              stack match {
                case Nil =>
                  (().asRight[MerklePatriciaVerificationError]: Return).asRight[List[Frame]].pure[F]
                case (digest, prefix) :: rest =>
                  index.get(digest) match {
                    case None =>
                      // An overlapping subtree whose node we don't hold ⇒ an in-range leaf was omitted.
                      (Right(
                        (InvalidWitness(
                          s"Incomplete range proof: missing subtree for prefix ${Nibble.toHex(prefix).value} within range"
                        ): MerklePatriciaVerificationError).asLeft[Unit]
                      ): Either[List[Frame], Return]).pure[F]
                    case Some(leaf: MerklePatriciaCommitment.Leaf) =>
                      val fullPath = prefix ++ leaf.remaining
                      if (inRange(fullPath) && !returnedInRangePaths.contains(fullPath))
                        (Right(
                          (InvalidWitness(
                            s"Incomplete range proof: in-range leaf ${Nibble.toHex(fullPath).value} reached but not returned"
                          ): MerklePatriciaVerificationError).asLeft[Unit]
                        ): Either[List[Frame], Return]).pure[F]
                      else
                        (rest.asLeft[Return]).pure[F]
                    case Some(ext: MerklePatriciaCommitment.Extension) =>
                      val childPrefix = prefix ++ ext.shared
                      val next =
                        if (subtreeOverlapsRange(childPrefix)) (ext.childDigest, childPrefix) :: rest
                        else rest
                      (next.asLeft[Return]).pure[F]
                    case Some(branch: MerklePatriciaCommitment.Branch) =>
                      val children = branch.pathsDigest.toList.flatMap {
                        case (nibble, childDigest) =>
                          val childPrefix = prefix :+ nibble
                          if (subtreeOverlapsRange(childPrefix)) List((childDigest, childPrefix)) else Nil
                      }
                      (children ++: rest).asLeft[Return].pure[F]
                  }
              }

            if (index.isEmpty)
              MerklePatriciaNode.Branch.empty[F].map { emptyRoot =>
                Either.cond(
                  root === emptyRoot.digest,
                  (),
                  InvalidWitness("Evidence-free range proof requires the canonical empty-trie root"): MerklePatriciaVerificationError
                )
              }
            else
              Async[F].tailRecM[List[Frame], Return](List((root, Seq.empty[Nibble])))(step)
          }
        }

        (hexOrdering.compare(proof.startPath, proof.endPath) > 0)
          .pure[F]
          .ifM(
            ifTrue = (InvalidPath(
              s"Invalid range: startPath ${proof.startPath.value} > endPath ${proof.endPath.value}"
            ): MerklePatriciaVerificationError)
              .asLeft[Unit]
              .pure[F],
            ifFalse =
              for {
                inclusionResult <- verifyInclusionProofs
                rangeResult <- verifyPathsInRange
                orderResult <- verifyPathsOrdered
                boundaryResult <- verifyBoundaries
                completenessResult <- verifyCompleteness
              } yield
                for {
                  _ <- inclusionResult
                  _ <- rangeResult
                  _ <- orderResult
                  _ <- boundaryResult
                  _ <- completenessResult
                } yield ()
          )
      }.handleError(e => (InvalidWitness(s"Range verification failed: ${e.getMessage}"): MerklePatriciaVerificationError).asLeft[Unit])
    }

  object syntax {

    implicit class MerklePatriciaRangeProofOps(private val proof: MerklePatriciaRangeProof) extends AnyVal {

      def confirmRange[F[_]](implicit V: MerklePatriciaRangeVerifier[F]): F[Either[MerklePatriciaVerificationError, Unit]] =
        V.confirmRange(proof)
    }
  }
}
