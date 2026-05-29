package io.constellationnetwork.security.smt

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

/** Verifies a native [[SmtProof]] against a trusted [[SmtRoot]], returning a [[Verified]]-gated [[SmtEntry]] (so the result cannot be
  * consumed unverified — same discipline as `FollowVerifyCore`).
  *
  * Verification is the SAME authentication-path fold for inclusion and absence — they differ only in what sits at the terminating slot:
  *   - INCLUSION: assert `Hasher.hashBytes(value) === proof.valueDigest` (mandatory value-binding ⇒ [[SmtProofError.ValueBindingFailed]] on
  *     mismatch), recompute the leaf digest from `(position, valueDigest)`, fold the siblings up, compare to the trusted root.
  *   - ABSENCE/Default: fold the EMPTY placeholder (`Hash.empty`) up through the siblings, compare to the trusted root.
  *   - ABSENCE/OtherLeaf: recompute the occupying leaf's position `Hasher.hash(occupyingKey)`; assert it differs from the queried position
  *     (else it is not a genuine other leaf ⇒ [[SmtProofError.MalformedProof]]); recompute its leaf digest from `(occupyingPosition,
  *     occupyingDataDigest)`, fold up, compare to the trusted root. The root match also enforces the shared-prefix relationship.
  *
  * Any path longer than the 256-bit position space is [[SmtProofError.MalformedProof]]; a fold that does not reproduce the trusted root is
  * [[SmtProofError.RootMismatch]].
  */
trait SmtVerifier[F[_]] {
  def verify(root: SmtRoot, proof: SmtProof): F[Either[SmtProofError, Verified[SmtEntry]]]
}

object SmtVerifier {

  def apply[F[_]: SmtVerifier]: SmtVerifier[F] = implicitly

  def make[F[_]: Async: Hasher]: SmtVerifier[F] = new SmtVerifier[F] {

    def verify(root: SmtRoot, proof: SmtProof): F[Either[SmtProofError, Verified[SmtEntry]]] =
      if (proof.siblings.length > SmtHashing.PositionBits)
        (SmtProofError.MalformedProof(proof.key, SmtProofError.MalformedReason.PathTooDeep): SmtProofError)
          .asLeft[Verified[SmtEntry]]
          .pure[F]
      else
        proof match {
          case SmtProof.Inclusion(key, value, valueDigest, siblings) =>
            Hasher[F].hashBytes(value).flatMap { computed =>
              if (computed =!= valueDigest)
                (SmtProofError.ValueBindingFailed(key): SmtProofError).asLeft[Verified[SmtEntry]].pure[F]
              else
                SmtHashing.position[F](key).flatMap { pos =>
                  SmtHashing
                    .leafDigest[F](pos, valueDigest)
                    .flatMap(leaf => foldUp(pos, leaf, siblings))
                    .map(recomputed => bindRoot(root, recomputed, SmtEntry.Present(key, value)))
                }
            }

          case SmtProof.Absence(key, AbsenceWitness.Default, siblings) =>
            SmtHashing.position[F](key).flatMap { pos =>
              foldUp(pos, SmtHashing.empty, siblings)
                .map(recomputed => bindRoot(root, recomputed, SmtEntry.Absent(key)))
            }

          case SmtProof.Absence(key, AbsenceWitness.OtherLeaf(occupyingKey, occupyingDataDigest), siblings) =>
            (SmtHashing.position[F](key), SmtHashing.position[F](occupyingKey)).tupled.flatMap {
              case (pos, occPos) =>
                if (occPos === pos)
                  (SmtProofError.MalformedProof(key, SmtProofError.MalformedReason.OtherLeafCollidesWithKey): SmtProofError)
                    .asLeft[Verified[SmtEntry]]
                    .pure[F]
                else
                  SmtHashing
                    .leafDigest[F](occPos, occupyingDataDigest)
                    .flatMap(leaf => foldUp(pos, leaf, siblings))
                    .map(recomputed => bindRoot(root, recomputed, SmtEntry.Absent(key)))
            }
        }

    /** Fold a terminating digest `start` (sitting at depth = `siblings.length`) up to the root, choosing left/right at each level by the
      * corresponding bit of `position`. `siblings` is top-down (root-first); we consume it deepest-first.
      */
    private def foldUp(position: Hash, start: Hash, siblings: List[SmtSibling]): F[Hash] = {
      val depth = siblings.length
      // (level, sibling) pairs, deepest level first: level d-1 down to 0.
      val indexed = siblings.reverse.zipWithIndex.map { case (sib, i) => (depth - 1 - i, sib.digest) }
      indexed.foldLeftM(start) {
        case (cur, (level, sibling)) =>
          SmtHashing.combine[F](SmtHashing.bit(position, level), cur, sibling)
      }
    }

    private def bindRoot(root: SmtRoot, recomputed: Hash, entry: SmtEntry): Either[SmtProofError, Verified[SmtEntry]] =
      if (recomputed === root.value) Verified.makeInternal(entry).asRight[SmtProofError]
      else (SmtProofError.RootMismatch(root.value, recomputed): SmtProofError).asLeft[Verified[SmtEntry]]
  }
}
