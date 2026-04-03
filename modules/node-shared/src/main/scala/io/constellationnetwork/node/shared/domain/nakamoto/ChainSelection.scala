package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.VrfOutput

/** Fork choice rule for Nakamoto consensus.
  *
  * Determines which chain tip to follow when multiple competing snapshots exist. Combines GRANDPA-style attestation weight with
  * Taktikos-style tiebreakers.
  *
  * Fork choice priority:
  *   1. Most attestation weight (heaviest tip from TipTracker)
  *   1. Longest chain (highest ordinal)
  *   1. Earliest slot (lower slot number = produced sooner, won a harder lottery)
  *   1. Lower VRF output (deterministic tiebreak using VRF hash as unsigned BigInt)
  */
trait ChainSelection[F[_]] {

  /** Compare two chain tips and return the preferred one.
    *
    * @return
    *   the preferred tip (tipA or tipB)
    */
  def compare(tipA: ChainTip, tipB: ChainTip): F[ChainTip]

  /** Select the best tip from a set of candidates.
    *
    * @return
    *   Some(best) or None if candidates is empty
    */
  def selectBest(candidates: List[ChainTip]): F[Option[ChainTip]]

  /** Check if we should switch from our current tip to a new candidate.
    *
    * Returns false if current tip is finalized (even if candidate has more weight), because finalized tips cannot be reverted.
    *
    * @return
    *   true if candidate beats current and current is not finalized
    */
  def shouldSwitch(current: ChainTip, candidate: ChainTip): F[Boolean]
}

object ChainSelection {

  /** Minimum weight difference to consider attestation weights meaningfully different. Avoids floating-point edge cases when weights are
    * "equal".
    */
  val WeightEpsilon: Double = 0.001

  def make[F[_]: Monad](tipTracker: TipTracker[F]): ChainSelection[F] =
    new ChainSelection[F] {

      def compare(tipA: ChainTip, tipB: ChainTip): F[ChainTip] =
        for {
          weightA <- tipTracker.attestationWeight(tipA.hash)
          weightB <- tipTracker.attestationWeight(tipB.hash)
        } yield {
          val weightDiff = weightA - weightB

          if (Math.abs(weightDiff) > WeightEpsilon) {
            // Attestation weight is significantly different - pick heavier
            if (weightDiff > 0) tipA else tipB
          } else {
            // Weights are equal (within epsilon) - fall through to tiebreakers
            compareByTiebreakers(tipA, tipB)
          }
        }

      def selectBest(candidates: List[ChainTip]): F[Option[ChainTip]] =
        candidates match {
          case Nil         => none[ChainTip].pure[F]
          case head :: Nil => head.some.pure[F]
          case head :: tail =>
            tail.foldLeftM(head) { (best, candidate) =>
              compare(best, candidate)
            }.map(_.some)
        }

      def shouldSwitch(current: ChainTip, candidate: ChainTip): F[Boolean] =
        // If tips are identical (same hash), no switch needed
        if (current.hash === candidate.hash)
          false.pure[F]
        else
          for {
            lastFinalized <- tipTracker.lastFinalized
            candidateIsBetter <- compare(current, candidate).map(winner => winner.hash === candidate.hash)
          } yield {
            // Don't switch if current is already finalized
            val currentIsFinalized = lastFinalized.exists {
              case (finalizedHash, _) =>
                finalizedHash === current.hash
            }

            candidateIsBetter && !currentIsFinalized
          }

      /** Compare tips using deterministic tiebreakers (when attestation weight is equal).
        *
        * Order of priority:
        *   1. Higher ordinal wins (longer chain)
        *   1. Lower slot wins (earlier production = harder lottery)
        *   1. Lower VRF output wins (deterministic, comparing as unsigned BigInt)
        */
      private def compareByTiebreakers(tipA: ChainTip, tipB: ChainTip): ChainTip = {
        // Tiebreaker 1: Higher ordinal (longer chain)
        if (tipA.ordinal != tipB.ordinal) {
          if (tipA.ordinal > tipB.ordinal) tipA else tipB
        }
        // Tiebreaker 2: Lower slot (earlier production)
        else if (tipA.slot.value.value != tipB.slot.value.value) {
          if (tipA.slot.value.value < tipB.slot.value.value) tipA else tipB
        }
        // Tiebreaker 3: Lower VRF output (deterministic)
        else {
          val cmp = compareVrfOutputs(tipA.vrfOutput, tipB.vrfOutput)
          if (cmp <= 0) tipA else tipB
        }
      }

      /** Compare VRF outputs as unsigned BigInts.
        *
        * @return
        *   negative if a < b, zero if a == b, positive if a > b
        */
      private def compareVrfOutputs(a: VrfOutput, b: VrfOutput): Int = {
        val bigA = BigInt(1, a.toBytes) // unsigned interpretation
        val bigB = BigInt(1, b.toBytes)
        bigA.compare(bigB)
      }
    }
}
