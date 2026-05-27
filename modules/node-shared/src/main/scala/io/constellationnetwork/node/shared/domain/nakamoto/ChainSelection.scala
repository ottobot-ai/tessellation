package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.VrfOutput

/** Fork choice rule for Nakamoto consensus — purely structural (Bitcoin/Praos/Polkadot-BABE pattern).
  *
  * **Strict separation of fork choice and finality.** Fork choice picks the best live branch from block-header structure alone (longest
  * chain, density on long forks). Finality (attestation ≥ 2/3 OR depth-k) is computed elsewhere by `TipTracker` and feeds into chain
  * selection only as a *constraint* via `shouldSwitch`'s "don't revert below the finalized head" check. This matches Polkadot's
  * BABE/GRANDPA split, Cardano Praos/Genesis, and the original Nakamoto rule. It is deliberately NOT Ethereum's LMD-GHOST blend — which
  * couples latest-attestation weight into the comparator and has produced a long tail of balancing/bouncing/avalanche attacks (Neu/Tas/Tse
  * IACR 2022/289; D'Amato/Zanolini IACR 2023/279). For a 50-100 validator chain with depth-k fallback, the dynamic- availability
  * head-convergence properties LMD-GHOST buys aren't worth the attack surface.
  *
  * '''The pre-removal bug.''' An earlier version called `tipTracker.attestationWeight(tipHash)` per-hash and let the heavier tip win if
  * either side cleared `minQuorum`. This is a category error: peers always attest the *current* chain head, never older ancestors, so
  * `attestationWeight(olderHash)` is structurally zero for any snapshot being relayed via sync. A forked node's self-attestation on its own
  * tip (1/3 in a 3-node cluster) defeated every incoming snapshot from the majority chain (weight 0), and the structural rule never ran.
  * The node stayed permanently on its 1/3 fork. Stripping that path makes `compare` deterministic on observable header data and lets
  * maxvalid-tk / maxvalid-bg do their job.
  *
  * Compare algorithm (structural only):
  *   1. **Short forks** (common ancestor within `kLookback`): tiebreakers on tips: ordinal (longer chain wins) → slot (earlier wins) → VRF
  *      output (lower wins). This is Bifrost's maxvalid-tk. 2. **Long forks** (fork depth exceeds `kLookback`): block-density comparison
  *      within `sWindow` slots from the fork point. This is Bifrost's maxvalid-bg.
  *
  * Finality (orthogonal to this file) is the union of two independent rules:
  *   - attestation ≥ 2/3 weight on a chain-canonical hash (BFT-classic, fast path in healthy network)
  *   - depth-k confirmation (probabilistic, fallback when attestations stall) Whichever fires first marks the tip finalized.
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
    * Returns false if current tip is finalized, because finalized tips cannot be reverted.
    *
    * @return
    *   true if candidate beats current and current is not finalized
    */
  def shouldSwitch(current: ChainTip, candidate: ChainTip): F[Boolean]
}

object ChainSelection {

  /** Default parameters (tunable per deployment). */
  val DefaultKLookback: Long = 50L // blocks before switching to density rule
  val DefaultSWindow: Long = 200L // slot window for density comparison

  /** Create a ChainSelection that uses purely structural (Bifrost maxvalid-tk + maxvalid-bg) fork choice.
    *
    * Attestation weight does not enter `compare`. `tipTracker` is retained only so `shouldSwitch` can consult `lastFinalized` to refuse
    * reverting below a finalized head — a *constraint* on fork choice, not a tiebreaker.
    *
    * @param tipTracker
    *   used by `shouldSwitch` to query the finalized head; not used in `compare`.
    * @param fetchParent
    *   given a ChainTip, retrieve its parent (for ancestor traversal).
    * @param kLookback
    *   max blocks to traverse before switching to density rule.
    * @param sWindow
    *   forward-looking slot window for density comparison.
    */
  def make[F[_]: Monad](
    tipTracker: TipTracker[F],
    fetchParent: ChainTip => F[Option[ChainTip]],
    kLookback: Long = DefaultKLookback,
    sWindow: Long = DefaultSWindow
  ): ChainSelection[F] =
    new ChainSelection[F] {

      def compare(tipA: ChainTip, tipB: ChainTip): F[ChainTip] =
        if (tipA.hash === tipB.hash) tipA.pure[F]
        else findCommonAncestorAndSelect(tipA, tipB)

      def selectBest(candidates: List[ChainTip]): F[Option[ChainTip]] =
        candidates match {
          case Nil         => none[ChainTip].pure[F]
          case head :: Nil => head.some.pure[F]
          case head :: tail =>
            tail.foldLeftM(head)((best, candidate) => compare(best, candidate)).map(_.some)
        }

      def shouldSwitch(current: ChainTip, candidate: ChainTip): F[Boolean] =
        if (current.hash === candidate.hash)
          false.pure[F]
        else
          for {
            lastFinalized <- tipTracker.lastFinalized
            winner <- compare(current, candidate)
          } yield {
            val candidateWins = winner.hash === candidate.hash
            val currentIsFinalized = lastFinalized.exists { case (fh, _) => fh === current.hash }
            candidateWins && !currentIsFinalized
          }

      /** Walk back both tines to find common ancestor, then apply appropriate rule. */
      private def findCommonAncestorAndSelect(tipA: ChainTip, tipB: ChainTip): F[ChainTip] =
        // Simple case: if one is ancestor of the other (same chain), longer wins
        if (tipA.parentHash === tipB.hash) tipA.pure[F]
        else if (tipB.parentHash === tipA.hash) tipB.pure[F]
        else {
          // Build both tines back to common ancestor (or kLookback)
          buildTines(List(tipA), List(tipB), 0L).map {
            case (tineA, tineB, exceedsK) =>
              if (exceedsK) {
                // Long fork: density comparison within sWindow
                densityCompare(tineA, tineB, tipA, tipB)
              } else {
                // Short fork: standard tiebreakers on tips
                standardCompare(tipA, tipB)
              }
          }
        }

      /** Recursively build tines back to common ancestor. Returns (tineA, tineB, exceedsKLookback). Each tine is oldest-first (ancestor at
        * head, tip at end).
        */
      private def buildTines(
        tineA: List[ChainTip],
        tineB: List[ChainTip],
        depth: Long
      ): F[(List[ChainTip], List[ChainTip], Boolean)] = {
        val headA = tineA.head
        val headB = tineB.head

        // Common ancestor found
        if (headA.hash === headB.hash)
          (tineA, tineB, depth > kLookback).pure[F]
        // Exceeded k-lookback — switch to density
        else if (depth > kLookback)
          (tineA, tineB, true).pure[F]
        else {
          // Walk back the taller tine (or both if equal height)
          val aOrdinal = headA.ordinal
          val bOrdinal = headB.ordinal

          if (aOrdinal > bOrdinal) {
            // Walk A back
            fetchParent(headA).flatMap {
              case Some(parentA) => buildTines(parentA :: tineA, tineB, depth + 1)
              case None          => (tineA, tineB, depth > kLookback).pure[F] // hit genesis
            }
          } else if (bOrdinal > aOrdinal) {
            // Walk B back
            fetchParent(headB).flatMap {
              case Some(parentB) => buildTines(tineA, parentB :: tineB, depth + 1)
              case None          => (tineA, tineB, depth > kLookback).pure[F]
            }
          } else {
            // Equal height — walk both back
            (fetchParent(headA), fetchParent(headB)).tupled.flatMap {
              case (Some(parentA), Some(parentB)) =>
                buildTines(parentA :: tineA, parentB :: tineB, depth + 1)
              case _ =>
                (tineA, tineB, depth > kLookback).pure[F]
            }
          }
        }
      }

      /** Standard (short fork) comparison: height → slot → VRF tiebreak. */
      private def standardCompare(tipA: ChainTip, tipB: ChainTip): ChainTip =
        // 1. Higher ordinal (longer chain)
        if (tipA.ordinal != tipB.ordinal) {
          if (tipA.ordinal > tipB.ordinal) tipA else tipB
        }
        // 2. Lower slot (earlier production = harder lottery)
        else if (tipA.slot.value.value != tipB.slot.value.value) {
          if (tipA.slot.value.value < tipB.slot.value.value) tipA else tipB
        }
        // 3. Lower VRF output (deterministic)
        else {
          if (compareVrfOutputs(tipA.vrfOutput, tipB.vrfOutput) <= 0) tipA else tipB
        }

      /** Density comparison: count blocks within sWindow from the fork point. More blocks in the window = denser chain = better. On tie:
        * fall back to VRF tiebreak on tips.
        */
      private def densityCompare(
        tineA: List[ChainTip],
        tineB: List[ChainTip],
        tipA: ChainTip,
        tipB: ChainTip
      ): ChainTip = {
        // The fork point is the common ancestor (head of each tine)
        val forkSlot = tineA.headOption.map(_.slot.value.value).getOrElse(0L)

        // Count blocks within sWindow slots from fork point
        val densityA = tineA.count(t => t.slot.value.value - forkSlot <= sWindow)
        val densityB = tineB.count(t => t.slot.value.value - forkSlot <= sWindow)

        if (densityA != densityB) {
          if (densityA > densityB) tipA else tipB
        } else {
          // Equal density: VRF tiebreak on tips
          if (compareVrfOutputs(tipA.vrfOutput, tipB.vrfOutput) <= 0) tipA else tipB
        }
      }

      /** Compare VRF outputs as unsigned BigInts. */
      private def compareVrfOutputs(a: VrfOutput, b: VrfOutput): Int = {
        val bigA = BigInt(1, a.toBytes)
        val bigB = BigInt(1, b.toBytes)
        bigA.compare(bigB)
      }
    }
}
