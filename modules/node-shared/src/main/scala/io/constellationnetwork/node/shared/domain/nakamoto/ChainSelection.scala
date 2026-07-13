package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.VrfOutput

/** Fork choice rule for Nakamoto consensus — purely structural (Bitcoin/Praos/Polkadot-BABE pattern).
  *
  * **Strict separation of fork choice and Phase 2.** Fork choice picks the best valid branch from header structure (maxvalid-tk for shallow
  * forks, maxvalid-bg density for long forks). Decided-attestation T_weight or k1 depth makes an exact hash operational but reversible; it
  * does not create a BFT lock or absolute floor. It is deliberately NOT Ethereum's LMD-GHOST blend, which couples latest-attestation weight
  * into the comparator and has produced a long tail of balancing/bouncing/avalanche attacks (Neu/Tas/Tse IACR 2022/289; D'Amato/Zanolini
  * IACR 2023/279). For a 50-100 validator chain with depth-k fallback, the dynamic- availability head-convergence properties LMD-GHOST buys
  * aren't worth the attack surface.
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
  * Phase 2 (orthogonal to `compare`) is the union of two exact-hash rules:
  *   - decided-attestation T_weight from the Avalanche/Snowball optimistic rail
  *   - canonical k1 depth fallback. Neither rule introduces global BFT voting/locking.
  *
  * '''Transitional implementation gap.''' [[shouldSwitch]] still offers a flag-selected k1 or k2 ordinal floor. Neither behavior is the
  * locked target. P2 density reorgs remain objectively comparable; k2 only recommends local retention/proof/recovery capacity. When the
  * true MRCA is older than retained history, the node must enter `RecoveryRequired`, reconstruct exact authenticated history, and resume
  * ordinary comparison. It must not refuse or select a branch merely because a local floor was crossed.
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
    * Current code returns false at a flag-selected legacy k1/k2 floor. That behavior is transitional and violates the target rule above.
    *
    * @return
    *   true if the current implementation permits switching to the structurally preferred candidate
    */
  def shouldSwitch(current: ChainTip, candidate: ChainTip): F[Boolean]
}

object ChainSelection {

  /** Default parameters. Retained as fallbacks ONLY for call sites (tests) that don't wire the config-derived values. The production call
    * site (`GlobalSnapshotConsensus`) now passes `NakamotoConfig.kLookback` (= k₁ + 1) and `NakamotoConfig.sWindow` (= round(R/3)) —
    * Track-3 S3 killed the hardcoded 50/200 there.
    */
  val DefaultKLookback: Long = 50L // blocks before switching to density rule
  val DefaultSWindow: Long = 200L // slot window for density comparison

  /** Create a ChainSelection that uses purely structural (Bifrost maxvalid-tk + maxvalid-bg) fork choice.
    *
    * Attestation weight does not enter `compare`. `tipTracker` is retained only for the transitional k1 clamp in `shouldSwitch`; target
    * fork choice removes that clamp while retaining exact-hash Phase-2 rollback notifications.
    *
    * @param tipTracker
    *   used by `shouldSwitch` (legacy/flag-OFF path) to query the k₁ finalized head; not used in `compare`.
    * @param fetchParent
    *   given a ChainTip, retrieve its parent (for ancestor traversal).
    * @param kLookback
    *   max fork depth treated as a "short" fork (tip tiebreak); deeper forks use the density rule.
    * @param sWindow
    *   forward-looking slot window for density comparison.
    * @param maxAncestorDepth
    *   current local bound on the true-MRCA walk. Exhaustion must ultimately signal RecoveryRequired rather than determine the winner
    * @param settledOrdinalReader
    *   transitional reader for the legacy k2 floor. Target removes its fork-choice role
    * @param bandDensityReorgEnabled
    *   transitional selector between legacy k1 and k2 floor behavior. Neither setting is the locked target
    */
  def make[F[_]: Monad](
    tipTracker: TipTracker[F],
    fetchParent: ChainTip => F[Option[ChainTip]],
    kLookback: Long = DefaultKLookback,
    sWindow: Long = DefaultSWindow,
    maxAncestorDepth: Long = DefaultKLookback,
    settledOrdinalReader: Option[F[Long]] = None,
    bandDensityReorgEnabled: Boolean = false
  ): ChainSelection[F] =
    new ChainSelection[F] {

      // Local ancestor-walk bound for the true-MRCA search. Flag OFF reproduces the pre-S3 truncation
      // (`depth > kLookback`) EXACTLY via `kLookback + 1` (so `depth >= bound` ⟺ `depth > kLookback`);
      // flag ON walks up to local k2 capacity. Bound exhaustion must become RecoveryRequired in target code.
      private val ancestorWalkBound: Long =
        if (bandDensityReorgEnabled) maxAncestorDepth else kLookback + 1L

      // Transitional legacy k2 floor reader (band path only); forbidden as target fork-choice input.
      private val settledOrdinalF: F[Long] =
        settledOrdinalReader.getOrElse(Monad[F].pure(0L))

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
        else if (!bandDensityReorgEnabled)
          // FLAG OFF — transitional legacy k1 hash-exact clamp. Fork choice is
          // frozen at the k₁ finalized head: refuse to switch away from a tip that IS the finalized head.
          for {
            lastFinalized <- tipTracker.lastFinalized
            winner <- compare(current, candidate)
          } yield {
            val candidateWins = winner.hash === candidate.hash
            val currentIsFinalized = lastFinalized.exists { case (fh, _) => fh === current.hash }
            candidateWins && !currentIsFinalized
          }
        else
          // FLAG ON — transitional legacy k2 floor. The current implementation's ONLY
          // reorg refusal here is one that would rewrite history at/below the settled (k₂) ordinal: refuse
          // iff the fork's common ancestor is STRICTLY below settled (its deepest reverted block,
          // mrca.ordinal + 1, would then be ≤ settled). A fork deeper than the k₂ walk bound (MRCA not
          // found) is treated as below-settled ⇒ refuse — it cannot be a legitimate band reorg.
          for {
            settled <- settledOrdinalF
            winner <- compare(current, candidate)
            forkOrdinalOpt <- forkAncestorOrdinal(current, candidate)
          } yield {
            val candidateWins = winner.hash === candidate.hash
            val revertsAtOrBelowSettled = forkOrdinalOpt.fold(true)(_ < settled)
            candidateWins && !revertsAtOrBelowSettled
          }

      /** Walk back both tines to the true common ancestor (bounded by `ancestorWalkBound`), then apply the short/long-fork rule. */
      private def findCommonAncestorAndSelect(tipA: ChainTip, tipB: ChainTip): F[ChainTip] =
        // Simple case: if one is the immediate parent of the other (same chain), the descendant wins (longer).
        if (tipA.parentHash === tipB.hash) tipA.pure[F]
        else if (tipB.parentHash === tipA.hash) tipB.pure[F]
        else
          buildTines(List(tipA), List(tipB), 0L).map {
            case (tineA, tineB, mrca, forkDepth) =>
              if (forkDepth > kLookback)
                // Long fork: density comparison within sWindow from the (true) fork point.
                densityCompare(tineA, tineB, tipA, tipB, mrca)
              else
                // Short fork: standard tip tiebreakers.
                standardCompare(tipA, tipB)
          }

      /** Most-recent common-ancestor ordinal of the two tips (the fork point), or None if the fork is deeper than `ancestorWalkBound` (> k₂
        * under the flag) or the walk hit an evicted/genesis gap. Reuses the SAME symmetric tine walk as `compare`, so the fork point it
        * reports is identical to the one the density comparison anchored on.
        */
      private def forkAncestorOrdinal(tipA: ChainTip, tipB: ChainTip): F[Option[Long]] =
        if (tipA.hash === tipB.hash) Option(tipA.ordinal).pure[F]
        else if (tipA.parentHash === tipB.hash) Option(tipB.ordinal).pure[F]
        else if (tipB.parentHash === tipA.hash) Option(tipA.ordinal).pure[F]
        else buildTines(List(tipA), List(tipB), 0L).map { case (_, _, mrca, _) => mrca.map(_.ordinal) }

      /** Recursively build tines back to the true most-recent common ancestor (MRCA), bounded by `ancestorWalkBound`.
        *
        * Returns `(tineA, tineB, mrca, forkDepth)`:
        *   - `tineA`/`tineB` oldest-first (ancestor-or-truncation-head at head, tip at end),
        *   - `mrca` = Some(commonAncestor) when found within the bound, else None (truncated / parent gap),
        *   - `forkDepth` = number of walk levels at stop.
        *
        * '''Commutativity (Track-3 S3).''' Which tine is walked at each step is decided by ORDINAL comparison (not arg position), and every
        * stop condition (heads equal, or `depth >= bound`, or a parent gap) is symmetric — so the MRCA and `forkDepth` are IDENTICAL for
        * `(A, B)` and `(B, A)`. The prior implementation truncated the MRCA search at `kLookback` (before the true fork point for band
        * forks) and let `densityCompare` anchor on the FIRST argument's truncation head, breaking `compare(A,B) == compare(B,A)`; walking
        * to the real MRCA is what fixes it.
        */
      private def buildTines(
        tineA: List[ChainTip],
        tineB: List[ChainTip],
        depth: Long
      ): F[(List[ChainTip], List[ChainTip], Option[ChainTip], Long)] = {
        val headA = tineA.head
        val headB = tineB.head

        // Common ancestor found.
        if (headA.hash === headB.hash)
          (tineA, tineB, Some(headA): Option[ChainTip], depth).pure[F]
        // Exceeded the ancestor-walk bound — give up finding the MRCA (flag OFF: reproduces the legacy
        // `depth > kLookback` truncation; flag ON: required history exceeds local k2 capacity).
        else if (depth >= ancestorWalkBound)
          (tineA, tineB, Option.empty[ChainTip], depth).pure[F]
        else {
          // Walk back the taller tine (or both if equal height) — the choice is symmetric in arg order.
          val aOrdinal = headA.ordinal
          val bOrdinal = headB.ordinal

          if (aOrdinal > bOrdinal) {
            fetchParent(headA).flatMap {
              case Some(parentA) => buildTines(parentA :: tineA, tineB, depth + 1L)
              case None          => (tineA, tineB, Option.empty[ChainTip], depth).pure[F] // hit genesis / evicted
            }
          } else if (bOrdinal > aOrdinal) {
            fetchParent(headB).flatMap {
              case Some(parentB) => buildTines(tineA, parentB :: tineB, depth + 1L)
              case None          => (tineA, tineB, Option.empty[ChainTip], depth).pure[F]
            }
          } else {
            // Equal height — walk both back.
            (fetchParent(headA), fetchParent(headB)).tupled.flatMap {
              case (Some(parentA), Some(parentB)) =>
                buildTines(parentA :: tineA, parentB :: tineB, depth + 1L)
              case _ =>
                (tineA, tineB, Option.empty[ChainTip], depth).pure[F]
            }
          }
        }
      }

      /** Standard (short fork) comparison: height → slot → deterministic (VRF, then hash) tiebreak. */
      private def standardCompare(tipA: ChainTip, tipB: ChainTip): ChainTip =
        // 1. Higher ordinal (longer chain).
        if (tipA.ordinal != tipB.ordinal) {
          if (tipA.ordinal > tipB.ordinal) tipA else tipB
        }
        // 2. Lower slot (earlier production = harder lottery).
        else if (tipA.slot.value.value != tipB.slot.value.value) {
          if (tipA.slot.value.value < tipB.slot.value.value) tipA else tipB
        }
        // 3. Fully deterministic tip tiebreak.
        else deterministicTip(tipA, tipB)

      /** Density comparison (maxvalid-bg): more blocks within `sWindow` slots of the fork point = denser chain = better. On tie: the
        * fully-deterministic tip tiebreak.
        *
        * '''Track-3 S3 commutativity fix.''' The fork anchor `forkSlot` is the TRUE MRCA slot when the common ancestor was found (`mrca =
        * Some`) — which is SYMMETRIC in arg order, replacing the previous `tineA.head` anchor that made `compare(A,B) ≠ compare(B,A)` on
        * band forks (the tineA-only asymmetry). Because both tines descend from the MRCA, every `slot - forkSlot ≥ 0`, so the prior
        * "negative `(slot − forkSlot)` always counts" artifact is gone too. The `None` (unfound-MRCA) fallback to the legacy `tineA.head`
        * anchor is only reachable OFF the band path (flag-OFF deep-fork truncation — preserved byte-for-byte — or a > k₂ fork, which is
        * below the settled freeze and cannot legitimately reorg).
        */
      private def densityCompare(
        tineA: List[ChainTip],
        tineB: List[ChainTip],
        tipA: ChainTip,
        tipB: ChainTip,
        mrca: Option[ChainTip]
      ): ChainTip = {
        // The fork point is the true common ancestor (symmetric); legacy fallback = tineA head (only off-band).
        val forkSlot =
          mrca.map(_.slot.value.value).getOrElse(tineA.headOption.map(_.slot.value.value).getOrElse(0L))

        // Count blocks within sWindow slots from the fork point.
        val densityA = tineA.count(t => t.slot.value.value - forkSlot <= sWindow)
        val densityB = tineB.count(t => t.slot.value.value - forkSlot <= sWindow)

        if (densityA != densityB) {
          if (densityA > densityB) tipA else tipB
        } else {
          // Equal density: fully deterministic tip tiebreak.
          deterministicTip(tipA, tipB)
        }
      }

      /** Fully deterministic, commutative final tiebreak: lower VRF output wins; on an EXACT VRF tie (cryptographically unreachable for
        * distinct honest snapshots — only an equivocating same-slot producer) the lexicographically-lower snapshot hash wins. This totality
        * is what makes `compare` commutative for EVERY input pair (cluster-uniformity): `compare(A,B) == compare(B,A)`. The hash tiebreak
        * is a strict improvement over the pre-S3 `vrf <= 0 ? tipA : tipB` (which was arg-order-dependent on an exact VRF tie); it changes
        * NO honest outcome (VRF collisions don't occur) and makes the adversarial equivocation case converge.
        */
      private def deterministicTip(tipA: ChainTip, tipB: ChainTip): ChainTip = {
        val vrfCmp = compareVrfOutputs(tipA.vrfOutput, tipB.vrfOutput)
        if (vrfCmp < 0) tipA
        else if (vrfCmp > 0) tipB
        else if (tipA.hash.value.compareTo(tipB.hash.value) <= 0) tipA
        else tipB
      }

      /** Compare VRF outputs as unsigned BigInts. */
      private def compareVrfOutputs(a: VrfOutput, b: VrfOutput): Int = {
        val bigA = BigInt(1, a.toBytes)
        val bigB = BigInt(1, b.toBytes)
        bigA.compare(bigB)
      }
    }
}
