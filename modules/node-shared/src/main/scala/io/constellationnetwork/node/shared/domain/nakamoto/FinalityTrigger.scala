package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Phase 1 → Phase 2 finality trigger.
  *
  * The 4-phase finality model (see `docs/nakamoto/attestation-and-finality.md` §0) has three Phase 1→2 triggers: `T_weight` (2/3
  * attestation weight), `T_count` (1-validator-1-vote ≥ 2/3 attester count), and `T_depth1` (depth-k₁ confirmation). All triggers run in
  * parallel with max-of semantics: a snapshot advances to Phase 2 as soon as ANY trigger qualifies it.
  *
  * Each trigger is a small monotone observable: given a `ConsensusState`, compute the highest ordinal the trigger has qualified. The result
  * may NEVER go backwards — once a trigger has qualified ordinal N, every ord ≤ N is also qualified by that trigger.
  *
  * Use [[FinalityTrigger.triggersFor]] for the inverse lookup ("which triggers qualified ord N?"), built directly from each trigger's
  * `latestQualifyingOrdinal`. This is free observability for the future chain-quality observable (task #138) and post-mortem
  * "why-did-this-finalize-when-it-did" analysis.
  */
trait FinalityTrigger[F[_]] {

  /** Stable kind tag for telemetry, lookups, doc references. */
  def kind: FinalityTrigger.Kind

  /** Monotone non-decreasing — once a trigger qualifies ordinal N, it has qualified every ord ≤ N. Refreshed only via
    * [[evaluateAndAdvance]]; [[evaluate]] never mutates this.
    */
  def latestQualifyingOrdinal: F[SnapshotOrdinal]

  /** Compute the qualifying ordinal from current state. Implementations of this method ONLY READ external state; they MUST NOT mutate the
    * trigger's internal `latestQualifyingOrdinal`. Useful for tests + debug. The caller is responsible for advancing the trigger via
    * [[evaluateAndAdvance]] in production.
    */
  def evaluate(state: FinalityTrigger.ConsensusState[F]): F[SnapshotOrdinal]

  /** Run [[evaluate]]; if the result is strictly greater than the stored latest, advance the Ref. Returns the new latestQualifyingOrdinal
    * (which is either the just-evaluated ord, or the previous one if evaluate went backwards).
    */
  def evaluateAndAdvance(state: FinalityTrigger.ConsensusState[F]): F[SnapshotOrdinal]
}

object FinalityTrigger {

  /** Tag for each Phase 1→2 trigger. Currently implemented: [[Kind.TWeight]], [[Kind.TCount]], [[Kind.TDepth1]]. Reserved for future task:
    * [[Kind.TDepth2]] (#137).
    */
  sealed trait Kind { def name: String }
  object Kind {
    case object TWeight extends Kind { val name = "t_weight" }
    case object TCount extends Kind { val name = "t_count" }
    case object TDepth1 extends Kind { val name = "t_depth1" }
    case object TDepth2 extends Kind { val name = "t_depth2" } // reserved for #137

    val all: List[Kind] = List(TWeight, TCount, TDepth1, TDepth2)
  }

  /** Snapshot of inputs each trigger may read at evaluation time. Concrete triggers ignore fields they don't care about. Adding a field
    * here is the only place to update callers — keep it minimal.
    *
    * The `F` is carried so `canonicalHashAt` can stay effectful (chain walk).
    *
    * @param selfId
    *   this node's PeerId. Used by [[TWeightTrigger]] to self-exclude (task #133).
    * @param bestTipOrdinal
    *   ordinal of the current best chain tip. Used by [[TDepth1Trigger]] to compute `bestTipOrdinal - k₁`.
    * @param bestTipHash
    *   hash of the current best chain tip. Used by [[TWeightTrigger]] to walk back into canonical history.
    * @param canonicalHashAt
    *   chain walk: returns the hash on our canonical chain at the given ordinal (typically `chainStore.walkBackTo(bestTipHash, ord)`). Used
    *   by [[TWeightTrigger]] to filter cross-fork attestations from the weight sum.
    */
  final case class ConsensusState[F[_]](
    selfId: PeerId,
    bestTipOrdinal: SnapshotOrdinal,
    bestTipHash: Hash,
    canonicalHashAt: Long => F[Option[Hash]]
  )

  /** Pure lookup: which triggers have qualified `ord`?
    *
    * Monotonicity means a trigger qualifies `ord` iff its `latestQualifyingOrdinal >= ord`. Set-valued so all subsets are representable (a
    * snapshot can be qualified by any combination of T_weight, T_count, T_depth1, T_depth2).
    *
    * Returned as a `Set[Kind]` (not `List`) because order is irrelevant — the lookup is "did each trigger qualify yet?", not "in which
    * order did they fire?".
    */
  def triggersFor[F[_]: Monad](
    triggers: List[FinalityTrigger[F]],
    ord: SnapshotOrdinal
  ): F[Set[Kind]] =
    triggers
      .traverse(t => t.latestQualifyingOrdinal.map(latest => Option.when(latest >= ord)(t.kind)))
      .map(_.flatten.toSet)

  /** Helper: choose the highest `latestQualifyingOrdinal` across a list of triggers. Equivalent to today's `max(t_weight, t_depth1)` before
    * `chainStore.finalize` — preserves the max-of semantics under any composition.
    */
  def maxLatestQualifyingOrdinal[F[_]: Monad](
    triggers: List[FinalityTrigger[F]]
  ): F[SnapshotOrdinal] =
    triggers.traverse(_.latestQualifyingOrdinal).map { ords =>
      if (ords.isEmpty) SnapshotOrdinal.MinValue
      else ords.maxBy(_.value.value)
    }

  /** Common construction: a trigger backed by a `Ref[F, SnapshotOrdinal]` that only ever increases.
    *
    * @param myKind
    *   the trigger's stable tag
    * @param init
    *   initial qualifying ordinal (typically `SnapshotOrdinal.MinValue`)
    * @param eval
    *   the evaluation function — read-only inspection of `ConsensusState`
    */
  def fromRef[F[_]: Sync](
    myKind: Kind,
    init: SnapshotOrdinal
  )(eval: ConsensusState[F] => F[SnapshotOrdinal]): F[FinalityTrigger[F]] =
    Ref.of[F, SnapshotOrdinal](init).map { ref =>
      new FinalityTrigger[F] {
        def kind: Kind = myKind

        def latestQualifyingOrdinal: F[SnapshotOrdinal] = ref.get

        def evaluate(state: ConsensusState[F]): F[SnapshotOrdinal] = eval(state)

        def evaluateAndAdvance(state: ConsensusState[F]): F[SnapshotOrdinal] =
          eval(state).flatMap { newOrd =>
            ref.updateAndGet { prev =>
              // math.max via Order — never let the Ref go backwards even if `eval` regresses.
              if (newOrd.value.value > prev.value.value) newOrd else prev
            }
          }
      }
    }
}

/** Production builder for the `T_weight` trigger (2/3 attestation finality on the canonical chain).
  *
  * Wraps [[TipTracker.highestFinalizedOrdinal]] — self-excludes `state.selfId` (#133), filters attestations by canonical-hash match (#119
  * fork-recovery-deadlock fix), and walks attestation ordinals from highest down accumulating weight.
  */
object TWeightTrigger {

  /** @param tipTracker
    *   attestation accumulator (source of weights + canonical-filter walk)
    * @param threshold
    *   cumulative stake fraction required to qualify (e.g. `TipTracker.FinalityThreshold` = 2/3)
    */
  def make[F[_]: Sync](
    tipTracker: TipTracker[F],
    threshold: Ratio
  ): F[FinalityTrigger[F]] =
    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TWeight, SnapshotOrdinal.MinValue) { state =>
      tipTracker
        .highestFinalizedOrdinal(state.selfId, threshold, state.canonicalHashAt)
        .map {
          case Some((ord, _)) => SnapshotOrdinal.unsafeApply(ord)
          case None           => SnapshotOrdinal.MinValue
        }
    }
}

/** Production builder for the `T_depth1` trigger (depth-k₁ confirmation finality).
  *
  * Qualifies ordinal `bestTipOrdinal - k`, clamped to `MinValue` while the chain is shorter than `k`. This is the structural Bitcoin-style
  * fallback that fires when attestation triggers stall (small clusters, partition, adversarial 1/3).
  */
object TDepth1Trigger {

  /** @param k
    *   confirmation depth (typically `NAKAMOTO_CONFIRMATION_DEPTH`, default 255).
    */
  def make[F[_]: Sync](k: Long): F[FinalityTrigger[F]] =
    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TDepth1, SnapshotOrdinal.MinValue) { state =>
      val bestOrd = state.bestTipOrdinal.value.value
      val qualifying = bestOrd - k
      val ord =
        if (qualifying > 0L) SnapshotOrdinal.unsafeApply(qualifying)
        else SnapshotOrdinal.MinValue
      Sync[F].pure(ord)
    }
}

/** Production builder for the `T_count` trigger (1-validator-1-vote count finality on the canonical chain).
  *
  * Counts the number of DISTINCT attesters whose `tipHash` matches our canonical chain at their `tipOrdinal`, walks attestation ordinals
  * from highest down, and qualifies the highest ordinal where the cumulative count reaches `ceil(threshold * validatorCount)`. Counts
  * peers, not weights — under equal stake this ties with [[TWeightTrigger]]; under future stake-weighted VRF this is strictly stronger
  * evidence (a single high-stake validator can hit the 2/3 *weight* threshold alone, but cannot fake a count of distinct attesters).
  *
  * '''Why hash-aware''': same as `T_weight`. Attesting to ordinal N on fork A must not count toward finalizing ordinal N on fork B.
  *
  * '''Why self-exclusion''' (task #133): a node MUST NOT count its own attestation toward its own finality threshold, otherwise it can
  * self-finalize a divergent fork and trip the finality-safety gate in `chainStore.finalize`, locking the node out of canonical recovery
  * (the "fork-recovery deadlock" of #119). Parity with [[TWeightTrigger]].
  *
  * '''Denominator''': `validatorCount` (full seedlist), NOT the observed-active set. Counting against the full validator set means a
  * partition that loses 1/3 of the network correctly DOES NOT count-finalize (count below 2/3 of full). This is intentional — depth-k
  * fallback (T_depth1) handles the partitioned case.
  */
object TCountTrigger {

  /** @param tipTracker
    *   attestation accumulator. Read for `allAttestations`.
    * @param stakeRegistry
    *   validator registry. Read for `validatorCount` (the threshold denominator — full seedlist).
    * @param threshold
    *   fraction of validators required (e.g. `TipTracker.FinalityThreshold` = 2/3). Reuses the same env knob as `T_weight` so both 2/3
    *   triggers share one configuration surface.
    */
  def make[F[_]: Sync](
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    threshold: Ratio
  ): F[FinalityTrigger[F]] =
    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TCount, SnapshotOrdinal.MinValue) { state =>
      for {
        attestations <- tipTracker.allAttestations
        validatorCount <- stakeRegistry.validatorCount
        // Self-exclusion (#133): drop our own attestation before the canonical-hash filter.
        // Parity with TWeight — see the docstring on this object for why.
        nonSelf = attestations.iterator.filter { case (peerId, _) => peerId =!= state.selfId }.toList
        // Canonical-hash filter: keep only attestations whose tipHash is on our chain at their
        // tipOrdinal. Attestations on other forks contribute zero count for finalizing our chain.
        onChainOrdinals <- nonSelf.traverse[F, Option[Long]] {
          case (_, att) =>
            state.canonicalHashAt(att.tipOrdinal).map {
              case Some(localHash) if localHash === att.tipHash => Some(att.tipOrdinal)
              case _                                            => None
            }
        }
      } yield {
        // GRANDPA ancestor rule (parity with TWeight): attesting to ord N with a hash on our
        // canonical chain implicitly attests to all ancestors of that hash. Walk from highest
        // ordinal down, accumulating the count of distinct peers reached so far. The highest
        // ordinal at which the cumulative count meets the threshold is T_count's qualifying ord.
        //
        // Each peer's latest attestation supersedes earlier ones (TipTracker.recordAttestation
        // contract), so the map is already keyed by peer — `nonSelf` has at most one entry per
        // peer, which becomes one ordinal in `onChainOrdinals`. No de-duplication needed here.
        val sortedDesc = onChainOrdinals.flatten.sorted(Ordering[Long].reverse)
        // Exact threshold count: smallest n such that n * thresholdDenom >= thresholdNum * validatorCount.
        // This matches `n / validatorCount >= threshold` without IEEE 754 division (Ratio is BigInt math).
        // Note: at validatorCount=0 the required count is 0, but `sortedDesc` is also empty (no peers ⇒
        // no attestations), so `collectFirst` yields None → MinValue. Safe in cold-start.
        val thresholdNum = threshold.numerator
        val thresholdDen = threshold.denominator
        // ceil(thresholdNum * validatorCount / thresholdDen) — done with BigInt to avoid Long overflow
        // and stay consensus-deterministic with the rest of the Ratio path.
        val required: BigInt = {
          val target = thresholdNum * BigInt(validatorCount)
          val q = target / thresholdDen
          if (target % thresholdDen == 0) q else q + 1
        }
        var cumCount: BigInt = BigInt(0)
        sortedDesc.collectFirst {
          case ordinal if { cumCount = cumCount + 1; cumCount >= required } =>
            SnapshotOrdinal.unsafeApply(ordinal)
        }.getOrElse(SnapshotOrdinal.MinValue)
      }
    }
}
