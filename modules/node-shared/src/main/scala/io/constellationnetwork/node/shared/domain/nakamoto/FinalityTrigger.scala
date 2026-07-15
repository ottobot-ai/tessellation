package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Ordinal-projection trigger abstraction plus transitional legacy telemetry kinds.
  *
  * The locked GL0 lifecycle has P0/P1/P2 only. An exact locally authenticated and executed snapshot reaches reversible P2 through
  * decided-attestation `T_weight` or canonical k1 depth. This is an OR of independent optimistic/depth rails, not a BFT vote/lock/QC.
  * `T_count` and `T_depth2` remain represented by current executable telemetry, but they are not target finality predicates: count is
  * subsumed by decided-attestation weight, and k2 is only recommended local retention/proof/recovery capacity.
  *
  * Each trigger is currently a monotone ordinal observable: given a `ConsensusState`, compute the highest ordinal the trigger has
  * qualified. This type cannot identify the qualifying hash, represent same-ordinal replacement, or own target Phase-2 state. The
  * hash-bound finality coordinator must replace this projection at the state-changing boundary; these values may remain telemetry.
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

  /** Stable tag for current trigger/telemetry instances. Target P2 kinds are [[Kind.TWeight]] and [[Kind.TDepth1]]. [[Kind.TCount]] and
    * [[Kind.TDepth2]] are legacy implementation debt and must not be interpreted as extra phases or finality rails.
    */
  sealed trait Kind { def name: String }
  object Kind {
    case object TWeight extends Kind { val name = "t_weight" }
    case object TCount extends Kind { val name = "t_count" }
    case object TDepth1 extends Kind { val name = "t_depth1" }
    case object TDepth2 extends Kind { val name = "t_depth2" }

    val all: List[Kind] = List(TWeight, TCount, TDepth1, TDepth2)
  }

  /** Snapshot of inputs each trigger may read at evaluation time. Concrete triggers ignore fields they don't care about. Adding a field
    * here is the only place to update callers — keep it minimal.
    *
    * The `F` is carried so `canonicalHashAt` can stay effectful (chain walk).
    *
    * @param selfId
    *   this node's PeerId. Retained for [[TCountTrigger]] self-exclusion (task #133, still needed for the count path because T_count counts
    *   distinct attesters and self-counting would double-promote a node's own evidence under the count rule). NOT used by
    *   [[TWeightTrigger]] in the current wiring. That omission of `selfId` does not make its sticky margin observer-independent: receipt
    *   order can still produce opposing decisions from the same eventual current attestations.
    * @param bestTipOrdinal
    *   ordinal of the current best chain tip. Used by [[TDepth1Trigger]] to compute `bestTipOrdinal - k₁`.
    * @param bestTipHash
    *   hash of the current best chain tip. Used by [[TWeightTrigger]] to walk back into canonical history.
    * @param canonicalHashAt
    *   chain walk: returns the hash on our canonical chain at the given ordinal (typically `chainStore.walkBackTo(bestTipHash, ord)`). Used
    *   by [[TWeightTrigger]] to filter cross-fork entries from the transitional sticky-decision set.
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
    * current telemetry can report any combination of T_weight, T_count, T_depth1, T_depth2).
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

  /** Legacy helper: choose the highest ordinal projection across a list of triggers. It cannot select or prove an exact hash and therefore
    * must not drive the target Phase-2 transition coordinator.
    */
  def maxLatestQualifyingOrdinal[F[_]: Monad](
    triggers: List[FinalityTrigger[F]]
  ): F[SnapshotOrdinal] =
    triggers.traverse(_.latestQualifyingOrdinal).map { ords =>
      if (ords.isEmpty) SnapshotOrdinal.MinValue
      else ords.maxBy(_.value.value)
    }

  /** Common transitional construction backed by an ordinal-only `Ref` that only ever increases. This cannot represent a same-ordinal
    * canonical replacement; use it for calculator telemetry, not canonical phase ownership.
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

/** Observability seam for the chain-quality HTTP route (task #138).
  *
  * Wraps a list of [[FinalityTrigger]] instances and answers "which triggers qualified ord N?" without exposing the trigger objects
  * themselves to the HTTP layer. Constructed once per leader-loop instance (closing over the in-scope triggers) and handed to the HTTP
  * layer via a `Ref[F, Option[FinalityTriggerView[F]]]` populated at startup. Pure observability — never feeds back into consensus.
  */
trait FinalityTriggerView[F[_]] {

  /** Which current trigger/telemetry calculators have qualified the given ordinal? Set-valued so legacy kinds remain observable. Only
    * `T_weight` and `T_depth1` are target P2 rails. Empty means no calculator has qualified `ord` yet.
    */
  def triggersFor(ord: SnapshotOrdinal): F[Set[FinalityTrigger.Kind]]
}

object FinalityTriggerView {
  def apply[F[_]](implicit F: FinalityTriggerView[F]): FinalityTriggerView[F] = F

  /** Wrap a concrete trigger list as a view. The list is captured by reference; the view reads each trigger's current
    * `latestQualifyingOrdinal` on every call (no caching), so it always reflects the most recent `evaluateAndAdvance` result.
    */
  def fromTriggers[F[_]: cats.Monad](triggers: List[FinalityTrigger[F]]): FinalityTriggerView[F] =
    new FinalityTriggerView[F] {
      def triggersFor(ord: SnapshotOrdinal): F[Set[FinalityTrigger.Kind]] =
        FinalityTrigger.triggersFor(triggers, ord)
    }
}

/** Transitional builder occupying the target `T_weight` trigger position.
  *
  * The trigger's qualifying ordinal is the highest ordinal where the sibling [[SnowballAccumulator]] has a sticky latest-attestation margin
  * decision matching our canonical hash. The sibling does not implement the intended K/alpha query cascade and its first-crossing decision
  * is arrival-order sensitive. This trigger therefore marks the integration point; it is not yet a sound Avalanche/Snowball Phase-2 rail.
  *
  * '''Cross-fork filter.''' Decisions on a divergent fork (decided hash differs from our canonical hash at that ordinal) are skipped via
  * the same `canonicalHashAt` predicate the legacy weight-sum path used. Only decisions matching our canonical chain contribute.
  *
  * '''Threshold parameter.''' Retained for signature stability; not used by the transitional margin path (decision is margin-based, not
  * threshold-based). This calculator is telemetry only while optimistic activation is dark. Canonical k1 depth is the sole live Phase-2
  * rail until the real sampled cascade lands.
  */
object TWeightTrigger {

  /** @param tipTracker
    *   attestation tracker whose sibling transitional margin accumulator is read here
    * @param threshold
    *   retained for signature stability; not used by the sticky-margin decision (margin-based, not threshold-based)
    */
  def make[F[_]: Sync](
    tipTracker: TipTracker[F],
    threshold: Ratio
  ): F[FinalityTrigger[F]] = {
    val _ = threshold // explicitly unused; preserved on signature for caller stability
    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TWeight, SnapshotOrdinal.MinValue) { state =>
      tipTracker
        .highestSnowballDecidedOrdinal(state.canonicalHashAt)
        .map {
          case Some(ord) => SnapshotOrdinal.unsafeApply(ord)
          case None      => SnapshotOrdinal.MinValue
        }
    }
  }
}

/** Production builder for the `T_depth1` canonical-depth Phase-2 fallback.
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

/** Legacy builder for the `T_depth2` local retention/tower watermark.
  *
  * Mirrors [[TDepth1Trigger]] structurally — qualifies ordinal `bestTipOrdinal - k₂`, clamped to `MinValue` while the chain is shorter than
  * `k₂`. The difference is only the constant: `k₂ ≫ k₁` so a strictly deeper / strictly later qualifying ordinal.
  *
  * This calculator remains wired by transitional code. Its output may schedule local retention/proof work, but it does not advance a
  * protocol phase, certify common prefix, authorize irreversible pruning, or constrain objective fork choice. k2 = 100*k1 is the
  * recommended local capacity from `NakamotoConfig.keepDepthBehindFinalized`.
  */
object TDepth2Trigger {

  /** @param k2
    *   recommended local retention/proof/recovery capacity k2 = 100*k1
    */
  def make[F[_]: Sync](k2: Long): F[FinalityTrigger[F]] =
    FinalityTrigger.fromRef[F](FinalityTrigger.Kind.TDepth2, SnapshotOrdinal.MinValue) { state =>
      val bestOrd = state.bestTipOrdinal.value.value
      val qualifying = bestOrd - k2
      val ord =
        if (qualifying > 0L) SnapshotOrdinal.unsafeApply(qualifying)
        else SnapshotOrdinal.MinValue
      Sync[F].pure(ord)
    }
}

/** Legacy `T_count` calculator retained for telemetry during FinalityGate migration.
  *
  * Counts the number of DISTINCT attesters whose `tipHash` matches our canonical chain at their `tipOrdinal`, walks attestation ordinals
  * from highest down, and qualifies the highest ordinal where the cumulative count reaches `ceil(threshold * validatorCount)`. Counts
  * peers, not weights — under equal stake this ties with [[TWeightTrigger]]; under future stake-weighted VRF this is strictly stronger
  * evidence (a single high-stake validator can hit the 2/3 *weight* threshold alone, but cannot fake a count of distinct attesters).
  *
  * '''Why hash-aware''': same as `T_weight`. Attesting to ordinal N on fork A must not qualify ordinal N on fork B.
  *
  * Live code retains this calculator for telemetry only. `T_count` must be removed/subsumed by decided-attestation `T_weight`, not treated
  * as a third P2 rail.
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
        // Legacy self-exclusion before the canonical-hash filter.
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
        // Legacy ancestor aggregation: attesting to ord N with a hash on our
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
