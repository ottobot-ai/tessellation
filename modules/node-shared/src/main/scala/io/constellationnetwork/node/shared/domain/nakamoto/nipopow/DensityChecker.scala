package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

/** §3 NIPoPoW S4.4 — pure density-violation detector. Wraps the standalone relative-error computation that both the [[TowerVerifier]] and
  * the (S5) `dag_nakamoto_tower_density_relative_error{level}` Prometheus gauge consume.
  *
  * '''Definition''' (per proposal §5.3): for a super-level µ with `observedDensity = chainLength / totalLevel0Length` and
  * `targetDensity = f_0 · 2^(-µ)`, the relative error is
  *
  * `relativeError = |observedDensity - targetDensity| / targetDensity`
  *
  * '''Acceptance bound''': proposal §5.4 / S6 acceptance criterion is 5% (verified at 10M slots in `paper/main.tex` Table 1, achieved 1–13%
  * per level — 5% is the tight default for v1 validators). Validators reject proofs with `relativeError > 5%`; observability gauge reports
  * the raw ratio.
  *
  * Pure `Ratio` arithmetic — no Double, no rounding. Consistent with the Bifrost-derived consensus determinism contract.
  */
object DensityChecker {

  /** Compute `|observed - target| / target`, exact `Ratio`.
    *
    * Edge cases:
    *   - `target == 0` → returns `Ratio.One` (defensively maximal — any deviation against a zero target is a 100% miss; never happens for
    *     valid super-level params where `targetDensity = 1/2^µ > 0`).
    *   - `observed == target` → returns `Ratio.Zero`.
    *
    * '''Sign normalization.''' We pick the larger-then-smaller branch explicitly because `Ratio` doesn't normalize a negative-denominator
    * representation through the `apply` constructor's gcd reduction — a subtraction that yields `(2/-8)` keeps that sign, and downstream
    * `/` perpetuates it. Picking branches by ordering avoids that path entirely.
    */
  def relativeError(observed: Ratio, target: Ratio): Ratio =
    if (target == Ratio.Zero) Ratio.One
    else {
      val diff = if (observed >= target) observed - target else target - observed
      diff / target
    }

  /** Convenience overload — accepts raw `observed` and `totalLevel0` counts and a target `Ratio`. Computes `observed / totalLevel0` for the
    * observed density before delegating to [[relativeError]].
    */
  def relativeError(observed: Long, totalLevel0: Long, target: Ratio): Ratio =
    if (totalLevel0 <= 0L) Ratio.One
    else relativeError(Ratio(BigInt(observed), BigInt(totalLevel0)), target)
}
