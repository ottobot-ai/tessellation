package io.constellationnetwork.schema.nakamoto

import io.constellationnetwork.numerics.Ratio

/** Test-only LDD snowplow config fixture.
  *
  * Pinned to the PRODUCTION `application.conf` `nakamoto.ldd` values (γ=16, ψ=1, fA=1/2, fB=1/20) so suites exercise the SAME eligibility
  * curve the chain runs. This is TEST DATA, not a production fallback — production reads the HOCON config, and there is intentionally NO
  * `LddConfig.Default` in the main sources (a source-level default silently drifts from config; that drift was a real bug).
  *
  * Visible to every test scope: `shared/src/test` directly, and `node-shared` / `dag-l0` / … via their `shared % "…;test->test"` dep.
  *
  * If the production LDD parameters change, update this in lock-step AND regenerate the `EligibilityDeterminismGoldenSuite` vectors.
  */
object LddConfigFixture {

  /** The canonical production-aligned LDD config (γ=16). Use this wherever a suite just needs "the real curve." */
  val production: LddConfig = LddConfig(
    lddCutoff = 16,
    offset = 1,
    baselineDifficulty = Ratio(1, 20),
    amplitude = Ratio(1, 2)
  )
}
