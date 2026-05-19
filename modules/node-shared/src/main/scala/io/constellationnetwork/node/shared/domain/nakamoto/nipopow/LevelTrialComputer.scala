package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.Exp
import io.constellationnetwork.numerics.implicits._

import org.bouncycastle.crypto.digests.Blake2bDigest

/** Producer + verifier side computation of §3 NIPoPoW super-level trials (L1..L9).
  *
  * Per `docs/nakamoto/NIPOPOW-PROPOSAL.md` §2.1 eq. 1+5 + §2.2:
  *
  * For each snapshot S with VRF output `ρ_S`, base-block gap `g_µ` since the previous level-µ hit, and L0 slot gap
  * `δ_S` against the parent snapshot:
  *
  *   - `τ_µ(S)   = Blake2b512(ρ_S ‖ "TEST-" ++ µ) / 2^512`               (domain-separated rehash → Ratio ∈ [0, 1))
  *   - `θ_µ(g)   = p_µ^max · (1 − exp(−(g − ψ_super) / σ_µ))   if g ≥ ψ_super, else 0`   (shifted-exp over `g_µ`)
  *   - `θ_µ^eff  = θ_µ(g_µ) · min(1, δ_S/γ)`                              (L0 slot-gap gating, γ = `lddCutoff`)
  *   - Pass condition: `τ_µ(S) < θ_µ^eff(g_µ, δ_S)`
  *
  * The L0 trial is NOT computed here — every existing snapshot is a level-0 hit by chain construction (the L0
  * production gate against the standard `f(δ)` LDD snowplow is the existing [[EligibilityChecker.checkEligibility]]
  * path and stays unchanged).
  *
  * '''Independence (NOT nested rarity).''' Each level has its own domain-separated `τ_µ`, so passing `µ` gives zero
  * probabilistic boost to passing `µ-1`. Per `NIPOPOW-PROPOSAL.md:162-166`, this is a deliberate construction choice
  * that strengthens security by forcing the adversary to satisfy L independent constraints simultaneously; the PoW
  * `τ < 2^(-µ)` analog has the right densities but no security gain.
  *
  * '''Determinism.''' All arithmetic is exact `Ratio` (no `Double`, no `math.exp`/`math.pow`). The Bifrost
  * `Exp` interpreter is the same one [[EligibilityChecker]] uses for its `(1 - f)^stake` evaluation — byte-identical
  * across JVMs/CPUs.
  */
class LevelTrialComputer[F[_]: Monad](exp: Exp[F]) {

  import LevelTrialComputer._

  /** Compute the gating multiplier `min(1, δ_S / γ)`. Returns 0 for `δ_S ≤ 0` defensively (gap of 0 doesn't happen
    * for adjacent snapshots — slots are strictly monotonic — but a 0 input shouldn't pass the gate even by accident).
    */
  def gating(deltaSlot: Long, gamma: Long): Ratio =
    if (deltaSlot <= 0L) Ratio.Zero
    else if (deltaSlot >= gamma) Ratio.One
    else Ratio(BigInt(deltaSlot), BigInt(gamma))

  /** Compute `θ_µ(g_µ)` = `p_µ^max · (1 - exp(-(g_µ - ψ_super)/σ_µ))` for `g_µ ≥ ψ_super`, else 0. */
  def thresholdMu(gMu: Long, params: SuperLevelParam): F[Ratio] =
    if (gMu < SuperLevelParams.PsiSuper) Monad[F].pure(Ratio.Zero)
    else {
      // -(g_µ - ψ_super) as Ratio; positive numerator scaled by -1
      val numerator: Ratio = Ratio(BigInt(-(gMu - SuperLevelParams.PsiSuper)), BigInt(1))
      val expArg: Ratio = numerator / params.sigma
      exp.evaluate(expArg).map(e => params.pMax * (Ratio.One - e))
    }

  /** Compute `θ_µ^eff(g_µ, δ_S) = θ_µ(g_µ) · min(1, δ_S/γ)`. */
  def effectiveThreshold(gMu: Long, deltaSlot: Long, gamma: Long, params: SuperLevelParam): F[Ratio] =
    thresholdMu(gMu, params).map(_ * gating(deltaSlot, gamma))

  /** Run a single super-level trial for level µ.
    *
    *   - `vrfOutput` — 64-byte `ρ_S` from [[EligibilityChecker.vrfProofForSlot]] / `vrfProofToHash`
    *   - `level` — the super-level µ (1..L-1); requires [[SuperLevelParams.at]] to be defined
    *   - `gMu` — base-block gap (snapshots since previous level-µ hit, NOT slots)
    *   - `deltaSlot` — L0 slot gap vs parent snapshot
    *   - `gamma` — `LddConfig.lddCutoff` (default 15)
    */
  def runTrial(
    vrfOutput: Array[Byte],
    level: Int,
    gMu: Long,
    deltaSlot: Long,
    gamma: Long
  ): F[LevelTrial] =
    SuperLevelParams.at(level) match {
      case None =>
        // Out-of-range µ — defensive: tau=0, threshold=0, passed=false. Caller should validate.
        Monad[F].pure(LevelTrial(level, Ratio.Zero, Ratio.Zero, passed = false))
      case Some(params) =>
        val tau = tauForLevel(vrfOutput, level)
        effectiveThreshold(gMu, deltaSlot, gamma, params).map(thresh => LevelTrial(level, tau, thresh, tau < thresh))
    }

  /** Run all L-1 super-level trials in one pass. Returns a `Vector` indexed 0..L-2 (i.e. `result(i)` has `level = i+1`).
    *
    *   - `gapsPerSuperLevel` — vector of length `SuperLevelCount` (= L-1). `gapsPerSuperLevel(i)` is `g_(i+1)`.
    *     Caller is responsible for tracking these per-level base-block gap counters across snapshots.
    */
  def runAll(
    vrfOutput: Array[Byte],
    gapsPerSuperLevel: Vector[Long],
    deltaSlot: Long,
    gamma: Long
  ): F[Vector[LevelTrial]] = {
    require(
      gapsPerSuperLevel.size == SuperLevelParams.SuperLevelCount,
      s"gapsPerSuperLevel must have ${SuperLevelParams.SuperLevelCount} entries, got ${gapsPerSuperLevel.size}"
    )
    SuperLevelParams.Levels.zipWithIndex.traverse {
      case (params, idx) => runTrial(vrfOutput, params.level, gapsPerSuperLevel(idx), deltaSlot, gamma)
    }
  }
}

object LevelTrialComputer {

  /** Domain-separation string for the level-µ trial. Per `NIPOPOW-PROPOSAL.md` §2.1 eq. 1 — `"TEST-" ++ µ` for µ ≥ 1. */
  def domainSeparator(level: Int): Array[Byte] = s"TEST-$level".getBytes("US-ASCII")

  /** 2^512 — denominator for normalizing a 64-byte hash output into `Ratio ∈ [0, 1)`. Matches
    * [[EligibilityChecker.vrfOutputAsRatio]] — same distribution by construction.
    */
  private val NormalizationConstant: BigInt = BigInt(2).pow(512)

  /** Compute `τ_µ(S) = Blake2b512(ρ_S ‖ "TEST-" ++ µ) / 2^512`. */
  def tauForLevel(vrfOutput: Array[Byte], level: Int): Ratio = {
    val digest = new Blake2bDigest(512)
    digest.update(vrfOutput, 0, vrfOutput.length)
    val sep = domainSeparator(level)
    digest.update(sep, 0, sep.length)
    val out = new Array[Byte](64)
    digest.doFinal(out, 0)
    Ratio(BigInt(1, out), NormalizationConstant)
  }

  def make[F[_]: Monad](exp: Exp[F]): LevelTrialComputer[F] = new LevelTrialComputer[F](exp)
}
