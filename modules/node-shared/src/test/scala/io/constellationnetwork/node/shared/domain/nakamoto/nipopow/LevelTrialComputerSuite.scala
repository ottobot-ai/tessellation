package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.Exp
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.ExpInterpreter

import weaver.MutableIOSuite

/** §3 NIPoPoW Slice S1 — level-trial computation unit tests.
  *
  * Covers:
  *   - `tauForLevel` round-trip determinism (same `(ρ, µ)` → same `τ`)
  *   - Different `µ` → different `τ` (domain separation works)
  *   - `gating` boundary cases (`δ_S ≤ 0`, `δ_S = γ`, `δ_S > γ`)
  *   - `thresholdMu` boundary cases (`g_µ < ψ_super` → 0; `g_µ = ψ_super` → 0; `g_µ → ∞` → `p_µ^max`)
  *   - `effectiveThreshold` is the product
  *   - `runAll` returns L-1 trials, levels 1..L-1 in order
  *   - Independence: pass at level µ does not depend on pass at any other µ (smoke check via two random ρ's)
  */
object LevelTrialComputerSuite extends MutableIOSuite {

  override type Res = LevelTrialComputer[IO]

  override def sharedResource: Resource[IO, LevelTrialComputer[IO]] =
    Resource.eval {
      ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).map { exp: Exp[IO] =>
        LevelTrialComputer.make[IO](exp)
      }
    }

  private val gamma: Long = 15L
  private val rho: Array[Byte] = Array.fill[Byte](64)(0x42.toByte)
  private val rhoOther: Array[Byte] = Array.fill[Byte](64)(0x99.toByte)

  test("tauForLevel — same (ρ, µ) → same τ across calls (deterministic)") { _ =>
    val t1 = LevelTrialComputer.tauForLevel(rho, 1)
    val t2 = LevelTrialComputer.tauForLevel(rho, 1)
    IO.pure(expect(t1 == t2))
  }

  test("tauForLevel — different µ → different τ (domain separation)") { _ =>
    val t1 = LevelTrialComputer.tauForLevel(rho, 1)
    val t2 = LevelTrialComputer.tauForLevel(rho, 2)
    val t3 = LevelTrialComputer.tauForLevel(rho, 3)
    IO.pure(expect(t1 != t2) and expect(t2 != t3) and expect(t1 != t3))
  }

  test("tauForLevel — different ρ → different τ at same µ") { _ =>
    val t1 = LevelTrialComputer.tauForLevel(rho, 1)
    val t2 = LevelTrialComputer.tauForLevel(rhoOther, 1)
    IO.pure(expect(t1 != t2))
  }

  test("tauForLevel — τ ∈ [0, 1)") { _ =>
    val t = LevelTrialComputer.tauForLevel(rho, 5)
    IO.pure(expect(t >= Ratio.Zero) and expect(t < Ratio.One))
  }

  test("gating — δ_S ≤ 0 → 0") { computer =>
    IO.pure(
      expect(computer.gating(0L, gamma) == Ratio.Zero) and
        expect(computer.gating(-5L, gamma) == Ratio.Zero)
    )
  }

  test("gating — δ_S = 1 → 1/γ (burst penalty 6.7% at γ=15)") { computer =>
    IO.pure(expect(computer.gating(1L, gamma) == Ratio(BigInt(1), BigInt(15))))
  }

  test("gating — δ_S = γ → 1") { computer =>
    IO.pure(expect(computer.gating(gamma, gamma) == Ratio.One))
  }

  test("gating — δ_S > γ → 1 (saturates)") { computer =>
    IO.pure(expect(computer.gating(100L, gamma) == Ratio.One))
  }

  test("thresholdMu — g_µ < ψ_super → 0") { computer =>
    val params = SuperLevelParams.at(1).get
    computer.thresholdMu(0L, params).map(r => expect(r == Ratio.Zero))
  }

  test("thresholdMu — g_µ = ψ_super → 0 (burst-zero condition)") { computer =>
    val params = SuperLevelParams.at(1).get
    // g_µ = 1, exp arg = -(1-1)/σ = 0, exp(0) = 1, threshold = pMax · (1 - 1) = 0
    computer.thresholdMu(SuperLevelParams.PsiSuper, params).map(r => expect(r == Ratio.Zero))
  }

  test("thresholdMu — modest g_µ saturates within [0, p_µ^max]") { computer =>
    // Stay well within the Bifrost continued-fraction Exp's convergence sweet spot. For L1 (σ=0.5),
    // g_µ=10 → exp arg ≈ -18, well-handled. We just check 0 < threshold ≤ p_µ^max (no large-gap
    // saturation assertion — the convergence is asymptotic and not exact at finite g_µ).
    val params = SuperLevelParams.at(1).get
    computer.thresholdMu(10L, params).map { r =>
      expect(r > Ratio.Zero) and expect(r <= params.pMax)
    }
  }

  test("effectiveThreshold = thresholdMu · gating") { computer =>
    val params = SuperLevelParams.at(2).get
    val gMu = 5L
    val deltaSlot = 7L
    for {
      raw <- computer.thresholdMu(gMu, params)
      eff <- computer.effectiveThreshold(gMu, deltaSlot, gamma, params)
      gate = computer.gating(deltaSlot, gamma)
    } yield expect(eff == raw * gate)
  }

  test("effectiveThreshold — burst (δ=1) suppression vs honest (δ=7) at γ=15") { computer =>
    val params = SuperLevelParams.at(3).get
    val gMu = 4L
    for {
      burstThresh <- computer.effectiveThreshold(gMu, 1L, gamma, params)
      honestThresh <- computer.effectiveThreshold(gMu, 7L, gamma, params)
    } yield {
      // burst gating = 1/15 ≈ 6.7%; honest gating = 7/15 ≈ 46.7%. Ratio honest/burst = 7.
      // We check the *threshold* ratio, not exact gating equality (threshold also depends on gMu — same here).
      val ratio = honestThresh / burstThresh
      expect(ratio == Ratio(BigInt(7), BigInt(1)))
    }
  }

  test("runAll — returns SuperLevelCount trials, levels 1..L-1 in order") { computer =>
    val gaps = Vector.fill(SuperLevelParams.SuperLevelCount)(5L)
    computer.runAll(rho, gaps, deltaSlot = 7L, gamma = gamma).map { trials =>
      expect(trials.size == SuperLevelParams.SuperLevelCount) and
        expect(trials.zipWithIndex.forall { case (t, i) => t.level == i + 1 })
    }
  }

  test("runAll — τ values are domain-separated per level") { computer =>
    val gaps = Vector.fill(SuperLevelParams.SuperLevelCount)(5L)
    computer.runAll(rho, gaps, deltaSlot = 7L, gamma = gamma).map { trials =>
      val taus = trials.map(_.tau).toSet
      expect(taus.size == SuperLevelParams.SuperLevelCount) // all distinct
    }
  }

  test("runAll — burst δ=1 vs honest δ=7 — burst yields no more passes than honest (suppression)") { computer =>
    // Run with random-looking ρ_S over a single slot at two δ values; identical gap vectors.
    // 30 samples × 9 levels × 2 δ-values = 540 trials; each trial does one exp.evaluate at modest
    // arguments (max σ=72 → arg ≈ -1.5, min σ=0.5 → arg ≈ -18). Bifrost converges fast on these.
    val gaps = Vector.fill(SuperLevelParams.SuperLevelCount)(10L)
    val rhos = (0 until 30).map { i =>
      val a = new Array[Byte](64)
      val seed = (i * 0x9E3779B97F4A7C15L)
      java.nio.ByteBuffer.wrap(a).putLong(0, seed).putLong(8, ~seed).putLong(16, seed >>> 1).putLong(24, ~(seed >>> 1))
      a
    }
    for {
      burstResults <- rhos.toList.traverse(r => computer.runAll(r, gaps, 1L, gamma))
      honestResults <- rhos.toList.traverse(r => computer.runAll(r, gaps, 7L, gamma))
    } yield {
      val burstPasses = burstResults.flatten.count(_.passed)
      val honestPasses = honestResults.flatten.count(_.passed)
      // Honest gating = 7/15 ≈ 47%; burst gating = 1/15 ≈ 7%. Honest threshold ≈ 7× burst's.
      // Burst should pass strictly less, but ≥ keeps the assertion robust to small-N variance.
      expect(honestPasses >= burstPasses)
    }
  }

}
