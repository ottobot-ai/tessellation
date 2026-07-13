package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.SecureRandom

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, LddConfigFixture}

import weaver.MutableIOSuite

object EligibilityCheckerSuite extends MutableIOSuite {

  override type Res = (EligibilityChecker[IO], CanonicalOperatorConsensusPopulation)

  override def sharedResource: Resource[IO, Res] =
    for {
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      operators <- CanonicalOperatorConsensusFixture.makePopulation(2)
    } yield (EligibilityChecker.make[IO](log1p, exp), operators)

  // Production-aligned LDD curve (γ=16). Suites that pin γ-specific values below track this.
  private val defaultConfig = LddConfigFixture.production
  private val alwaysEligibleConfig =
    LddConfig(lddCutoff = 1, offset = 0, baselineDifficulty = Ratio.One, amplitude = Ratio.One)
  private val random = new SecureRandom()

  private def randomEta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  // Tolerance for asserting Ratio threshold matches an analytical Double reference.
  private val Tol: Ratio = Ratio(1, BigInt(10).pow(6))

  // ============ Threshold Function Tests ============

  test("threshold is 0 when slotGap < offset") {
    case (checker, _) =>
      val configWithOffset = LddConfig(lddCutoff = 15, offset = 5, baselineDifficulty = Ratio(1, 20), amplitude = Ratio(1, 2))
      for {
        thresh0 <- checker.threshold(Ratio.One, 0, configWithOffset)
        thresh4 <- checker.threshold(Ratio.One, 4, configWithOffset)
      } yield expect(thresh0 == Ratio.Zero).and(expect(thresh4 == Ratio.Zero))
  }

  test("threshold ramps linearly in ramp region") {
    case (checker, _) =>
      // offset=0, cutoff=10, amplitude=1/2, baseline=1/20.
      // At slotGap=5: difficulty = 1/2 × 5/10 = 1/4. threshold@stake=1.0 = 1 - (3/4)^1 = 1/4.
      // At slotGap=9: difficulty = 1/2 × 9/10 = 9/20. threshold@stake=1.0 = 1 - (11/20)^1 = 9/20.
      val config = LddConfig(lddCutoff = 10, offset = 0, baselineDifficulty = Ratio(1, 20), amplitude = Ratio(1, 2))
      for {
        threshMid <- checker.threshold(Ratio.One, 5, config)
        threshAlmostCutoff <- checker.threshold(Ratio.One, 9, config)
      } yield
        expect((threshMid - Ratio(1, 4)).abs < Tol)
          .and(expect((threshAlmostCutoff - Ratio(9, 20)).abs < Tol))
  }

  test("threshold equals baselineDifficulty in recovery region (δ ≥ γ) at stake=1") {
    case (checker, _) =>
      for {
        // δ=20 and δ=100 are both ≥ γ=16, so both sit in the baseline recovery region.
        thresh20 <- checker.threshold(Ratio.One, 20, defaultConfig)
        thresh100 <- checker.threshold(Ratio.One, 100, defaultConfig)
      } yield
        // At stake=1 the formula 1 - (1-f)^1 = f, so threshold = baselineDifficulty exactly (within Lentz precision).
        expect((thresh20 - defaultConfig.baselineDifficulty).abs < Tol)
          .and(expect((thresh100 - defaultConfig.baselineDifficulty).abs < Tol))
  }

  test("threshold with relativeStake=0 is always 0") {
    case (checker, _) =>
      for {
        t0 <- checker.threshold(Ratio.Zero, 0, defaultConfig)
        t10 <- checker.threshold(Ratio.Zero, 10, defaultConfig)
        t100 <- checker.threshold(Ratio.Zero, 100, defaultConfig)
      } yield
        // exp(0 × x) = 1, so threshold = 1 - 1 = 0 exactly.
        expect(t0 == Ratio.Zero).and(expect(t10 == Ratio.Zero)).and(expect(t100 == Ratio.Zero))
  }

  test("threshold scales with relativeStake via (1 - (1-f)^stake)") {
    case (checker, _) =>
      // f = 1/2, stake = 1/2. threshold = 1 - (1/2)^(1/2) ≈ 0.2929.
      val f = Ratio(1, 2)
      val config = LddConfig(lddCutoff = 10, offset = 0, baselineDifficulty = f, amplitude = f)
      for {
        threshHalfStake <- checker.threshold(Ratio(1, 2), 10, config)
        expectedDouble = 1.0 - math.pow(0.5, 0.5)
        expectedRatio = Ratio(expectedDouble, 12)
      } yield expect((threshHalfStake - expectedRatio).abs < Tol)
  }

  // ============ N-Independence Tests ============

  test("N-independent: P(≥1 winner) ≈ f(δ) for N equal validators") {
    case (checker, _) =>
      // Pick a difficulty config so the recovery region holds at slotGap=10. f = 0.3.
      val fRatio = Ratio(3, 10)
      val config = LddConfig(lddCutoff = 1, offset = 0, baselineDifficulty = fRatio, amplitude = fRatio)
      val slotGap = 10L
      for {
        probAtLeastOne <- List(3, 10, 100).traverse { n =>
          checker.threshold(Ratio(1, n), slotGap, config).map { tPerValidator =>
            // Cast to BigDecimal for the (1-t)^n exponentiation across N — Double arithmetic is fine
            // for this *expectation check*, since the test asserts a probability identity, not
            // consensus behavior. The threshold itself is exact-Ratio.
            val t = tPerValidator.toDouble
            1.0 - math.pow(1.0 - t, n.toDouble)
          }
        }
      } yield expect(probAtLeastOne.forall(p => math.abs(p - 0.3) < 0.001))
  }

  // ============ VRF Integration Tests ============

  test("registered VRF proof roundtrip produces a canonical output ratio") {
    case (checker, operators) =>
      val operator = operators.operators.head
      val eta = randomEta()
      val slot = Slot.unsafeApply(100L)
      for {
        result <- checker.checkEligibility(operator.localVrfSecret, slot, 1L, eta, Ratio.One, alwaysEligibleConfig)
        proofAndOutput <- IO.fromOption(result)(new IllegalStateException("registered threshold-one proof was not eligible"))
        (proof, output) = proofAndOutput
        verified <- checker.verifyEligibility(
          operator.resolvedPair.vrfPublicKey.toBytes,
          slot,
          1L,
          eta,
          Ratio.One,
          alwaysEligibleConfig,
          proof
        )
        ratio = EligibilityChecker.vrfOutputAsRatio(output)
      } yield
        expect(verified)
          .and(expect(output.length == 64))
          .and(expect(ratio >= Ratio.Zero))
          .and(expect(ratio < Ratio.One))
  }

  test("registered VRF draws produce output ratios in [0, 1)") {
    case (checker, operators) =>
      val operator = operators.operators.head
      for {
        results <- (1 to 50).toList.traverse { draw =>
          val eta = randomEta()
          val slot = Slot.unsafeApply(draw.toLong)
          checker
            .checkEligibility(operator.localVrfSecret, slot, 1L, eta, Ratio.One, alwaysEligibleConfig)
            .flatMap(IO.fromOption(_)(new IllegalStateException("registered threshold-one proof was not eligible")))
            .map { case (_, output) => EligibilityChecker.vrfOutputAsRatio(output) }
        }
      } yield expect(results.forall(_ >= Ratio.Zero)).and(expect(results.forall(_ < Ratio.One)))
  }

  test("checkEligibility + verifyEligibility roundtrip uses one preregistered pair") {
    case (checker, operators) =>
      val operator = operators.operators.head
      val eta = randomEta()
      val slotGap = 20L
      val relativeStake = Ratio.One
      for {
        foundAttempt <- {
          // Loop until we find one eligible slot, up to N attempts. Stake=1 means threshold = baseline = 1/20,
          // so ~1-in-20 attempts wins on average.
          def loop(attempt: Int): IO[Boolean] =
            if (attempt >= 1000) IO.pure(false)
            else {
              val slot = Slot.unsafeApply(attempt.toLong)
              checker.checkEligibility(operator.localVrfSecret, slot, slotGap, eta, relativeStake, defaultConfig).flatMap {
                case Some((proof, _)) =>
                  checker
                    .verifyEligibility(
                      operator.resolvedPair.vrfPublicKey.toBytes,
                      slot,
                      slotGap,
                      eta,
                      relativeStake,
                      defaultConfig,
                      proof
                    )
                    .flatMap {
                      case true  => IO.pure(true)
                      case false => loop(attempt + 1)
                    }
                case None => loop(attempt + 1)
              }
            }
          loop(0)
        }
      } yield expect(foundAttempt, "Should find at least one eligible slot in 1000 attempts")
  }

  test("verifyEligibility fails with a different preregistered operator's VRF key") {
    case (checker, operators) =>
      val producer = operators.operators.head
      val otherRegisteredOperator = operators.operators.tail.head
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L
      val proof = checker.vrfProofForSlot(producer.localVrfSecret, slot, eta)
      checker
        .verifyEligibility(
          otherRegisteredOperator.resolvedPair.vrfPublicKey.toBytes,
          slot,
          slotGap,
          eta,
          Ratio.One,
          defaultConfig,
          proof
        )
        .map(verified => expect(!verified))
  }

  test("verifyEligibility fails with tampered proof") {
    case (checker, operators) =>
      val operator = operators.operators.head
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L
      val proof = checker.vrfProofForSlot(operator.localVrfSecret, slot, eta)
      val tampered = {
        val t = proof.clone()
        t(0) = (t(0) ^ 0xff).toByte
        t
      }
      checker
        .verifyEligibility(
          operator.resolvedPair.vrfPublicKey.toBytes,
          slot,
          slotGap,
          eta,
          Ratio.One,
          defaultConfig,
          tampered
        )
        .map(verified => expect(!verified))
  }

  test("verifyEligibility fails with wrong slot") {
    case (checker, operators) =>
      val operator = operators.operators.head
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val wrongSlot = Slot.unsafeApply(11L)
      val slotGap = 20L
      val proof = checker.vrfProofForSlot(operator.localVrfSecret, slot, eta)
      checker
        .verifyEligibility(
          operator.resolvedPair.vrfPublicKey.toBytes,
          wrongSlot,
          slotGap,
          eta,
          Ratio.One,
          defaultConfig,
          proof
        )
        .map(verified => expect(!verified))
  }

  test("verifyEligibility fails with wrong eta") {
    case (checker, operators) =>
      val operator = operators.operators.head
      val eta = randomEta()
      val wrongEta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L
      val proof = checker.vrfProofForSlot(operator.localVrfSecret, slot, eta)
      checker
        .verifyEligibility(
          operator.resolvedPair.vrfPublicKey.toBytes,
          slot,
          slotGap,
          wrongEta,
          Ratio.One,
          defaultConfig,
          proof
        )
        .map(verified => expect(!verified))
  }

  // ============ Epoch Eta Tests ============

  test("computeNextEta is deterministic") { _ =>
    IO {
      val prevEta = randomEta()
      val epoch = 42L
      val rhoHashes = List(randomEta(), randomEta(), randomEta())
      val eta1 = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)
      val eta2 = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)
      expect(java.util.Arrays.equals(eta1, eta2))
    }
  }

  test("computeNextEta produces 32-byte output") { _ =>
    IO {
      val prevEta = randomEta()
      val nextEta = EligibilityChecker.computeNextEta(prevEta, 1L, List(randomEta()))
      expect(nextEta.length == 32)
    }
  }

  test("computeNextEta differs with different inputs") { _ =>
    IO {
      val prevEta = randomEta()
      val rhoHashes = List(randomEta())
      val eta1 = EligibilityChecker.computeNextEta(prevEta, 1L, rhoHashes)
      val eta2 = EligibilityChecker.computeNextEta(prevEta, 2L, rhoHashes)
      val eta3 = EligibilityChecker.computeNextEta(randomEta(), 1L, rhoHashes)
      val eta4 = EligibilityChecker.computeNextEta(prevEta, 1L, List(randomEta()))
      expect(!java.util.Arrays.equals(eta1, eta2))
        .and(expect(!java.util.Arrays.equals(eta1, eta3)))
        .and(expect(!java.util.Arrays.equals(eta1, eta4)))
    }
  }

  test("computeNextEta handles empty rhoNonceHashes") { _ =>
    IO {
      val eta = EligibilityChecker.computeNextEta(randomEta(), 1L, List.empty)
      expect(eta.length == 32)
    }
  }

  // ============ Edge Cases ============

  test("vrfProofForSlot requires 32-byte eta") {
    case (checker, operators) =>
      IO {
        val result = scala.util.Try(
          checker.vrfProofForSlot(operators.operators.head.localVrfSecret, Slot.unsafeApply(0L), new Array[Byte](16))
        )
        expect(result.isFailure)
      }
  }

  test("threshold handles edge case difficulty = 1") {
    case (checker, _) =>
      val config = LddConfig(lddCutoff = 1, offset = 0, baselineDifficulty = Ratio.One, amplitude = Ratio.One)
      for {
        thresh <- checker.threshold(Ratio.One, 10, config)
      } yield expect(thresh == Ratio.One)
  }

  test("threshold handles slotGap = 0 with offset = 0") {
    case (checker, _) =>
      val config = LddConfig(lddCutoff = 15, offset = 0, baselineDifficulty = Ratio(1, 20), amplitude = Ratio(1, 2))
      for {
        thresh <- checker.threshold(Ratio.One, 0, config)
      } yield expect(thresh == Ratio.Zero) // difficulty = 0 → short-circuit
  }

  test("default config has ψ=1 buffer: zero probability in slot immediately after snapshot") {
    case (checker, _) =>
      for {
        threshDelta0 <- checker.threshold(Ratio.One, 0, defaultConfig)
        threshDelta1 <- checker.threshold(Ratio.One, 1, defaultConfig)
        threshDelta2 <- checker.threshold(Ratio.One, 2, defaultConfig)
        // δ=2 at γ=16: difficulty = 1/2 × (2-1)/(16-1) = 1/30. At stake=1: threshold = 1 - (29/30)^1 = 1/30.
        expectedRampStart = Ratio(1, 30)
      } yield
        expect(threshDelta0 == Ratio.Zero)
          .and(expect(threshDelta1 == Ratio.Zero))
          .and(expect((threshDelta2 - expectedRampStart).abs < Tol))
  }

  // ============ Stake-weighted Property: threshold is monotone non-decreasing in relativeStake ============
  //
  // §1.1 sanity check on top of the existing equal-weight tests: with a fixed slotGap + LddConfig,
  // threshold(s, δ, cfg) must be monotone non-decreasing in `s` because the closed form is
  //   threshold(s, δ) = 1 - (1 - f(δ))^s
  // and 1 - f(δ) ∈ [0, 1] so x ↦ x^s is non-increasing in s for x ∈ [0, 1].
  test("stakeWeightedProperty: threshold monotone non-decreasing in relativeStake at fixed slotGap+config") {
    case (checker, _) =>
      val slotGap = 10L
      // Use the default config (f recovery region holds at slotGap=10 ≥ lddCutoff=15? — checked below).
      // Pick a config whose recovery region is at slotGap=10 and whose f is non-degenerate.
      val cfg = LddConfig(lddCutoff = 5, offset = 0, baselineDifficulty = Ratio(3, 10), amplitude = Ratio(3, 10))
      val stakes = List(Ratio(1, 8), Ratio(1, 4), Ratio(1, 2), Ratio(3, 4))
      for {
        thresholds <- stakes.traverse(checker.threshold(_, slotGap, cfg))
      } yield {
        val pairs = thresholds.zip(thresholds.drop(1))
        expect(pairs.forall { case (lo, hi) => lo <= hi })
      }
  }

  // Helper: traverse for List in IO context (cats stdlib alternative).
  private implicit class TraverseListIO[A](xs: List[A]) {
    def traverse[B](f: A => IO[B]): IO[List[B]] =
      xs.foldLeft(IO.pure(List.empty[B])) { (accIO, a) =>
        for {
          acc <- accIO
          b <- f(a)
        } yield acc :+ b
      }
  }
}
