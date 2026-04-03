package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.SecureRandom

import cats.effect.IO

import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.vrf.EcVrf25519

import weaver.SimpleIOSuite

object EligibilityCheckerSuite extends SimpleIOSuite {

  private val vrf = new EcVrf25519()
  private val defaultConfig = LddConfig.Default
  private val random = new SecureRandom()

  private def randomSk(): Array[Byte] = {
    val sk = new Array[Byte](32)
    random.nextBytes(sk)
    sk
  }

  private def randomEta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  // ============ Threshold Function Tests ============

  test("threshold is 0 when slotGap < offset") {
    IO {
      val configWithOffset = LddConfig(
        lddCutoff = 15,
        offset = 5, // Non-zero offset
        baselineDifficulty = 0.05,
        amplitude = 0.5
      )
      val thresh0 = EligibilityChecker.threshold(1.0, 0, configWithOffset)
      val thresh4 = EligibilityChecker.threshold(1.0, 4, configWithOffset)
      expect.eql(0.0, thresh0) and expect.eql(0.0, thresh4)
    }
  }

  test("threshold ramps linearly in ramp region") {
    IO {
      val config = LddConfig(
        lddCutoff = 10,
        offset = 0,
        baselineDifficulty = 0.05,
        amplitude = 0.5
      )
      // At slotGap = 5 (halfway), difficulty should be 0.5 * 5/10 = 0.25
      // For relativeStake = 1.0, threshold = 1 - (1 - 0.25)^1 = 0.25
      val threshMid = EligibilityChecker.threshold(1.0, 5, config)
      expect(math.abs(threshMid - 0.25) < 0.0001) and {
        // At slotGap == lddCutoff, we're in recovery region (baseline)
        // The ramp is for slotGap < lddCutoff
        // At slotGap = 9, should be 0.5 * 9/10 = 0.45
        val threshAlmostCutoff = EligibilityChecker.threshold(1.0, 9, config)
        expect(math.abs(threshAlmostCutoff - 0.45) < 0.0001)
      }
    }
  }

  test("threshold equals baselineDifficulty in recovery region (δ ≥ γ)") {
    IO {
      val thresh15 = EligibilityChecker.threshold(1.0, 15, defaultConfig)
      val thresh100 = EligibilityChecker.threshold(1.0, 100, defaultConfig)
      // For relativeStake = 1.0, threshold ≈ difficulty (floating point)
      expect(math.abs(thresh15 - defaultConfig.baselineDifficulty) < 1e-10) and
        expect(math.abs(thresh100 - defaultConfig.baselineDifficulty) < 1e-10)
    }
  }

  test("threshold with relativeStake=1.0 approximately equals f(δ)") {
    IO {
      val config = LddConfig(lddCutoff = 15, offset = 0, baselineDifficulty = 0.05, amplitude = 0.5)
      // In recovery: f(δ) = 0.05
      val threshRecovery = EligibilityChecker.threshold(1.0, 20, config)
      expect(math.abs(threshRecovery - 0.05) < 1e-10) and {
        // In ramp at δ=7: f(δ) = 0.5 * 7/15 = 0.2333...
        val fAtDelta7 = 0.5 * 7.0 / 15.0
        val threshRamp = EligibilityChecker.threshold(1.0, 7, config)
        expect(math.abs(threshRamp - fAtDelta7) < 0.0001)
      }
    }
  }

  test("threshold with relativeStake=0.0 is always 0") {
    IO {
      val thresh0Gap = EligibilityChecker.threshold(0.0, 0, defaultConfig)
      val thresh10Gap = EligibilityChecker.threshold(0.0, 10, defaultConfig)
      val thresh100Gap = EligibilityChecker.threshold(0.0, 100, defaultConfig)
      expect.eql(0.0, thresh0Gap) and
        expect.eql(0.0, thresh10Gap) and
        expect.eql(0.0, thresh100Gap)
    }
  }

  test("threshold scales with relativeStake via (1 - (1-f)^stake)") {
    IO {
      val f = 0.5 // amplitude at cutoff
      val config = LddConfig(lddCutoff = 10, offset = 0, baselineDifficulty = f, amplitude = f)

      // At slotGap = cutoff, difficulty = baselineDifficulty = 0.5
      val threshHalfStake = EligibilityChecker.threshold(0.5, 10, config)
      // threshold = 1 - (1 - 0.5)^0.5 = 1 - 0.5^0.5 = 1 - 0.7071 ≈ 0.2929
      val expected = 1.0 - math.pow(0.5, 0.5)
      expect(math.abs(threshHalfStake - expected) < 0.0001)
    }
  }

  // ============ N-Independence Tests ============

  test("N-independent: P(≥1 winner) ≈ f(δ) for N equal validators") {
    IO {
      // Taktikos property: regardless of N validators, probability of at least one winner
      // should be approximately f(δ) when validators have equal stake summing to 1
      val f = 0.3 // difficulty
      val config = LddConfig(lddCutoff = 1, offset = 0, baselineDifficulty = f, amplitude = f)
      val slotGap = 10L // In recovery region

      def probNoWinner(n: Int): Double = {
        val stakePerValidator = 1.0 / n
        val threshPerValidator = EligibilityChecker.threshold(stakePerValidator, slotGap, config)
        // P(validator doesn't win) = 1 - threshold
        // P(no winner among N) = (1 - thresh)^N
        math.pow(1.0 - threshPerValidator, n.toDouble)
      }

      val probAtLeastOne3 = 1.0 - probNoWinner(3)
      val probAtLeastOne10 = 1.0 - probNoWinner(10)
      val probAtLeastOne100 = 1.0 - probNoWinner(100)

      // All should be approximately f(δ) = 0.3
      expect(math.abs(probAtLeastOne3 - f) < 0.001) and
        expect(math.abs(probAtLeastOne10 - f) < 0.001) and
        expect(math.abs(probAtLeastOne100 - f) < 0.001)
    }
  }

  // ============ VRF Integration Tests ============

  test("VRF proof roundtrip: prove → hash → normalize") {
    IO {
      val sk = randomSk()
      val eta = randomEta()
      val slot = Slot.unsafeApply(100L)

      val proof = EligibilityChecker.vrfProofForSlot(sk, slot, eta)
      val outputOpt = vrf.vrfProofToHash(proof)

      expect(outputOpt.isDefined) and {
        val output = outputOpt.get
        expect.eql(64, output.length) and {
          val normalized = EligibilityChecker.normalizeVrfOutput(output)
          expect(normalized >= 0.0) and expect(normalized < 1.0)
        }
      }
    }
  }

  test("normalizeVrfOutput produces values in [0, 1)") {
    IO {
      // Test with various VRF outputs
      val results = (1 to 100).map { _ =>
        val sk = randomSk()
        val eta = randomEta()
        val slot = Slot.unsafeApply(random.nextLong().abs)
        val proof = EligibilityChecker.vrfProofForSlot(sk, slot, eta)
        val output = vrf.vrfProofToHash(proof).get
        EligibilityChecker.normalizeVrfOutput(output)
      }

      expect(results.forall(_ >= 0.0)) and
        expect(results.forall(_ < 1.0))
    }
  }

  test("checkEligibility + verifyEligibility roundtrip") {
    IO {
      val eta = randomEta()
      val slotGap = 20L // In recovery region
      val relativeStake = 1.0 // Full stake for higher chance of eligibility

      // Run multiple times to get an eligible result
      var found = false
      var attempt = 0

      while (!found && attempt < 1000) {
        val testSk = randomSk()
        val testVk = vrf.getVerificationKey(testSk)
        val testSlot = Slot.unsafeApply(attempt.toLong)

        EligibilityChecker.checkEligibility(testSk, testSlot, slotGap, eta, relativeStake, defaultConfig) match {
          case Some((p, _)) =>
            // Verify the proof
            val verified = EligibilityChecker.verifyEligibility(testVk, testSlot, slotGap, eta, relativeStake, defaultConfig, p)
            if (verified) {
              found = true
            }
          case None =>
        }
        attempt += 1
      }

      expect(found, s"Should find at least one eligible slot in $attempt attempts")
    }
  }

  test("verifyEligibility fails with wrong VRF key") {
    IO {
      val sk1 = randomSk()
      val sk2 = randomSk()
      val vk2 = vrf.getVerificationKey(sk2) // Different key
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L

      val proof = EligibilityChecker.vrfProofForSlot(sk1, slot, eta)

      // Verification with wrong public key should fail
      val verified = EligibilityChecker.verifyEligibility(vk2, slot, slotGap, eta, 1.0, defaultConfig, proof)
      expect(!verified)
    }
  }

  test("verifyEligibility fails with tampered proof") {
    IO {
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L

      val proof = EligibilityChecker.vrfProofForSlot(sk, slot, eta)
      val tamperedProof = proof.clone()
      tamperedProof(0) = (tamperedProof(0) ^ 0xff).toByte

      val verified = EligibilityChecker.verifyEligibility(vk, slot, slotGap, eta, 1.0, defaultConfig, tamperedProof)
      expect(!verified)
    }
  }

  test("verifyEligibility fails with wrong slot") {
    IO {
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val wrongSlot = Slot.unsafeApply(11L)
      val slotGap = 20L

      val proof = EligibilityChecker.vrfProofForSlot(sk, slot, eta)

      val verified = EligibilityChecker.verifyEligibility(vk, wrongSlot, slotGap, eta, 1.0, defaultConfig, proof)
      expect(!verified)
    }
  }

  test("verifyEligibility fails with wrong eta") {
    IO {
      val sk = randomSk()
      val vk = vrf.getVerificationKey(sk)
      val eta = randomEta()
      val wrongEta = randomEta()
      val slot = Slot.unsafeApply(10L)
      val slotGap = 20L

      val proof = EligibilityChecker.vrfProofForSlot(sk, slot, eta)

      val verified = EligibilityChecker.verifyEligibility(vk, slot, slotGap, wrongEta, 1.0, defaultConfig, proof)
      expect(!verified)
    }
  }

  // ============ Epoch Eta Tests ============

  test("computeNextEta is deterministic") {
    IO {
      val prevEta = randomEta()
      val epoch = 42L
      val rhoHashes = List(randomEta(), randomEta(), randomEta())

      val eta1 = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)
      val eta2 = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)

      expect(java.util.Arrays.equals(eta1, eta2))
    }
  }

  test("computeNextEta produces 32-byte output") {
    IO {
      val prevEta = randomEta()
      val epoch = 1L
      val rhoHashes = List(randomEta())

      val nextEta = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)
      expect.eql(32, nextEta.length)
    }
  }

  test("computeNextEta differs with different inputs") {
    IO {
      val prevEta = randomEta()
      val epoch = 1L
      val rhoHashes = List(randomEta())

      val eta1 = EligibilityChecker.computeNextEta(prevEta, epoch, rhoHashes)
      val eta2 = EligibilityChecker.computeNextEta(prevEta, epoch + 1, rhoHashes)
      val eta3 = EligibilityChecker.computeNextEta(randomEta(), epoch, rhoHashes)
      val eta4 = EligibilityChecker.computeNextEta(prevEta, epoch, List(randomEta()))

      expect(!java.util.Arrays.equals(eta1, eta2)) and
        expect(!java.util.Arrays.equals(eta1, eta3)) and
        expect(!java.util.Arrays.equals(eta1, eta4))
    }
  }

  test("computeNextEta handles empty rhoNonceHashes") {
    IO {
      val prevEta = randomEta()
      val epoch = 1L

      val eta = EligibilityChecker.computeNextEta(prevEta, epoch, List.empty)
      expect.eql(32, eta.length)
    }
  }

  // ============ Edge Cases ============

  test("vrfProofForSlot requires 32-byte eta") {
    IO {
      val sk = randomSk()
      val slot = Slot.unsafeApply(0L)
      val shortEta = new Array[Byte](16)

      val result = scala.util.Try {
        EligibilityChecker.vrfProofForSlot(sk, slot, shortEta)
      }

      expect(result.isFailure)
    }
  }

  test("threshold handles edge case difficulty = 1.0") {
    IO {
      // If somehow difficulty >= 1.0, should return 1.0
      val config = LddConfig(lddCutoff = 1, offset = 0, baselineDifficulty = 1.0, amplitude = 1.0)
      val thresh = EligibilityChecker.threshold(1.0, 10, config)
      expect.eql(1.0, thresh)
    }
  }

  test("threshold handles slotGap = 0 with offset = 0") {
    IO {
      // At slotGap = 0, offset = 0: should be in ramp with difficulty = 0
      val config = LddConfig(lddCutoff = 15, offset = 0, baselineDifficulty = 0.05, amplitude = 0.5)
      val thresh = EligibilityChecker.threshold(1.0, 0, config)
      // difficulty = 0.5 * (0 - 0) / (15 - 0) = 0
      expect.eql(0.0, thresh)
    }
  }
}
