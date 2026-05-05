package io.constellationnetwork.security.vrf

import java.security.SecureRandom

import cats.effect.IO

import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.LddConfig

import weaver.SimpleIOSuite

object EcVrf25519UniformitySuite extends SimpleIOSuite {

  private val vrf = new EcVrf25519()

  // Inline Double-based threshold (lossy reference) — used for the simulation envelope check and as a smoke
  // test that BouncyCastle VRF outputs distribute uniformly. The exact-Ratio threshold is exercised in
  // RatioNumericsSuite + EligibilityCheckerSuite. Cfg.amplitude / baselineDifficulty are Ratio; convert
  // once at the top via .toDouble (safe — test-only path, never on consensus).
  private def threshold(relativeStake: Double, slotGap: Long, cfg: LddConfig): Double = {
    val amp = cfg.amplitude.toDouble
    val base = cfg.baselineDifficulty.toDouble
    val f =
      if (slotGap < cfg.offset) 0.0
      else if (slotGap < cfg.lddCutoff) amp * (slotGap - cfg.offset).toDouble / (cfg.lddCutoff - cfg.offset).toDouble
      else base
    if (f <= 0.0) 0.0
    else if (f >= 1.0) 1.0
    else 1.0 - math.pow(1.0 - f, relativeStake)
  }

  private def normalize(vrfOutput: Array[Byte]): Double = {
    val n = BigInt(1, vrfOutput)
    val d = BigDecimal(BigInt(2).pow(512))
    (BigDecimal(n) / d).toDouble
  }

  private def vrfMessage(slot: Long, eta: Array[Byte]): Array[Byte] = {
    val s = java.nio.ByteBuffer.allocate(8).putLong(slot).array()
    eta ++ s
  }

  // Reproducibility: fixed RNG seed so failures are debuggable.
  private val rngSeed: Long = 0x1deaL
  private def freshRng(): SecureRandom = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(rngSeed)
    r
  }

  /** Cluster-shaped simulation of the 8-node test:
    *
    *   - 8 keys, equal stake (0.125 each)
    *   - 6 000 slot trials (≈ 100 min at 1 s slots)
    *   - shared eta across all nodes (rotates every 600 slots → 10 periods)
    *   - per-slot threshold derived from a slot_gap that resets to 1 when ANY node wins
    *
    * Asserts that the per-node win-count distribution is consistent with iid Bernoulli with the empirical threshold integral. Specifically:
    * (1) cluster total within ±20 % of expectation, (2) per-node sample stdev within 3× the iid theoretical stdev. The 3× tolerance is
    * loose on purpose — this is a smoke test for "BouncyCastle VRF doesn't introduce per-key bias", not a precise variance bound.
    */
  test("VRF — 8-key cluster simulation: per-key win counts within iid Bernoulli envelope") {
    IO {
      val cfg = LddConfig.Default.copy(lddCutoff = 16) // matches docker-compose default
      val numNodes = 8
      val numSlots = 6000L
      val etaRotation = 600L
      val relativeStake = 1.0 / numNodes

      val rng = freshRng()
      val seeds: Array[Array[Byte]] = Array.fill(numNodes) {
        val s = new Array[Byte](32)
        rng.nextBytes(s)
        s
      }
      val genesisEta: Array[Byte] = {
        val e = new Array[Byte](32); rng.nextBytes(e); e
      }

      val winsPerNode = Array.fill(numNodes)(0)
      var lastWinSlot: Long = -1L
      var sumThresholdsAcrossSlots: Double = 0.0
      var slotsCounted: Long = 0L

      var slot = 0L
      while (slot < numSlots) {
        val gap = if (lastWinSlot < 0) slot + 1 else slot - lastWinSlot
        val period = slot / etaRotation
        // chain-derived eta surrogate: deterministic from genesis + period
        val eta =
          if (period == 0) genesisEta
          else {
            val mix = new Array[Byte](32)
            var i = 0
            while (i < 32) { mix(i) = (genesisEta(i) ^ period.toByte).toByte; i += 1 }
            mix
          }
        val thresh = threshold(relativeStake, gap, cfg)
        sumThresholdsAcrossSlots += thresh
        slotsCounted += 1L

        var anyoneWon = false
        var n = 0
        while (n < numNodes) {
          val proof = vrf.vrfProof(seeds(n), vrfMessage(slot, eta))
          val outputOpt = vrf.vrfProofToHash(proof)
          val won = outputOpt.exists { out =>
            normalize(out) < thresh
          }
          if (won) {
            winsPerNode(n) += 1
            anyoneWon = true
          }
          n += 1
        }
        if (anyoneWon) lastWinSlot = slot
        slot += 1L
      }

      val total = winsPerNode.sum
      val mean = total.toDouble / numNodes
      val variance = winsPerNode.iterator.map(w => (w - mean) * (w - mean)).sum / (numNodes - 1)
      val sampleStd = math.sqrt(variance)

      // Expected per-node count = sum_t threshold(gap_t) (since per-node trials are iid given the cluster gap process).
      val expectedPerNode = sumThresholdsAcrossSlots
      // iid Bernoulli stdev per node (lower bound on what we should see — gap correlation only adds variance).
      val theoreticalStd = math.sqrt(sumThresholdsAcrossSlots * (1.0 - sumThresholdsAcrossSlots / numSlots.toDouble))

      val report =
        s"""
           |[VRF uniformity sim]
           |  cluster total wins      = $total over ${numSlots} slots, $numNodes nodes
           |  per-node wins            = ${winsPerNode.mkString(", ")}
           |  per-node mean            = ${"%.2f".format(mean)}
           |  per-node sample stdev    = ${"%.2f".format(sampleStd)}
           |  expected per-node (iid)  = ${"%.2f".format(expectedPerNode)}
           |  theoretical iid stdev    = ${"%.2f".format(theoreticalStd)}
           |  variance ratio (obs/iid) = ${"%.2f".format(variance / (theoreticalStd * theoreticalStd))}
           |  min / max                = ${winsPerNode.min} / ${winsPerNode.max}
           |""".stripMargin
      println(report)

      val totalLowerBound = (numNodes * expectedPerNode * 0.8).toInt
      val totalUpperBound = (numNodes * expectedPerNode * 1.2).toInt
      val totalInRange = total >= totalLowerBound && total <= totalUpperBound
      val varianceWithin3xIid = sampleStd <= 3.0 * theoreticalStd

      expect(totalInRange, s"cluster total $total outside [$totalLowerBound, $totalUpperBound] (mean ${expectedPerNode * numNodes})")
        .and(expect(varianceWithin3xIid, s"sample stdev $sampleStd > 3× iid theoretical stdev $theoreticalStd"))
        .and(expect(winsPerNode.min > 0, "every key should have at least one win"))
    }
  }

  /** Direct VRF-output uniformity check on a single fixed key, no thresholding. Verifies that BouncyCastle EcVrf25519 outputs distribute
    * approximately uniformly in [0,1) over varied (slot, eta) inputs.
    */
  test("VRF — single-key output histogram is approximately uniform on [0,1)") {
    IO {
      val rng = freshRng()
      val sk = new Array[Byte](32); rng.nextBytes(sk)
      val eta = new Array[Byte](32); rng.nextBytes(eta)
      val numSamples = 20000

      val numBuckets = 10
      val buckets = Array.fill(numBuckets)(0)
      var slot = 0L
      while (slot < numSamples) {
        val proof = vrf.vrfProof(sk, vrfMessage(slot, eta))
        val outputOpt = vrf.vrfProofToHash(proof)
        outputOpt.foreach { out =>
          val v = math.min(0.999999999, normalize(out))
          val b = (v * numBuckets).toInt
          buckets(b) += 1
        }
        slot += 1L
      }

      val expectedPerBucket = numSamples.toDouble / numBuckets
      // Chi-squared test, df=9; critical value at α=0.001 ≈ 27.88. Use 35 as a comfortable upper bound to
      // avoid flakes — anything higher implies a real distributional skew, not noise.
      val chiSq = buckets.iterator.map { c =>
        val d = c - expectedPerBucket
        d * d / expectedPerBucket
      }.sum
      println(s"[VRF histogram] buckets = ${buckets.mkString(",")}, chi^2 = ${"%.2f".format(chiSq)} (df=9, threshold=35)")
      expect(chiSq < 35.0, s"chi-squared $chiSq exceeds threshold 35 — VRF output non-uniform")
    }
  }
}
