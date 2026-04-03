package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.vrf.EcVrf25519

import org.bouncycastle.crypto.digests.Blake2bDigest

/**
 * VRF-based leader eligibility checker using LDD snowplow threshold.
 *
 * For each slot, a validator:
 * 1. Computes VRF proof over (eta || slot)
 * 2. Extracts VRF output hash (64 bytes)
 * 3. Normalizes to [0,1) as testValue
 * 4. Computes threshold from LDD snowplow + relative stake
 * 5. Eligible if threshold > testValue
 */
object EligibilityChecker {

  private val vrf = new EcVrf25519()

  /**
   * Compute LDD snowplow threshold.
   * threshold = 1 - (1 - f(δ))^relativeStake
   */
  def threshold(relativeStake: Double, slotGap: Long, config: LddConfig): Double = {
    val difficulty: Double =
      if (slotGap < config.offset) 0.0
      else if (slotGap < config.lddCutoff) {
        val psi = config.offset
        val gamma = config.lddCutoff
        config.amplitude * (slotGap - psi).toDouble / (gamma - psi).toDouble
      } else config.baselineDifficulty

    if (difficulty <= 0.0) 0.0
    else if (difficulty >= 1.0) 1.0
    else 1.0 - math.pow(1.0 - difficulty, relativeStake)
  }

  /**
   * Compute VRF proof for a slot.
   * Message = eta (32 bytes) || slot (8 bytes big-endian)
   */
  def vrfProofForSlot(vrfSK: Array[Byte], slot: Slot, eta: Array[Byte]): Array[Byte] = {
    require(eta.length == 32, s"Eta must be 32 bytes, got ${eta.length}")
    val slotBytes = java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
    val message = eta ++ slotBytes
    vrf.vrfProof(vrfSK, message)
  }

  /**
   * Normalize VRF output to [0, 1) for threshold comparison.
   * testValue = BigInt(vrfOutput) / 2^512
   */
  def normalizeVrfOutput(vrfOutput: Array[Byte]): Double = {
    val testValue = BigInt(1, vrfOutput) // unsigned big-endian
    val norm = BigDecimal(BigInt(2).pow(512))
    (BigDecimal(testValue) / norm).toDouble
  }

  /**
   * Check if a validator is eligible to produce a snapshot in the given slot.
   *
   * @param vrfSK 32-byte VRF secret key (Ed25519 seed)
   * @param slot current slot
   * @param slotGap slots since last snapshot was produced by anyone
   * @param eta current epoch randomness (32 bytes)
   * @param relativeStake this validator's stake fraction [0,1]
   * @param config LDD parameters
   * @return Some(proof, vrfOutput) if eligible, None if not
   */
  def checkEligibility(
    vrfSK: Array[Byte],
    slot: Slot,
    slotGap: Long,
    eta: Array[Byte],
    relativeStake: Double,
    config: LddConfig
  ): Option[(Array[Byte], Array[Byte])] = {
    val proof = vrfProofForSlot(vrfSK, slot, eta)
    val vrfOutput = vrf.vrfProofToHash(proof).getOrElse(return None)
    val testValue = normalizeVrfOutput(vrfOutput)
    val thresh = threshold(relativeStake, slotGap, config)

    if (thresh > testValue) Some((proof, vrfOutput))
    else None
  }

  /**
   * Verify that a snapshot's VRF proof is valid and the producer was eligible.
   */
  def verifyEligibility(
    vrfVK: Array[Byte],
    slot: Slot,
    slotGap: Long,
    eta: Array[Byte],
    relativeStake: Double,
    config: LddConfig,
    proof: Array[Byte]
  ): Boolean = {
    val slotBytes = java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
    val message = eta ++ slotBytes

    if (!vrf.vrfVerify(vrfVK, message, proof)) return false

    val vrfOutput = vrf.vrfProofToHash(proof).getOrElse(return false)
    val testValue = normalizeVrfOutput(vrfOutput)
    val thresh = threshold(relativeStake, slotGap, config)

    thresh > testValue
  }

  // ------- Epoch Eta -------

  /**
   * Compute epoch randomness from previous eta and VRF outputs.
   * eta_{e+1} = Blake2b-256(eta_e || epoch || concat(rhoNonceHashes))
   */
  def computeNextEta(previousEta: Array[Byte], epoch: Long, rhoNonceHashes: List[Array[Byte]]): Array[Byte] = {
    val digest = new Blake2bDigest(256)
    digest.update(previousEta, 0, previousEta.length)
    val epochBytes = java.nio.ByteBuffer.allocate(8).putLong(epoch).array()
    digest.update(epochBytes, 0, epochBytes.length)
    rhoNonceHashes.foreach(h => digest.update(h, 0, h.length))
    val out = new Array[Byte](32)
    digest.doFinal(out, 0)
    out
  }
}
