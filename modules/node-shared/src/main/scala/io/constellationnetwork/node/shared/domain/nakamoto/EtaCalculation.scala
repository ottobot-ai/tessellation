package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.security.hash.Hash

import org.bouncycastle.crypto.digests.Blake2bDigest

/** Chain-derived eta calculation following Bifrost/Cardano pattern.
  *
  * Eta for rotation period N is derived from VRF outputs in the first 2/3 of rotation period N-1. Genesis eta is used for rotation period 0
  * (and period 1, since period 0 has no predecessor).
  *
  * This ensures:
  *   - All nodes seeing the same chain compute the same eta (deterministic from chain)
  *   - Eta for period N is knowable at the 2/3 point of period N-1 (lookahead)
  *   - A single adversary cannot grind eta without controlling 2/3 of slot leaders
  */
object EtaCalculation {

  /** Compute which rotation period a slot belongs to. Period 0 = slots [0, etaRotationSlots), Period 1 = [etaRotationSlots,
    * 2*etaRotationSlots), etc.
    */
  def rotationPeriod(slot: Long, etaRotationSlots: Long): Long =
    slot / etaRotationSlots

  /** Compute the slot range for a rotation period. Returns (startSlot, endSlot) inclusive of start, exclusive of end.
    */
  def rotationPeriodRange(period: Long, etaRotationSlots: Long): (Long, Long) =
    (period * etaRotationSlots, (period + 1) * etaRotationSlots)

  /** Compute the 2/3 cutoff slot within a rotation period. VRF outputs from slots < cutoff in the period contribute to next period's eta.
    */
  def twoThirdsCutoff(period: Long, etaRotationSlots: Long): Long = {
    val (start, _) = rotationPeriodRange(period, etaRotationSlots)
    start + (etaRotationSlots * 2 / 3)
  }

  /** Determine which eta to use for a given slot.
    *
    *   - Period 0: genesis eta
    *   - Period 1: genesis eta (no predecessor period to derive from)
    *   - Period N (N >= 2): eta derived from VRF outputs in first 2/3 of period N-1
    *
    * The caller must supply the VRF outputs from the chain for the relevant period. This method only handles the "which period and what
    * inputs" logic.
    */
  def etaForSlot(
    slot: Long,
    etaRotationSlots: Long,
    genesisEta: Array[Byte],
    lookupVrfOutputsForPeriod: Long => List[Array[Byte]]
  ): Array[Byte] = {
    val period = rotationPeriod(slot, etaRotationSlots)

    if (period <= 1) {
      // Periods 0 and 1 use genesis eta
      genesisEta
    } else {
      // Period N (>= 2): derive from VRF outputs in first 2/3 of period N-1
      val sourcePeriod = period - 1
      val vrfOutputs = lookupVrfOutputsForPeriod(sourcePeriod)

      if (vrfOutputs.isEmpty) {
        // No blocks in source period — use previous eta (degenerate case)
        // In production, this should be rare with reasonable LDD params
        genesisEta
      } else {
        computeEta(genesisEta, period, vrfOutputs)
      }
    }
  }

  /** Compute eta from previous eta, epoch number, and VRF outputs.
    *
    * eta_N = Blake2b-256(eta_{N-1} || epoch || vrfOutput_1 || ... || vrfOutput_k)
    *
    * This matches EligibilityChecker.computeNextEta but is the canonical reference.
    */
  def computeEta(previousEta: Array[Byte], epoch: Long, vrfOutputs: List[Array[Byte]]): Array[Byte] = {
    require(previousEta.length == 32, s"previousEta must be 32 bytes, got ${previousEta.length}")

    val digest = new Blake2bDigest(256)
    digest.update(previousEta, 0, previousEta.length)

    val epochBytes = java.nio.ByteBuffer.allocate(8).putLong(epoch).array()
    digest.update(epochBytes, 0, epochBytes.length)

    vrfOutputs.foreach { output =>
      digest.update(output, 0, output.length)
    }

    val result = new Array[Byte](32)
    digest.doFinal(result, 0)
    result
  }

  /** Extract VRF outputs from a chain segment for a specific rotation period's first 2/3.
    *
    * Given a list of (slot, vrfOutput) pairs from the chain, filter to those in the first 2/3 of the specified rotation period.
    */
  def extractVrfOutputsForPeriod(
    chainVrfOutputs: List[(Long, Array[Byte])],
    period: Long,
    etaRotationSlots: Long
  ): List[Array[Byte]] = {
    val (periodStart, _) = rotationPeriodRange(period, etaRotationSlots)
    val cutoff = twoThirdsCutoff(period, etaRotationSlots)

    chainVrfOutputs.filter { case (slot, _) => slot >= periodStart && slot < cutoff }
      .sortBy(_._1) // ensure deterministic ordering by slot
      .map(_._2)
  }
}
