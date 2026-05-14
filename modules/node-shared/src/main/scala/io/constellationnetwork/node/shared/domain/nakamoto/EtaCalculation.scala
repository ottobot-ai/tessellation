package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.security.hash.Hash

import org.bouncycastle.crypto.digests.Blake2bDigest

/** Chain-derived eta calculation following Bifrost/Cardano pattern.
  *
  * Eta for rotation period N is derived from VRF outputs in the first 2/3 of rotation period N-1. Genesis eta is used for rotation period 0
  * (and period 1, since period 0 has no predecessor).
  *
  * Rotation periods are keyed on **snapshot ordinal**, not slot — slots are LDD-paced and lumpy; ordinals are 1:1 with snapshots and give a
  * stable R that satisfies the Praos R ≥ 3·k₁ stability bound. See `docs/nakamoto/attestation-and-finality.md` §1.
  *
  * This ensures:
  *   - All nodes seeing the same chain compute the same eta (deterministic from chain)
  *   - Eta for period N is knowable at the 2/3 point of period N-1 (lookahead)
  *   - A single adversary cannot grind eta without controlling 2/3 of slot leaders
  */
object EtaCalculation {

  /** Compute which rotation period an ordinal belongs to. Period 0 = ordinals [0, etaRotationSnapshots), Period 1 = [etaRotationSnapshots,
    * 2*etaRotationSnapshots), etc.
    *
    * Keyed on **ordinal**, not slot — the security argument is about CP-safety of VRF inputs (a snapshot-indexed property), and slot rate
    * varies under LDD-fill drift. See `docs/nakamoto/attestation-and-finality.md` §1.
    */
  def rotationPeriod(ordinal: Long, etaRotationSnapshots: Long): Long =
    ordinal / etaRotationSnapshots

  /** Compute the ordinal range for a rotation period. Returns (startOrdinal, endOrdinal) inclusive of start, exclusive of end.
    */
  def rotationPeriodRange(period: Long, etaRotationSnapshots: Long): (Long, Long) =
    (period * etaRotationSnapshots, (period + 1) * etaRotationSnapshots)

  /** Compute the 2/3 cutoff ordinal within a rotation period. VRF outputs from ordinals < cutoff in the period contribute to next period's
    * eta.
    */
  def twoThirdsCutoff(period: Long, etaRotationSnapshots: Long): Long = {
    val (start, _) = rotationPeriodRange(period, etaRotationSnapshots)
    start + (etaRotationSnapshots * 2 / 3)
  }

  /** Determine which eta to use for a given ordinal.
    *
    *   - Period 0: genesis eta
    *   - Period 1: genesis eta (no predecessor period to derive from)
    *   - Period N (N >= 2): eta derived from VRF outputs in first 2/3 of period N-1
    *
    * The caller must supply the VRF outputs from the chain for the relevant period. This method only handles the "which period and what
    * inputs" logic.
    */
  def etaForOrdinal(
    ordinal: Long,
    etaRotationSnapshots: Long,
    genesisEta: Array[Byte],
    lookupVrfOutputsForPeriod: Long => List[Array[Byte]]
  ): Array[Byte] = {
    val period = rotationPeriod(ordinal, etaRotationSnapshots)

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
    * Given a list of (ordinal, vrfOutput) pairs from the chain, filter to those in the first 2/3 of the specified rotation period. The
    * `Long` in the input pairs is the snapshot's **ordinal** (matches the snapshot-indexed rotation unit).
    */
  def extractVrfOutputsForPeriod(
    chainVrfOutputs: List[(Long, Array[Byte])],
    period: Long,
    etaRotationSnapshots: Long
  ): List[Array[Byte]] = {
    val (periodStart, _) = rotationPeriodRange(period, etaRotationSnapshots)
    val cutoff = twoThirdsCutoff(period, etaRotationSnapshots)

    chainVrfOutputs.filter { case (ordinal, _) => ordinal >= periodStart && ordinal < cutoff }
      .sortBy(_._1) // ensure deterministic ordering by ordinal
      .map(_._2)
  }
}
