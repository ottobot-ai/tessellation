package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.security.hash.Hash

import org.bouncycastle.crypto.digests.Blake2bDigest

/** Chain-derived eta calculation following Bifrost/Cardano pattern.
  *
  * Eta for rotation period N>=2 is derived from VRF outputs in the first 2/3 of rotation period N-1. Periods 0 AND 1 use the
  * genesis-derivable bootstrap eta `Blake2b(genesisEta ‖ period)` (distinct per period, no VRF-output dependency) — the Cardano/Praos
  * bootstrap convention — so the first eta rotation (period 0 → 1) cannot fork on disagreement about period 0's still-unsettled VRF
  * outputs. First VRF-folded eta is period 2.
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
    *   - Periods 0 and 1: genesis-derivable bootstrap eta [[bootstrapEta]] = `Blake2b(genesisEta ‖ period)` (distinct, no VRF dependency)
    *   - Period N (N >= 2): eta derived from VRF outputs in first 2/3 of period N-1; degenerate empty-source case falls back to
    *     [[bootstrapEta]]
    *
    * Cardano/Praos bootstrap (supersedes the #259 "COMPUTED period-1" convention): periods 0 and 1 fold NO VRF outputs, so every node
    * computes them identically from genesis and the first eta rotation cannot fork. This reference must agree byte-for-byte with the
    * canonical per-period eta at every other site — [[EtaStateManager.getEta]], the producer/validator eta in `SnapshotLeaderLoop` /
    * `NakamotoSyncDaemon`, and the committee draw — all of which now key on `period <= 1` and route periods 0/1 through [[bootstrapEta]].
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
      // Cardano/Praos bootstrap: periods 0 AND 1 are derived purely from genesis (no dependency on any
      // period's VRF outputs), but DISTINCT per period via the folded period number. The first VRF-folded
      // eta is period 2 — by then period 1 is deeply finalized and its first-2/3 VRF outputs are agreed
      // cluster-wide, so the rotation cannot fork. (The prior #259 "COMPUTED period-1" convention folded
      // period 0's UNSETTLED outputs here and forked at the 0→1 boundary — the ord≈R finalization stall.)
      bootstrapEta(genesisEta, period)
    } else {
      // Period N (>= 2): derive from VRF outputs in first 2/3 of period N-1
      val sourcePeriod = period - 1
      val vrfOutputs = lookupVrfOutputsForPeriod(sourcePeriod)

      if (vrfOutputs.isEmpty) {
        // Source period has no VRF outputs — should not happen for N>=2 in a healthy chain (the source is
        // finalized before this is read). Use the per-period genesis-derivable value, NOT raw genesisEta:
        // a lagging node must not substitute a value a caught-up node won't reproduce.
        bootstrapEta(genesisEta, period)
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

  /** Bootstrap eta for the genesis-derivable rotation periods (0 and 1): `Blake2b-256(genesisEta ‖ period)` with NO VRF-output dependency.
    * Distinct per period via the folded period number, yet computable by every node from genesis alone — so the first eta rotation (period
    * 0 → 1) cannot fork on disagreement about period 0's (still-unsettled) VRF outputs. Implemented as [[computeEta]] with an empty output
    * list, so it is byte-identical to the period >= 2 fold base. This is the Cardano/Praos bootstrap convention (first VRF-folded nonce at
    * period 2). Callers MUST route periods 0 and 1 through this at EVERY eta site (producer, validator, committee draw, follower adopt,
    * boundary write) or the cluster forks — see the per-site list in `docs/nakamoto/attestation-and-finality.md` §1.
    */
  def bootstrapEta(genesisEta: Array[Byte], period: Long): Array[Byte] =
    computeEta(genesisEta, period, List.empty)

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
