package io.constellationnetwork.node.shared.domain.nakamoto

import weaver.SimpleIOSuite

object EtaCalculationSuite extends SimpleIOSuite {

  val etaRotation = 600L
  val genesisEta: Array[Byte] = Array.fill(32)(0x42.toByte)

  pureTest("rotationPeriod computes correctly") {
    expect.all(
      EtaCalculation.rotationPeriod(0, etaRotation) == 0L,
      EtaCalculation.rotationPeriod(599, etaRotation) == 0L,
      EtaCalculation.rotationPeriod(600, etaRotation) == 1L,
      EtaCalculation.rotationPeriod(1199, etaRotation) == 1L,
      EtaCalculation.rotationPeriod(1200, etaRotation) == 2L
    )
  }

  pureTest("global snapshot artifact period follows the predecessor at R-1, R, and R+1") {
    val boundary = etaRotation
    expect.all(
      EtaCalculation.globalSnapshotArtifactPeriod(boundary - 1L, boundary) ==
        io.constellationnetwork.schema.nakamoto.EtaPeriod.Zero,
      EtaCalculation.globalSnapshotArtifactPeriod(boundary, boundary) ==
        io.constellationnetwork.schema.nakamoto.EtaPeriod.Zero,
      EtaCalculation.globalSnapshotArtifactPeriod(boundary + 1L, boundary) ==
        io.constellationnetwork.schema.nakamoto.EtaPeriod(1L),
      EtaCalculation.globalSnapshotArtifactPeriod(0L, boundary) ==
        io.constellationnetwork.schema.nakamoto.EtaPeriod.Zero
    )
  }

  pureTest("parent-indexed KES maintenance prepares the same period as its next child") {
    def maintenancePeriod(parentOrdinal: Long) =
      io.constellationnetwork.schema.nakamoto.EtaPeriod(EtaCalculation.rotationPeriod(parentOrdinal, etaRotation))

    expect.all(
      EtaCalculation.globalSnapshotArtifactPeriod(etaRotation, etaRotation) == maintenancePeriod(etaRotation - 1L),
      EtaCalculation.globalSnapshotArtifactPeriod(etaRotation + 1L, etaRotation) == maintenancePeriod(etaRotation),
      EtaCalculation.globalSnapshotArtifactPeriod(etaRotation + 2L, etaRotation) == maintenancePeriod(etaRotation + 1L)
    )
  }

  pureTest("leader stake lookback is derived from the exact parent ordinal") {
    expect.all(
      EtaCalculation.leaderStakeLookbackPeriod(0L, etaRotation) == io.constellationnetwork.schema.nakamoto.EtaPeriod(-2L),
      EtaCalculation.leaderStakeLookbackPeriod(599L, etaRotation) == io.constellationnetwork.schema.nakamoto.EtaPeriod(-2L),
      EtaCalculation.leaderStakeLookbackPeriod(600L, etaRotation) == io.constellationnetwork.schema.nakamoto.EtaPeriod(-1L),
      EtaCalculation.leaderStakeLookbackPeriod(1199L, etaRotation) == io.constellationnetwork.schema.nakamoto.EtaPeriod(-1L),
      EtaCalculation.leaderStakeLookbackPeriod(1200L, etaRotation) == io.constellationnetwork.schema.nakamoto.EtaPeriod(0L)
    )
  }

  pureTest("rotationPeriodRange returns correct bounds") {
    val (start, end) = EtaCalculation.rotationPeriodRange(2, etaRotation)
    expect.all(
      start == 1200L,
      end == 1800L
    )
  }

  pureTest("twoThirdsCutoff is at 2/3 of period") {
    // Period 1: ordinals [600, 1200), 2/3 cutoff at 600 + 400 = 1000
    expect(EtaCalculation.twoThirdsCutoff(1, etaRotation) == 1000L)
  }

  pureTest("period 0 uses bootstrap eta (Cardano/Praos bootstrap)") {
    // Period 0 is the genesis-derivable bootstrap eta `bootstrapEta(genesisEta, 0)` =
    // computeEta(genesisEta, 0, List.empty) — NOT raw genesisEta — and folds NO VRF outputs.
    val eta = EtaCalculation.etaForOrdinal(100, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(EtaCalculation.bootstrapEta(genesisEta, 0))) &&
    expect(!eta.sameElements(genesisEta)) &&
    expect(eta.length == 32)
  }

  pureTest("period 1 uses bootstrap eta (Cardano/Praos bootstrap)") {
    // Period 1 is also genesis-derivable: `bootstrapEta(genesisEta, 1)` = computeEta(genesisEta, 1,
    // List.empty), with NO VRF-output dependency and DISTINCT from period 0 (H(g‖0) ≠ H(g‖1)).
    val eta = EtaCalculation.etaForOrdinal(700, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(EtaCalculation.bootstrapEta(genesisEta, 1))) &&
    expect(!eta.sameElements(genesisEta)) &&
    expect(!eta.sameElements(EtaCalculation.bootstrapEta(genesisEta, 0))) &&
    expect(eta.length == 32)
  }

  pureTest("period 1 is INDEPENDENT of period 0 VRF outputs (bootstrap convention)") {
    // The key new invariant (inverse of the old #259 COMPUTED-period-1 test): period 1 folds NO VRF
    // outputs, so feeding DIFFERENT period-0 outputs leaves the period-1 eta UNCHANGED — it always
    // equals `bootstrapEta(genesisEta, 1)`. This is what makes the first eta rotation (0 → 1) fork-proof.
    val outputsA = List(Array.fill(64)(0x01.toByte), Array.fill(64)(0x02.toByte))
    val outputsB = List(Array.fill(64)(0xaa.toByte), Array.fill(64)(0xbb.toByte))
    val etaA = EtaCalculation.etaForOrdinal(700, etaRotation, genesisEta, period => if (period == 0) outputsA else Nil)
    val etaB = EtaCalculation.etaForOrdinal(700, etaRotation, genesisEta, period => if (period == 0) outputsB else Nil)
    val expected = EtaCalculation.bootstrapEta(genesisEta, 1)
    expect(etaA.sameElements(expected)) &&
    expect(etaB.sameElements(expected)) &&
    expect(etaA.sameElements(etaB)) && // period 1 is invariant under period-0 outputs
    expect(etaA.length == 32)
  }

  pureTest("period 2 derives from period 1 VRF outputs") {
    val fakeVrfOutputs = List(Array.fill(64)(0x01.toByte), Array.fill(64)(0x02.toByte))
    val eta = EtaCalculation.etaForOrdinal(
      1300, // period 2
      etaRotation,
      genesisEta,
      period => if (period == 1) fakeVrfOutputs else Nil
    )
    // Should not be genesis eta
    expect(!eta.sameElements(genesisEta)) &&
    expect(eta.length == 32)
  }

  pureTest("period 2 with no blocks in period 1 falls back to bootstrap eta (not raw genesis)") {
    // Degenerate empty-source case for N>=2: falls back to the per-period bootstrapEta(genesisEta, 2),
    // NOT raw genesisEta — a lagging node must not substitute a value a caught-up node won't reproduce.
    val eta = EtaCalculation.etaForOrdinal(1300, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(EtaCalculation.bootstrapEta(genesisEta, 2))) &&
    expect(!eta.sameElements(genesisEta))
  }

  pureTest("computeEta is deterministic") {
    val outputs = List(Array.fill(64)(0xaa.toByte), Array.fill(64)(0xbb.toByte))
    val eta1 = EtaCalculation.computeEta(genesisEta, 2, outputs)
    val eta2 = EtaCalculation.computeEta(genesisEta, 2, outputs)
    expect(eta1.sameElements(eta2))
  }

  pureTest("computeEta differs for different epochs") {
    val outputs = List(Array.fill(64)(0xaa.toByte))
    val eta1 = EtaCalculation.computeEta(genesisEta, 2, outputs)
    val eta2 = EtaCalculation.computeEta(genesisEta, 3, outputs)
    expect(!eta1.sameElements(eta2))
  }

  pureTest("computeEta differs for different VRF outputs") {
    val outputs1 = List(Array.fill(64)(0xaa.toByte))
    val outputs2 = List(Array.fill(64)(0xbb.toByte))
    val eta1 = EtaCalculation.computeEta(genesisEta, 2, outputs1)
    val eta2 = EtaCalculation.computeEta(genesisEta, 2, outputs2)
    expect(!eta1.sameElements(eta2))
  }

  pureTest("computeEta differs for different previous eta") {
    val outputs = List(Array.fill(64)(0xaa.toByte))
    val eta1 = EtaCalculation.computeEta(Array.fill(32)(0x01.toByte), 2, outputs)
    val eta2 = EtaCalculation.computeEta(Array.fill(32)(0x02.toByte), 2, outputs)
    expect(!eta1.sameElements(eta2))
  }

  pureTest("extractVrfOutputsForPeriod filters to first 2/3") {
    // Period 1: ordinals [600, 1200), cutoff at 1000
    val chainOutputs = List(
      (550L, Array.fill(64)(0x00.toByte)), // period 0 — excluded
      (650L, Array.fill(64)(0x01.toByte)), // period 1, before cutoff — included
      (900L, Array.fill(64)(0x02.toByte)), // period 1, before cutoff — included
      (999L, Array.fill(64)(0x03.toByte)), // period 1, before cutoff — included
      (1000L, Array.fill(64)(0x04.toByte)), // period 1, AT cutoff — excluded (cutoff is exclusive)
      (1100L, Array.fill(64)(0x05.toByte)), // period 1, after cutoff — excluded
      (1300L, Array.fill(64)(0x06.toByte)) // period 2 — excluded
    )

    val extracted = EtaCalculation.extractVrfOutputsForPeriod(chainOutputs, 1, etaRotation)
    expect(extracted.length == 3) &&
    expect(extracted(0).head == 0x01.toByte) &&
    expect(extracted(1).head == 0x02.toByte) &&
    expect(extracted(2).head == 0x03.toByte)
  }

  pureTest("extractVrfOutputsForPeriod returns empty for period with no blocks") {
    val chainOutputs = List(
      (50L, Array.fill(64)(0x01.toByte)) // period 0 only
    )
    val extracted = EtaCalculation.extractVrfOutputsForPeriod(chainOutputs, 1, etaRotation)
    expect(extracted.isEmpty)
  }

  pureTest("extractVrfOutputsForPeriod orders by ordinal") {
    val chainOutputs = List(
      (800L, Array.fill(64)(0x02.toByte)),
      (650L, Array.fill(64)(0x01.toByte)),
      (900L, Array.fill(64)(0x03.toByte))
    )
    val extracted = EtaCalculation.extractVrfOutputsForPeriod(chainOutputs, 1, etaRotation)
    expect(extracted.length == 3) &&
    expect(extracted(0).head == 0x01.toByte) && // ordinal 650
    expect(extracted(1).head == 0x02.toByte) && // ordinal 800
    expect(extracted(2).head == 0x03.toByte) // ordinal 900
  }

  pureTest("eta is 32 bytes (Blake2b-256)") {
    val outputs = List(Array.fill(64)(0xff.toByte))
    val eta = EtaCalculation.computeEta(genesisEta, 5, outputs)
    expect(eta.length == 32)
  }

  pureTest("full flow: period 0 → 1 → 2 with chain data (Cardano/Praos bootstrap)") {
    // Simulate chain: some snapshots in period 0, some in period 1
    val period0Outputs = (0 until 10).map(i => (i * 50L, Array.fill(64)(i.toByte))).toList
    val period1Outputs = (0 until 8).map(i => ((600 + i * 50).toLong, Array.fill(64)((i + 100).toByte))).toList
    val allOutputs = period0Outputs ++ period1Outputs
    val lookup: Long => List[Array[Byte]] = period => EtaCalculation.extractVrfOutputsForPeriod(allOutputs, period, etaRotation)

    // Periods 0 AND 1: genesis-derivable bootstrapEta (fold NO VRF outputs, distinct per period) — so
    // the first rotation cannot fork on period 0's still-unsettled outputs. Period 2: first VRF-folded
    // eta, derived from period 1's first 2/3 outputs.
    val eta0 = EtaCalculation.etaForOrdinal(100, etaRotation, genesisEta, lookup)
    val eta1 = EtaCalculation.etaForOrdinal(700, etaRotation, genesisEta, lookup)
    val eta2 = EtaCalculation.etaForOrdinal(1300, etaRotation, genesisEta, lookup)
    expect(eta0.sameElements(EtaCalculation.bootstrapEta(genesisEta, 0))) &&
    expect(eta1.sameElements(EtaCalculation.bootstrapEta(genesisEta, 1))) &&
    expect(!eta0.sameElements(eta1)) && // distinct per period (H(g‖0) ≠ H(g‖1))
    expect(eta1.length == 32) &&
    expect(eta2.sameElements(EtaCalculation.computeEta(genesisEta, 2, lookup(1)))) &&
    expect(!eta2.sameElements(eta1)) &&
    expect(eta2.length == 32)
  }
}
