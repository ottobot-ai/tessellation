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

  pureTest("rotationPeriodRange returns correct bounds") {
    val (start, end) = EtaCalculation.rotationPeriodRange(2, etaRotation)
    expect.all(
      start == 1200L,
      end == 1800L
    )
  }

  pureTest("twoThirdsCutoff is at 2/3 of period") {
    // Period 1: slots [600, 1200), 2/3 cutoff at 600 + 400 = 1000
    expect(EtaCalculation.twoThirdsCutoff(1, etaRotation) == 1000L)
  }

  pureTest("period 0 uses genesis eta") {
    val eta = EtaCalculation.etaForSlot(100, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(genesisEta))
  }

  pureTest("period 1 uses genesis eta") {
    val eta = EtaCalculation.etaForSlot(700, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(genesisEta))
  }

  pureTest("period 2 derives from period 1 VRF outputs") {
    val fakeVrfOutputs = List(Array.fill(64)(0x01.toByte), Array.fill(64)(0x02.toByte))
    val eta = EtaCalculation.etaForSlot(
      1300, // period 2
      etaRotation,
      genesisEta,
      period => if (period == 1) fakeVrfOutputs else Nil
    )
    // Should not be genesis eta
    expect(!eta.sameElements(genesisEta)) &&
    expect(eta.length == 32)
  }

  pureTest("period 2 with no blocks in period 1 falls back to genesis eta") {
    val eta = EtaCalculation.etaForSlot(1300, etaRotation, genesisEta, _ => Nil)
    expect(eta.sameElements(genesisEta))
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
    // Period 1: slots [600, 1200), cutoff at 1000
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

  pureTest("extractVrfOutputsForPeriod orders by slot") {
    val chainOutputs = List(
      (800L, Array.fill(64)(0x02.toByte)),
      (650L, Array.fill(64)(0x01.toByte)),
      (900L, Array.fill(64)(0x03.toByte))
    )
    val extracted = EtaCalculation.extractVrfOutputsForPeriod(chainOutputs, 1, etaRotation)
    expect(extracted.length == 3) &&
    expect(extracted(0).head == 0x01.toByte) && // slot 650
    expect(extracted(1).head == 0x02.toByte) && // slot 800
    expect(extracted(2).head == 0x03.toByte) // slot 900
  }

  pureTest("eta is 32 bytes (Blake2b-256)") {
    val outputs = List(Array.fill(64)(0xff.toByte))
    val eta = EtaCalculation.computeEta(genesisEta, 5, outputs)
    expect(eta.length == 32)
  }

  pureTest("full flow: period 0 → 1 → 2 with chain data") {
    // Simulate chain: some blocks in period 0, some in period 1
    val period0Outputs = (0 until 10).map(i => (i * 50L, Array.fill(64)(i.toByte))).toList
    val period1Outputs = (0 until 8).map(i => ((600 + i * 50).toLong, Array.fill(64)((i + 100).toByte))).toList
    val allOutputs = period0Outputs ++ period1Outputs

    // Period 0 and 1: genesis eta
    val eta0 = EtaCalculation.etaForSlot(100, etaRotation, genesisEta, _ => Nil)
    val eta1 = EtaCalculation.etaForSlot(700, etaRotation, genesisEta, _ => Nil)
    expect(eta0.sameElements(genesisEta)) &&
    expect(eta1.sameElements(genesisEta)) && {
      // Period 2: derived from period 1's first 2/3 outputs
      val eta2 = EtaCalculation.etaForSlot(
        1300,
        etaRotation,
        genesisEta,
        period => EtaCalculation.extractVrfOutputsForPeriod(allOutputs, period, etaRotation)
      )
      expect(!eta2.sameElements(genesisEta)) &&
      expect(eta2.length == 32)
    }
  }
}
