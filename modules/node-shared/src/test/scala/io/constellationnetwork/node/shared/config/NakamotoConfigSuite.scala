package io.constellationnetwork.node.shared.config

import scala.util.Try

import io.constellationnetwork.env.AppEnvironment._
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.numerics.Ratio

import eu.timepit.refined.pureconfig._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import weaver.SimpleIOSuite

/** Parse + invariant tests for the per-environment `nakamoto.confirmation-depth-k` (k₁) block.
  *
  * k₁ moved from a single scalar to a `{ mainnet, testnet, integrationnet, dev }` block typed `Map[AppEnvironment, PosLong]`, resolved once
  * at the use site (`confirmationDepthK(env)`). R and k₂ DERIVE from the resolved k₁. This decode goes through two non-trivial pureconfig
  * pieces compile can't check — the `environmentToPosLongMapReader` and the field-rename `ProductHint` (`confirmationDepthKByEnv` ←
  * `confirmation-depth-k`) — so a regression here would only surface at node boot. Values are powers of two by design (no off-by-one).
  */
object NakamotoConfigSuite extends SimpleIOSuite {

  private lazy val nakamoto: NakamotoConfig =
    ConfigSource.default.at("nakamoto").loadOrThrow[NakamotoConfig]

  pureTest("confirmation-depth-k parses the per-environment block (1024 / 256 / 256 / 32)") {
    expect.all(
      nakamoto.confirmationDepthK(Mainnet).value == 1024L,
      nakamoto.confirmationDepthK(Testnet).value == 256L,
      nakamoto.confirmationDepthK(Integrationnet).value == 256L,
      nakamoto.confirmationDepthK(Dev).value == 32L
    )
  }

  pureTest("R = round(3.1·k₁) and k₂ = 100·k₁ derive from the resolved per-env k₁") {
    expect.all(
      // dev k₁=32 → R=round(99.2)=99, k₂=3200
      nakamoto.etaRotationSnapshots(Dev).value == 99L,
      nakamoto.keepDepthBehindFinalized(Dev).value == 3200L,
      // mainnet k₁=1024 → R=round(3174.4)=3174, k₂=102400
      nakamoto.etaRotationSnapshots(Mainnet).value == 3174L,
      nakamoto.keepDepthBehindFinalized(Mainnet).value == 102400L,
      // testnet / integrationnet k₁=256 → k₂=25600. `keepDepthBehindFinalized` is THE single canonical k₂ accessor
      // (Track-3 S1 removed the duplicate inline `ArchivalDepthK = 100·k₁` in SnapshotLeaderLoop); guard it across every env.
      nakamoto.keepDepthBehindFinalized(Testnet).value == 25600L,
      nakamoto.keepDepthBehindFinalized(Integrationnet).value == 25600L
    )
  }

  pureTest("Track-3 S3: kLookback = k₁+1 and sWindow = round(R/3) derive from per-env k₁; band-density flag defaults OFF") {
    expect.all(
      // dev k₁=32 → kLookback=33; R=99 → sWindow=round(33.0)=33
      nakamoto.kLookback(Dev) == 33L,
      nakamoto.sWindow(Dev) == 33L,
      // mainnet k₁=1024 → kLookback=1025; R=3174 → sWindow=round(1058.0)=1058
      nakamoto.kLookback(Mainnet) == 1025L,
      nakamoto.sWindow(Mainnet) == 1058L,
      // testnet/integrationnet k₁=256 → kLookback=257; R=round(793.6)=794 → sWindow=round(264.67)=265
      nakamoto.kLookback(Testnet) == 257L,
      nakamoto.sWindow(Testnet) == 265L,
      nakamoto.kLookback(Integrationnet) == 257L,
      nakamoto.sWindow(Integrationnet) == 265L,
      // CONFIG-FLAG defaults OFF (byte-identical k₁-freeze baseline — the 2026-06-27 storm backstop).
      !nakamoto.bandDensityReorgEnabled
    )
  }

  pureTest("eta and density windows use exact half-up integer arithmetic at every remainder boundary") {
    val etaByK1 = (1L to 10L).map(NakamotoConfig.etaRotationSnapshotsFromK1).toList
    val densityByEta = (3L to 8L).map(NakamotoConfig.sWindowFromEtaRotation).toList

    expect.all(
      etaByK1 == List(3L, 6L, 9L, 12L, 16L, 19L, 22L, 25L, 28L, 31L),
      densityByEta == List(1L, 1L, 2L, 2L, 2L, 3L)
    )
  }

  pureTest("exact derived-parameter arithmetic rejects nonpositive input and Long overflow") {
    expect.all(
      Try(NakamotoConfig.etaRotationSnapshotsFromK1(0L)).isFailure,
      Try(NakamotoConfig.sWindowFromEtaRotation(0L)).isFailure,
      Try(NakamotoConfig.etaRotationSnapshotsFromK1(Long.MaxValue)).failed.toOption.exists(_.isInstanceOf[ArithmeticException])
    )
  }

  pureTest("ldd snowplow parses EXACT fractions from HOCON (baseline=1/20, amplitude=1/2, γ=16) — no Double round-trip") {
    // The whole point of the `fromDoubles` removal: `"1/20"` decodes to EXACTLY `Ratio(1, 20)`, not the garbage
    // 18-digit-denominator rational you get from round-tripping the Double `0.05` through `Ratio(double, 18)`. This
    // loads `application.conf`'s `nakamoto.ldd` block through the `ConfigReader[Ratio]`, so it guards the parse end-to-end.
    expect.all(
      nakamoto.ldd.cutoff == 16,
      nakamoto.ldd.offset == 1,
      nakamoto.ldd.baseline == Ratio(1, 20),
      nakamoto.ldd.amplitude == Ratio(1, 2)
    )
  }
}
