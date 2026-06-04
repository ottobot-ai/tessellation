package io.constellationnetwork.node.shared.config

import io.constellationnetwork.node.shared.config.types._

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import weaver.SimpleIOSuite

/** Round-trip + invariant tests for [[CommitteeConfig]] — the committee draw/quorum decouple (`nakamoto.committee.{k-draw,k-quorum}`),
  * cluster-wide and shared by the per-metagraph committee gate AND the per-shard committee.
  *
  *   - The `application.conf` defaults decode to the 8-node testnet pair (`kDraw = 8`, `kQuorum = 6`).
  *   - HOCON overrides (mirroring the `${?NAKAMOTO_COMMITTEE_K_*}` env substitutions) decode through the same path.
  *   - `validated` enforces the fail-fast invariant `0 < kQuorum <= kDraw` (the cluster-split footgun: a quorum larger than the draw can
  *     never be reached by the drawn committee).
  */
object CommitteeConfigSuite extends SimpleIOSuite {

  private def loadDefault: CommitteeConfig =
    ConfigSource.default.at("nakamoto.committee").loadOrThrow[CommitteeConfig]

  private def loadWithOverride(overrideHocon: String): CommitteeConfig = {
    val parsed = ConfigFactory
      .parseString(overrideHocon, ConfigParseOptions.defaults())
      .withFallback(ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults()))
      .resolve()
    ConfigSource.fromConfig(parsed).at("nakamoto.committee").loadOrThrow[CommitteeConfig]
  }

  pureTest("default config decodes to the 8-node testnet draw/quorum pair (kDraw = 8, kQuorum = 6)") {
    val cfg = loadDefault
    // kDraw = N = 8 ⇒ threshold min(8·(1/8), 1) = 1 saturates ⇒ committee = everyone ⇒ E[|committee|] = 8 ≥ kQuorum = 6.
    expect.all(
      cfg.kDraw == 8,
      cfg.kQuorum == 6,
      // The default already satisfies the invariant, so `validated` returns it unchanged.
      cfg.validated == cfg,
      // Expected committee (kDraw, since σ saturates) ≥ quorum — the P(|committee| ≥ kQuorum) = 1 guarantee at the default.
      cfg.kDraw >= cfg.kQuorum
    )
  }

  pureTest("HOCON override applies on top of defaults") {
    val cfg = loadWithOverride(
      """
        |nakamoto.committee {
        |  k-draw = 12
        |  k-quorum = 7
        |}
      """.stripMargin
    )
    expect.all(cfg.kDraw == 12, cfg.kQuorum == 7, cfg.validated == cfg)
  }

  pureTest("validated rejects kQuorum > kDraw (the cluster-split footgun)") {
    val bad = CommitteeConfig(kDraw = 4, kQuorum = 6)
    val caught =
      try { bad.validated; false }
      catch { case _: IllegalArgumentException => true }
    expect(caught)
  }

  pureTest("validated rejects non-positive kQuorum") {
    val zeroQuorum = CommitteeConfig(kDraw = 8, kQuorum = 0)
    val negQuorum = CommitteeConfig(kDraw = 8, kQuorum = -1)
    val caught: CommitteeConfig => Boolean = cfg =>
      try { cfg.validated; false }
      catch { case _: IllegalArgumentException => true }
    expect.all(caught(zeroQuorum), caught(negQuorum))
  }

  pureTest("validated rejects non-positive kDraw") {
    val zeroDraw = CommitteeConfig(kDraw = 0, kQuorum = 1)
    val caught =
      try { zeroDraw.validated; false }
      catch { case _: IllegalArgumentException => true }
    expect(caught)
  }

  pureTest("validated accepts kQuorum == kDraw (boundary)") {
    val boundary = CommitteeConfig(kDraw = 5, kQuorum = 5)
    expect(boundary.validated == boundary)
  }
}
