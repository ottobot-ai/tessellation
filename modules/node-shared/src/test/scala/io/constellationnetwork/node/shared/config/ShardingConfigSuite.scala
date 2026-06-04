package io.constellationnetwork.node.shared.config

import io.constellationnetwork.node.shared.config.types._

import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import weaver.SimpleIOSuite

/** Round-trip tests for [[ShardingConfig]] — the hierarchical-shard-checkpoints v1 typed config shape (see
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §4.2).
  *
  *   - The defaults declared in `application.conf` under `nakamoto.sharding` decode to a degenerate single-shard config (`numShards = 1`) —
  *     the new code path is a no-op for current setups.
  *   - Overrides supplied via HOCON (mirroring what the `${?NAKAMOTO_*}` env substitutions would yield) decode through the same path. We
  *     use HOCON-string overrides instead of mutating real env vars because the JVM cannot mutate its own `System.getenv()`; the
  *     `${?ENV_VAR}` substitution itself is well-tested by typesafe-config and is not re-verified here.
  */
object ShardingConfigSuite extends SimpleIOSuite {

  /** Load the `nakamoto.sharding` subtree from the packaged `application.conf` defaults. */
  private def loadDefault: ShardingConfig =
    ConfigSource.default.at("nakamoto.sharding").loadOrThrow[ShardingConfig]

  /** Build a `ShardingConfig` by parsing an inline HOCON string that overrides `nakamoto.sharding.*` keys. The fallback chain mirrors what
    * the application would see at runtime: caller-provided HOCON > `application.conf` defaults.
    */
  private def loadWithOverride(overrideHocon: String): ShardingConfig = {
    val parsed = ConfigFactory
      .parseString(overrideHocon, ConfigParseOptions.defaults())
      .withFallback(ConfigFactory.load(ConfigParseOptions.defaults(), ConfigResolveOptions.defaults()))
      .resolve()
    ConfigSource.fromConfig(parsed).at("nakamoto.sharding").loadOrThrow[ShardingConfig]
  }

  pureTest("default config decodes to single-shard degenerate values") {
    val cfg = loadDefault
    expect.all(
      cfg.numShards == 1,
      cfg.finality.k1Shard == 8L,
      cfg.checkpoint.tAliveMs == 30000L,
      cfg.checkpoint.tBurst == 64,
      // EXECUTION-SHARDING R-1: default per-shard raw-binary buffer cap.
      cfg.checkpoint.binaryBufferCap == 4096,
      // Slice 19: default partition-hard threshold is 10 minutes.
      cfg.observability.tPartitionHardMs == 600000L,
      // Slice 17: default non-participation slashing — 33% missed-rate ceiling, min 5 duties before evaluating.
      cfg.slashing.maxMissedPctPerEpoch == 33,
      cfg.slashing.minDenominatorPerEpoch == 5L
    )
  }

  pureTest("HOCON override applies on top of defaults") {
    val cfg = loadWithOverride(
      """
        |nakamoto.sharding {
        |  num-shards = 4
        |  finality { k1-shard = 16 }
        |  checkpoint { t-alive-ms = 45000, t-burst = 128, binary-buffer-cap = 8192 }
        |  observability { t-partition-hard-ms = 90000 }
        |  slashing { max-missed-pct-per-epoch = 50, min-denominator-per-epoch = 10 }
        |}
      """.stripMargin
    )
    expect.all(
      cfg.numShards == 4,
      cfg.finality.k1Shard == 16L,
      cfg.checkpoint.tAliveMs == 45000L,
      cfg.checkpoint.tBurst == 128,
      cfg.checkpoint.binaryBufferCap == 8192,
      cfg.observability.tPartitionHardMs == 90000L,
      cfg.slashing.maxMissedPctPerEpoch == 50,
      cfg.slashing.minDenominatorPerEpoch == 10L
    )
  }

  pureTest("partial HOCON override leaves untouched keys at their defaults") {
    val cfg = loadWithOverride(
      """
        |nakamoto.sharding {
        |  num-shards = 2
        |}
      """.stripMargin
    )
    expect.all(
      cfg.numShards == 2,
      cfg.finality.k1Shard == 8L,
      cfg.checkpoint.tAliveMs == 30000L,
      cfg.checkpoint.tBurst == 64,
      cfg.checkpoint.binaryBufferCap == 4096,
      cfg.observability.tPartitionHardMs == 600000L,
      cfg.slashing.maxMissedPctPerEpoch == 33,
      cfg.slashing.minDenominatorPerEpoch == 5L
    )
  }
}
