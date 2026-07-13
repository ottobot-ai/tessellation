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
      cfg.retention.retainedCheckpoints == 8L,
      cfg.checkpoint.binaryBufferCap == 4096
    )
  }

  pureTest("HOCON override applies on top of defaults") {
    val cfg = loadWithOverride(
      """
        |nakamoto.sharding {
        |  num-shards = 4
        |  retention { retained-checkpoints = 16 }
        |  checkpoint { binary-buffer-cap = 8192 }
        |}
      """.stripMargin
    )
    expect.all(
      cfg.numShards == 4,
      cfg.retention.retainedCheckpoints == 16L,
      cfg.checkpoint.binaryBufferCap == 8192
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
      cfg.retention.retainedCheckpoints == 8L,
      cfg.checkpoint.binaryBufferCap == 4096
    )
  }
}
