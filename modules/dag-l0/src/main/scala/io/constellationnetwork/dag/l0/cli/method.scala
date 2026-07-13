package io.constellationnetwork.dag.l0.cli

import cats.syntax.all._

import io.constellationnetwork.dag.l0.config.types._
import io.constellationnetwork.env._
import io.constellationnetwork.env.env._
import io.constellationnetwork.ext.decline.WithOpts
import io.constellationnetwork.ext.decline.decline._
import io.constellationnetwork.node.shared.cli.opts.genesisPathOpts
import io.constellationnetwork.node.shared.cli.{CliMethod, CollateralAmountOpts}
import io.constellationnetwork.node.shared.config.MainnetRewardsConfig
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.infrastructure.statechannel.StateChannelAllowanceLists
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.security.hash.Hash

import com.monovore.decline.Opts
import com.monovore.decline.refined._
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Path

object method {

  /** Unified Nakamoto GL0 command.
    *
    * Bootstrap mode is auto-detected at startup from the flags provided:
    *
    *   1. `--rollback-hash HASH` → load a specific locally persisted snapshot. 2. Local snapshot data on disk → cold restart from the
    *      latest ordinal. 3. `--nakamoto-peer URL` → rejected until authenticated ancestry replay exists. 4. Otherwise → fresh genesis.
    *
    * Every mode currently requires the positional genesis argument to be the canonical L0 JSON containing the complete, paired operator
    * KES/VRF key anchor. A CSV, missing argument, or incomplete JSON fails before services become ready. This remains mandatory until the
    * active operator-key registry is persisted in and recoverable from rooted consensus state.
    *
    * Nakamoto tunables (LDD params, finality knobs, sidecar config, genesis time) are configured via `NAKAMOTO_*` env vars. See
    * `GlobalSnapshotConsensus.scala` for defaults.
    */
  case class RunNakamoto(
    keyStore: StorePath,
    alias: KeyAlias,
    password: Password,
    dbConfig: DBConfig,
    httpConfig: HttpConfig,
    environment: AppEnvironment,
    seedlistPath: Option[SeedListPath],
    collateralAmount: Option[Amount],
    prioritySeedlistPath: Option[SeedListPath],
    allowanceListPath: Option[AllowanceListPath],
    // Bootstrap source flags (all optional — auto-detect picks the right path)
    genesisPath: Option[Path],
    rollbackHash: Option[Hash],
    peerToJoin: Option[String],
    startingEpochProgress: EpochProgress
  ) extends CliMethod {

    def appConfig(c: AppConfigReader, shared: SharedConfig): AppConfig = AppConfig(
      rewards = MainnetRewardsConfig.classicMainnetRewardsConfig,
      snapshot = c.snapshot,
      stateChannel = c.stateChannel,
      peerDiscovery = c.peerDiscovery,
      incremental = c.incremental,
      shared = shared
    )

    val stateChannelAllowanceLists = StateChannelAllowanceLists.get(environment)

    val l0SeedlistPath = seedlistPath

  }

  object RunNakamoto extends WithOpts[RunNakamoto] {

    private val startingEpochProgressOpts: Opts[EpochProgress] = Opts
      .option[NonNegLong]("startingEpochProgress", "Set starting progress for rewarding at the specific epoch")
      .map(EpochProgress(_))
      .withDefault(EpochProgress.MinValue)

    private val genesisPathOpt: Opts[Option[Path]] =
      genesisPathOpts.map(_.some).withDefault(none)

    private val rollbackHashOpt: Opts[Option[Hash]] =
      Opts.option[Hash]("rollback-hash", "Anchor recovery: load this specific snapshot hash from disk or peer").orNone

    private val peerToJoinOpt: Opts[Option[String]] =
      Opts.option[String]("nakamoto-peer", "HTTP URL of a Nakamoto peer to download chain from (e.g. http://node-0:9000)").orNone

    val opts: Opts[RunNakamoto] = Opts.subcommand("run-nakamoto", "Run Nakamoto GL0 consensus (VRF production, attestation finality)") {
      (
        StorePath.opts,
        KeyAlias.opts,
        Password.opts,
        db.opts,
        http.opts,
        AppEnvironment.opts,
        SeedListPath.opts,
        CollateralAmountOpts.opts,
        SeedListPath.priorityOpts,
        AllowanceListPath.opts,
        genesisPathOpt,
        rollbackHashOpt,
        peerToJoinOpt,
        startingEpochProgressOpts
      ).mapN(RunNakamoto.apply)
    }
  }

  val opts: Opts[RunNakamoto] = RunNakamoto.opts
}
