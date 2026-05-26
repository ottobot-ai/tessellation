package io.constellationnetwork.tools.cli

import java.nio.file.Path

import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

import io.constellationnetwork.ext.decline.WithOpts
import io.constellationnetwork.ext.decline.decline.{coercibleArgument, _}

import com.comcast.ip4s.{Host, Port}
import com.monovore.decline.Opts
import com.monovore.decline.refined.refTypeArgument
import eu.timepit.refined.api.RefType.refinedRefType
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.numeric.GreaterEqual
import eu.timepit.refined.refineV
import eu.timepit.refined.string.Url
import eu.timepit.refined.types.numeric._

object method {

  type IntGreaterEqual2 = Int Refined GreaterEqual[2]
  type UrlString = String Refined Url

  sealed trait CliMethod

  case class BasicOpts(
    baseUrl: UrlString,
    take: Option[PosLong],
    chunkSize: PosInt,
    delay: Option[FiniteDuration],
    retryAttempts: NonNegInt,
    verbose: Boolean,
    fee: NonNegLong
  )

  case class SendTransactionsCmd(
    basicOpts: BasicOpts,
    walletsOpts: WalletsOpts
  ) extends CliMethod

  case class SendStateChannelSnapshotCmd(
    baseUrl: UrlString
  ) extends CliMethod

  case class GetLatestSnapshotInfoCmd(
    networkHost: Host,
    networkPort: Port
  ) extends CliMethod

  case class TxSenderCmd(configPath: String) extends CliMethod

  /** Shard-assignment query helper (shard-sortition workstream Slice S7 — e2e harness).
    *
    * Given a DAG address and a shard count `M`, prints the [[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment]]
    * `shardId` to stdout. Reuses the production `ShardAssignment.make[F](numShards).shardIdFor(addr)` path verbatim — the address is routed
    * through the same `Hasher[F]` (`SHA256(Brotli(circeJson(addr)))`) the consensus code uses, so there is zero risk of hash-chain drift
    * between this helper and the running cluster.
    *
    * Used by the metagraph key-grinding routine in `docker/bin/compose-runner.sh` (and its dry-run in
    * `docker/bin/grind-metagraph-shards-dryrun.sh`) to spread metagraphs evenly across shards for the N≫K_S test topology. No consensus
    * behaviour changes — this is a read-only diagnostic over existing schema.
    */
  case class ShardIdCmd(address: String, numShards: Int) extends CliMethod

  /** Batch shard-assignment scan (shard-sortition workstream Slice S7 — e2e harness dry-run).
    *
    * Mints `count` fresh Ed25519 keypairs, derives each public key's DAG address (the same `toAddress` derivation the metagraph genesis
    * operator key feeds into), and prints one `address shardId` line per keypair under
    * [[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment]] with the given shard count. Folding the keygen + shardId
    * computation into a SINGLE JVM invocation makes the bash grind dry-run fast (one process instead of `3 * candidates`). Read-only over
    * existing schema — no consensus behaviour changes.
    */
  case class ShardScanCmd(count: Int, numShards: Int) extends CliMethod

  /** Tier-1 test-vector generator. See `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.1 and `project_test_vector_pattern` for
    * the design rationale. Emits a byte-deterministic `l0-genesis.json` (operator set + delegated-stake records + node-collateral records +
    * protocol params + initial balances) into `outputDir`. Optionally synthesizes a `cl1-genesis.json` (single-metagraph for Tier-1).
    */
  case class GenerateGenesisCmd(
    outputDir: Path,
    numOperators: Int,
    stakeDistribution: List[BigDecimal],
    stakeBudgetDatum: Long,
    collateralPerOperator: Long,
    initialBalancesCsv: Option[Path],
    seed: Long,
    keysFromDir: Option[Path],
    networkMagic: String,
    startingEpochProgress: Long
  ) extends CliMethod

  sealed trait WalletsOpts
  case class GeneratedWallets(count: IntGreaterEqual2, genesisPath: Path) extends WalletsOpts
  case class LoadedWallets(walletsPath: Path, alias: String, password: String) extends WalletsOpts

  object SendTransactionsCmd extends WithOpts[SendTransactionsCmd] {
    private val basicOpts = (
      Opts.argument[String](metavar = "baseUrl").map(withProtocol).mapValidated(refineV[Url](_).toValidatedNel),
      Opts.option[PosLong]("take", "Number of transactions. Infinite if unspecified.", "t").orNone,
      Opts.option[PosInt]("chunk", "Size of a chunk, default 1.", "c").withDefault(PosInt(1)),
      Opts.option[FiniteDuration]("delay", "Delay before sending each transaction.", "d").orNone,
      Opts
        .option[NonNegInt]("retryAttempts", "Number of retry attempts to send transaction, default 10.")
        .withDefault(NonNegInt(10)),
      Opts.flag("verbose", "Print individual transactions.", "v").map(_ => true).withDefault(false),
      Opts.option[NonNegLong]("fee", "Transaction fee, default 1.", "f").withDefault(NonNegLong(1L))
    ).mapN(BasicOpts.apply)

    private val generatedWallets = (
      Opts.option[IntGreaterEqual2]("generateWallets", "Number of wallets to generate, at least 2."),
      Opts.option[Path]("genesisPath", "Specifies where genesis should be stored.")
    ).mapN(GeneratedWallets)

    private val loadedWallets = (
      Opts.option[Path]("loadWallets", "Specifies where wallets (.p12 files) will be loaded from."),
      Opts.option[String]("alias", "Universal alias for all keys, default `alias`.").withDefault("alias"),
      Opts.option[String]("password", "Universal password for all keys, default `password`.").withDefault("password")
    ).mapN(LoadedWallets.apply)

    val opts: Opts[SendTransactionsCmd] = Opts.subcommand("send-transactions", "Send sample transactions") {
      (
        basicOpts,
        generatedWallets.orElse(loadedWallets)
      ).mapN(SendTransactionsCmd.apply)
    }
  }

  object SendStateChannelSnapshotCmd extends WithOpts[SendStateChannelSnapshotCmd] {

    val opts: Opts[SendStateChannelSnapshotCmd] =
      Opts.subcommand("send-state-channel-snapshot", "Send sample state-channel snapshot") {
        Opts
          .argument[String](metavar = "baseUrl")
          .map(withProtocol)
          .mapValidated(refineV[Url](_).toValidatedNel)
          .map(SendStateChannelSnapshotCmd.apply)
      }
  }

  object GetLatestSnapshotInfoCmd {
    val opts: Opts[GetLatestSnapshotInfoCmd] =
      Opts.subcommand("get-latest-snapshot-info", "Get latest snapshot-info") {
        (
          Opts.argument[Host](metavar = "host"),
          Opts.argument[Port](metavar = "port")
        ).mapN(GetLatestSnapshotInfoCmd.apply)
      }
  }

  object TxSenderCmd {
    val opts: Opts[TxSenderCmd] =
      Opts.subcommand("tx-sender", "Send transactions from a config file to a public network") {
        Opts
          .option[String]("config", "Path to config file", "c")
          .withDefault("tx-sender.conf")
          .map(TxSenderCmd.apply)
      }
  }

  object GenerateGenesisCmd {
    private def parseWeights(s: String): List[BigDecimal] =
      if (s.trim.isEmpty) List.empty
      else
        s.split(",")
          .toList
          .map(_.trim)
          .filter(_.nonEmpty)
          .flatMap(p => scala.util.Try(BigDecimal(p)).toOption)

    val opts: Opts[GenerateGenesisCmd] = Opts.subcommand(
      "generate-genesis",
      "Generate a Tier-1 l0-genesis.json test-vector fixture"
    ) {
      (
        Opts.option[Path]("output-dir", "Directory to write l0-genesis.json (and optional cl1-genesis.json) into."),
        Opts.option[Int]("num-operators", "Number of validator operators in the genesis."),
        Opts
          .option[String](
            "stake-distribution",
            "Comma-separated relative weights summing to ≈1.0. Defaults to uniform if omitted."
          )
          .withDefault("")
          .map(parseWeights),
        Opts
          .option[Long]("stake-budget-datum", "Total stake budget in datum (1 DAG = 1e8 datum).")
          .withDefault(1000000000000L),
        Opts
          .option[Long](
            "collateral-per-operator",
            "Per-operator node-collateral in datum. Omit (or 0) to skip collateral records."
          )
          .withDefault(0L),
        Opts
          .option[Path](
            "initial-balances-csv",
            "Optional CSV (address,balance) to seed cl1 balances; if omitted, synthesizes per-operator stipends."
          )
          .orNone,
        Opts.option[Long]("seed", "Required: PRNG seed for byte-determinism."),
        Opts
          .option[Path]("keys-from", "Optional directory containing nodes/N/key.p12 to reuse existing operator keys.")
          .orNone,
        Opts.option[String]("network-magic", "Test-cluster network magic string.").withDefault("test-cluster"),
        Opts.option[Long]("starting-epoch-progress", "Initial epoch progress for the genesis.").withDefault(0L)
      ).mapN(GenerateGenesisCmd.apply)
    }
  }

  object ShardIdCmd {
    val opts: Opts[ShardIdCmd] = Opts.subcommand(
      "shard-id",
      "Print the static ShardAssignment shardId for a DAG address under a given shard count (e2e harness — Slice S7)."
    ) {
      (
        Opts.option[String]("address", "DAG address to assign (the metagraph identifier / genesis.address)."),
        Opts.option[Int]("num-shards", "Cluster-wide shard count M (must be > 0).")
      ).mapN(ShardIdCmd.apply)
    }
  }

  object ShardScanCmd {
    val opts: Opts[ShardScanCmd] = Opts.subcommand(
      "shard-scan",
      "Mint N random keypairs and print 'address shardId' for each under a shard count (e2e harness dry-run — Slice S7)."
    ) {
      (
        Opts.option[Int]("count", "Number of random keypairs/addresses to generate."),
        Opts.option[Int]("num-shards", "Cluster-wide shard count M (must be > 0).")
      ).mapN(ShardScanCmd.apply)
    }
  }

  val opts: Opts[CliMethod] =
    SendTransactionsCmd.opts
      .orElse(SendStateChannelSnapshotCmd.opts)
      .orElse(GetLatestSnapshotInfoCmd.opts)
      .orElse(TxSenderCmd.opts)
      .orElse(GenerateGenesisCmd.opts)
      .orElse(ShardIdCmd.opts)
      .orElse(ShardScanCmd.opts)

  private val defaultProtocol = "http://"

  private def withProtocol(url: String): String =
    if (url.matches("^[a-z]+://"))
      url
    else
      defaultProtocol + url
}
