package io.constellationnetwork.tools

import java.nio.file.{Path => JPath}
import java.security.KeyPair

import cats.Applicative
import cats.data.NonEmptyList
import cats.effect._
import cats.effect.std.{Console, Random}
import cats.syntax.all._

import scala.concurrent.duration._
import scala.math.Integral.Implicits._

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.keytool.KeyStoreUtils
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.genesis.types.GenesisCSVAccount
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.GlobalSnapshotInfoLocalFileSystemStorage
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.shared.sharedKryoRegistrar
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import io.constellationnetwork.tools.TransactionGenerator._
import io.constellationnetwork.tools.cli.method._

import com.comcast.ip4s.{Host, Port}
import com.monovore.decline._
import com.monovore.decline.effect._
import eu.timepit.refined.auto._
import eu.timepit.refined.cats._
import eu.timepit.refined.types.numeric._
import fs2.data.csv._
import fs2.data.csv.generic.semiauto.deriveRowEncoder
import fs2.io.file.{Files, Path, WalkOptions}
import fs2.{Pipe, Stream, text}
import org.http4s._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
object Main
    extends CommandIOApp(
      name = "",
      header = "Constellation Tools",
      version = BuildInfo.version
    ) {

  /** Continuously sends transactions to a cluster
    *
    * @example
    *   {{{ send-transactions localhost:9100
    * --loadWallets kubernetes/data/genesis-keys/ }}}
    *
    * @example
    *   {{{ send-transactions localhost:9100
    * --generateWallets 100
    * --genesisPath genesis.csv }}}
    *
    * @return
    */
  override def main: Opts[IO[ExitCode]] =
    cli.method.opts.map { method =>
      SecurityProvider.forAsync[IO].use { implicit sp =>
        KryoSerializer.forAsync[IO](sharedKryoRegistrar).use { implicit kryo =>
          method match {
            case TxSenderCmd(configPath) =>
              val hasher: Hasher[IO] = Hasher.forKryo[IO]
              TransactionSender.run(configPath)(hasher, sp).as(ExitCode.Success)
            case _ =>
              JsonSerializer.forAsync[IO].asResource.use { implicit jsonSerializer =>
                implicit val hasher = Hasher.forJson[IO]
                EmberClientBuilder.default[IO].build.use { client =>
                  Random.scalaUtilRandom[IO].flatMap { implicit random =>
                    (method match {
                      case SendTransactionsCmd(basicOpts, walletsOpts) =>
                        walletsOpts match {
                          case w: GeneratedWallets => sendTxsUsingGeneratedWallets(client, basicOpts, w)
                          case l: LoadedWallets    => sendTxsUsingLoadedWallets(client, basicOpts, l)
                        }
                      case SendStateChannelSnapshotCmd(baseUrl) =>
                        sendStateChannelSnapshot(client, baseUrl)
                      case GetLatestSnapshotInfoCmd(networkHost, networkPort) =>
                        getLatestSnapshotInfo(client, networkHost, networkPort)
                      case g: GenerateGenesisCmd =>
                        generateGenesis[IO](g)
                      case _ => IO.raiseError(new Throwable("Not implemented"))
                    }).as(ExitCode.Success)
                  }
                }
              }
          }
        }
      }
    }

  def getLatestSnapshotInfo[F[+_]: Async: KryoSerializer: JsonSerializer: Console](
    client: Client[F],
    networkHost: Host,
    networkPort: Port
  ): F[Unit] = {
    def getCombined =
      client.expect[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)](
        s"http://$networkHost:$networkPort/global-snapshots/latest/combined"
      )

    getCombined.flatMap {
      case (snapshot, info) =>
        console.green(s"Fetched combined snapshot at ordinal=${snapshot.ordinal.show}") >>
          GlobalSnapshotInfoLocalFileSystemStorage.make[F](Path("./snapshot_info")).flatMap { infoStorage =>
            infoStorage.write(snapshot.ordinal, info) >> console.green(s"SnapshotInfo has been written on disk")
          }
    }.handleErrorWith { err =>
      console.red(s"Error fetching or writing snapshot info on disk: ${err.getMessage()}")
    }
  }

  def sendStateChannelSnapshot[F[_]: Async: Hasher: SecurityProvider: Console](
    client: Client[F],
    baseUrl: UrlString
  ): F[Unit] =
    for {
      key <- generateKeys(1).map(_.head)
      address = key.getPublic.toAddress
      _ <- console.green(s"Generated address: $address")
      snapshot = StateChannelSnapshotBinary(Hash.empty, "test".getBytes, SnapshotFee.MinValue)
      signedSnapshot <- Signed.forAsyncHasher(snapshot, key)
      hashed <- signedSnapshot.toHashed
      _ <- console.green(s"Snapshot hash: ${hashed.hash.show}, proofs hash: ${hashed.proofsHash.show}")
      _ <- postStateChannelSnapshot(client, baseUrl)(signedSnapshot, address)
    } yield ()

  def sendTxsUsingGeneratedWallets[F[_]: Async: Random: Hasher: SecurityProvider: Console](
    client: Client[F],
    basicOpts: BasicOpts,
    walletsOpts: GeneratedWallets
  ): F[Unit] =
    console.green(s"Configuration: ") >>
      console.yellow(s"Wallets: ${walletsOpts.count.show}") >>
      console.yellow(s"Genesis path: ${walletsOpts.genesisPath.toString.show}") >>
      generateKeys[F](PosInt.unsafeFrom(walletsOpts.count)).flatMap { keys =>
        createGenesis(walletsOpts.genesisPath, keys) >>
          console.cyan("Genesis created. Please start the network and continue... [ENTER]") >>
          Console[F].readLine >>
          sendTransactions(client, basicOpts, keys.map(AddressParams(_)))
            .flatMap(_ => Async[F].sleep(10.seconds))
            .flatMap(_ => checkLastReferences(client, basicOpts.baseUrl, keys.map(_.getPublic.toAddress)))
      }

  def sendTxsUsingLoadedWallets[F[_]: Async: Random: Hasher: SecurityProvider: Console](
    client: Client[F],
    basicOpts: BasicOpts,
    walletsOpts: LoadedWallets
  ): F[Unit] =
    for {
      keys <- loadKeys(walletsOpts).map(NonEmptyList.fromList).flatMap {
        Async[F].fromOption(_, new Throwable("Keys not found"))
      }
      _ <- console.green(s"Loaded ${keys.size} keys")
      addressParams <- keys.traverse { key =>
        getLastReference(client, basicOpts.baseUrl)(key.getPublic.toAddress)
          .map(lastTxRef => AddressParams(key, lastTxRef))
      }
      _ <- sendTransactions(client, basicOpts, addressParams)
      _ <- checkLastReferences(client, basicOpts.baseUrl, addressParams.map(_.address))
    } yield ()

  def sendTransactions[F[_]: Async: Random: Hasher: SecurityProvider: Console](
    client: Client[F],
    basicOpts: BasicOpts,
    addressParams: NonEmptyList[AddressParams]
  ): F[Unit] =
    Clock[F].monotonic.flatMap { startTime =>
      Ref.of(0L).flatMap { counterR =>
        val printProgressApplied = counterR.get.flatMap(printProgress(startTime, _))
        val progressPrinter = Stream
          .awakeEvery(1.seconds)
          .evalMap(_ => printProgressApplied)

        infiniteTransactionStream(basicOpts.chunkSize, basicOpts.fee, addressParams)
          .flatTap(tx =>
            Stream.retry(
              postTransaction(client, basicOpts.baseUrl)(tx)
                .handleErrorWith(e => console.red(e.show) >> e.raiseError[F, Unit]),
              0.5.seconds,
              d => (d * 1.25).asInstanceOf[FiniteDuration],
              basicOpts.retryAttempts
            )
          )
          .through(applyLimit(basicOpts.take))
          .through(applyDelay(basicOpts.delay))
          .evalTap(printTx[F](basicOpts.verbose))
          .evalMap(_ => counterR.update(_ |+| 1L))
          .handleErrorWith(e => Stream.eval(console.red(e.show)))
          .mergeHaltL(progressPrinter)
          .append(Stream.eval(printProgressApplied))
          .compile
          .drain
      }
    }

  def applyLimit[F[_], A](maybeLimit: Option[PosLong]): Pipe[F, A, A] =
    in => maybeLimit.map(in.take(_)).getOrElse(in)

  def applyDelay[F[_]: Temporal, A](delay: Option[FiniteDuration]): Pipe[F, A, A] =
    in => delay.map(in.spaced(_)).getOrElse(in)

  def loadKeys[F[_]: Async: SecurityProvider](opts: LoadedWallets): F[List[KeyPair]] =
    Files
      .forAsync[F]
      .walk(Path.fromNioPath(opts.walletsPath), WalkOptions.Default.withMaxDepth(1).withFollowLinks(false))
      .filter(_.extName === ".p12")
      .evalMap { keyFile =>
        KeyStoreUtils.readKeyPairFromStore(
          keyFile.toString,
          opts.alias,
          opts.password.toCharArray,
          opts.password.toCharArray
        )
      }
      .compile
      .toList

  def generateKeys[F[_]: Async: SecurityProvider](wallets: PosInt): F[NonEmptyList[KeyPair]] =
    NonEmptyList.fromListUnsafe((1 to wallets).toList).traverse { _ =>
      KeyPairGenerator.makeKeyPair[F]
    }

  def createGenesis[F[_]: Async](genesisPath: JPath, keys: NonEmptyList[KeyPair]): F[Unit] = {
    implicit val encoder: RowEncoder[GenesisCSVAccount] = deriveRowEncoder

    Stream
      .emits[F, KeyPair](keys.toList)
      .map(_.getPublic.toAddress)
      .map(_.value.toString)
      .map(GenesisCSVAccount(_, 100000000L))
      .through(encodeWithoutHeaders[GenesisCSVAccount]())
      .through(text.utf8.encode)
      .through(Files.forAsync[F].writeAll(Path.fromNioPath(genesisPath)))
      .compile
      .drain
  }

  /** Tier-1 test-vector emit. Parallel to `createGenesis` (which emits the legacy CSV format) but targets the `L0GenesisData` schema. See
    * `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.1 and `project_test_vector_pattern` memory.
    *
    * Determinism: the emitted `l0-genesis.json` is byte-deterministic for a given (seed, flag-set). Verified by running the generator twice
    * and `diff -r`-ing the outputs (see commit message).
    */
  def generateGenesis[F[_]: Async: SecurityProvider: Console](cmd: cli.method.GenerateGenesisCmd): F[Unit] = {
    import io.constellationnetwork.tools.genesis.GenesisGenerator
    import io.circe.Printer

    val opts = GenesisGenerator.GeneratorOpts(
      outputDir = cmd.outputDir.toString,
      numOperators = cmd.numOperators,
      stakeDistribution =
        if (cmd.stakeDistribution.nonEmpty) cmd.stakeDistribution
        else List.fill(cmd.numOperators)(BigDecimal(1) / BigDecimal(cmd.numOperators)),
      stakeBudgetDatum = cmd.stakeBudgetDatum,
      collateralPerOperator = cmd.collateralPerOperator,
      initialBalancesCsv = cmd.initialBalancesCsv.map(_.toString),
      seed = cmd.seed,
      keysFromDir = cmd.keysFromDir.map(_.toString),
      networkMagic = cmd.networkMagic,
      startingEpochProgress = cmd.startingEpochProgress
    )

    val invocation = s"generate-genesis --num-operators ${cmd.numOperators} --seed ${cmd.seed}" +
      (if (cmd.stakeDistribution.nonEmpty) s" --stake-distribution ${cmd.stakeDistribution.mkString(",")}" else "") +
      s" --stake-budget-datum ${cmd.stakeBudgetDatum}" +
      s" --collateral-per-operator ${cmd.collateralPerOperator}" +
      s" --network-magic ${cmd.networkMagic}"

    // Canonical printer: sort keys, keep nulls (vrfPublicKey/kesPublicKey are intentionally null
    // in Tier-1), 2-space indent (human-reviewable diffs). `Printer.spaces2` defaults sortKeys=false,
    // so we override it via .copy(sortKeys = true) — sortKeys is what makes the output
    // byte-deterministic across JVMs (circe HashMap iteration order is not stable).
    val printer = Printer.spaces2.copy(sortKeys = true, dropNullValues = false)

    for {
      outputs <- GenesisGenerator.generate[F](opts, invocation)
      _ <- Async[F].blocking {
        java.nio.file.Files.createDirectories(cmd.outputDir)
      }
      l0Json = printer.print(io.circe.syntax.EncoderOps(outputs.l0Genesis).asJson)
      _ <- Async[F].blocking {
        java.nio.file.Files.writeString(cmd.outputDir.resolve("l0-genesis.json"), l0Json)
      }
      _ <- outputs.cl1Genesis.traverse_ { cl1 =>
        Async[F].blocking {
          val cl1Json = printer.print(io.circe.syntax.EncoderOps(cl1).asJson)
          java.nio.file.Files.writeString(cmd.outputDir.resolve("cl1-genesis.json"), cl1Json)
        }
      }
      // §1.2 Slice 3b: persist per-operator KES SK blobs to <outputDir>/keys/operator-<N>/kes-sk.bin
      // for the gl0 container to mount + load at startup (Slice 3d / Slice 4). The public KES
      // registration certs live in the L0 genesis JSON itself.
      skPaths <- GenesisGenerator.writeKesSecretKeys[F](cmd.outputDir.toString, outputs.kesSecretKeys)
      _ <- console.green[F](s"Wrote l0-genesis.json (${l0Json.length} bytes) to ${cmd.outputDir}")
      _ <- console.green[F](s"  operators: ${outputs.l0Genesis.operators.size}")
      _ <- console.green[F](s"  delegatedStakes: ${outputs.l0Genesis.delegatedStakes.size}")
      _ <- console.green[F](s"  nodeCollaterals: ${outputs.l0Genesis.nodeCollaterals.size}")
      _ <- console.green[F](s"  initialBalances: ${outputs.l0Genesis.initialBalances.size}")
      _ <- console.green[F](s"  kesRegistrations: ${outputs.l0Genesis.kesRegistrations.map(_.size).getOrElse(0)}")
      _ <- console.green[F](s"  kesSecretKeys written: ${skPaths.size} (under ${cmd.outputDir}/keys/operator-*)")
    } yield ()
  }

  def printProgress[F[_]: Async: Console](startTime: FiniteDuration, counter: Long): F[Unit] =
    Clock[F].monotonic.flatMap { currentTime =>
      val (minutes, seconds) = (currentTime - startTime).toSeconds /% 60
      console.green(s"$counter transactions sent in ${minutes}m ${seconds}s")
    }

  def printTx[F[_]: Applicative: Console](verbose: Boolean)(tx: Signed[Transaction]): F[Unit] =
    Applicative[F].whenA(verbose) {
      console.cyan(s"Transaction sent ordinal=${tx.ordinal} sourceAddress=${tx.source}")
    }

  def postStateChannelSnapshot[F[_]: Async](
    client: Client[F],
    baseUrl: UrlString
  )(snapshot: Signed[StateChannelSnapshotBinary], address: Address): F[Unit] = {
    val target = Uri.unsafeFromString(baseUrl.toString).addPath(s"state-channels/${address.value.value}/snapshot")
    val req = Request[F](method = Method.POST, uri = target).withEntity(snapshot)

    client.successful(req).void
  }

  def postTransaction[F[_]: Async](client: Client[F], baseUrl: UrlString)(
    tx: Signed[Transaction]
  ): F[Unit] = {
    val target = Uri.unsafeFromString(baseUrl.toString).addPath("transactions")
    val req = Request[F](method = Method.POST, uri = target).withEntity(tx)

    client
      .successful(req)
      .void
  }

  def checkLastReferences[F[_]: Async: Console](
    client: Client[F],
    baseUrl: UrlString,
    addresses: NonEmptyList[Address]
  ): F[Unit] =
    addresses.traverse(checkLastReference(client, baseUrl)).void

  def checkLastReference[F[_]: Async: Console](client: Client[F], baseUrl: UrlString)(address: Address): F[Unit] =
    getLastReference(client, baseUrl)(address).flatMap { reference =>
      console.green(s"Reference for address: ${address} is ${reference.show}")
    }

  def getLastReference[F[_]: Async](client: Client[F], baseUrl: UrlString)(
    address: Address
  ): F[TransactionReference] = {
    val target = Uri.unsafeFromString(baseUrl.toString).addPath(s"transactions/last-reference/${address.value.value}")
    val req = Request[F](method = Method.GET, uri = target)

    client.expect[TransactionReference](req)
  }

  object console {
    def red[F[_]: Console](t: String) = Console[F].println(s"${scala.Console.RED}${t}${scala.Console.RESET}")
    def cyan[F[_]: Console](t: String) = Console[F].println(s"${scala.Console.CYAN}${t}${scala.Console.RESET}")
    def green[F[_]: Console](t: String) = Console[F].println(s"${scala.Console.GREEN}${t}${scala.Console.RESET}")
    def yellow[F[_]: Console](t: String) = Console[F].println(s"${scala.Console.YELLOW}${t}${scala.Console.RESET}")
  }
}
