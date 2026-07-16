package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.lang.reflect.{InvocationHandler, Proxy}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import io.constellationnetwork.dag.l0.config.types.AppConfigReader
import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.ext.pureconfig._
import io.constellationnetwork.node.shared.http.routes.DebugRoutes

import com.typesafe.config.ConfigFactory
import eu.timepit.refined.pureconfig._
import org.http4s.{Method, Request, Uri}
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._
import weaver.SimpleIOSuite

object GlobalLegacyBftIngressDisabledSuite extends SimpleIOSuite {

  private val retiredGl0BftSources = List(
    "GlobalSnapshotConsensusStateCreator.scala",
    "GlobalSnapshotConsensusStateAdvancer.scala",
    "GlobalSnapshotConsensusStateRemover.scala",
    "GlobalSnapshotConsensusOps.scala",
    "schema.scala"
  )

  test("GL0 runtime exposes no generic BFT storage, routes, manager, or result") {
    readSources.map {
      case Sources(
            main,
            consensus,
            services,
            httpApi,
            download,
            daemons,
            publisher,
            packageObject,
            debugRoutes,
            currencyConsensus,
            currencyHttp,
            currencyMain
          ) =>
        val ingress = sliceBetween(main, "rumorHandler =", "forkRecoveryService =")

        expect.all(
          !ingress.contains("services.consensus.handler"),
          !consensus.contains("ConsensusStorage"),
          !consensus.contains("ConsensusRoutes"),
          !consensus.contains("ConsensusManager"),
          !consensus.contains("new Consensus("),
          !consensus.contains("LegacyBftConsensusDisabled"),
          !services.contains("val consensus:"),
          !httpApi.contains("ConsensusInfoRoutes"),
          !httpApi.contains("services.consensus"),
          httpApi.contains("DebugRoutes[F]("),
          httpApi.contains("None,"),
          !download.contains("consensus.manager"),
          !daemons.contains("ConsensusConfig"),
          !daemons.contains("getLastConsensusOutcome"),
          publisher.contains("events.evalMap(signAndPublish"),
          !publisher.contains("EventTriggerGuard"),
          !publisher.contains("ConsensusConfig"),
          !packageObject.contains("GlobalSnapshotConsensus[F[_]]"),
          !packageObject.contains("GlobalConsensusStorage"),
          !packageObject.contains("GlobalConsensusManager"),
          debugRoutes.contains("Option[SnapshotConsensus"),
          currencyConsensus.contains("ConsensusEventLoop.build["),
          currencyHttp.contains("ConsensusInfoRoutes"),
          currencyHttp.contains("Some(services.consensus)"),
          currencyMain.contains("services.consensus.handler")
        )
    }
  }

  test("compiled GL0 services omit the generic consensus API") {
    IO.blocking {
      val gl0Methods = Class
        .forName("io.constellationnetwork.dag.l0.modules.Services")
        .getMethods
        .iterator
        .map(_.getName)
        .toSet
      expect(!gl0Methods.contains("consensus"))
    }
  }

  test("GL0 debug routing does not expose generic consensus endpoints") {
    val routes = DebugRoutes[IO](
      unreachable(classOf[ClusterStorage[IO]]),
      None,
      unreachable(classOf[Gossip[IO]]),
      unreachable(classOf[Session[IO]])
    ).publicRoutes

    routes
      .run(Request[IO](method = Method.GET, uri = Uri.unsafeFromString("/debug/consensus/1/resources")))
      .value
      .map(result => expect(result.isEmpty))
  }

  test("GL0 config has no BFT consensus subtree and retains proposal bounds") {
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val config = ConfigFactory.parseFile(root.resolve("modules/dag-l0/src/main/resources/dag-l0.conf").toFile).resolve()
      val startupConfig = ConfigSource.resources("dag-l0.conf").withFallback(ConfigSource.default).load[AppConfigReader]

      expect.all(
        !config.hasPath("snapshot.consensus"),
        config.getInt("snapshot.event-cutter.max-binary-size-bytes") == 20971520,
        config.getInt("snapshot.event-cutter.max-update-node-parameters-size") == 100,
        startupConfig.exists(_.snapshot.eventCutter.maxBinarySizeBytes.value == 20971520),
        startupConfig.exists(_.snapshot.eventCutter.maxUpdateNodeParametersSize.value == 100)
      )
    }
  }

  test("retired GL0 BFT state sources are absent and CurrencyL0 state sources remain") {
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
      val gl0SnapshotDir = root.resolve("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot")
      val currencySnapshotDir =
        root.resolve("modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot")

      expect.all(
        retiredGl0BftSources.forall(name => Files.notExists(gl0SnapshotDir.resolve(name))),
        Files.exists(currencySnapshotDir.resolve("CurrencySnapshotConsensusStateCreator.scala")),
        Files.exists(currencySnapshotDir.resolve("CurrencySnapshotConsensusStateAdvancer.scala")),
        Files.exists(currencySnapshotDir.resolve("CurrencySnapshotConsensusStateRemover.scala")),
        Files.exists(currencySnapshotDir.resolve("CurrencySnapshotConsensusOps.scala")),
        Files.exists(currencySnapshotDir.resolve("schema.scala"))
      )
    }
  }

  private final case class Sources(
    main: String,
    consensus: String,
    services: String,
    httpApi: String,
    download: String,
    daemons: String,
    publisher: String,
    packageObject: String,
    debugRoutes: String,
    currencyConsensus: String,
    currencyHttp: String,
    currencyMain: String
  )

  private def readSources: IO[Sources] = IO.blocking {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())

    def read(relative: String): String =
      new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8)

    Sources(
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala"),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala"),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/Services.scala"),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/HttpApi.scala"),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/snapshot/programs/Download.scala"),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/Daemons.scala"),
      read(
        "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotEventsPublisherDaemon.scala"
      ),
      read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/package.scala"),
      read("modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/routes/DebugRoutes.scala"),
      read("modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensus.scala"),
      read("modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/modules/HttpApi.scala"),
      read("modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/CurrencyL0App.scala")
    )
  }

  private def repositoryRoot(start: Path): Path =
    Iterator
      .iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.isDirectory(path.resolve("modules/dag-l0")))
      .getOrElse(throw new IllegalStateException(s"Could not locate repository root from $start"))

  private def sliceBetween(source: String, startAnchor: String, endAnchor: String): String = {
    val start = source.indexOf(startAnchor)
    require(start >= 0, s"Missing source anchor: $startAnchor")
    val end = source.indexOf(endAnchor, start)
    require(end >= 0, s"Missing source anchor after '$startAnchor': $endAnchor")
    source.substring(start, end)
  }

  private def unreachable[A](interface: Class[A]): A =
    Proxy
      .newProxyInstance(
        interface.getClassLoader,
        Array(interface),
        new InvocationHandler {
          def invoke(_proxy: AnyRef, method: java.lang.reflect.Method, _args: Array[AnyRef]): AnyRef =
            throw new AssertionError(s"GL0 debug consensus route unexpectedly invoked ${interface.getName}.${method.getName}")
        }
      )
      .asInstanceOf[A]
}
