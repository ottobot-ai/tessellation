package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import cats.effect.IO

import io.constellationnetwork.dag.l0.infrastructure.snapshot.schema.GlobalConsensusKind
import io.constellationnetwork.node.shared.infrastructure.consensus.declaration.{Facility, MajoritySignature, Proposal}
import io.constellationnetwork.node.shared.infrastructure.consensus.message._
import io.constellationnetwork.schema.gossip._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex

import io.circe.Json
import weaver.SimpleIOSuite

object GlobalLegacyBftIngressDisabledSuite extends SimpleIOSuite {

  private val peerId = PeerId(Hex("0d" * 64))

  private def peerRumor(contentType: ContentType): RumorRaw =
    PeerRumorRaw(peerId, Ordinal.MinValue, Json.Null, contentType)

  private val legacyRumors: List[(String, RumorRaw)] = List(
    "facility" -> peerRumor(ContentType.of[ConsensusPeerDeclaration[SnapshotOrdinal, Facility]]),
    "proposal" -> peerRumor(ContentType.of[ConsensusPeerDeclaration[SnapshotOrdinal, Proposal]]),
    "majority-signature" -> peerRumor(ContentType.of[ConsensusPeerDeclaration[SnapshotOrdinal, MajoritySignature]]),
    "declaration-ack" -> peerRumor(ContentType.of[ConsensusPeerDeclarationAck[SnapshotOrdinal, GlobalConsensusKind]]),
    "artifact" -> CommonRumorRaw(Json.Null, ContentType.of[ConsensusArtifact[SnapshotOrdinal, GlobalIncrementalSnapshot]]),
    "withdraw" -> peerRumor(ContentType.of[ConsensusWithdrawPeerDeclaration[SnapshotOrdinal, GlobalConsensusKind]])
  )

  legacyRumors.foreach {
    case (family, rumor) =>
      test(s"Nakamoto GL0 has no handler for legacy $family rumors") {
        GlobalSnapshotConsensus.disabledLegacyHandler[IO].run((rumor, peerId)).value.map { handled =>
          expect(handled.isEmpty)
        }
      }
  }

  test("the disabled GL0 lifecycle manager owns no queue and fails closed") {
    val manager = GlobalSnapshotConsensus.disabledLegacyManager[IO]

    for {
      register <- manager.registerForConsensus(SnapshotOrdinal.MinValue).attempt
      reset <- manager.resetForRecovery.attempt
      withdraw <- manager.withdrawFromConsensus.attempt
      fieldTypes = manager.getClass.getDeclaredFields.iterator.map(_.getType.getName).toList
    } yield
      expect.all(
        register.left.exists(_.isInstanceOf[GlobalSnapshotConsensus.LegacyBftConsensusDisabled]),
        reset.left.exists(_.isInstanceOf[GlobalSnapshotConsensus.LegacyBftConsensusDisabled]),
        withdraw.left.exists(_.isInstanceOf[GlobalSnapshotConsensus.LegacyBftConsensusDisabled]),
        !fieldTypes.exists(_.contains("cats.effect.std.Queue"))
      )
  }

  test("GL0 starts bounded rumor consumption without registering the generic BFT engine") {
    readSources.map {
      case (main, consensus, currencyConsensus, currencyMain, oldHandlerExists) =>
        val ingress = sliceBetween(main, "rumorHandler =", "forkRecoveryService =")
        val bootstrapStart = main.indexOf("// Unified Nakamoto bootstrap")
        val bootstrapEnd = main.indexOf("}).asResource", bootstrapStart)
        val daemonsStart = main.indexOf("Daemons\n        .startNakamoto", bootstrapEnd)
        val inputRelease = main.indexOf("consensusInputGateControl.releaseInput", daemonsStart)
        val subscriptionAck = main.indexOf("services.sidecarSubscriptionReadiness.awaitBothAndRun", inputRelease)
        val readyHandoff = main.indexOf("storages.node.setNodeState(NodeState.Ready)", subscriptionAck)
        val publicServer = main.indexOf("MkHttpServer[IO].newEmber(ServerName(\"public\")", daemonsStart)
        val p2pServer = main.indexOf("MkHttpServer[IO].newEmber(ServerName(\"p2p\")", publicServer)
        val cliServer = main.indexOf("MkHttpServer[IO].newEmber(ServerName(\"cli\")", p2pServer)

        expect.all(
          ingress.contains("eventRumorHandler"),
          !ingress.contains("services.consensus.handler"),
          main.sliding("gossipDaemon.startAsInitialValidator".length).count(_ == "gossipDaemon.startAsInitialValidator") == 1,
          main.contains("consensusInputGateControl.awaitChainSeed"),
          main.contains("consensusInputGateControl.markLocalStateReady"),
          bootstrapStart >= 0,
          bootstrapEnd > bootstrapStart,
          daemonsStart > bootstrapEnd,
          publicServer > daemonsStart,
          p2pServer > publicServer,
          cliServer > p2pServer,
          inputRelease > cliServer,
          subscriptionAck > inputRelease,
          readyHandoff > subscriptionAck,
          consensus.sliding("consensusInputGate.awaitBootstrap".length).count(_ == "consensusInputGate.awaitBootstrap") >= 2,
          consensus.contains("chainSeedGate = chainSeedGate"),
          !consensus.contains("ConsensusEventLoop"),
          !consensus.contains("Queue.unbounded"),
          !consensus.contains("GlobalConsensusHandler"),
          !consensus.contains("ConsensusManager.make"),
          !consensus.contains("ConsensusDirectSender"),
          !consensus.contains("loop.queue"),
          !consensus.contains("loop.manager"),
          !oldHandlerExists,
          currencyConsensus.contains("ConsensusEventLoop.build["),
          currencyMain.contains("services.consensus.handler")
        )
    }
  }

  private def readSources: IO[(String, String, String, String, Boolean)] =
    IO.blocking {
      val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())

      def read(relative: String): String =
        new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8)

      val oldHandler =
        root.resolve(
          "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalConsensusHandler.scala"
        )

      (
        read("modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala"),
        read(
          "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala"
        ),
        read(
          "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensus.scala"
        ),
        read("modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/CurrencyL0App.scala"),
        Files.exists(oldHandler)
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
}
