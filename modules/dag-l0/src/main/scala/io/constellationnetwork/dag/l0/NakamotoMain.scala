package io.constellationnetwork.dag.l0

import cats.effect._
import cats.syntax.all._

import io.constellationnetwork.BuildInfo
import io.constellationnetwork.dag.l0.cli.method._
import io.constellationnetwork.dag.l0.config.types._
import io.constellationnetwork.dag.l0.modules._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.node.shared.app.{DagL0, NodeShared, TessellationIOApp}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoConsensusLoop
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoConsensusLoop.LoopCommand
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.cluster.ClusterId
import io.constellationnetwork.schema.semver.TessellationVersion

import com.monovore.decline.Opts
import eu.timepit.refined.auto._
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.generic.auto._
import pureconfig.module.enumeratum._

/** Nakamoto consensus variant of dag-l0.
  *
  * This is the integration entry point for running dag-l0 with VRF/LDD
  * slot-based consensus instead of BFT facilitator rounds.
  *
  * For the PoC, this starts the slot loop alongside the existing consensus
  * infrastructure. The slot loop evaluates VRF eligibility each tick and
  * produces snapshots when the node wins.
  *
  * Usage: run with `--nakamoto` flag or set env NAKAMOTO_CONSENSUS=true
  */
object NakamotoMain
    extends TessellationIOApp[Run](
      name = "dag-l0-nakamoto",
      header = "Tessellation Node (Nakamoto Consensus PoC)",
      version = TessellationVersion.unsafeFrom(BuildInfo.version),
      clusterId = ClusterId("6d7f1d6a-213a-4148-9d45-d7200f555ecf"),
      layer = DagL0
    ) {

  val opts: Opts[Run] = cli.method.opts

  protected val configFiles: List[String] = List("dag-l0.conf")

  type KryoRegistrationIdRange = DagL0KryoRegistrationIdRange

  val kryoRegistrar: Map[Class[_], KryoRegistrationId[KryoRegistrationIdRange]] =
    dagL0KryoRegistrar

  def run(method: Run, nodeShared: NodeShared[IO, Run]): Resource[IO, Unit] = {
    import nodeShared._

    for {
      implicit0(logger: SelfAwareStructuredLogger[IO]) <- Resource.eval(
        Slf4jLogger.create[IO]
      )

      // For the PoC, we start the Nakamoto slot loop as an independent fiber.
      // It doesn't replace the BFT consensus yet — it runs alongside it,
      // logging slot evaluations. This lets us verify:
      //   1. Slot clock ticks correctly
      //   2. VRF eligibility evaluates per slot
      //   3. Sidecar connectivity works
      //   4. Attestation flow works
      //
      // Once validated, we swap the BFT consensus out entirely.

      genesisTimeMs = System.currentTimeMillis() // PoC: genesis = now

      nakamotoLoop <- Resource.eval(
        NakamotoConsensusLoop.build[IO](
          selfId = nodeId,
          keyPair = keyPair,
          startOrdinal = SnapshotOrdinal.MinValue,
          startHash = io.constellationnetwork.security.hash.Hash.empty,
          slotDurationMs = 1000L,
          genesisTimeMs = genesisTimeMs
        )
      )

      // Start the slot loop fiber
      _ <- Resource.eval(
        logger.info(s"Starting Nakamoto slot loop (genesis=$genesisTimeMs, peer=$nodeId)")
      )
      _ <- nakamotoLoop.run.compile.drain.background

      // Activate after a short delay to let the node initialize
      _ <- Resource.eval(
        Async[IO].sleep(scala.concurrent.duration.FiniteDuration(3, "seconds")) >>
          nakamotoLoop.commandQueue.offer(LoopCommand.Activate) >>
          logger.info("Nakamoto consensus loop activated")
      )

      // Keep alive until shutdown
      _ <- Resource.eval(Async[IO].never[Unit])
    } yield ()
  }
}
