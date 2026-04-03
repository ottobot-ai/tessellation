package io.constellationnetwork.dag.l0

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.BuildInfo
import io.constellationnetwork.dag.l0.cli.method._
import io.constellationnetwork.dag.l0.modules._
import io.constellationnetwork.ext.kryo._
import io.constellationnetwork.node.shared.app.{DagL0, NodeShared, TessellationIOApp}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarConfig
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{NakamotoConsensusDriver, SidecarClient}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.cluster.ClusterId
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.SlotCertificate
import io.constellationnetwork.schema.semver.TessellationVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.vrf.{EcVrf25519, VrfKeyDeriver}

import com.monovore.decline.Opts
import eu.timepit.refined.auto._
import fs2.Stream
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** dag-l0 with Nakamoto/VRF slot-based consensus. */
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
      implicit0(logger: SelfAwareStructuredLogger[IO]) <- Resource.eval(Slf4jLogger.create[IO])

      _ <- Resource.eval(logger.info("═══════════════════════════════════════════════"))
      _ <- Resource.eval(logger.info("  NAKAMOTO CONSENSUS PoC"))
      _ <- Resource.eval(logger.info(s"  Node: $nodeId"))
      _ <- Resource.eval(logger.info("═══════════════════════════════════════════════"))

      // Extract raw secp256k1 private key bytes, derive VRF Ed25519 seed
      rawPrivKey = keyPair.getPrivate match {
        case ecKey: java.security.interfaces.ECPrivateKey =>
          val bytes = ecKey.getS.toByteArray
          // BigInteger may prepend a zero byte for sign; strip if >32 bytes
          if (bytes.length > 32) bytes.drop(bytes.length - 32) else bytes
        case other =>
          // Fallback: use encoded form
          other.getEncoded.takeRight(32)
      }
      vrfSeed = VrfKeyDeriver.deriveVrfSeed(rawPrivKey)
      vrf = new EcVrf25519()
      vrfPK = vrf.getVerificationKey(vrfSeed)

      _ <- Resource.eval(
        logger.info(s"VRF pubkey: ${vrfPK.take(8).map("%02x".format(_)).mkString}...")
      )

      // All nodes must share genesis time — env var or current time for PoC
      genesisTimeMs = sys.env.getOrElse("NAKAMOTO_GENESIS_MS", System.currentTimeMillis().toString).toLong

      genesisEta = java.security.MessageDigest
        .getInstance("SHA-256")
        .digest("nakamoto-poc-genesis".getBytes("UTF-8"))

      stateRef <- Resource.eval(
        Ref.of[IO, NakamotoConsensusDriver.DriverState](
          NakamotoConsensusDriver.DriverState.initial(0L, Hash.empty, genesisEta)
        )
      )

      driverConfig = NakamotoConsensusDriver.DriverConfig(
        slotDurationMs = 1000L,
        genesisTimeMs = genesisTimeMs,
        ldd = LddConfig.Default,
        slotsPerEpoch = 60L
      )

      // Sidecar gRPC client — connect to the Go libp2p sidecar
      sidecarConfig = SidecarConfig(
        host = sys.env.getOrElse("SIDECAR_HOST", "127.0.0.1"),
        grpcPort = sys.env.getOrElse("SIDECAR_GRPC_PORT", "50051").toInt
      )
      sidecarClient <- SidecarClient.makeResource[IO](sidecarConfig)

      onSlotWon = (slot: Long, cert: SlotCertificate) =>
        for {
          _ <- logger.info(s"📦 SNAPSHOT slot=$slot proof=${cert.vrfProof.value.value.take(16)}...")
          // Publish via gRPC to sidecar for GossipSub broadcast
          _ <- sidecarClient
            .publishSnapshot(
              SidecarClient.mkSnapshot(
                hash = Array.emptyByteArray, // TODO: real snapshot hash
                slot = slot,
                ordinal = 0L, // TODO: real ordinal from snapshot storage
                parentHash = Array.emptyByteArray,
                vrfProof = cert.vrfProof.toBytes,
                vrfPublicKey = cert.vrfPublicKey.toBytes,
                eta = genesisEta,
                payload = Array.emptyByteArray, // TODO: serialized snapshot
                producerId = nodeId.value.value.getBytes("UTF-8")
              )
            )
            .void
            .handleErrorWith(e => logger.warn(s"Sidecar publish failed (sidecar down?): ${e.getMessage}"))
        } yield ()

      slotStream = NakamotoConsensusDriver.slotLoop[IO](
        selfId = nodeId,
        vrfSecretKey = vrfSeed,
        vrfPublicKey = vrfPK,
        stateRef = stateRef,
        config = driverConfig,
        stakeWeight = IO.pure(BigDecimal(1.0)),
        onSlotWon = onSlotWon
      )

      _ <- slotStream.compile.drain.background

      _ <- Resource.eval(
        IO.sleep(3.seconds) >>
          stateRef.update(_.copy(isActive = true)) >>
          logger.info("🚀 Slot loop ACTIVE")
      )

      // Periodic status every 30s
      _ <- Stream
        .awakeEvery[IO](30.seconds)
        .evalMap(_ =>
          stateRef.get.flatMap(s =>
            logger.info(
              s"📊 slot=${s.currentSlot} produced=${s.totalProduced} finalized=${s.totalFinalized}"
            )
          )
        )
        .compile
        .drain
        .background

      _ <- Resource.eval(IO.never[Unit])
    } yield ()
  }
}
