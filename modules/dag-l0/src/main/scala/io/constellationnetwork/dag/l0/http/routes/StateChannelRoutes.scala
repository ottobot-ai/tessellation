package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.applicativeError._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.domain.statechannel.StateChannelService
import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.SnapshotBinarySenderTimeoutsConfig
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}

import eu.timepit.refined.auto._
import io.circe.shapes._
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.dsl.Http4sDsl
import org.http4s.server.middleware.Timeout
import org.http4s.{EntityDecoder, HttpRoutes}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import shapeless.HNil
import shapeless.syntax.singleton._

final case class StateChannelRoutes[F[_]: Async: Hasher: JsonSerializer](
  stateChannelService: StateChannelService[F],
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  snapshotBinarySenderTimeoutsConfig: SnapshotBinarySenderTimeoutsConfig,
  sidecarClient: SidecarClient.SidecarClientAlgebra[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {
  protected val prefixPath: InternalUrlPrefix = "/state-channels"
  implicit val decoder: EntityDecoder[F, Array[Byte]] = EntityDecoder.byteArrayDecoder[F]

  private val logger = Slf4jLogger.getLoggerFromName[F]("StateChannelRoutes")

  // After a CL0-originated binary is accepted locally, re-broadcast it over the libp2p
  // metagraph-binaries topic so peer GL0s receive it without needing CL0 to HTTP-push to
  // every GL0 separately. Synchronous best-effort: we await the publish before responding
  // 200 so CL0 sees a bounded Ok latency, but any publish error is swallowed (logged at
  // WARN). Gossip delivery is still best-effort at the transport layer — duplicate
  // receives on peer nodes are idempotent (process short-circuits on "already accepted").
  private def broadcastMetagraphBinary(address: Address, signed: Signed[StateChannelSnapshotBinary]): F[Unit] =
    JsonSerializer[F]
      .serialize(signed)
      .flatMap { bytes =>
        sidecarClient.publishMetagraphBinary(SidecarClient.mkMetagraphBinary(address.value.value, bytes)).void
      }
      .handleErrorWith(e => logger.warn(e)(s"Failed to gossip metagraph binary for $address"))

  // RELIABLE GENESIS SEEDING (#28). A metagraph's genesis-full binary (`lastSnapshotHash == Hash.empty`) is sent ONCE by
  // ml0; under sharding it must reach the shard committee's per-shard buffer (fed by this gossip topic via
  // NakamotoSyncDaemon's `MetagraphBinary` intake) for gl0 to seed `lastCurrencySnapshots[mg]` and unblock cl1's
  // first-currency-snapshot bootstrap. A single best-effort GossipSub publish can miss the (rotating) shard leader, after
  // which the slow ChainSync stuck-detection fallback loses the race against cl1's 90s timeout. So we re-publish the
  // genesis a few times: the sidecar uses GossipSub's DEFAULT seqno-based message-id, so each re-publish is a DISTINCT
  // message (NOT seen-cache-deduped) and re-propagates, reliably reaching every gl0 node's shard buffer within seconds.
  // Fired in the BACKGROUND so the inbound POST still returns promptly. Idempotent downstream (dedup by binary hash in the
  // shard buffer + orphan buffer), so the extra publishes are harmless. Only the genesis is amplified — incrementals are
  // frequent + each is already broadcast, so they need no help.
  private val GenesisReGossipCount: Int = 5
  private val GenesisReGossipInterval = 2.seconds

  private def reGossipGenesis(address: Address, signed: Signed[StateChannelSnapshotBinary]): F[Unit] =
    List
      .fill(GenesisReGossipCount)(())
      .traverse(_ => Async[F].sleep(GenesisReGossipInterval) >> broadcastMetagraphBinary(address, signed))
      .void

  // Inbound metagraph binaries use whatever context is currently in head.
  // DO NOT finality-gate this path: in Nakamoto mode, head is ~always ahead
  // of finalized during active production, so gating here would reject every
  // incoming binary. The validator re-runs acceptance with the CL0-baked
  // globalSyncView via forcedGlobalSyncView when snapshot production includes
  // the binary — that's where the race is handled. FinalityGate is retained
  // at the outbound-retrieval routes (SnapshotRoutes), not here.
  protected val public: HttpRoutes[F] = Timeout(snapshotBinarySenderTimeoutsConfig.routes)(HttpRoutes.of[F] {
    case req @ POST -> Root / AddressVar(address) / "snapshot" =>
      req
        .as[Signed[StateChannelSnapshotBinary]]
        .flatMap { signed =>
          val output = StateChannelOutput(address, signed)
          snapshotStorage.head
            .map(_.map((output, _)))
            .flatMap(_.traverse {
              case (out, snapshotAndState) =>
                stateChannelService.process(out, snapshotAndState)
            })
            .flatMap {
              case Some(Left(errors)) => BadRequest(errors)
              case Some(Right(_))     =>
                // For the genesis-full (chain head), amplify the gossip in the background so it reliably reaches the
                // shard committee buffer fast (see reGossipGenesis). Incrementals broadcast once (frequent → reliable).
                val reGossip =
                  if (signed.value.lastSnapshotHash === Hash.empty) Async[F].start(reGossipGenesis(address, signed)).void
                  else Async[F].unit
                broadcastMetagraphBinary(address, signed) >> reGossip >> Ok()
              case None => ServiceUnavailable(("message" ->> "Node not yet ready to accept metagraph snapshots.") :: HNil)
            }
        }
  })

}
