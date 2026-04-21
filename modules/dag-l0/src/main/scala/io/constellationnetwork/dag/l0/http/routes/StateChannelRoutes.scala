package io.constellationnetwork.dag.l0.http.routes

import cats.effect.Async
import cats.syntax.applicativeError._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.traverse._

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
              case Some(Right(_))     => broadcastMetagraphBinary(address, signed) >> Ok()
              case None               => ServiceUnavailable(("message" ->> "Node not yet ready to accept metagraph snapshots.") :: HNil)
            }
        }
  })

}
