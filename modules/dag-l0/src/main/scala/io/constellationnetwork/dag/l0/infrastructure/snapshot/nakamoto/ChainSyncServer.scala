package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.all._

import scala.concurrent.{ExecutionContext, Future}

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.security.hash.Hash

import io.grpc.stub.StreamObserver
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Implements the ChainSyncInbound gRPC service — serves local chain data
  * to peer requests relayed through the sidecar.
  *
  * When a remote peer asks for snapshots by hash or chain points, the sidecar
  * calls these methods on the JVM, which looks up data in the NakamotoChainStore.
  */
object ChainSyncServer {

  def make[F[_]: Async](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    dispatcher: Dispatcher[F]
  )(implicit ec: ExecutionContext): pb.ChainSyncInboundGrpc.ChainSyncInbound = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncServer")

    new pb.ChainSyncInboundGrpc.ChainSyncInbound {
      override def serveSnapshots(
        request: pb.ServeSnapshotsRequest,
        responseObserver: StreamObserver[pb.Snapshot]
      ): Unit = {
        dispatcher.unsafeRunAndForget {
          Async[F].delay {
            val hashes = request.hashes.map(h => Hash(new String(h.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))

            // Look up each hash in the chain store and stream back
            hashes.foreach { hash =>
              val result = dispatcher.unsafeRunSync(chainStore.get(hash))
              result match {
                case Some(stored) =>
                  // Reconstruct the pb.Snapshot from the stored data
                  val payload = {
                    import io.circe.syntax._
                    val snapshotJson = stored.signedSnapshot.asJson
                    val contextJson = stored.context.asJson
                    val combined = io.circe.Json.obj("snapshot" -> snapshotJson, "context" -> contextJson)
                    combined.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                  }

                  val snap = pb.Snapshot(
                    hash = com.google.protobuf.ByteString.copyFrom(stored.hash.value.getBytes),
                    slot = stored.slot,
                    ordinal = stored.ordinal,
                    parentHash = com.google.protobuf.ByteString.copyFrom(stored.parentHash.value.getBytes),
                    payload = com.google.protobuf.ByteString.copyFrom(payload),
                    vrfProof = com.google.protobuf.ByteString.copyFrom(stored.vrfOutput)
                  )
                  responseObserver.onNext(snap)

                case None =>
                  // Hash not found — skip it (peer will notice the gap)
                  ()
              }
            }
            responseObserver.onCompleted()
          }
        }
      }

      override def serveChainPoints(
        request: pb.ServeChainPointsRequest
      ): Future[pb.ServeChainPointsResponse] = {
        val effect = for {
          chain <- chainStore.chainFromTip
          bestTip <- chainStore.bestTip
        } yield {
          // Build sparse chain points: every 10th ordinal + tip + genesis
          val points = chain.zipWithIndex.collect {
            case (stored, idx) if idx % 10 == 0 || idx == 0 || idx == chain.length - 1 =>
              pb.ChainPoint(
                hash = com.google.protobuf.ByteString.copyFrom(stored.hash.value.getBytes),
                ordinal = stored.ordinal
              )
          }

          pb.ServeChainPointsResponse(
            points = points,
            tipHash = bestTip.map(t => com.google.protobuf.ByteString.copyFrom(t.hash.value.getBytes)).getOrElse(com.google.protobuf.ByteString.EMPTY),
            tipOrdinal = bestTip.map(_.ordinal).getOrElse(0L)
          )
        }

        dispatcher.unsafeToFuture(effect)
      }
    }
  }
}
