package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.security.hash.Hash

import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Manages active chain synchronization — fetches missing parent snapshots
  * from peers when gossip delivers out of order.
  *
  * When NakamotoSyncDaemon buffers a snapshot because its parent is missing,
  * it signals this manager to fetch the parent. The manager sends a
  * FetchSnapshots request via the sidecar's ChainSyncOutbound gRPC service,
  * which opens a libp2p stream to a peer and retrieves the missing snapshot.
  *
  * Fetched snapshots are fed back through the normal handleSnapshot pipeline
  * for validation and storage, which triggers drainPendingChildren to process
  * any buffered descendants.
  */
object ChainSyncManager {

  trait ChainSyncManagerAlgebra[F[_]] {
    /** Request a missing parent snapshot. The fetch happens in a background fiber.
      * Deduplicates: if this hash is already being fetched, the call is a no-op.
      */
    def requestMissing(parentHash: Hash): F[Unit]
  }

  def make[F[_]: Async](
    channel: ManagedChannel,
    onFetched: pb.Snapshot => F[Unit]
  ): F[ChainSyncManagerAlgebra[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncManager")
    val stub = pb.ChainSyncOutboundGrpc.blockingStub(channel)

    for {
      inflightRef <- Ref.of[F, Set[Hash]](Set.empty)
    } yield new ChainSyncManagerAlgebra[F] {

      def requestMissing(parentHash: Hash): F[Unit] =
        inflightRef.modify { inflight =>
          if (inflight.contains(parentHash))
            (inflight, true) // already fetching
          else
            (inflight + parentHash, false)
        }.flatMap { alreadyInflight =>
          if (alreadyInflight)
            Async[F].unit
          else
            // Fire-and-forget background fetch
            Async[F].start(fetchAndDeliver(parentHash)).void
        }

      private def fetchAndDeliver(parentHash: Hash): F[Unit] = {
        val hashBytes = com.google.protobuf.ByteString.copyFrom(parentHash.value.getBytes)
        val request = pb.FetchSnapshotsRequest(hashes = Seq(hashBytes))

        val fetch: F[List[pb.Snapshot]] = Async[F].blocking {
          stub.fetchSnapshots(request).toList
        }

        val work: F[Unit] = for {
          _ <- logger.info(s"🔗 ChainSync: fetching missing parent ${parentHash.value.take(16)}")
          snapshots <- fetch
          _ <- logger.info(s"🔗 ChainSync: received ${snapshots.size} snapshot(s) for ${parentHash.value.take(16)}")
          _ <- snapshots.traverse_(onFetched)
        } yield ()

        Async[F].guaranteeCase(
          work.handleErrorWith(e =>
            logger.warn(s"🔗 ChainSync: fetch failed for ${parentHash.value.take(16)}: ${e.getMessage}")
          )
        )(_ => inflightRef.update(_ - parentHash))
      }
    }
  }
}
