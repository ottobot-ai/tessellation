package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Manages active chain synchronization — fetches missing parent snapshots from peers when gossip delivers out of order.
  *
  * When NakamotoSyncDaemon buffers a snapshot because its parent is missing, it signals this manager to fetch the parent. The manager sends
  * a FetchSnapshots request via the sidecar's ChainSyncOutbound gRPC service, which opens a libp2p stream to a peer and retrieves the
  * missing snapshot.
  *
  * Fetched snapshots are fed back through the normal handleSnapshot pipeline for validation and storage, which triggers
  * drainPendingChildren to process any buffered descendants.
  */
object ChainSyncManager {

  trait ChainSyncManagerAlgebra[F[_]] {

    /** Request a missing parent snapshot. The fetch happens in a background fiber. Deduplicates: if this hash is already being fetched, the
      * call is a no-op.
      */
    def requestMissing(parentHash: Hash): F[Unit]

    /** #259: actively pull missing metagraph state-channel binaries by VALUE-hash from a peer over the existing ChainSync protocol.
      * Synchronous (the caller — the daemon's stuck-detection tick — already runs on its own fiber and rate-limits per `(mg, parentHash)`).
      * Mirrors `requestMissing`'s blocking pattern: the call blocks on the gRPC server-stream, drains it to a `List`, and on ANY error logs
      * a warning and returns an empty list (recovery is best-effort — the next tick retries). The returned `pb.MetagraphBinaryResponse`s
      * carry `signedBinary` = the gossip-wire `JsonSerializer` bytes; the caller re-feeds each through the committee gate via the gossip
      * path.
      *
      * `binaryHashes` are value-hashes (`signed.toHashed.hash`); they are encoded on the wire as UTF-8 of the canonical Hash hex string,
      * identical to how `requestMissing` passes a hash (correction D — keep hex-vs-raw consistent or every server-side lookup misses).
      */
    def fetchMetagraphBinaries(metagraphAddress: Address, binaryHashes: List[Hash]): F[List[pb.MetagraphBinaryResponse]]
  }

  def make[F[_]: Async](
    channel: ManagedChannel,
    onFetched: ChainSyncStateResponse[pb.Snapshot] => F[Unit]
  ): F[ChainSyncManagerAlgebra[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncManager")
    val stub = pb.ChainSyncOutboundGrpc.blockingStub(channel)

    for {
      inflightRef <- Ref.of[F, Set[Hash]](Set.empty)
    } yield
      new ChainSyncManagerAlgebra[F] {

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
            // #56.8: parse each pb.Snapshot's `finalized` + `branch_id` fields into the ADT.
            // Empty response → NotFound. Always invoke `onFetched` even for NotFound so the consumer
            // observes the absence (legacy path silently dropped). Single-hash request, so at most one
            // pb.Snapshot in the response per invocation.
            _ <-
              if (snapshots.isEmpty) onFetched(ChainSyncStateResponse.NotFound(parentHash))
              else snapshots.traverse_(snap => onFetched(parsePbSnapshot(snap)))
          } yield ()

          Async[F].guaranteeCase(
            work.handleErrorWith(e => logger.warn(s"🔗 ChainSync: fetch failed for ${parentHash.value.take(16)}: ${e.getMessage}"))
          )(_ => inflightRef.update(_ - parentHash))
        }

        def fetchMetagraphBinaries(metagraphAddress: Address, binaryHashes: List[Hash]): F[List[pb.MetagraphBinaryResponse]] =
          if (binaryHashes.isEmpty) Async[F].pure(List.empty)
          else {
            // Same hex-vs-raw encoding as `requestMissing` (correction D): UTF-8 of the canonical hash hex.
            val hashBytes = binaryHashes.map(h => com.google.protobuf.ByteString.copyFrom(h.value.getBytes))
            val request = pb.FetchMetagraphBinariesRequest(
              metagraphAddress = metagraphAddress.value.value,
              binaryHashes = hashBytes
            )
            Async[F]
              .blocking(stub.fetchMetagraphBinaries(request).toList)
              .flatTap { responses =>
                logger.info(
                  s"🔗 ChainSync: fetched ${responses.size} metagraph binary(s) for mg=$metagraphAddress " +
                    s"(${binaryHashes.size} value-hash(es) requested: ${binaryHashes.map(_.value.take(12)).mkString(",")})"
                )
              }
              .handleErrorWith { e =>
                logger.warn(
                  s"🔗 ChainSync: metagraph-binary fetch failed for mg=$metagraphAddress " +
                    s"(${binaryHashes.map(_.value.take(12)).mkString(",")}): ${e.getMessage}"
                ) >> Async[F].pure(List.empty[pb.MetagraphBinaryResponse])
              }
          }
      }
  }

  /** Parse the disposition fields of a `pb.Snapshot` (added in #56.8) into the Scala-side ADT.
    *
    * Convention: `finalized=true` → `Finalized`; `finalized=false` → `Provisional` with `branch_id` decoded as a hash. If a snapshot
    * arrives with `finalized=false` but `branch_id` empty (e.g. an old peer that hasn't been regenerated against the new proto), we treat
    * it as `Provisional` with the snapshot's own hash as branch id — preserves the "every newly-produced snapshot is its own branch tip"
    * convention.
    *
    * `private[nakamoto]` so the test suite can verify wire parsing without exposing it as public API.
    */
  private[nakamoto] def parsePbSnapshot(snap: pb.Snapshot): ChainSyncStateResponse[pb.Snapshot] =
    if (snap.finalized) ChainSyncStateResponse.Finalized(snap)
    else {
      val branchHash =
        if (snap.branchId.isEmpty)
          // Pre-#56.8 peer or wire compatibility: fall back to the snapshot's own hash as the branch id.
          // The `branchId` field is bytes-encoded UTF-8 of the hash hex string (matching `pb.Snapshot.hash`).
          Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        else
          Hash(new String(snap.branchId.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
      ChainSyncStateResponse.Provisional(snap, BranchId(branchHash))
    }
}
