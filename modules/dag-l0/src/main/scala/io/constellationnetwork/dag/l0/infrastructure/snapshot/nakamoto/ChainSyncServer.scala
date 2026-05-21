package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.all._

import scala.concurrent.{ExecutionContext, Future}

import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.hash.Hash

import io.grpc.stub.StreamObserver
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Implements the ChainSyncInbound gRPC service — serves local chain data to peer requests relayed through the sidecar.
  *
  * When a remote peer asks for snapshots by hash or chain points, the sidecar calls these methods on the JVM, which looks up data in the
  * NakamotoChainStore with disk fallback via SnapshotStorage.
  */
object ChainSyncServer {

  def make[F[_]: Async: HasherSelector](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    dispatcher: Dispatcher[F]
  )(implicit ec: ExecutionContext): pb.ChainSyncInboundGrpc.ChainSyncInbound = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncServer")

    new pb.ChainSyncInboundGrpc.ChainSyncInbound {
      override def serveSnapshots(
        request: pb.ServeSnapshotsRequest,
        responseObserver: StreamObserver[pb.Snapshot]
      ): Unit =
        dispatcher.unsafeRunAndForget {
          val hashes = request.hashes.map(h => Hash(new String(h.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))

          dispatcher.unsafeRunSync(
            logger.info(s"ChainSync SERVE: ${hashes.size} hash(es) requested: ${hashes.map(_.value.take(12)).mkString(",")}")
          )

          // Look up the finalized boundary ONCE per request — all hashes in this batch are
          // classified against the same snapshot of `lastFinalizedOrdinal`. Reading per-hash would
          // race: a finalize landing mid-iteration could flip a snapshot from Provisional to
          // Finalized between two hashes, producing inconsistent disposition for callers that
          // batch hashes spanning the finality boundary.
          chainStore.lastFinalizedOrdinal.flatMap { finalizedOrdinal =>
            hashes.toList.traverse_ { hash =>
              // Two-tier lookup (Path 1, Finding 2):
              //   1. `chainStore.get(hash)` — in-memory `byHash`, full `StoredSnapshot` (hot path).
              //   2. `snapshotStorage.get(hash)` — disk-backed by-hash file index; engaged whenever
              //      Fix B's k₁-bounded retention has evicted the in-memory entry. Disk stores only
              //      `Signed[GlobalIncrementalSnapshot]` (no GSI context), so the disk-fallback
              //      response carries an empty/placeholder context-encoded payload — peer-side
              //      handler must tolerate the slim form (matches the BackfillSnapshot shape and the
              //      legacy `serveByRange` path).
              chainStore.get(hash).flatMap {
                case Some(stored) =>
                  val payload = {
                    import io.circe.syntax._
                    val snapshotJson = stored.signedSnapshot.asJson
                    val contextJson = stored.context.asJson
                    val combined = io.circe.Json.obj("snapshot" -> snapshotJson, "context" -> contextJson)
                    combined.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                  }
                  // Extract VRF proof, public key, and producer ID from the SlotCertificate.
                  // stored.vrfOutput is the VRF OUTPUT (hash of gamma), NOT the proof.
                  // The validator needs the actual proof bytes for verification.
                  val cert = stored.signedSnapshot.value.slotCertificate
                  val vrfProofBytes = cert.map(_.vrfProof.value.toBytes).getOrElse(Array.empty[Byte])
                  val vrfPkBytes = cert.map(_.vrfPublicKey.value.toBytes).getOrElse(Array.empty[Byte])
                  val producerIdBytes = stored.signedSnapshot.proofs.head.id.hex.toBytes
                  val parentSlot = cert.map(_.parentSlot.value.value).getOrElse(0L)
                  // #56.8 disposition: snapshots at ordinal ≤ finalizedOrdinal are on the canonical
                  // chain by construction (chainStore.finalize prunes non-canonical at-or-below
                  // finalized in the same step that advances `lastFinalizedOrdinal`). Snapshots
                  // above are pending — `branch_id` is the snapshot's own hash (branch identity =
                  // tip hash in the multi-branch overlay model).
                  val isFinalized = stored.ordinal <= finalizedOrdinal
                  val branchIdBytes =
                    if (isFinalized) com.google.protobuf.ByteString.EMPTY
                    else com.google.protobuf.ByteString.copyFrom(stored.hash.value.getBytes)
                  val snap = pb.Snapshot(
                    hash = com.google.protobuf.ByteString.copyFrom(stored.hash.value.getBytes),
                    slot = stored.slot,
                    ordinal = stored.ordinal,
                    parentHash = com.google.protobuf.ByteString.copyFrom(stored.parentHash.value.getBytes),
                    payload = com.google.protobuf.ByteString.copyFrom(payload),
                    vrfProof = com.google.protobuf.ByteString.copyFrom(vrfProofBytes),
                    vrfPublicKey = com.google.protobuf.ByteString.copyFrom(vrfPkBytes),
                    producerId = com.google.protobuf.ByteString.copyFrom(producerIdBytes),
                    parentSlot = parentSlot,
                    finalized = isFinalized,
                    branchId = branchIdBytes
                  )
                  Async[F].delay(responseObserver.onNext(snap))

                case None =>
                  // In-memory miss — try disk-backed by-hash lookup. Path 1 Finding 2: under Fix B's
                  // bounded retention, hashes older than `keepDepthBehindFinalized` are absent from
                  // `byHash` even though the snapshot is on disk.
                  HasherSelector[F].withCurrent { implicit hasher =>
                    snapshotStorage.get(hash).flatMap {
                      case Some(signedSnapshot) =>
                        signedSnapshot.toHashed[F].flatMap { hashed =>
                          import io.circe.syntax._
                          // Disk doesn't carry `GlobalSnapshotInfo`; peer must reconstruct or
                          // fall back to backfill. Encode snapshot-only to stay consistent with
                          // `serveByRange`. Peers asking by-hash for evicted snapshots typically
                          // are doing historical / NIPoPoW queries — those don't need GSI context.
                          val payload = signedSnapshot.asJson.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                          val cert = signedSnapshot.value.slotCertificate
                          val vrfProofBytes = cert.map(_.vrfProof.value.toBytes).getOrElse(Array.empty[Byte])
                          val vrfPkBytes = cert.map(_.vrfPublicKey.value.toBytes).getOrElse(Array.empty[Byte])
                          val producerIdBytes = signedSnapshot.proofs.head.id.hex.toBytes
                          val slot = cert.map(_.slot.value.value).getOrElse(0L)
                          val parentSlot = cert.map(_.parentSlot.value.value).getOrElse(0L)
                          val ordinal = signedSnapshot.value.ordinal.value.value
                          // Disposition uses the same finalizedOrdinal snapshot taken at request top.
                          // A disk-resident snapshot below `finalizedOrdinal` is canonical by the
                          // same Fix-B invariant (only canonical entries survive eviction at-or-below
                          // finalized).
                          val isFinalized = ordinal <= finalizedOrdinal
                          val branchIdBytes =
                            if (isFinalized) com.google.protobuf.ByteString.EMPTY
                            else com.google.protobuf.ByteString.copyFrom(hashed.hash.value.getBytes)
                          val snap = pb.Snapshot(
                            hash = com.google.protobuf.ByteString.copyFrom(hashed.hash.value.getBytes),
                            slot = slot,
                            ordinal = ordinal,
                            parentHash = com.google.protobuf.ByteString.copyFrom(hashed.lastSnapshotHash.value.getBytes),
                            payload = com.google.protobuf.ByteString.copyFrom(payload),
                            vrfProof = com.google.protobuf.ByteString.copyFrom(vrfProofBytes),
                            vrfPublicKey = com.google.protobuf.ByteString.copyFrom(vrfPkBytes),
                            producerId = com.google.protobuf.ByteString.copyFrom(producerIdBytes),
                            parentSlot = parentSlot,
                            finalized = isFinalized,
                            branchId = branchIdBytes
                          )
                          logger
                            .info(
                              s"ChainSync SERVE: hash ${hash.value.take(12)} from disk (in-memory evicted, ordinal=$ordinal)"
                            ) >>
                            Async[F].delay(responseObserver.onNext(snap))
                        }

                      case None =>
                        // NotFound on disk too. Existing behavior. The Scala-side
                        // `ChainSyncStateResponse.NotFound` ADT case is constructed by the consumer
                        // when it observes a requested hash absent from the response stream — the
                        // wire format hasn't been extended for explicit not-found yet.
                        logger.info(s"ChainSync SERVE: hash ${hash.value.take(12)} NOT in chain store or disk") >>
                          Async[F].unit
                    }
                  }
              }
            } >> Async[F].delay(responseObserver.onCompleted())
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
            tipHash = bestTip
              .map(t => com.google.protobuf.ByteString.copyFrom(t.hash.value.getBytes))
              .getOrElse(com.google.protobuf.ByteString.EMPTY),
            tipOrdinal = bestTip.map(_.ordinal).getOrElse(0L)
          )
        }

        dispatcher.unsafeToFuture(effect)
      }

      override def serveByRange(
        request: pb.FetchByRangeRequest,
        responseObserver: StreamObserver[pb.BackfillSnapshot]
      ): Unit =
        dispatcher.unsafeRunAndForget {
          import eu.timepit.refined.types.numeric.NonNegLong

          val start = request.startOrdinal
          val end = request.endOrdinal

          logger.info(s"ChainSync SERVE RANGE: ordinals $start to $end") >>
            HasherSelector[F].withCurrent { implicit hasher =>
              (start to end).toList.traverse_ { ord =>
                val ordinal = io.constellationnetwork.schema.SnapshotOrdinal(NonNegLong.unsafeFrom(ord))
                snapshotStorage.get(ordinal).flatMap {
                  case Some(signedSnapshot) =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      val payload = {
                        import io.circe.syntax._
                        signedSnapshot.asJson.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                      }
                      val cert = signedSnapshot.value.slotCertificate
                      val snap = pb.BackfillSnapshot(
                        hash = com.google.protobuf.ByteString.copyFrom(hashed.hash.value.getBytes),
                        slot = cert.map(_.slot.value.value).getOrElse(0L),
                        ordinal = ord,
                        parentHash = com.google.protobuf.ByteString.copyFrom(hashed.lastSnapshotHash.value.getBytes),
                        payload = com.google.protobuf.ByteString.copyFrom(payload),
                        vrfProof = com.google.protobuf.ByteString
                          .copyFrom(cert.map(_.vrfProof.value.toBytes).getOrElse(Array.empty[Byte])),
                        vrfPublicKey = com.google.protobuf.ByteString
                          .copyFrom(cert.map(_.vrfPublicKey.value.toBytes).getOrElse(Array.empty[Byte])),
                        producerId = com.google.protobuf.ByteString
                          .copyFrom(signedSnapshot.proofs.head.id.hex.toBytes),
                        parentSlot = cert.map(_.parentSlot.value.value).getOrElse(0L)
                      )
                      Async[F].delay(responseObserver.onNext(snap))
                    }
                  case None =>
                    logger.debug(s"ChainSync SERVE RANGE: ordinal=$ord not found") >>
                      Async[F].unit
                }
              }
            } >> Async[F].delay(responseObserver.onCompleted())
        }
    }
  }
}
