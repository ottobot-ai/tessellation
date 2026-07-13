package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.syntax.all._

import scala.concurrent.{ExecutionContext, Future}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.HasherSelector
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.refineV
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Implements the ChainSyncInbound gRPC service — serves local chain data to peer requests relayed through the sidecar.
  *
  * When a remote peer asks for snapshots by hash or chain points, the sidecar calls these methods on the JVM, which looks up data in the
  * NakamotoChainStore with disk fallback via SnapshotStorage.
  */
object ChainSyncServer {

  private[nakamoto] val MaxHashesPerRequest = 64

  private[nakamoto] def validateHashCount(kind: String, count: Int): Either[String, Unit] =
    Either.cond(count <= MaxHashesPerRequest, (), s"too many $kind: $count > $MaxHashesPerRequest")

  def make[F[_]: Async: HasherSelector: JsonSerializer](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    // #259: recent-FINALIZED snapshots — the authoritative source for a peer's metagraph-binary
    // fetch. We look up each requested value-hash among the binaries each finalized snapshot
    // carries in `stateChannelSnapshots`.
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    // #259: orphan buffer, scanned NON-DESTRUCTIVELY (`peekForValueHash`) so we can also serve a
    // binary that is buffered locally but not yet folded into a finalized snapshot.
    orphanBuffer: MetagraphOrphanBuffer[F],
    dataDir: java.nio.file.Path,
    dispatcher: Dispatcher[F]
  )(implicit ec: ExecutionContext): pb.ChainSyncInboundGrpc.ChainSyncInbound = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ChainSyncServer")

    def runBounded[A](kind: String, count: Int, responseObserver: StreamObserver[A])(serve: => F[Unit]): Unit =
      validateHashCount(kind, count).fold(
        error => responseObserver.onError(Status.RESOURCE_EXHAUSTED.withDescription(error).asRuntimeException()),
        _ => dispatcher.unsafeRunAndForget(serve)
      )

    new pb.ChainSyncInboundGrpc.ChainSyncInbound {
      override def serveSnapshots(
        request: pb.ServeSnapshotsRequest,
        responseObserver: StreamObserver[pb.Snapshot]
      ): Unit =
        runBounded("snapshot hashes", request.hashes.size, responseObserver) {
          val hashes = request.hashes.map(h => Hash(new String(h.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))

          // Look up the finalized boundary ONCE per request — all hashes in this batch are
          // classified against the same snapshot of `lastFinalizedOrdinal`. Reading per-hash would
          // race: a finalize landing mid-iteration could flip a snapshot from Provisional to
          // Finalized between two hashes, producing inconsistent disposition for callers that
          // batch hashes spanning the finality boundary.
          logger.info(
            s"ChainSync SERVE: ${hashes.size} hash(es) requested: ${hashes.map(_.value.take(12)).mkString(",")}"
          ) >> chainStore.lastFinalizedOrdinal.flatMap { finalizedOrdinal =>
            hashes.toList.traverse_ { hash =>
              // Two-tier lookup (Path 1, Finding 2):
              //   1. `chainStore.get(hash)` — in-memory `byHash`, full `StoredSnapshot` (hot path).
              //   2. `snapshotStorage.get(hash)` — disk-backed by-hash file index; engaged whenever
              //      Fix B's k₁-bounded retention has evicted the in-memory entry. Disk stores only
              //      `Signed[GlobalIncrementalSnapshot]` (no GSI context), so the same payload object is sent without its optional context.
              //      The peer recursively obtains ancestry and recreates context by ordinary transition replay; peer-carried context is
              //      never installed as state.
              chainStore.get(hash).flatMap {
                case Some(stored) =>
                  SnapshotKesStorage.get[F](dataDir, stored.hash).flatMap {
                    case None =>
                      logger.error(
                        s"ChainSync SERVE: refusing hash ${stored.hash.value.take(12)} because its KES evidence is unavailable"
                      )
                    case Some(kesSignature) =>
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
                      val etaBytes = cert.map(c => Hex(c.eta.value).toBytes).getOrElse(Array.empty[Byte])
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
                        eta = com.google.protobuf.ByteString.copyFrom(etaBytes),
                        producerId = com.google.protobuf.ByteString.copyFrom(producerIdBytes),
                        parentSlot = parentSlot,
                        finalized = isFinalized,
                        branchId = branchIdBytes,
                        kesSignature = com.google.protobuf.ByteString.copyFrom(kesSignature)
                      )
                      Async[F].delay(responseObserver.onNext(snap))
                  }

                case None =>
                  // In-memory miss — try disk-backed by-hash lookup. Path 1 Finding 2: under Fix B's
                  // bounded retention, hashes older than `keepDepthBehindFinalized` are absent from
                  // `byHash` even though the snapshot is on disk.
                  HasherSelector[F].withCurrent { implicit hasher =>
                    snapshotStorage.get(hash).flatMap {
                      case Some(signedSnapshot) =>
                        signedSnapshot.toHashed[F].flatMap { hashed =>
                          SnapshotKesStorage.get[F](dataDir, hashed.hash).flatMap {
                            case None =>
                              logger.error(
                                s"ChainSync SERVE: refusing disk hash ${hashed.hash.value.take(12)} because its KES evidence is unavailable"
                              )
                            case Some(kesSignature) =>
                              import io.circe.syntax._
                              // Greenfield wire shape: every ChainSync snapshot payload is an object
                              // with a required `snapshot` member and an optional `context` member.
                              // Disk does not retain context, so omit that member. The receiver derives
                              // context by replay in either case.
                              val payload = io.circe.Json
                                .obj("snapshot" -> signedSnapshot.asJson)
                                .noSpaces
                                .getBytes(java.nio.charset.StandardCharsets.UTF_8)
                              val cert = signedSnapshot.value.slotCertificate
                              val vrfProofBytes = cert.map(_.vrfProof.value.toBytes).getOrElse(Array.empty[Byte])
                              val vrfPkBytes = cert.map(_.vrfPublicKey.value.toBytes).getOrElse(Array.empty[Byte])
                              val etaBytes = cert.map(c => Hex(c.eta.value).toBytes).getOrElse(Array.empty[Byte])
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
                                eta = com.google.protobuf.ByteString.copyFrom(etaBytes),
                                producerId = com.google.protobuf.ByteString.copyFrom(producerIdBytes),
                                parentSlot = parentSlot,
                                finalized = isFinalized,
                                branchId = branchIdBytes,
                                kesSignature = com.google.protobuf.ByteString.copyFrom(kesSignature)
                              )
                              logger
                                .info(
                                  s"ChainSync SERVE: hash ${hash.value.take(12)} from disk (in-memory evicted, ordinal=$ordinal)"
                                ) >>
                                Async[F].delay(responseObserver.onNext(snap))
                          }
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

      // #259: serve local metagraph state-channel binaries to a peer's active-recovery fetch.
      //
      // The peer's gl0 orphan-buffered a binary whose parent it never admitted; it now pulls that
      // binary by VALUE-hash so it can re-feed it through its own committee gate. We answer from two
      // local sources, in priority order:
      //   1. RECENT-FINALIZED snapshots (`getLastN`): each carries its accepted binaries in
      //      `stateChannelSnapshots[address]` as a `NonEmptyList[Signed[StateChannelSnapshotBinary]]`.
      //      A binary here is authoritative — already admitted + finalized by this node.
      //   2. ORPHAN BUFFER (NON-DESTRUCTIVE `peekForValueHash`): a binary we ourselves buffered but
      //      have not yet folded into a finalized snapshot. Serving it does NOT remove it from our
      //      buffer (correction A) — a peer fetch never mutates our admission state.
      //
      // Correction C (wire format): finalized-snapshot matches are serialized with
      // `JsonSerializer[F].serialize(signed)` — the EXACT bytes the gossip path produces
      // (StateChannelRoutes.broadcastMetagraphBinary), so the peer's re-feed
      // (`handleMetagraphBinary` → `processBytes`) deserializes them identically. Orphan-buffer
      // matches are served as the raw buffered wire bytes, which ARE those same gossip bytes.
      //
      // Correction D (value-hash identity): requested hashes arrive as UTF-8 of the canonical Hash
      // hex string (matching `ChainSyncManager.requestMissing` / `serveSnapshots` decode). We compare
      // against `signed.toHashed.map(_.hash)` = the binary's *value*-hash — the SAME identity the
      // orphan buffer keys on and gl0 writes into `lastStateChannelSnapshotHashes`. NOT a wire-bytes
      // digest.
      //
      // Correction B (no unsafeRunSync, no Any): the whole lookup is one `F` run via
      // `dispatcher.unsafeRunAndForget`; inside it we use effectful `filterA` / `collectFirstSomeM`
      // and a `Ref` dedup accumulator — never `.unsafeRunSync` in a loop.
      override def serveMetagraphBinaries(
        request: pb.FetchMetagraphBinariesRequest,
        responseObserver: StreamObserver[pb.MetagraphBinaryResponse]
      ): Unit =
        runBounded("metagraph binary hashes", request.binaryHashes.size, responseObserver) {
          refineV[DAGAddressRefined](request.metagraphAddress) match {
            case Left(err) =>
              logger.warn(
                s"ChainSync SERVE BINARIES: invalid metagraph address '${request.metagraphAddress}' ($err)"
              ) >> Async[F].delay(responseObserver.onCompleted())

            case Right(refined) =>
              val address = Address(refined)
              // Value-hashes: UTF-8 of the canonical Hash hex string (correction D).
              val requested: List[Hash] =
                request.binaryHashes.toList.map(h => Hash(new String(h.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))
              val requestedSet: Set[Hash] = requested.toSet

              def emit(bytes: Array[Byte]): F[Unit] =
                Async[F].delay(
                  responseObserver.onNext(
                    pb.MetagraphBinaryResponse(signedBinary = com.google.protobuf.ByteString.copyFrom(bytes))
                  )
                )

              if (requestedSet.isEmpty)
                Async[F].delay(responseObserver.onCompleted())
              else
                HasherSelector[F].withCurrent { implicit hasher =>
                  // Dedup so we serve each value-hash at most once across both sources.
                  cats.effect.Ref.of[F, Set[Hash]](Set.empty[Hash]).flatMap { servedRef =>
                    val serveFromFinalized: F[Unit] =
                      lastNGlobalSnapshotStorage.getLastN.flatMap { snapshots =>
                        snapshots.traverse_ { hashed =>
                          hashed.signed.value.stateChannelSnapshots.get(address) match {
                            case None           => Async[F].unit
                            case Some(binaries) =>
                              // Effectful filter on the binary's VALUE-hash, skipping anything already served.
                              binaries.toList.filterA { signed =>
                                signed.toHashed[F].flatMap { hb =>
                                  servedRef.get.map(served => requestedSet.contains(hb.hash) && !served.contains(hb.hash))
                                }
                              }.flatMap {
                                _.traverse_ { signed =>
                                  signed.toHashed[F].flatMap { hb =>
                                    // Correction C: exact gossip-path bytes.
                                    JsonSerializer[F].serialize(signed).flatMap { wireBytes =>
                                      servedRef.update(_ + hb.hash) >>
                                        logger.info(
                                          s"ChainSync SERVE BINARIES: mg=$address served value-hash ${hb.hash.value.take(12)} from finalized snapshot"
                                        ) >>
                                        emit(wireBytes)
                                    }
                                  }
                                }
                              }
                          }
                        }
                      }

                    // Correction A: NON-DESTRUCTIVE orphan-buffer peek for any still-unserved hash.
                    val serveFromOrphanBuffer: F[Unit] =
                      servedRef.get.flatMap { servedSoFar =>
                        val stillMissing = requested.filterNot(servedSoFar.contains).distinct
                        stillMissing.traverse_ { wanted =>
                          orphanBuffer
                            .peekForValueHash(address, wanted) { bytes =>
                              // Deserialize-then-hash via the SAME gossip codec; return the value-hash.
                              JsonSerializer[F]
                                .deserialize[Signed[StateChannelSnapshotBinary]](bytes)
                                .flatMap {
                                  case Left(_)       => Async[F].pure(none[Hash])
                                  case Right(signed) => signed.toHashed[F].map(hb => hb.hash.some)
                                }
                            }
                            .flatMap {
                              case None => Async[F].unit
                              case Some(bytes) =>
                                servedRef.update(_ + wanted) >>
                                  logger.info(
                                    s"ChainSync SERVE BINARIES: mg=$address served value-hash ${wanted.value.take(12)} from orphan buffer (non-destructive)"
                                  ) >>
                                  emit(bytes)
                            }
                        }
                      }

                    logger.info(
                      s"ChainSync SERVE BINARIES: ${requested.size} value-hash(es) requested for mg=$address: ${requested.map(_.value.take(12)).mkString(",")}"
                    ) >>
                      serveFromFinalized >>
                      serveFromOrphanBuffer >>
                      Async[F].delay(responseObserver.onCompleted())
                  }
                }
          }
        }
    }
  }
}
