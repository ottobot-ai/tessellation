package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.parser
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Background daemon that fills historical chain gaps after catch-up.
  *
  * When a node teleports to the network tip via catchUpFromGossip, ordinals between its old tip and the new tip are missing from disk. This
  * daemon walks backward from the caught-up tip through parent hashes, fetching each snapshot via the existing ChainSync protocol, validating
  * structurally (hash + signature), and persisting to disk.
  *
  * Production (VRF election) is paused during backfill. Attestation continues.
  *
  * Uses existing FetchSnapshots RPC (hash-based, sequential). Parallel range-based fetching is a future optimization.
  */
object BackfillDaemon {

  /** Persistent cursor tracking backfill progress. Survives crashes via atomic file write. */
  final case class BackfillCursor(
    nextHashToFetch: String, // hex hash of the next snapshot to fetch (walking backward)
    targetOrdinal: Long, // stop when we reach this ordinal (1 = genesis)
    currentOrdinal: Long, // ordinal of the last successfully stored snapshot
    startedAtOrdinal: Long, // ordinal where backfill started (the caught-up tip)
    createdAtMs: Long
  )

  object BackfillCursor {
    import io.circe.{Decoder, Encoder}
    import io.circe.generic.semiauto._
    implicit val encoder: Encoder[BackfillCursor] = deriveEncoder
    implicit val decoder: Decoder[BackfillCursor] = deriveDecoder
  }

  /** Read the backfill cursor from disk. Returns None if no backfill is in progress. */
  def loadCursor[F[_]: Async](dataDir: Path): F[Option[BackfillCursor]] =
    Async[F].blocking {
      val cursorFile = dataDir.resolve("backfill-cursor.json")
      if (Files.exists(cursorFile)) {
        val json = new String(Files.readAllBytes(cursorFile), StandardCharsets.UTF_8)
        parser.decode[BackfillCursor](json).toOption
      } else None
    }

  /** Persist cursor atomically (write to temp, rename). */
  private def saveCursor[F[_]: Async](dataDir: Path, cursor: BackfillCursor): F[Unit] =
    Async[F].blocking {
      import io.circe.syntax._
      val cursorFile = dataDir.resolve("backfill-cursor.json")
      val tmpFile = dataDir.resolve("backfill-cursor.json.tmp")
      Files.write(tmpFile, cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
      val _ = Files.move(tmpFile, cursorFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
    }

  /** Clear cursor (backfill complete). */
  private def clearCursor[F[_]: Async](dataDir: Path): F[Unit] =
    Async[F].blocking {
      val cursorFile = dataDir.resolve("backfill-cursor.json")
      Files.deleteIfExists(cursorFile)
      ()
    }

  /** Run the backfill daemon. Blocks until backfill is complete or fails.
    *
    * @param cursor
    *   Starting cursor (from catch-up or crash recovery)
    * @param channel
    *   gRPC channel to the Go sidecar (for ChainSync)
    * @param snapshotFileStorage
    *   Disk storage for writing backfilled snapshots
    * @param productionGate
    *   Gate to pause/resume VRF production
    * @param dataDir
    *   Directory for persisting cursor file
    */
  def run[F[_]: Async: HasherSelector: SecurityProvider](
    cursor: BackfillCursor,
    channel: ManagedChannel,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    productionGate: ProductionGate[F],
    dataDir: Path
  ): F[Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("BackfillDaemon")
    val stub = pb.ChainSyncOutboundGrpc.blockingStub(channel)

    def fetchByHash(hash: Hash): F[Option[pb.Snapshot]] = Async[F].blocking {
      val hashBytes = com.google.protobuf.ByteString.copyFrom(hash.value.getBytes)
      val request = pb.FetchSnapshotsRequest(hashes = Seq(hashBytes))
      stub.fetchSnapshots(request).toList.headOption
    }

    def parseSnapshot(snap: pb.Snapshot): F[Option[Signed[GlobalIncrementalSnapshot]]] = Async[F].delay {
      if (snap.payload.size() > 0) {
        val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
        (for {
          json <- io.circe.parser.parse(payloadStr)
          snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
          snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
        } yield snapshot).toOption
      } else None
    }

    // Sequential walk-back: fetch parent, validate, store, repeat
    def walkBack(cursorRef: Ref[F, BackfillCursor]): F[Unit] =
      cursorRef.get.flatMap { cur =>
        if (cur.currentOrdinal <= cur.targetOrdinal) {
          // Reached target — done
          logger.info(
            s"Backfill complete: filled ordinals ${cur.startedAtOrdinal} down to ${cur.targetOrdinal} " +
              s"(${cur.startedAtOrdinal - cur.targetOrdinal} snapshots)"
          ) >>
            clearCursor(dataDir) >>
            productionGate.resume(ProductionGate.ChainBackfill) >>
            logger.info("Production resumed after backfill completion")
        } else {
          val hash = Hash(cur.nextHashToFetch)
          fetchByHash(hash).flatMap {
            case Some(snap) =>
              parseSnapshot(snap).flatMap {
                case Some(signedSnapshot) =>
                  // Structural validation: write to disk (write verifies hash integrity)
                  HasherSelector[F].withCurrent { implicit hasher =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      val parentHash = hashed.lastSnapshotHash.value
                      val ordinal = signedSnapshot.ordinal.value.value
                      snapshotStorage.writeForBackfill(signedSnapshot) >> {
                          val newCursor = cur.copy(
                            nextHashToFetch = parentHash,
                            currentOrdinal = ordinal
                          )
                          cursorRef.set(newCursor) >>
                            // Persist cursor every 10 snapshots for crash recovery
                            Async[F].whenA(ordinal % 10 == 0)(saveCursor(dataDir, newCursor)) >>
                            Async[F].whenA(ordinal % 50 == 0)(
                              logger.info(s"Backfill progress: ordinal=$ordinal (${cur.startedAtOrdinal - ordinal}/${cur.startedAtOrdinal - cur.targetOrdinal} filled)")
                            ) >>
                            walkBack(cursorRef)
                        }
                    }
                  }
                case None =>
                  logger.warn(s"Backfill: could not parse snapshot payload for hash=${hash.value.take(12)}") >>
                    // Save cursor and retry later
                    cursorRef.get.flatMap(saveCursor(dataDir, _)) >>
                    Async[F].sleep(scala.concurrent.duration.FiniteDuration(5, "seconds")) >>
                    walkBack(cursorRef)
              }
            case None =>
              // Peer doesn't have this snapshot — wait and retry
              logger.warn(s"Backfill: ChainSync returned nothing for hash=${hash.value.take(12)} at ordinal~${cur.currentOrdinal}. Retrying in 5s.") >>
                cursorRef.get.flatMap(saveCursor(dataDir, _)) >>
                Async[F].sleep(scala.concurrent.duration.FiniteDuration(5, "seconds")) >>
                walkBack(cursorRef)
          }
        }
      }

    for {
      _ <- productionGate.pause(ProductionGate.ChainBackfill)
      _ <- logger.info(
        s"Starting backfill: ordinal ${cursor.startedAtOrdinal} → ${cursor.targetOrdinal} " +
          s"(${cursor.startedAtOrdinal - cursor.targetOrdinal} snapshots to fetch)"
      )
      _ <- saveCursor(dataDir, cursor)
      cursorRef <- Ref.of[F, BackfillCursor](cursor)
      _ <- walkBack(cursorRef)
    } yield ()
  }
}
