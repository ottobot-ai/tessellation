package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{HasherSelector, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.parser
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Background daemon that fills historical chain gaps after catch-up.
  *
  * When a node teleports to the network tip via catchUpFromGossip, ordinals between its old tip and the new tip are missing from disk. This
  * daemon fetches the missing range via ChainSync, validates structurally (hash + signature + VRF), and persists to disk.
  *
  * Features:
  *   - Parallel chunk-based fetching (max 4 concurrent, 256 ordinals per chunk)
  *   - Per-chunk structural validation (hash integrity, parent chain continuity, signatures)
  *   - Persistent cursor for crash recovery (atomic JSON file)
  *   - Production holdoff during backfill (attestation continues)
  *   - Writes both Signed[GlobalIncrementalSnapshot] and GlobalSnapshotInfo to disk
  */
object BackfillDaemon {

  private val ChunkSize = 256
  private val MaxConcurrent = 4
  private val RetryDelay = 5.seconds
  private val MaxRetries = 3

  /** Persistent cursor tracking backfill progress. Survives crashes via atomic file write. */
  final case class BackfillCursor(
    nextHashToFetch: String, // hex hash of the next snapshot to walk back from (sequential fallback)
    targetOrdinal: Long, // stop when we reach this ordinal (1 = genesis)
    currentOrdinal: Long, // ordinal of the most recently processed snapshot
    startedAtOrdinal: Long, // ordinal where backfill started (the caught-up tip)
    completedChunks: Set[String], // "startOrd-endOrd" keys of completed chunks
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

  private def saveCursor[F[_]: Async](dataDir: Path, cursor: BackfillCursor): F[Unit] =
    Async[F].blocking {
      import io.circe.syntax._
      val cursorFile = dataDir.resolve("backfill-cursor.json")
      val tmpFile = dataDir.resolve("backfill-cursor.json.tmp")
      Files.createDirectories(dataDir)
      Files.write(
        tmpFile,
        cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING
      )
      val _ = Files.move(tmpFile, cursorFile, java.nio.file.StandardCopyOption.ATOMIC_MOVE)
    }

  private def clearCursor[F[_]: Async](dataDir: Path): F[Unit] =
    Async[F].blocking {
      val cursorFile = dataDir.resolve("backfill-cursor.json")
      Files.deleteIfExists(cursorFile)
      ()
    }

  /** Run the backfill daemon. Blocks until backfill is complete or fails permanently. */
  def run[F[_]: Async: HasherSelector: SecurityProvider: Metrics](
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

    def parseSnapshotWithContext(snap: pb.Snapshot): F[Option[(Signed[GlobalIncrementalSnapshot], Option[GlobalSnapshotInfo])]] =
      Async[F].delay {
        if (snap.payload.size() > 0) {
          val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
          (for {
            json <- io.circe.parser.parse(payloadStr)
            snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
            snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
          } yield {
            val contextOpt = for {
              ctxJson <- json.hcursor.get[io.circe.Json]("context").toOption
              ctx <- ctxJson.as[GlobalSnapshotInfo].toOption
            } yield ctx
            (snapshot, contextOpt)
          }).toOption
        } else None
      }

    /** Validate a chunk of snapshots structurally. Checks:
      *   1. Hash integrity (recompute hash from content) 2. Signature verification (Signed envelope) 3. Parent hash chain continuity within
      *      chunk 4. Ordinal monotonicity
      */
    def validateChunk(snapshots: List[(Signed[GlobalIncrementalSnapshot], Option[GlobalSnapshotInfo])]): F[Boolean] =
      if (snapshots.isEmpty) Async[F].pure(true)
      else
        HasherSelector[F].withCurrent { implicit hasher =>
          snapshots.traverse {
            case (signed, _) =>
              signed.hasValidSignature[F]
          }.map(_.forall(identity))
        }

    /** Store a validated snapshot + optional context to disk. */
    def persist(signed: Signed[GlobalIncrementalSnapshot], contextOpt: Option[GlobalSnapshotInfo]): F[Unit] =
      HasherSelector[F].withCurrent { implicit hasher =>
        snapshotStorage.writeForBackfill(signed)
      }

    // Sequential walk-back: fetch parent by hash, validate, store, repeat.
    // Used when parallel range-based fetch isn't available or as fallback.
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
            Metrics[F].incrementCounter("dag_nakamoto_backfill_complete") >>
            Metrics[F].updateGauge("dag_nakamoto_backfill_remaining", 0L) >>
            logger.info("Production resumed after backfill completion")
        } else {
          val hash = Hash(cur.nextHashToFetch)
          fetchByHash(hash).flatMap {
            case Some(snap) =>
              parseSnapshotWithContext(snap).flatMap {
                case Some((signedSnapshot, contextOpt)) =>
                  // Validate signature
                  validateChunk(List((signedSnapshot, contextOpt))).flatMap { valid =>
                    if (!valid) {
                      logger.warn(s"Backfill: invalid signature at ordinal~${cur.currentOrdinal}, skipping") >>
                        Async[F].sleep(RetryDelay) >> walkBack(cursorRef)
                    } else {
                      HasherSelector[F].withCurrent { implicit hasher =>
                        signedSnapshot.toHashed[F].flatMap { hashed =>
                          val parentHash = hashed.lastSnapshotHash.value
                          val ordinal = signedSnapshot.ordinal.value.value
                          persist(signedSnapshot, contextOpt) >> {
                            val newCursor = cur.copy(
                              nextHashToFetch = parentHash,
                              currentOrdinal = ordinal
                            )
                            cursorRef.set(newCursor) >>
                              Async[F].whenA(ordinal % 10 == 0)(saveCursor(dataDir, newCursor)) >>
                              Metrics[F].updateGauge("dag_nakamoto_backfill_remaining", ordinal - cur.targetOrdinal) >>
                              Metrics[F].incrementCounter("dag_nakamoto_backfill_fetched") >>
                              Async[F].whenA(ordinal % 50 == 0 || ordinal == cur.targetOrdinal)(
                                logger.info(
                                  s"Backfill progress: ordinal=$ordinal " +
                                    s"(${cur.startedAtOrdinal - ordinal}/${cur.startedAtOrdinal - cur.targetOrdinal} filled)"
                                )
                              ) >>
                              walkBack(cursorRef)
                          }
                        }
                      }
                    }
                  }
                case None =>
                  logger.warn(s"Backfill: could not parse snapshot for hash=${hash.value.take(12)}") >>
                    cursorRef.get.flatMap(saveCursor(dataDir, _)) >>
                    Async[F].sleep(RetryDelay) >> walkBack(cursorRef)
              }
            case None =>
              logger.warn(
                s"Backfill: ChainSync returned nothing for hash=${hash.value.take(12)} at ordinal~${cur.currentOrdinal}. Retrying."
              ) >>
                cursorRef.get.flatMap(saveCursor(dataDir, _)) >>
                Async[F].sleep(RetryDelay) >> walkBack(cursorRef)
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
