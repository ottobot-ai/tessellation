package io.constellationnetwork.node.shared.infrastructure.snapshot.storage

import java.io.{FileOutputStream, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files => JFiles, StandardCopyOption}
import java.util.UUID

import cats.effect._
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.storage.LocalFileSystemStorage

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import fs2.Stream
import fs2.concurrent.SignallingRef
import fs2.io.file._
import io.circe.syntax._
import io.circe.{Encoder, Printer}
import org.http4s._
import org.http4s.headers._

@derive(eqv, show, encoder, decoder)
case class LastCheckpointInfo(
  ordinal: SnapshotOrdinal,
  epochProgress: EpochProgress,
  hash: Hash
)

object LastCheckpointInfo {
  def empty(): LastCheckpointInfo =
    LastCheckpointInfo(
      SnapshotOrdinal.MinValue,
      EpochProgress.MinValue,
      Hash.empty
    )
}

final class CombinedSnapshotCheckpointFileSystemStorage[
  F[_]: Async: Files,
  S <: Snapshot,
  SI <: SnapshotInfo[_]
](
  path: Path,
  lastSnapshotInfo: SignallingRef[F, LastCheckpointInfo],
  concurrentStreams: Semaphore[F],
  openWriter: Path => Resource[F, OutputStreamWriter]
)(
  implicit encSigned: Encoder[Signed[S]],
  encState: Encoder[SI]
) extends LocalFileSystemStorage[F, Array[Byte]](path) {
  // 2026-06-10 (run bf65au6y3 forensics): retention MUST out-live the head→finality gap. Checkpoints are
  // written at HEAD ordinals every `checkpointIntervalEpochs`, but the Nakamoto `FinalizedSnapshotReader`
  // only serves a checkpoint once it is AT-OR-BELOW the finalized ordinal. Under depth-rail finality the
  // gap is exactly k₁ (32 dev / 255 prod) — with only 2 retained files at 5-epoch cadence (10-epoch
  // coverage), every checkpoint was DELETED before finality could reach it and `/latest/combined` starved
  // mathematically (404 on a healthy, producing node). The old value silently assumed T_count's ~1-3-ord
  // gap, i.e. the serving layer depended on the FAST path of finality. 64 files × 5 epochs = 320-epoch
  // coverage ≥ prod k₁ with margin; checkpoint files are the bound, not size (each ~1-5 MB).
  private val maxCheckpointsStored = 64
  private val checkpointIntervalEpochs = 5

  private def writeJsonTupleStream(
    ordinal: SnapshotOrdinal,
    snapshot: Signed[S],
    state: SI
  ): F[Unit] = {
    val filePath = path / ordinal.value.value.toString
    val temporaryPath = path / s".${ordinal.value.value}.${UUID.randomUUID().toString}.tmp"

    val writeTemporary = openWriter(temporaryPath).use { writer =>
      val printer = Printer.noSpaces.copy(dropNullValues = true)
      Concurrent[F].blocking {
        writer.append('[')
        printer.unsafePrintToAppendable(snapshot.asJson, writer)
        writer.append(',')
        printer.unsafePrintToAppendable(state.asJson, writer)
        writer.append(']')
        ()
      }
    }

    val publish = Concurrent[F].blocking {
      JFiles.move(
        temporaryPath.toNioPath,
        filePath.toNioPath,
        StandardCopyOption.ATOMIC_MOVE,
        StandardCopyOption.REPLACE_EXISTING
      )
      ()
    }

    val cleanupTemporary = Concurrent[F].blocking(JFiles.deleteIfExists(temporaryPath.toNioPath)).void

    // Atomic replacement guarantees whole-generation visibility to concurrent readers. This is not an fsync/crash-durability claim.
    Async[F].guarantee(
      writeTemporary >> Async[F].uncancelable(_ => publish),
      cleanupTemporary
    )
  }

  def getAsHttpResponse(ordinal: SnapshotOrdinal): F[Option[Response[F]]] = {
    val file = path / ordinal.value.value.toString

    Files[F].exists(file).flatMap {
      case false => Concurrent[F].pure(None)
      case true =>
        Files[F].size(file).flatMap { size =>
          val fileStream: Stream[F, Byte] = Files[F].readAll(file, 64 * 1024, Flags.Read)
          val bodyWithPermit: Stream[F, Byte] =
            Stream.resource(concurrentStreams.permit).flatMap { _ =>
              fileStream
            }

          Response[F](
            status = Status.Ok,
            headers = Headers(
              `Content-Type`(MediaType.application.json),
              `Content-Length`(size)
            ),
            body = bodyWithPermit
          ).some.pure[F]
        }
    }
  }

  def getAsStream(ordinal: SnapshotOrdinal): F[Option[Stream[F, Byte]]] = {
    val file = path / ordinal.value.value.toString
    Files[F].exists(file).flatMap {
      case false => Concurrent[F].pure(None)
      case true =>
        val fileStream: Stream[F, Byte] =
          Stream.resource(concurrentStreams.permit).flatMap { _ =>
            Files[F].readAll(file, 64 * 1024, Flags.Read)
          }
        fileStream.some.pure[F]
    }
  }

  private def readFrom(handle: FileHandle[F]): Stream[F, Byte] =
    Stream.unfoldChunkEval(0L) { offset =>
      handle
        .read(64 * 1024, offset)
        .map(_.filter(_.nonEmpty).map(chunk => (chunk, offset + chunk.size.toLong)))
    }

  /** Validate and serve one exact open checkpoint file without reopening the ordinal path between the check and the response body.
    * Validation may compile `bytes` multiple times: every run reads the same open file handle from offset zero. The body holds the file
    * handle and stream permit until the HTTP consumer completes or cancels it. A validation or I/O failure is fail-closed (`None`).
    */
  def getAsValidatedHttpResponse(
    ordinal: SnapshotOrdinal,
    validate: Stream[F, Byte] => F[Boolean],
    transform: Stream[F, Byte] => Stream[F, Byte]
  ): F[Option[Response[F]]] = {
    val file = path / ordinal.value.value.toString
    val opened = concurrentStreams.permit.flatMap(_ => Files[F].open(file, Flags.Read))

    Files[F]
      .exists(file)
      .ifM(
        Ref.of[F, F[Unit]](Async[F].unit).flatMap { releaseOnCancel =>
          Async[F].uncancelable { poll =>
            poll(opened.allocated).attempt.flatMap {
              case Left(_) => Option.empty[Response[F]].pure[F]
              case Right((handle, rawRelease)) =>
                val installGuard =
                  CombinedSnapshotCheckpointFileSystemStorage
                    .releaseOnce(rawRelease)
                    .flatTap(releaseOnCancel.set)
                    .handleErrorWith(error => rawRelease.attempt >> error.raiseError[F, F[Unit]])

                installGuard.flatMap { release =>
                  val bytes = readFrom(handle)
                  val transferOwnership =
                    validate(bytes).attempt.flatMap {
                      case Right(true) =>
                        Async[F].delay(transform(bytes)).attempt.flatMap {
                          case Right(body) =>
                            Response[F](
                              status = Status.Ok,
                              headers = Headers(`Content-Type`(MediaType.application.json)),
                              body = body.onFinalize(release)
                            ).some.pure[F]
                          case Left(_) => release.as(Option.empty[Response[F]])
                        }
                      case _ => release.as(Option.empty[Response[F]])
                    }

                  // The outer cancellation handler also covers a pending cancellation observed as the uncancelable allocation/guard
                  // scope returns. On success the response body owns the same release-once token.
                  poll(transferOwnership)
                }
            }
          }
            .onCancel(releaseOnCancel.get.flatten)
        },
        Option.empty[Response[F]].pure[F]
      )
  }

  def getAsValidatedHttpResponse(
    ordinal: SnapshotOrdinal,
    validate: Stream[F, Byte] => F[Boolean]
  ): F[Option[Response[F]]] =
    getAsValidatedHttpResponse(ordinal, validate, identity)

  /** Validate an exact checkpoint while holding one open handle for the whole check. Unlike the response method, this always releases the
    * handle before returning and is intended for metadata/fallback decisions that must distinguish absence from a forbidden file.
    */
  def validateExact(ordinal: SnapshotOrdinal, validate: Stream[F, Byte] => F[Boolean]): F[Boolean] = {
    val file = path / ordinal.value.value.toString
    val opened = concurrentStreams.permit.flatMap(_ => Files[F].open(file, Flags.Read))

    Files[F]
      .exists(file)
      .ifM(
        opened.use(handle => validate(readFrom(handle))).handleError(_ => false),
        false.pure[F]
      )
  }

  def exists(ordinal: SnapshotOrdinal): F[Boolean] =
    exists(toOrdinalName(ordinal))

  private def cleanupOldCombinedSnapshots(): F[Unit] =
    listStoredOrdinals.flatMap { ordinalsStream =>
      ordinalsStream.compile.toList.flatMap { ordinals =>
        val sortedOrdinals = ordinals.sorted(Ordering[SnapshotOrdinal].reverse)
        if (sortedOrdinals.length > maxCheckpointsStored) {
          val ordinalsToDelete = sortedOrdinals.drop(maxCheckpointsStored)
          ordinalsToDelete.traverse_(delete)
        } else Concurrent[F].unit
      }
    }

  def tryWrite(ordinal: SnapshotOrdinal, snapshot: Signed[S], state: SI, snapshotHash: Hash): F[Unit] =
    lastSnapshotInfo.get.flatMap { last =>
      val shouldUpdate = snapshot.epochProgress.value.value % checkpointIntervalEpochs === 0 || last === LastCheckpointInfo.empty()
      if (shouldUpdate) {
        writeJsonTupleStream(ordinal, snapshot, state) >>
          cleanupOldCombinedSnapshots() >>
          lastSnapshotInfo.set(LastCheckpointInfo(ordinal, snapshot.epochProgress, snapshotHash))
      } else {
        Concurrent[F].unit
      }
    }

  def delete(ordinal: SnapshotOrdinal): F[Unit] =
    delete(toOrdinalName(ordinal))

  def listStoredOrdinals: F[Stream[F, SnapshotOrdinal]] =
    listFiles.map {
      _.map(_.name)
        .map(_.toLongOption)
        .map(_.flatMap(SnapshotOrdinal(_)))
        .collect { case Some(a) => a }
    }

  def deleteAbove(ordinal: SnapshotOrdinal): F[Unit] =
    listStoredOrdinals.flatMap {
      _.filter(_ > ordinal)
        .evalMap(delete)
        .compile
        .drain
    }

  def getLatestOrdinal: F[Option[SnapshotOrdinal]] =
    listStoredOrdinals.flatMap { ordinalsStream =>
      ordinalsStream.compile.toList.map { ordinals =>
        if (ordinals.isEmpty) None
        else Some(ordinals.max)
      }
    }

  /** Highest stored checkpoint ordinal at or below the given ceiling, or None. Used by finality-gated HTTP routes so the fallback returned
    * from `/latest/combined` never includes a checkpoint beyond the finalized ordinal — even though newer checkpoints may exist on disk.
    */
  def getLatestOrdinalAtOrBelow(ceiling: SnapshotOrdinal): F[Option[SnapshotOrdinal]] =
    listStoredOrdinals.flatMap { ordinalsStream =>
      ordinalsStream.compile.toList.map { ordinals =>
        val eligible = ordinals.filter(_.value.value <= ceiling.value.value)
        if (eligible.isEmpty) None
        else Some(eligible.max)
      }
    }

  def getLatestAsHttpResponse: F[Option[Response[F]]] =
    getLatestOrdinal.flatMap {
      case Some(ordinal) => getAsHttpResponse(ordinal)
      case None          => Concurrent[F].pure(None)
    }

  def getLatestAsHttpResponseAtOrBelow(ceiling: SnapshotOrdinal): F[Option[Response[F]]] =
    getLatestOrdinalAtOrBelow(ceiling).flatMap {
      case Some(ordinal) => getAsHttpResponse(ordinal)
      case None          => Concurrent[F].pure(None)
    }

  def getLatestAsStream: F[Option[Stream[F, Byte]]] =
    getLatestOrdinal.flatMap {
      case Some(ordinal) => getAsStream(ordinal)
      case None          => Concurrent[F].pure(None)
    }

  def getLatestCheckpointInfo: F[LastCheckpointInfo] =
    lastSnapshotInfo.get

  private def toOrdinalName(ordinal: SnapshotOrdinal): String =
    ordinal.value.value.toString
}

object CombinedSnapshotCheckpointFileSystemStorage {

  private[storage] def releaseOnce[F[_]: Async](release: F[Unit]): F[F[Unit]] =
    (Ref.of[F, Boolean](false), Deferred[F, Either[Throwable, Unit]]).mapN { (claimed, completed) =>
      val runRelease = Async[F].uncancelable { _ =>
        release.attempt.flatTap(result => completed.complete(result).void).flatMap(_.liftTo[F])
      }

      Async[F].uncancelable { _ =>
        claimed.modify {
          case false => true -> runRelease
          case true  => true -> completed.get.flatMap(_.liftTo[F])
        }.flatten
      }
    }

  private def defaultOpenWriter[F[_]: Async](temporaryPath: Path): Resource[F, OutputStreamWriter] = {
    val output = Resource.make(
      Async[F].blocking {
        val file = new java.io.File(temporaryPath.toString)
        Option(file.getParentFile).foreach(_.mkdirs())
        new FileOutputStream(file)
      }
    )(stream => Async[F].blocking(stream.close()))

    output.flatMap { stream =>
      Resource.make(Async[F].blocking(new OutputStreamWriter(stream, UTF_8)))(writer => Async[F].blocking(writer.close()))
    }
  }

  def make[
    F[_]: Async: Files,
    S <: Snapshot,
    SI <: SnapshotInfo[_]
  ](
    path: Path
  )(implicit encSigned: Encoder[Signed[S]], encState: Encoder[SI]): F[CombinedSnapshotCheckpointFileSystemStorage[F, S, SI]] =
    makeWithWriter(path, defaultOpenWriter[F])

  private[storage] def makeWithWriter[
    F[_]: Async: Files,
    S <: Snapshot,
    SI <: SnapshotInfo[_]
  ](
    path: Path,
    openWriter: Path => Resource[F, OutputStreamWriter]
  )(implicit encSigned: Encoder[Signed[S]], encState: Encoder[SI]): F[CombinedSnapshotCheckpointFileSystemStorage[F, S, SI]] = for {
    lastCheckpointInfo <- SignallingRef.of[F, LastCheckpointInfo](LastCheckpointInfo.empty())
    concurrentStreams <- Semaphore[F](5)
    storage = new CombinedSnapshotCheckpointFileSystemStorage[F, S, SI](path, lastCheckpointInfo, concurrentStreams, openWriter)
    _ <- storage.createDirectoryIfNotExists().rethrowT
    // BOOT RESCAN (2026-06-10): a restart used to leave the tracked ref EMPTY even though servable
    // checkpoint files sat on disk — the Nakamoto `FinalizedSnapshotReader` then 404'd `/latest/combined`
    // until the next write landed AND finality overtook it (run bf65au6y3: a restarted node's combined
    // endpoint starved for 30+ min during a depth-rail window, stalling the e2e driver). Restore the
    // highest on-disk ordinal at boot; epochProgress/hash are sentinels (the storage holds Encoders only,
    // not Decoders) — serving keys on the ordinal + the file bytes, and the sentinels are replaced by the
    // next real `tryWrite` (≤ checkpointIntervalEpochs away). Note the sentinel also leaves the
    // `last === empty()` force-write check inert, which is correct: with files on disk there is nothing
    // to force.
    restored <- storage.listStoredOrdinals.flatMap(_.compile.toList).map(_.maxOption)
    _ <- restored match {
      case Some(maxOrd) => lastCheckpointInfo.set(LastCheckpointInfo(maxOrd, EpochProgress.MinValue, Hash.empty))
      case None         => Async[F].unit
    }
  } yield storage
}
