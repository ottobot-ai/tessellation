package io.constellationnetwork.storage.durable

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.{FileChannel, OverlappingFileLockException}
import java.nio.file._

import cats.effect.{Async, Resource}
import cats.syntax.all._

import scala.util.control.NonFatal

sealed abstract class DurableFileError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object DurableFileError {
  final case class DirectoryAlreadyOwned(path: Path)
      extends DurableFileError(s"Durable storage directory is already owned by another live process: $path")

  final case class DirectoryLockFailed(path: Path, cause0: Throwable)
      extends DurableFileError(s"Unable to acquire or release durable storage directory lock: $path", cause0)

  final case class AtomicMoveRequired(path: Path, cause0: Throwable)
      extends DurableFileError(s"Atomic replacement is required for durable storage: $path", cause0)

  final case class TargetHasNoParent(path: Path) extends DurableFileError(s"Durable replacement target must have a parent directory: $path")
}

/** Minimal filesystem algebra for crash-safe local artifacts.
  *
  * The exclusive lock is held for the complete [[Resource]] lifetime. Atomic replacement has no non-atomic fallback. Callers remain
  * responsible for defining artifact formats, readback validation, and translating these storage errors into domain-specific failures.
  */
trait DurableFileOps[F[_]] {
  def acquireExclusiveLock(path: Path): Resource[F, Unit]
  def createDirectories(path: Path): F[Unit]
  def createTempFile(directory: Path, prefix: String, suffix: String): F[Path]
  def write(path: Path, bytes: Array[Byte]): F[Unit]
  def forceFile(path: Path): F[Unit]
  def atomicMoveReplace(source: Path, target: Path): F[Unit]
  def forceDirectory(path: Path): F[Unit]
  def openInput(path: Path): Resource[F, InputStream]
  def isDirectory(path: Path): F[Boolean]
  def deleteIfExists(path: Path): F[Unit]
}

object DurableFileOps {
  def nio[F[_]: Async]: DurableFileOps[F] = new DurableFileOps[F] {
    def acquireExclusiveLock(path: Path): Resource[F, Unit] =
      Resource.make {
        Async[F].blocking {
          val channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)

          def closeAfterFailure(failure: Throwable): Unit =
            try channel.close()
            catch { case NonFatal(closeFailure) => failure.addSuppressed(closeFailure) }

          try {
            val lock =
              try channel.tryLock()
              catch { case _: OverlappingFileLockException => null }

            if (lock eq null) throw DurableFileError.DirectoryAlreadyOwned(path)
            else channel -> lock
          } catch {
            case failure: DurableFileError =>
              closeAfterFailure(failure)
              throw failure
            case NonFatal(failure) =>
              val typed = DurableFileError.DirectoryLockFailed(path, failure)
              closeAfterFailure(typed)
              throw typed
          }
        }
      } {
        case (channel, lock) =>
          Async[F].blocking {
            var releaseFailure: Throwable = null
            try lock.release()
            catch { case NonFatal(failure) => releaseFailure = failure }
            try channel.close()
            catch {
              case NonFatal(failure) if releaseFailure ne null => releaseFailure.addSuppressed(failure)
              case NonFatal(failure)                           => releaseFailure = failure
            }
            if (releaseFailure ne null) throw DurableFileError.DirectoryLockFailed(path, releaseFailure)
          }
      }.void

    def createDirectories(path: Path): F[Unit] = Async[F].blocking(Files.createDirectories(path)).void

    def createTempFile(directory: Path, prefix: String, suffix: String): F[Path] =
      Async[F].blocking(Files.createTempFile(directory, prefix, suffix))

    def write(path: Path, bytes: Array[Byte]): F[Unit] = Async[F].blocking {
      val channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)
      try {
        val buffer = ByteBuffer.wrap(bytes)
        while (buffer.hasRemaining) channel.write(buffer)
      } finally channel.close()
    }

    def forceFile(path: Path): F[Unit] = Async[F].blocking {
      val channel = FileChannel.open(path, StandardOpenOption.WRITE)
      try channel.force(true)
      finally channel.close()
    }

    def atomicMoveReplace(source: Path, target: Path): F[Unit] =
      Async[F]
        .blocking(Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING))
        .void
        .adaptError {
          case error: AtomicMoveNotSupportedException => DurableFileError.AtomicMoveRequired(target, error)
        }

    def forceDirectory(path: Path): F[Unit] = Async[F].blocking {
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      try channel.force(true)
      finally channel.close()
    }

    def openInput(path: Path): Resource[F, InputStream] =
      Resource.make(Async[F].blocking(Files.newInputStream(path, StandardOpenOption.READ)))(stream => Async[F].blocking(stream.close()))

    def isDirectory(path: Path): F[Boolean] = Async[F].blocking(Files.isDirectory(path))

    def deleteIfExists(path: Path): F[Unit] = Async[F].blocking(Files.deleteIfExists(path)).void
  }
}
