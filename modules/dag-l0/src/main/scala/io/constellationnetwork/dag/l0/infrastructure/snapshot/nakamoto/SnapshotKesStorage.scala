package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.file.{Files, Path, StandardCopyOption}

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.security.hash.Hash

/** Durable public evidence needed to replay a snapshot fetched after the in-memory chain entry was evicted.
  *
  * A snapshot's KES signature is intentionally outside the signed snapshot body because it signs that body's hash. It therefore has to be
  * persisted beside the snapshot; accepting an empty signature during recovery would turn KES into a live-gossip-only check. Files are
  * content-addressed by the already-verified snapshot hash and contain no private key material.
  */
object SnapshotKesStorage {

  private val HashPattern = "(?i)[0-9a-f]{64}".r
  private val DirectoryName = "nakamoto-snapshot-kes-v1"

  private def pathFor(dataDir: Path, hash: Hash): Either[Throwable, Path] =
    hash.value match {
      case HashPattern() => Right(dataDir.resolve(DirectoryName).resolve(hash.value.toLowerCase))
      case _             => Left(new IllegalArgumentException(s"Invalid snapshot hash for KES storage: ${hash.value}"))
    }

  def put[F[_]: Async](dataDir: Path, hash: Hash, signature: Array[Byte]): F[Unit] =
    if (signature.isEmpty)
      Async[F].raiseError(new IllegalArgumentException(s"Refusing to persist an empty KES signature for $hash"))
    else
      Async[F].fromEither(pathFor(dataDir, hash)).flatMap { target =>
        Async[F].blocking {
          val directory = target.getParent
          Files.createDirectories(directory)
          if (Files.exists(target)) {
            val existing = Files.readAllBytes(target)
            if (!java.util.Arrays.equals(existing, signature))
              throw new IllegalStateException(s"Conflicting KES evidence for snapshot ${hash.value}")
          } else {
            val temporary = Files.createTempFile(directory, s"${hash.value}.", ".tmp")
            try {
              Files.write(temporary, signature)
              try {
                val _ = Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE)
              }
              catch {
                case _: java.nio.file.FileAlreadyExistsException =>
                  val existing = Files.readAllBytes(target)
                  if (!java.util.Arrays.equals(existing, signature))
                    throw new IllegalStateException(s"Conflicting KES evidence for snapshot ${hash.value}")
                case _: java.nio.file.AtomicMoveNotSupportedException =>
                  val _ = Files.move(temporary, target)
              }
            } finally {
              val _ = Files.deleteIfExists(temporary)
            }
          }
        }
      }

  def get[F[_]: Async](dataDir: Path, hash: Hash): F[Option[Array[Byte]]] =
    pathFor(dataDir, hash) match {
      case Left(_) => none[Array[Byte]].pure[F]
      case Right(path) =>
        Async[F].blocking {
          if (Files.isRegularFile(path)) Some(Files.readAllBytes(path)) else None
        }
    }
}
