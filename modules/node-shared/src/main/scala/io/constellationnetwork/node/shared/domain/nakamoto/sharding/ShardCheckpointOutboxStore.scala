package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.io.{ByteArrayOutputStream, FileNotFoundException, InputStream}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.{ByteBuffer, ByteOrder}
import java.security.MessageDigest

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import io.constellationnetwork.storage.durable.{DurableAtomicWriter, DurableFileOps, DurableWriteHook}

/** Caller-selected local custody bounds. These values are not protocol or consensus parameters. */
private[node] final class ShardCheckpointOutboxLimits private (
  val maxOpaqueIdBytes: Int,
  val maxArtifactBytes: Long
) {
  private[sharding] val maxRecordBytes: Int =
    Math.addExact(ShardCheckpointOutboxStore.RecordOverhead, Math.addExact(maxOpaqueIdBytes, maxArtifactBytes.toInt))
}

private[node] object ShardCheckpointOutboxLimits {
  def from(
    maxOpaqueIdBytes: Int,
    maxArtifactBytes: Long
  ): Either[ShardCheckpointOutboxRejected.InvalidLimits, ShardCheckpointOutboxLimits] = {
    val errors = List(
      Option.when(maxOpaqueIdBytes <= 0)(s"maxOpaqueIdBytes must be positive, got $maxOpaqueIdBytes"),
      Option.when(maxArtifactBytes <= 0L)(s"maxArtifactBytes must be positive, got $maxArtifactBytes"),
      Option.when(maxArtifactBytes > Int.MaxValue.toLong)(
        s"maxArtifactBytes must fit the exact-byte in-memory API, got $maxArtifactBytes"
      ),
      Option.when(
        maxOpaqueIdBytes > 0 && maxArtifactBytes > 0L && maxArtifactBytes <= Int.MaxValue.toLong &&
          maxOpaqueIdBytes.toLong + maxArtifactBytes + ShardCheckpointOutboxStore.RecordOverhead > Int.MaxValue.toLong
      )("configured record bound exceeds the exact-byte in-memory API")
    ).flatten

    Either.cond(
      errors.isEmpty,
      new ShardCheckpointOutboxLimits(maxOpaqueIdBytes, maxArtifactBytes),
      ShardCheckpointOutboxRejected.InvalidLimits(errors.mkString("; "))
    )
  }
}

/** Store-bound expected-head token. It has no schema, codec, consensus identity, or authority outside this live store. */
private[node] final class ShardCheckpointOutboxHead private[sharding] (
  private[sharding] val occupiedChecksum: Option[Array[Byte]],
  private[sharding] val storeIdentity: AnyRef
)

/** A verified view of the one local outbox slot. Loaded custody is not permission to publish or attest to the bytes. */
private[node] sealed trait ShardCheckpointOutboxSnapshot {
  def head: ShardCheckpointOutboxHead
}

private[node] object ShardCheckpointOutboxSnapshot {
  final class Empty private[sharding] (val head: ShardCheckpointOutboxHead) extends ShardCheckpointOutboxSnapshot

  final class Occupied private[sharding] (
    val head: ShardCheckpointOutboxHead,
    val custody: DurablyCustodiedShardCheckpoint
  ) extends ShardCheckpointOutboxSnapshot
}

/** Store-local witness of verified durable custody only.
  *
  * This is deliberately not serializable, has no codec, and cannot be converted into a publish, validity, execution, quorum, or finality
  * capability. A restart may load this witness, but a separate future integration must revalidate the opaque artifact before any emission.
  */
private[node] final class DurablyCustodiedShardCheckpoint private[sharding] (
  val byteLength: Long,
  private[sharding] val opaqueId: Array[Byte],
  private[sharding] val recordChecksum: Array[Byte],
  private[sharding] val storeIdentity: AnyRef
) {
  def copyOpaqueId: Array[Byte] = opaqueId.clone()
}

private[node] sealed abstract class ShardCheckpointOutboxError(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

private[node] sealed abstract class ShardCheckpointOutboxRejected(message: String, cause: Throwable = null)
    extends ShardCheckpointOutboxError(message, cause)

private[node] object ShardCheckpointOutboxRejected {
  final case class InvalidLimits(detail: String)
      extends ShardCheckpointOutboxRejected(s"Invalid local shard-checkpoint outbox limits: $detail")

  final case class InvalidOpaqueId(detail: String)
      extends ShardCheckpointOutboxRejected(s"Rejected opaque shard-checkpoint outbox id: $detail")

  final case class InvalidArtifact(detail: String)
      extends ShardCheckpointOutboxRejected(s"Rejected opaque shard-checkpoint outbox bytes: $detail")

  final case class ForeignHead()
      extends ShardCheckpointOutboxRejected("Expected-head token belongs to a different live shard-checkpoint outbox store")

  final case class ForeignCustody()
      extends ShardCheckpointOutboxRejected("Durable-custody witness belongs to a different live shard-checkpoint outbox store")
}

private[node] sealed abstract class ShardCheckpointOutboxUnavailable(message: String, cause: Throwable = null)
    extends ShardCheckpointOutboxError(message, cause)

private[node] object ShardCheckpointOutboxUnavailable {
  final case class InvalidDirectory(path: Path, detail: String, cause0: Throwable = null)
      extends ShardCheckpointOutboxUnavailable(s"Unsafe local shard-checkpoint outbox directory at $path: $detail", cause0)

  final case class StoreClosed(path: Path)
      extends ShardCheckpointOutboxUnavailable(s"Local shard-checkpoint outbox store is closed: $path")

  final case class ArtifactTooLarge(actualBytes: Long, maximumBytes: Long)
      extends ShardCheckpointOutboxUnavailable(
        s"Opaque shard-checkpoint outbox artifact has $actualBytes bytes; local limit is $maximumBytes"
      )

  final case class DurableWriteFailed(path: Path, cause0: Throwable)
      extends ShardCheckpointOutboxUnavailable(s"Local shard-checkpoint outbox durable write failed at $path", cause0)

  final case class StorageAccessFailed(operation: String, path: Path, cause0: Throwable)
      extends ShardCheckpointOutboxUnavailable(
        s"Local shard-checkpoint outbox storage is unavailable during $operation at $path",
        cause0
      )
}

private[node] sealed abstract class ShardCheckpointOutboxRecoveryRequired(message: String, cause: Throwable = null)
    extends ShardCheckpointOutboxError(message, cause)

private[node] object ShardCheckpointOutboxRecoveryRequired {
  final case class ConflictingOccupiedSlot(path: Path)
      extends ShardCheckpointOutboxRecoveryRequired(
        s"Local shard-checkpoint outbox slot at $path contains different opaque bytes; authenticated recovery required"
      )

  final case class CorruptRecord(path: Path, detail: String, cause0: Throwable = null)
      extends ShardCheckpointOutboxRecoveryRequired(
        s"Corrupt local shard-checkpoint outbox record at $path: $detail; authenticated recovery required",
        cause0
      )

  final case class MissingOccupiedRecord(path: Path)
      extends ShardCheckpointOutboxRecoveryRequired(
        s"Expected occupied local shard-checkpoint outbox record is missing at $path; authenticated recovery required"
      )

  final case class UnexpectedDirectoryEntry(path: Path)
      extends ShardCheckpointOutboxRecoveryRequired(
        s"Unexpected entry in dedicated local shard-checkpoint outbox directory: $path; authenticated recovery required"
      )

  final case class DirectoryEnumerationLimitExceeded(path: Path, observedEntries: Int, maximumEntries: Int)
      extends ShardCheckpointOutboxRecoveryRequired(
        s"Local shard-checkpoint outbox directory at $path has at least $observedEntries entries; " +
          s"safe local bound is $maximumEntries; authenticated recovery required"
      )
}

/** Dark exact-byte custody/CAS boundary.
  *
  * The caller supplies both an opaque bounded identifier and exact opaque bytes. This API cannot derive a checkpoint or consensus identity,
  * validate a checkpoint, release a record, or publish anything. Successful installation means only that one local slot durably contains
  * those exact bytes after forced atomic replacement and verified readback.
  */
private[node] trait ShardCheckpointOutboxStore[F[_]] {
  def load: F[ShardCheckpointOutboxSnapshot]

  def install(
    expectedHead: ShardCheckpointOutboxHead,
    opaqueId: Array[Byte],
    exactBytes: Array[Byte]
  ): F[DurablyCustodiedShardCheckpoint]

  def readExact(custody: DurablyCustodiedShardCheckpoint): F[Array[Byte]]
}

private[node] object ShardCheckpointOutboxStore {
  import ShardCheckpointOutboxRecoveryRequired._
  import ShardCheckpointOutboxRejected._
  import ShardCheckpointOutboxSnapshot._
  import ShardCheckpointOutboxUnavailable._

  private[sharding] final class OutboxWriteArtifact private[sharding] (
    val opaqueIdBytes: Int,
    val artifactBytes: Long
  )

  private sealed trait Lifecycle
  private case object Open extends Lifecycle
  private case object Closed extends Lifecycle

  private final class LocalRecord(
    val opaqueId: Array[Byte],
    val exactBytes: Array[Byte],
    val checksum: Array[Byte]
  )

  private val LockFileName = ".shard-checkpoint-outbox.lock"
  private val RecordFileName = "held.local"
  private val TemporaryPrefix = s".$RecordFileName."
  private val TemporarySuffix = ".tmp"
  private val MaxDirectoryEntries = 64

  private val LocalMagic = 0x53434f5554425831L // "SCOUTBX1", a private storage envelope marker only.
  private val LocalFormat = 1
  private val ChecksumBytes = 32
  private val HeaderBytes = java.lang.Long.BYTES + (2 * java.lang.Integer.BYTES) + java.lang.Long.BYTES
  private[sharding] val RecordOverhead: Int = HeaderBytes + ChecksumBytes

  def resource[F[_]: Async](
    directory: Path,
    limits: ShardCheckpointOutboxLimits
  ): Resource[F, ShardCheckpointOutboxStore[F]] =
    resourceWith(directory, limits, DurableFileOps.nio[F], DurableWriteHook.noop[F, OutboxWriteArtifact])

  private[sharding] def resourceWith[F[_]: Async](
    directory: Path,
    limits: ShardCheckpointOutboxLimits,
    fileOps: DurableFileOps[F],
    writeHook: DurableWriteHook[F, OutboxWriteArtifact]
  ): Resource[F, ShardCheckpointOutboxStore[F]] =
    for {
      configuredLimits <- Resource.eval(
        Async[F].fromOption(Option(limits), ShardCheckpointOutboxRejected.InvalidLimits("null limits"))
      )
      root <- Resource.eval(prepareRoot(directory, fileOps))
      _ <- fileOps.acquireExclusiveLock(root.resolve(LockFileName))
      mutex <- Resource.eval(Semaphore[F](1L))
      lifecycle <- Resource.eval(Ref.of[F, Lifecycle](Open))
      store = new LiveStore[F](root, configuredLimits, fileOps, writeHook, mutex, lifecycle)
      _ <- Resource.eval(store.validateOnOpen)
      opened <- Resource.make(Async[F].pure(store: ShardCheckpointOutboxStore[F]))(_ => store.close)
    } yield opened

  private final class LiveStore[F[_]: Async](
    directory: Path,
    limits: ShardCheckpointOutboxLimits,
    fileOps: DurableFileOps[F],
    writeHook: DurableWriteHook[F, OutboxWriteArtifact],
    mutex: Semaphore[F],
    lifecycle: Ref[F, Lifecycle]
  ) extends ShardCheckpointOutboxStore[F] {
    private val storeIdentity = new AnyRef
    private val recordPath = directory.resolve(RecordFileName)
    private val writer = new DurableAtomicWriter[F](fileOps)

    def load: F[ShardCheckpointOutboxSnapshot] =
      withOpen(readRecord(recordPath).map(toSnapshot))

    def install(
      expectedHead: ShardCheckpointOutboxHead,
      opaqueId: Array[Byte],
      exactBytes: Array[Byte]
    ): F[DurablyCustodiedShardCheckpoint] =
      for {
        expected <- validateHead(expectedHead)
        capturedId <- captureId(opaqueId)
        capturedBytes <- captureArtifact(exactBytes)
        stored <- withOpen {
          readRecord(recordPath).flatMap {
            case Some(current) if sameArtifact(current, capturedId, capturedBytes) =>
              validateOccupiedExpectation(expected, current) >> Async[F].pure(toCustody(current))
            case Some(_) =>
              Async[F].raiseError[DurablyCustodiedShardCheckpoint](ConflictingOccupiedSlot(recordPath))
            case None if expected.occupiedChecksum.nonEmpty =>
              Async[F].raiseError[DurablyCustodiedShardCheckpoint](MissingOccupiedRecord(recordPath))
            case None =>
              durableInstall(capturedId, capturedBytes)
          }
        }
      } yield stored

    def readExact(custody: DurablyCustodiedShardCheckpoint): F[Array[Byte]] =
      validateCustody(custody) >> withOpen {
        readRecord(recordPath).flatMap {
          case None => Async[F].raiseError(MissingOccupiedRecord(recordPath))
          case Some(record)
              if MessageDigest.isEqual(record.checksum, custody.recordChecksum) &&
                MessageDigest.isEqual(record.opaqueId, custody.opaqueId) &&
                record.exactBytes.length.toLong == custody.byteLength =>
            Async[F].delay(record.exactBytes.clone())
          case Some(_) =>
            Async[F].raiseError(CorruptRecord(recordPath, "record no longer matches the store-bound custody witness"))
        }
      }

    private[sharding] def validateOnOpen: F[Unit] =
      withOpen(cleanStaleTemporaries >> readRecord(recordPath).void)

    private[sharding] def close: F[Unit] = mutex.permit.use(_ => lifecycle.set(Closed))

    private def durableInstall(
      opaqueId: Array[Byte],
      exactBytes: Array[Byte]
    ): F[DurablyCustodiedShardCheckpoint] = {
      val envelope = encode(opaqueId, exactBytes)
      val artifact = new OutboxWriteArtifact(opaqueId.length, exactBytes.length.toLong)

      writer
        .replace(recordPath, envelope, artifact, writeHook)(path =>
          readRecord(path).flatMap {
            case Some(record) => Async[F].pure(record)
            case None         => Async[F].raiseError[LocalRecord](MissingOccupiedRecord(path))
          }
        )
        .flatMap { installed =>
          if (sameArtifact(installed, opaqueId, exactBytes))
            Async[F].pure[DurablyCustodiedShardCheckpoint](toCustody(installed))
          else Async[F].raiseError[DurablyCustodiedShardCheckpoint](ConflictingOccupiedSlot(recordPath))
        }
        .handleErrorWith {
          case error: ShardCheckpointOutboxRecoveryRequired => Async[F].raiseError(error)
          case error: ShardCheckpointOutboxRejected         => Async[F].raiseError(error)
          case error: ShardCheckpointOutboxUnavailable      => Async[F].raiseError(error)
          case error                                         => Async[F].raiseError(DurableWriteFailed(recordPath, error))
        }
    }

    private def validateOccupiedExpectation(expected: ShardCheckpointOutboxHead, current: LocalRecord): F[Unit] =
      expected.occupiedChecksum match {
        case None => Async[F].unit // Idempotent retry after an earlier install whose return was lost.
        case Some(checksum) if MessageDigest.isEqual(checksum, current.checksum) => Async[F].unit
        case Some(_) => Async[F].raiseError(CorruptRecord(recordPath, "expected head differs from the occupied durable record"))
      }

    private def validateHead(head: ShardCheckpointOutboxHead): F[ShardCheckpointOutboxHead] =
      Async[F].delay {
        if (head eq null) throw ForeignHead()
        if (head.storeIdentity ne storeIdentity) throw ForeignHead()
        head
      }

    private def validateCustody(custody: DurablyCustodiedShardCheckpoint): F[Unit] =
      Async[F].delay {
        if ((custody eq null) || (custody.storeIdentity ne storeIdentity)) throw ForeignCustody()
      }

    private def captureId(value: Array[Byte]): F[Array[Byte]] =
      Async[F].delay {
        val supplied = Option(value).getOrElse(throw InvalidOpaqueId("null byte array"))
        if (supplied.isEmpty) throw InvalidOpaqueId("empty byte array")
        if (supplied.length > limits.maxOpaqueIdBytes)
          throw InvalidOpaqueId(s"${supplied.length} bytes exceeds local limit ${limits.maxOpaqueIdBytes}")
        supplied.clone()
      }

    private def captureArtifact(value: Array[Byte]): F[Array[Byte]] =
      Async[F].delay {
        val supplied = Option(value).getOrElse(throw InvalidArtifact("null byte array"))
        if (supplied.isEmpty) throw InvalidArtifact("empty byte array")
        if (supplied.length.toLong > limits.maxArtifactBytes)
          throw ArtifactTooLarge(supplied.length.toLong, limits.maxArtifactBytes)
        supplied.clone()
      }

    private def withOpen[A](operation: => F[A]): F[A] =
      mutex.permit.use(_ => ensureOpen >> Async[F].defer(operation))

    private def ensureOpen: F[Unit] =
      lifecycle.get.flatMap {
        case Open   => Async[F].unit
        case Closed => Async[F].raiseError(StoreClosed(directory))
      }

    private def toSnapshot(record: Option[LocalRecord]): ShardCheckpointOutboxSnapshot =
      record match {
        case None => new Empty(new ShardCheckpointOutboxHead(None, storeIdentity))
        case Some(value) =>
          val head = new ShardCheckpointOutboxHead(value.checksum.clone().some, storeIdentity)
          new Occupied(head, toCustody(value))
      }

    private def toCustody(record: LocalRecord): DurablyCustodiedShardCheckpoint =
      new DurablyCustodiedShardCheckpoint(
        record.exactBytes.length.toLong,
        record.opaqueId.clone(),
        record.checksum.clone(),
        storeIdentity
      )

    private def cleanStaleTemporaries: F[Unit] =
      listDirectoryBounded(directory).flatMap { entries =>
        val unexpected = entries.find { path =>
          val name = path.getFileName.toString
          name != LockFileName && name != RecordFileName && !(name.startsWith(TemporaryPrefix) && name.endsWith(TemporarySuffix))
        }

        unexpected match {
          case Some(path) => Async[F].raiseError(UnexpectedDirectoryEntry(path))
          case None =>
            val temporary = entries.filter { path =>
              val name = path.getFileName.toString
              name.startsWith(TemporaryPrefix) && name.endsWith(TemporarySuffix)
            }
            temporary.traverse_(deleteVerifiedTemporary) >>
              Async[F].whenA(temporary.nonEmpty)(fileOps.forceDirectory(directory))
        }
      }

    private def deleteVerifiedTemporary(path: Path): F[Unit] =
      readOptionalAttributes(path).flatMap {
        case Some(attributes) if attributes.isRegularFile && !attributes.isSymbolicLink => fileOps.deleteIfExists(path)
        case Some(_) => Async[F].raiseError(UnexpectedDirectoryEntry(path))
        case None    => Async[F].raiseError(CorruptRecord(path, "stale temporary disappeared during startup cleanup"))
      }

    private def readRecord(path: Path): F[Option[LocalRecord]] =
      readOptionalAttributes(path).flatMap {
        case None => Async[F].pure[Option[LocalRecord]](None)
        case Some(attributes) if !attributes.isRegularFile || attributes.isSymbolicLink =>
          Async[F].raiseError[Option[LocalRecord]](CorruptRecord(path, "record is not a no-follow regular file"))
        case Some(attributes) if attributes.size() > limits.maxRecordBytes.toLong =>
          Async[F].raiseError[Option[LocalRecord]](
            CorruptRecord(path, s"record has ${attributes.size()} bytes; local read bound is ${limits.maxRecordBytes}")
          )
        case Some(_) =>
          fileOps
            .openInput(path)
            .use(readBounded(path, _, limits.maxRecordBytes))
            .flatMap(bytes => decode(path, bytes, limits).liftTo[F])
            .map(_.some)
      }.handleErrorWith {
        case error: ShardCheckpointOutboxError => Async[F].raiseError(error)
        case _: FileNotFoundException          => Async[F].raiseError(CorruptRecord(path, "record disappeared during verified readback"))
        case _: NoSuchFileException            => Async[F].raiseError(CorruptRecord(path, "record disappeared during verified readback"))
        case error                             => Async[F].raiseError(StorageAccessFailed("verified readback", path, error))
      }
  }

  private def prepareRoot[F[_]: Async](directory: Path, fileOps: DurableFileOps[F]): F[Path] =
    Option(directory) match {
      case None => Async[F].raiseError(InvalidDirectory(null, "null path"))
      case Some(path) =>
        fileOps.createDirectories(path).adaptError { case error => InvalidDirectory(path, "unable to create directory", error) } >>
          Async[F]
            .blocking(Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS))
            .flatMap { attributes =>
              Async[F].raiseUnless(attributes.isDirectory && !attributes.isSymbolicLink)(
                InvalidDirectory(path, "path is not a no-follow directory")
              )
            }
            .as(path)
    }

  private def listDirectoryBounded[F[_]: Async](directory: Path): F[Vector[Path]] =
    Async[F].blocking {
      val stream = Files.list(directory)
      try {
        val iterator = stream.iterator().asScala
        val builder = Vector.newBuilder[Path]
        var count = 0
        while (iterator.hasNext && count < MaxDirectoryEntries + 1) {
          builder += iterator.next()
          count += 1
        }
        if (iterator.hasNext || count > MaxDirectoryEntries)
          throw DirectoryEnumerationLimitExceeded(directory, count, MaxDirectoryEntries)
        builder.result()
      } finally stream.close()
    }

  private def readOptionalAttributes[F[_]: Async](path: Path): F[Option[BasicFileAttributes]] =
    Async[F]
      .blocking(Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS).some)
      .recover { case _: NoSuchFileException => None }

  private def readBounded[F[_]: Async](path: Path, input: InputStream, maximumBytes: Int): F[Array[Byte]] =
    Async[F].blocking {
      val output = new ByteArrayOutputStream(Math.min(maximumBytes, 8192))
      val buffer = new Array[Byte](8192)
      var done = false
      while (!done) {
        val read = input.read(buffer)
        if (read < 0) done = true
        else if (read > 0) {
          if (output.size().toLong + read > maximumBytes.toLong)
            throw CorruptRecord(path, s"record exceeds local read bound $maximumBytes")
          output.write(buffer, 0, read)
        }
      }
      output.toByteArray
    }

  /** This envelope and checksum are private storage diagnostics. They are never checkpoint bytes, a signature preimage, or a content ID. */
  private def encode(opaqueId: Array[Byte], exactBytes: Array[Byte]): Array[Byte] = {
    val bodyBytes = HeaderBytes + opaqueId.length + exactBytes.length
    val output = ByteBuffer.allocate(bodyBytes + ChecksumBytes).order(ByteOrder.BIG_ENDIAN)
    output.putLong(LocalMagic)
    output.putInt(LocalFormat)
    output.putInt(opaqueId.length)
    output.putLong(exactBytes.length.toLong)
    output.put(opaqueId)
    output.put(exactBytes)
    val checksum = MessageDigest.getInstance("SHA-256").digest(output.array().take(bodyBytes))
    output.put(checksum)
    output.array()
  }

  private def decode(
    path: Path,
    bytes: Array[Byte],
    limits: ShardCheckpointOutboxLimits
  ): Either[CorruptRecord, LocalRecord] =
    Either.catchNonFatal {
      if (bytes.length < RecordOverhead) throw new IllegalArgumentException("record is shorter than its fixed envelope")

      val input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
      val magic = input.getLong()
      val format = input.getInt()
      val opaqueIdLength = input.getInt()
      val artifactLength = input.getLong()

      if (magic != LocalMagic) throw new IllegalArgumentException("local envelope marker mismatch")
      if (format != LocalFormat) throw new IllegalArgumentException(s"unsupported local envelope format $format")
      if (opaqueIdLength <= 0) throw new IllegalArgumentException(s"invalid opaque id length $opaqueIdLength")
      if (opaqueIdLength > limits.maxOpaqueIdBytes)
        throw new IllegalArgumentException(
          s"opaque id length $opaqueIdLength exceeds local field bound ${limits.maxOpaqueIdBytes}"
        )
      if (artifactLength <= 0L || artifactLength > Int.MaxValue.toLong)
        throw new IllegalArgumentException(s"invalid artifact length $artifactLength")
      if (artifactLength > limits.maxArtifactBytes)
        throw new IllegalArgumentException(
          s"artifact length $artifactLength exceeds local field bound ${limits.maxArtifactBytes}"
        )

      val expectedLength = Math.addExact(RecordOverhead.toLong, Math.addExact(opaqueIdLength.toLong, artifactLength))
      if (expectedLength != bytes.length.toLong)
        throw new IllegalArgumentException(s"envelope length $expectedLength does not match file length ${bytes.length}")

      val opaqueId = new Array[Byte](opaqueIdLength)
      val artifact = new Array[Byte](artifactLength.toInt)
      input.get(opaqueId)
      input.get(artifact)
      val suppliedChecksum = new Array[Byte](ChecksumBytes)
      input.get(suppliedChecksum)
      val computedChecksum = MessageDigest.getInstance("SHA-256").digest(bytes.take(bytes.length - ChecksumBytes))
      if (!MessageDigest.isEqual(suppliedChecksum, computedChecksum))
        throw new IllegalArgumentException("local envelope checksum mismatch")

      new LocalRecord(opaqueId, artifact, suppliedChecksum)
    }.leftMap(error => CorruptRecord(path, error.getMessage, error))

  private def sameArtifact(record: LocalRecord, opaqueId: Array[Byte], exactBytes: Array[Byte]): Boolean =
    MessageDigest.isEqual(record.opaqueId, opaqueId) && MessageDigest.isEqual(record.exactBytes, exactBytes)
}
