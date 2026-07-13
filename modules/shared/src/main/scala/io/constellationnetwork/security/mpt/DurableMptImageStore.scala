package io.constellationnetwork.security.mpt

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.util.Arrays

import cats.Parallel
import cats.effect.Async
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.util.control.NonFatal

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import scodec.bits.ByteVector

/** A content-addressed, immutable local image of the complete consensus MPT key/value set.
  *
  * This is a dark durability primitive. Nothing in [[io.constellationnetwork.schema.mpt.MptStore]], the branch overlay, snapshot
  * finalization, or recovery consumes it yet. In particular, this component does not close ROOT-002/003/004, does not make finality
  * publication transactional, and does not activate exact-parent branch rejection.
  *
  * An image becomes active only after [[publish]] installs its receipt as the active manifest. Preparing an image never changes that
  * manifest. Images are never deleted here, so the preceding generation remains available after a failed prepare or publish. Retention
  * and cutoff belong to a later transaction design, not this primitive.
  *
  * The compare-and-set and reader-serialization guarantees apply to exactly one live store instance with exclusive ownership of its
  * directory. The caller must create `directory` and durably install that directory entry in its parent before constructing this store; an
  * existence check cannot establish parent-entry durability. Activation additionally requires a lifetime OS directory lock, bounded
  * streaming image decode, an MPT root builder bound to the active-era consensus hasher, and a receipt bound to an exact canonical snapshot
  * anchor (not merely ordinal and root).
  */
trait DurableMptImageStore[F[_]] {

  /** Capture, independently rebuild, encode, force, atomically install, and read-back verify an inactive image.
    *
    * The input is copied before the asynchronous MPT build begins. The caller may subsequently mutate arrays which backed a
    * `ByteVector.view`; those mutations cannot change the prepared image.
    */
  def prepare(
    generation: Long,
    ordinal: SnapshotOrdinal,
    expectedRoot: MptRoot,
    entries: Map[Hex, ByteVector]
  ): F[MptImageReceipt]

  /** Within one exclusively owned live store instance, atomically make a prepared image active if the current manifest's image id and
    * digest equal `expectedPrior`.
    *
    * Publishing the receipt which is already active is idempotent. A different receipt at the same generation fails with a typed
    * [[DurableMptImageError.GenerationConflict]].
    */
  def publish(receipt: MptImageReceipt, expectedPrior: Option[MptImagePointer]): F[Unit]

  /** Return the active receipt only after its manifest and image have both been verified. */
  def activeReceipt: F[Option[MptImageReceipt]]

  /** Read the exact active receipt, verifying manifest identity, image digest and metadata, entry count, and independently rebuilt root. */
  def read(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]]
}

final case class MptImageId(value: Hash)
final case class MptImageDigest(value: Hash)

final case class MptImagePointer(imageId: MptImageId, digest: MptImageDigest)

object MptImagePointer {
  def fromReceipt(receipt: MptImageReceipt): MptImagePointer = MptImagePointer(receipt.imageId, receipt.digest)
}

final case class MptImageReceipt(
  formatVersion: Int,
  generation: Long,
  imageId: MptImageId,
  ordinal: SnapshotOrdinal,
  root: MptRoot,
  digest: MptImageDigest,
  entryCount: Int
) {
  def pointer: MptImagePointer = MptImagePointer.fromReceipt(this)
}

sealed abstract class DurableMptImageError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object DurableMptImageError {
  final case class InvalidGeneration(generation: Long)
      extends DurableMptImageError(s"MPT image generation must be non-negative, got $generation")

  final case class InvalidEntry(message0: String) extends DurableMptImageError(message0)

  final case class RootMismatch(expected: MptRoot, actual: MptRoot)
      extends DurableMptImageError(s"MPT image root mismatch: expected=${expected.value.value} actual=${actual.value.value}")

  final case class CorruptImage(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  final case class CorruptManifest(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  case object NoActiveManifest extends DurableMptImageError("No active MPT image manifest")

  final case class ReceiptManifestMismatch(expected: MptImageReceipt, active: MptImageReceipt)
      extends DurableMptImageError(
        s"MPT image receipt is not active: expected generation=${expected.generation} image=${expected.imageId.value.value}, " +
          s"active generation=${active.generation} image=${active.imageId.value.value}"
      )

  final case class CompareAndSetConflict(expected: Option[MptImagePointer], actual: Option[MptImagePointer])
      extends DurableMptImageError(s"MPT image manifest compare-and-set conflict: expected=$expected actual=$actual")

  final case class GenerationConflict(generation: Long, active: MptImageReceipt, candidate: MptImageReceipt)
      extends DurableMptImageError(
        s"MPT image generation $generation is already occupied by ${active.imageId.value.value}; " +
          s"candidate=${candidate.imageId.value.value}"
      )

  final case class NonConsecutiveGeneration(active: Long, candidate: Long)
      extends DurableMptImageError(s"MPT image generation must advance by one: active=$active candidate=$candidate")

  final case class InitialGenerationMustBeZero(candidate: Long)
      extends DurableMptImageError(s"The first active MPT image must use generation zero, got $candidate")

  final case class RootDirectoryMustExist(path: Path)
      extends DurableMptImageError(s"MPT image root directory must already exist and be a directory: $path")

  final case class AtomicMoveRequired(path: Path, cause0: Throwable)
      extends DurableMptImageError(s"Atomic move is required for MPT image durability: $path", cause0)

  final case class PublishRecoveryFailed(publishFailure: Throwable, recoveryFailure: Throwable)
      extends DurableMptImageError(
        s"MPT image manifest publish failed and the prior manifest could not be restored: ${publishFailure.getMessage}",
        recoveryFailure
      ) {
    addSuppressed(publishFailure)
  }
}

object DurableMptImageStore {
  val CurrentFormatVersion: Int = 1

  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](directory: Path): F[DurableMptImageStore[F]] =
    makeWith(directory, DurableMptImageFileOps.nio[F], MptImageFaultInjector.noop[F], MptImageRootBuilder.consensus[F])

  private[mpt] def makeWith[F[_]: Async](
    directory: Path,
    fileOps: DurableMptImageFileOps[F],
    faults: MptImageFaultInjector[F],
    rootBuilder: MptImageRootBuilder[F]
  ): F[DurableMptImageStore[F]] = {
    val imagesDirectory = DurableMptImageLayout.images(directory)

    for {
      rootExists <- fileOps.isDirectory(directory)
      _ <- Async[F].raiseUnless(rootExists)(DurableMptImageError.RootDirectoryMustExist(directory))
      _ <- fileOps.createDirectories(imagesDirectory)
      _ <- fileOps.forceDirectory(imagesDirectory)
      _ <- fileOps.forceDirectory(directory)
      mutex <- Semaphore[F](1L)
    } yield new LiveDurableMptImageStore[F](directory, fileOps, faults, rootBuilder, mutex)
  }
}

private[mpt] object DurableMptImageLayout {
  val ActiveManifestName: String = "active.manifest"
  val ImagesDirectoryName: String = "images"

  def images(directory: Path): Path = directory.resolve(ImagesDirectoryName)
  def activeManifest(directory: Path): Path = directory.resolve(ActiveManifestName)
  def image(directory: Path, imageId: MptImageId): Path = images(directory).resolve(s"${imageId.value.value}.mpti")
}

private[mpt] sealed trait MptImageArtifact
private[mpt] object MptImageArtifact {
  case object Image extends MptImageArtifact
  case object Manifest extends MptImageArtifact
  case object ManifestRecovery extends MptImageArtifact
}

private[mpt] sealed trait MptImageFaultPoint
private[mpt] object MptImageFaultPoint {
  final case class Write(artifact: MptImageArtifact) extends MptImageFaultPoint
  final case class ForceFile(artifact: MptImageArtifact) extends MptImageFaultPoint
  final case class AtomicMove(artifact: MptImageArtifact) extends MptImageFaultPoint
  final case class ForceDirectory(artifact: MptImageArtifact) extends MptImageFaultPoint
  final case class ReadBack(artifact: MptImageArtifact) extends MptImageFaultPoint
}

private[mpt] trait MptImageFaultInjector[F[_]] {
  def before(point: MptImageFaultPoint): F[Unit]
}

private[mpt] object MptImageFaultInjector {
  def noop[F[_]: Async]: MptImageFaultInjector[F] = new MptImageFaultInjector[F] {
    def before(point: MptImageFaultPoint): F[Unit] = Async[F].unit
  }
}

private[mpt] trait MptImageRootBuilder[F[_]] {
  def rebuild(entries: Vector[(Hex, ByteVector)]): F[MptRoot]
}

private[mpt] object MptImageRootBuilder {
  def consensus[F[_]: Async: Parallel: Hasher: JsonSerializer]: MptImageRootBuilder[F] = new MptImageRootBuilder[F] {
    def rebuild(entries: Vector[(Hex, ByteVector)]): F[MptRoot] = {
      val bytes = entries.iterator.map { case (key, value) => key -> value.toArray }.toMap
      MerklePatriciaTrie.makeParallelFromBytes[F](bytes).map(_.rootHash)
    }
  }
}

private[mpt] trait DurableMptImageFileOps[F[_]] {
  def createDirectories(path: Path): F[Unit]
  def createTempFile(directory: Path, prefix: String, suffix: String): F[Path]
  def write(path: Path, bytes: Array[Byte]): F[Unit]
  def forceFile(path: Path): F[Unit]
  def atomicMoveReplace(source: Path, target: Path): F[Unit]
  def forceDirectory(path: Path): F[Unit]
  def readAllBytes(path: Path): F[Array[Byte]]
  def exists(path: Path): F[Boolean]
  def isDirectory(path: Path): F[Boolean]
  def deleteIfExists(path: Path): F[Unit]
}

private[mpt] object DurableMptImageFileOps {
  def nio[F[_]: Async]: DurableMptImageFileOps[F] = new DurableMptImageFileOps[F] {
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
        .adaptError { case error: java.nio.file.AtomicMoveNotSupportedException =>
          DurableMptImageError.AtomicMoveRequired(target, error)
        }

    def forceDirectory(path: Path): F[Unit] = Async[F].blocking {
      val channel = FileChannel.open(path, StandardOpenOption.READ)
      try channel.force(true)
      finally channel.close()
    }

    def readAllBytes(path: Path): F[Array[Byte]] = Async[F].blocking(Files.readAllBytes(path))

    def exists(path: Path): F[Boolean] = Async[F].blocking(Files.exists(path))

    def isDirectory(path: Path): F[Boolean] = Async[F].blocking(Files.isDirectory(path))

    def deleteIfExists(path: Path): F[Unit] = Async[F].blocking(Files.deleteIfExists(path)).void
  }
}

private final class LiveDurableMptImageStore[F[_]: Async](
  directory: Path,
  fileOps: DurableMptImageFileOps[F],
  faults: MptImageFaultInjector[F],
  rootBuilder: MptImageRootBuilder[F],
  publishMutex: Semaphore[F]
) extends DurableMptImageStore[F] {
  import DurableMptImageError._
  import MptImageArtifact._
  import MptImageFaultPoint._

  private val activeManifestPath = DurableMptImageLayout.activeManifest(directory)

  def prepare(
    generation: Long,
    ordinal: SnapshotOrdinal,
    expectedRoot: MptRoot,
    entries: Map[Hex, ByteVector]
  ): F[MptImageReceipt] =
    for {
      _ <- validateGeneration(generation)
      captured <- capture(entries)
      actualRoot <- rootBuilder.rebuild(captured)
      _ <- ensureRoot(expectedRoot, actualRoot)
      imageBytes <- Async[F].delay(MptImageEncoding.encodeImage(generation, ordinal, expectedRoot, captured))
      digest <- Async[F].delay(MptImageDigest(Hash.fromBytes(imageBytes)))
      imageId <- Async[F].delay(MptImageEncoding.imageId(digest))
      receipt = MptImageReceipt(
        DurableMptImageStore.CurrentFormatVersion,
        generation,
        imageId,
        ordinal,
        expectedRoot,
        digest,
        captured.size
      )
      target = DurableMptImageLayout.image(directory, imageId)
      _ <- durableAtomicWrite(target, imageBytes, Image, verifyImageBytes(receipt, _), applyFaults = true)
    } yield receipt

  def publish(receipt: MptImageReceipt, expectedPrior: Option[MptImagePointer]): F[Unit] =
    publishMutex.permit.use { _ =>
      for {
        _ <- validateReceiptShape(receipt)
        _ <- readImage(receipt)
        priorBytes <- readOptional(activeManifestPath)
        prior <- priorBytes.traverse(decodeManifest)
        _ <- prior.traverse_(readImage)
        _ <- validatePublication(prior, receipt, expectedPrior)
        _ <-
          if (prior.contains(receipt)) Async[F].unit
          else publishManifest(receipt, priorBytes)
      } yield ()
    }

  def activeReceipt: F[Option[MptImageReceipt]] = publishMutex.permit.use(_ => activeReceiptUnlocked)

  def read(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]] =
    publishMutex.permit.use { _ =>
      for {
        active <- activeReceiptUnlocked.flatMap(_.liftTo[F](NoActiveManifest))
        _ <- Async[F].raiseUnless(active == receipt)(ReceiptManifestMismatch(receipt, active))
        entries <- readImage(receipt)
      } yield entries
    }

  private def activeReceiptUnlocked: F[Option[MptImageReceipt]] =
    readOptional(activeManifestPath).flatMap(_.traverse(bytes => decodeManifest(bytes).flatTap(readImage)))

  private def publishManifest(receipt: MptImageReceipt, priorBytes: Option[Array[Byte]]): F[Unit] = {
    val bytes = MptImageEncoding.encodeManifest(receipt)

    Async[F].uncancelable { _ =>
      durableAtomicWrite(activeManifestPath, bytes, Manifest, verifyManifestBytes(receipt, _), applyFaults = true).handleErrorWith {
        publishFailure =>
          restorePriorManifest(priorBytes).attempt.flatMap {
            case Right(_)              => Async[F].raiseError(publishFailure)
            case Left(recoveryFailure) => Async[F].raiseError(PublishRecoveryFailed(publishFailure, recoveryFailure))
          }
      }
    }
  }

  private def restorePriorManifest(priorBytes: Option[Array[Byte]]): F[Unit] =
    priorBytes match {
      case Some(bytes) =>
        durableAtomicWrite(
          activeManifestPath,
          bytes,
          ManifestRecovery,
          actual =>
            if (Arrays.equals(actual, bytes)) Async[F].unit
            else Async[F].raiseError(CorruptManifest("Prior MPT image manifest read-back differs after recovery")),
          applyFaults = false
        )
      case None =>
        fileOps.deleteIfExists(activeManifestPath) >> fileOps.forceDirectory(directory)
    }

  private def validatePublication(
    active: Option[MptImageReceipt],
    candidate: MptImageReceipt,
    expectedPrior: Option[MptImagePointer]
  ): F[Unit] =
    active match {
      case Some(current) if current == candidate => Async[F].unit
      case Some(current) if current.generation == candidate.generation =>
        Async[F].raiseError(GenerationConflict(candidate.generation, current, candidate))
      case Some(current) if candidate.generation != current.generation + 1L =>
        Async[F].raiseError(NonConsecutiveGeneration(current.generation, candidate.generation))
      case None if candidate.generation != 0L =>
        Async[F].raiseError(InitialGenerationMustBeZero(candidate.generation))
      case _ =>
        val actual = active.map(_.pointer)
        Async[F].raiseUnless(actual == expectedPrior)(CompareAndSetConflict(expectedPrior, actual))
    }

  private def readImage(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]] = {
    val path = DurableMptImageLayout.image(directory, receipt.imageId)
    fileOps.readAllBytes(path).flatMap(verifyImageBytes(receipt, _))
  }

  private def verifyImageBytes(receipt: MptImageReceipt, bytes: Array[Byte]): F[SortedMap[Hex, ByteVector]] =
    for {
      digest <- Async[F].delay(MptImageDigest(Hash.fromBytes(bytes)))
      _ <- Async[F].raiseUnless(digest == receipt.digest)(
        CorruptImage(s"MPT image digest mismatch for ${receipt.imageId.value.value}: expected=${receipt.digest.value.value} actual=${digest.value.value}")
      )
      expectedImageId <- Async[F].delay(MptImageEncoding.imageId(digest))
      _ <- Async[F].raiseUnless(expectedImageId == receipt.imageId)(
        CorruptImage(s"MPT image id mismatch: expected=${receipt.imageId.value.value} actual=${expectedImageId.value.value}")
      )
      decoded <- Async[F].fromEither(MptImageEncoding.decodeImage(bytes))
      _ <- Async[F].raiseUnless(decoded.generation == receipt.generation)(
        CorruptImage(s"MPT image generation mismatch: expected=${receipt.generation} actual=${decoded.generation}")
      )
      _ <- Async[F].raiseUnless(decoded.ordinal == receipt.ordinal)(
        CorruptImage(s"MPT image ordinal mismatch: expected=${receipt.ordinal.value.value} actual=${decoded.ordinal.value.value}")
      )
      _ <- Async[F].raiseUnless(decoded.root == receipt.root)(
        CorruptImage(s"MPT image encoded root mismatch: expected=${receipt.root.value.value} actual=${decoded.root.value.value}")
      )
      _ <- Async[F].raiseUnless(decoded.entries.size == receipt.entryCount)(
        CorruptImage(s"MPT image entry-count mismatch: expected=${receipt.entryCount} actual=${decoded.entries.size}")
      )
      actualRoot <- rootBuilder.rebuild(decoded.entries.toVector)
      _ <- ensureRoot(receipt.root, actualRoot)
    } yield decoded.entries

  private def verifyManifestBytes(receipt: MptImageReceipt, bytes: Array[Byte]): F[Unit] =
    decodeManifest(bytes).flatMap { decoded =>
      Async[F].raiseUnless(decoded == receipt)(
        CorruptManifest(s"MPT image manifest read-back mismatch: expected=$receipt actual=$decoded")
      )
    }

  private def decodeManifest(bytes: Array[Byte]): F[MptImageReceipt] =
    Async[F].fromEither(MptImageEncoding.decodeManifest(bytes))

  private def durableAtomicWrite[A](
    target: Path,
    bytes: Array[Byte],
    artifact: MptImageArtifact,
    verify: Array[Byte] => F[A],
    applyFaults: Boolean
  ): F[A] = {
    def fault(point: MptImageFaultPoint): F[Unit] = if (applyFaults) faults.before(point) else Async[F].unit

    fileOps.createTempFile(target.getParent, s".${target.getFileName.toString}.", ".tmp").flatMap { temporary =>
      val operation =
        for {
          _ <- fault(Write(artifact)) >> fileOps.write(temporary, bytes)
          _ <- fault(ForceFile(artifact)) >> fileOps.forceFile(temporary)
          _ <- fault(AtomicMove(artifact)) >> fileOps
            .atomicMoveReplace(temporary, target)
            .adaptError { case error: java.nio.file.AtomicMoveNotSupportedException => AtomicMoveRequired(target, error) }
          _ <- fault(ForceDirectory(artifact)) >> fileOps.forceDirectory(target.getParent)
          actual <- fault(ReadBack(artifact)) >> fileOps.readAllBytes(target)
          verified <- verify(actual)
        } yield verified

      Async[F].guarantee(operation, fileOps.deleteIfExists(temporary).handleError(_ => ()))
    }
  }

  private def readOptional(path: Path): F[Option[Array[Byte]]] =
    fileOps.exists(path).ifM(fileOps.readAllBytes(path).map(Some(_)), Async[F].pure(None))

  private def capture(entries: Map[Hex, ByteVector]): F[Vector[(Hex, ByteVector)]] =
    Async[F].delay {
      entries.iterator
        .map { case (key, value) => key -> ByteVector.view(value.toArray.clone()) }
        .toVector
        .sortBy(_._1.value)
    }.flatTap(_.traverse_ { case (key, _) => validateKey(key) })

  private def validateKey(key: Hex): F[Unit] = {
    val value = key.value
    val canonical = value.nonEmpty && (value.length & 1) == 0 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
    Async[F].raiseUnless(canonical)(InvalidEntry(s"MPT image key must be non-empty, even-length lowercase hex: '$value'"))
  }

  private def validateGeneration(generation: Long): F[Unit] =
    Async[F].raiseWhen(generation < 0L)(InvalidGeneration(generation))

  private def validateReceiptShape(receipt: MptImageReceipt): F[Unit] =
    for {
      _ <- validateGeneration(receipt.generation)
      _ <- Async[F].raiseUnless(receipt.formatVersion == DurableMptImageStore.CurrentFormatVersion)(
        CorruptManifest(s"Unsupported MPT image receipt version ${receipt.formatVersion}")
      )
      _ <- Async[F].raiseWhen(receipt.entryCount < 0)(CorruptManifest(s"Negative MPT image entry count ${receipt.entryCount}"))
      _ <- validateReceiptHash("image id", receipt.imageId.value)
      _ <- validateReceiptHash("image digest", receipt.digest.value)
      _ <- validateReceiptHash("MPT root", receipt.root.value)
    } yield ()

  private def validateReceiptHash(label: String, hash: Hash): F[Unit] = {
    val value = hash.value
    val canonical = value.length == 64 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
    Async[F].raiseUnless(canonical)(CorruptManifest(s"MPT image receipt has a non-canonical $label: '$value'"))
  }

  private def ensureRoot(expected: MptRoot, actual: MptRoot): F[Unit] =
    Async[F].raiseUnless(expected == actual)(RootMismatch(expected, actual))
}

private[mpt] object MptImageEncoding {
  import DurableMptImageError._

  private val ImageMagic = "TNMPTIMG".getBytes(StandardCharsets.US_ASCII)
  private val ManifestMagic = "TNMPTMNF".getBytes(StandardCharsets.US_ASCII)
  private val HashBytes = 32
  private val ManifestPayloadBytes = ManifestMagic.length + 4 + 8 + HashBytes + 8 + HashBytes + HashBytes + 4
  private val ManifestBytes = ManifestPayloadBytes + HashBytes
  private val ImageIdDomain = "tessellation-nakamoto/mpt-image-id/v1\u0000".getBytes(StandardCharsets.UTF_8)

  final case class DecodedImage(
    generation: Long,
    ordinal: SnapshotOrdinal,
    root: MptRoot,
    entries: SortedMap[Hex, ByteVector]
  )

  def imageId(digest: MptImageDigest): MptImageId = {
    val digestBytes = hashBytes(digest.value)
    MptImageId(Hash.fromBytes(ImageIdDomain ++ digestBytes))
  }

  def encodeImage(
    generation: Long,
    ordinal: SnapshotOrdinal,
    root: MptRoot,
    entries: Vector[(Hex, ByteVector)]
  ): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val data = new DataOutputStream(output)
    try {
      data.write(ImageMagic)
      data.writeInt(DurableMptImageStore.CurrentFormatVersion)
      data.writeLong(generation)
      data.writeLong(ordinal.value.value)
      data.write(hashBytes(root.value))
      data.writeInt(entries.size)
      entries.foreach {
        case (key, value) =>
          val keyBytes = key.value.getBytes(StandardCharsets.US_ASCII)
          val valueBytes = value.toArray
          data.writeInt(keyBytes.length)
          data.write(keyBytes)
          data.writeInt(valueBytes.length)
          data.write(valueBytes)
      }
      data.flush()
      output.toByteArray
    } finally data.close()
  }

  def decodeImage(bytes: Array[Byte]): Either[CorruptImage, DecodedImage] =
    decode("MPT image", CorruptImage.apply) {
      val input = new DataInputStream(new ByteArrayInputStream(bytes))
      try {
        requireMagic(input, ImageMagic, "image")
        requireVersion(input, "image")
        val generation = input.readLong()
        if (generation < 0L) throw new IOException(s"Negative generation $generation")
        val ordinalValue = input.readLong()
        if (ordinalValue < 0L) throw new IOException(s"Negative ordinal $ordinalValue")
        val root = MptRoot(readHash(input))
        val count = input.readInt()
        if (count < 0) throw new IOException(s"Negative entry count $count")
        if (count > input.available() / 8) throw new IOException(s"Impossible entry count $count for ${input.available()} remaining bytes")

        val ordering: Ordering[Hex] = Ordering.by(_.value)
        var entries = SortedMap.empty[Hex, ByteVector](ordering)
        var previous: Option[String] = None
        var index = 0
        while (index < count) {
          val keyLength = checkedLength(input, s"key[$index]")
          val keyBytes = readExact(input, keyLength)
          val key = new String(keyBytes, StandardCharsets.US_ASCII)
          val canonical = key.nonEmpty && (key.length & 1) == 0 && key.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
          if (!canonical) throw new IOException(s"Non-canonical hex key at index $index")
          if (previous.exists(_ >= key)) throw new IOException(s"Keys are not strictly sorted at index $index")
          val valueLength = checkedLength(input, s"value[$index]")
          val value = readExact(input, valueLength)
          entries = entries.updated(Hex(key), ByteVector.view(value))
          previous = Some(key)
          index += 1
        }
        if (input.available() != 0) throw new IOException(s"Trailing ${input.available()} bytes")
        DecodedImage(generation, SnapshotOrdinal.unsafeApply(ordinalValue), root, entries)
      } finally input.close()
    }

  def encodeManifest(receipt: MptImageReceipt): Array[Byte] = {
    val output = new ByteArrayOutputStream(ManifestBytes)
    val data = new DataOutputStream(output)
    try {
      data.write(ManifestMagic)
      data.writeInt(DurableMptImageStore.CurrentFormatVersion)
      data.writeLong(receipt.generation)
      data.write(hashBytes(receipt.imageId.value))
      data.writeLong(receipt.ordinal.value.value)
      data.write(hashBytes(receipt.root.value))
      data.write(hashBytes(receipt.digest.value))
      data.writeInt(receipt.entryCount)
      data.flush()
      val payload = output.toByteArray
      output.write(Hash.sha256DigestFromBytes(payload).toByteArray)
      output.toByteArray
    } finally data.close()
  }

  def decodeManifest(bytes: Array[Byte]): Either[CorruptManifest, MptImageReceipt] =
    decode("MPT image manifest", CorruptManifest.apply) {
      if (bytes.length != ManifestBytes) throw new IOException(s"Expected $ManifestBytes bytes, got ${bytes.length}")
      val payload = Arrays.copyOf(bytes, ManifestPayloadBytes)
      val expectedChecksum = Arrays.copyOfRange(bytes, ManifestPayloadBytes, ManifestBytes)
      val actualChecksum = Hash.sha256DigestFromBytes(payload).toByteArray
      if (!Arrays.equals(expectedChecksum, actualChecksum)) throw new IOException("Manifest checksum mismatch")

      val input = new DataInputStream(new ByteArrayInputStream(payload))
      try {
        requireMagic(input, ManifestMagic, "manifest")
        val version = requireVersion(input, "manifest")
        val generation = input.readLong()
        if (generation < 0L) throw new IOException(s"Negative generation $generation")
        val imageId = MptImageId(readHash(input))
        val ordinalValue = input.readLong()
        if (ordinalValue < 0L) throw new IOException(s"Negative ordinal $ordinalValue")
        val root = MptRoot(readHash(input))
        val digest = MptImageDigest(readHash(input))
        val count = input.readInt()
        if (count < 0) throw new IOException(s"Negative entry count $count")
        if (input.available() != 0) throw new IOException(s"Trailing ${input.available()} manifest bytes")
        MptImageReceipt(version, generation, imageId, SnapshotOrdinal.unsafeApply(ordinalValue), root, digest, count)
      } finally input.close()
    }

  private def decode[A, E](label: String, error: (String, Throwable) => E)(thunk: => A): Either[E, A] =
    try Right(thunk)
    catch {
      case failure: EOFException => Left(error(s"Truncated $label", failure))
      case failure: IOException  => Left(error(s"Invalid $label: ${failure.getMessage}", failure))
      case NonFatal(failure)     => Left(error(s"Unable to decode $label: ${failure.getMessage}", failure))
    }

  private def requireMagic(input: DataInputStream, expected: Array[Byte], label: String): Unit = {
    val actual = readExact(input, expected.length)
    if (!Arrays.equals(actual, expected)) throw new IOException(s"Invalid $label magic")
  }

  private def requireVersion(input: DataInputStream, label: String): Int = {
    val version = input.readInt()
    if (version != DurableMptImageStore.CurrentFormatVersion) throw new IOException(s"Unsupported $label version $version")
    version
  }

  private def checkedLength(input: DataInputStream, field: String): Int = {
    val length = input.readInt()
    if (length < 0 || length > input.available()) throw new IOException(s"Invalid $field length $length with ${input.available()} bytes left")
    length
  }

  private def readHash(input: DataInputStream): Hash = Hash(Hex.fromBytes(readExact(input, HashBytes)).value)

  private def readExact(input: DataInputStream, length: Int): Array[Byte] = {
    val result = new Array[Byte](length)
    input.readFully(result)
    result
  }

  private def hashBytes(hash: Hash): Array[Byte] = {
    val value = hash.value
    val canonical = value.length == HashBytes * 2 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
    if (!canonical) throw new IllegalArgumentException(s"Expected canonical 32-byte hash, got '$value'")
    Hex(value).toBytes
  }
}
