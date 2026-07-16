package io.constellationnetwork.node.shared.domain.nakamoto.custody

import java.io.{ByteArrayOutputStream, FileNotFoundException, InputStream}
import java.nio.ByteBuffer
import java.nio.channels._
import java.nio.file._
import java.nio.file.attribute.{BasicFileAttributeView, BasicFileAttributes, PosixFilePermission}
import java.security.MessageDigest
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.constellationnetwork.storage.durable._

/** Caller-selected local resource limits. These are storage limits, not protocol-validity parameters. */
private[node] final class MetagraphBinaryCustodyLimits private (
  val maxItems: Int,
  val maxTotalBytes: Long,
  val maxArtifactBytes: Long
)

private[node] object MetagraphBinaryCustodyLimits {
  def from(
    maxItems: Int,
    maxTotalBytes: Long,
    maxArtifactBytes: Long
  ): Either[MetagraphBinaryCustodyUnavailable.InvalidLimits, MetagraphBinaryCustodyLimits] = {
    val errors = List(
      Option.when(maxItems <= 0)(s"maxItems must be positive, got $maxItems"),
      Option.when(maxTotalBytes <= 0L)(s"maxTotalBytes must be positive, got $maxTotalBytes"),
      Option.when(maxArtifactBytes <= 0L)(s"maxArtifactBytes must be positive, got $maxArtifactBytes"),
      Option.when(maxArtifactBytes > Int.MaxValue.toLong)(
        s"maxArtifactBytes must fit the exact-byte in-memory API, got $maxArtifactBytes"
      )
    ).flatten

    Either.cond(
      errors.isEmpty,
      new MetagraphBinaryCustodyLimits(maxItems, maxTotalBytes, maxArtifactBytes),
      MetagraphBinaryCustodyUnavailable.InvalidLimits(errors.mkString("; "))
    )
  }
}

/** SHA-256 address in this local custody namespace only. It has deliberately no encoder, decoder, or schema representation. */
private[node] final class LocalCustodyAddress private[custody] (private[custody] val hex: String) {
  override def equals(other: Any): Boolean = other match {
    case that: LocalCustodyAddress => hex == that.hex
    case _                         => false
  }

  override def hashCode(): Int = hex.hashCode
}

private[node] object LocalCustodyAddress {
  private val HexDigits = "0123456789abcdef".toCharArray

  private[custody] def fromExactBytes(bytes: Array[Byte]): LocalCustodyAddress = {
    val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
    val hex = digest.foldLeft(new StringBuilder(digest.length * 2)) { (builder, byte) =>
      builder.append(HexDigits((byte >> 4) & 0xf)).append(HexDigits(byte & 0xf))
    }
    new LocalCustodyAddress(hex.toString)
  }

  private[custody] def fromCanonicalHex(hex: String): LocalCustodyAddress = new LocalCustodyAddress(hex)
}

/** Store-local diagnostic only. It is not a receipt and has no consensus meaning. */
private[node] final class MetagraphBinaryCustodyUsage private[custody] (
  val items: Int,
  val totalBytes: Long
)

/** An unforgeable store-bound witness that one exact byte string completed local durable write and verified readback.
  *
  * This deliberately is not a case class, does not implement `Serializable`, has no codec, and is bound to one live store instance. It is
  * not source authentication, binary admission, DA evidence, an execution receipt, a signature preimage, a quorum contribution, or economic
  * authority.
  */
private[node] final class DurablyStoredBinary private[custody] (
  val contentAddress: LocalCustodyAddress,
  val byteLength: Long,
  private[custody] val storeIdentity: AnyRef
)

private[node] sealed abstract class MetagraphBinaryCustodyError(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

private[node] sealed abstract class MetagraphBinaryCustodyRejected(message: String, cause: Throwable = null)
    extends MetagraphBinaryCustodyError(message, cause)

private[node] object MetagraphBinaryCustodyRejected {
  final case class InvalidArtifact(detail: String) extends MetagraphBinaryCustodyRejected(s"Rejected metagraph-binary bytes: $detail")

  final case class InvalidContentAddress(detail: String)
      extends MetagraphBinaryCustodyRejected(s"Rejected local custody content address: $detail")

  final case class ForeignCapability()
      extends MetagraphBinaryCustodyRejected("DurablyStoredBinary belongs to a different live custody store")
}

private[node] sealed abstract class MetagraphBinaryCustodyUnavailable(message: String, cause: Throwable = null)
    extends MetagraphBinaryCustodyError(message, cause)

private[node] object MetagraphBinaryCustodyUnavailable {
  final case class InvalidLimits(detail: String)
      extends MetagraphBinaryCustodyUnavailable(s"Invalid local metagraph-binary custody limits: $detail")

  final case class DirectoryPreconditionFailed(path: Path, detail: String)
      extends MetagraphBinaryCustodyUnavailable(
        s"Metagraph-binary custody path is not a safe dedicated directory at $path: $detail"
      )

  final case class LockPreconditionFailed(path: Path, detail: String)
      extends MetagraphBinaryCustodyUnavailable(
        s"Metagraph-binary custody lock path is unsafe at $path: $detail"
      )

  final case class StoreClosed(path: Path) extends MetagraphBinaryCustodyUnavailable(s"Metagraph-binary custody store is closed: $path")

  final case class SecureStorageUnsupported(path: Path, detail: String, cause0: Throwable = null)
      extends MetagraphBinaryCustodyUnavailable(
        s"Metagraph-binary custody requires a pinned secure-directory provider at $path: $detail",
        cause0
      )

  final case class OwnershipIdentityChanged(path: Path, detail: String, cause0: Throwable = null)
      extends MetagraphBinaryCustodyUnavailable(
        s"Metagraph-binary custody ownership identity changed at $path: $detail",
        cause0
      )

  final case class ArtifactTooLarge(actualBytes: Long, maximumBytes: Long)
      extends MetagraphBinaryCustodyUnavailable(
        s"Local metagraph-binary custody backpressure: artifact size $actualBytes exceeds caller limit $maximumBytes"
      )

  final case class CapacityExceeded(
    currentItems: Int,
    currentBytes: Long,
    incomingBytes: Long,
    limits: MetagraphBinaryCustodyLimits
  ) extends MetagraphBinaryCustodyUnavailable(
        s"Local metagraph-binary custody capacity would be exceeded " +
          s"(items=$currentItems/${limits.maxItems}, bytes=$currentBytes+${incomingBytes}/${limits.maxTotalBytes})"
      )

  final case class ExistingInventoryExceedsLimits(
    actualItems: Int,
    actualBytes: Long,
    limits: MetagraphBinaryCustodyLimits
  ) extends MetagraphBinaryCustodyUnavailable(
        s"Existing metagraph-binary custody inventory exceeds local limits " +
          s"(items=$actualItems/${limits.maxItems}, bytes=$actualBytes/${limits.maxTotalBytes})"
      )

  final case class ExistingArtifactExceedsLimit(
    contentAddress: LocalCustodyAddress,
    actualBytes: Long,
    maximumBytes: Long
  ) extends MetagraphBinaryCustodyUnavailable(
        s"Existing metagraph-binary custody artifact ${contentAddress.hex} has $actualBytes bytes; " +
          s"local per-artifact limit is $maximumBytes"
      )

  final case class InventoryEnumerationLimitExceeded(observedItems: Long, maximumItems: Int)
      extends MetagraphBinaryCustodyUnavailable(
        s"Existing metagraph-binary custody inventory has at least $observedItems items; local limit is $maximumItems"
      )

  final case class DirectoryEnumerationLimitExceeded(observedEntries: Long, maximumEntries: Long)
      extends MetagraphBinaryCustodyUnavailable(
        s"Metagraph-binary custody directory has at least $observedEntries non-lock entries; safe local bound is $maximumEntries"
      )

  final case class DurableWriteFailed(contentAddress: LocalCustodyAddress, cause0: Throwable)
      extends MetagraphBinaryCustodyUnavailable(
        s"Local durable write failed for metagraph-binary content ${contentAddress.hex}",
        cause0
      )

  final case class StorageAccessFailed(operation: String, path: Path, cause0: Throwable)
      extends MetagraphBinaryCustodyUnavailable(
        s"Local metagraph-binary custody storage is unavailable during $operation at $path",
        cause0
      )
}

private[node] sealed abstract class MetagraphBinaryCustodyRecoveryRequired(message: String, cause: Throwable = null)
    extends MetagraphBinaryCustodyError(message, cause)

private[node] object MetagraphBinaryCustodyRecoveryRequired {
  final case class MissingArtifact(contentAddress: LocalCustodyAddress, path: Path, cause0: Throwable = null)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Missing metagraph-binary custody artifact ${contentAddress.hex} at $path; authenticated recovery required",
        cause0
      )

  final case class CorruptArtifact(contentAddress: LocalCustodyAddress, path: Path, detail: String, cause0: Throwable = null)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Corrupt metagraph-binary custody artifact ${contentAddress.hex} at $path: $detail; authenticated recovery required",
        cause0
      )

  final case class UnexpectedDirectoryEntry(path: Path)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Unexpected entry in dedicated metagraph-binary custody directory: $path; authenticated recovery required"
      )

  final case class ArtifactReadFailed(contentAddress: LocalCustodyAddress, path: Path, cause0: Throwable)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Failed to verify metagraph-binary custody artifact ${contentAddress.hex} at $path; authenticated recovery required",
        cause0
      )

  final case class ContentAddressCollision(contentAddress: LocalCustodyAddress, path: Path)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Exact bytes differ at the same metagraph-binary custody address ${contentAddress.hex} at $path; " +
          "authenticated recovery required"
      )

  final case class InventoryByteCountOverflow(currentBytes: Long, incomingBytes: Long)
      extends MetagraphBinaryCustodyRecoveryRequired(
        s"Metagraph-binary custody inventory byte count overflowed at $currentBytes + $incomingBytes; " +
          "authenticated recovery required"
      )
}

/** Dark exact-byte custody primitive. Nothing in this interface can emit a portable receipt, signature, quorum result, or state-validity
  * claim, and this store is intentionally not wired to intake, shard buffering, checkpoint production, or GL0 acceptance.
  */
private[node] trait MetagraphBinaryCustodyStore[F[_]] {

  /** Store the exact caller-supplied bytes by SHA-256 content address. A successful return is possible only after file and directory force,
    * atomic installation, and exact readback verification. Repeating the same bytes is idempotent and consumes no additional capacity.
    */
  def putExact(bytes: Array[Byte]): F[DurablyStoredBinary]

  /** Reopen one known content address, minting a new store-local capability only after exact readback verification. */
  def openExact(contentAddress: LocalCustodyAddress): F[DurablyStoredBinary]

  /** Read and reverify the exact bytes named by a capability minted by this live store. */
  def readExact(stored: DurablyStoredBinary): F[Array[Byte]]

  /** Local storage accounting only. Consensus code must not branch on this diagnostic. */
  def usage: F[MetagraphBinaryCustodyUsage]
}

private[node] object MetagraphBinaryCustodyStore {
  private[custody] sealed trait BinaryArtifact
  private[custody] final class ExactBinaryArtifact(val contentAddress: LocalCustodyAddress) extends BinaryArtifact

  private[custody] trait ArtifactOpenHook[F[_]] {
    def beforeOpen(path: Path): F[Unit]
  }

  private[custody] object ArtifactOpenHook {
    def noop[F[_]: Async]: ArtifactOpenHook[F] = new ArtifactOpenHook[F] {
      def beforeOpen(path: Path): F[Unit] = Async[F].unit
    }
  }

  private final case class Inventory(entries: Map[LocalCustodyAddress, Long], totalBytes: Long) {
    def usage: MetagraphBinaryCustodyUsage = new MetagraphBinaryCustodyUsage(entries.size, totalBytes)

    def add(contentAddress: LocalCustodyAddress, bytes: Long): Inventory =
      if (entries.contains(contentAddress)) this
      else {
        val nextTotal =
          try Math.addExact(totalBytes, bytes)
          catch {
            case _: ArithmeticException =>
              throw MetagraphBinaryCustodyRecoveryRequired.InventoryByteCountOverflow(totalBytes, bytes)
          }
        Inventory(entries.updated(contentAddress, bytes), nextTotal)
      }
  }

  private object Inventory {
    val empty: Inventory = Inventory(Map.empty, 0L)
  }

  private sealed trait Lifecycle
  private case object Open extends Lifecycle
  private final case class Poisoned(cause: MetagraphBinaryCustodyError) extends Lifecycle
  private case object Closed extends Lifecycle

  private val LockFileName = ".metagraph-binary-custody.lock"
  private val ArtifactSuffix = ".binary"
  private val ContentHashLength = 64

  private final case class StablePathIdentity(path: Path, fileKey: AnyRef)

  /** Authority for the dedicated local directory is held through one secure directory descriptor, not through the caller's pathname. Java
    * cannot exclude a malicious process running as the same OS identity from unlink/ABA attacks inside an owner-writable directory; this
    * local store therefore requires a trusted host account with no concurrent out-of-band mutator. Stable identity checks turn accidental
    * replacement or a violated host precondition into a fail-closed local Unavailable error.
    */
  private final class PinnedCustodyDirectory[F[_]: Async] private (
    val root: Path,
    componentIdentities: Vector[StablePathIdentity],
    rootFileKey: AnyRef,
    directoryStream: SecureDirectoryStream[Path],
    directoryChannel: FileChannel,
    val lockName: Path,
    lockFileKey: AnyRef,
    lockChannel: FileChannel,
    lock: FileLock
  ) {
    val lockPath: Path = root.resolve(lockName)
    private val storeCloseAttempted = new AtomicBoolean(false)

    def artifactPath(leaf: Path): Path = root.resolve(leaf)

    def validateOwnership(operation: String): F[Unit] =
      Async[F].blocking {
        componentIdentities.foreach { expected =>
          val current = readDirectoryIdentity(expected.path)
          if (current.fileKey != expected.fileKey)
            throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
              expected.path,
              s"path component fileKey changed during $operation"
            )
        }

        ensureTrustedDirectoryPermissions(root)

        val pinnedRoot = readDirectoryIdentity(directoryStream, root)
        if (pinnedRoot.fileKey != rootFileKey)
          throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
            root,
            s"pinned directory fileKey changed during $operation"
          )

        if (!directoryChannel.isOpen)
          throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
            root,
            s"pinned directory force channel is closed during $operation"
          )

        if (!lockChannel.isOpen || !lock.isValid)
          throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
            lockPath,
            s"held lock is no longer valid during $operation"
          )

        val relativeLock = readRegularFileIdentity(directoryStream, lockName, lockPath)
        val absoluteLock = readRegularFileIdentity(lockPath)
        if (relativeLock.fileKey != lockFileKey || absoluteLock.fileKey != lockFileKey)
          throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
            lockPath,
            s"lock directory entry changed during $operation"
          )
      }.handleErrorWith {
        case error: MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged => Async[F].raiseError(error)
        case error =>
          Async[F].raiseError(
            MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
              root,
              s"unable to revalidate pinned ownership during $operation",
              error
            )
          )
      }

    def freshDirectoryStream: Resource[F, SecureDirectoryStream[Path]] =
      Resource.make {
        Async[F].blocking {
          val opened = directoryStream.newDirectoryStream(Paths.get("."), LinkOption.NOFOLLOW_LINKS)
          val secure = opened match {
            case value: SecureDirectoryStream[Path @unchecked] => value
            case _ =>
              opened.close()
              throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
                root,
                "provider returned a non-secure child directory stream"
              )
          }
          val identity = readDirectoryIdentity(secure, root)
          if (identity.fileKey != rootFileKey) {
            secure.close()
            throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
              root,
              "fresh relative directory stream does not name the pinned root"
            )
          }
          secure
        }
      }(stream => Async[F].blocking(stream.close()))

    def openFile(leaf: Path, options: Set[OpenOption]): Resource[F, FileChannel] =
      Resource.make {
        Async[F].blocking {
          requireLeaf(leaf)
          requireFileChannel(
            directoryStream.newByteChannel(leaf, options.asJava),
            artifactPath(leaf),
            "provider did not return a forceable FileChannel"
          )
        }
      }(channel => Async[F].blocking(channel.close()))

    def readAttributes(leaf: Path): F[BasicFileAttributes] =
      Async[F].blocking {
        requireLeaf(leaf)
        readAttributesRelative(directoryStream, leaf, artifactPath(leaf))
      }

    def deleteIfExists(leaf: Path): F[Boolean] =
      Async[F].blocking {
        requireLeaf(leaf)
        try {
          directoryStream.deleteFile(leaf)
          true
        } catch { case _: NoSuchFileException => false }
      }

    def atomicMove(source: Path, target: Path): F[Unit] =
      Async[F].blocking {
        requireLeaf(source)
        requireLeaf(target)
        directoryStream.move(source, directoryStream, target)
      }.void

    def forceDirectory: F[Unit] =
      Async[F].blocking(directoryChannel.force(true))

    def closeStoreOwnership: F[Unit] =
      Async[F].delay(storeCloseAttempted.set(true)) >> validateOwnership("store close")

    def release: F[Unit] =
      Async[F].uncancelable { _ =>
        val validate =
          if (storeCloseAttempted.get()) Async[F].pure[Either[Throwable, Unit]](Right(()))
          else validateOwnership("resource release").attempt

        validate.flatMap { validation =>
          Async[F].blocking {
            var failure: Throwable = validation.swap.toOption.orNull

            def closeStep(step: => Unit): Unit =
              try step
              catch {
                case NonFatal(error) if failure ne null => failure.addSuppressed(error)
                case NonFatal(error)                    => failure = error
              }

            closeStep(lock.release())
            closeStep(lockChannel.close())
            closeStep(directoryChannel.close())
            closeStep(directoryStream.close())

            if (failure ne null) throw failure
          }
        }
      }
  }

  private object PinnedCustodyDirectory {
    def resource[F[_]: Async](
      root: Path,
      openDirectory: Path => DirectoryStream[Path]
    ): Resource[F, PinnedCustodyDirectory[F]] =
      Resource.make(acquire[F](root, openDirectory))(_.release)

    private def acquire[F[_]: Async](
      root: Path,
      openDirectory: Path => DirectoryStream[Path]
    ): F[PinnedCustodyDirectory[F]] =
      Async[F].blocking {
        var directoryStream: SecureDirectoryStream[Path] = null
        var directoryChannel: FileChannel = null
        var lockChannel: FileChannel = null
        var lock: FileLock = null

        try {
          val componentIdentities = captureDirectoryIdentities(root)
          val rootFileKey = componentIdentities.last.fileKey
          ensureTrustedDirectoryPermissions(root)

          val opened = openDirectory(root)
          directoryStream = opened match {
            case value: SecureDirectoryStream[Path @unchecked] => value
            case _ =>
              opened.close()
              throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
                root,
                "filesystem provider does not expose SecureDirectoryStream"
              )
          }

          val pinnedRoot = readDirectoryIdentity(directoryStream, root)
          if (pinnedRoot.fileKey != rootFileKey)
            throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
              root,
              "root changed while opening the secure directory descriptor"
            )

          directoryChannel = requireFileChannel(
            directoryStream.newByteChannel(
              Paths.get("."),
              Set[OpenOption](StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS).asJava
            ),
            root,
            "provider cannot open the pinned directory as a forceable FileChannel"
          )
          directoryChannel.force(true)

          val lockName = Paths.get(LockFileName)
          val lockPath = root.resolve(lockName)
          val before = readOptionalAttributesRelative(directoryStream, lockName, lockPath)
          before.foreach(attributes => requireRegularFile(attributes, lockPath))

          lockChannel = requireFileChannel(
            directoryStream.newByteChannel(
              lockName,
              Set[OpenOption](StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).asJava
            ),
            lockPath,
            "provider cannot open the custody lock as a FileChannel"
          )
          val after = readRegularFileIdentity(directoryStream, lockName, lockPath)
          before.foreach { attributes =>
            val beforeKey = requireFileKey(attributes, lockPath)
            if (beforeKey != after.fileKey)
              throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
                lockPath,
                "lock changed while its no-follow channel was opened"
              )
          }

          lock =
            try lockChannel.tryLock()
            catch { case _: OverlappingFileLockException => null }
          if (lock eq null)
            throw MetagraphBinaryCustodyUnavailable.StorageAccessFailed(
              "acquire lock",
              lockPath,
              new java.io.IOException("custody directory lock is already held")
            )

          val locked = readRegularFileIdentity(directoryStream, lockName, lockPath)
          if (locked.fileKey != after.fileKey)
            throw MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
              lockPath,
              "lock changed while exclusive ownership was acquired"
            )

          directoryChannel.force(true)

          new PinnedCustodyDirectory[F](
            root,
            componentIdentities,
            rootFileKey,
            directoryStream,
            directoryChannel,
            lockName,
            locked.fileKey,
            lockChannel,
            lock
          )
        } catch {
          case NonFatal(error) =>
            val failure: Throwable = error match {
              case typed: MetagraphBinaryCustodyError => typed
              case other =>
                MetagraphBinaryCustodyUnavailable.StorageAccessFailed("pin directory ownership", root, other)
            }

            def closeStep(step: => Unit): Unit =
              try step
              catch { case NonFatal(closeError) => failure.addSuppressed(closeError) }

            if (lock ne null) closeStep(lock.release())
            if (lockChannel ne null) closeStep(lockChannel.close())
            if (directoryChannel ne null) closeStep(directoryChannel.close())
            if (directoryStream ne null) closeStep(directoryStream.close())
            throw failure
        }
      }
  }

  /** Exclusively own an already-created dedicated directory. The caller must durably install the directory entry in its parent before
    * constructing this store; creating a directory here and forcing only its contents would make the first successful capability claim
    * stronger crash durability than the filesystem establishes.
    */
  def resource[F[_]: Async](
    directory: Path,
    limits: MetagraphBinaryCustodyLimits
  ): Resource[F, MetagraphBinaryCustodyStore[F]] =
    resourceWith(directory, limits, DurableWriteHook.noop[F, BinaryArtifact])

  private[custody] def resourceWith[F[_]: Async](
    directory: Path,
    limits: MetagraphBinaryCustodyLimits,
    writeHook: DurableWriteHook[F, BinaryArtifact]
  ): Resource[F, MetagraphBinaryCustodyStore[F]] =
    resourceWith(directory, limits, writeHook, path => Files.newDirectoryStream(path), ArtifactOpenHook.noop[F])

  private[custody] def resourceWith[F[_]: Async](
    directory: Path,
    limits: MetagraphBinaryCustodyLimits,
    writeHook: DurableWriteHook[F, BinaryArtifact],
    openDirectory: Path => DirectoryStream[Path]
  ): Resource[F, MetagraphBinaryCustodyStore[F]] =
    resourceWith(directory, limits, writeHook, openDirectory, ArtifactOpenHook.noop[F])

  private[custody] def resourceWith[F[_]: Async](
    directory: Path,
    limits: MetagraphBinaryCustodyLimits,
    writeHook: DurableWriteHook[F, BinaryArtifact],
    openDirectory: Path => DirectoryStream[Path],
    artifactOpenHook: ArtifactOpenHook[F]
  ): Resource[F, MetagraphBinaryCustodyStore[F]] = {
    val root = directory.toAbsolutePath.normalize()

    for {
      pinned <- PinnedCustodyDirectory.resource[F](root, openDirectory)
      mutex <- Resource.eval(Semaphore[F](1L))
      lifecycle <- Resource.eval(Ref.of[F, Lifecycle](Open))
      identity <- Resource.eval(Async[F].delay(new Object))
      initial <- Resource.eval(scanInventory(pinned, limits, artifactOpenHook))
      inventory <- Resource.eval(Ref.of[F, Inventory](initial))
      store <- Resource.make(
        Async[F].delay(
          new LiveMetagraphBinaryCustodyStore[F](
            pinned,
            limits,
            writeHook,
            artifactOpenHook,
            mutex,
            lifecycle,
            identity,
            inventory
          )
        )
      )(_.close)
    } yield store
  }

  private def captureDirectoryIdentities(root: Path): Vector[StablePathIdentity] = {
    val absolute = root.toAbsolutePath.normalize()
    val filesystemRoot = Option(absolute.getRoot).getOrElse(
      throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(absolute, "absolute path has no filesystem root")
    )
    val paths = absolute.iterator().asScala.scanLeft(filesystemRoot)((current, component) => current.resolve(component)).toVector
    paths.map(readDirectoryIdentity)
  }

  private def readDirectoryIdentity(path: Path): StablePathIdentity = {
    val attributes =
      try Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
      catch {
        case _: NoSuchFileException =>
          throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(path, "directory component does not exist")
      }
    if (attributes.isSymbolicLink)
      throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(path, "symbolic-link component")
    if (!attributes.isDirectory)
      throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(path, "non-directory component")
    StablePathIdentity(path, requireFileKey(attributes, path))
  }

  private def readDirectoryIdentity(
    directoryStream: SecureDirectoryStream[Path],
    diagnosticPath: Path
  ): StablePathIdentity = {
    val view = Option(directoryStream.getFileAttributeView(classOf[BasicFileAttributeView])).getOrElse(
      throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
        diagnosticPath,
        "provider exposes no BasicFileAttributeView for the pinned directory"
      )
    )
    val attributes = view.readAttributes()
    if (!attributes.isDirectory)
      throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(
        diagnosticPath,
        "secure directory descriptor does not name a directory"
      )
    StablePathIdentity(diagnosticPath, requireFileKey(attributes, diagnosticPath))
  }

  private def readRegularFileIdentity(path: Path): StablePathIdentity = {
    val attributes = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
    requireRegularFile(attributes, path)
    StablePathIdentity(path, requireFileKey(attributes, path))
  }

  private def readRegularFileIdentity(
    directoryStream: SecureDirectoryStream[Path],
    leaf: Path,
    diagnosticPath: Path
  ): StablePathIdentity = {
    val attributes = readAttributesRelative(directoryStream, leaf, diagnosticPath)
    requireRegularFile(attributes, diagnosticPath)
    StablePathIdentity(diagnosticPath, requireFileKey(attributes, diagnosticPath))
  }

  private def readOptionalAttributesRelative(
    directoryStream: SecureDirectoryStream[Path],
    leaf: Path,
    diagnosticPath: Path
  ): Option[BasicFileAttributes] =
    try readAttributesRelative(directoryStream, leaf, diagnosticPath).some
    catch { case _: NoSuchFileException => None }

  private def readAttributesRelative(
    directoryStream: SecureDirectoryStream[Path],
    leaf: Path,
    diagnosticPath: Path
  ): BasicFileAttributes = {
    requireLeaf(leaf)
    val view = Option(
      directoryStream.getFileAttributeView(leaf, classOf[BasicFileAttributeView], LinkOption.NOFOLLOW_LINKS)
    ).getOrElse(
      throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
        diagnosticPath,
        "provider exposes no relative no-follow BasicFileAttributeView"
      )
    )
    view.readAttributes()
  }

  private def requireRegularFile(attributes: BasicFileAttributes, path: Path): Unit =
    if (attributes.isSymbolicLink)
      throw MetagraphBinaryCustodyUnavailable.LockPreconditionFailed(path, "symbolic-link")
    else if (!attributes.isRegularFile)
      throw MetagraphBinaryCustodyUnavailable.LockPreconditionFailed(path, "not a regular file")

  private def requireFileKey(attributes: BasicFileAttributes, path: Path): AnyRef =
    Option(attributes.fileKey()).getOrElse(
      throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
        path,
        "filesystem provider exposes no stable fileKey"
      )
    )

  private def requireFileChannel(channel: SeekableByteChannel, path: Path, detail: String): FileChannel =
    channel match {
      case fileChannel: FileChannel => fileChannel
      case other =>
        try other.close()
        catch { case NonFatal(_) => () }
        throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(path, detail)
    }

  private def requireLeaf(path: Path): Unit = {
    val value = Option(path).map(_.toString).getOrElse("")
    if (path == null || path.isAbsolute || path.getNameCount != 1 || value == "." || value == "..")
      throw new IllegalArgumentException(s"custody operation requires one relative leaf name, got $path")
  }

  private def ensureTrustedDirectoryPermissions(root: Path): Unit = {
    val permissions =
      try Files.getPosixFilePermissions(root, LinkOption.NOFOLLOW_LINKS).asScala
      catch {
        case error: UnsupportedOperationException =>
          throw MetagraphBinaryCustodyUnavailable.SecureStorageUnsupported(
            root,
            "provider cannot enforce POSIX directory write permissions",
            error
          )
      }
    val externallyWritable = Set(PosixFilePermission.GROUP_WRITE, PosixFilePermission.OTHERS_WRITE).intersect(permissions)
    if (externallyWritable.nonEmpty)
      throw MetagraphBinaryCustodyUnavailable.DirectoryPreconditionFailed(
        root,
        s"dedicated directory is writable outside its owner: ${externallyWritable.mkString(",")}"
      )
  }

  private def scanInventory[F[_]: Async](
    pinned: PinnedCustodyDirectory[F],
    limits: MetagraphBinaryCustodyLimits,
    artifactOpenHook: ArtifactOpenHook[F]
  ): F[Inventory] = {
    val scan = for {
      _ <- pinned.validateOwnership("inventory scan start")
      enumerated <- enumerateArtifacts[F](pinned, limits.maxItems)
      (artifactPaths, deletedTemporary) = enumerated
      _ <- Async[F].whenA(deletedTemporary)(pinned.forceDirectory)
      inventory <- artifactPaths.sortBy(_._1.hex).foldM(Inventory.empty) {
        case (current, (contentAddress, leaf)) =>
          for {
            bytes <- readAndVerify[F](
              pinned,
              leaf,
              contentAddress,
              None,
              limits,
              existingInventory = true,
              artifactOpenHook = artifactOpenHook
            )
            next <- Async[F].delay(current.add(contentAddress, bytes.length.toLong))
            _ <- Async[F].raiseWhen(next.totalBytes > limits.maxTotalBytes)(
              MetagraphBinaryCustodyUnavailable.ExistingInventoryExceedsLimits(
                next.entries.size,
                next.totalBytes,
                limits
              )
            )
          } yield next
      }
      _ <- pinned.validateOwnership("inventory scan end")
    } yield inventory

    scan.handleErrorWith {
      case error: MetagraphBinaryCustodyError => Async[F].raiseError(error)
      case error =>
        Async[F].raiseError(
          MetagraphBinaryCustodyUnavailable.StorageAccessFailed("scan inventory", pinned.root, error)
        )
    }
  }

  private def enumerateArtifacts[F[_]: Async](
    pinned: PinnedCustodyDirectory[F],
    maxItems: Int
  ): F[(Vector[(LocalCustodyAddress, Path)], Boolean)] =
    pinned.freshDirectoryStream.use { stream =>
      Async[F].blocking {
        val iterator = stream.iterator()
        val artifacts = Vector.newBuilder[(LocalCustodyAddress, Path)]
        // Exclusive ownership plus the store semaphore permits at most one in-flight atomic-writer temp at process crash.
        val maximumDirectoryEntries = maxItems.toLong + 1L
        var directoryEntryCount = 0L
        var artifactCount = 0L
        var deletedTemporary = false

        while (iterator.hasNext) {
          val enumerated = iterator.next()
          val leaf = enumerated.getFileName
          requireLeaf(leaf)
          val path = pinned.artifactPath(leaf)
          if (leaf == pinned.lockName) ()
          else {
            directoryEntryCount = Math.addExact(directoryEntryCount, 1L)
            if (directoryEntryCount > maximumDirectoryEntries)
              throw MetagraphBinaryCustodyUnavailable.DirectoryEnumerationLimitExceeded(
                directoryEntryCount,
                maximumDirectoryEntries
              )

            if (isStaleTemporary(leaf.toString)) {
              try stream.deleteFile(leaf)
              catch { case _: NoSuchFileException => () }
              deletedTemporary = true
            } else {
              val contentAddress = parseArtifactName(leaf.toString).fold(
                _ => throw MetagraphBinaryCustodyRecoveryRequired.UnexpectedDirectoryEntry(path),
                identity
              )
              artifactCount = Math.addExact(artifactCount, 1L)
              if (artifactCount > maxItems.toLong)
                throw MetagraphBinaryCustodyUnavailable.InventoryEnumerationLimitExceeded(artifactCount, maxItems)
              artifacts += contentAddress -> leaf
            }
          }
        }

        artifacts.result() -> deletedTemporary
      }
    }

  private def isStaleTemporary(name: String): Boolean =
    name.startsWith(".") && name.endsWith(".tmp") && {
      val withoutLeadingDot = name.drop(1)
      val addressEnd = withoutLeadingDot.indexOf(ArtifactSuffix)
      addressEnd == ContentHashLength &&
      isCanonicalHash(withoutLeadingDot.take(ContentHashLength)) &&
      withoutLeadingDot.drop(ContentHashLength).startsWith(s"$ArtifactSuffix.")
    }

  private def parseArtifactName(name: String): Either[Unit, LocalCustodyAddress] = {
    val expectedLength = ContentHashLength + ArtifactSuffix.length
    if (name.length != expectedLength || !name.endsWith(ArtifactSuffix)) Left(())
    else {
      val value = name.take(ContentHashLength)
      Either.cond(isCanonicalHash(value), LocalCustodyAddress.fromCanonicalHex(value), ())
    }
  }

  private def validateContentAddress(
    contentAddress: LocalCustodyAddress
  ): Either[MetagraphBinaryCustodyRejected.InvalidContentAddress, LocalCustodyAddress] =
    Option(contentAddress)
      .flatMap(address => Option(address.hex))
      .filter(isCanonicalHash)
      .toRight(MetagraphBinaryCustodyRejected.InvalidContentAddress("expected exactly 64 lowercase hexadecimal characters"))
      .map(_ => contentAddress)

  private def isCanonicalHash(value: String): Boolean =
    value.length == ContentHashLength && value.forall(character =>
      character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
    )

  private def artifactLeaf(contentAddress: LocalCustodyAddress): Path =
    Paths.get(contentAddress.hex + ArtifactSuffix)

  private def readAndVerify[F[_]: Async](
    pinned: PinnedCustodyDirectory[F],
    leaf: Path,
    expectedAddress: LocalCustodyAddress,
    expectedLength: Option[Long],
    limits: MetagraphBinaryCustodyLimits,
    existingInventory: Boolean,
    artifactOpenHook: ArtifactOpenHook[F]
  ): F[Array[Byte]] = {
    val path = pinned.artifactPath(leaf)

    def artifactTooLarge(actualBytes: Long): MetagraphBinaryCustodyError =
      if (existingInventory)
        MetagraphBinaryCustodyUnavailable.ExistingArtifactExceedsLimit(
          expectedAddress,
          actualBytes,
          limits.maxArtifactBytes
        )
      else
        MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
          expectedAddress,
          path,
          s"artifact size $actualBytes exceeds caller limit ${limits.maxArtifactBytes}"
        )

    val verify = for {
      opened <- openArtifactNoFollow[F](pinned, leaf, expectedAddress, artifactOpenHook).use {
        case (input, openedSize, openedKey) =>
          for {
            _ <- Async[F].raiseWhen(openedSize > limits.maxArtifactBytes)(artifactTooLarge(openedSize))
            _ <- expectedLength.traverse_ { length =>
              Async[F].raiseUnless(openedSize == length)(
                MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
                  expectedAddress,
                  path,
                  s"expected $length bytes, opened $openedSize"
                )
              )
            }
            bytes <- readBounded[F](input, expectedAddress, path, limits.maxArtifactBytes, artifactTooLarge)
          } yield bytes -> openedKey
      }
      (bytes, openedKey) = opened
      closingAttributes <- pinned.readAttributes(leaf)
      _ <- Async[F].raiseUnless(
        closingAttributes.isRegularFile &&
          !closingAttributes.isSymbolicLink &&
          requireFileKey(closingAttributes, path) == openedKey
      )(
        MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
          path,
          "artifact directory entry changed while exact bytes were read"
        )
      )
      _ <- expectedLength.traverse_ { length =>
        Async[F].raiseUnless(bytes.length.toLong == length)(
          MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
            expectedAddress,
            path,
            s"expected $length bytes, read ${bytes.length}"
          )
        )
      }
      actualAddress <- Async[F].blocking(LocalCustodyAddress.fromExactBytes(bytes))
      _ <- Async[F].raiseUnless(actualAddress == expectedAddress)(
        MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
          expectedAddress,
          path,
          s"exact-byte local SHA-256 was ${actualAddress.hex}"
        )
      )
    } yield bytes

    verify.handleErrorWith {
      case error: MetagraphBinaryCustodyRecoveryRequired => Async[F].raiseError(error)
      case error: MetagraphBinaryCustodyUnavailable      => Async[F].raiseError(error)
      case error: NoSuchFileException =>
        Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.MissingArtifact(expectedAddress, path, error))
      case error: FileNotFoundException =>
        Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.MissingArtifact(expectedAddress, path, error))
      case error: FileSystemException =>
        pinned.readAttributes(leaf).attempt.flatMap {
          case Right(attributes) if attributes.isSymbolicLink || !attributes.isRegularFile =>
            Async[F].raiseError(
              MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
                expectedAddress,
                path,
                "artifact directory entry is not a no-follow regular file",
                error
              )
            )
          case Left(_: NoSuchFileException) =>
            Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.MissingArtifact(expectedAddress, path, error))
          case _ =>
            Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.ArtifactReadFailed(expectedAddress, path, error))
        }
      case error =>
        Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.ArtifactReadFailed(expectedAddress, path, error))
    }
  }

  private def openArtifactNoFollow[F[_]: Async](
    pinned: PinnedCustodyDirectory[F],
    leaf: Path,
    expectedAddress: LocalCustodyAddress,
    artifactOpenHook: ArtifactOpenHook[F]
  ): Resource[F, (InputStream, Long, AnyRef)] = {
    val path = pinned.artifactPath(leaf)

    Resource.eval(pinned.readAttributes(leaf)).flatMap { before =>
      Resource
        .eval(
          Async[F].raiseUnless(before.isRegularFile && !before.isSymbolicLink)(
            MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(expectedAddress, path, "not a no-follow regular file")
          ) >> Async[F].delay(requireFileKey(before, path))
        )
        .flatMap { expectedKey =>
          Resource.eval(artifactOpenHook.beforeOpen(path)) >>
            pinned
              .openFile(leaf, Set[OpenOption](StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS))
              .evalMap { channel =>
                for {
                  afterOpen <- pinned.readAttributes(leaf)
                  _ <- Async[F].raiseUnless(
                    afterOpen.isRegularFile &&
                      !afterOpen.isSymbolicLink &&
                      requireFileKey(afterOpen, path) == expectedKey
                  )(
                    MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
                      path,
                      "artifact directory entry changed while its no-follow channel was opened"
                    )
                  )
                  size <- Async[F].blocking(channel.size())
                } yield (Channels.newInputStream(channel), size, expectedKey)
              }
        }
    }
  }

  private def readBounded[F[_]: Async](
    input: InputStream,
    expectedAddress: LocalCustodyAddress,
    path: Path,
    maximumBytes: Long,
    artifactTooLarge: Long => MetagraphBinaryCustodyError
  ): F[Array[Byte]] =
    Async[F].blocking {
      val output = new ByteArrayOutputStream(math.min(maximumBytes, 8192L).toInt)
      val buffer = new Array[Byte](8192)
      var total = 0L
      var read = input.read(buffer)

      while (read >= 0) {
        if (read > 0) {
          total =
            try Math.addExact(total, read.toLong)
            catch {
              case _: ArithmeticException => throw artifactTooLarge(Long.MaxValue)
            }
          if (total > maximumBytes) throw artifactTooLarge(total)
          output.write(buffer, 0, read)
        }
        read = input.read(buffer)
      }
      output.toByteArray
    }.handleErrorWith {
      case error: MetagraphBinaryCustodyError => Async[F].raiseError(error)
      case error =>
        Async[F].raiseError(MetagraphBinaryCustodyRecoveryRequired.ArtifactReadFailed(expectedAddress, path, error))
    }

  private final class LiveMetagraphBinaryCustodyStore[F[_]: Async](
    pinned: PinnedCustodyDirectory[F],
    limits: MetagraphBinaryCustodyLimits,
    writeHook: DurableWriteHook[F, BinaryArtifact],
    artifactOpenHook: ArtifactOpenHook[F],
    mutex: Semaphore[F],
    lifecycle: Ref[F, Lifecycle],
    storeIdentity: AnyRef,
    inventory: Ref[F, Inventory]
  ) extends MetagraphBinaryCustodyStore[F] {
    private val directory = pinned.root

    def putExact(bytes: Array[Byte]): F[DurablyStoredBinary] =
      withOpen {
        capture(bytes).flatMap { captured =>
          Async[F].blocking(LocalCustodyAddress.fromExactBytes(captured)).flatMap { contentAddress =>
            val leaf = artifactLeaf(contentAddress)
            val path = pinned.artifactPath(leaf)

            inventory.get.flatMap { current =>
              current.entries.get(contentAddress) match {
                case Some(expectedLength) =>
                  readAndVerify(
                    pinned,
                    leaf,
                    contentAddress,
                    expectedLength.some,
                    limits,
                    existingInventory = false,
                    artifactOpenHook = artifactOpenHook
                  ).flatMap { stored =>
                    if (stored.sameElements(captured)) mint(contentAddress, expectedLength)
                    else
                      Async[F].raiseError(
                        MetagraphBinaryCustodyRecoveryRequired.ContentAddressCollision(contentAddress, path)
                      )
                  }
                case None => storeNew(current, contentAddress, leaf, captured)
              }
            }
          }
        }
      }

    def openExact(contentAddress: LocalCustodyAddress): F[DurablyStoredBinary] =
      withOpen {
        validateContentAddress(contentAddress).liftTo[F].flatMap { validated =>
          inventory.get.flatMap { current =>
            current.entries.get(validated) match {
              case Some(expectedLength) =>
                readAndVerify(
                  pinned,
                  artifactLeaf(validated),
                  validated,
                  expectedLength.some,
                  limits,
                  existingInventory = false,
                  artifactOpenHook = artifactOpenHook
                ) >>
                  mint(validated, expectedLength)
              case None =>
                Async[F].raiseError(
                  MetagraphBinaryCustodyRecoveryRequired.MissingArtifact(
                    validated,
                    pinned.artifactPath(artifactLeaf(validated))
                  )
                )
            }
          }
        }
      }

    def readExact(stored: DurablyStoredBinary): F[Array[Byte]] =
      withOpen {
        if ((stored eq null) || (stored.storeIdentity ne storeIdentity))
          Async[F].raiseError(MetagraphBinaryCustodyRejected.ForeignCapability())
        else
          inventory.get.flatMap { current =>
            current.entries.get(stored.contentAddress) match {
              case Some(expectedLength) if expectedLength == stored.byteLength =>
                readAndVerify(
                  pinned,
                  artifactLeaf(stored.contentAddress),
                  stored.contentAddress,
                  expectedLength.some,
                  limits,
                  existingInventory = false,
                  artifactOpenHook = artifactOpenHook
                )
              case Some(expectedLength) =>
                Async[F].raiseError(
                  MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
                    stored.contentAddress,
                    pinned.artifactPath(artifactLeaf(stored.contentAddress)),
                    s"capability length ${stored.byteLength} differs from inventory length $expectedLength"
                  )
                )
              case None =>
                Async[F].raiseError(
                  MetagraphBinaryCustodyRecoveryRequired.MissingArtifact(
                    stored.contentAddress,
                    pinned.artifactPath(artifactLeaf(stored.contentAddress))
                  )
                )
            }
          }
      }

    def usage: F[MetagraphBinaryCustodyUsage] = withOpen(inventory.get.map(_.usage))

    private[custody] def close: F[Unit] =
      mutex.permit.use(_ => lifecycle.set(Closed) >> pinned.closeStoreOwnership)

    private def withOpen[A](operation: => F[A]): F[A] =
      mutex.permit.use { _ =>
        ensureOpen >>
          pinned.validateOwnership("operation start") >>
          Async[F].defer(operation).guarantee(pinned.validateOwnership("operation end"))
      }

    private def ensureOpen: F[Unit] =
      lifecycle.get.flatMap {
        case Open            => Async[F].unit
        case Poisoned(cause) => Async[F].raiseError(cause)
        case Closed          => Async[F].raiseError(MetagraphBinaryCustodyUnavailable.StoreClosed(directory))
      }

    private def capture(bytes: Array[Byte]): F[Array[Byte]] =
      Async[F].blocking {
        val supplied = Option(bytes).getOrElse(throw MetagraphBinaryCustodyRejected.InvalidArtifact("null byte array"))
        if (supplied.length.toLong > limits.maxArtifactBytes)
          throw MetagraphBinaryCustodyUnavailable.ArtifactTooLarge(supplied.length.toLong, limits.maxArtifactBytes)
        supplied.clone()
      }

    private def storeNew(
      current: Inventory,
      contentAddress: LocalCustodyAddress,
      leaf: Path,
      captured: Array[Byte]
    ): F[DurablyStoredBinary] =
      Async[F]
        .delay(Math.addExact(current.totalBytes, captured.length.toLong))
        .adaptError {
          case _: ArithmeticException =>
            MetagraphBinaryCustodyRecoveryRequired.InventoryByteCountOverflow(
              current.totalBytes,
              captured.length.toLong
            )
        }
        .flatMap { newTotal =>
          if (current.entries.size >= limits.maxItems || newTotal > limits.maxTotalBytes)
            Async[F].raiseError(
              MetagraphBinaryCustodyUnavailable.CapacityExceeded(
                current.entries.size,
                current.totalBytes,
                captured.length.toLong,
                limits
              )
            )
          else
            ensureAbsent(leaf, contentAddress) >> durableCreate(contentAddress, leaf, captured)
        }

    private def ensureAbsent(leaf: Path, contentAddress: LocalCustodyAddress): F[Unit] = {
      val path = pinned.artifactPath(leaf)
      pinned.readAttributes(leaf).attempt.flatMap {
        case Left(_: NoSuchFileException) => Async[F].unit
        case Left(error)                  => Async[F].raiseError(error)
        case Right(_) =>
          Async[F].raiseError(
            MetagraphBinaryCustodyRecoveryRequired.CorruptArtifact(
              contentAddress,
              path,
              "content-addressed target exists but is absent from the verified inventory"
            )
          )
      }
    }

    private def durableCreate(
      contentAddress: LocalCustodyAddress,
      targetLeaf: Path,
      captured: Array[Byte]
    ): F[DurablyStoredBinary] =
      Async[F].uncancelable { poll =>
        val artifact = new ExactBinaryArtifact(contentAddress)
        val temporaryLeaf = Paths.get(s".${targetLeaf.toString}.${UUID.randomUUID().toString}.tmp")
        val temporaryPath = pinned.artifactPath(temporaryLeaf)
        val targetPath = pinned.artifactPath(targetLeaf)
        var moved = false

        val write = pinned
          .openFile(
            temporaryLeaf,
            Set[OpenOption](StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
          )
          .use { channel =>
            for {
              initialAttributes <- pinned.readAttributes(temporaryLeaf)
              _ <- Async[F].raiseUnless(initialAttributes.isRegularFile && !initialAttributes.isSymbolicLink)(
                MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
                  temporaryPath,
                  "new temporary directory entry is not a no-follow regular file"
                )
              )
              temporaryKey <- Async[F].delay(requireFileKey(initialAttributes, temporaryPath))
              _ <- poll(at(artifact, DurableWriteStage.Write)(writeExact(channel, captured)))
              _ <- poll(at(artifact, DurableWriteStage.ForceFile)(Async[F].blocking(channel.force(true))))
              beforeMove <- pinned.readAttributes(temporaryLeaf)
              _ <- Async[F].raiseUnless(
                beforeMove.isRegularFile &&
                  !beforeMove.isSymbolicLink &&
                  requireFileKey(beforeMove, temporaryPath) == temporaryKey
              )(
                MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
                  temporaryPath,
                  "temporary directory entry changed before atomic move"
                )
              )
              _ <- ensureAbsent(targetLeaf, contentAddress)
              _ <- writeHook.onEvent(DurableWriteEvent(artifact, DurableWriteStage.AtomicMove, DurableWriteBoundary.Before))
              _ <- pinned.atomicMove(temporaryLeaf, targetLeaf)
              _ <- Async[F].delay { moved = true }
              _ <- writeHook.onEvent(DurableWriteEvent(artifact, DurableWriteStage.AtomicMove, DurableWriteBoundary.After))
              installedAttributes <- pinned.readAttributes(targetLeaf)
              _ <- Async[F].raiseUnless(
                installedAttributes.isRegularFile &&
                  !installedAttributes.isSymbolicLink &&
                  requireFileKey(installedAttributes, targetPath) == temporaryKey
              )(
                MetagraphBinaryCustodyUnavailable.OwnershipIdentityChanged(
                  targetPath,
                  "atomic move did not install the opened temporary file"
                )
              )
              _ <- at(artifact, DurableWriteStage.ForceDirectory)(pinned.forceDirectory)
              stored <- at(artifact, DurableWriteStage.ReadBack)(
                readAndVerify(
                  pinned,
                  targetLeaf,
                  contentAddress,
                  captured.length.toLong.some,
                  limits,
                  existingInventory = false,
                  artifactOpenHook = artifactOpenHook
                )
              )
              _ <- Async[F].raiseUnless(stored.sameElements(captured))(
                MetagraphBinaryCustodyRecoveryRequired.ContentAddressCollision(contentAddress, targetPath)
              )
              _ <- inventory.update(_.add(contentAddress, captured.length.toLong))
              capability <- mint(contentAddress, captured.length.toLong)
            } yield capability
          }
          .guarantee(pinned.deleteIfExists(temporaryLeaf).void)

        write.handleErrorWith { original =>
          if (moved) reconcileAfterMovedWriteFailure(contentAddress, original)
          else Async[F].raiseError(MetagraphBinaryCustodyUnavailable.DurableWriteFailed(contentAddress, original))
        }
      }

    private def writeExact(channel: FileChannel, bytes: Array[Byte]): F[Unit] =
      Async[F].blocking {
        channel.position(0L)
        channel.truncate(0L)
        val buffer = ByteBuffer.wrap(bytes)
        while (buffer.hasRemaining) channel.write(buffer)
      }

    private def at[A](artifact: BinaryArtifact, stage: DurableWriteStage)(operation: F[A]): F[A] =
      writeHook.onEvent(DurableWriteEvent(artifact, stage, DurableWriteBoundary.Before)) >>
        operation.flatTap(_ => writeHook.onEvent(DurableWriteEvent(artifact, stage, DurableWriteBoundary.After)))

    private def reconcileAfterMovedWriteFailure(
      contentAddress: LocalCustodyAddress,
      original: Throwable
    ): F[DurablyStoredBinary] = {
      val reconcile = pinned.forceDirectory >> scanInventory(pinned, limits, artifactOpenHook)

      reconcile.attempt.flatMap {
        case Right(recovered) =>
          inventory.set(recovered).attempt.flatMap {
            case Right(_) =>
              Async[F].raiseError(MetagraphBinaryCustodyUnavailable.DurableWriteFailed(contentAddress, original))
            case Left(error) =>
              poison(
                MetagraphBinaryCustodyRecoveryRequired.ArtifactReadFailed(
                  contentAddress,
                  pinned.artifactPath(artifactLeaf(contentAddress)),
                  error
                ),
                original
              )
          }
        case Left(recovery: MetagraphBinaryCustodyRecoveryRequired) =>
          poison(recovery, original)
        case Left(rejected: MetagraphBinaryCustodyRejected) =>
          poison(rejected, original)
        case Left(unavailable: MetagraphBinaryCustodyUnavailable) =>
          poison(unavailable, original)
        case Left(other) =>
          val recovery = MetagraphBinaryCustodyRecoveryRequired.ArtifactReadFailed(
            contentAddress,
            pinned.artifactPath(artifactLeaf(contentAddress)),
            other
          )
          poison(recovery, original)
      }
    }

    private def poison[A](failure: MetagraphBinaryCustodyError, original: Throwable): F[A] =
      Async[F].delay {
        if (failure ne original) failure.addSuppressed(original)
      } >> lifecycle.set(Poisoned(failure)) >> Async[F].raiseError(failure)

    private def mint(contentAddress: LocalCustodyAddress, byteLength: Long): F[DurablyStoredBinary] =
      Async[F].delay(new DurablyStoredBinary(contentAddress, byteLength, storeIdentity))
  }
}
