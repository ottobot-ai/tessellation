package io.constellationnetwork.security.mpt

import java.io._
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.security.{DigestInputStream, MessageDigest}
import java.util.Arrays

import cats.Parallel
import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Semaphore
import cats.effect.syntax.all._
import cats.syntax.all._
import cats.~>

import scala.collection.immutable.SortedMap
import scala.util.control.NonFatal

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.storage.durable.{DurableAtomicWriter, DurableFileError, DurableFileOps, DurableWriteHook}

import scodec.bits.ByteVector

/** A content-addressed, immutable local image of the complete consensus MPT key/value set.
  *
  * This is a dark durability primitive. Nothing in [[io.constellationnetwork.schema.mpt.MptStore]], the branch overlay, snapshot
  * finalization, or recovery consumes it yet. In particular, this component does not close ROOT-002/003/004, does not make finality
  * publication transactional, and does not activate exact-parent branch rejection.
  *
  * Before reversible-publication initialization, an image becomes active only after [[publish]] installs its receipt as the legacy active
  * manifest. After explicit initialization, the additive publication journal owns selection and may point to a verified image or explicit
  * pristine state. Preparing an image never changes either selector. Images are never deleted here, so preceding generations remain
  * available after a failed prepare or publication. Retention and cutoff belong to a later transaction design, not this primitive.
  *
  * The compare-and-set and reader-serialization guarantees apply while one live store resource exclusively owns its directory through the
  * lifetime OS lock. The caller must create `directory` and durably install that directory entry in its parent before constructing this
  * store; an existence check cannot establish parent-entry durability. This primitive bounds streaming image decode and distinguishes
  * pristine state from loss of a previously published active manifest, and distinguishes missing required artifacts from other read
  * failures. Runtime wiring still requires an active-era semantic verifier for every physical key/value, authentication of the supplied
  * exact anchor against hash-bound finality state, and one recoverable intent spanning every finality sink. The raw root rebuild, supplied
  * era labels, and self-consistent manifest/journal do not authenticate a publication revision, authorize a rollback, or establish those
  * external facts.
  */
sealed trait DurableMptImageStore[F[_]] {

  /** Capture, independently rebuild, exact-anchor/era bind, encode, force, atomically install, and read-back verify an inactive image.
    *
    * The input is copied before the asynchronous MPT build begins. The caller may subsequently mutate arrays which backed a
    * `ByteVector.view`; those mutations cannot change the prepared image.
    */
  def prepare(
    generation: Long,
    anchor: GlobalSnapshotStateRef,
    entries: Map[Hex, ByteVector]
  ): F[MptImageReceipt]

  /** Within one exclusively owned live store instance, atomically make a prepared image active if the current manifest's image id and
    * digest equal `expectedPrior`.
    *
    * Publishing the receipt which is already active is idempotent. A different receipt at the same generation fails with a typed
    * [[DurableMptImageError.GenerationConflict]].
    */
  def publish(receipt: MptImageReceipt, expectedPrior: Option[MptImagePointer]): F[Unit]

  /** Durably initialize the additive reversible-publication journal from the exact authenticated legacy active receipt.
    *
    * Initialization never infers state from a corrupt or missing legacy artifact. Once initialized, the journal owns active-image selection
    * and legacy [[publish]] is rejected. The legacy manifest and image encodings remain unchanged.
    */
  def initializeActivePublication(expectedLegacy: Option[MptImageReceipt]): F[MptActivePublication]

  /** Atomically replace the exact journal state.
    *
    * `target.revision` must be exactly one greater than `expectedBefore.revision`. A durable retry which observes `target` already active
    * is idempotent; every other state must equal `expectedBefore` in full or the transition fails. This recovery primitive may select any
    * independently verified prepared image, including a lower image generation, or explicit pristine `None`. It does not decide whether a
    * rollback is authorized or whether either snapshot is canonical.
    */
  def transitionActive(expectedBefore: MptActivePublication, target: MptActivePublication): F[Unit]

  /** Return the exact initialized publication state after verifying the journal, marker, and any referenced image. */
  def activePublication: F[Option[MptActivePublication]]

  /** Run `use` while holding the publication mutex with a store-minted witness of the exact initialized local publication.
    *
    * The witness is minted only after verifying the publication journal, initialization marker, any residual legacy selector, and any
    * referenced image. It proves local durable readback only: it does not authenticate canonicality, authorize a transition, or verify the
    * semantic meaning of MPT entries. The witness is lease-scoped; effectful readback fails with [[DurableMptImageError.LeaseExpired]]
    * after `use` returns or is canceled. `use` must not re-enter this store because the publication mutex is not reentrant.
    */
  def withVerifiedActivePublication[A](use: VerifiedActiveMptPublication[F] => F[A]): F[A]

  /** Return the active receipt only after its manifest and image have both been verified. */
  def activeReceipt: F[Option[MptImageReceipt]]

  /** Read the exact active receipt, verifying manifest identity, image digest and metadata, entry count, and independently rebuilt root. */
  def read(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]]
}

final case class MptImageId(value: Hash)
final case class MptImageDigest(value: Hash)
final case class MptImageCodecEra(value: Hash)
final case class MptImageRootEra(value: Hash)

final class MptImageReadLimits private (
  val maxImageBytes: Long,
  val maxEntries: Int,
  val maxKeyBytes: Int,
  val maxValueBytes: Int
)

object MptImageReadLimits {
  def from(
    maxImageBytes: Long,
    maxEntries: Int,
    maxKeyBytes: Int,
    maxValueBytes: Int
  ): Either[DurableMptImageError.InvalidReadLimits, MptImageReadLimits] = {
    val errors = List(
      Option.when(maxImageBytes < MptImageEncoding.ImageHeaderBytes)(
        s"maxImageBytes must be at least ${MptImageEncoding.ImageHeaderBytes}, got $maxImageBytes"
      ),
      Option.when(maxImageBytes > Int.MaxValue.toLong)(
        s"maxImageBytes must fit the current in-memory image encoder, got $maxImageBytes"
      ),
      Option.when(maxEntries <= 0)(s"maxEntries must be positive, got $maxEntries"),
      Option.when(maxKeyBytes <= 0)(s"maxKeyBytes must be positive, got $maxKeyBytes"),
      Option.when(maxValueBytes <= 0)(s"maxValueBytes must be positive, got $maxValueBytes"),
      Option.when(maxKeyBytes.toLong > maxImageBytes)(
        s"maxKeyBytes cannot exceed maxImageBytes: key=$maxKeyBytes image=$maxImageBytes"
      ),
      Option.when(maxValueBytes.toLong > maxImageBytes)(
        s"maxValueBytes cannot exceed maxImageBytes: value=$maxValueBytes image=$maxImageBytes"
      )
    ).flatten

    Either.cond(
      errors.isEmpty,
      new MptImageReadLimits(maxImageBytes, maxEntries, maxKeyBytes, maxValueBytes),
      DurableMptImageError.InvalidReadLimits(errors.mkString("; "))
    )
  }

  private[mpt] def validate(limits: MptImageReadLimits): Either[DurableMptImageError.InvalidReadLimits, Unit] =
    from(limits.maxImageBytes, limits.maxEntries, limits.maxKeyBytes, limits.maxValueBytes).void
}

final case class MptImagePointer(imageId: MptImageId, digest: MptImageDigest)

object MptImagePointer {
  def fromReceipt(receipt: MptImageReceipt): MptImagePointer = MptImagePointer(receipt.imageId, receipt.digest)
}

final case class MptImageReceipt(
  formatVersion: Int,
  generation: Long,
  imageId: MptImageId,
  anchor: GlobalSnapshotStateRef,
  codecEra: MptImageCodecEra,
  rootEra: MptImageRootEra,
  digest: MptImageDigest,
  entryCount: Int
) {
  def pointer: MptImagePointer = MptImagePointer.fromReceipt(this)
}

final case class MptPublicationRevision(value: Long)

final case class MptActivePublication(revision: MptPublicationRevision, image: Option[MptImageReceipt])

/** A store-minted, lease-scoped witness of an exact locally verified active publication.
  *
  * This final non-case type has no Scala-visible constructor or `copy`. It is a wiring capability, not a sandbox against code already
  * executing inside the validator JVM. See [[DurableMptImageStore.withVerifiedActivePublication]].
  */
final class VerifiedActiveMptPublication[F[_]] private (val read: F[MptActivePublication])

sealed abstract class DurableMptImageError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

sealed trait MptImageArtifactRef

object MptImageArtifactRef {
  case object ActiveManifest extends MptImageArtifactRef
  case object InitializationMarker extends MptImageArtifactRef
  case object ActivePublication extends MptImageArtifactRef
  case object PublicationInitializationMarker extends MptImageArtifactRef
  final case class Image(imageId: MptImageId) extends MptImageArtifactRef
}

object DurableMptImageError {
  final case class InvalidReadLimits(message0: String) extends DurableMptImageError(message0)

  final case class ImageLimitExceeded(field: String, maximum: Long, actual: Long)
      extends DurableMptImageError(s"MPT image $field exceeds limit: maximum=$maximum actual=$actual")

  final case class InvalidGeneration(generation: Long)
      extends DurableMptImageError(s"MPT image generation must be non-negative, got $generation")

  final case class InvalidEntry(message0: String) extends DurableMptImageError(message0)

  final case class InvalidAnchor(message0: String) extends DurableMptImageError(message0)

  final case class RootMismatch(expected: MptRoot, actual: MptRoot)
      extends DurableMptImageError(s"MPT image root mismatch: expected=${expected.value.value} actual=${actual.value.value}")

  final case class CorruptImage(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  final case class CorruptManifest(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  final case class CorruptInitializationMarker(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  final case class CorruptPublication(message0: String, cause0: Throwable = null) extends DurableMptImageError(message0, cause0)

  final case class CorruptPublicationInitializationMarker(message0: String, cause0: Throwable = null)
      extends DurableMptImageError(message0, cause0)

  final case class ReceiptManifestMismatch(expected: MptImageReceipt, active: MptImageReceipt)
      extends DurableMptImageError(
        s"MPT image receipt is not active: expected generation=${expected.generation} image=${expected.imageId.value.value}, " +
          s"active generation=${active.generation} image=${active.imageId.value.value}"
      )

  final case class CompareAndSetConflict(expected: Option[MptImagePointer], actual: Option[MptImagePointer])
      extends DurableMptImageError(s"MPT image manifest compare-and-set conflict: expected=$expected actual=$actual")

  final case class InvalidPublicationRevision(revision: MptPublicationRevision)
      extends DurableMptImageError(s"MPT publication revision must be non-negative, got ${revision.value}")

  final case class NonConsecutivePublicationRevision(
    expectedBefore: MptPublicationRevision,
    target: MptPublicationRevision
  ) extends DurableMptImageError(
        s"MPT publication revision must advance by one: before=${expectedBefore.value} target=${target.value}"
      )

  final case class PublicationRevisionExhausted(revision: MptPublicationRevision)
      extends DurableMptImageError(s"MPT publication revision is exhausted at ${revision.value}")

  final case class PublicationCompareAndSetConflict(expected: MptActivePublication, actual: MptActivePublication)
      extends DurableMptImageError(s"MPT active-publication compare-and-set conflict: expected=$expected actual=$actual")

  final case class PublicationInitializationConflict(
    expectedLegacy: Option[MptImageReceipt],
    actualLegacy: Option[MptImageReceipt]
  ) extends DurableMptImageError(
        s"MPT publication initialization does not match the legacy active receipt: expected=$expectedLegacy actual=$actualLegacy"
      )

  final case class PublicationJournalAlreadyInitialized(actual: MptActivePublication)
      extends DurableMptImageError(s"MPT active-publication journal is already initialized at $actual")

  case object PublicationJournalNotInitialized extends DurableMptImageError("MPT active-publication journal is not initialized")

  final case class PublicationJournalOwnsActiveState(actual: MptActivePublication)
      extends DurableMptImageError(s"MPT active-publication journal owns active image selection at $actual")

  final case class LegacyPublicationDisagreement(publication: MptActivePublication, legacy: MptImageReceipt)
      extends DurableMptImageError(
        s"MPT active-publication journal disagrees with the residual legacy manifest: publication=$publication legacy=$legacy"
      )

  case object LeaseExpired extends DurableMptImageError("The verified MPT active-publication lease has expired")

  final case class PublicationRecoveryFailed(publishFailure: Throwable, recoveryFailure: Throwable)
      extends DurableMptImageError(
        s"MPT active-publication write failed and the prior journal could not be restored: ${publishFailure.getMessage}",
        recoveryFailure
      ) {
    addSuppressed(publishFailure)
  }

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

  final case class DirectoryAlreadyOwned(path: Path)
      extends DurableMptImageError(s"MPT image root directory is already owned by another live store: $path")

  final case class DirectoryLockFailed(path: Path, cause0: Throwable)
      extends DurableMptImageError(s"Unable to acquire or release the MPT image directory lock: $path", cause0)

  final case class MissingArtifact(artifact: MptImageArtifactRef, path: Path, cause0: Throwable = null)
      extends DurableMptImageError(s"Required MPT image artifact is missing: artifact=$artifact path=$path", cause0)

  final case class ArtifactReadFailed(artifact: MptImageArtifactRef, path: Path, cause0: Throwable)
      extends DurableMptImageError(s"Unable to read MPT image artifact: artifact=$artifact path=$path", cause0)

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
  val CurrentFormatVersion: Int = 2

  def resource[F[_]: Async](
    directory: Path,
    codecEra: MptImageCodecEra,
    rootVerifier: MptImageRootVerifier[F],
    limits: MptImageReadLimits
  ): Resource[F, DurableMptImageStore[F]] =
    resourceWith(directory, DurableFileOps.nio[F], DurableWriteHook.noop[F, MptImageArtifact], codecEra, rootVerifier, limits)

  private[mpt] def resourceWith[F[_]: Async](
    directory: Path,
    fileOps: DurableFileOps[F],
    faults: DurableWriteHook[F, MptImageArtifact],
    codecEra: MptImageCodecEra,
    rootVerifier: MptImageRootVerifier[F],
    limits: MptImageReadLimits
  ): Resource[F, DurableMptImageStore[F]] = {
    val imagesDirectory = DurableMptImageLayout.images(directory)
    val lockPath = DurableMptImageLayout.ownerLock(directory)

    for {
      _ <- Resource.eval(Async[F].fromEither(MptImageReadLimits.validate(limits)))
      rootExists <- Resource.eval(fileOps.isDirectory(directory))
      _ <- Resource.eval(Async[F].raiseUnless(rootExists)(DurableMptImageError.RootDirectoryMustExist(directory)))
      _ <- adaptDurableResource(fileOps.acquireExclusiveLock(lockPath))
      _ <- Resource.eval(fileOps.createDirectories(imagesDirectory))
      _ <- Resource.eval(fileOps.forceDirectory(imagesDirectory))
      _ <- Resource.eval(fileOps.forceDirectory(directory))
      mutex <- Resource.eval(Semaphore[F](1L))
    } yield VerifiedActiveMptPublication.liveStore[F](
      directory,
      fileOps,
      faults,
      codecEra,
      rootVerifier,
      limits,
      mutex
    )
  }

  private def adaptDurableResource[F[_]: Async, A](resource: Resource[F, A]): Resource[F, A] =
    resource.mapK(new (F ~> F) {
      def apply[B](effect: F[B]): F[B] = adaptDurableEffect(effect)
    })

  private[mpt] def adaptDurableEffect[F[_]: Async, A](effect: F[A]): F[A] =
    effect.adaptError { case error: DurableFileError => toMptError(error) }

  private def toMptError(error: DurableFileError): Throwable =
    error match {
      case DurableFileError.DirectoryAlreadyOwned(path)      => DurableMptImageError.DirectoryAlreadyOwned(path)
      case DurableFileError.DirectoryLockFailed(path, cause) => DurableMptImageError.DirectoryLockFailed(path, cause)
      case DurableFileError.AtomicMoveRequired(path, cause)  => DurableMptImageError.AtomicMoveRequired(path, cause)
      case other                                             => other
    }
}

private[mpt] object DurableMptImageLayout {
  val ActiveManifestName: String = "active.manifest"
  val ActivePublicationName: String = "active.publication"
  val InitializationMarkerName: String = ".initialized"
  val PublicationInitializationMarkerName: String = ".active-publication.initialized"
  val ImagesDirectoryName: String = "images"
  val OwnerLockName: String = ".owner.lock"

  def images(directory: Path): Path = directory.resolve(ImagesDirectoryName)
  def activeManifest(directory: Path): Path = directory.resolve(ActiveManifestName)
  def activePublication(directory: Path): Path = directory.resolve(ActivePublicationName)
  def initializationMarker(directory: Path): Path = directory.resolve(InitializationMarkerName)
  def publicationInitializationMarker(directory: Path): Path = directory.resolve(PublicationInitializationMarkerName)
  def ownerLock(directory: Path): Path = directory.resolve(OwnerLockName)
  def image(directory: Path, imageId: MptImageId): Path = images(directory).resolve(s"${imageId.value.value}.mpti")
}

private[mpt] sealed trait MptImageArtifact
private[mpt] object MptImageArtifact {
  case object Image extends MptImageArtifact
  case object Manifest extends MptImageArtifact
  case object ManifestRecovery extends MptImageArtifact
  case object InitializationMarker extends MptImageArtifact
  case object Publication extends MptImageArtifact
  case object PublicationInitializationMarker extends MptImageArtifact
}

trait MptImageRootVerifier[F[_]] {

  /** Local persistence/root-algorithm identifier only. This does not prove that entry keys and values obey the advertised consensus schema.
    */
  def rootEra: MptImageRootEra
  def rebuild(entries: Vector[(Hex, ByteVector)]): F[MptRoot]
}

object MptImageRootVerifier {
  def consensus[F[_]: Async: Parallel: Hasher: JsonSerializer](era: MptImageRootEra): MptImageRootVerifier[F] =
    new MptImageRootVerifier[F] {
      val rootEra: MptImageRootEra = era

      def rebuild(entries: Vector[(Hex, ByteVector)]): F[MptRoot] = {
        val bytes = entries.iterator.map { case (key, value) => key -> value.toArray }.toMap
        MerklePatriciaTrie.makeParallelFromBytes[F](bytes).map(_.rootHash)
      }
    }
}

object VerifiedActiveMptPublication {
  private[mpt] def liveStore[F[_]: Async](
    directory: Path,
    fileOps: DurableFileOps[F],
    faults: DurableWriteHook[F, MptImageArtifact],
    codecEra: MptImageCodecEra,
    rootVerifier: MptImageRootVerifier[F],
    limits: MptImageReadLimits,
    publishMutex: Semaphore[F]
  ): DurableMptImageStore[F] = {
    val mint = (readEffect: F[MptActivePublication]) => new VerifiedActiveMptPublication[F](readEffect)

    new LiveDurableMptImageStore[F](
      directory,
      fileOps,
      faults,
      codecEra,
      rootVerifier,
      limits,
      publishMutex,
      mint
    )
  }
}

private final class LiveDurableMptImageStore[F[_]: Async](
  directory: Path,
  fileOps: DurableFileOps[F],
  faults: DurableWriteHook[F, MptImageArtifact],
  codecEra: MptImageCodecEra,
  rootVerifier: MptImageRootVerifier[F],
  limits: MptImageReadLimits,
  publishMutex: Semaphore[F],
  mintVerified: F[MptActivePublication] => VerifiedActiveMptPublication[F]
) extends DurableMptImageStore[F] {
  import DurableMptImageError._
  import MptImageArtifact._

  private val activeManifestPath = DurableMptImageLayout.activeManifest(directory)
  private val activePublicationPath = DurableMptImageLayout.activePublication(directory)
  private val initializationMarkerPath = DurableMptImageLayout.initializationMarker(directory)
  private val publicationInitializationMarkerPath = DurableMptImageLayout.publicationInitializationMarker(directory)
  private val atomicWriter = new DurableAtomicWriter[F](fileOps)

  def prepare(
    generation: Long,
    anchor: GlobalSnapshotStateRef,
    entries: Map[Hex, ByteVector]
  ): F[MptImageReceipt] =
    for {
      _ <- validateGeneration(generation)
      _ <- validateAnchor(anchor)
      _ <- validateEraHash("codec era", codecEra.value)
      _ <- validateEraHash("root era", rootVerifier.rootEra.value)
      captured <- capture(entries)
      actualRoot <- rootVerifier.rebuild(captured)
      _ <- ensureRoot(anchor.mptRoot, actualRoot)
      imageBytes <- Async[F].delay(
        MptImageEncoding.encodeImage(generation, anchor, codecEra, rootVerifier.rootEra, captured)
      )
      _ <- ensureLimit("encoded bytes", limits.maxImageBytes, imageBytes.length.toLong)
      digest <- Async[F].delay(MptImageDigest(Hash.fromBytes(imageBytes)))
      imageId <- Async[F].delay(MptImageEncoding.imageId(digest))
      receipt = MptImageReceipt(
        DurableMptImageStore.CurrentFormatVersion,
        generation,
        imageId,
        anchor,
        codecEra,
        rootVerifier.rootEra,
        digest,
        captured.size
      )
      target = DurableMptImageLayout.image(directory, imageId)
      _ <- durableAtomicWrite(target, imageBytes, Image, verifyImageAt(receipt, _), applyFaults = true)
    } yield receipt

  def publish(receipt: MptImageReceipt, expectedPrior: Option[MptImagePointer]): F[Unit] =
    publishMutex.permit.use { _ =>
      for {
        publication <- readOptionalPublication
        _ <- publication.traverse_(state => Async[F].raiseError[Unit](PublicationJournalOwnsActiveState(state)))
        _ <- validateReceiptShape(receipt)
        _ <- readImage(receipt)
        prior <- readOptionalManifest
        _ <- prior.traverse_(readImage)
        _ <- validatePublication(prior, receipt, expectedPrior)
        _ <-
          if (prior.contains(receipt)) ensureInitializationMarker(applyFaults = true)
          else publishManifest(receipt, prior)
      } yield ()
    }

  def initializeActivePublication(expectedLegacy: Option[MptImageReceipt]): F[MptActivePublication] =
    publishMutex.permit.use(_ => initializeActivePublicationUnlocked(expectedLegacy))

  def transitionActive(expectedBefore: MptActivePublication, target: MptActivePublication): F[Unit] =
    publishMutex.permit.use { _ =>
      for {
        _ <- validatePublicationShape(expectedBefore)
        _ <- validatePublicationShape(target)
        _ <- validatePublicationTransition(expectedBefore, target)
        actual <- readOptionalPublication.flatMap(_.liftTo[F](PublicationJournalNotInitialized))
        _ <- checkLegacyAgreement(actual)
        _ <-
          if (actual == target) cleanupLegacyArtifacts(target)
          else if (actual != expectedBefore) Async[F].raiseError(PublicationCompareAndSetConflict(expectedBefore, actual))
          else
            target.image.traverse_(readImage) >>
              cleanupLegacyArtifacts(actual) >>
              publishPublication(target, actual.some, ensureMarker = false)
      } yield ()
    }

  def activePublication: F[Option[MptActivePublication]] =
    publishMutex.permit.use { _ =>
      readOptionalPublication.flatTap(_.traverse_(checkLegacyAgreement))
    }

  def withVerifiedActivePublication[A](use: VerifiedActiveMptPublication[F] => F[A]): F[A] =
    publishMutex.permit.use { _ =>
      for {
        publication <- readOptionalPublication
          .flatMap(_.liftTo[F](PublicationJournalNotInitialized))
          .flatTap(checkLegacyAgreement)
        state <- Ref.of[F, Option[MptActivePublication]](publication.some)
        lease = mintVerified(state.get.flatMap(_.liftTo[F](LeaseExpired)))
        result <- Async[F].defer(use(lease)).guarantee(state.set(None))
      } yield result
    }

  def activeReceipt: F[Option[MptImageReceipt]] = publishMutex.permit.use(_ => selectedActiveReceiptUnlocked)

  def read(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]] =
    publishMutex.permit.use { _ =>
      for {
        active <- selectedActiveReceiptUnlocked.flatMap(
          _.liftTo[F](MissingArtifact(MptImageArtifactRef.ActiveManifest, activeManifestPath))
        )
        _ <- Async[F].raiseUnless(active == receipt)(ReceiptManifestMismatch(receipt, active))
        entries <- readImage(receipt)
      } yield entries
    }

  private def selectedActiveReceiptUnlocked: F[Option[MptImageReceipt]] =
    readOptionalPublication.flatMap {
      case Some(publication) => checkLegacyAgreement(publication).as(publication.image)
      case None              => activeLegacyReceiptUnlocked
    }

  private def activeLegacyReceiptUnlocked: F[Option[MptImageReceipt]] =
    readOptionalManifest.flatMap(
      _.traverse(receipt => readRequiredInitializationMarker >> readImage(receipt).as(receipt))
    )

  private def initializeActivePublicationUnlocked(expectedLegacy: Option[MptImageReceipt]): F[MptActivePublication] =
    for {
      _ <- expectedLegacy.traverse_(validateReceiptShape)
      rawPublication <- readOptionalPublicationRaw
      markerExists <- readOptionalPublicationInitializationMarker
      publication <- (rawPublication, markerExists) match {
        case (None, true) =>
          Async[F].raiseError[MptActivePublication](
            MissingArtifact(MptImageArtifactRef.ActivePublication, activePublicationPath)
          )
        case (None, false)           => initializeFromLegacy(expectedLegacy)
        case (Some(existing), true)  => resumeInitializedPublication(existing, expectedLegacy, ensureMarker = false)
        case (Some(existing), false) => resumeInitializedPublication(existing, expectedLegacy, ensureMarker = true)
      }
    } yield publication

  private def initializeFromLegacy(expectedLegacy: Option[MptImageReceipt]): F[MptActivePublication] =
    for {
      actualLegacy <- activeLegacyReceiptUnlocked
      _ <- Async[F].raiseUnless(actualLegacy == expectedLegacy)(
        PublicationInitializationConflict(expectedLegacy, actualLegacy)
      )
      target = MptActivePublication(MptPublicationRevision(0L), actualLegacy)
      _ <- publishPublication(target, None, ensureMarker = true)
      _ <- cleanupLegacyArtifacts(target)
    } yield target

  private def resumeInitializedPublication(
    existing: MptActivePublication,
    expectedLegacy: Option[MptImageReceipt],
    ensureMarker: Boolean
  ): F[MptActivePublication] =
    for {
      _ <- validatePublicationShape(existing)
      _ <- existing.image.traverse_(readImage)
      _ <-
        if (existing.revision.value != 0L) Async[F].raiseError[Unit](PublicationJournalAlreadyInitialized(existing))
        else
          Async[F].raiseUnless(existing.image == expectedLegacy)(
            PublicationInitializationConflict(expectedLegacy, existing.image)
          )
      _ <- checkLegacyAgreement(existing)
      _ <- Async[F].whenA(ensureMarker)(ensurePublicationInitializationMarker(applyFaults = true))
      _ <- cleanupLegacyArtifacts(existing)
    } yield existing

  private def publishManifest(receipt: MptImageReceipt, prior: Option[MptImageReceipt]): F[Unit] = {
    val bytes = MptImageEncoding.encodeManifest(receipt)

    Async[F].uncancelable { _ =>
      (durableAtomicWrite(activeManifestPath, bytes, Manifest, verifyManifestAt(receipt, _), applyFaults = true) >>
        ensureInitializationMarker(applyFaults = true)).handleErrorWith { publishFailure =>
        restorePriorManifest(prior).attempt.flatMap {
          case Right(_)              => Async[F].raiseError(publishFailure)
          case Left(recoveryFailure) => Async[F].raiseError(PublishRecoveryFailed(publishFailure, recoveryFailure))
        }
      }
    }
  }

  private def restorePriorManifest(prior: Option[MptImageReceipt]): F[Unit] =
    prior match {
      case Some(receipt) =>
        val bytes = MptImageEncoding.encodeManifest(receipt)
        durableAtomicWrite(
          activeManifestPath,
          bytes,
          ManifestRecovery,
          path => verifyManifestAt(receipt, path),
          applyFaults = false
        ) >> ensureInitializationMarker(applyFaults = false)
      case None =>
        fileOps.deleteIfExists(activeManifestPath) >>
          fileOps.deleteIfExists(initializationMarkerPath) >>
          fileOps.forceDirectory(directory)
    }

  private def publishPublication(
    target: MptActivePublication,
    prior: Option[MptActivePublication],
    ensureMarker: Boolean
  ): F[Unit] = {
    val bytes = MptImageEncoding.encodePublication(target)

    Async[F].uncancelable { _ =>
      (durableAtomicWrite(
        activePublicationPath,
        bytes,
        Publication,
        path => verifyPublicationAt(target, path),
        applyFaults = true
      ) >> Async[F].whenA(ensureMarker)(ensurePublicationInitializationMarker(applyFaults = true))).handleErrorWith { publishFailure =>
        restorePriorPublication(prior).attempt.flatMap {
          case Right(_)              => Async[F].raiseError(publishFailure)
          case Left(recoveryFailure) => Async[F].raiseError(PublicationRecoveryFailed(publishFailure, recoveryFailure))
        }
      }
    }
  }

  private def restorePriorPublication(prior: Option[MptActivePublication]): F[Unit] =
    prior match {
      case Some(publication) =>
        durableAtomicWrite(
          activePublicationPath,
          MptImageEncoding.encodePublication(publication),
          Publication,
          path => verifyPublicationAt(publication, path),
          applyFaults = false
        ) >> ensurePublicationInitializationMarker(applyFaults = false)
      case None =>
        fileOps.deleteIfExists(publicationInitializationMarkerPath) >>
          fileOps.forceDirectory(directory) >>
          fileOps.deleteIfExists(activePublicationPath) >>
          fileOps.forceDirectory(directory)
    }

  private def cleanupLegacyArtifacts(publication: MptActivePublication): F[Unit] =
    checkLegacyAgreement(publication) >>
      fileOps.deleteIfExists(activeManifestPath) >>
      fileOps.forceDirectory(directory) >>
      fileOps.deleteIfExists(initializationMarkerPath) >>
      fileOps.forceDirectory(directory)

  private def checkLegacyAgreement(publication: MptActivePublication): F[Unit] =
    readOptionalLegacyManifestRaw.flatMap {
      case None => Async[F].unit
      case Some(legacy) =>
        readRequiredInitializationMarker >>
          validateReceiptShape(legacy) >>
          readImage(legacy) >>
          Async[F].raiseUnless(publication.image.contains(legacy))(LegacyPublicationDisagreement(publication, legacy))
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

  private def validatePublicationShape(publication: MptActivePublication): F[Unit] =
    validatePublicationRevision(publication.revision) >> publication.image.traverse_(validateReceiptShape)

  private def validatePublicationRevision(revision: MptPublicationRevision): F[Unit] =
    Async[F].raiseWhen(revision.value < 0L)(InvalidPublicationRevision(revision))

  private def nextPublicationRevision(revision: MptPublicationRevision): F[MptPublicationRevision] =
    if (revision.value == Long.MaxValue) Async[F].raiseError(PublicationRevisionExhausted(revision))
    else Async[F].pure(MptPublicationRevision(revision.value + 1L))

  private def validatePublicationTransition(expectedBefore: MptActivePublication, target: MptActivePublication): F[Unit] =
    nextPublicationRevision(expectedBefore.revision).flatMap { expectedTargetRevision =>
      Async[F].raiseUnless(target.revision == expectedTargetRevision)(
        NonConsecutivePublicationRevision(expectedBefore.revision, target.revision)
      )
    }

  private def readImage(receipt: MptImageReceipt): F[SortedMap[Hex, ByteVector]] = {
    val path = DurableMptImageLayout.image(directory, receipt.imageId)
    verifyImageAt(receipt, path)
  }

  private def verifyImageAt(receipt: MptImageReceipt, path: Path): F[SortedMap[Hex, ByteVector]] =
    for {
      decodedFile <- readRequiredArtifact(MptImageArtifactRef.Image(receipt.imageId), path) {
        fileOps.openInput(path).use { stream =>
          Async[F].blocking(MptImageEncoding.decodeImage(stream, limits)).flatMap(Async[F].fromEither)
        }
      }
      digest = decodedFile.digest
      _ <- Async[F].raiseUnless(digest == receipt.digest)(
        CorruptImage(
          s"MPT image digest mismatch for ${receipt.imageId.value.value}: expected=${receipt.digest.value.value} actual=${digest.value.value}"
        )
      )
      expectedImageId <- Async[F].delay(MptImageEncoding.imageId(digest))
      _ <- Async[F].raiseUnless(expectedImageId == receipt.imageId)(
        CorruptImage(s"MPT image id mismatch: expected=${receipt.imageId.value.value} actual=${expectedImageId.value.value}")
      )
      decoded = decodedFile.image
      _ <- Async[F].raiseUnless(decoded.generation == receipt.generation)(
        CorruptImage(s"MPT image generation mismatch: expected=${receipt.generation} actual=${decoded.generation}")
      )
      _ <- Async[F].raiseUnless(decoded.anchor == receipt.anchor)(
        CorruptImage(s"MPT image anchor mismatch: expected=${receipt.anchor} actual=${decoded.anchor}")
      )
      _ <- Async[F].raiseUnless(decoded.codecEra == receipt.codecEra)(
        CorruptImage(s"MPT image codec-era mismatch: expected=${receipt.codecEra.value.value} actual=${decoded.codecEra.value.value}")
      )
      _ <- Async[F].raiseUnless(decoded.rootEra == receipt.rootEra)(
        CorruptImage(s"MPT image root-era mismatch: expected=${receipt.rootEra.value.value} actual=${decoded.rootEra.value.value}")
      )
      _ <- Async[F].raiseUnless(receipt.codecEra == codecEra)(
        CorruptImage(
          s"MPT image codec era is not accepted by this store: expected=${codecEra.value.value} actual=${receipt.codecEra.value.value}"
        )
      )
      _ <- Async[F].raiseUnless(receipt.rootEra == rootVerifier.rootEra)(
        CorruptImage(
          s"MPT image root era is not accepted by this verifier: expected=${rootVerifier.rootEra.value.value} actual=${receipt.rootEra.value.value}"
        )
      )
      _ <- Async[F].raiseUnless(decoded.entries.size == receipt.entryCount)(
        CorruptImage(s"MPT image entry-count mismatch: expected=${receipt.entryCount} actual=${decoded.entries.size}")
      )
      actualRoot <- rootVerifier.rebuild(decoded.entries.toVector)
      _ <- ensureRoot(receipt.anchor.mptRoot, actualRoot)
    } yield decoded.entries

  private def verifyManifestAt(receipt: MptImageReceipt, path: Path): F[Unit] =
    readRequiredManifest(path).flatMap { decoded =>
      Async[F].raiseUnless(decoded == receipt)(
        CorruptManifest(s"MPT image manifest read-back mismatch: expected=$receipt actual=$decoded")
      )
    }

  private def verifyPublicationAt(publication: MptActivePublication, path: Path): F[Unit] =
    readRequiredPublication(path).flatMap { decoded =>
      Async[F].raiseUnless(decoded == publication)(
        CorruptPublication(s"MPT active-publication read-back mismatch: expected=$publication actual=$decoded")
      )
    }

  private def readManifestRaw(path: Path): F[MptImageReceipt] =
    fileOps.openInput(path).use { stream =>
      Async[F].blocking(MptImageEncoding.decodeManifest(stream)).flatMap(Async[F].fromEither)
    }

  private def readPublicationRaw(path: Path): F[MptActivePublication] =
    fileOps.openInput(path).use { stream =>
      Async[F].blocking(MptImageEncoding.decodePublication(stream)).flatMap(Async[F].fromEither)
    }

  private def readInitializationMarkerRaw(path: Path): F[Unit] =
    fileOps.openInput(path).use { stream =>
      Async[F].blocking(MptImageEncoding.decodeInitializationMarker(stream)).flatMap(Async[F].fromEither)
    }

  private def readPublicationInitializationMarkerRaw(path: Path): F[Unit] =
    fileOps.openInput(path).use { stream =>
      Async[F].blocking(MptImageEncoding.decodePublicationInitializationMarker(stream)).flatMap(Async[F].fromEither)
    }

  private def readRequiredManifest(path: Path): F[MptImageReceipt] =
    readRequiredArtifact(MptImageArtifactRef.ActiveManifest, path)(readManifestRaw(path))

  private def readRequiredPublication(path: Path): F[MptActivePublication] =
    readRequiredArtifact(MptImageArtifactRef.ActivePublication, path)(readPublicationRaw(path))

  private def readRequiredInitializationMarker: F[Unit] =
    readRequiredArtifact(MptImageArtifactRef.InitializationMarker, initializationMarkerPath)(
      readInitializationMarkerRaw(initializationMarkerPath)
    )

  private def readOptionalInitializationMarker: F[Boolean] =
    readInitializationMarkerRaw(initializationMarkerPath).as(true).handleErrorWith {
      case _: NoSuchFileException        => Async[F].pure(false)
      case _: FileNotFoundException      => Async[F].pure(false)
      case failure: DurableMptImageError => Async[F].raiseError(failure)
      case failure =>
        Async[F].raiseError(
          ArtifactReadFailed(MptImageArtifactRef.InitializationMarker, initializationMarkerPath, failure)
        )
    }

  private def readOptionalPublicationInitializationMarker: F[Boolean] =
    readPublicationInitializationMarkerRaw(publicationInitializationMarkerPath).as(true).handleErrorWith {
      case _: NoSuchFileException        => Async[F].pure(false)
      case _: FileNotFoundException      => Async[F].pure(false)
      case failure: DurableMptImageError => Async[F].raiseError(failure)
      case failure =>
        Async[F].raiseError(
          ArtifactReadFailed(
            MptImageArtifactRef.PublicationInitializationMarker,
            publicationInitializationMarkerPath,
            failure
          )
        )
    }

  private def ensureInitializationMarker(applyFaults: Boolean): F[Unit] =
    readOptionalInitializationMarker.flatMap {
      case true => Async[F].unit
      case false =>
        durableAtomicWrite(
          initializationMarkerPath,
          MptImageEncoding.encodeInitializationMarker,
          InitializationMarker,
          path => readRequiredArtifact(MptImageArtifactRef.InitializationMarker, path)(readInitializationMarkerRaw(path)),
          applyFaults
        )
    }

  private def ensurePublicationInitializationMarker(applyFaults: Boolean): F[Unit] =
    readOptionalPublicationInitializationMarker.flatMap {
      case true => Async[F].unit
      case false =>
        durableAtomicWrite(
          publicationInitializationMarkerPath,
          MptImageEncoding.encodePublicationInitializationMarker,
          PublicationInitializationMarker,
          path =>
            readRequiredArtifact(MptImageArtifactRef.PublicationInitializationMarker, path)(
              readPublicationInitializationMarkerRaw(path)
            ),
          applyFaults
        )
    }

  private def readOptionalPublication: F[Option[MptActivePublication]] =
    for {
      publication <- readOptionalPublicationRaw
      markerExists <- readOptionalPublicationInitializationMarker
      verified <- (publication, markerExists) match {
        case (None, false) => Async[F].pure(Option.empty[MptActivePublication])
        case (None, true) =>
          Async[F].raiseError[Option[MptActivePublication]](
            MissingArtifact(MptImageArtifactRef.ActivePublication, activePublicationPath)
          )
        case (Some(_), false) =>
          Async[F].raiseError[Option[MptActivePublication]](
            MissingArtifact(
              MptImageArtifactRef.PublicationInitializationMarker,
              publicationInitializationMarkerPath
            )
          )
        case (Some(value), true) =>
          validatePublicationShape(value) >> value.image.traverse_(readImage) >> Async[F].pure(value.some)
      }
    } yield verified

  private def readOptionalPublicationRaw: F[Option[MptActivePublication]] =
    readPublicationRaw(activePublicationPath).map(_.some).handleErrorWith {
      case _: NoSuchFileException        => Async[F].pure(None)
      case _: FileNotFoundException      => Async[F].pure(None)
      case failure: DurableMptImageError => Async[F].raiseError(failure)
      case failure =>
        Async[F].raiseError(ArtifactReadFailed(MptImageArtifactRef.ActivePublication, activePublicationPath, failure))
    }

  private def readOptionalLegacyManifestRaw: F[Option[MptImageReceipt]] =
    readManifestRaw(activeManifestPath).map(_.some).handleErrorWith {
      case _: NoSuchFileException        => Async[F].pure(None)
      case _: FileNotFoundException      => Async[F].pure(None)
      case failure: DurableMptImageError => Async[F].raiseError(failure)
      case failure =>
        Async[F].raiseError(ArtifactReadFailed(MptImageArtifactRef.ActiveManifest, activeManifestPath, failure))
    }

  private def readOptionalManifest: F[Option[MptImageReceipt]] =
    readManifestRaw(activeManifestPath).flatTap(validateReceiptShape).map(_.some).handleErrorWith {
      case failure: NoSuchFileException   => missingManifestOrPristine(failure)
      case failure: FileNotFoundException => missingManifestOrPristine(failure)
      case failure: DurableMptImageError  => Async[F].raiseError(failure)
      case failure =>
        Async[F].raiseError(ArtifactReadFailed(MptImageArtifactRef.ActiveManifest, activeManifestPath, failure))
    }

  private def missingManifestOrPristine(missing: Throwable): F[Option[MptImageReceipt]] =
    readOptionalInitializationMarker.flatMap {
      case false => Async[F].pure(None)
      case true =>
        Async[F].raiseError(
          MissingArtifact(MptImageArtifactRef.ActiveManifest, activeManifestPath, missing)
        )
    }

  private def readRequiredArtifact[A](artifact: MptImageArtifactRef, path: Path)(read: F[A]): F[A] =
    read.handleErrorWith {
      case failure: DurableMptImageError  => Async[F].raiseError(failure)
      case failure: NoSuchFileException   => Async[F].raiseError(MissingArtifact(artifact, path, failure))
      case failure: FileNotFoundException => Async[F].raiseError(MissingArtifact(artifact, path, failure))
      case failure                        => Async[F].raiseError(ArtifactReadFailed(artifact, path, failure))
    }

  private def durableAtomicWrite[A](
    target: Path,
    bytes: Array[Byte],
    artifact: MptImageArtifact,
    verify: Path => F[A],
    applyFaults: Boolean
  ): F[A] = {
    val hook = if (applyFaults) faults else DurableWriteHook.noop[F, MptImageArtifact]
    DurableMptImageStore.adaptDurableEffect(atomicWriter.replace(target, bytes, artifact, hook)(verify))
  }

  private def capture(entries: Map[Hex, ByteVector]): F[Vector[(Hex, ByteVector)]] =
    for {
      _ <- ensureLimit("entry count", limits.maxEntries.toLong, entries.size.toLong)
      _ <- entries.toVector.traverse_ {
        case (key, value) =>
          validateKey(key) >>
            ensureLimit("key bytes", limits.maxKeyBytes.toLong, key.value.length.toLong) >>
            ensureLimit("value bytes", limits.maxValueBytes.toLong, value.size)
      }
      encodedSize <- Async[F].delay {
        try
          entries.foldLeft(MptImageEncoding.ImageHeaderBytes) {
            case (total, (key, value)) =>
              val withFraming = Math.addExact(total, 8L)
              val withKey = Math.addExact(withFraming, key.value.length.toLong)
              Math.addExact(withKey, value.size)
          }
        catch { case _: ArithmeticException => Long.MaxValue }
      }
      _ <- ensureLimit("encoded bytes", limits.maxImageBytes, encodedSize)
      captured <- Async[F].delay {
        entries.iterator.map { case (key, value) => key -> ByteVector.view(value.toArray.clone()) }.toVector
          .sortBy(_._1.value)
      }
    } yield captured

  private def validateKey(key: Hex): F[Unit] = {
    val value = key.value
    val canonical = value.nonEmpty && (value.length & 1) == 0 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
    Async[F].raiseUnless(canonical)(InvalidEntry(s"MPT image key must be non-empty, even-length lowercase hex: '$value'"))
  }

  private def validateGeneration(generation: Long): F[Unit] =
    Async[F].raiseWhen(generation < 0L)(InvalidGeneration(generation))

  private def ensureLimit(field: String, maximum: Long, actual: Long): F[Unit] =
    Async[F].raiseWhen(actual > maximum)(ImageLimitExceeded(field, maximum, actual))

  private def validateReceiptShape(receipt: MptImageReceipt): F[Unit] =
    for {
      _ <- validateGeneration(receipt.generation)
      _ <- Async[F].raiseUnless(receipt.formatVersion == DurableMptImageStore.CurrentFormatVersion)(
        CorruptManifest(s"Unsupported MPT image receipt version ${receipt.formatVersion}")
      )
      _ <- Async[F].raiseWhen(receipt.entryCount < 0)(CorruptManifest(s"Negative MPT image entry count ${receipt.entryCount}"))
      _ <- ensureLimit("entry count", limits.maxEntries.toLong, receipt.entryCount.toLong)
      _ <- validateReceiptHash("image id", receipt.imageId.value)
      _ <- validateReceiptHash("image digest", receipt.digest.value)
      _ <- validateReceiptHash("snapshot hash", receipt.anchor.hash)
      _ <- Async[F].raiseWhen(receipt.anchor.hash == Hash.empty)(
        CorruptManifest("Hash.empty is reserved and cannot identify an MPT image snapshot")
      )
      _ <- validateReceiptHash("parent hash", receipt.anchor.parentHash)
      _ <- validateReceiptHash("MPT root", receipt.anchor.mptRoot.value)
      _ <- validateReceiptHash("codec era", receipt.codecEra.value)
      _ <- validateReceiptHash("root era", receipt.rootEra.value)
    } yield ()

  private def validateAnchor(anchor: GlobalSnapshotStateRef): F[Unit] =
    for {
      _ <- validateAnchorHash("snapshot hash", anchor.hash)
      _ <- Async[F].raiseWhen(anchor.hash == Hash.empty)(InvalidAnchor("Hash.empty is reserved and cannot identify an MPT image snapshot"))
      _ <- validateAnchorHash("parent hash", anchor.parentHash)
      _ <- validateAnchorHash("MPT root", anchor.mptRoot.value)
    } yield ()

  private def validateAnchorHash(label: String, hash: Hash): F[Unit] =
    Async[F].raiseUnless(isCanonicalHash(hash))(InvalidAnchor(s"MPT image anchor has a non-canonical $label: '${hash.value}'"))

  private def validateEraHash(label: String, hash: Hash): F[Unit] =
    Async[F].raiseUnless(isCanonicalHash(hash))(InvalidAnchor(s"MPT image has a non-canonical $label: '${hash.value}'"))

  private def validateReceiptHash(label: String, hash: Hash): F[Unit] =
    Async[F].raiseUnless(isCanonicalHash(hash))(CorruptManifest(s"MPT image receipt has a non-canonical $label: '${hash.value}'"))

  private def isCanonicalHash(hash: Hash): Boolean = {
    val value = hash.value
    value.length == 64 && value.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
  }

  private def ensureRoot(expected: MptRoot, actual: MptRoot): F[Unit] =
    Async[F].raiseUnless(expected == actual)(RootMismatch(expected, actual))
}

private[mpt] object MptImageEncoding {
  import DurableMptImageError._

  private val ImageMagic = "TNMPTIM2".getBytes(StandardCharsets.US_ASCII)
  private val ManifestMagic = "TNMPTMN2".getBytes(StandardCharsets.US_ASCII)
  private val InitializationMagic = "TNMPTIN2".getBytes(StandardCharsets.US_ASCII)
  private val PublicationMagic = "TNMPTAP1".getBytes(StandardCharsets.US_ASCII)
  private val PublicationInitializationMagic = "TNMPTPI1".getBytes(StandardCharsets.US_ASCII)
  private val PublicationFormatVersion = 1
  private val HashBytes = 32
  val ImageHeaderBytes: Long = ImageMagic.length.toLong + 4L + 8L + (5L * HashBytes) + 8L + 4L
  private val ManifestPayloadBytes = ManifestMagic.length + 4 + 8 + (7 * HashBytes) + 8 + 4
  private val ManifestBytes = ManifestPayloadBytes + HashBytes
  private val InitializationPayloadBytes = InitializationMagic.length + 4
  private val InitializationBytes = InitializationPayloadBytes + HashBytes
  private val PublicationBasePayloadBytes = PublicationMagic.length + 4 + 8 + 1
  private val PublicationMaximumBytes = PublicationBasePayloadBytes + 4 + ManifestBytes + HashBytes
  private val PublicationInitializationPayloadBytes = PublicationInitializationMagic.length + 4
  private val PublicationInitializationBytes = PublicationInitializationPayloadBytes + HashBytes
  private val ImageIdDomain = "tessellation-nakamoto/mpt-image-id/v2\u0000".getBytes(StandardCharsets.UTF_8)

  final case class DecodedImage(
    generation: Long,
    anchor: GlobalSnapshotStateRef,
    codecEra: MptImageCodecEra,
    rootEra: MptImageRootEra,
    entries: SortedMap[Hex, ByteVector]
  )

  final case class DecodedImageFile(image: DecodedImage, digest: MptImageDigest)

  def imageId(digest: MptImageDigest): MptImageId = {
    val digestBytes = hashBytes(digest.value)
    MptImageId(Hash.fromBytes(ImageIdDomain ++ digestBytes))
  }

  def encodeImage(
    generation: Long,
    anchor: GlobalSnapshotStateRef,
    codecEra: MptImageCodecEra,
    rootEra: MptImageRootEra,
    entries: Vector[(Hex, ByteVector)]
  ): Array[Byte] = {
    val output = new ByteArrayOutputStream()
    val data = new DataOutputStream(output)
    try {
      data.write(ImageMagic)
      data.writeInt(DurableMptImageStore.CurrentFormatVersion)
      data.writeLong(generation)
      data.write(hashBytes(anchor.hash))
      data.write(hashBytes(anchor.parentHash))
      data.writeLong(anchor.ordinal.value.value)
      data.write(hashBytes(anchor.mptRoot.value))
      data.write(hashBytes(codecEra.value))
      data.write(hashBytes(rootEra.value))
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

  def decodeImage(source: InputStream, limits: MptImageReadLimits): Either[DurableMptImageError, DecodedImageFile] =
    try {
      val bounded = new CountingBoundedInputStream(source, limits.maxImageBytes, "encoded bytes")
      val digest = MessageDigest.getInstance("SHA-256")
      val input = new DataInputStream(new DigestInputStream(bounded, digest))

      requireMagic(input, ImageMagic, "image")
      requireVersion(input, "image")
      val generation = input.readLong()
      if (generation < 0L) throw new IOException(s"Negative generation $generation")
      val snapshotHash = readHash(input)
      val parentHash = readHash(input)
      val ordinalValue = input.readLong()
      if (ordinalValue < 0L) throw new IOException(s"Negative ordinal $ordinalValue")
      val root = MptRoot(readHash(input))
      val codecEra = MptImageCodecEra(readHash(input))
      val rootEra = MptImageRootEra(readHash(input))
      val count = input.readInt()
      if (count < 0) throw new IOException(s"Negative entry count $count")
      if (count > limits.maxEntries) throw ImageLimitExceeded("entry count", limits.maxEntries.toLong, count.toLong)

      val ordering: Ordering[Hex] = Ordering.by(_.value)
      var entries = SortedMap.empty[Hex, ByteVector](ordering)
      var previous: Option[String] = None
      var index = 0
      while (index < count) {
        val keyLength = checkedLength(input, bounded, s"key[$index]", limits.maxKeyBytes)
        val keyBytes = readExact(input, keyLength)
        val key = new String(keyBytes, StandardCharsets.US_ASCII)
        val canonical = key.nonEmpty && (key.length & 1) == 0 && key.forall(c => c >= '0' && c <= '9' || c >= 'a' && c <= 'f')
        if (!canonical) throw new IOException(s"Non-canonical hex key at index $index")
        if (previous.exists(_ >= key)) throw new IOException(s"Keys are not strictly sorted at index $index")
        val valueLength = checkedLength(input, bounded, s"value[$index]", limits.maxValueBytes)
        val value = readExact(input, valueLength)
        entries = entries.updated(Hex(key), ByteVector.view(value))
        previous = Some(key)
        index += 1
      }
      if (input.read() != -1) throw new IOException("Trailing image bytes")
      val anchor = GlobalSnapshotStateRef(SnapshotOrdinal.unsafeApply(ordinalValue), snapshotHash, parentHash, root)
      val image = DecodedImage(generation, anchor, codecEra, rootEra, entries)
      val imageDigest = MptImageDigest(Hash(Hex.fromBytes(digest.digest()).value))
      Right(DecodedImageFile(image, imageDigest))
    } catch {
      case failure: ImageLimitExceeded => Left(failure)
      case failure: EOFException       => Left(CorruptImage("Truncated MPT image", failure))
      case failure: IOException        => Left(CorruptImage(s"Invalid MPT image: ${failure.getMessage}", failure))
      case NonFatal(failure)           => Left(CorruptImage(s"Unable to decode MPT image: ${failure.getMessage}", failure))
    }

  def encodeManifest(receipt: MptImageReceipt): Array[Byte] = {
    val output = new ByteArrayOutputStream(ManifestBytes)
    val data = new DataOutputStream(output)
    try {
      data.write(ManifestMagic)
      data.writeInt(DurableMptImageStore.CurrentFormatVersion)
      data.writeLong(receipt.generation)
      data.write(hashBytes(receipt.imageId.value))
      data.write(hashBytes(receipt.anchor.hash))
      data.write(hashBytes(receipt.anchor.parentHash))
      data.writeLong(receipt.anchor.ordinal.value.value)
      data.write(hashBytes(receipt.anchor.mptRoot.value))
      data.write(hashBytes(receipt.codecEra.value))
      data.write(hashBytes(receipt.rootEra.value))
      data.write(hashBytes(receipt.digest.value))
      data.writeInt(receipt.entryCount)
      data.flush()
      val payload = output.toByteArray
      output.write(Hash.sha256DigestFromBytes(payload).toByteArray)
      output.toByteArray
    } finally data.close()
  }

  def encodeInitializationMarker: Array[Byte] = {
    val payload = ByteBuffer
      .allocate(InitializationPayloadBytes)
      .put(InitializationMagic)
      .putInt(DurableMptImageStore.CurrentFormatVersion)
      .array()
    payload ++ Hash.sha256DigestFromBytes(payload).toByteArray
  }

  def encodePublication(publication: MptActivePublication): Array[Byte] = {
    val output = new ByteArrayOutputStream(PublicationMaximumBytes)
    val data = new DataOutputStream(output)
    try {
      data.write(PublicationMagic)
      data.writeInt(PublicationFormatVersion)
      data.writeLong(publication.revision.value)
      publication.image match {
        case None => data.writeByte(0)
        case Some(receipt) =>
          val manifest = encodeManifest(receipt)
          data.writeByte(1)
          data.writeInt(manifest.length)
          data.write(manifest)
      }
      data.flush()
      val payload = output.toByteArray
      output.write(Hash.sha256DigestFromBytes(payload).toByteArray)
      output.toByteArray
    } finally data.close()
  }

  def encodePublicationInitializationMarker: Array[Byte] = {
    val payload = ByteBuffer
      .allocate(PublicationInitializationPayloadBytes)
      .put(PublicationInitializationMagic)
      .putInt(PublicationFormatVersion)
      .array()
    payload ++ Hash.sha256DigestFromBytes(payload).toByteArray
  }

  def decodeInitializationMarker(source: InputStream): Either[DurableMptImageError, Unit] =
    try {
      val input = new DataInputStream(source)
      val bytes = readExact(input, InitializationBytes)
      if (input.read() != -1) throw new IOException("Trailing initialization-marker bytes")
      val payload = Arrays.copyOf(bytes, InitializationPayloadBytes)
      val expectedChecksum = Arrays.copyOfRange(bytes, InitializationPayloadBytes, InitializationBytes)
      val actualChecksum = Hash.sha256DigestFromBytes(payload).toByteArray
      if (!Arrays.equals(expectedChecksum, actualChecksum)) throw new IOException("Initialization-marker checksum mismatch")

      val decoded = new DataInputStream(new ByteArrayInputStream(payload))
      requireMagic(decoded, InitializationMagic, "initialization marker")
      requireVersion(decoded, "initialization marker")
      if (decoded.available() != 0) throw new IOException(s"Trailing ${decoded.available()} initialization-marker bytes")
      Right(())
    } catch {
      case failure: EOFException =>
        Left(CorruptInitializationMarker("Truncated MPT image initialization marker", failure))
      case failure: IOException =>
        Left(CorruptInitializationMarker(s"Invalid MPT image initialization marker: ${failure.getMessage}", failure))
      case NonFatal(failure) =>
        Left(CorruptInitializationMarker(s"Unable to decode MPT image initialization marker: ${failure.getMessage}", failure))
    }

  def decodePublication(source: InputStream): Either[DurableMptImageError, MptActivePublication] =
    try {
      val bounded = new CountingBoundedInputStream(source, PublicationMaximumBytes.toLong, "publication bytes")
      val digest = MessageDigest.getInstance("SHA-256")
      val digesting = new DigestInputStream(bounded, digest)
      val input = new DataInputStream(digesting)

      requireMagic(input, PublicationMagic, "active publication")
      val version = input.readInt()
      if (version != PublicationFormatVersion) throw new IOException(s"Unsupported active-publication version $version")
      val revision = input.readLong()
      if (revision < 0L) throw new IOException(s"Negative active-publication revision $revision")
      val image = input.readUnsignedByte() match {
        case 0 => None
        case 1 =>
          val manifestLength = input.readInt()
          if (manifestLength != ManifestBytes)
            throw new IOException(s"Invalid embedded manifest length $manifestLength, expected $ManifestBytes")
          val manifestBytes = readExact(input, manifestLength)
          decodeManifest(new ByteArrayInputStream(manifestBytes)) match {
            case Right(receipt) => receipt.some
            case Left(failure)  => throw new IOException("Invalid embedded MPT image manifest", failure)
          }
        case tag => throw new IOException(s"Invalid active-publication image option tag $tag")
      }

      digesting.on(false)
      val expectedChecksum = readExact(input, HashBytes)
      val actualChecksum = digest.digest()
      if (!Arrays.equals(expectedChecksum, actualChecksum)) throw new IOException("Active-publication checksum mismatch")
      if (input.read() != -1) throw new IOException("Trailing active-publication bytes")
      Right(MptActivePublication(MptPublicationRevision(revision), image))
    } catch {
      case failure: ImageLimitExceeded => Left(failure)
      case failure: EOFException       => Left(CorruptPublication("Truncated MPT active publication", failure))
      case failure: IOException        => Left(CorruptPublication(s"Invalid MPT active publication: ${failure.getMessage}", failure))
      case NonFatal(failure) => Left(CorruptPublication(s"Unable to decode MPT active publication: ${failure.getMessage}", failure))
    }

  def decodePublicationInitializationMarker(source: InputStream): Either[DurableMptImageError, Unit] =
    try {
      val input = new DataInputStream(source)
      val bytes = readExact(input, PublicationInitializationBytes)
      if (input.read() != -1) throw new IOException("Trailing publication-initialization-marker bytes")
      val payload = Arrays.copyOf(bytes, PublicationInitializationPayloadBytes)
      val expectedChecksum = Arrays.copyOfRange(bytes, PublicationInitializationPayloadBytes, PublicationInitializationBytes)
      val actualChecksum = Hash.sha256DigestFromBytes(payload).toByteArray
      if (!Arrays.equals(expectedChecksum, actualChecksum))
        throw new IOException("Publication-initialization-marker checksum mismatch")

      val decoded = new DataInputStream(new ByteArrayInputStream(payload))
      requireMagic(decoded, PublicationInitializationMagic, "publication initialization marker")
      val version = decoded.readInt()
      if (version != PublicationFormatVersion)
        throw new IOException(s"Unsupported publication-initialization-marker version $version")
      if (decoded.available() != 0)
        throw new IOException(s"Trailing ${decoded.available()} publication-initialization-marker bytes")
      Right(())
    } catch {
      case failure: EOFException =>
        Left(CorruptPublicationInitializationMarker("Truncated MPT publication initialization marker", failure))
      case failure: IOException =>
        Left(
          CorruptPublicationInitializationMarker(
            s"Invalid MPT publication initialization marker: ${failure.getMessage}",
            failure
          )
        )
      case NonFatal(failure) =>
        Left(
          CorruptPublicationInitializationMarker(
            s"Unable to decode MPT publication initialization marker: ${failure.getMessage}",
            failure
          )
        )
    }

  def decodeManifest(source: InputStream): Either[DurableMptImageError, MptImageReceipt] =
    try {
      val bounded = new CountingBoundedInputStream(source, ManifestBytes.toLong, "manifest bytes")
      val stream = new DataInputStream(bounded)
      val bytes = readExact(stream, ManifestBytes)
      if (stream.read() != -1) throw new IOException("Trailing manifest bytes")
      if (bytes.length != ManifestBytes) throw new IOException(s"Expected $ManifestBytes bytes, got ${bytes.length}")
      val payload = Arrays.copyOf(bytes, ManifestPayloadBytes)
      val expectedChecksum = Arrays.copyOfRange(bytes, ManifestPayloadBytes, ManifestBytes)
      val actualChecksum = Hash.sha256DigestFromBytes(payload).toByteArray
      if (!Arrays.equals(expectedChecksum, actualChecksum)) throw new IOException("Manifest checksum mismatch")

      val input = new DataInputStream(new ByteArrayInputStream(payload))
      requireMagic(input, ManifestMagic, "manifest")
      val version = requireVersion(input, "manifest")
      val generation = input.readLong()
      if (generation < 0L) throw new IOException(s"Negative generation $generation")
      val imageId = MptImageId(readHash(input))
      val snapshotHash = readHash(input)
      val parentHash = readHash(input)
      val ordinalValue = input.readLong()
      if (ordinalValue < 0L) throw new IOException(s"Negative ordinal $ordinalValue")
      val root = MptRoot(readHash(input))
      val codecEra = MptImageCodecEra(readHash(input))
      val rootEra = MptImageRootEra(readHash(input))
      val digest = MptImageDigest(readHash(input))
      val count = input.readInt()
      if (count < 0) throw new IOException(s"Negative entry count $count")
      if (input.available() != 0) throw new IOException(s"Trailing ${input.available()} manifest bytes")
      val anchor = GlobalSnapshotStateRef(SnapshotOrdinal.unsafeApply(ordinalValue), snapshotHash, parentHash, root)
      Right(MptImageReceipt(version, generation, imageId, anchor, codecEra, rootEra, digest, count))
    } catch {
      case failure: ImageLimitExceeded => Left(failure)
      case failure: EOFException       => Left(CorruptManifest("Truncated MPT image manifest", failure))
      case failure: IOException        => Left(CorruptManifest(s"Invalid MPT image manifest: ${failure.getMessage}", failure))
      case NonFatal(failure)           => Left(CorruptManifest(s"Unable to decode MPT image manifest: ${failure.getMessage}", failure))
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

  private def checkedLength(
    input: DataInputStream,
    bounded: CountingBoundedInputStream,
    field: String,
    maximum: Int
  ): Int = {
    val length = input.readInt()
    if (length < 0) throw new IOException(s"Negative $field length $length")
    if (length > maximum) throw ImageLimitExceeded(s"$field bytes", maximum.toLong, length.toLong)
    bounded.requireRemaining(field, length)
    length
  }

  private final class CountingBoundedInputStream(delegate: InputStream, maximum: Long, field: String) extends FilterInputStream(delegate) {
    private var consumed = 0L

    def remainingCapacity: Long = maximum - consumed

    def requireRemaining(declaredField: String, length: Int): Unit =
      if (length.toLong > remainingCapacity)
        throw ImageLimitExceeded(s"$declaredField bytes within remaining $field", remainingCapacity, length.toLong)

    private def advance(amount: Long): Unit =
      if (amount > 0L) {
        consumed = Math.addExact(consumed, amount)
        if (consumed > maximum) throw ImageLimitExceeded(field, maximum, consumed)
      }

    override def read(): Int = {
      val value = super.read()
      if (value != -1) advance(1L)
      value
    }

    override def read(bytes: Array[Byte], offset: Int, length: Int): Int =
      if (length == 0) 0
      else {
        val remainingThroughLimit = maximum - consumed + 1L
        val boundedLength = math.min(length.toLong, math.max(1L, remainingThroughLimit)).toInt
        val read = super.read(bytes, offset, boundedLength)
        if (read > 0) advance(read.toLong)
        read
      }

    override def skip(requested: Long): Long = {
      val remainingThroughLimit = maximum - consumed + 1L
      val skipped = super.skip(math.min(requested, math.max(1L, remainingThroughLimit)))
      advance(skipped)
      skipped
    }
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
