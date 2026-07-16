package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.Arrays

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs.{
  finalityDomainCodec,
  pathChunkPayloadCodec,
  pathManifestPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorCodecs.{
  coordinatorAuditRecordPayloadCodec,
  coordinatorHeadPayloadCodec,
  recoveryRecordPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.{
  finalityCoreBatchPayloadCodec,
  releasedCoreRecordPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableEnvelopeKind.{
  AuditRecord => AuditRecordEnvelope,
  CoordinatorHead => CoordinatorHeadEnvelope,
  EffectManifest => EffectManifestEnvelope,
  EffectOutboxHead => EffectOutboxHeadEnvelope,
  EffectReceipt => EffectReceiptEnvelope,
  ImmutableArtifact => ImmutableArtifactEnvelope,
  InitializationMarker => InitializationMarkerEnvelope,
  RecoveryRecord => RecoveryRecordEnvelope
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityEffectCodecs.{
  finalityEffectManifestPayloadCodec,
  finalityEffectOutboxHeadPayloadCodec,
  terminalEffectReceiptPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator.{
  CoreValidationContext,
  PreviousEffectManifest,
  ResolvedPathManifest
}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptActivePublication
import io.constellationnetwork.storage.durable._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.Codec
import scodec.bits.ByteVector

final case class FinalityDurableStoreLimits(
  maxCoordinatorHeadBytes: Long,
  maxEffectOutboxHeadBytes: Long,
  maxImmutableArtifactBytes: Long,
  maxEffectManifestBytes: Long,
  maxAuditRecordBytes: Long,
  maxRecoveryRecordBytes: Long,
  maxEffectReceiptBytes: Long,
  maxPathTraversalBytes: Long,
  maxPathTraversalChunks: Long,
  maxOutboxTraversalManifests: Long,
  maxAuditTraversalRecords: Long
)

object FinalityDurableStoreLimits {
  val default: FinalityDurableStoreLimits =
    FinalityDurableStoreLimits(
      maxCoordinatorHeadBytes = 8L * 1024L * 1024L,
      maxEffectOutboxHeadBytes = 1024L * 1024L,
      maxImmutableArtifactBytes = 64L * 1024L * 1024L,
      maxEffectManifestBytes = 16L * 1024L * 1024L,
      maxAuditRecordBytes = 16L * 1024L * 1024L,
      maxRecoveryRecordBytes = 16L * 1024L * 1024L,
      maxEffectReceiptBytes = 1024L * 1024L,
      maxPathTraversalBytes = 512L * 1024L * 1024L,
      maxPathTraversalChunks = 1000000L,
      maxOutboxTraversalManifests = 100000L,
      maxAuditTraversalRecords = 1000000L
    )

  def validate(limits: FinalityDurableStoreLimits): Either[FinalityDurableStoreError.InvalidLimits, Unit] = {
    val envelopeLimits = List(
      "maxCoordinatorHeadBytes" -> limits.maxCoordinatorHeadBytes,
      "maxEffectOutboxHeadBytes" -> limits.maxEffectOutboxHeadBytes,
      "maxImmutableArtifactBytes" -> limits.maxImmutableArtifactBytes,
      "maxEffectManifestBytes" -> limits.maxEffectManifestBytes,
      "maxAuditRecordBytes" -> limits.maxAuditRecordBytes,
      "maxRecoveryRecordBytes" -> limits.maxRecoveryRecordBytes,
      "maxEffectReceiptBytes" -> limits.maxEffectReceiptBytes
    )
    val errors = envelopeLimits.collect {
      case (name, value) if value <= 0L => s"$name must be positive, got $value"
      case (name, value) if value > FinalityDurableEnvelope.MaxPayloadBytes.toLong =>
        s"$name must be at most ${FinalityDurableEnvelope.MaxPayloadBytes}, got $value"
    } ++ List(
      Option.when(limits.maxPathTraversalBytes <= 0L)(
        s"maxPathTraversalBytes must be positive, got ${limits.maxPathTraversalBytes}"
      ),
      Option.when(limits.maxPathTraversalChunks <= 0L)(
        s"maxPathTraversalChunks must be positive, got ${limits.maxPathTraversalChunks}"
      ),
      Option.when(limits.maxOutboxTraversalManifests <= 0L)(
        s"maxOutboxTraversalManifests must be positive, got ${limits.maxOutboxTraversalManifests}"
      ),
      Option.when(limits.maxAuditTraversalRecords <= 0L)(
        s"maxAuditTraversalRecords must be positive, got ${limits.maxAuditTraversalRecords}"
      )
    ).flatten

    Either.cond(errors.isEmpty, (), FinalityDurableStoreError.InvalidLimits(errors.mkString("; ")))
  }
}

sealed abstract class FinalityDurableStoreError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object FinalityDurableStoreError {
  final case class InvalidLimits(detail: String) extends FinalityDurableStoreError(s"Invalid finality durable-store limits: $detail")

  final case class InvalidIdentifier(label: String, value: String)
      extends FinalityDurableStoreError(s"$label is not canonical fixed-width lowercase hexadecimal: $value")

  final case class InvalidDomain(detail: String) extends FinalityDurableStoreError(s"Invalid configured finality domain: $detail")

  final case class UnsafePath(path: Path)
      extends FinalityDurableStoreError(s"Finality store path escapes or aliases the configured root: $path")

  final case class UnsafeFile(path: Path, fileType: String)
      extends FinalityDurableStoreError(s"Finality store target must be a regular non-symlink file: path=$path type=$fileType")

  final case class UnsafeDirectory(path: Path)
      extends FinalityDurableStoreError(s"Finality store directory must be a real non-symlink directory: $path")

  final case class Missing(kind: FinalityDurableEnvelopeKind, path: Path)
      extends FinalityDurableStoreError(s"Missing durable ${kind.label}: $path")

  final case class Corrupt(kind: FinalityDurableEnvelopeKind, path: Path, detail: String, cause0: Throwable = null)
      extends FinalityDurableStoreError(s"Corrupt durable ${kind.label} at $path: $detail", cause0)

  final case class IdentityMismatch(label: String, detail: String)
      extends FinalityDurableStoreError(s"Finality $label identity mismatch: $detail")

  final case class ImmutableLocatorConflict(path: Path)
      extends FinalityDurableStoreError(s"Immutable finality locator already contains different canonical bytes: $path")

  final case class AtomicImmutableInstallRequired(path: Path, cause0: Throwable)
      extends FinalityDurableStoreError(s"Atomic create-new install is required for immutable finality artifact: $path", cause0)

  final case class StructuralValidationFailed(violations: List[FinalityIntentValidator.Violation])
      extends FinalityDurableStoreError(
        s"Finality durable transition failed structural validation: ${violations.map(v => s"${v.path}: ${v.invariant}").mkString("; ")}"
      )

  final case class CoordinatorNotInitialized() extends FinalityDurableStoreError("Finality coordinator head is not initialized")

  final case class EffectOutboxNotInitialized() extends FinalityDurableStoreError("Finality effect outbox head is not initialized")

  final case class CoordinatorCompareAndSetConflict(
    expected: Option[Hash],
    actual: Option[Hash]
  ) extends FinalityDurableStoreError(s"Finality coordinator compare-and-set conflict: expected=$expected actual=$actual")

  final case class EffectOutboxCompareAndSetConflict(expected: Option[Hash], actual: Option[Hash])
      extends FinalityDurableStoreError(s"Finality effect-outbox compare-and-set conflict: expected=$expected actual=$actual")

  final case class RecoveryModeIsAbsorbing()
      extends FinalityDurableStoreError(
        "RecoveryRequired is absorbing; this store exposes no ordinary recovery-exit mutation"
      )

  final case class RestorationProtocolNotDurable()
      extends FinalityDurableStoreError(
        "Restoration is disabled until ForkChoiceOrphanClaim has objective fork-choice verification and an exact durable restoration bundle"
      )

  final case class MissingEffectReceipt(manifest: EffectManifestPointer, effectId: EffectId)
      extends FinalityDurableStoreError(s"Missing terminal effect receipt: manifest=$manifest effectId=$effectId")

  final case class UnknownEffect(manifest: EffectManifestPointer, effectId: EffectId)
      extends FinalityDurableStoreError(s"Effect receipt names no command in manifest: manifest=$manifest effectId=$effectId")

  final case class PathTraversalLimitExceeded(limit: String, maximum: Long, attempted: BigInt)
      extends FinalityDurableStoreError(
        s"Path traversal exceeded local recovery work bound: limit=$limit maximum=$maximum attempted=$attempted"
      )

  final case class InvalidPath(detail: String) extends FinalityDurableStoreError(s"Invalid finality path: $detail")

  final case class OutboxTraversalLimitExceeded(maximum: Long, attempted: BigInt)
      extends FinalityDurableStoreError(
        s"Effect-manifest traversal exceeded local work bound: maximum=$maximum attempted=$attempted"
      )

  final case class AuditTraversalLimitExceeded(maximum: Long, attempted: BigInt)
      extends FinalityDurableStoreError(
        s"Coordinator audit traversal exceeded local recovery work bound: maximum=$maximum attempted=$attempted"
      )
}

sealed trait FinalityDurableWriteArtifact extends Product with Serializable

object FinalityDurableWriteArtifact {
  final case class CoordinatorHead(revision: HeadRevision) extends FinalityDurableWriteArtifact
  final case class EffectOutboxHead(revision: EffectOutboxRevision) extends FinalityDurableWriteArtifact
  final case class Immutable(path: Path) extends FinalityDurableWriteArtifact
}

final case class StructurallyBoundCoreBatch(pointer: FinalityCoreBatchPointer, batch: FinalityCoreBatch)

/** Startup result after checksums, exact identities, and the complete audit chain have been resolved. In `Running`, current
  * released/core-batch payloads, paths, and effect dependencies are also resolved. In absorbing `RecoveryRequired`, those optional
  * dependency fields deliberately remain empty because their absence or corruption may be the recorded reason recovery was entered.
  *
  * This is durability verification, not finality authorization. Before activation, the coordinator must still run `validateCoreBatch` with
  * resolved paths, prior context, effect semantics, current branch selection, and exact MPT readback.
  */
final case class DurablyVerifiedCoordinatorHead private[finality] (
  value: CoordinatorHead,
  payloadDigest: Hash,
  audit: CoordinatorAuditRecord,
  recovery: Option[RecoveryRecord],
  releasedBatch: Option[StructurallyBoundCoreBatch],
  activeBatch: Option[StructurallyBoundCoreBatch],
  latestEffectManifest: Option[FinalityEffectManifest]
)

final case class DurablyVerifiedEffectOutboxHead private[finality] (
  value: FinalityEffectOutboxHead,
  payloadDigest: Hash
)

final case class VerifiedStoredPath private[finality] (
  intentId: IntentId,
  commitment: PathCommitment,
  firstChunk: ImmutableArtifactPointer,
  chunkCount: Long,
  encodedChunkBytes: Long
)

private object FinalityDurableStoreInternals {
  final case class PathScan(
    index: Long,
    expectedPointer: Option[ImmutableArtifactPointer],
    entries: Long,
    chunks: Long,
    encodedBytes: Long,
    firstPointer: Option[ImmutableArtifactPointer],
    firstEntry: Option[GlobalSnapshotStateRef],
    lastEntry: Option[GlobalSnapshotStateRef],
    accumulator: FinalityIdentity.PathEntriesAccumulator
  )

  final case class HeadDependencies(
    releasedBatch: Option[StructurallyBoundCoreBatch],
    activeBatch: Option[StructurallyBoundCoreBatch],
    latestEffectManifest: Option[FinalityEffectManifest]
  )

  final case class ResolvedEffectManifest(
    pointer: EffectManifestPointer,
    manifest: FinalityEffectManifest
  )

  final case class VerifiedAuditTail(
    currentAudit: CoordinatorAuditRecord,
    currentRecovery: Option[RecoveryRecord]
  )

  sealed trait TargetStatus extends Product with Serializable

  object TargetStatus {
    case object Missing extends TargetStatus
    case object Regular extends TargetStatus
    final case class Unsafe(fileType: String) extends TargetStatus
  }
}

sealed trait FinalityDurableCasResult[+A] extends Product with Serializable

object FinalityDurableCasResult {
  final case class Installed[A](value: A) extends FinalityDurableCasResult[A]
  final case class AlreadyInstalled[A](value: A) extends FinalityDurableCasResult[A]
}

sealed trait FinalityDurableStore[F[_]] {
  def putArtifact(pointer: ImmutableArtifactPointer, bytes: ByteVector): F[Unit]
  def readArtifact(pointer: ImmutableArtifactPointer): F[ByteVector]

  def readCoreBatch(pointer: FinalityCoreBatchPointer): F[StructurallyBoundCoreBatch]
  def readCoreBatchByIntent(intentId: IntentId): F[StructurallyBoundCoreBatch]

  def readReleasedCore(pointer: ReleasedCorePointer): F[ReleasedCore]

  def verifyStoredPath(intentId: IntentId, commitment: PathCommitment): F[VerifiedStoredPath]

  def readEffectManifest(pointer: EffectManifestPointer): F[FinalityEffectManifest]

  private[finality] def persistStructurallyValidatedEffectReceipt(
    coordinator: DurablyVerifiedCoordinatorHead,
    receipt: TerminalEffectReceipt
  ): F[Unit]
  def readEffectReceipt(manifest: EffectManifestPointer, effectId: EffectId): F[Option[TerminalEffectReceipt]]

  def coordinatorHead: F[Option[DurablyVerifiedCoordinatorHead]]
  def initializeCoordinator(
    mutation: FinalityCoordinatorMutation
  ): F[FinalityDurableCasResult[DurablyVerifiedCoordinatorHead]]
  def compareAndSetCoordinator(
    expected: DurablyVerifiedCoordinatorHead,
    mutation: FinalityCoordinatorMutation
  ): F[FinalityDurableCasResult[DurablyVerifiedCoordinatorHead]]

  def effectOutboxHead(
    coordinator: DurablyVerifiedCoordinatorHead
  ): F[Option[DurablyVerifiedEffectOutboxHead]]
  def initializeEffectOutbox(
    coordinator: DurablyVerifiedCoordinatorHead,
    next: FinalityEffectOutboxHead
  ): F[FinalityDurableCasResult[DurablyVerifiedEffectOutboxHead]]
  def compareAndSetEffectOutbox(
    coordinator: DurablyVerifiedCoordinatorHead,
    expected: DurablyVerifiedEffectOutboxHead,
    next: FinalityEffectOutboxHead
  ): F[FinalityDurableCasResult[DurablyVerifiedEffectOutboxHead]]
}

object FinalityDurableStore {
  def validateDomain(domain: FinalityDomain): Either[FinalityDurableStoreError.InvalidDomain, Unit] = {
    val fields = List(
      "networkId" -> domain.networkId,
      "genesisHash" -> domain.genesisHash,
      "protocolEra" -> domain.protocolEra
    )
    val invalid = fields.collect {
      case (name, hash) if !isCanonicalNonZeroHash(hash) => name
    }
    Either.cond(
      invalid.isEmpty,
      (),
      FinalityDurableStoreError.InvalidDomain(
        s"${invalid.mkString(", ")} must be nonzero 64-character lowercase hexadecimal hashes"
      )
    )
  }

  def make[F[_]: Async](
    directory: Path,
    domain: FinalityDomain,
    limits: FinalityDurableStoreLimits = FinalityDurableStoreLimits.default
  ): Resource[F, FinalityDurableStore[F]] = {
    val fileOps = DurableFileOps.nio[F]
    make(directory, domain, limits, fileOps, DurableWriteHook.noop[F, FinalityDurableWriteArtifact])
  }

  def make[F[_]: Async](
    directory: Path,
    domain: FinalityDomain,
    limits: FinalityDurableStoreLimits,
    fileOps: DurableFileOps[F],
    writeHook: DurableWriteHook[F, FinalityDurableWriteArtifact]
  ): Resource[F, FinalityDurableStore[F]] = {
    val root = directory.toAbsolutePath.normalize()
    val lockPath = root.resolve(".lock")

    for {
      _ <- Resource.eval(Async[F].fromEither(FinalityDurableStoreLimits.validate(limits)))
      _ <- Resource.eval(Async[F].fromEither(validateDomain(domain)))
      _ <- Resource.eval(ensureRootDirectory[F](root, fileOps))
      _ <- Resource.eval(rejectUnsafeLockIfPresent[F](lockPath))
      _ <- fileOps.acquireExclusiveLock(lockPath)
      headMutex <- Resource.eval(Semaphore[F](1L))
      outboxMutex <- Resource.eval(Semaphore[F](1L))
      headCache <- Resource.eval(Ref.of[F, Option[DurablyVerifiedCoordinatorHead]](None))
      outboxCache <- Resource.eval(Ref.of[F, Option[DurablyVerifiedEffectOutboxHead]](None))
      store = new LiveFinalityDurableStore[F](
        root,
        domain,
        limits,
        fileOps,
        writeHook,
        headMutex,
        outboxMutex,
        headCache,
        outboxCache
      )
      _ <- Resource.eval(store.initializeLayout)
    } yield store
  }

  private def isCanonicalNonZeroHash(hash: Hash): Boolean = {
    val value = hash.value
    value.length == 64 && value.exists(_ != '0') && value.forall { character =>
      character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
    }
  }

  private def ensureRootDirectory[F[_]: Async](root: Path, fileOps: DurableFileOps[F]): F[Unit] =
    Option(root.getParent).traverse_(parent => Async[F].blocking(verifyExistingDirectoryComponents(parent))) >>
      fileOps.createDirectories(root) >> Async[F].blocking {
        verifyExistingDirectoryComponents(root)
        if (Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS))
          throw FinalityDurableStoreError.UnsafeDirectory(root)
      }

  private def rejectUnsafeLockIfPresent[F[_]: Async](path: Path): F[Unit] =
    Async[F].blocking {
      if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
        val attributes = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
        if (Files.isSymbolicLink(path)) throw FinalityDurableStoreError.UnsafeFile(path, "symbolic-link")
        else if (!attributes.isRegularFile)
          throw FinalityDurableStoreError.UnsafeFile(path, if (attributes.isDirectory) "directory" else "special-file")
      }
    }

  private[finality] def verifyExistingDirectoryComponents(path: Path): Unit = {
    val absolute = path.toAbsolutePath.normalize()
    var current = absolute.getRoot
    absolute.iterator().asScala.foreach { component =>
      current = current.resolve(component)
      if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
        if (Files.isSymbolicLink(current) || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS))
          throw FinalityDurableStoreError.UnsafeDirectory(current)
      }
    }
  }
}

private final class FinalityDurableLayout(root: Path) {
  import FinalityDurableStoreError._

  val normalizedRoot: Path = root.toAbsolutePath.normalize()
  val storeMarker: Path = under(normalizedRoot, "store.initialized")
  val coordinatorHead: Path = under(normalizedRoot, "coordinator.head")
  val coordinatorMarker: Path = under(normalizedRoot, "coordinator.initialized")
  val effectOutboxHead: Path = under(normalizedRoot, "effects.head")
  val effectOutboxMarker: Path = under(normalizedRoot, "effects.initialized")

  val immutableRoot: Path = under(normalizedRoot, "immutable")
  val batchLocatorRoot: Path = under(normalizedRoot, "batch-by-intent")
  val pathChunkLocatorRoot: Path = under(normalizedRoot, "path-chunks")
  val effectManifestRoot: Path = under(normalizedRoot, "effect-manifests")
  val auditRoot: Path = under(normalizedRoot, "audit")
  val recoveryRoot: Path = under(normalizedRoot, "recovery")
  val receiptRoot: Path = under(normalizedRoot, "effect-receipts")

  val directories: List[Path] =
    List(
      immutableRoot,
      batchLocatorRoot,
      pathChunkLocatorRoot,
      effectManifestRoot,
      auditRoot,
      recoveryRoot,
      receiptRoot
    )

  def artifact(pointer: ImmutableArtifactPointer): Path = {
    val kindDirectory = under(immutableRoot, artifactKindDirectory(pointer.kind))
    under(kindDirectory, s"${hex("artifact id", pointer.id.value)}.bin")
  }

  def artifactKindRoot(kind: FinalityArtifactKind): Path = under(immutableRoot, artifactKindDirectory(kind))

  def coreBatchByIntent(intentId: IntentId): Path =
    under(batchLocatorRoot, s"${hex("intent id", intentId.value)}.bin")

  def pathChunk(intentId: IntentId, manifestId: ArtifactId, index: NonNegLong): Path = {
    val intent = under(pathChunkLocatorRoot, hex("intent id", intentId.value))
    val manifest = under(intent, hex("manifest id", manifestId.value))
    under(manifest, f"${index.value}%019d.bin")
  }

  def pathChunkParents(intentId: IntentId, manifestId: ArtifactId): List[Path] = {
    val intent = under(pathChunkLocatorRoot, hex("intent id", intentId.value))
    List(intent, under(intent, hex("manifest id", manifestId.value)))
  }

  def effectManifest(pointer: EffectManifestPointer): Path =
    under(effectManifestRoot, s"${hex("effect manifest id", pointer.id.value)}.bin")

  def audit(pointer: AuditPointer): Path = under(auditRoot, s"${hex("audit id", pointer.id.value)}.bin")

  def recovery(pointer: RecoveryRecordPointer): Path =
    under(recoveryRoot, s"${hex("recovery id", pointer.id.value)}.bin")

  def receipt(manifest: EffectManifestPointer, effectId: EffectId): Path = {
    val manifestDirectory = under(receiptRoot, hex("effect manifest id", manifest.id.value))
    under(manifestDirectory, s"${hex("effect id", effectId.value)}.bin")
  }

  def receiptParent(manifest: EffectManifestPointer): Path =
    under(receiptRoot, hex("effect manifest id", manifest.id.value))

  private def artifactKindDirectory(kind: FinalityArtifactKind): String =
    kind match {
      case FinalityArtifactKind.CoreBatch                   => "01-core-batch"
      case FinalityArtifactKind.ReleasedCoreRecord          => "02-released-core"
      case FinalityArtifactKind.PathManifest                => "03-path-manifest"
      case FinalityArtifactKind.PathChunk                   => "04-path-chunk"
      case FinalityArtifactKind.DecidedAttestationEvidence  => "05-decided-attestation"
      case FinalityArtifactKind.DepthK1Evidence             => "06-depth-k1"
      case FinalityArtifactKind.ForkChoiceDecisionEvidence  => "08-fork-choice-decision"
      case FinalityArtifactKind.PreparedSemanticState       => "09-prepared-semantic"
      case FinalityArtifactKind.AuthenticatedTargetAnchor   => "10-authenticated-target"
      case FinalityArtifactKind.AppliedSemanticStateReceipt => "11-applied-semantic"
      case FinalityArtifactKind.AuthenticatedAnchorReceipt  => "12-authenticated-anchor"
      case FinalityArtifactKind.PriorSemanticStateReceipt   => "13-prior-semantic"
      case FinalityArtifactKind.PriorAnchorReceipt          => "14-prior-anchor"
      case FinalityArtifactKind.EffectPayload               => "15-effect-payload"
    }

  private def hex(label: String, hash: Hash): String = {
    val value = hash.value
    val canonical = value.length == 64 && value.forall { character =>
      character >= '0' && character <= '9' || character >= 'a' && character <= 'f'
    }
    if (!canonical) throw InvalidIdentifier(label, value)
    value
  }

  private def under(parent: Path, child: String): Path = {
    val candidate = parent.resolve(child).normalize()
    if (!candidate.startsWith(normalizedRoot)) throw UnsafePath(candidate)
    candidate
  }
}

private final class LiveFinalityDurableStore[F[_]: Async](
  root: Path,
  domain: FinalityDomain,
  limits: FinalityDurableStoreLimits,
  fileOps: DurableFileOps[F],
  writeHook: DurableWriteHook[F, FinalityDurableWriteArtifact],
  headMutex: Semaphore[F],
  outboxMutex: Semaphore[F],
  headCache: Ref[F, Option[DurablyVerifiedCoordinatorHead]],
  outboxCache: Ref[F, Option[DurablyVerifiedEffectOutboxHead]]
) extends FinalityDurableStore[F] {
  import FinalityDurableCasResult._
  import FinalityDurableStoreInternals._
  import FinalityDurableStoreError._
  import DurableWriteBoundary._
  import DurableWriteStage._

  private val layout = new FinalityDurableLayout(root)
  private val atomicWriter = new DurableAtomicWriter[F](fileOps)

  private val StoreMarkerSuffix: Byte = 0
  private val CoordinatorMarkerSuffix: Byte = 1
  private val EffectOutboxMarkerSuffix: Byte = 2

  def initializeLayout: F[Unit] =
    for {
      _ <- initializeStoreMarker
      _ <- ensureDirectories(layout.directories)
      _ <- verifyDirectoryTree
      coordinator <- loadCoordinatorStartupUnlocked
      _ <- headCache.set(coordinator)
      _ <- coordinator match {
        case Some(value) =>
          value.value.mode match {
            case CoordinatorMode.Running =>
              loadEffectOutboxStartupUnlocked(value)
                .flatMap(outboxCache.set)
                .handleErrorWith(error =>
                  recoverExpectedStartupDependencyFailure(value, error).flatMap { recovered =>
                    headCache.set(Some(recovered)) >> outboxCache.set(None)
                  }
                )
            case _: CoordinatorMode.RecoveryRequired =>
              outboxCache.set(None)
          }
        case None =>
          loadRawEffectOutboxUnlocked.flatMap {
            case None => Async[F].unit
            case Some(_) =>
              Async[F].raiseError[Unit](
                IdentityMismatch("effect outbox", "persisted outbox cannot exist without a coordinator head")
              )
          }
      }
    } yield ()

  private def initializeStoreMarker: F[Unit] =
    readOptionalEnvelope(layout.storeMarker, InitializationMarkerEnvelope, 1024L).flatMap {
      case Some(payload) => verifyMarker(payload, StoreMarkerSuffix)
      case None =>
        for {
          coordinatorMarker <- readOptionalEnvelope(
            layout.coordinatorMarker,
            InitializationMarkerEnvelope,
            1024L
          )
          outboxMarker <- readOptionalEnvelope(
            layout.effectOutboxMarker,
            InitializationMarkerEnvelope,
            1024L
          )
          _ <- coordinatorMarker.traverse_(payload => verifyMarker(payload, CoordinatorMarkerSuffix))
          _ <- outboxMarker.traverse_(payload => verifyMarker(payload, EffectOutboxMarkerSuffix))
          roleBound = coordinatorMarker.nonEmpty || outboxMarker.nonEmpty
          pristine <- if (roleBound) Async[F].pure(false) else isPristineStoreDirectory
          _ <- Async[F].raiseUnless(roleBound || pristine)(
            Missing(InitializationMarkerEnvelope, layout.storeMarker)
          )
          domainBytes <- encodePayload("FinalityDomain", finalityDomainCodec, domain)
          _ <- createOrVerifyImmutable(
            layout.storeMarker,
            InitializationMarkerEnvelope,
            markerPayload(domainBytes, StoreMarkerSuffix),
            1024L
          )
        } yield ()
    }

  private def isPristineStoreDirectory: F[Boolean] =
    Async[F].blocking {
      val allowedDirectories = layout.directories.map(_.getFileName.toString).toSet
      val entries = Files.newDirectoryStream(layout.normalizedRoot)
      try
        entries.iterator().asScala.forall { path =>
          val name = path.getFileName.toString
          if (name == ".lock") true
          else if (allowedDirectories.contains(name))
            !Files.isSymbolicLink(path) &&
            Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
            isEmptyDirectory(path)
          else
            name.startsWith(".store.initialized.") &&
            name.endsWith(".tmp") &&
            !Files.isSymbolicLink(path) &&
            Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
        }
      finally entries.close()
    }

  private def isEmptyDirectory(path: Path): Boolean = {
    val entries = Files.newDirectoryStream(path)
    try !entries.iterator().hasNext
    finally entries.close()
  }

  def putArtifact(pointer: ImmutableArtifactPointer, bytes: ByteVector): F[Unit] = {
    val payload = bytes.toArray
    for {
      derived <- liftIdentity(FinalityIdentity.artifactPointerFromBytes(pointer.kind, pointer.encoding, bytes))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("immutable artifact", s"expected=$pointer derived=$derived")
      )
      _ <- Async[F].raiseUnless(pointer.byteLength.value == payload.length.toLong)(
        IdentityMismatch(
          "immutable artifact length",
          s"pointer=${pointer.byteLength.value} payload=${payload.length}"
        )
      )
      _ <- ensureDirectory(layout.artifactKindRoot(pointer.kind))
      _ <- createOrVerifyImmutable(
        layout.artifact(pointer),
        ImmutableArtifactEnvelope,
        payload,
        limits.maxImmutableArtifactBytes
      )
    } yield ()
  }

  def readArtifact(pointer: ImmutableArtifactPointer): F[ByteVector] =
    for {
      payload <- readRequiredEnvelope(
        layout.artifact(pointer),
        ImmutableArtifactEnvelope,
        limits.maxImmutableArtifactBytes
      )
      _ <- Async[F].raiseUnless(pointer.byteLength.value == payload.length.toLong)(
        IdentityMismatch(
          "immutable artifact length",
          s"pointer=${pointer.byteLength.value} payload=${payload.length}"
        )
      )
      bytes = ByteVector.view(payload)
      derived <- liftIdentity(FinalityIdentity.artifactPointerFromBytes(pointer.kind, pointer.encoding, bytes))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("immutable artifact", s"expected=$pointer derived=$derived")
      )
    } yield bytes

  private def putCoreBatch(pointer: FinalityCoreBatchPointer, batch: FinalityCoreBatch): F[Unit] =
    for {
      payload <- encodePayload("FinalityCoreBatch", finalityCoreBatchPayloadCodec, batch)
      derivedArtifact <- liftIdentity(
        FinalityIdentity.artifactPointerFromBytes(
          FinalityArtifactKind.CoreBatch,
          ByteVector.view(payload)
        )
      )
      _ <- ensureCoreBatchPointer(pointer, batch)
      _ <- Async[F].raiseUnless(pointer.artifact == derivedArtifact)(
        IdentityMismatch("core batch pointer", s"pointer=${pointer.artifact} derived=$derivedArtifact")
      )
      _ <- putArtifact(pointer.artifact, ByteVector.view(payload))
      _ <- createOrVerifyImmutable(
        layout.coreBatchByIntent(pointer.intentId),
        ImmutableArtifactEnvelope,
        payload,
        limits.maxImmutableArtifactBytes
      )
    } yield ()

  def readCoreBatch(pointer: FinalityCoreBatchPointer): F[StructurallyBoundCoreBatch] =
    for {
      bytes <- readArtifact(pointer.artifact)
      batch <- decodePayload("FinalityCoreBatch", finalityCoreBatchPayloadCodec, bytes.toArray)
      _ <- ensureCoreBatchPointer(pointer, batch)
    } yield StructurallyBoundCoreBatch(pointer, batch)

  def readCoreBatchByIntent(intentId: IntentId): F[StructurallyBoundCoreBatch] =
    for {
      payload <- readRequiredEnvelope(
        layout.coreBatchByIntent(intentId),
        ImmutableArtifactEnvelope,
        limits.maxImmutableArtifactBytes
      )
      batch <- decodePayload("FinalityCoreBatch", finalityCoreBatchPayloadCodec, payload)
      artifact <- liftIdentity(
        FinalityIdentity.artifactPointerFromBytes(FinalityArtifactKind.CoreBatch, ByteVector.view(payload))
      )
      pointer = FinalityCoreBatchPointer(batch.intentId, batch.scope.attempt, batch.scope.generation, artifact)
      _ <- ensureCoreBatchPointer(pointer, batch)
      _ <- Async[F].raiseUnless(pointer.intentId == intentId)(
        IdentityMismatch("core batch intent locator", s"locator=$intentId payload=${pointer.intentId}")
      )
      _ <- readArtifact(artifact)
    } yield StructurallyBoundCoreBatch(pointer, batch)

  private def putReleasedCore(released: ReleasedCore): F[Unit] =
    for {
      batch <- readCoreBatchByIntent(released.pointer.intentId)
      _ <- validate(FinalityIntentValidator.validateReleasedCore(batch.batch, released))
      payload <- encodePayload("ReleasedCoreRecordPayload", releasedCoreRecordPayloadCodec, released.payload)
      derived <- liftIdentity(
        FinalityIdentity.artifactPointerFromBytes(
          FinalityArtifactKind.ReleasedCoreRecord,
          ByteVector.view(payload)
        )
      )
      _ <- Async[F].raiseUnless(
        released.pointer.record == derived &&
          released.record == ScopedArtifactRef(released.pointer.intentId, derived)
      )(
        IdentityMismatch("released core record", s"released=${released.pointer.record} derived=$derived")
      )
      _ <- putArtifact(derived, ByteVector.view(payload))
    } yield ()

  def readReleasedCore(pointer: ReleasedCorePointer): F[ReleasedCore] =
    for {
      bytes <- readArtifact(pointer.record)
      payload <- decodePayload("ReleasedCoreRecordPayload", releasedCoreRecordPayloadCodec, bytes.toArray)
      released = ReleasedCore(pointer, ScopedArtifactRef(pointer.intentId, pointer.record), payload)
      batch <- readCoreBatchByIntent(pointer.intentId)
      _ <- validate(FinalityIntentValidator.validateReleasedCore(batch.batch, released))
    } yield released

  private def putPathManifest(ref: PathManifestRef, payload: PathManifestPayload): F[Unit] =
    for {
      _ <- Async[F].raiseUnless(payload == ref.commitment.payload)(
        IdentityMismatch("path manifest payload", s"commitment=${ref.commitment.payload} payload=$payload")
      )
      encoded <- encodePayload("PathManifestPayload", pathManifestPayloadCodec, payload)
      _ <- putArtifact(ref.commitment.manifest, ByteVector.view(encoded))
      _ <- Async[F].raiseUnless(ref.manifest.artifact == ref.commitment.manifest)(
        IdentityMismatch("path manifest reference", s"ref=${ref.manifest.artifact} commitment=${ref.commitment.manifest}")
      )
    } yield ()

  private def putPathChunk(pointer: ImmutableArtifactPointer, chunk: PathChunk): F[Unit] =
    for {
      payload <- encodePayload("PathChunk", pathChunkPayloadCodec, chunk)
      derived <- liftIdentity(
        FinalityIdentity.artifactPointerFromBytes(FinalityArtifactKind.PathChunk, ByteVector.view(payload))
      )
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("path chunk", s"expected=$pointer derived=$derived")
      )
      _ <- putArtifact(pointer, ByteVector.view(payload))
      _ <- ensureDirectories(layout.pathChunkParents(chunk.intentId, chunk.manifestId))
      _ <- createOrVerifyImmutable(
        layout.pathChunk(chunk.intentId, chunk.manifestId, chunk.chunkIndex),
        ImmutableArtifactEnvelope,
        payload,
        limits.maxImmutableArtifactBytes
      )
    } yield ()

  private def putEffectManifest(pointer: EffectManifestPointer, manifest: FinalityEffectManifest): F[Unit] =
    for {
      _ <- requireDomain("effect manifest", manifest.scope.domain)
      payload <- encodePayload("FinalityEffectManifest", finalityEffectManifestPayloadCodec, manifest)
      derived <- liftIdentity(FinalityIdentity.effectManifestPointer(manifest))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("effect manifest", s"expected=$pointer derived=$derived")
      )
      _ <- createOrVerifyImmutable(
        layout.effectManifest(pointer),
        EffectManifestEnvelope,
        payload,
        limits.maxEffectManifestBytes
      )
    } yield ()

  def readEffectManifest(pointer: EffectManifestPointer): F[FinalityEffectManifest] =
    for {
      payload <- readRequiredEnvelope(
        layout.effectManifest(pointer),
        EffectManifestEnvelope,
        limits.maxEffectManifestBytes
      )
      manifest <- decodePayload("FinalityEffectManifest", finalityEffectManifestPayloadCodec, payload)
      _ <- requireDomain("effect manifest", manifest.scope.domain)
      derived <- liftIdentity(FinalityIdentity.effectManifestPointer(manifest))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("effect manifest", s"expected=$pointer derived=$derived")
      )
    } yield manifest

  /** Structural persistence only. This does not prove sink CAS/readback provenance. A package-owned sink executor must be the sole caller
    * before effect delivery is activated.
    */
  private[finality] def persistStructurallyValidatedEffectReceipt(
    coordinator: DurablyVerifiedCoordinatorHead,
    receipt: TerminalEffectReceipt
  ): F[Unit] =
    withCurrentCoordinator(coordinator, requireRunning = true) {
      for {
        manifest <- readEffectManifest(receipt.manifest)
        command <- effectCommands(manifest)
          .find(_.effectId == receipt.effectId)
          .liftTo[F](UnknownEffect(receipt.manifest, receipt.effectId))
        _ <- validate(FinalityIntentValidator.validateEffectReceipt(command, receipt.manifest, receipt))
        _ <- requirePredecessorReceipt(manifest, command)
        payload <- encodePayload("TerminalEffectReceipt", terminalEffectReceiptPayloadCodec, receipt)
        _ <- ensureDirectory(layout.receiptParent(receipt.manifest))
        _ <- createOrVerifyImmutable(
          layout.receipt(receipt.manifest, receipt.effectId),
          EffectReceiptEnvelope,
          payload,
          limits.maxEffectReceiptBytes
        )
      } yield ()
    }

  def readEffectReceipt(
    manifest: EffectManifestPointer,
    effectId: EffectId
  ): F[Option[TerminalEffectReceipt]] =
    readOptionalEnvelope(
      layout.receipt(manifest, effectId),
      EffectReceiptEnvelope,
      limits.maxEffectReceiptBytes
    ).flatMap(
      _.traverse { payload =>
        for {
          receipt <- decodePayload("TerminalEffectReceipt", terminalEffectReceiptPayloadCodec, payload)
          resolvedManifest <- readEffectManifest(manifest)
          command <- effectCommands(resolvedManifest)
            .find(_.effectId == effectId)
            .liftTo[F](UnknownEffect(manifest, effectId))
          _ <- validate(FinalityIntentValidator.validateEffectReceipt(command, manifest, receipt))
        } yield receipt
      }
    )

  private def requirePredecessorReceipt(
    manifest: FinalityEffectManifest,
    command: ScopedEffectCommand
  ): F[Unit] =
    command.predecessor match {
      case None => Async[F].unit
      case Some(predecessor) =>
        for {
          previousPointer <- manifest.previous.liftTo[F](
            IdentityMismatch("effect predecessor", s"${command.kind} names $predecessor without a prior manifest")
          )
          previousManifest <- readEffectManifest(previousPointer)
          previousCommand <- effectCommands(previousManifest)
            .find(_.kind == command.kind)
            .liftTo[F](IdentityMismatch("effect predecessor", s"prior manifest lacks ${command.kind}"))
          _ <- Async[F].raiseUnless(previousCommand.effectId == predecessor)(
            IdentityMismatch(
              "effect predecessor",
              s"command=${command.effectId} expected=$predecessor prior=${previousCommand.effectId}"
            )
          )
          receipt <- readEffectReceipt(previousPointer, predecessor)
          _ <- receipt.liftTo[F](MissingEffectReceipt(previousPointer, predecessor))
        } yield ()
    }

  def verifyStoredPath(intentId: IntentId, commitment: PathCommitment): F[VerifiedStoredPath] =
    for {
      manifestBytes <- readArtifact(commitment.manifest)
      payload <- decodePayload("PathManifestPayload", pathManifestPayloadCodec, manifestBytes.toArray)
      _ <- Async[F].raiseUnless(payload == commitment.payload)(
        IdentityMismatch("path manifest", s"commitment=${commitment.payload} payload=$payload")
      )
      _ <- Async[F].raiseUnless(commitment.summary.entryCount.value > 0L)(
        InvalidPath("entryCount must be positive")
      )
      initial = PathScan(
        index = 0L,
        expectedPointer = None,
        entries = 0L,
        chunks = 0L,
        encodedBytes = 0L,
        firstPointer = None,
        firstEntry = None,
        lastEntry = None,
        accumulator = FinalityIdentity.PathEntriesAccumulator.empty
      )
      verified <- Async[F].tailRecM(initial)(scanPathChunk(intentId, commitment, _))
    } yield verified

  def coordinatorHead: F[Option[DurablyVerifiedCoordinatorHead]] =
    headMutex.permit.use(_ => loadCoordinatorUnlocked)

  def initializeCoordinator(
    mutation: FinalityCoordinatorMutation
  ): F[FinalityDurableCasResult[DurablyVerifiedCoordinatorHead]] =
    headMutex.permit.use { _ =>
      val next = mutation.head
      val audit = mutation.audit
      loadCoordinatorUnlocked.flatMap {
        case Some(actual) if actual.value == next =>
          verifyAlreadyInstalled(actual, mutation).as(AlreadyInstalled(actual))
        case Some(actual) =>
          Async[F].raiseError(
            CoordinatorCompareAndSetConflict(None, Some(actual.payloadDigest))
          )
        case None =>
          for {
            _ <- validate(
              FinalityIntentValidator.validateCoordinatorTransition(None, next, audit, mutation.recoveryRecord)
            )
            _ <- Async[F].raiseUnless(mutation.preparedBundle.isEmpty)(
              IdentityMismatch("initial coordinator mutation", "cannot contain a prepared bundle")
            )
            _ <- verifyHeadDependencies(next, None, verifyFullEffectLineage = false)
            auditPointer <- putAudit(audit)
            _ <- Async[F].raiseUnless(next.auditTail.contains(auditPointer))(
              IdentityMismatch("coordinator audit tail", s"head=${next.auditTail} derived=$auditPointer")
            )
            _ <- replaceCoordinatorHead(next)
            domainBytes <- encodePayload("FinalityDomain", finalityDomainCodec, domain)
            _ <- createOrVerifyImmutable(
              layout.coordinatorMarker,
              InitializationMarkerEnvelope,
              markerPayload(domainBytes, CoordinatorMarkerSuffix),
              1024L
            )
            loaded <- loadCoordinatorUnlocked.flatMap(_.liftTo[F](CoordinatorNotInitialized()))
          } yield Installed(loaded)
      }
    }

  def compareAndSetCoordinator(
    expected: DurablyVerifiedCoordinatorHead,
    mutation: FinalityCoordinatorMutation
  ): F[FinalityDurableCasResult[DurablyVerifiedCoordinatorHead]] =
    headMutex.permit.use { _ =>
      val next = mutation.head
      val audit = mutation.audit
      val recovery = mutation.recoveryRecord
      val loadCurrent =
        if (audit.mutation == CoordinatorMutationKind.RecoveryEntered) loadCoordinatorJournalUnlocked
        else loadCoordinatorUnlocked
      loadCurrent.flatMap {
        case Some(actual) if actual.value == next =>
          verifyAlreadyInstalled(actual, mutation).as(AlreadyInstalled(actual))
        case Some(actual) if actual.payloadDigest == expected.payloadDigest && actual.value == expected.value =>
          actual.value.mode match {
            case _: CoordinatorMode.RecoveryRequired => Async[F].raiseError(RecoveryModeIsAbsorbing())
            case CoordinatorMode.Running =>
              for {
                _ <- validate(
                  FinalityIntentValidator.validateCoordinatorTransition(
                    Some(actual.value),
                    next,
                    audit,
                    recovery
                  )
                )
                _ <- persistMutationDependencies(actual.value, mutation)
                _ <-
                  if (audit.mutation == CoordinatorMutationKind.RecoveryEntered) Async[F].unit
                  else verifyHeadDependencies(next, Some(actual), verifyFullEffectLineage = false).void
                _ <- recovery.traverse_(putRecovery)
                auditPointer <- putAudit(audit)
                _ <- Async[F].raiseUnless(next.auditTail.contains(auditPointer))(
                  IdentityMismatch("coordinator audit tail", s"head=${next.auditTail} derived=$auditPointer")
                )
                _ <- replaceCoordinatorHead(next)
                loaded <- loadCoordinatorUnlocked.flatMap(_.liftTo[F](CoordinatorNotInitialized()))
              } yield Installed(loaded)
          }
        case actual =>
          Async[F].raiseError(
            CoordinatorCompareAndSetConflict(
              Some(expected.payloadDigest),
              actual.map(_.payloadDigest)
            )
          )
      }
    }

  def effectOutboxHead(
    coordinator: DurablyVerifiedCoordinatorHead
  ): F[Option[DurablyVerifiedEffectOutboxHead]] =
    outboxMutex.permit.use { _ =>
      ensureCurrentCoordinator(coordinator, requireRunning = true) >>
        loadEffectOutboxUnlocked(coordinator).flatTap(_ => ensureCurrentCoordinator(coordinator, requireRunning = true))
    }

  def initializeEffectOutbox(
    coordinator: DurablyVerifiedCoordinatorHead,
    next: FinalityEffectOutboxHead
  ): F[FinalityDurableCasResult[DurablyVerifiedEffectOutboxHead]] =
    outboxMutex.permit.use { _ =>
      ensureCurrentCoordinator(coordinator, requireRunning = true) >>
        loadEffectOutboxUnlocked(coordinator).flatMap {
          case Some(actual) if actual.value == next => Async[F].pure(AlreadyInstalled(actual))
          case Some(actual) =>
            Async[F].raiseError(
              EffectOutboxCompareAndSetConflict(None, Some(actual.payloadDigest))
            )
          case None =>
            for {
              _ <- Async[F].raiseUnless(
                next.revision.value.value == 0L && next.cursor == EffectOutboxCursor(None, None)
              )(
                StructuralValidationFailed(
                  List(FinalityIntentValidator.Violation("outbox", "initial head must be revision zero with an empty cursor"))
                )
              )
              _ <- validate(FinalityIntentValidator.validateOutbox(coordinator.value, next))
              _ <- withCurrentCoordinator(coordinator, requireRunning = true) {
                replaceEffectOutboxHead(next) >>
                  encodePayload("FinalityDomain", finalityDomainCodec, domain).flatMap { domainBytes =>
                    createOrVerifyImmutable(
                      layout.effectOutboxMarker,
                      InitializationMarkerEnvelope,
                      markerPayload(domainBytes, EffectOutboxMarkerSuffix),
                      1024L
                    )
                  }
              }
              loaded <- loadEffectOutboxUnlocked(coordinator).flatMap(_.liftTo[F](EffectOutboxNotInitialized()))
            } yield Installed(loaded)
        }
    }

  private def scanPathChunk(
    intentId: IntentId,
    commitment: PathCommitment,
    state: PathScan
  ): F[Either[PathScan, VerifiedStoredPath]] = {
    val attemptedChunks = BigInt(state.chunks) + 1
    for {
      _ <- Async[F].raiseWhen(attemptedChunks > BigInt(limits.maxPathTraversalChunks))(
        PathTraversalLimitExceeded("chunks", limits.maxPathTraversalChunks, attemptedChunks)
      )
      index <- NonNegLong.from(state.index).leftMap(error => InvalidPath(error)).liftTo[F]
      chunkPayload <- readRequiredEnvelope(
        layout.pathChunk(intentId, commitment.manifest.id, index),
        ImmutableArtifactEnvelope,
        limits.maxImmutableArtifactBytes
      )
      chunk <- decodePayload("PathChunk", pathChunkPayloadCodec, chunkPayload)
      derived <- liftIdentity(
        FinalityIdentity.artifactPointerFromBytes(
          FinalityArtifactKind.PathChunk,
          ByteVector.view(chunkPayload)
        )
      )
      _ <- state.expectedPointer.traverse_(expected =>
        Async[F].raiseUnless(expected == derived)(
          IdentityMismatch("path chunk link", s"expected=$expected derived=$derived")
        )
      )
      _ <- Async[F].raiseUnless(
        chunk.intentId == intentId &&
          chunk.manifestId == commitment.manifest.id &&
          chunk.chunkIndex.value == state.index
      )(
        InvalidPath(
          s"chunk locator mismatch at index ${state.index}: intent=${chunk.intentId} manifest=${chunk.manifestId} index=${chunk.chunkIndex}"
        )
      )
      chunkEntries = chunk.entriesOldestFirst.toList
      _ <- Async[F].raiseUnless(chunkEntries.nonEmpty && chunkEntries.size <= PathChunk.MaxEntries)(
        InvalidPath(s"chunk ${state.index} entry count must be in [1,${PathChunk.MaxEntries}]")
      )
      attemptedBytes = BigInt(state.encodedBytes) + BigInt(chunkPayload.length)
      _ <- Async[F].raiseWhen(attemptedBytes > BigInt(limits.maxPathTraversalBytes))(
        PathTraversalLimitExceeded("bytes", limits.maxPathTraversalBytes, attemptedBytes)
      )
      attemptedEntries = BigInt(state.entries) + BigInt(chunkEntries.size)
      _ <- Async[F].raiseWhen(attemptedEntries > BigInt(commitment.summary.entryCount.value))(
        InvalidPath(
          s"chunk ${state.index} overruns committed entryCount ${commitment.summary.entryCount.value}"
        )
      )
      folded <- foldPathEntries(state.accumulator, state.lastEntry, chunkEntries)
      nextIndex = BigInt(state.index) + 1
      _ <- Async[F].raiseWhen(nextIndex > BigInt(Long.MaxValue))(InvalidPath("chunk index exhausted"))
      nextIndexLong = nextIndex.toLong
      _ <- chunk.next match {
        case Some(next) =>
          Async[F].raiseUnless(
            chunkEntries.size == PathChunk.MaxEntries &&
              next.intentId == intentId &&
              next.manifestId == commitment.manifest.id &&
              BigInt(next.chunkIndex.value) == nextIndex &&
              next.artifact.kind == FinalityArtifactKind.PathChunk
          )(
            InvalidPath(
              s"nonterminal chunk ${state.index} must be full and point to the exact contiguous locator"
            )
          )
        case None => Async[F].unit
      }
      updated = PathScan(
        index = nextIndexLong,
        expectedPointer = chunk.next.map(_.artifact),
        entries = attemptedEntries.toLong,
        chunks = attemptedChunks.toLong,
        encodedBytes = attemptedBytes.toLong,
        firstPointer = state.firstPointer.orElse(Some(derived)),
        firstEntry = state.firstEntry.orElse(chunkEntries.headOption),
        lastEntry = chunkEntries.lastOption,
        accumulator = folded._1
      )
      result <- chunk.next match {
        case Some(_) => Async[F].pure(Left(updated))
        case None    => finishPath(intentId, commitment, updated, folded._2).map(Right(_))
      }
    } yield result
  }

  private def foldPathEntries(
    initial: FinalityIdentity.PathEntriesAccumulator,
    prior: Option[GlobalSnapshotStateRef],
    entries: List[GlobalSnapshotStateRef]
  ): F[(FinalityIdentity.PathEntriesAccumulator, Option[GlobalSnapshotStateRef])] =
    entries
      .foldLeftM((initial, prior)) {
        case ((accumulator, previous), current) =>
          for {
            _ <- previous.traverse_(before => ensureAdjacent(before, current))
            next <- liftIdentity(accumulator.append(current))
          } yield next -> Some(current)
      }

  private def finishPath(
    intentId: IntentId,
    commitment: PathCommitment,
    state: PathScan,
    lastEntry: Option[GlobalSnapshotStateRef]
  ): F[VerifiedStoredPath] =
    for {
      root <- liftIdentity(state.accumulator.root)
      _ <- Async[F].raiseUnless(
        state.entries == commitment.summary.entryCount.value &&
          state.firstEntry.contains(commitment.summary.oldest) &&
          lastEntry.contains(commitment.summary.newest) &&
          root == commitment.entriesRoot
      )(
        InvalidPath(
          s"terminal path does not match committed count/endpoints/root: entries=${state.entries} root=$root"
        )
      )
      first <- state.firstPointer.liftTo[F](InvalidPath("path has no first chunk"))
    } yield
      VerifiedStoredPath(
        intentId = intentId,
        commitment = commitment,
        firstChunk = first,
        chunkCount = state.chunks,
        encodedChunkBytes = state.encodedBytes
      )

  private def ensureAdjacent(before: GlobalSnapshotStateRef, after: GlobalSnapshotStateRef): F[Unit] =
    Async[F].raiseUnless(
      BigInt(after.ordinal.value.value) == BigInt(before.ordinal.value.value) + 1 &&
        after.parentHash == before.hash
    )(
      InvalidPath(s"noncontiguous entries: before=$before after=$after")
    )

  /** This store proves durability and exact identity only. The live coordinator must still compare `scope.selection.branchRevision` with
    * fork choice immediately before release; no filesystem transaction can supply that volatile canonical-branch CAS.
    */
  private def persistMutationDependencies(
    before: CoordinatorHead,
    mutation: FinalityCoordinatorMutation
  ): F[Unit] =
    (mutation.audit.mutation, mutation.preparedBundle) match {
      case (CoordinatorMutationKind.Prepared, Some(bundle)) =>
        persistPreparedBundle(before, mutation.head, bundle)
      case (CoordinatorMutationKind.Prepared, None) =>
        Async[F].raiseError(
          IdentityMismatch("prepared mutation", "PreparedCoreBundle is mandatory")
        )
      case (other, Some(_)) =>
        Async[F].raiseError(
          IdentityMismatch("coordinator mutation", s"$other must not carry a PreparedCoreBundle")
        )
      case (CoordinatorMutationKind.Released, None) =>
        mutation.head.released
          .liftTo[F](IdentityMismatch("released mutation", "new head has no released core"))
          .flatMap(putReleasedCore)
      case (CoordinatorMutationKind.RestorationStarted, None) | (CoordinatorMutationKind.RestoredAbandoned, None) |
          (CoordinatorMutationKind.AbandonedRetired, None) =>
        Async[F].raiseError(RestorationProtocolNotDurable())
      case _ => Async[F].unit
    }

  private def verifyAlreadyInstalled(
    actual: DurablyVerifiedCoordinatorHead,
    mutation: FinalityCoordinatorMutation
  ): F[Unit] =
    for {
      _ <- Async[F].raiseUnless(
        actual.audit == mutation.audit && actual.recovery == mutation.recoveryRecord
      )(
        IdentityMismatch(
          "idempotent coordinator mutation",
          "supplied audit or recovery record differs from the installed mutation"
        )
      )
      before = mutation.audit.before.map(commitment => CoordinatorHead.fromCommitment(commitment, mutation.audit.priorAudit))
      _ <- validate(
        FinalityIntentValidator.validateCoordinatorTransition(
          before,
          mutation.head,
          mutation.audit,
          mutation.recoveryRecord
        )
      )
      _ <- (mutation.audit.mutation, mutation.preparedBundle) match {
        case (CoordinatorMutationKind.Prepared, Some(bundle)) =>
          before
            .liftTo[F](IdentityMismatch("idempotent prepared mutation", "missing prior coordinator head"))
            .flatMap(prior => persistPreparedBundle(prior, mutation.head, bundle))
        case (CoordinatorMutationKind.Prepared, None) =>
          Async[F].raiseError(IdentityMismatch("idempotent prepared mutation", "missing PreparedCoreBundle"))
        case (other, Some(_)) =>
          Async[F].raiseError(
            IdentityMismatch("idempotent coordinator mutation", s"$other must not carry a PreparedCoreBundle")
          )
        case _ => Async[F].unit
      }
    } yield ()

  private def persistPreparedBundle(
    before: CoordinatorHead,
    next: CoordinatorHead,
    bundle: PreparedCoreBundle
  ): F[Unit] =
    for {
      active <- next.active.liftTo[F](IdentityMismatch("prepared mutation", "new head has no active core"))
      _ <- Async[F].raiseUnless(
        active.batch == bundle.pointer &&
          active.scope == bundle.batch.scope &&
          active.stage == CoreStage.Prepared
      )(
        IdentityMismatch("prepared mutation", s"headActive=$active bundle=${bundle.pointer}/${bundle.batch.scope}")
      )
      context <- coreValidationContext(before.released, before.publication)
      _ <- validate(
        FinalityIntentValidator.validateCoreBatch(
          bundle.batch,
          bundle.effectManifest,
          bundle.resolvedPaths,
          context
        )
      )
      _ <- ensureCoreBatchPointer(bundle.pointer, bundle.batch)
      derivedManifest <- liftIdentity(FinalityIdentity.effectManifestPointer(bundle.effectManifest))
      _ <- Async[F].raiseUnless(bundle.batch.effectManifest == derivedManifest)(
        IdentityMismatch(
          "prepared effect manifest",
          s"batch=${bundle.batch.effectManifest} bundle=$derivedManifest"
        )
      )
      _ <- putEffectManifest(bundle.batch.effectManifest, bundle.effectManifest)
      _ <- bundle.resolvedPaths.traverse_(persistResolvedPath)
      _ <- verifyBatchArtifactClosure(bundle.batch, bundle.effectManifest)
      _ <- bundle.resolvedPaths.traverse_(path => verifyStoredPath(path.intentId, path.commitment).void)
      _ <- putCoreBatch(bundle.pointer, bundle.batch)
    } yield ()

  private def persistResolvedPath(path: ResolvedPathManifest): F[Unit] = {
    val manifest = PathManifestRef(
      path.commitment,
      ScopedArtifactRef(path.intentId, path.commitment.manifest)
    )
    putPathManifest(manifest, path.payload) >>
      path.chunks.traverse_(chunk => putPathChunk(chunk.artifact, chunk.value))
  }

  private def coreValidationContext(
    prior: Option[ReleasedCore],
    currentPublication: MptActivePublication
  ): F[CoreValidationContext] =
    prior
      .traverse(released =>
        readEffectManifest(released.payload.effectManifest).map(manifest =>
          PreviousEffectManifest(released.payload.effectManifest, manifest)
        )
      )
      .map(previous => CoreValidationContext(prior, previous, currentPublication))

  private def verifyBatchArtifactClosure(
    batch: FinalityCoreBatch,
    manifest: FinalityEffectManifest
  ): F[Unit] = {
    val artifacts =
      batch.selectionEvidence.artifact ::
        batch.qualification.evidence.artifact ::
        batch.prepared.semanticState.artifact ::
        batch.prepared.authenticatedAnchor.artifact ::
        effectCommands(manifest).map(_.payload.artifact)

    requireDomain("core batch", batch.scope.domain) >>
      requireDomain("effect manifest", manifest.scope.domain) >>
      artifacts.distinct.traverse_(pointer => readArtifact(pointer).void) >>
      corePathCommitments(batch).traverse_(commitment => verifyStoredPath(batch.intentId, commitment).void)
  }

  private def corePathCommitments(batch: FinalityCoreBatch): List[PathCommitment] = {
    val transition = batch.transition match {
      case value: CoreTransition.Advance => value.adopted.commitment :: Nil
      case value: CoreTransition.ForkChoiceReplacement =>
        value.orphaned.commitment :: value.adopted.commitment :: Nil
      case value: CoreTransition.ForkChoiceRollbackToOperationalMrca => value.orphaned.commitment :: Nil
    }
    batch.scope.selection.lineage :: transition ::: batch.qualification.ancestorClosure.toList.map(_.commitment)
  }

  private def verifyActiveStageArtifacts(active: ActiveCoreIntent): F[Unit] =
    active.stage match {
      case CoreStage.Prepared => Async[F].unit
      case CoreStage.CoreApplied(receipt) =>
        verifyReleasedReceiptArtifacts(receipt)
      case _: CoreStage.RestoringPrior | _: CoreStage.RestoredAbandoned =>
        Async[F].raiseError(RestorationProtocolNotDurable())
    }

  private def verifyReleasedReceiptArtifacts(receipt: ReleasedCoreReceipt): F[Unit] =
    List(receipt.semanticReceipt.artifact, receipt.authenticatedAnchorReceipt.artifact)
      .traverse_(pointer => readArtifact(pointer).void)

  private def verifyHeadDependencies(
    head: CoordinatorHead,
    previous: Option[DurablyVerifiedCoordinatorHead],
    verifyFullEffectLineage: Boolean
  ): F[HeadDependencies] =
    for {
      active <- head.active.traverse { active =>
        for {
          stored <- readCoreBatch(active.batch)
          located <- readCoreBatchByIntent(active.batch.intentId)
          _ <- Async[F].raiseUnless(located == stored)(
            IdentityMismatch("active core batch locator", s"head=${active.batch} locator=${located.pointer}")
          )
          _ <- Async[F].raiseUnless(
            stored.batch.intentId == active.batch.intentId && stored.batch.scope == active.scope
          )(
            IdentityMismatch("active core batch", s"head=${active.batch}/${active.scope} batch=${stored.batch}")
          )
          manifest <- readEffectManifest(stored.batch.effectManifest)
          _ <- verifyBatchArtifactClosure(stored.batch, manifest)
          _ <- verifyActiveStageArtifacts(active)
        } yield stored
      }
      released <- head.released.traverse { embedded =>
        for {
          record <- readReleasedCore(embedded.pointer)
          _ <- Async[F].raiseUnless(record == embedded)(
            IdentityMismatch("released core", s"head=$embedded stored=$record")
          )
          batch <- readCoreBatchByIntent(embedded.pointer.intentId)
          _ <- validate(FinalityIntentValidator.validateReleasedCore(batch.batch, embedded))
          manifest <- readEffectManifest(batch.batch.effectManifest)
          _ <- verifyBatchArtifactClosure(batch.batch, manifest)
          _ <- verifyReleasedReceiptArtifacts(embedded.payload.receipt)
        } yield batch
      }
      manifest <- head.effects.tail.traverse(readEffectManifest)
      _ <- verifyEffectTail(
        head.effects.tail,
        manifest,
        previous,
        verifyFullEffectLineage
      )
    } yield HeadDependencies(released, active, manifest)

  private def loadCoordinatorStartupUnlocked: F[Option[DurablyVerifiedCoordinatorHead]] =
    loadCoordinatorFromDiskUnlocked(None, verifyFullAudit = true).flatMap {
      case None          => Async[F].pure(None)
      case Some(journal) =>
        // The journal/audit chain is valid, but a Running head is not authority
        // until every current dependency is also verified. The temporary cache
        // is private to Resource acquisition and exists only so the exact-head
        // CAS can append an absorbing recovery record if dependency validation
        // fails. The store is never yielded with this unverified Running value.
        headCache.set(Some(journal)) >>
          verifyCoordinatorDependencies(journal, None, verifyFullEffectLineage = true)
            .handleErrorWith(error => recoverExpectedStartupDependencyFailure(journal, error))
            .map(_.some)
    }

  private def recoverExpectedStartupDependencyFailure(
    journal: DurablyVerifiedCoordinatorHead,
    failure: Throwable
  ): F[DurablyVerifiedCoordinatorHead] =
    failure match {
      case expected: FinalityDurableStoreError if NonFatal(expected) =>
        enterStartupDependencyRecovery(journal, expected)
      case expected: FinalityIdentityError if NonFatal(expected) =>
        enterStartupDependencyRecovery(journal, expected)
      case unexpected => Async[F].raiseError(unexpected)
    }

  private def enterStartupDependencyRecovery(
    journal: DurablyVerifiedCoordinatorHead,
    failure: Throwable
  ): F[DurablyVerifiedCoordinatorHead] =
    for {
      mutation <- Async[F].fromEither(
        FinalityCoordinatorKernel
          .enterRecovery(
            journal.value,
            RecoveryReason.StartupDependencyFailure(startupDependencyFailureDigest(journal, failure))
          )
          .leftMap(error => IdentityMismatch("startup dependency recovery mutation", error.toString))
      )
      installed <- compareAndSetCoordinator(journal, mutation)
    } yield
      installed match {
        case Installed(value)        => value
        case AlreadyInstalled(value) => value
      }

  private def startupDependencyFailureDigest(
    journal: DurablyVerifiedCoordinatorHead,
    failure: Throwable
  ): Hash = {
    val fields = List(
      "tessellation.finality.startup-dependency-failure.v1",
      journal.payloadDigest.value,
      failure.getClass.getName,
      Option(failure.getMessage).getOrElse("")
    )
    val preimage = fields.foldLeft(ByteVector.empty) { (bytes, field) =>
      val encoded = ByteVector.view(field.getBytes(StandardCharsets.UTF_8))
      val length = ByteVector.view(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(encoded.length).array())
      bytes ++ length ++ encoded
    }

    Hash.fromBytes(preimage.toArray)
  }

  private def loadCoordinatorUnlocked: F[Option[DurablyVerifiedCoordinatorHead]] =
    headCache.get.flatMap { cached =>
      loadCoordinatorFromDiskUnlocked(cached, verifyFullAudit = false)
        .flatMap(
          _.traverse(journal =>
            if (cached.contains(journal)) Async[F].pure(journal)
            else verifyCoordinatorDependencies(journal, cached, verifyFullEffectLineage = false)
          )
        )
        .flatTap(headCache.set)
    }

  private def loadCoordinatorJournalUnlocked: F[Option[DurablyVerifiedCoordinatorHead]] =
    headCache.get.flatMap(cached => loadCoordinatorFromDiskUnlocked(cached, verifyFullAudit = false))

  private def loadCoordinatorFromDiskUnlocked(
    cached: Option[DurablyVerifiedCoordinatorHead],
    verifyFullAudit: Boolean
  ): F[Option[DurablyVerifiedCoordinatorHead]] =
    for {
      marker <- readOptionalEnvelope(layout.coordinatorMarker, InitializationMarkerEnvelope, 1024L)
      raw <- readOptionalCoordinatorHead
      result <- (marker, raw) match {
        case (None, None) =>
          cached match {
            case None    => Async[F].pure(None)
            case Some(_) => Async[F].raiseError(Missing(CoordinatorHeadEnvelope, layout.coordinatorHead))
          }
        case (Some(_), None) =>
          Async[F].raiseError[Option[DurablyVerifiedCoordinatorHead]](
            Missing(CoordinatorHeadEnvelope, layout.coordinatorHead)
          )
        case (None, Some((head, payload))) =>
          verifyCoordinatorJournal(head, payload, cached, verifyFullAudit).flatTap { _ =>
            encodePayload("FinalityDomain", finalityDomainCodec, domain).flatMap { domainBytes =>
              createOrVerifyImmutable(
                layout.coordinatorMarker,
                InitializationMarkerEnvelope,
                markerPayload(domainBytes, CoordinatorMarkerSuffix),
                1024L
              )
            }
          }.map(_.some)
        case (Some(markerPayloadBytes), Some((head, payload))) =>
          verifyMarker(markerPayloadBytes, CoordinatorMarkerSuffix) >>
            verifyCoordinatorJournal(head, payload, cached, verifyFullAudit).map(_.some)
      }
    } yield result

  private def verifyCoordinatorJournal(
    head: CoordinatorHead,
    payload: Array[Byte],
    cached: Option[DurablyVerifiedCoordinatorHead],
    verifyFullAudit: Boolean
  ): F[DurablyVerifiedCoordinatorHead] = {
    val digest = Hash.fromBytes(payload)
    cached match {
      case Some(value) if value.value == head && value.payloadDigest == digest => Async[F].pure(value)
      case Some(value) if value.value == head =>
        Async[F].raiseError(
          Corrupt(CoordinatorHeadEnvelope, layout.coordinatorHead, "same decoded head has a different payload digest")
        )
      case _ =>
        val verifyAudit =
          if (verifyFullAudit) verifyAuditChain(head)
          else verifyNextAuditLink(cached, head)
        verifyAudit.map(auditTail =>
          DurablyVerifiedCoordinatorHead(
            value = head,
            payloadDigest = digest,
            audit = auditTail.currentAudit,
            recovery = auditTail.currentRecovery,
            releasedBatch = None,
            activeBatch = None,
            latestEffectManifest = None
          )
        )
    }
  }

  private def verifyCoordinatorDependencies(
    journal: DurablyVerifiedCoordinatorHead,
    previous: Option[DurablyVerifiedCoordinatorHead],
    verifyFullEffectLineage: Boolean
  ): F[DurablyVerifiedCoordinatorHead] =
    journal.value.mode match {
      case _: CoordinatorMode.RecoveryRequired => Async[F].pure(journal)
      case CoordinatorMode.Running =>
        verifyHeadDependencies(journal.value, previous, verifyFullEffectLineage).map(dependencies =>
          journal.copy(
            releasedBatch = dependencies.releasedBatch,
            activeBatch = dependencies.activeBatch,
            latestEffectManifest = dependencies.latestEffectManifest
          )
        )
    }

  private def verifyNextAuditLink(
    cached: Option[DurablyVerifiedCoordinatorHead],
    current: CoordinatorHead
  ): F[VerifiedAuditTail] =
    for {
      pointer <- current.auditTail.liftTo[F](
        Corrupt(CoordinatorHeadEnvelope, layout.coordinatorHead, "head has no immutable audit tail")
      )
      audit <- readAudit(pointer)
      _ <- rejectUndurableAuditMutation(audit.mutation)
      recovery <- current.mode match {
        case CoordinatorMode.Running                  => Async[F].pure(Option.empty[RecoveryRecord])
        case CoordinatorMode.RecoveryRequired(record) => readRecovery(record).map(_.some)
      }
      _ <- validate(
        FinalityIntentValidator.validateCoordinatorTransition(
          cached.map(_.value),
          current,
          audit,
          recovery
        )
      )
    } yield VerifiedAuditTail(audit, recovery)

  private def verifyAuditChain(current: CoordinatorHead): F[VerifiedAuditTail] = {
    final case class Scan(
      expected: CoordinatorHead,
      pointer: AuditPointer,
      seen: Long,
      currentResult: Option[VerifiedAuditTail]
    )

    current.auditTail
      .liftTo[F](Corrupt(CoordinatorHeadEnvelope, layout.coordinatorHead, "head has no immutable audit tail"))
      .flatMap { pointer =>
        Async[F].tailRecM(Scan(current, pointer, 0L, None)) { scan =>
          val attempted = BigInt(scan.seen) + 1
          for {
            _ <- Async[F].raiseWhen(attempted > BigInt(limits.maxAuditTraversalRecords))(
              AuditTraversalLimitExceeded(limits.maxAuditTraversalRecords, attempted)
            )
            audit <- readAudit(scan.pointer)
            _ <- rejectUndurableAuditMutation(audit.mutation)
            recovery <- scan.expected.mode match {
              case CoordinatorMode.Running                  => Async[F].pure(Option.empty[RecoveryRecord])
              case CoordinatorMode.RecoveryRequired(record) => readRecovery(record).map(_.some)
            }
            before = audit.before.map(commitment => CoordinatorHead.fromCommitment(commitment, audit.priorAudit))
            _ <- validate(
              FinalityIntentValidator.validateCoordinatorTransition(before, scan.expected, audit, recovery)
            )
            result = scan.currentResult.orElse(Some(VerifiedAuditTail(audit, recovery)))
            next <- (audit.before, audit.priorAudit) match {
              case (None, None) =>
                result
                  .liftTo[F](IdentityMismatch("audit chain", "verified chain produced no current record"))
                  .map(value => Right(value))
              case (Some(_), Some(priorPointer)) =>
                before
                  .liftTo[F](IdentityMismatch("audit chain", "prior commitment could not be reconstructed"))
                  .map(priorHead => Left(Scan(priorHead, priorPointer, scan.seen + 1L, result)))
              case _ =>
                Async[F].raiseError[Either[Scan, VerifiedAuditTail]](
                  IdentityMismatch(
                    "audit chain",
                    "before commitment and priorAudit must either both exist or both be absent"
                  )
                )
            }
          } yield next
        }
      }
  }

  private def rejectUndurableAuditMutation(mutation: CoordinatorMutationKind): F[Unit] =
    mutation match {
      case CoordinatorMutationKind.RestorationStarted | CoordinatorMutationKind.RestoredAbandoned |
          CoordinatorMutationKind.AbandonedRetired =>
        Async[F].raiseError(RestorationProtocolNotDurable())
      case _ => Async[F].unit
    }

  private def readOptionalCoordinatorHead: F[Option[(CoordinatorHead, Array[Byte])]] =
    readOptionalEnvelope(
      layout.coordinatorHead,
      CoordinatorHeadEnvelope,
      limits.maxCoordinatorHeadBytes
    ).flatMap(
      _.traverse(payload => decodePayload("CoordinatorHead", coordinatorHeadPayloadCodec, payload).map(_ -> payload))
    )

  private def putAudit(record: CoordinatorAuditRecord): F[AuditPointer] =
    for {
      pointer <- liftIdentity(FinalityIdentity.auditPointer(record))
      payload <- encodePayload("CoordinatorAuditRecord", coordinatorAuditRecordPayloadCodec, record)
      _ <- createOrVerifyImmutable(
        layout.audit(pointer),
        AuditRecordEnvelope,
        payload,
        limits.maxAuditRecordBytes
      )
    } yield pointer

  private def readAudit(pointer: AuditPointer): F[CoordinatorAuditRecord] =
    for {
      payload <- readRequiredEnvelope(layout.audit(pointer), AuditRecordEnvelope, limits.maxAuditRecordBytes)
      record <- decodePayload("CoordinatorAuditRecord", coordinatorAuditRecordPayloadCodec, payload)
      derived <- liftIdentity(FinalityIdentity.auditPointer(record))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("audit record", s"expected=$pointer derived=$derived")
      )
    } yield record

  private def putRecovery(record: RecoveryRecord): F[RecoveryRecordPointer] =
    for {
      pointer <- liftIdentity(FinalityIdentity.recoveryPointer(record))
      payload <- encodePayload("RecoveryRecord", recoveryRecordPayloadCodec, record)
      _ <- createOrVerifyImmutable(
        layout.recovery(pointer),
        RecoveryRecordEnvelope,
        payload,
        limits.maxRecoveryRecordBytes
      )
    } yield pointer

  private def readRecovery(pointer: RecoveryRecordPointer): F[RecoveryRecord] =
    for {
      payload <- readRequiredEnvelope(layout.recovery(pointer), RecoveryRecordEnvelope, limits.maxRecoveryRecordBytes)
      record <- decodePayload("RecoveryRecord", recoveryRecordPayloadCodec, payload)
      derived <- liftIdentity(FinalityIdentity.recoveryPointer(record))
      _ <- Async[F].raiseUnless(derived == pointer)(
        IdentityMismatch("recovery record", s"expected=$pointer derived=$derived")
      )
    } yield record

  private def replaceCoordinatorHead(head: CoordinatorHead): F[Unit] =
    for {
      payload <- encodePayload("CoordinatorHead", coordinatorHeadPayloadCodec, head)
      envelope <- encodeEnvelope(CoordinatorHeadEnvelope, payload, limits.maxCoordinatorHeadBytes)
      _ <- rejectUnsafeExisting(layout.coordinatorHead)
      _ <- atomicWriter
        .replace(
          layout.coordinatorHead,
          envelope,
          FinalityDurableWriteArtifact.CoordinatorHead(head.revision),
          writeHook
        )(path => readRequiredEnvelope(path, CoordinatorHeadEnvelope, limits.maxCoordinatorHeadBytes))
        .flatMap(readback =>
          Async[F].raiseUnless(Arrays.equals(readback, payload))(
            Corrupt(CoordinatorHeadEnvelope, layout.coordinatorHead, "verified readback differs from requested payload")
          )
        )
    } yield ()

  private def withCurrentCoordinator[A](
    expected: DurablyVerifiedCoordinatorHead,
    requireRunning: Boolean
  )(operation: F[A]): F[A] =
    headMutex.permit.use { _ =>
      loadCoordinatorUnlocked.flatMap {
        case Some(actual) if actual.payloadDigest == expected.payloadDigest && actual.value == expected.value =>
          if (requireRunning)
            actual.value.mode match {
              case CoordinatorMode.Running             => operation
              case _: CoordinatorMode.RecoveryRequired => Async[F].raiseError(RecoveryModeIsAbsorbing())
            }
          else operation
        case actual =>
          Async[F].raiseError(
            CoordinatorCompareAndSetConflict(
              Some(expected.payloadDigest),
              actual.map(_.payloadDigest)
            )
          )
      }
    }

  private def ensureCurrentCoordinator(
    expected: DurablyVerifiedCoordinatorHead,
    requireRunning: Boolean
  ): F[Unit] = withCurrentCoordinator(expected, requireRunning)(Async[F].unit)

  private def loadRawEffectOutboxUnlocked: F[Option[(FinalityEffectOutboxHead, Array[Byte])]] =
    for {
      marker <- readOptionalEnvelope(layout.effectOutboxMarker, InitializationMarkerEnvelope, 1024L)
      rawPayload <- readOptionalEnvelope(
        layout.effectOutboxHead,
        EffectOutboxHeadEnvelope,
        limits.maxEffectOutboxHeadBytes
      )
      result <- (marker, rawPayload) match {
        case (None, None) => Async[F].pure(None)
        case (Some(_), None) =>
          Async[F].raiseError[Option[(FinalityEffectOutboxHead, Array[Byte])]](
            Missing(EffectOutboxHeadEnvelope, layout.effectOutboxHead)
          )
        case (None, Some(payload)) =>
          for {
            head <- decodePayload("FinalityEffectOutboxHead", finalityEffectOutboxHeadPayloadCodec, payload)
            domainBytes <- encodePayload("FinalityDomain", finalityDomainCodec, domain)
            _ <- createOrVerifyImmutable(
              layout.effectOutboxMarker,
              InitializationMarkerEnvelope,
              markerPayload(domainBytes, EffectOutboxMarkerSuffix),
              1024L
            )
          } yield Some(head -> payload)
        case (Some(markerPayloadBytes), Some(payload)) =>
          verifyMarker(markerPayloadBytes, EffectOutboxMarkerSuffix) >>
            decodePayload("FinalityEffectOutboxHead", finalityEffectOutboxHeadPayloadCodec, payload)
              .map(head => Some(head -> payload))
      }
    } yield result

  private def loadEffectOutboxStartupUnlocked(
    coordinator: DurablyVerifiedCoordinatorHead
  ): F[Option[DurablyVerifiedEffectOutboxHead]] =
    loadRawEffectOutboxUnlocked.flatMap(
      _.traverse {
        case (head, payload) =>
          validate(FinalityIntentValidator.validateOutbox(coordinator.value, head)) >>
            verifyOutboxCursorAuthority(coordinator.value, head.cursor).as(
              DurablyVerifiedEffectOutboxHead(head, Hash.fromBytes(payload))
            )
      }
    )

  private def loadEffectOutboxUnlocked(
    coordinator: DurablyVerifiedCoordinatorHead
  ): F[Option[DurablyVerifiedEffectOutboxHead]] =
    (outboxCache.get, loadRawEffectOutboxUnlocked).tupled.flatMap {
      case (Some(_), None) => Async[F].raiseError(Missing(EffectOutboxHeadEnvelope, layout.effectOutboxHead))
      case (None, None)    => Async[F].pure(None)
      case (cached, Some((head, payload))) =>
        val digest = Hash.fromBytes(payload)
        cached match {
          case Some(value) if value.value == head && value.payloadDigest == digest =>
            Async[F].pure(Some(value))
          case Some(value) if value.value == head =>
            Async[F].raiseError(
              Corrupt(
                EffectOutboxHeadEnvelope,
                layout.effectOutboxHead,
                "same decoded outbox head has a different payload digest"
              )
            )
          case Some(value) =>
            for {
              completions <- resolveEffectCompletions(
                value.value.cursor,
                head.cursor,
                coordinator.value.effects.tail
              )
              _ <- validate(
                FinalityIntentValidator.validateOutboxTransition(
                  value.value,
                  head,
                  coordinator.value,
                  completions
                )
              )
              verified = DurablyVerifiedEffectOutboxHead(head, digest)
              _ <- outboxCache.set(Some(verified))
            } yield Some(verified)
          case None =>
            for {
              _ <- Async[F].raiseUnless(
                head.revision.value.value == 0L && head.cursor == EffectOutboxCursor(None, None)
              )(
                StructuralValidationFailed(
                  List(
                    FinalityIntentValidator.Violation(
                      "outbox",
                      "a newly observed runtime outbox must be revision zero with an empty cursor"
                    )
                  )
                )
              )
              _ <- validate(FinalityIntentValidator.validateOutbox(coordinator.value, head))
              verified = DurablyVerifiedEffectOutboxHead(head, digest)
              _ <- outboxCache.set(Some(verified))
            } yield Some(verified)
        }
    }

  private def verifyOutboxCursorAuthority(
    coordinator: CoordinatorHead,
    cursor: EffectOutboxCursor
  ): F[Unit] =
    cursor.completedThrough match {
      case None => Async[F].unit
      case Some(_) =>
        resolveEffectCompletions(
          EffectOutboxCursor(None, None),
          cursor,
          coordinator.effects.tail
        ).flatMap(completions =>
          Async[F].raiseWhen(completions.isEmpty)(
            IdentityMismatch("effect outbox cursor", "nonempty cursor has no verified completion lineage")
          )
        )
    }

  private def replaceEffectOutboxHead(head: FinalityEffectOutboxHead): F[Unit] =
    for {
      payload <- encodePayload("FinalityEffectOutboxHead", finalityEffectOutboxHeadPayloadCodec, head)
      envelope <- encodeEnvelope(
        EffectOutboxHeadEnvelope,
        payload,
        limits.maxEffectOutboxHeadBytes
      )
      _ <- rejectUnsafeExisting(layout.effectOutboxHead)
      _ <- atomicWriter
        .replace(
          layout.effectOutboxHead,
          envelope,
          FinalityDurableWriteArtifact.EffectOutboxHead(head.revision),
          writeHook
        )(path => readRequiredEnvelope(path, EffectOutboxHeadEnvelope, limits.maxEffectOutboxHeadBytes))
        .flatMap(readback =>
          Async[F].raiseUnless(Arrays.equals(readback, payload))(
            Corrupt(
              EffectOutboxHeadEnvelope,
              layout.effectOutboxHead,
              "verified readback differs from requested payload"
            )
          )
        )
    } yield ()

  private def resolveEffectCompletions(
    before: EffectOutboxCursor,
    after: EffectOutboxCursor,
    coordinatorTail: Option[EffectManifestPointer]
  ): F[List[FinalityIntentValidator.CompletedEffectManifest]] =
    if (before == after) Async[F].pure(Nil)
    else
      for {
        tail <- coordinatorTail.liftTo[F](
          IdentityMismatch("effect manifest lineage", "cursor advances without a coordinator effect tail")
        )
        beforeGeneration = before.completedThrough.fold(BigInt(-1))(value => BigInt(value.value.value))
        afterGeneration <- after.completedThrough
          .map(value => BigInt(value.value.value))
          .liftTo[F](IdentityMismatch("effect outbox cursor", "advanced cursor has no completed generation"))
        afterPointer <- after.completedManifest.liftTo[F](
          IdentityMismatch("effect outbox cursor", "advanced cursor has no completed manifest")
        )
        lineage <- walkEffectManifestLineage(tail, beforeGeneration)
        oldestFirst = lineage
        resolvedAfter <- oldestFirst
          .find(node => BigInt(node.pointer.generation.value.value) == afterGeneration)
          .liftTo[F](
            IdentityMismatch("effect manifest lineage", s"generation $afterGeneration is absent from coordinator tail")
          )
        _ <- Async[F].raiseUnless(resolvedAfter.pointer == afterPointer)(
          IdentityMismatch("effect outbox cursor", s"cursor=$afterPointer canonical=${resolvedAfter.pointer}")
        )
        _ <- before.completedManifest.traverse_ { expectedBefore =>
          oldestFirst
            .find(_.pointer.generation == expectedBefore.generation)
            .liftTo[F](
              IdentityMismatch(
                "effect manifest lineage",
                s"prior cursor ${expectedBefore.generation} is absent from coordinator tail"
              )
            )
            .flatMap(actual =>
              Async[F].raiseUnless(actual.pointer == expectedBefore)(
                IdentityMismatch("effect manifest lineage", s"prior cursor=$expectedBefore canonical=${actual.pointer}")
              )
            )
        }
        range = oldestFirst.filter { node =>
          val generation = BigInt(node.pointer.generation.value.value)
          generation > beforeGeneration && generation <= afterGeneration
        }
        completions <- range.traverse { node =>
          val commands = effectCommands(node.manifest)
          commands.traverse { command =>
            readEffectReceipt(node.pointer, command.effectId)
              .flatMap(_.liftTo[F](MissingEffectReceipt(node.pointer, command.effectId)))
          }.map(receipts => FinalityIntentValidator.CompletedEffectManifest(node.pointer, node.manifest, receipts))
        }
      } yield completions

  private def verifyEffectManifestLineage(tail: EffectManifestPointer): F[Unit] =
    walkEffectManifestLineage(tail, BigInt(-1)).flatMap(
      _.traverse_(node => verifyEffectManifestPayloads(node.manifest))
    )

  private def verifyEffectTail(
    tail: Option[EffectManifestPointer],
    manifest: Option[FinalityEffectManifest],
    previous: Option[DurablyVerifiedCoordinatorHead],
    verifyFullLineage: Boolean
  ): F[Unit] =
    if (verifyFullLineage) tail.traverse_(verifyEffectManifestLineage)
    else
      (tail, manifest, previous.flatMap(_.value.effects.tail)) match {
        case (None, None, None) => Async[F].unit
        case (None, None, Some(prior)) =>
          Async[F].raiseError(
            IdentityMismatch("effect manifest tail", s"runtime head removed prior tail $prior")
          )
        case (Some(current), Some(value), Some(prior)) if current == prior =>
          previous.flatMap(_.latestEffectManifest) match {
            case Some(previousValue) =>
              Async[F].raiseUnless(value == previousValue)(
                IdentityMismatch("effect manifest tail", "unchanged pointer decoded to different manifest bytes")
              )
            case None =>
              Async[F].raiseError(
                IdentityMismatch("effect manifest tail", "cached prior tail has no verified manifest")
              )
          }
        case (Some(current), Some(value), prior) =>
          val contiguous = prior match {
            case None => current.generation.value.value == 0L && value.previous.isEmpty
            case Some(pointer) =>
              BigInt(current.generation.value.value) == BigInt(pointer.generation.value.value) + 1 &&
              value.previous.contains(pointer)
          }
          Async[F].raiseUnless(contiguous)(
            IdentityMismatch(
              "effect manifest tail",
              s"new tail $current is not the exact successor of $prior"
            )
          ) >> verifyEffectManifestPayloads(value)
        case _ =>
          Async[F].raiseError(
            IdentityMismatch("effect manifest tail", "pointer and decoded manifest presence differ")
          )
      }

  private def verifyEffectManifestPayloads(manifest: FinalityEffectManifest): F[Unit] =
    requireDomain("effect manifest", manifest.scope.domain) >>
      effectCommands(manifest).map(_.payload.artifact).distinct.traverse_(pointer => readArtifact(pointer).void)

  private def walkEffectManifestLineage(
    tail: EffectManifestPointer,
    stopGeneration: BigInt
  ): F[List[ResolvedEffectManifest]] = {
    final case class Scan(next: EffectManifestPointer, seen: Long, accumulated: List[ResolvedEffectManifest])

    Async[F].tailRecM(Scan(tail, 0L, Nil)) { scan =>
      val attempted = BigInt(scan.seen) + 1
      for {
        _ <- Async[F].raiseWhen(attempted > BigInt(limits.maxOutboxTraversalManifests))(
          OutboxTraversalLimitExceeded(limits.maxOutboxTraversalManifests, attempted)
        )
        manifest <- readEffectManifest(scan.next)
        node = ResolvedEffectManifest(scan.next, manifest)
        generation = BigInt(scan.next.generation.value.value)
        updated = node :: scan.accumulated
        result <-
          if (generation <= stopGeneration) Async[F].pure(Right(updated))
          else
            manifest.previous match {
              case Some(previous) =>
                Async[F].raiseUnless(
                  BigInt(previous.generation.value.value) + 1 == generation
                )(
                  IdentityMismatch(
                    "effect manifest lineage",
                    s"noncontiguous link: current=${scan.next.generation} previous=${previous.generation}"
                  )
                ) >> Async[F].pure(Left(Scan(previous, scan.seen + 1L, updated)))
              case None =>
                Async[F].raiseUnless(generation == 0 && stopGeneration < 0)(
                  IdentityMismatch(
                    "effect manifest lineage",
                    s"early terminal at generation $generation before floor $stopGeneration"
                  )
                ) >> Async[F].pure(Right(updated))
            }
      } yield result
    }
  }

  private def effectCommands(manifest: FinalityEffectManifest): List[ScopedEffectCommand] =
    List(
      manifest.chainStoreProjection,
      manifest.snapshotStorageProjection,
      manifest.tipTrackerProjection,
      manifest.overlayCacheProjection,
      manifest.serviceAvailabilityWatermarkProjection,
      manifest.latestSliceProjection,
      manifest.followProjectionRing,
      manifest.accumulatorChangesetPromotion,
      manifest.signedBytesPromotion,
      manifest.sidecarOutboxReconciliation,
      manifest.shardAnchorAndWatermarkReconciliation,
      manifest.binaryConfirmationAndRequeue,
      manifest.committeeAdmissionMaintenance,
      manifest.etaCommitteeAnchorReconciliation,
      manifest.mempoolReconciliation,
      manifest.towerIndexReconciliation,
      manifest.downstreamFollowerEventEnqueue,
      manifest.snapshotRetentionPruning
    )

  private def ensureCoreBatchPointer(pointer: FinalityCoreBatchPointer, batch: FinalityCoreBatch): F[Unit] =
    for {
      derivedIntent <- liftIdentity(FinalityIdentity.intentId(batch.scope))
      _ <- requireDomain("core batch", batch.scope.domain)
      _ <- Async[F].raiseUnless(
        pointer.intentId == batch.intentId &&
          pointer.intentId == derivedIntent &&
          pointer.attempt == batch.scope.attempt &&
          pointer.generation == batch.scope.generation
      )(
        IdentityMismatch("core batch", s"pointer=$pointer batch=${batch.intentId}/${batch.scope.attempt}/${batch.scope.generation}")
      )
    } yield ()

  private def requireDomain(label: String, actual: FinalityDomain): F[Unit] =
    Async[F].raiseUnless(actual == domain)(
      IdentityMismatch(label, s"configuredDomain=$domain artifactDomain=$actual")
    )

  private def createOrVerifyImmutable(
    target: Path,
    kind: FinalityDurableEnvelopeKind,
    payload: Array[Byte],
    maxPayloadBytes: Long
  ): F[Unit] =
    for {
      envelope <- encodeEnvelope(kind, payload, maxPayloadBytes)
      _ <- ensureDirectory(target.getParent)
      existing <- targetStatus(target)
      _ <- existing match {
        case TargetStatus.Missing          => installImmutable(target, kind, payload, envelope, maxPayloadBytes)
        case TargetStatus.Regular          => verifyImmutable(target, kind, payload, maxPayloadBytes)
        case TargetStatus.Unsafe(fileType) => Async[F].raiseError(UnsafeFile(target, fileType))
      }
    } yield ()

  private def installImmutable(
    target: Path,
    kind: FinalityDurableEnvelopeKind,
    payload: Array[Byte],
    envelope: Array[Byte],
    maxPayloadBytes: Long
  ): F[Unit] = {
    val artifact = FinalityDurableWriteArtifact.Immutable(target)
    Resource
      .make(fileOps.createTempFile(target.getParent, s".${target.getFileName.toString}.", ".tmp"))(fileOps.deleteIfExists)
      .use { temporary =>
        val prepare =
          at(artifact, Write)(fileOps.write(temporary, envelope)) >>
            at(artifact, ForceFile)(fileOps.forceFile(temporary))

        val commit = Async[F].uncancelable { _ =>
          at(artifact, AtomicMove)(
            Async[F].blocking(Files.createLink(target, temporary)).void
          ).attempt.flatMap {
            case Right(_) =>
              at(artifact, ForceDirectory)(fileOps.forceDirectory(target.getParent)) >>
                at(artifact, ReadBack)(verifyImmutable(target, kind, payload, maxPayloadBytes))
            case Left(_: FileAlreadyExistsException) => verifyImmutable(target, kind, payload, maxPayloadBytes)
            case Left(error)                         => Async[F].raiseError[Unit](AtomicImmutableInstallRequired(target, error))
          }
        }

        prepare >> commit
      }
  }

  private def verifyImmutable(
    target: Path,
    kind: FinalityDurableEnvelopeKind,
    payload: Array[Byte],
    maxPayloadBytes: Long
  ): F[Unit] =
    readRequiredEnvelope(target, kind, maxPayloadBytes).flatMap { actual =>
      Async[F].raiseUnless(Arrays.equals(actual, payload))(ImmutableLocatorConflict(target))
    }

  private def readRequiredEnvelope(
    path: Path,
    kind: FinalityDurableEnvelopeKind,
    maxPayloadBytes: Long
  ): F[Array[Byte]] =
    readOptionalEnvelope(path, kind, maxPayloadBytes).flatMap(_.liftTo[F](Missing(kind, path)))

  private def readOptionalEnvelope(
    path: Path,
    kind: FinalityDurableEnvelopeKind,
    maxPayloadBytes: Long
  ): F[Option[Array[Byte]]] =
    targetStatus(path).flatMap {
      case TargetStatus.Missing          => Async[F].pure(None)
      case TargetStatus.Unsafe(fileType) => Async[F].raiseError(UnsafeFile(path, fileType))
      case TargetStatus.Regular =>
        Async[F].blocking {
          val channel = Files.newByteChannel(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
          val input = Channels.newInputStream(channel)
          try FinalityDurableEnvelope.decode(input, kind, maxPayloadBytes)
          finally input.close()
        }.flatMap {
          case Right(decoded) => Async[F].pure[Option[Array[Byte]]](Some(decoded.payload))
          case Left(error) =>
            Async[F].raiseError[Option[Array[Byte]]](Corrupt(kind, path, error.getMessage, error))
        }.adaptError {
          case error: FinalityDurableStoreError => error
          case _: NoSuchFileException           => Missing(kind, path)
          case NonFatal(error)                  => Corrupt(kind, path, error.getMessage, error)
        }
    }

  private def targetStatus(path: Path): F[TargetStatus] =
    Async[F].blocking {
      if (!path.toAbsolutePath.normalize().startsWith(layout.normalizedRoot)) throw UnsafePath(path)
      FinalityDurableStore.verifyExistingDirectoryComponents(path.getParent)
      if (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) TargetStatus.Missing
      else {
        val attributes = Files.readAttributes(path, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS)
        if (Files.isSymbolicLink(path)) TargetStatus.Unsafe("symbolic-link")
        else if (attributes.isRegularFile) TargetStatus.Regular
        else if (attributes.isDirectory) TargetStatus.Unsafe("directory")
        else TargetStatus.Unsafe("special-file")
      }
    }

  private def rejectUnsafeExisting(path: Path): F[Unit] =
    targetStatus(path).flatMap {
      case TargetStatus.Missing | TargetStatus.Regular => Async[F].unit
      case TargetStatus.Unsafe(fileType)               => Async[F].raiseError(UnsafeFile(path, fileType))
    }

  private def ensureDirectories(paths: List[Path]): F[Unit] = paths.traverse_(ensureDirectory)

  private def ensureDirectory(path: Path): F[Unit] =
    for {
      _ <- Async[F].raiseUnless(path.toAbsolutePath.normalize().startsWith(layout.normalizedRoot))(UnsafePath(path))
      _ <- Async[F].blocking(FinalityDurableStore.verifyExistingDirectoryComponents(path.getParent))
      _ <- fileOps.createDirectories(path)
      _ <- Async[F].blocking {
        FinalityDurableStore.verifyExistingDirectoryComponents(path)
        if (Files.isSymbolicLink(path) || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS))
          throw UnsafeDirectory(path)
      }
    } yield ()

  private def verifyDirectoryTree: F[Unit] =
    (layout.normalizedRoot :: layout.directories).traverse_(ensureDirectory)

  private def markerPayload(domainBytes: Array[Byte], suffix: Byte): Array[Byte] = {
    val payload = Arrays.copyOf(domainBytes, domainBytes.length + 1)
    payload(domainBytes.length) = suffix
    payload
  }

  private def verifyMarker(payload: Array[Byte], suffix: Byte): F[Unit] =
    for {
      domainBytes <- encodePayload("FinalityDomain", finalityDomainCodec, domain)
      expected = markerPayload(domainBytes, suffix)
      _ <- Async[F].raiseUnless(Arrays.equals(payload, expected))(
        IdentityMismatch("initialization marker domain", "network/genesis/protocol-era or marker role differs")
      )
    } yield ()

  private def encodeEnvelope(
    kind: FinalityDurableEnvelopeKind,
    payload: Array[Byte],
    maxPayloadBytes: Long
  ): F[Array[Byte]] =
    Async[F].fromEither(FinalityDurableEnvelope.encode(kind, payload, maxPayloadBytes))

  private def encodePayload[A](valueType: String, codec: Codec[A], value: A): F[Array[Byte]] =
    Async[F].fromEither(FinalityDurableEnvelope.encodePayload(valueType, codec, value))

  private def decodePayload[A](valueType: String, codec: Codec[A], payload: Array[Byte]): F[A] =
    Async[F].fromEither(FinalityDurableEnvelope.decodePayload(valueType, codec, payload))

  private def liftIdentity[A](result: Either[FinalityIdentityError, A]): F[A] =
    Async[F].fromEither(result)

  private def validate[A](result: FinalityIntentValidator.ValidationResult[A]): F[A] =
    result.toEither.leftMap(violations => StructuralValidationFailed(violations.toNonEmptyList.toList)).liftTo[F]

  private def at[A](
    artifact: FinalityDurableWriteArtifact,
    stage: DurableWriteStage
  )(operation: F[A]): F[A] =
    writeHook.onEvent(DurableWriteEvent(artifact, stage, Before)) >>
      operation.flatTap(_ => writeHook.onEvent(DurableWriteEvent(artifact, stage, After)))
  def compareAndSetEffectOutbox(
    coordinator: DurablyVerifiedCoordinatorHead,
    expected: DurablyVerifiedEffectOutboxHead,
    next: FinalityEffectOutboxHead
  ): F[FinalityDurableCasResult[DurablyVerifiedEffectOutboxHead]] =
    outboxMutex.permit.use { _ =>
      ensureCurrentCoordinator(coordinator, requireRunning = true) >>
        loadEffectOutboxUnlocked(coordinator).flatMap {
          case Some(actual) if actual.value == next => Async[F].pure(AlreadyInstalled(actual))
          case Some(actual) if actual.payloadDigest == expected.payloadDigest && actual.value == expected.value =>
            for {
              completions <- resolveEffectCompletions(
                expected.value.cursor,
                next.cursor,
                coordinator.value.effects.tail
              )
              _ <- validate(
                FinalityIntentValidator.validateOutboxTransition(
                  expected.value,
                  next,
                  coordinator.value,
                  completions
                )
              )
              _ <- withCurrentCoordinator(coordinator, requireRunning = true)(replaceEffectOutboxHead(next))
              loaded <- loadEffectOutboxUnlocked(coordinator).flatMap(_.liftTo[F](EffectOutboxNotInitialized()))
            } yield Installed(loaded)
          case actual =>
            Async[F].raiseError(
              EffectOutboxCompareAndSetConflict(
                Some(expected.payloadDigest),
                actual.map(_.payloadDigest)
              )
            )
        }
    }
}
