package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.io.InputStream
import java.nio.file.{Files => NioFiles, _}

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs.{pathChunkPayloadCodec, pathManifestPayloadCodec}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCodecFixtures._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorCodecs.coordinatorAuditRecordPayloadCodec
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableCasResult.{AlreadyInstalled, Installed}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableEnvelopeKind.ImmutableArtifact
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityDurableStoreError._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityIntentValidator._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.storage.durable._

import fs2.io.file.Files
import scodec.bits.ByteVector
import shapeless.test.illTyped
import weaver.SimpleIOSuite

object FinalityDurableStoreSuite extends SimpleIOSuite {

  private final case class StoredPathFixture(
    intentId: IntentId,
    commitment: PathCommitment,
    chunks: List[(PathChunk, Array[Byte])]
  )

  private final case class RawArtifact(pointer: ImmutableArtifactPointer, bytes: ByteVector)

  private final case class EconomicFixture(
    targetSeed: Int,
    batch: FinalityCoreBatch,
    manifest: FinalityEffectManifest,
    paths: List[ResolvedPathManifest],
    context: CoreValidationContext,
    appliedReceipt: ReleasedCoreReceipt,
    released: ReleasedCore,
    rawArtifacts: List[RawArtifact],
    terminalReceipts: List[TerminalEffectReceipt]
  )

  private case object SimulatedCrash extends RuntimeException("simulated durable-write crash")

  private def kernel[A](result: Either[FinalityCoordinatorKernelError, A]): A =
    result.fold(error => throw new IllegalStateException(error.toString), identity)

  private val mptCodecEra = MptImageCodecEra(hash(47))
  private val mptRootEra = MptImageRootEra(hash(48))
  private val mptReadLimits = MptImageReadLimits
    .from(maxImageBytes = 1024L * 1024L, maxEntries = 1000, maxKeyBytes = 1024, maxValueBytes = 256 * 1024)
    .fold(throw _, identity)
  private val mptRootVerifier = new MptImageRootVerifier[IO] {
    val rootEra: MptImageRootEra = mptRootEra
    def rebuild(entries: Vector[(Hex, ByteVector)]): IO[MptRoot] = IO.pure(MptRoot(hash(49)))
  }

  private def initializeCoordinator(durable: FinalityDurableStore[IO]): IO[DurablyVerifiedCoordinatorHead] =
    Files[IO].tempDirectory.use { directory =>
      DurableMptImageStore
        .resource[IO](directory.toNioPath, mptCodecEra, mptRootVerifier, mptReadLimits)
        .use { mpt =>
          mpt.initializeActivePublication(None) >>
            mpt.withVerifiedActivePublication(FinalityCoordinatorKernel.initializeDurably(_, durable)) >>
            durable.coordinatorHead.map(_.get)
        }
    }

  private def casValue[A](result: FinalityDurableCasResult[A]): A =
    result match {
      case Installed(value)        => value
      case AlreadyInstalled(value) => value
    }

  private def errorIs[A <: Throwable: ClassTag](result: Either[Throwable, _]): Boolean =
    result.left.toOption.exists(implicitly[ClassTag[A]].runtimeClass.isInstance)

  private def store(
    root: Path,
    domain: FinalityDomain = finalityDomain,
    limits: FinalityDurableStoreLimits = FinalityDurableStoreLimits.default,
    hook: DurableWriteHook[IO, FinalityDurableWriteArtifact] = DurableWriteHook.noop[IO, FinalityDurableWriteArtifact]
  ) =
    FinalityDurableStore.make[IO](root, domain, limits, DurableFileOps.nio[IO], hook)

  private def persistedFiles(root: Path): IO[Map[String, ByteVector]] =
    IO.blocking {
      if (!NioFiles.exists(root, LinkOption.NOFOLLOW_LINKS)) Map.empty
      else {
        val paths = NioFiles.walk(root)
        try
          paths
            .iterator()
            .asScala
            .filter(path =>
              NioFiles.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) &&
                path.getFileName.toString != ".lock"
            )
            .map(path => root.relativize(path).toString -> ByteVector.view(NioFiles.readAllBytes(path)))
            .toMap
        finally paths.close()
      }
    }

  private def auditPath(root: Path, pointer: AuditPointer): Path =
    root.resolve("audit").resolve(s"${pointer.id.value.value}.bin")

  private def recoveryPath(root: Path, pointer: RecoveryRecordPointer): Path =
    root.resolve("recovery").resolve(s"${pointer.id.value.value}.bin")

  private def coreBatchArtifactPath(root: Path, pointer: FinalityCoreBatchPointer): Path =
    root
      .resolve("immutable")
      .resolve("01-core-batch")
      .resolve(s"${pointer.artifact.id.value.value}.bin")

  private def batchLocatorPath(root: Path, intentId: IntentId): Path =
    root.resolve("batch-by-intent").resolve(s"${intentId.value.value}.bin")

  private def effectManifestPath(root: Path, pointer: EffectManifestPointer): Path =
    root.resolve("effect-manifests").resolve(s"${pointer.id.value.value}.bin")

  private def effectOutboxHeadPath(root: Path): Path =
    root.resolve("effects.head")

  private def artifactPath(root: Path, pointer: ImmutableArtifactPointer): Path =
    root
      .resolve("immutable")
      .resolve("14-effect-payload")
      .resolve(s"${pointer.id.value.value}.bin")

  private def pathLocator(root: Path, intentId: IntentId, manifestId: ArtifactId, index: Long): Path =
    root
      .resolve("path-chunks")
      .resolve(intentId.value.value)
      .resolve(manifestId.value.value)
      .resolve(f"$index%019d.bin")

  private def encodePayload[A](name: String, codec: scodec.Codec[A], value: A): Array[Byte] =
    FinalityDurableEnvelope.encodePayload(name, codec, value).fold(throw _, identity)

  private def envelope(payload: Array[Byte]): Array[Byte] =
    FinalityDurableEnvelope
      .encode(ImmutableArtifact, payload, FinalityDurableStoreLimits.default.maxImmutableArtifactBytes)
      .fold(throw _, identity)

  private def writeLocator(path: Path, payload: Array[Byte]): IO[Unit] =
    IO.blocking {
      NioFiles.createDirectories(path.getParent)
      NioFiles.write(path, envelope(payload), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      ()
    }

  private def rawArtifact(kind: FinalityArtifactKind, seed: Int): RawArtifact = {
    val bytes = ByteVector.fromValidHex(f"$seed%08x")
    val pointer = FinalityIdentity.artifactPointerFromBytes(kind, bytes).fold(throw _, identity)
    RawArtifact(pointer, bytes)
  }

  private def path(
    role: PathRole,
    entries: NonEmptyList[io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef]
  ): (PathCommitment, NonEmptyList[io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef]) = {
    val root = FinalityIdentity.pathEntriesRoot(entries.toList).fold(throw _, identity)
    val summary = PathSummary(role, entries.head, entries.last, nonNeg(entries.size.toLong))
    val payload = PathManifestPayload(summary, root)
    val manifest = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathManifest, pathManifestPayloadCodec, payload)
      .fold(throw _, identity)

    PathCommitment(summary, manifest, root) -> entries
  }

  private def resolvePath(
    intentId: IntentId,
    fixture: (PathCommitment, NonEmptyList[io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef])
  ): ResolvedPathManifest = {
    val (commitment, entries) = fixture
    val chunk = PathChunk(intentId, commitment.manifest.id, nonNeg(0L), entries, None)
    val chunkArtifact = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathChunk, pathChunkPayloadCodec, chunk)
      .fold(throw _, identity)

    ResolvedPathManifest(
      intentId,
      commitment,
      commitment.payload,
      List(ResolvedPathChunk(chunkArtifact, chunk))
    )
  }

  private def effectPlan(commands: List[EffectCommandCommitment]): EffectPlanCommitment =
    EffectPlanCommitment(
      previous = commands.headOption.flatMap(_ => None),
      chainStoreProjection = commands(0),
      snapshotStorageProjection = commands(1),
      tipTrackerProjection = commands(2),
      overlayCacheProjection = commands(3),
      serviceAvailabilityWatermarkProjection = commands(4),
      latestSliceProjection = commands(5),
      followProjectionRing = commands(6),
      accumulatorChangesetPromotion = commands(7),
      signedBytesPromotion = commands(8),
      sidecarOutboxReconciliation = commands(9),
      shardAnchorAndWatermarkReconciliation = commands(10),
      binaryConfirmationAndRequeue = commands(11),
      committeeAdmissionMaintenance = commands(12),
      etaCommitteeAnchorReconciliation = commands(13),
      mempoolReconciliation = commands(14),
      towerIndexReconciliation = commands(15),
      downstreamFollowerEventEnqueue = commands(16),
      snapshotRetentionPruning = commands(17)
    )

  private def effectManifest(
    scope: EffectScope,
    previous: Option[EffectManifestPointer],
    commands: List[ScopedEffectCommand]
  ): FinalityEffectManifest =
    FinalityEffectManifest(
      scope = scope,
      previous = previous,
      chainStoreProjection = commands(0),
      snapshotStorageProjection = commands(1),
      tipTrackerProjection = commands(2),
      overlayCacheProjection = commands(3),
      serviceAvailabilityWatermarkProjection = commands(4),
      latestSliceProjection = commands(5),
      followProjectionRing = commands(6),
      accumulatorChangesetPromotion = commands(7),
      signedBytesPromotion = commands(8),
      sidecarOutboxReconciliation = commands(9),
      shardAnchorAndWatermarkReconciliation = commands(10),
      binaryConfirmationAndRequeue = commands(11),
      committeeAdmissionMaintenance = commands(12),
      etaCommitteeAnchorReconciliation = commands(13),
      mempoolReconciliation = commands(14),
      towerIndexReconciliation = commands(15),
      downstreamFollowerEventEnqueue = commands(16),
      snapshotRetentionPruning = commands(17)
    )

  private def economicFixture(prior: Option[EconomicFixture] = None): EconomicFixture = {
    val generationNumber = prior.fold(0L)(value => value.batch.scope.generation.value.value + 1L)
    val seedBase = 2000 + generationNumber.toInt * 1000
    val targetSeed = seedBase + 20
    val target = state(
      generationNumber + 1L,
      targetSeed,
      prior.fold(targetSeed - 1)(_.targetSeed)
    )
    val generation = ReleaseGeneration(nonNeg(generationNumber))
    val attempt = IntentAttempt(nonNeg(generationNumber))
    val lineage = path(PathRole.CanonicalLineage, NonEmptyList.one(target))
    val adopted = path(PathRole.Adopted, NonEmptyList.one(target))
    val selectionEvidence = rawArtifact(FinalityArtifactKind.ForkChoiceDecisionEvidence, seedBase + 100)
    val qualificationEvidence = rawArtifact(FinalityArtifactKind.DecidedAttestationEvidence, seedBase + 101)
    val semanticState = rawArtifact(FinalityArtifactKind.PreparedSemanticState, seedBase + 102)
    val authenticatedAnchor = rawArtifact(FinalityArtifactKind.AuthenticatedTargetAnchor, seedBase + 103)
    val image = MptImageReceipt(
      formatVersion = DurableMptImageStore.CurrentFormatVersion,
      generation = generationNumber,
      imageId = MptImageId(hash(seedBase + 110)),
      anchor = target,
      codecEra = MptImageCodecEra(hash(seedBase + 111)),
      rootEra = MptImageRootEra(hash(seedBase + 112)),
      digest = MptImageDigest(hash(seedBase + 113)),
      entryCount = 1
    )
    val expectedPublication = prior
      .map(_.released.payload.receipt.activePublication)
      .getOrElse(MptActivePublication(MptPublicationRevision(0L), None))
    val targetPublication = MptActivePublication(
      MptPublicationRevision(expectedPublication.revision.value + 1L),
      Some(image)
    )
    val preparedCommitment = PreparedCoreCommitment(
      target,
      image,
      expectedPublication,
      targetPublication,
      semanticState.pointer,
      authenticatedAnchor.pointer
    )
    val transitionShape = TransitionShape.Advance(adopted._1)
    val transitionDigest = FinalityIdentity.transitionDigest(transitionShape).fold(throw _, identity)
    val previousCommands = prior.map(value => commands(value.manifest))
    val commandData = FinalityCodecFixtures.effectKinds.zipWithIndex.map {
      case (kind, index) =>
        val seed = seedBase + 200 + index * 10
        val previous = previousCommands.flatMap(_.lift(index))
        val expectedBefore = previous.map(_.desiredAfter).getOrElse(EffectStateDigest(hash(seed + 2)))
        val expectedRevision = previous.map(_.desiredRevision).getOrElse(EffectSinkRevision(nonNeg(0L)))
        val desiredAfter = EffectStateDigest(hash(seed + 3))
        val desiredRevision = EffectSinkRevision(nonNeg(expectedRevision.value.value + 1L))
        val payload = rawArtifact(FinalityArtifactKind.EffectPayload, seed + 4)
        val identityPreimage = EffectCommandIdentity(
          finalityDomain,
          generation,
          prior.map(_.batch.scope.target),
          transitionDigest,
          target,
          kind,
          expectedBefore,
          expectedRevision,
          desiredAfter,
          desiredRevision,
          payload.pointer,
          previous.map(_.effectId)
        )
        val commitment = EffectCommandCommitment(
          kind,
          FinalityIdentity.effectId(identityPreimage).fold(throw _, identity),
          expectedBefore,
          expectedRevision,
          desiredAfter,
          desiredRevision,
          payload.pointer,
          previous.map(_.effectId)
        )
        commitment -> payload
    }
    val commandCommitments = commandData.map(_._1)
    val previousManifest = prior.map(value => FinalityIdentity.effectManifestPointer(value.manifest).fold(throw _, identity))
    val plan = effectPlan(commandCommitments).copy(previous = previousManifest)
    val scope = IntentScope(
      domain = finalityDomain,
      generation = generation,
      attempt = attempt,
      expectedPrior = prior.map(_.released.pointer),
      selection = CanonicalSelectionToken(
        CanonicalBranchRevision(nonNeg(generationNumber + 1L)),
        target,
        target,
        ForkChoiceDecision(selectionEvidence.pointer),
        lineage._1
      ),
      transition = transitionShape,
      qualification = OperationalQualificationScope(
        OperationalRail.DecidedAttestationTWeight,
        target,
        target,
        None,
        qualificationEvidence.pointer
      ),
      prepared = preparedCommitment,
      effects = plan,
      target = target
    )
    val intentId = FinalityIdentity.intentId(scope).fold(throw _, identity)
    val effectScope = EffectScope(
      finalityDomain,
      intentId,
      generation,
      prior.map(_.batch.scope.target),
      transitionDigest,
      target
    )
    val scopedCommands = commandCommitments.map { command =>
      ScopedEffectCommand(
        effectScope,
        command.kind,
        command.effectId,
        command.expectedBefore,
        command.expectedRevision,
        command.desiredAfter,
        command.desiredRevision,
        ScopedArtifactRef(intentId, command.payload),
        command.predecessor
      )
    }
    val manifest = effectManifest(effectScope, previousManifest, scopedCommands)
    val manifestPointer = FinalityIdentity.effectManifestPointer(manifest).fold(throw _, identity)
    val qualification = OperationalQualification.DecidedAttestationTWeight(
      target,
      target,
      None,
      ScopedArtifactRef(intentId, qualificationEvidence.pointer)
    )
    val prepared = PreparedCoreTarget(
      target,
      image,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, semanticState.pointer),
      ScopedArtifactRef(intentId, authenticatedAnchor.pointer)
    )
    val batch = FinalityCoreBatch(
      intentId,
      scope,
      ScopedArtifactRef(intentId, selectionEvidence.pointer),
      CoreTransition.Advance(
        PathManifestRef(adopted._1, ScopedArtifactRef(intentId, adopted._1.manifest))
      ),
      qualification,
      prepared,
      manifestPointer
    )
    val semanticReceipt = rawArtifact(FinalityArtifactKind.AppliedSemanticStateReceipt, seedBase + 900)
    val anchorReceipt = rawArtifact(FinalityArtifactKind.AuthenticatedAnchorReceipt, seedBase + 901)
    val appliedReceipt = ReleasedCoreReceipt(
      intentId,
      generation,
      target,
      expectedPublication,
      targetPublication,
      ScopedArtifactRef(intentId, semanticReceipt.pointer),
      ScopedArtifactRef(intentId, anchorReceipt.pointer)
    )
    val releasedPayload = ReleasedCoreRecordPayload(qualification, appliedReceipt, manifestPointer)
    val releasedArtifact = FinalityIdentity
      .artifactPointer(
        FinalityArtifactKind.ReleasedCoreRecord,
        FinalityCoreCodecs.releasedCoreRecordPayloadCodec,
        releasedPayload
      )
      .fold(throw _, identity)
    val releasedPointer = ReleasedCorePointer(generation, intentId, target, releasedArtifact)
    val released = ReleasedCore(
      releasedPointer,
      ScopedArtifactRef(intentId, releasedArtifact),
      releasedPayload
    )
    val terminalReceipts = scopedCommands.map { command =>
      AppliedEffectReceipt(
        manifestPointer,
        command.effectId,
        command.expectedBefore,
        command.expectedRevision,
        command.desiredAfter,
        command.desiredRevision
      ): TerminalEffectReceipt
    }
    val rawArtifacts =
      List(selectionEvidence, qualificationEvidence, semanticState, authenticatedAnchor) ++
        commandData.map(_._2) ++ List(semanticReceipt, anchorReceipt)
    val context = CoreValidationContext(
      prior.map(_.released),
      prior.map(value => PreviousEffectManifest(previousManifest.get, value.manifest)),
      expectedPublication
    )

    EconomicFixture(
      targetSeed,
      batch,
      manifest,
      List(resolvePath(intentId, lineage), resolvePath(intentId, adopted)),
      context,
      appliedReceipt,
      released,
      rawArtifacts,
      terminalReceipts
    )
  }

  private def commands(manifest: FinalityEffectManifest): List[ScopedEffectCommand] =
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

  private def putRawArtifacts(
    durable: FinalityDurableStore[IO],
    fixture: EconomicFixture
  ): IO[Unit] =
    fixture.rawArtifacts.traverse_(artifact => durable.putArtifact(artifact.pointer, artifact.bytes))

  private def prepareFixture(
    durable: FinalityDurableStore[IO],
    before: DurablyVerifiedCoordinatorHead,
    fixture: EconomicFixture
  ): IO[DurablyVerifiedCoordinatorHead] =
    for {
      _ <- putRawArtifacts(durable, fixture)
      mutation = kernel(
        FinalityCoordinatorKernel.prepare(
          before.value,
          fixture.batch,
          fixture.manifest,
          fixture.paths,
          fixture.context
        )
      )
      prepared <- durable.compareAndSetCoordinator(before, mutation)
    } yield casValue(prepared)

  private def multiChunkPath: StoredPathFixture = {
    val intentId = IntentId(hash(700))
    val entries = (0 to PathChunk.MaxEntries).toList.map { index =>
      val seed = 1000 + index
      state(index.toLong + 1L, seed, seed - 1)
    }
    val summary = PathSummary(
      PathRole.CanonicalLineage,
      entries.head,
      entries.last,
      nonNeg(entries.size.toLong)
    )
    val entriesRoot = FinalityIdentity.pathEntriesRoot(entries).fold(throw _, identity)
    val manifestPayload = PathManifestPayload(summary, entriesRoot)
    val manifest = FinalityIdentity
      .artifactPointer(FinalityArtifactKind.PathManifest, pathManifestPayloadCodec, manifestPayload)
      .fold(throw _, identity)
    val commitment = PathCommitment(summary, manifest, entriesRoot)
    val terminal = PathChunk(
      intentId,
      manifest.id,
      nonNeg(1L),
      NonEmptyList.one(entries.last),
      None
    )
    val terminalBytes = encodePayload("PathChunk", pathChunkPayloadCodec, terminal)
    val terminalPointer = FinalityIdentity
      .artifactPointerFromBytes(FinalityArtifactKind.PathChunk, ByteVector.view(terminalBytes))
      .fold(throw _, identity)
    val first = PathChunk(
      intentId,
      manifest.id,
      nonNeg(0L),
      NonEmptyList.fromListUnsafe(entries.take(PathChunk.MaxEntries)),
      Some(PathChunkPointer(intentId, manifest.id, nonNeg(1L), terminalPointer))
    )
    val firstBytes = encodePayload("PathChunk", pathChunkPayloadCodec, first)

    StoredPathFixture(intentId, commitment, List(first -> firstBytes, terminal -> terminalBytes))
  }

  test("configured domain mismatch fails without mutating persisted store bytes") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val wrongDomain = finalityDomain.copy(networkId = hash(999))

      for {
        _ <- store(root).use(_ => IO.unit)
        before <- persistedFiles(root)
        reopened <- store(root, wrongDomain).use(_ => IO.unit).attempt
        after <- persistedFiles(root)
      } yield
        expect.all(
          errorIs[IdentityMismatch](reopened),
          before == after
        )
    }
  }

  test("missing store marker on a non-pristine store fails closed and is not recreated") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val bytes = ByteVector.fromValidHex("01020304")
      val pointer = FinalityIdentity
        .artifactPointerFromBytes(FinalityArtifactKind.EffectPayload, bytes)
        .fold(throw _, identity)
      val marker = root.resolve("store.initialized")

      for {
        _ <- store(root).use(_.putArtifact(pointer, bytes))
        _ <- IO.blocking(NioFiles.delete(marker))
        before <- persistedFiles(root)
        reopened <- store(root).use(_ => IO.unit).attempt
        after <- persistedFiles(root)
        markerExists <- IO.blocking(NioFiles.exists(marker, LinkOption.NOFOLLOW_LINKS))
      } yield
        expect.all(
          errorIs[Missing](reopened),
          !markerExists,
          before == after
        )
    }
  }

  test("content-addressed locator publishers are absent from the public store API") {
    IO {
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putCoreBatch(null, null)""")
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putPathManifest(null, null)""")
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putPathChunk(null, null)""")
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putEffectManifest(null, null)""")
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putReleasedCore(null)""")
      illTyped("""null.asInstanceOf[FinalityDurableStore[IO]].putEffectReceipt(null)""")
      illTyped("""FinalityCoordinatorKernel.coreApplied(null, null)""")
      illTyped("""FinalityCoordinatorKernel.release(null, null, null)""")
      expect(true)
    }
  }

  test("a valid Prepared dependency closure reconstructs exactly after restart") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val fixture = economicFixture()

      for {
        expected <- store(root).use { durable =>
          for {
            initial <- initializeCoordinator(durable)
            prepared <- prepareFixture(durable, initial, fixture)
          } yield prepared
        }
        restored <- store(root).use(_.coordinatorHead.map(_.get))
      } yield
        expect.all(
          expected.value.active.exists(_.batch.intentId == fixture.batch.intentId),
          expected.activeBatch.exists(_.batch == fixture.batch),
          expected.latestEffectManifest.isEmpty,
          restored.value == expected.value,
          restored.payloadDigest == expected.payloadDigest,
          restored.activeBatch == expected.activeBatch,
          restored.latestEffectManifest == expected.latestEffectManifest
        )
    }
  }

  test("first-arrival path and batch locator poisoning cannot install a Prepared head") {
    def poisonPath(root: Path): IO[Boolean] = {
      val fixture = economicFixture()
      val resolved = fixture.paths.head
      val original = resolved.chunks.head.value
      val poisoned = PathChunk(
        IntentId(hash(810)),
        original.manifestId,
        original.chunkIndex,
        original.entriesOldestFirst,
        original.next
      )
      val poisonedBytes = encodePayload("PathChunk", pathChunkPayloadCodec, poisoned)

      store(root).use { durable =>
        for {
          initial <- initializeCoordinator(durable)
          _ <- putRawArtifacts(durable, fixture)
          _ <- writeLocator(
            pathLocator(root, fixture.batch.intentId, resolved.commitment.manifest.id, 0L),
            poisonedBytes
          )
          mutation = kernel(
            FinalityCoordinatorKernel.prepare(
              initial.value,
              fixture.batch,
              fixture.manifest,
              fixture.paths,
              fixture.context
            )
          )
          result <- durable.compareAndSetCoordinator(initial, mutation).attempt
          current <- durable.coordinatorHead
        } yield errorIs[ImmutableLocatorConflict](result) && current.exists(_.value == initial.value)
      }
    }

    def poisonBatch(root: Path): IO[Boolean] = {
      val fixture = economicFixture()
      val other = economicFixture(Some(fixture))
      val wrongPayload = encodePayload(
        "FinalityCoreBatch",
        FinalityCoreCodecs.finalityCoreBatchPayloadCodec,
        other.batch
      )

      store(root).use { durable =>
        for {
          initial <- initializeCoordinator(durable)
          _ <- putRawArtifacts(durable, fixture)
          _ <- writeLocator(batchLocatorPath(root, fixture.batch.intentId), wrongPayload)
          mutation = kernel(
            FinalityCoordinatorKernel.prepare(
              initial.value,
              fixture.batch,
              fixture.manifest,
              fixture.paths,
              fixture.context
            )
          )
          result <- durable.compareAndSetCoordinator(initial, mutation).attempt
          current <- durable.coordinatorHead
        } yield errorIs[ImmutableLocatorConflict](result) && current.exists(_.value == initial.value)
      }
    }

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      for {
        pathRejected <- poisonPath(base.resolve("path"))
        batchRejected <- poisonBatch(base.resolve("batch"))
      } yield expect.all(pathRejected, batchRejected)
    }
  }

  test("coordinator CAS rejects a stale exact head while initialization remains idempotent") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")

      store(root).use { durable =>
        for {
          installed <- initializeCoordinator(durable)
          retry <- initializeCoordinator(durable)
          emptyOutbox = FinalityEffectOutboxHead(
            EffectOutboxRevision(nonNeg(0L)),
            EffectOutboxCursor(None, None)
          )
          _ <- durable.initializeEffectOutbox(installed, emptyOutbox)
          outboxRetry <- durable.initializeEffectOutbox(installed, emptyOutbox)
          recoveryA = kernel(
            FinalityCoordinatorKernel.enterRecovery(
              installed.value,
              RecoveryReason.JournalCorruption(hash(710))
            )
          )
          recoveryB = kernel(
            FinalityCoordinatorKernel.enterRecovery(
              installed.value,
              RecoveryReason.JournalCorruption(hash(711))
            )
          )
          installedRecovery <- durable.compareAndSetCoordinator(installed, recoveryA)
          rejectedCoordinatorCas <- durable.compareAndSetCoordinator(installed, recoveryB).attempt
        } yield
          expect.all(
            retry == installed,
            outboxRetry.isInstanceOf[AlreadyInstalled[_]],
            installedRecovery.isInstanceOf[Installed[_]],
            errorIs[CoordinatorCompareAndSetConflict](rejectedCoordinatorCas)
          )
      }
    }
  }

  test("restart reconstructs an initialized coordinator and empty outbox from exact durable records") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )

      for {
        expected <- store(root).use { durable =>
          for {
            coordinator <- initializeCoordinator(durable)
            outboxResult <- durable.initializeEffectOutbox(coordinator, emptyOutbox)
            outbox = outboxResult.asInstanceOf[Installed[DurablyVerifiedEffectOutboxHead]].value
          } yield coordinator -> outbox
        }
        restored <- store(root).use { durable =>
          for {
            coordinator <- durable.coordinatorHead.map(_.get)
            outbox <- durable.effectOutboxHead(coordinator).map(_.get)
          } yield coordinator -> outbox
        }
      } yield
        expect.all(
          restored._1.value == expected._1.value,
          restored._1.value.lineageRevision == expected._1.value.lineageRevision,
          restored._1.value.lineageRevision.value.value == 0L,
          restored._1.payloadDigest == expected._1.payloadDigest,
          restored._1.audit == expected._1.audit,
          restored._2 == expected._2
        )
    }
  }

  test("audit-chain reconstruction retains a nonzero lineage revision from canonical durable bytes") {
    IO.fromEither(
      for {
        payload <- FinalityDurableEnvelope.encodePayload(
          "CoordinatorAuditRecord",
          coordinatorAuditRecordPayloadCodec,
          auditRecord
        )
        decoded <- FinalityDurableEnvelope.decodePayload(
          "CoordinatorAuditRecord",
          coordinatorAuditRecordPayloadCodec,
          payload
        )
      } yield decoded
    ).map { decoded =>
      val reconstructed = CoordinatorHead.fromCommitment(decoded.after, coordinatorHead.auditTail)

      expect.all(
        reconstructed == coordinatorHead,
        reconstructed.lineageRevision == CanonicalLineageRevision(nonNeg(7L))
      )
    }
  }

  test("RecoveryEntered remains durable after active dependencies disappear and rejects every outbox-head write") {
    def exercise(
      root: Path,
      destroyAndProbe: (
        FinalityDurableStore[IO],
        EconomicFixture,
        DurablyVerifiedCoordinatorHead
      ) => IO[Either[Throwable, Unit]],
      reason: (EconomicFixture, DurablyVerifiedCoordinatorHead) => RecoveryReason,
      expectedBroken: Either[Throwable, Unit] => Boolean
    ): IO[Boolean] = {
      val fixture = economicFixture()
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )
      val nextOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(1L)),
        EffectOutboxCursor(None, None)
      )

      for {
        result <- store(root).use { durable =>
          for {
            initial <- initializeCoordinator(durable)
            prepared <- prepareFixture(durable, initial, fixture)
            outbox <- durable.initializeEffectOutbox(prepared, emptyOutbox).map(casValue)
            brokenRead <- destroyAndProbe(durable, fixture, prepared)
            cached <- durable.coordinatorHead
            recovery = kernel(
              FinalityCoordinatorKernel.enterRecovery(prepared.value, reason(fixture, prepared))
            )
            recovered <- durable.compareAndSetCoordinator(prepared, recovery).map(casValue)
            current <- durable.coordinatorHead
            initializeRejected <- durable.initializeEffectOutbox(recovered, emptyOutbox).attempt
            casRejected <- durable.compareAndSetEffectOutbox(recovered, outbox, nextOutbox).attempt
          } yield
            (
              brokenRead,
              cached.exists(_.value == prepared.value),
              recovered,
              current,
              initializeRejected,
              casRejected
            )
        }
        restarted <- store(root).use(_.coordinatorHead)
      } yield
        expectedBroken(result._1) &&
          result._2 &&
          result._3.value.mode.isInstanceOf[CoordinatorMode.RecoveryRequired] &&
          result._3.activeBatch.isEmpty &&
          result._3.latestEffectManifest.isEmpty &&
          result._4.exists(_.value == result._3.value) &&
          errorIs[RecoveryModeIsAbsorbing](result._5) &&
          errorIs[RecoveryModeIsAbsorbing](result._6) &&
          restarted.exists(_.value == result._3.value) &&
          restarted.exists(_.activeBatch.isEmpty) &&
          restarted.exists(_.latestEffectManifest.isEmpty)
    }

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      val missingRoot = base.resolve("missing-core")
      val corruptRoot = base.resolve("corrupt-manifest")

      for {
        missing <- exercise(
          missingRoot,
          (durable, _, prepared) =>
            (IO.blocking(
              NioFiles.delete(coreBatchArtifactPath(missingRoot, prepared.value.active.get.batch))
            ) >> durable.readCoreBatch(prepared.value.active.get.batch).void).attempt,
          (_, prepared) => RecoveryReason.MissingCoreBatch(prepared.value.active.get.batch),
          errorIs[Missing]
        )
        corrupt <- exercise(
          corruptRoot,
          (durable, fixture, _) =>
            (IO
              .blocking(
                NioFiles.write(
                  effectManifestPath(corruptRoot, fixture.batch.effectManifest),
                  Array[Byte](1, 2, 3),
                  StandardOpenOption.TRUNCATE_EXISTING
                )
              )
              .void >> durable.readEffectManifest(fixture.batch.effectManifest).void).attempt,
          (fixture, _) => RecoveryReason.CorruptEffectManifest(fixture.batch.effectManifest, hash(820)),
          errorIs[Corrupt]
        )
      } yield expect.all(missing, corrupt)
    }
  }

  test("startup dependency failure atomically replaces Running with absorbing RecoveryRequired") {
    def exercise(root: Path, destroyDependency: (Path, EconomicFixture, DurablyVerifiedCoordinatorHead) => IO[Unit]): IO[Boolean] = {
      val fixture = economicFixture()

      for {
        running <- store(root).use { durable =>
          for {
            initial <- initializeCoordinator(durable)
            prepared <- prepareFixture(durable, initial, fixture)
          } yield prepared
        }
        _ <- destroyDependency(root, fixture, running)
        recovered <- store(root).use { durable =>
          for {
            current <- durable.coordinatorHead.map(_.get)
            outboxRejected <- durable.effectOutboxHead(current).attempt
          } yield current -> outboxRejected
        }
        restarted <- store(root).use(_.coordinatorHead.map(_.get))
        recoveryRecord = recovered._1.recovery
      } yield
        recovered._1.value.revision.value.value == running.value.revision.value.value + 1L &&
          recovered._1.value.mode.isInstanceOf[CoordinatorMode.RecoveryRequired] &&
          recovered._1.value.lastAttempt == running.value.lastAttempt &&
          recovered._1.value.released == running.value.released &&
          recovered._1.value.active == running.value.active &&
          recovered._1.value.effects == running.value.effects &&
          recovered._1.audit.mutation == CoordinatorMutationKind.RecoveryEntered &&
          recoveryRecord.exists(_.priorAudit == running.value.auditTail) &&
          recoveryRecord.exists(_.reason.isInstanceOf[RecoveryReason.StartupDependencyFailure]) &&
          recovered._1.activeBatch.isEmpty &&
          recovered._1.latestEffectManifest.isEmpty &&
          errorIs[RecoveryModeIsAbsorbing](recovered._2) &&
          restarted.value == recovered._1.value &&
          restarted.payloadDigest == recovered._1.payloadDigest &&
          restarted.recovery == recovered._1.recovery &&
          restarted.activeBatch.isEmpty &&
          restarted.latestEffectManifest.isEmpty
    }

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      for {
        missing <- exercise(
          base.resolve("missing-core-at-startup"),
          (root, _, running) => IO.blocking(NioFiles.delete(coreBatchArtifactPath(root, running.value.active.get.batch)))
        )
        corrupt <- exercise(
          base.resolve("corrupt-manifest-at-startup"),
          (root, fixture, _) =>
            IO.blocking(
              NioFiles.write(
                effectManifestPath(root, fixture.batch.effectManifest),
                Array[Byte](1, 2, 3),
                StandardOpenOption.TRUNCATE_EXISTING
              )
            ).void
        )
      } yield expect.all(missing, corrupt)
    }
  }

  test("RecoveryRequired startup ignores missing or corrupt outbox bytes without recreating or mutating them") {
    def exercise(root: Path, damageOutbox: Path => IO[Unit]): IO[Boolean] = {
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )
      val nextOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(1L)),
        EffectOutboxCursor(None, None)
      )

      for {
        installed <- store(root).use { durable =>
          for {
            running <- initializeCoordinator(durable)
            outbox <- durable.initializeEffectOutbox(running, emptyOutbox).map(casValue)
            mutation = kernel(
              FinalityCoordinatorKernel.enterRecovery(
                running.value,
                RecoveryReason.JournalCorruption(hash(902))
              )
            )
            recovered <- durable.compareAndSetCoordinator(running, mutation).map(casValue)
          } yield recovered -> outbox
        }
        _ <- damageOutbox(effectOutboxHeadPath(root))
        before <- persistedFiles(root)
        opened <- store(root).use { durable =>
          for {
            recovered <- durable.coordinatorHead.map(_.get)
            read <- durable.effectOutboxHead(recovered).attempt
            initialize <- durable.initializeEffectOutbox(recovered, emptyOutbox).attempt
            cas <- durable.compareAndSetEffectOutbox(recovered, installed._2, nextOutbox).attempt
          } yield (recovered, read, initialize, cas)
        }
        after <- persistedFiles(root)
        restarted <- store(root).use(_.coordinatorHead.map(_.get))
      } yield
        opened._1.value == installed._1.value &&
          opened._1.recovery == installed._1.recovery &&
          errorIs[RecoveryModeIsAbsorbing](opened._2) &&
          errorIs[RecoveryModeIsAbsorbing](opened._3) &&
          errorIs[RecoveryModeIsAbsorbing](opened._4) &&
          before == after &&
          restarted.value == installed._1.value
    }

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      for {
        missing <- exercise(
          base.resolve("missing-outbox-in-recovery"),
          path => IO.blocking(NioFiles.delete(path))
        )
        corrupt <- exercise(
          base.resolve("corrupt-outbox-in-recovery"),
          path =>
            IO.blocking(
              NioFiles.write(path, Array[Byte](9, 8, 7), StandardOpenOption.TRUNCATE_EXISTING)
            ).void
        )
      } yield expect.all(missing, corrupt)
    }
  }

  test("Running startup converts missing or corrupt initialized outbox state into absorbing recovery") {
    def exercise(root: Path, damageOutbox: Path => IO[Unit]): IO[Boolean] = {
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )

      for {
        running <- store(root).use { durable =>
          for {
            coordinator <- initializeCoordinator(durable)
            _ <- durable.initializeEffectOutbox(coordinator, emptyOutbox)
          } yield coordinator
        }
        _ <- damageOutbox(effectOutboxHeadPath(root))
        outboxBefore <- persistedFiles(root).map(_.filter { case (name, _) => name.startsWith("effects.") })
        recovered <- store(root).use { durable =>
          for {
            current <- durable.coordinatorHead.map(_.get)
            rejected <- durable.effectOutboxHead(current).attempt
          } yield current -> rejected
        }
        outboxAfter <- persistedFiles(root).map(_.filter { case (name, _) => name.startsWith("effects.") })
        restarted <- store(root).use(_.coordinatorHead.map(_.get))
      } yield
        recovered._1.value.revision.value.value == running.value.revision.value.value + 1L &&
          recovered._1.value.mode.isInstanceOf[CoordinatorMode.RecoveryRequired] &&
          recovered._1.recovery.exists(_.reason.isInstanceOf[RecoveryReason.StartupDependencyFailure]) &&
          recovered._1.activeBatch.isEmpty &&
          recovered._1.latestEffectManifest.isEmpty &&
          errorIs[RecoveryModeIsAbsorbing](recovered._2) &&
          outboxBefore == outboxAfter &&
          restarted.value == recovered._1.value &&
          restarted.payloadDigest == recovered._1.payloadDigest
    }

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      for {
        missing <- exercise(
          base.resolve("missing-running-outbox"),
          path => IO.blocking(NioFiles.delete(path))
        )
        corrupt <- exercise(
          base.resolve("corrupt-running-outbox"),
          path =>
            IO.blocking(
              NioFiles.write(path, Array[Byte](6, 5, 4), StandardOpenOption.TRUNCATE_EXISTING)
            ).void
        )
      } yield expect.all(missing, corrupt)
    }
  }

  test("startup dependency recovery survives crashes on both sides of the head replacement") {
    Files[IO].tempDirectory.use { temporary =>
      List(DurableWriteBoundary.Before, DurableWriteBoundary.After).zipWithIndex.traverse {
        case (boundary, index) =>
          val root = temporary.toNioPath.resolve(s"startup-recovery-crash-$index")
          val fixture = economicFixture()
          val hook = new DurableWriteHook[IO, FinalityDurableWriteArtifact] {
            def onEvent(event: DurableWriteEvent[FinalityDurableWriteArtifact]): IO[Unit] =
              event match {
                case DurableWriteEvent(
                      _: FinalityDurableWriteArtifact.CoordinatorHead,
                      DurableWriteStage.AtomicMove,
                      observedBoundary
                    ) if observedBoundary == boundary =>
                  IO.raiseError(SimulatedCrash)
                case _ => IO.unit
              }
          }

          for {
            running <- store(root).use { durable =>
              for {
                initial <- initializeCoordinator(durable)
                prepared <- prepareFixture(durable, initial, fixture)
              } yield prepared
            }
            _ <- IO.blocking(NioFiles.delete(coreBatchArtifactPath(root, running.value.active.get.batch)))
            crashed <- store(root, hook = hook).use(_ => IO.unit).attempt
            recovered <- store(root).use(_.coordinatorHead.map(_.get))
          } yield {
            val crashObserved = crashed.left.toOption.contains(SimulatedCrash)
            crashObserved &&
            recovered.value.mode.isInstanceOf[CoordinatorMode.RecoveryRequired] &&
            recovered.value.revision.value.value == running.value.revision.value.value + 1L &&
            recovered.recovery.exists(_.reason.isInstanceOf[RecoveryReason.StartupDependencyFailure])
          }
      }.map(results => expect(clue(results).forall(identity)))
    }
  }

  test("runtime caches reuse verified history, next-link CAS stays local, and restart revalidates the full chain") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val fixture = economicFixture()
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )

      for {
        live <- store(root).use { durable =>
          for {
            initial <- initializeCoordinator(durable)
            oldestAudit = initial.value.auditTail.get
            prepared <- prepareFixture(durable, initial, fixture)
            expectedOutbox <- durable.initializeEffectOutbox(prepared, emptyOutbox).map(casValue)
            _ <- IO.blocking(NioFiles.delete(auditPath(root, oldestAudit)))
            firstHead <- durable.coordinatorHead
            secondHead <- durable.coordinatorHead
            firstOutbox <- durable.effectOutboxHead(prepared)
            secondOutbox <- durable.effectOutboxHead(prepared)
            recovery = kernel(
              FinalityCoordinatorKernel.enterRecovery(
                prepared.value,
                RecoveryReason.JournalCorruption(hash(830))
              )
            )
            next <- durable.compareAndSetCoordinator(prepared, recovery).map(casValue)
          } yield
            (
              firstHead.exists(_.value == prepared.value),
              secondHead.exists(_.value == prepared.value),
              firstOutbox.contains(expectedOutbox),
              secondOutbox.contains(expectedOutbox),
              next.value.mode.isInstanceOf[CoordinatorMode.RecoveryRequired]
            )
        }
        restart <- store(root).use(_ => IO.unit).attempt
      } yield
        expect.all(
          live._1,
          live._2,
          live._3,
          live._4,
          live._5,
          errorIs[Missing](restart)
        )
    }
  }

  test("runtime caches never hide deletion of either mutable head") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val emptyOutbox = FinalityEffectOutboxHead(
        EffectOutboxRevision(nonNeg(0L)),
        EffectOutboxCursor(None, None)
      )

      store(root).use { durable =>
        for {
          coordinator <- initializeCoordinator(durable)
          _ <- durable.initializeEffectOutbox(coordinator, emptyOutbox)
          _ <- durable.coordinatorHead
          _ <- durable.effectOutboxHead(coordinator)
          _ <- IO.blocking(NioFiles.delete(root.resolve("effects.head")))
          missingOutbox <- durable.effectOutboxHead(coordinator).attempt
          _ <- IO.blocking(NioFiles.delete(root.resolve("coordinator.head")))
          missingCoordinator <- durable.coordinatorHead.attempt
        } yield
          expect.all(
            errorIs[Missing](missingOutbox),
            errorIs[Missing](missingCoordinator)
          )
      }
    }
  }

  test("invalid envelope-sized limits fail before creating the store directory") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val invalid = FinalityDurableStoreLimits.default.copy(maxImmutableArtifactBytes = Long.MaxValue)

      for {
        result <- store(root, limits = invalid).use(_ => IO.unit).attempt
        exists <- IO.blocking(NioFiles.exists(root, LinkOption.NOFOLLOW_LINKS))
      } yield
        expect.all(
          errorIs[InvalidLimits](result),
          !exists
        )
    }
  }

  test("fatal input-stream failures propagate through the envelope decoder") {
    IO {
      val fatal = new OutOfMemoryError("fatal envelope read")
      val input = new InputStream {
        override def read(): Int = throw fatal
        override def read(bytes: Array[Byte], offset: Int, length: Int): Int = throw fatal
      }
      var propagated = false

      try
        FinalityDurableEnvelope.decode(
          input,
          FinalityDurableEnvelopeKind.CoordinatorHead,
          FinalityDurableStoreLimits.default.maxCoordinatorHeadBytes
        )
      catch {
        case error: OutOfMemoryError if error eq fatal => propagated = true
      }

      expect(propagated)
    }
  }

  test("restart rejects a missing or corrupt audit tail and a missing recovery record") {
    def missingAudit(root: Path): IO[Boolean] =
      for {
        pointer <- store(root).use(durable => initializeCoordinator(durable).map(_.value.auditTail.get))
        _ <- IO.blocking(NioFiles.delete(auditPath(root, pointer)))
        result <- store(root).use(_ => IO.unit).attempt
      } yield errorIs[Missing](result)

    def corruptAudit(root: Path): IO[Boolean] =
      for {
        pointer <- store(root).use(durable => initializeCoordinator(durable).map(_.value.auditTail.get))
        _ <- IO.blocking(NioFiles.write(auditPath(root, pointer), Array[Byte](1, 2, 3))).void
        result <- store(root).use(_ => IO.unit).attempt
      } yield errorIs[Corrupt](result)

    def missingRecovery(root: Path): IO[Boolean] =
      for {
        pointer <- store(root).use { durable =>
          for {
            coordinator <- initializeCoordinator(durable)
            recovery = kernel(
              FinalityCoordinatorKernel.enterRecovery(
                coordinator.value,
                RecoveryReason.JournalCorruption(hash(720))
              )
            )
            _ <- durable.compareAndSetCoordinator(coordinator, recovery)
          } yield recovery.recoveryRecord.map(FinalityIdentity.recoveryPointer).flatMap(_.toOption).get
        }
        _ <- IO.blocking(NioFiles.delete(recoveryPath(root, pointer)))
        result <- store(root).use(_ => IO.unit).attempt
      } yield errorIs[Missing](result)

    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      for {
        missing <- missingAudit(base.resolve("missing-audit"))
        corrupt <- corruptAudit(base.resolve("corrupt-audit"))
        recovery <- missingRecovery(base.resolve("missing-recovery"))
      } yield expect.all(missing, corrupt, recovery)
    }
  }

  test("startup audit traversal obeys the configured recovery work bound") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val bounded = FinalityDurableStoreLimits.default.copy(maxAuditTraversalRecords = 1L)

      for {
        _ <- store(root).use { durable =>
          for {
            coordinator <- initializeCoordinator(durable)
            recovery = kernel(
              FinalityCoordinatorKernel.enterRecovery(
                coordinator.value,
                RecoveryReason.JournalCorruption(hash(730))
              )
            )
            _ <- durable.compareAndSetCoordinator(coordinator, recovery)
          } yield ()
        }
        result <- store(root, limits = bounded).use(_ => IO.unit).attempt
      } yield expect(errorIs[AuditTraversalLimitExceeded](result))
    }
  }

  test("path traversal is content-bound and stops at the configured chunk limit") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val fixture = multiChunkPath
      val manifestBytes = encodePayload(
        "PathManifestPayload",
        pathManifestPayloadCodec,
        fixture.commitment.payload
      )

      for {
        _ <- store(root).use { durable =>
          durable.putArtifact(fixture.commitment.manifest, ByteVector.view(manifestBytes)) >>
            fixture.chunks.traverse_ {
              case (chunk, bytes) =>
                writeLocator(
                  pathLocator(root, fixture.intentId, fixture.commitment.manifest.id, chunk.chunkIndex.value),
                  bytes
                )
            } >> durable.verifyStoredPath(fixture.intentId, fixture.commitment).void
        }
        bounded = FinalityDurableStoreLimits.default.copy(maxPathTraversalChunks = 1L)
        result <- store(root, limits = bounded)
          .use(
            _.verifyStoredPath(fixture.intentId, fixture.commitment)
          )
          .attempt
      } yield expect(errorIs[PathTraversalLimitExceeded](result))
    }
  }

  test("path locator poisoning is rejected before a committed path is accepted") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath.resolve("finality")
      val fixture = multiChunkPath
      val manifestBytes = encodePayload(
        "PathManifestPayload",
        pathManifestPayloadCodec,
        fixture.commitment.payload
      )
      val (first, _) = fixture.chunks.head
      val poisoned = first.copy(intentId = IntentId(hash(799)))
      val poisonedBytes = encodePayload("PathChunk", pathChunkPayloadCodec, poisoned)

      store(root).use { durable =>
        for {
          _ <- durable.putArtifact(fixture.commitment.manifest, ByteVector.view(manifestBytes))
          _ <- writeLocator(
            pathLocator(root, fixture.intentId, fixture.commitment.manifest.id, 0L),
            poisonedBytes
          )
          result <- durable.verifyStoredPath(fixture.intentId, fixture.commitment).attempt
        } yield expect(errorIs[InvalidPath](result))
      }
    }
  }

  test("symlink ancestors and immutable targets are rejected without following them") {
    Files[IO].tempDirectory.use { temporary =>
      val base = temporary.toNioPath
      val ancestorRoot = base.resolve("ancestor-store")
      val targetRoot = base.resolve("target-store")
      val outsideDirectory = base.resolve("outside-directory")
      val outsideFile = base.resolve("outside-file")
      val bytes = ByteVector.fromValidHex("aabbccdd")
      val pointer = FinalityIdentity
        .artifactPointerFromBytes(FinalityArtifactKind.EffectPayload, bytes)
        .fold(throw _, identity)

      for {
        _ <- store(ancestorRoot).use(_ => IO.unit)
        _ <- IO.blocking {
          NioFiles.createDirectory(outsideDirectory)
          NioFiles.createSymbolicLink(
            ancestorRoot.resolve("immutable").resolve("14-effect-payload"),
            outsideDirectory
          )
        }
        ancestorResult <- store(ancestorRoot).use(_.putArtifact(pointer, bytes)).attempt
        _ <- store(targetRoot).use(_ => IO.unit)
        _ <- IO.blocking {
          val target = artifactPath(targetRoot, pointer)
          NioFiles.createDirectories(target.getParent)
          NioFiles.write(outsideFile, Array[Byte](9, 8, 7), StandardOpenOption.CREATE_NEW)
          NioFiles.createSymbolicLink(target, outsideFile)
        }
        targetResult <- store(targetRoot).use(_.putArtifact(pointer, bytes)).attempt
        outsideBytes <- IO.blocking(NioFiles.readAllBytes(outsideFile))
      } yield
        expect.all(
          errorIs[UnsafeDirectory](ancestorResult),
          errorIs[UnsafeFile](targetResult),
          outsideBytes.sameElements(Array[Byte](9, 8, 7))
        )
    }
  }

  test("every immutable-audit and mutable-head crash boundary restarts from the last visible head") {
    Files[IO].tempDirectory.use { temporary =>
      val boundaries = List(DurableWriteBoundary.Before, DurableWriteBoundary.After)
      val cases = for {
        artifact <- List("audit", "head")
        stage <- DurableWriteStage.ordered
        boundary <- boundaries
      } yield (artifact, stage, boundary)

      cases.zipWithIndex.traverse {
        case ((artifact, stage, boundary), index) =>
          val root = temporary.toNioPath.resolve(s"case-$index")
          val hook = new DurableWriteHook[IO, FinalityDurableWriteArtifact] {
            def onEvent(event: DurableWriteEvent[FinalityDurableWriteArtifact]): IO[Unit] = {
              val matchesArtifact = (artifact, event.artifact) match {
                case ("head", _: FinalityDurableWriteArtifact.CoordinatorHead) => true
                case ("audit", FinalityDurableWriteArtifact.Immutable(path)) =>
                  Option(path.getParent).exists(_.getFileName.toString == "audit")
                case _ => false
              }

              if (matchesArtifact && event.stage == stage && event.boundary == boundary)
                IO.raiseError(SimulatedCrash)
              else IO.unit
            }
          }

          val headBecameVisible =
            artifact == "head" && (stage match {
              case DurableWriteStage.AtomicMove                                  => boundary == DurableWriteBoundary.After
              case DurableWriteStage.ForceDirectory | DurableWriteStage.ReadBack => true
              case _                                                             => false
            })

          for {
            crashed <- store(root, hook = hook).use(initializeCoordinator).attempt
            restored <- store(root).use(_.coordinatorHead)
          } yield {
            val crashObserved = crashed.left.toOption.exists {
              case SimulatedCrash                                                      => true
              case AtomicImmutableInstallRequired(_, cause) if cause == SimulatedCrash => true
              case _                                                                   => false
            }
            val valid =
              crashObserved &&
                restored.isDefined == headBecameVisible &&
                restored.forall(value =>
                  value.value.revision.value.value == 0L &&
                    value.value.publication == MptActivePublication(MptPublicationRevision(0L), None) &&
                    value.audit.mutation == CoordinatorMutationKind.Initialized &&
                    FinalityIdentity.auditPointer(value.audit).toOption == value.value.auditTail
                )
            (artifact, stage, boundary, crashed, restored.isDefined, headBecameVisible, valid)
          }
      }.map(results => expect(clue(results.filterNot(_._7)).isEmpty))
    }
  }
}
