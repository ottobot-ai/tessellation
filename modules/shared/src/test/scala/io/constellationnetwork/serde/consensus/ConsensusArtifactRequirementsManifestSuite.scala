package io.constellationnetwork.serde.consensus

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import io.constellationnetwork.schema.consensus.ArtifactAuthority.SourceAuthorization
import io.constellationnetwork.schema.consensus.ArtifactBinding._
import io.constellationnetwork.schema.consensus.ArtifactGap._
import io.constellationnetwork.schema.consensus.ConsensusArtifactKind._
import io.constellationnetwork.schema.consensus._
import io.constellationnetwork.serde.consensus.ConsensusArtifactRequirementsManifest.Violation._
import io.constellationnetwork.serde.consensus.ConsensusArtifactRequirementsManifest._

import weaver.FunSuite

object ConsensusArtifactRequirementsManifestSuite extends FunSuite {

  test("the declaration-only requirements inventories are structurally valid") {
    val artifactLabels = ConsensusArtifactKind.all.map(_.semanticLabel)
    val transcriptLabels = ConsensusTranscriptKind.all.map(_.semanticLabel)
    val finalityPayloadLabels = ConsensusFinalityPayloadKind.all.map(_.semanticLabel)
    val allLabels =
      artifactLabels ++ transcriptLabels ++ finalityPayloadLabels ++ ArtifactBinding.all.map(_.semanticLabel) ++
        ArtifactAuthority.all.map(_.semanticLabel)
    val forbiddenBftWords = Set("vote", "voted", "lock", "locked", "qc")
    val forbiddenBftPhrases = Set("quorum-certificate", "view-change")
    val containsGlobalBftVocabulary = allLabels.exists { label =>
      val labelWords = label.split("[.-]").toSet
      val permittedEconomicWords = Option.when(label.contains("token-lock"))(Set("lock")).getOrElse(Set.empty)

      (labelWords -- permittedEconomicWords).exists(forbiddenBftWords.contains) || forbiddenBftPhrases.exists(label.contains)
    }

    expect.all(
      validation.isEmpty,
      transcriptValidation.isEmpty,
      finalityPayloadValidation.isEmpty,
      entries.size == 48,
      entries.map(_.kind).toSet == ConsensusArtifactKind.all.toSet,
      artifactLabels.distinct.size == artifactLabels.size,
      ConsensusTranscriptKind.all.size == 10,
      transcriptEntries.size == 10,
      transcriptEntries.map(_.kind).toSet == ConsensusTranscriptKind.all.toSet,
      transcriptLabels.distinct.size == transcriptLabels.size,
      transcriptEntries.forall(_.authority == ArtifactAuthority.Eligibility),
      transcriptEntries.forall(_.bindings.contains(ExactEligibilityParent)),
      transcriptEntries.forall(_.bindings.contains(N2RosterStakeAndKeyView)),
      ConsensusFinalityPayloadKind.all.size == 14,
      finalityPayloadEntries.size == 14,
      finalityPayloadEntries.map(_.kind).toSet == ConsensusFinalityPayloadKind.all.toSet,
      finalityPayloadLabels.distinct.size == finalityPayloadLabels.size,
      !containsGlobalBftVocabulary,
      codecStatus == ManifestCodecStatus.Open,
      activationStatus == ManifestActivationStatus.DarkOnly
    )
  }

  test("finality payload authority and concrete codec/vector evidence stay narrowly scoped") {
    val byKind = finalityPayloadEntries.map(contract => contract.kind -> contract).toMap
    val qualificationKinds = finalityPayloadEntries.collect {
      case contract if contract.authority == ArtifactAuthority.FinalityQualification => contract.kind
    }.toSet
    val concreteKinds = finalityPayloadEntries.collect {
      case contract if contract.codecStatus == FinalityPayloadCodecStatus.ConcreteCanonicalCodec => contract.kind
    }.toSet
    val vectorKinds = finalityPayloadEntries.collect {
      case contract if contract.vectorStatus == FinalityPayloadVectorStatus.ConcreteFrozenVector => contract.kind
    }.toSet
    val concreteSourceTypes = finalityPayloadEntries.collect {
      case contract @ FinalityPayloadContract(_, FinalityPayloadShape.ConcreteSourceType(sourceType), _, _, _, _, _, _) =>
        contract.kind -> sourceType
    }.toMap
    val expectedConcrete = Set[ConsensusFinalityPayloadKind](
      ConsensusFinalityPayloadKind.CoreBatch,
      ConsensusFinalityPayloadKind.ReleasedCoreRecord,
      ConsensusFinalityPayloadKind.PathManifest,
      ConsensusFinalityPayloadKind.PathChunk
    )
    val expectedSourceTypes = Map[ConsensusFinalityPayloadKind, String](
      ConsensusFinalityPayloadKind.CoreBatch ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreBatch",
      ConsensusFinalityPayloadKind.ReleasedCoreRecord ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.ReleasedCoreRecordPayload",
      ConsensusFinalityPayloadKind.PathManifest ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.PathManifestPayload",
      ConsensusFinalityPayloadKind.PathChunk ->
        "io.constellationnetwork.node.shared.domain.snapshot.finality.PathChunk"
    )

    expect.all(
      qualificationKinds == Set[ConsensusFinalityPayloadKind](
        ConsensusFinalityPayloadKind.DecidedAttestationEvidence,
        ConsensusFinalityPayloadKind.DepthK1Evidence
      ),
      byKind(ConsensusFinalityPayloadKind.ForkChoiceDecisionEvidence).authority == ArtifactAuthority.ObjectiveEvidence,
      byKind(ConsensusFinalityPayloadKind.PathManifest).authority == ArtifactAuthority.Commitment,
      byKind(ConsensusFinalityPayloadKind.PathChunk).authority == ArtifactAuthority.Commitment,
      finalityPayloadEntries
        .filterNot(contract => qualificationKinds.contains(contract.kind))
        .forall(_.authority != ArtifactAuthority.FinalityQualification),
      finalityPayloadEntries.forall(_.authority != ArtifactAuthority.StateValidity),
      concreteKinds == expectedConcrete,
      vectorKinds == expectedConcrete,
      concreteSourceTypes == expectedSourceTypes,
      finalityPayloadEntries.filter(contract => expectedConcrete.contains(contract.kind)).forall(_.knownGaps.isEmpty),
      finalityPayloadEntries
        .filterNot(contract => expectedConcrete.contains(contract.kind))
        .forall(contract =>
          contract.payloadShape == FinalityPayloadShape.OpaquePointerOnly &&
            contract.knownGaps.contains(FinalityOpaquePayloadSchemaMissing)
        )
    )
  }

  test("finality payload validation rejects missing, duplicate, underbound, and overstated rows") {
    val duplicate = finalityPayloadEntries.head :: finalityPayloadEntries
    val missing = finalityPayloadEntries.filterNot(_.kind == ConsensusFinalityPayloadKind.EffectPayload)
    val wrongAuthority = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.ForkChoiceDecisionEvidence =>
        contract.copy(authority = ArtifactAuthority.FinalityQualification)
      case contract => contract
    }
    val withoutCommonBinding = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.CoreBatch =>
        contract.copy(bindings = contract.bindings - ParameterHash)
      case contract => contract
    }
    val withoutQualificationContext = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.DecidedAttestationEvidence =>
        contract.copy(bindings = contract.bindings - AttestationDecisionContext)
      case contract => contract
    }
    val opaqueClaimingConcrete = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.EffectPayload =>
        contract.copy(
          payloadShape = FinalityPayloadShape.ConcreteSourceType("invented.EffectPayload"),
          codecStatus = FinalityPayloadCodecStatus.ConcreteCanonicalCodec,
          vectorStatus = FinalityPayloadVectorStatus.ConcreteFrozenVector
        )
      case contract => contract
    }
    val concreteClaimingOpaque = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.CoreBatch =>
        contract.copy(
          payloadShape = FinalityPayloadShape.OpaquePointerOnly,
          codecStatus = FinalityPayloadCodecStatus.NoCanonicalPayloadCodec,
          vectorStatus = FinalityPayloadVectorStatus.NoCanonicalPayloadVector
        )
      case contract => contract
    }
    val wrongDefinitionStatus = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.PathChunk =>
        contract.copy(definitionStatus = ArtifactDefinitionStatus.TargetShapeOpen)
      case contract => contract
    }
    val withoutOpaqueGap = finalityPayloadEntries.map {
      case contract if contract.kind == ConsensusFinalityPayloadKind.PriorAnchorReceipt =>
        contract.copy(knownGaps = contract.knownGaps - FinalityOpaquePayloadSchemaMissing)
      case contract => contract
    }

    expect.all(
      validateFinalityPayloadEntries(duplicate).exists(_.isInstanceOf[DuplicateFinalityPayloadKind]),
      validateFinalityPayloadEntries(duplicate).exists(_.isInstanceOf[DuplicateFinalityPayloadSemanticLabel]),
      validateFinalityPayloadEntries(missing).exists(_.isInstanceOf[MissingFinalityPayloadKind]),
      validateFinalityPayloadEntries(wrongAuthority).exists(_.isInstanceOf[InvalidFinalityPayloadAuthority]),
      validateFinalityPayloadEntries(withoutCommonBinding).exists(_.isInstanceOf[MissingFinalityPayloadCommonBindings]),
      validateFinalityPayloadEntries(withoutQualificationContext)
        .exists(_.isInstanceOf[MissingFinalityPayloadAuthorityBindings]),
      validateFinalityPayloadEntries(opaqueClaimingConcrete).exists(_.isInstanceOf[InvalidFinalityPayloadEvidence]),
      validateFinalityPayloadEntries(concreteClaimingOpaque).exists(_.isInstanceOf[InvalidFinalityPayloadEvidence]),
      validateFinalityPayloadEntries(wrongDefinitionStatus).exists(_.isInstanceOf[InvalidFinalityPayloadDefinitionStatus]),
      validateFinalityPayloadEntries(withoutOpaqueGap).exists(_.isInstanceOf[MissingFinalityPayloadGap])
    )
  }

  test("known consensus-byte gaps remain explicit and cannot be mistaken for codec readiness") {
    def gaps(kind: ConsensusArtifactKind): Set[ArtifactGap] =
      entries.find(_.kind == kind).fold(Set.empty[ArtifactGap])(_.knownGaps)

    expect.all(
      gaps(GlobalStateAccumulator) == Set(AccumulatorOmitsConsumedAllowSpends, AccumulatorOmitsSlashings),
      gaps(GlobalFollowDelta) == Set(
        AccumulatorOmitsConsumedAllowSpends,
        AccumulatorOmitsSlashings,
        ChangeSetCannotCarryCompleteGlobalDelta
      ),
      gaps(GlobalMptKey) == Set(GlobalMptPhysicalKeyGrammarUnenforced),
      gaps(GlobalMptValue) == Set(SlashingValueCodecUsesJson),
      gaps(OptimisticTipAttestation) == Set(
        OptimisticTipAttestationMissingExactContext,
        OptimisticTipAttestationCarriesWallClock
      ),
      gaps(TowerProof) == Set(PortableTowerHistoricalEligibilityMissing),
      gaps(GlobalGenesisSnapshot) == Set(OrdinalZeroGenesisUsesLegacyShape),
      gaps(CurrencyGenesisSnapshot) == Set(OrdinalZeroGenesisUsesLegacyShape),
      gaps(MigrationManifest) == Set(O19MigrationPolicyOpen)
    )
  }

  test("selection transcripts require exact branch-historical preregistration evidence") {
    val duplicate = transcriptEntries.head :: transcriptEntries
    val withoutN2View = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.AdmissionVrf =>
        contract.copy(bindings = contract.bindings - N2RosterStakeAndKeyView)
      case contract => contract
    }
    val withoutAdmissionParent = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.AdmissionVrf =>
        contract.copy(bindings = contract.bindings - ExactSourceParent)
      case contract => contract
    }
    val withoutEligibilityParent = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.Gl0LeaderVrf =>
        contract.copy(bindings = contract.bindings - ExactEligibilityParent)
      case contract => contract
    }
    val wrongAuthority = transcriptEntries.map {
      case contract if contract.kind == ConsensusTranscriptKind.TowerEligibility =>
        contract.copy(authority = SourceAuthorization)
      case contract => contract
    }
    val withoutTower = transcriptEntries.filterNot(_.kind == ConsensusTranscriptKind.TowerEligibility)

    expect.all(
      validateTranscriptEntries(duplicate).exists(_.isInstanceOf[DuplicateTranscriptKind]),
      validateTranscriptEntries(duplicate).exists(_.isInstanceOf[DuplicateTranscriptSemanticLabel]),
      validateTranscriptEntries(withoutN2View).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(withoutAdmissionParent).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(withoutEligibilityParent).exists(_.isInstanceOf[MissingTranscriptBindings]),
      validateTranscriptEntries(wrongAuthority).exists(_.isInstanceOf[InvalidTranscriptAuthority]),
      validateTranscriptEntries(withoutTower).exists(_.isInstanceOf[MissingTranscriptKind])
    )
  }

  test("the pure validator rejects duplicate, incomplete, and authority-underbound declarations") {
    val duplicate = entries.head :: entries
    val withoutNetwork = entries.map {
      case contract if contract.kind == NativeDagBlock => contract.copy(bindings = contract.bindings - Network)
      case contract                                    => contract
    }
    val withoutDecisionContext = entries.map {
      case contract if contract.kind == DecidedOptimisticAttestationEvidence =>
        contract.copy(bindings = contract.bindings - AttestationDecisionContext)
      case contract => contract
    }
    val withoutWatchtowerCheckpoint = entries.map {
      case contract if contract.kind == PositiveWatchtowerCoverage =>
        contract.copy(bindings = contract.bindings - ExactCheckpoint)
      case contract => contract
    }
    val withoutWatchtowerAssignment = entries.map {
      case contract if contract.kind == PositiveWatchtowerCoverage =>
        contract.copy(bindings = contract.bindings - WatchtowerAssignmentContext)
      case contract => contract
    }
    val withoutMigration = entries.filterNot(_.kind == MigrationManifest)

    expect.all(
      validateEntries(duplicate).exists(_.isInstanceOf[DuplicateKind]),
      validateEntries(duplicate).exists(_.isInstanceOf[DuplicateSemanticLabel]),
      validateEntries(withoutNetwork).exists(_.isInstanceOf[MissingCommonBindings]),
      validateEntries(withoutDecisionContext).exists(_.isInstanceOf[MissingAuthorityBindings]),
      validateEntries(withoutWatchtowerCheckpoint).exists(_.isInstanceOf[MissingArtifactBindings]),
      validateEntries(withoutWatchtowerAssignment).exists(_.isInstanceOf[MissingArtifactBindings]),
      validateEntries(withoutMigration).exists(_.isInstanceOf[MissingKind])
    )
  }

  test("requirements metadata remains unreachable from production runtime code") {
    val root = repositoryRoot(Paths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    val sources = productionSources(root)
    val reviewedMetadataPaths = Set(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ArtifactAuthority.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ArtifactBinding.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusFinalityPayloadKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusArtifactKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusArtifactStatus.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusTranscriptKind.scala",
      "modules/shared/src/main/scala/io/constellationnetwork/serde/consensus/ConsensusArtifactRequirementsManifest.scala"
    )
    val metadataSymbols = List(
      "ArtifactAuthority",
      "ArtifactBinding",
      "ArtifactDefinitionStatus",
      "ArtifactGap",
      "ManifestCodecStatus",
      "ManifestActivationStatus",
      "ConsensusArtifactKind",
      "ConsensusFinalityPayloadKind",
      "FinalityPayloadShape",
      "FinalityPayloadCodecStatus",
      "FinalityPayloadVectorStatus",
      "ConsensusTranscriptKind",
      "TranscriptContract",
      "FinalityPayloadContract",
      "ConsensusArtifactRequirementsManifest"
    )
    val metadataReferences = sources.collect {
      case (path, contents) if metadataSymbols.exists(contents.contains) => path
    }.toSet
    val metadataContents = reviewedMetadataPaths.toList.map { path =>
      new String(Files.readAllBytes(root.resolve(path)), StandardCharsets.UTF_8)
    }
    val forbiddenRuntimeTypes = List("Hasher", "ImmutableCodec", "ProtocolEraId", "scodec.")

    expect.all(
      metadataReferences == reviewedMetadataPaths,
      metadataContents.forall(contents => forbiddenRuntimeTypes.forall(token => !contents.contains(token)))
    )
  }

  private def productionSources(root: Path): List[(String, String)] = {
    val stream = Files.walk(root.resolve("modules"))
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(_.getFileName.toString.endsWith(".scala"))
        .filter(_.toString.replace('\\', '/').contains("/src/main/scala/"))
        .map { path =>
          root.relativize(path).toString.replace('\\', '/') -> new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
        }
        .toList
    finally stream.close()
  }

  @annotation.tailrec
  private def repositoryRoot(candidate: Path): Path =
    if (Files.isDirectory(candidate.resolve("modules/shared")) && Files.isRegularFile(candidate.resolve("build.sbt"))) candidate
    else
      Option(candidate.getParent) match {
        case Some(parent) => repositoryRoot(parent)
        case None         => throw new IllegalStateException(s"Unable to locate repository root from ${sys.props("user.dir")}")
      }
}
