package io.constellationnetwork.serde.consensus

import io.constellationnetwork.schema.consensus.ArtifactAuthority._
import io.constellationnetwork.schema.consensus.ArtifactBinding._
import io.constellationnetwork.schema.consensus.ArtifactDefinitionStatus._
import io.constellationnetwork.schema.consensus.ArtifactGap._
import io.constellationnetwork.schema.consensus.ConsensusArtifactKind._
import io.constellationnetwork.schema.consensus.ConsensusTranscriptKind._
import io.constellationnetwork.schema.consensus._

/** Dark, declaration-only inventory for the eventual canonical consensus-byte contract.
  *
  * Nothing here serializes, hashes, signs, validates, stores, or activates an artifact. Semantic labels deliberately have no numeric tags
  * or byte encoding. `codecStatus` and `activationStatus` remain open/dark until the coordinated all-layer cutover.
  */
object ConsensusArtifactRequirementsManifest {

  final case class ArtifactContract(
    kind: ConsensusArtifactKind,
    authority: ArtifactAuthority,
    bindings: Set[ArtifactBinding],
    definitionStatus: ArtifactDefinitionStatus,
    knownGaps: Set[ArtifactGap] = Set.empty
  )

  final case class TranscriptContract(
    kind: ConsensusTranscriptKind,
    authority: ArtifactAuthority,
    bindings: Set[ArtifactBinding],
    definitionStatus: ArtifactDefinitionStatus
  )

  sealed trait Violation extends Product with Serializable { def description: String }

  object Violation {
    final case class DuplicateKind(kind: ConsensusArtifactKind) extends Violation {
      val description: String = s"duplicate artifact kind: ${kind.semanticLabel}"
    }
    final case class DuplicateSemanticLabel(label: String) extends Violation {
      val description: String = s"duplicate semantic label: $label"
    }
    final case class MissingKind(kind: ConsensusArtifactKind) extends Violation {
      val description: String = s"missing artifact kind: ${kind.semanticLabel}"
    }
    final case class InvalidSemanticLabel(kind: ConsensusArtifactKind) extends Violation {
      val description: String = s"invalid semantic label: ${kind.semanticLabel}"
    }
    final case class MissingCommonBindings(kind: ConsensusArtifactKind, missing: Set[ArtifactBinding]) extends Violation {
      val description: String =
        s"missing common bindings for ${kind.semanticLabel}: ${missing.toList.map(_.semanticLabel).sorted.mkString(",")}"
    }
    final case class MissingAuthorityBindings(kind: ConsensusArtifactKind, authority: ArtifactAuthority) extends Violation {
      val description: String = s"missing ${authority.semanticLabel} bindings for ${kind.semanticLabel}"
    }
    final case class MissingArtifactBindings(kind: ConsensusArtifactKind, missing: Set[ArtifactBinding]) extends Violation {
      val description: String =
        s"missing artifact bindings for ${kind.semanticLabel}: ${missing.toList.map(_.semanticLabel).sorted.mkString(",")}"
    }
    final case class IncompleteWithoutGap(kind: ConsensusArtifactKind) extends Violation {
      val description: String = s"known-incomplete artifact has no recorded gap: ${kind.semanticLabel}"
    }
    final case class OwnerBlockedWithoutOwnerGap(kind: ConsensusArtifactKind) extends Violation {
      val description: String = s"owner-blocked artifact has no O18/O19 gap: ${kind.semanticLabel}"
    }
    final case class DuplicateTranscriptKind(kind: ConsensusTranscriptKind) extends Violation {
      val description: String = s"duplicate transcript kind: ${kind.semanticLabel}"
    }
    final case class DuplicateTranscriptSemanticLabel(label: String) extends Violation {
      val description: String = s"duplicate transcript semantic label: $label"
    }
    final case class MissingTranscriptKind(kind: ConsensusTranscriptKind) extends Violation {
      val description: String = s"missing transcript kind: ${kind.semanticLabel}"
    }
    final case class InvalidTranscriptSemanticLabel(kind: ConsensusTranscriptKind) extends Violation {
      val description: String = s"invalid transcript semantic label: ${kind.semanticLabel}"
    }
    final case class InvalidTranscriptAuthority(kind: ConsensusTranscriptKind) extends Violation {
      val description: String = s"transcript is not eligibility-only: ${kind.semanticLabel}"
    }
    final case class MissingTranscriptBindings(kind: ConsensusTranscriptKind, missing: Set[ArtifactBinding]) extends Violation {
      val description: String =
        s"missing transcript bindings for ${kind.semanticLabel}: ${missing.toList.map(_.semanticLabel).sorted.mkString(",")}"
    }
  }

  val codecStatus: ManifestCodecStatus = ManifestCodecStatus.Open
  val activationStatus: ManifestActivationStatus = ManifestActivationStatus.DarkOnly

  private val embedded = Set[ArtifactBinding](EmbeddingArtifact)
  private val layerParent = Set[ArtifactBinding](ExactLayerParent)
  private val globalParent = Set[ArtifactBinding](ExactGlobalParent)
  private val globalRef = Set[ArtifactBinding](ExactGlobalSnapshotRef)
  private val ml0Parent = Set[ArtifactBinding](ExactMl0Parent, ExactPhase2Base)
  private val shardBase = Set[ArtifactBinding](ExactShardParent, ExactExecutionBase)

  private def contract(
    kind: ConsensusArtifactKind,
    authority: ArtifactAuthority,
    specificBindings: Set[ArtifactBinding],
    status: ArtifactDefinitionStatus = ExistingShapeNeedsAudit,
    gaps: Set[ArtifactGap] = Set.empty
  ): ArtifactContract =
    ArtifactContract(kind, authority, ArtifactBinding.artifactCommon ++ specificBindings, status, gaps)

  private val transcriptSpecificBindings: Map[ConsensusTranscriptKind, Set[ArtifactBinding]] = Map(
    Gl0LeaderVrf -> Set(ExactGlobalParent),
    AdmissionVrf -> Set(ExactPhase2Base, ExactSourceParent, MetagraphAndLane),
    ExecutionCommitteeVkHashDraw -> Set(ExactPhase2Base, ShardAndPeriod),
    ShardEtaDerivation -> Set(ExactPhase2Base, ShardAndPeriod),
    StaircaseRank -> Set(ExactShardParent, ExactExecutionBase, ShardAndPeriod),
    CheckpointVrfPossession -> Set(ExactShardParent, ExactExecutionBase, ShardAndPeriod),
    ExecutionSignatureVrfPossession -> Set(ExactShardParent, ExactExecutionBase, ShardAndPeriod),
    WatchtowerAssignment -> Set(
      ExactShardParent,
      ExactExecutionBase,
      ExactCheckpoint,
      ExactInputCommitment,
      ShardAndPeriod,
      WatchtowerAssignmentContext
    ),
    OptimisticSampling -> Set(ExactGlobalSnapshotRef, AttestationDecisionContext),
    TowerEligibility -> Set(ExactGlobalSnapshotRef, ChainSelectionWitness)
  )

  private val additionalArtifactRequirements: Map[ConsensusArtifactKind, Set[ArtifactBinding]] = Map(
    ShardCheckpointProposal -> (shardBase ++ Set(ExactInputCommitment)),
    ShardNamespaceDiff -> Set(ExactExecutionBase, ExactInputCommitment, NamespaceAndPreRoot, ReproducedExecutionResult),
    ShardExecutionSignature -> (shardBase ++ Set(
      ExactCheckpoint,
      ExactInputCommitment,
      ReproducedExecutionResult,
      RegistryView,
      ExecutionSignerIdentity
    )),
    ShardExecutionCertificate -> (shardBase ++ Set(
      ExactCheckpoint,
      ExactInputCommitment,
      ReproducedExecutionResult,
      RegistryView,
      ExecutionSignerIdentity
    )),
    PositiveWatchtowerCoverage -> (shardBase ++ Set(
      ExactCheckpoint,
      ExactInputCommitment,
      ReproducedExecutionResult,
      RegistryView,
      WatchtowerAssignmentContext,
      AssignedWatchtowerIdentity
    )),
    ShardChallengeEvidence -> Set(ExactOffenceParent, ExactCheckpoint, ExactInputCommitment, AccusedArtifacts),
    OptimisticTipAttestation -> (globalRef ++ globalParent ++ Set(AttestationDecisionContext)),
    TowerProof -> (globalRef ++ Set(
      AtomicKesVrfPair,
      N2RosterStakeAndKeyView,
      N1EtaEvidence,
      SelectionPeriodAndSlot,
      HistoricalTowerRootWitness,
      ChainSelectionWitness
    ))
  )

  private def transcriptContract(kind: ConsensusTranscriptKind): TranscriptContract =
    TranscriptContract(
      kind,
      Eligibility,
      ArtifactBinding.transcriptCommon ++ ArtifactBinding.eligibilityCommon ++ transcriptSpecificBindings(kind),
      TargetShapeOpen
    )

  val entries: List[ArtifactContract] = List(
    contract(ConsensusParameters, Commitment, Set(GenesisDeclaration), TargetShapeOpen),
    contract(OperatorConsensusIdentity, SourceAuthorization, Set(ExactActivationParent, RegistryView)),
    contract(NativeDagTransaction, SourceAuthorization, layerParent),
    contract(NativeDagBlock, SourceAuthorization, layerParent),
    contract(NativeAllowSpend, SourceAuthorization, layerParent),
    contract(NativeAllowSpendBlock, SourceAuthorization, layerParent),
    contract(NativeTokenLock, SourceAuthorization, layerParent),
    contract(NativeTokenLockBlock, SourceAuthorization, layerParent),
    contract(GlobalNodeParametersUpdate, SourceAuthorization, layerParent),
    contract(GlobalDelegatedStakeUpdate, SourceAuthorization, layerParent),
    contract(GlobalNodeCollateralUpdate, SourceAuthorization, layerParent),
    contract(
      GlobalGenesisSnapshot,
      Commitment,
      Set(GenesisDeclaration),
      KnownIncomplete,
      Set(OrdinalZeroGenesisUsesLegacyShape)
    ),
    contract(GlobalIncrementalSnapshot, StateValidity, globalParent),
    contract(GlobalSnapshotState, Commitment, embedded),
    contract(
      CurrencyGenesisSnapshot,
      Commitment,
      Set(GenesisDeclaration),
      KnownIncomplete,
      Set(OrdinalZeroGenesisUsesLegacyShape)
    ),
    contract(CurrencyIncrementalSnapshot, StateValidity, ml0Parent),
    contract(CurrencySnapshotState, Commitment, embedded),
    contract(
      FrameworkCurrencyEnvelope,
      SourceAuthorization,
      ml0Parent ++ Set(MetagraphAndLane),
      TargetShapeOpen,
      Set(ExplicitStateChannelLaneSchemaMissing, O18TransportByteContractOpen)
    ),
    contract(
      FrameworkCurrencyWithDataEnvelope,
      SourceAuthorization,
      ml0Parent ++ Set(MetagraphAndLane),
      TargetShapeOpen,
      Set(ExplicitStateChannelLaneSchemaMissing, O18TransportByteContractOpen)
    ),
    contract(
      DataOnlyEnvelope,
      SourceAuthorization,
      ml0Parent ++ Set(MetagraphAndLane),
      TargetShapeOpen,
      Set(ExplicitStateChannelLaneSchemaMissing, O18TransportByteContractOpen)
    ),
    contract(MetagraphSourceAuthentication, SourceAuthorization, ml0Parent ++ Set(MetagraphAndLane), TargetShapeOpen),
    contract(
      AdmissionCustodyReceipt,
      CustodyAvailability,
      Set(ExactSourceParent, RegistryView, CustodiedArtifact, RetentionScope),
      TargetShapeOpen,
      Set(O18TransportByteContractOpen)
    ),
    contract(CustomDataCommitment, Commitment, embedded, TargetShapeOpen, Set(O18TransportByteContractOpen)),
    contract(
      DataAvailabilityReceipt,
      CustodyAvailability,
      Set(ExactSourceParent, RegistryView, CustodiedArtifact, RetentionScope),
      TargetShapeOpen,
      Set(O18TransportByteContractOpen)
    ),
    contract(
      FrameworkEconomicIntent,
      SourceAuthorization,
      Set(ExactLayerParent, ExactPhase2Base, MetagraphAndLane),
      TargetShapeOpen,
      Set(EconomicGrammarOpen)
    ),
    contract(ShardCheckpointProposal, Commitment, additionalArtifactRequirements(ShardCheckpointProposal)),
    contract(
      ShardNamespaceDiff,
      Commitment,
      additionalArtifactRequirements(ShardNamespaceDiff),
      TargetShapeOpen,
      Set(CanonicalShardDiffMissing)
    ),
    contract(ShardExecutionSignature, StateValidity, additionalArtifactRequirements(ShardExecutionSignature)),
    contract(ShardExecutionCertificate, StateValidity, additionalArtifactRequirements(ShardExecutionCertificate), TargetShapeOpen),
    contract(
      PositiveWatchtowerCoverage,
      StateValidity,
      additionalArtifactRequirements(PositiveWatchtowerCoverage),
      TargetShapeOpen,
      Set(PositiveReplayCoverageSchemaMissing)
    ),
    contract(
      ShardChallengeEvidence,
      ObjectiveEvidence,
      additionalArtifactRequirements(ShardChallengeEvidence),
      TargetShapeOpen
    ),
    contract(
      GlobalSettlementTransition,
      Commitment,
      globalParent ++ Set(NamespaceAndPreRoot),
      TargetShapeOpen,
      Set(EconomicGrammarOpen)
    ),
    contract(
      GlobalFollowDelta,
      Commitment,
      globalRef,
      KnownIncomplete,
      Set(AccumulatorOmitsConsumedAllowSpends, AccumulatorOmitsSlashings, ChangeSetCannotCarryCompleteGlobalDelta)
    ),
    contract(
      DecidedOptimisticAttestationEvidence,
      FinalityQualification,
      globalRef ++ Set(AttestationDecisionContext),
      TargetShapeOpen
    ),
    contract(
      OptimisticTipAttestation,
      StateValidity,
      additionalArtifactRequirements(OptimisticTipAttestation),
      KnownIncomplete,
      Set(OptimisticTipAttestationMissingExactContext, OptimisticTipAttestationCarriesWallClock)
    ),
    contract(DepthK1Evidence, FinalityQualification, globalRef ++ Set(ChainSelectionWitness), TargetShapeOpen),
    contract(ForkChoiceEvidence, ObjectiveEvidence, globalRef ++ Set(ChainSelectionWitness), TargetShapeOpen),
    contract(
      TowerProof,
      ObjectiveEvidence,
      additionalArtifactRequirements(TowerProof),
      KnownIncomplete,
      Set(PortableTowerHistoricalEligibilityMissing)
    ),
    contract(GlobalMptKey, Commitment, embedded, KnownIncomplete, Set(GlobalMptPhysicalKeyGrammarUnenforced)),
    contract(GlobalMptValue, Commitment, embedded, KnownIncomplete, Set(SlashingValueCodecUsesJson)),
    contract(MptNodeCommitment, Commitment, embedded, ExistingShapeNeedsAudit),
    contract(GlobalMptRoot, Commitment, embedded),
    contract(MetagraphMptRoot, Commitment, embedded, TargetShapeOpen),
    contract(
      GlobalStateAccumulator,
      Commitment,
      embedded,
      KnownIncomplete,
      Set(AccumulatorOmitsConsumedAllowSpends, AccumulatorOmitsSlashings)
    ),
    contract(FinalityDurabilityRecord, LocalDurability, Set(DurabilityScope)),
    contract(MptDurabilityRecord, LocalDurability, Set(DurabilityScope)),
    contract(ChainAndReorgDurabilityRecord, LocalDurability, Set(DurabilityScope), TargetShapeOpen),
    contract(MigrationManifest, MigrationGenesis, Set(MigrationSource, MigrationTarget), OwnerDecisionBlocked, Set(O19MigrationPolicyOpen))
  )

  val transcriptEntries: List[TranscriptContract] = ConsensusTranscriptKind.all.map(transcriptContract)

  def validateEntries(contracts: List[ArtifactContract]): List[Violation] = {
    import Violation._

    val duplicateKinds = contracts.groupBy(_.kind).collect { case (kind, values) if values.sizeCompare(1) > 0 => DuplicateKind(kind) }
    val duplicateLabels = contracts
      .groupBy(_.kind.semanticLabel)
      .collect { case (label, values) if values.sizeCompare(1) > 0 => DuplicateSemanticLabel(label) }
    val missingKinds = (ConsensusArtifactKind.all.toSet -- contracts.map(_.kind).toSet).toList.map(MissingKind)
    val invalidLabels = contracts.collect {
      case contract if !contract.kind.semanticLabel.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*") => InvalidSemanticLabel(contract.kind)
    }
    val missingCommon = contracts.flatMap { contract =>
      val missing = ArtifactBinding.artifactCommon -- contract.bindings
      Option.when(missing.nonEmpty)(MissingCommonBindings(contract.kind, missing))
    }
    val missingAuthority = contracts.collect {
      case contract if !hasAuthorityBindings(contract) => MissingAuthorityBindings(contract.kind, contract.authority)
    }
    val missingArtifactBindings = contracts.flatMap { contract =>
      val missing = additionalArtifactRequirements.getOrElse(contract.kind, Set.empty) -- contract.bindings
      Option.when(missing.nonEmpty)(MissingArtifactBindings(contract.kind, missing))
    }
    val incompleteWithoutGap = contracts.collect {
      case contract if contract.definitionStatus == KnownIncomplete && contract.knownGaps.isEmpty => IncompleteWithoutGap(contract.kind)
    }
    val ownerBlockedWithoutGap = contracts.collect {
      case contract
          if contract.definitionStatus == OwnerDecisionBlocked &&
            contract.knownGaps.intersect(Set(O18TransportByteContractOpen, O19MigrationPolicyOpen)).isEmpty =>
        OwnerBlockedWithoutOwnerGap(contract.kind)
    }

    (duplicateKinds ++ duplicateLabels ++ missingKinds ++ invalidLabels ++ missingCommon ++ missingAuthority ++ missingArtifactBindings ++ incompleteWithoutGap ++ ownerBlockedWithoutGap).toList
      .sortBy(_.description)
  }

  val validation: List[Violation] = validateEntries(entries)

  def validateTranscriptEntries(contracts: List[TranscriptContract]): List[Violation] = {
    import Violation._

    val duplicateKinds = contracts.groupBy(_.kind).collect {
      case (kind, values) if values.sizeCompare(1) > 0 => DuplicateTranscriptKind(kind)
    }
    val duplicateLabels = contracts
      .groupBy(_.kind.semanticLabel)
      .collect { case (label, values) if values.sizeCompare(1) > 0 => DuplicateTranscriptSemanticLabel(label) }
    val missingKinds = (ConsensusTranscriptKind.all.toSet -- contracts.map(_.kind).toSet).toList.map(MissingTranscriptKind)
    val invalidLabels = contracts.collect {
      case contract if !contract.kind.semanticLabel.matches("[a-z0-9]+(?:[.-][a-z0-9]+)*") =>
        InvalidTranscriptSemanticLabel(contract.kind)
    }
    val invalidAuthorities = contracts.collect {
      case contract if contract.authority != Eligibility => InvalidTranscriptAuthority(contract.kind)
    }
    val missingBindings = contracts.flatMap { contract =>
      val required =
        ArtifactBinding.transcriptCommon ++ ArtifactBinding.eligibilityCommon ++ transcriptSpecificBindings.getOrElse(
          contract.kind,
          Set.empty
        )
      val missing = required -- contract.bindings
      Option.when(missing.nonEmpty)(MissingTranscriptBindings(contract.kind, missing))
    }

    (duplicateKinds ++ duplicateLabels ++ missingKinds ++ invalidLabels ++ invalidAuthorities ++ missingBindings).toList
      .sortBy(_.description)
  }

  val transcriptValidation: List[Violation] = validateTranscriptEntries(transcriptEntries)

  private def hasAuthorityBindings(contract: ArtifactContract): Boolean = {
    val hasAnchor = contract.bindings.exists(_.isExactAnchor)

    contract.authority match {
      case StateValidity => hasAnchor
      case FinalityQualification =>
        contract.bindings.contains(ExactGlobalSnapshotRef) &&
        (contract.bindings.contains(AttestationDecisionContext) || contract.bindings.contains(ChainSelectionWitness))
      case SourceAuthorization => hasAnchor
      case Eligibility         => hasAnchor && contract.bindings.contains(RegistryView) && contract.bindings.contains(EtaAndSlot)
      case CustodyAvailability =>
        contract.bindings.contains(ExactSourceParent) &&
        contract.bindings.contains(RegistryView) &&
        contract.bindings.contains(CustodiedArtifact) &&
        contract.bindings.contains(RetentionScope)
      case ObjectiveEvidence => hasAnchor
      case Commitment        => hasAnchor
      case LocalDurability   => contract.bindings.contains(DurabilityScope)
      case MigrationGenesis  => contract.bindings.contains(MigrationSource) && contract.bindings.contains(MigrationTarget)
    }
  }
}
