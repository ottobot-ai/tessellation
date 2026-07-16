package io.constellationnetwork.schema.consensus

/** Semantic field that an eventual canonical identity or signature preimage must bind.
  *
  * Labels in this file are not wire tags or byte-domain separators. Exact encodings remain open.
  */
sealed trait ArtifactBinding extends Product with Serializable {
  def semanticLabel: String
  def isExactAnchor: Boolean = false
}

object ArtifactBinding {
  sealed trait ExactAnchor extends ArtifactBinding {
    override val isExactAnchor: Boolean = true
  }

  case object Network extends ArtifactBinding { val semanticLabel: String = "network" }
  case object Genesis extends ArtifactBinding { val semanticLabel: String = "genesis" }
  case object ProtocolEra extends ArtifactBinding { val semanticLabel: String = "protocol-era" }
  case object ParameterHash extends ArtifactBinding { val semanticLabel: String = "parameter-hash" }
  case object ArtifactKind extends ArtifactBinding { val semanticLabel: String = "artifact-kind" }
  case object TranscriptKind extends ArtifactBinding { val semanticLabel: String = "transcript-kind" }
  case object CanonicalContent extends ArtifactBinding { val semanticLabel: String = "canonical-content" }

  case object GenesisDeclaration extends ExactAnchor { val semanticLabel: String = "genesis-declaration" }
  case object ExactLayerParent extends ExactAnchor { val semanticLabel: String = "exact-layer-parent" }
  case object ExactGlobalParent extends ExactAnchor { val semanticLabel: String = "exact-global-parent" }
  case object ExactGlobalSnapshotRef extends ExactAnchor { val semanticLabel: String = "exact-global-snapshot-ref" }
  case object ExactPhase2Base extends ExactAnchor { val semanticLabel: String = "exact-phase2-base" }
  case object ExactMl0Parent extends ExactAnchor { val semanticLabel: String = "exact-ml0-parent" }
  case object ExactShardParent extends ExactAnchor { val semanticLabel: String = "exact-shard-parent" }
  case object ExactExecutionBase extends ExactAnchor { val semanticLabel: String = "exact-execution-base" }
  case object ExactSourceParent extends ExactAnchor { val semanticLabel: String = "exact-source-parent" }
  case object ExactActivationParent extends ExactAnchor { val semanticLabel: String = "exact-activation-parent" }
  case object ExactOffenceParent extends ExactAnchor { val semanticLabel: String = "exact-offence-parent" }
  case object ExactEligibilityParent extends ExactAnchor { val semanticLabel: String = "exact-eligibility-parent" }
  case object ExactCheckpoint extends ExactAnchor { val semanticLabel: String = "exact-checkpoint" }
  case object ExactInputCommitment extends ExactAnchor { val semanticLabel: String = "exact-input-commitment" }
  case object EmbeddingArtifact extends ExactAnchor { val semanticLabel: String = "embedding-artifact" }
  case object DurabilityScope extends ExactAnchor { val semanticLabel: String = "durability-scope" }
  case object MigrationSource extends ExactAnchor { val semanticLabel: String = "migration-source" }
  case object MigrationTarget extends ExactAnchor { val semanticLabel: String = "migration-target" }

  case object RegistryView extends ArtifactBinding { val semanticLabel: String = "registry-view" }
  case object EtaAndSlot extends ArtifactBinding { val semanticLabel: String = "eta-and-slot" }
  case object AtomicKesVrfPair extends ArtifactBinding { val semanticLabel: String = "atomic-kes-vrf-pair" }
  case object N2RosterStakeAndKeyView extends ArtifactBinding { val semanticLabel: String = "n2-roster-stake-and-key-view" }
  case object N1EtaEvidence extends ArtifactBinding { val semanticLabel: String = "n1-eta-evidence" }
  case object SelectionPeriodAndSlot extends ArtifactBinding { val semanticLabel: String = "selection-period-and-slot" }
  case object SelectionPurpose extends ArtifactBinding { val semanticLabel: String = "selection-purpose" }
  case object HistoricalTowerRootWitness extends ArtifactBinding { val semanticLabel: String = "historical-tower-root-witness" }
  case object ReproducedExecutionResult extends ArtifactBinding { val semanticLabel: String = "reproduced-execution-result" }
  case object ExecutionSignerIdentity extends ArtifactBinding { val semanticLabel: String = "execution-signer-identity" }
  case object WatchtowerAssignmentContext extends ArtifactBinding { val semanticLabel: String = "watchtower-assignment-context" }
  case object AssignedWatchtowerIdentity extends ArtifactBinding { val semanticLabel: String = "assigned-watchtower-identity" }
  case object AttestationDecisionContext extends ArtifactBinding { val semanticLabel: String = "attestation-decision-context" }
  case object ChainSelectionWitness extends ArtifactBinding { val semanticLabel: String = "chain-selection-witness" }
  case object MetagraphAndLane extends ArtifactBinding { val semanticLabel: String = "metagraph-and-lane" }
  case object ShardAndPeriod extends ArtifactBinding { val semanticLabel: String = "shard-and-period" }
  case object CustodiedArtifact extends ArtifactBinding { val semanticLabel: String = "custodied-artifact" }
  case object RetentionScope extends ArtifactBinding { val semanticLabel: String = "retention-scope" }
  case object AccusedArtifacts extends ArtifactBinding { val semanticLabel: String = "accused-artifacts" }
  case object NamespaceAndPreRoot extends ArtifactBinding { val semanticLabel: String = "namespace-and-pre-root" }

  val artifactCommon: Set[ArtifactBinding] = Set(Network, Genesis, ProtocolEra, ParameterHash, ArtifactKind, CanonicalContent)

  val transcriptCommon: Set[ArtifactBinding] = Set(Network, Genesis, ProtocolEra, ParameterHash, TranscriptKind, CanonicalContent)

  val eligibilityCommon: Set[ArtifactBinding] =
    Set(
      ExactEligibilityParent,
      AtomicKesVrfPair,
      N2RosterStakeAndKeyView,
      N1EtaEvidence,
      SelectionPeriodAndSlot,
      SelectionPurpose
    )

  val all: List[ArtifactBinding] = List(
    Network,
    Genesis,
    ProtocolEra,
    ParameterHash,
    ArtifactKind,
    TranscriptKind,
    CanonicalContent,
    GenesisDeclaration,
    ExactLayerParent,
    ExactGlobalParent,
    ExactGlobalSnapshotRef,
    ExactPhase2Base,
    ExactMl0Parent,
    ExactShardParent,
    ExactExecutionBase,
    ExactSourceParent,
    ExactActivationParent,
    ExactOffenceParent,
    ExactEligibilityParent,
    ExactCheckpoint,
    ExactInputCommitment,
    EmbeddingArtifact,
    DurabilityScope,
    MigrationSource,
    MigrationTarget,
    RegistryView,
    EtaAndSlot,
    AtomicKesVrfPair,
    N2RosterStakeAndKeyView,
    N1EtaEvidence,
    SelectionPeriodAndSlot,
    SelectionPurpose,
    HistoricalTowerRootWitness,
    ReproducedExecutionResult,
    ExecutionSignerIdentity,
    WatchtowerAssignmentContext,
    AssignedWatchtowerIdentity,
    AttestationDecisionContext,
    ChainSelectionWitness,
    MetagraphAndLane,
    ShardAndPeriod,
    CustodiedArtifact,
    RetentionScope,
    AccusedArtifacts,
    NamespaceAndPreRoot
  )
}
