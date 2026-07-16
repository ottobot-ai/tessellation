package io.constellationnetwork.schema.consensus

sealed trait ArtifactDefinitionStatus extends Product with Serializable {
  def semanticLabel: String
}

object ArtifactDefinitionStatus {
  case object ExistingShapeNeedsAudit extends ArtifactDefinitionStatus {
    val semanticLabel: String = "existing-shape-needs-audit"
  }

  case object TargetShapeOpen extends ArtifactDefinitionStatus {
    val semanticLabel: String = "target-shape-open"
  }

  case object KnownIncomplete extends ArtifactDefinitionStatus {
    val semanticLabel: String = "known-incomplete"
  }

  case object OwnerDecisionBlocked extends ArtifactDefinitionStatus {
    val semanticLabel: String = "owner-decision-blocked"
  }
}

sealed trait ArtifactGap extends Product with Serializable {
  def semanticLabel: String
}

object ArtifactGap {
  case object OrdinalZeroGenesisUsesLegacyShape extends ArtifactGap {
    val semanticLabel: String = "ordinal-zero-genesis-uses-legacy-shape"
  }
  case object ExplicitStateChannelLaneSchemaMissing extends ArtifactGap {
    val semanticLabel: String = "explicit-state-channel-lane-schema-missing"
  }
  case object EconomicGrammarOpen extends ArtifactGap {
    val semanticLabel: String = "economic-grammar-open"
  }
  case object CanonicalShardDiffMissing extends ArtifactGap {
    val semanticLabel: String = "canonical-shard-diff-missing"
  }
  case object PositiveReplayCoverageSchemaMissing extends ArtifactGap {
    val semanticLabel: String = "positive-replay-coverage-schema-missing"
  }
  case object AccumulatorOmitsConsumedAllowSpends extends ArtifactGap {
    val semanticLabel: String = "accumulator-omits-consumed-allow-spends"
  }
  case object AccumulatorOmitsSlashings extends ArtifactGap {
    val semanticLabel: String = "accumulator-omits-slashings"
  }
  case object ChangeSetCannotCarryCompleteGlobalDelta extends ArtifactGap {
    val semanticLabel: String = "change-set-cannot-carry-complete-global-delta"
  }
  case object GlobalMptPhysicalKeyGrammarUnenforced extends ArtifactGap {
    val semanticLabel: String = "global-mpt-physical-key-grammar-unenforced"
  }
  case object SlashingValueCodecUsesJson extends ArtifactGap {
    val semanticLabel: String = "slashing-value-codec-uses-json"
  }
  case object OptimisticTipAttestationMissingExactContext extends ArtifactGap {
    val semanticLabel: String = "optimistic-tip-attestation-missing-exact-context"
  }
  case object OptimisticTipAttestationCarriesWallClock extends ArtifactGap {
    val semanticLabel: String = "optimistic-tip-attestation-carries-wall-clock"
  }
  case object PortableTowerHistoricalEligibilityMissing extends ArtifactGap {
    val semanticLabel: String = "portable-tower-historical-eligibility-missing"
  }
  case object FinalityOpaquePayloadSchemaMissing extends ArtifactGap {
    val semanticLabel: String = "finality-opaque-payload-schema-missing"
  }
  case object O18TransportByteContractOpen extends ArtifactGap {
    val semanticLabel: String = "o18-transport-byte-contract-open"
  }
  case object O19MigrationPolicyOpen extends ArtifactGap {
    val semanticLabel: String = "o19-migration-policy-open"
  }
}

sealed trait ManifestCodecStatus extends Product with Serializable
object ManifestCodecStatus {
  case object Open extends ManifestCodecStatus
}

sealed trait ManifestActivationStatus extends Product with Serializable
object ManifestActivationStatus {
  case object DarkOnly extends ManifestActivationStatus
}
