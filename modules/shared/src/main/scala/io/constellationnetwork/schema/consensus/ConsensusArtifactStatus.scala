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
  case object DelimiterFreeRumorSignaturePreimage extends ArtifactGap {
    val semanticLabel: String = "delimiter-free-rumor-signature-preimage"
  }
  case object RuntimeScalaTypeStringDiscriminator extends ArtifactGap {
    val semanticLabel: String = "runtime-scala-type-string-discriminator"
  }
  case object TransportDispositionLacksPortableArtifactBinding extends ArtifactGap {
    val semanticLabel: String = "transport-disposition-lacks-portable-artifact-binding"
  }
  case object TransportHintLacksPortableArtifactBinding extends ArtifactGap {
    val semanticLabel: String = "transport-hint-lacks-portable-artifact-binding"
  }
  case object MissingTransportRequestIdentity extends ArtifactGap {
    val semanticLabel: String = "missing-transport-request-identity"
  }
  case object BootstrapPhase2BundleSchemaOpen extends ArtifactGap {
    val semanticLabel: String = "bootstrap-phase2-bundle-schema-open"
  }
  case object BootstrapGenesisBundleSchemaOpen extends ArtifactGap {
    val semanticLabel: String = "bootstrap-genesis-bundle-schema-open"
  }
  case object GossipSubFullProtoDedupMutationFlood extends ArtifactGap {
    val semanticLabel: String = "gossipsub-full-proto-dedup-mutation-flood"
  }
  case object UnboundedAggregateTransportResponse extends ArtifactGap {
    val semanticLabel: String = "unbounded-aggregate-transport-response"
  }
  case object BootstrapCurrentChainWitnessSchemaOpen extends ArtifactGap {
    val semanticLabel: String = "bootstrap-current-chain-witness-schema-open"
  }
  case object NestedMl0ConsensusArtifactManifestOpen extends ArtifactGap {
    val semanticLabel: String = "nested-ml0-consensus-artifact-manifest-open"
  }
  case object NestedArtifactIdentityNotEnforced extends ArtifactGap {
    val semanticLabel: String = "nested-artifact-identity-not-enforced"
  }
  case object LiveRumorSourceAuthenticationUndecomposed extends ArtifactGap {
    val semanticLabel: String = "live-rumor-source-authentication-undecomposed"
  }
  case object LocalSubscriptionProductionGateControl extends ArtifactGap {
    val semanticLabel: String = "local-subscription-production-gate-control"
  }
  case object LossyPeerRumorGapRepairMissing extends ArtifactGap {
    val semanticLabel: String = "lossy-peer-rumor-gap-repair-missing"
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
