package io.constellationnetwork.schema.consensus

/** Closed inventory of grounded consensus-artifact families.
  *
  * `semanticLabel` is a review label, not a numeric wire tag or an encoded domain separator. Exact schemas and preimage bytes remain open.
  */
sealed abstract class ConsensusArtifactKind(val semanticLabel: String) extends Product with Serializable

object ConsensusArtifactKind {
  case object ConsensusParameters extends ConsensusArtifactKind("protocol.consensus-parameters")
  case object OperatorConsensusIdentity extends ConsensusArtifactKind("protocol.operator-consensus-identity")
  case object NativeDagTransaction extends ConsensusArtifactKind("gl1.native-dag-transaction")
  case object NativeDagBlock extends ConsensusArtifactKind("gl1.native-dag-block")
  case object NativeAllowSpend extends ConsensusArtifactKind("gl1.native-allow-spend")
  case object NativeAllowSpendBlock extends ConsensusArtifactKind("gl1.native-allow-spend-block")
  case object NativeTokenLock extends ConsensusArtifactKind("gl1.native-token-lock")
  case object NativeTokenLockBlock extends ConsensusArtifactKind("gl1.native-token-lock-block")
  case object GlobalNodeParametersUpdate extends ConsensusArtifactKind("gl0.event.node-parameters-update")
  case object GlobalDelegatedStakeUpdate extends ConsensusArtifactKind("gl0.event.delegated-stake-update")
  case object GlobalNodeCollateralUpdate extends ConsensusArtifactKind("gl0.event.node-collateral-update")

  case object GlobalGenesisSnapshot extends ConsensusArtifactKind("gl0.snapshot.genesis")
  case object GlobalIncrementalSnapshot extends ConsensusArtifactKind("gl0.snapshot.incremental")
  case object GlobalSnapshotState extends ConsensusArtifactKind("gl0.snapshot.state")
  case object CurrencyGenesisSnapshot extends ConsensusArtifactKind("ml0.snapshot.genesis")
  case object CurrencyIncrementalSnapshot extends ConsensusArtifactKind("ml0.snapshot.incremental")
  case object CurrencySnapshotState extends ConsensusArtifactKind("ml0.snapshot.state")

  case object FrameworkCurrencyEnvelope extends ConsensusArtifactKind("ml0.state-channel.framework-currency")
  case object FrameworkCurrencyWithDataEnvelope extends ConsensusArtifactKind("ml0.state-channel.framework-currency-with-data")
  case object DataOnlyEnvelope extends ConsensusArtifactKind("ml0.state-channel.data-only")
  case object MetagraphSourceAuthentication extends ConsensusArtifactKind("ml0.state-channel.source-authentication")
  case object AdmissionCustodyReceipt extends ConsensusArtifactKind("gl0.admission.custody-receipt")
  case object CustomDataCommitment extends ConsensusArtifactKind("ml0.custom-data.commitment")
  case object DataAvailabilityReceipt extends ConsensusArtifactKind("gl0.custom-data.availability-receipt")
  case object FrameworkEconomicIntent extends ConsensusArtifactKind("framework.economic-intent")

  case object ShardCheckpointProposal extends ConsensusArtifactKind("gl0.shard.checkpoint-proposal")
  case object ShardNamespaceDiff extends ConsensusArtifactKind("gl0.shard.namespace-confined-diff")
  case object ShardExecutionSignature extends ConsensusArtifactKind("gl0.shard.execution-signature")
  case object ShardExecutionCertificate extends ConsensusArtifactKind("gl0.shard.execution-certificate")
  case object PositiveWatchtowerCoverage extends ConsensusArtifactKind("gl0.shard.positive-watchtower-coverage")
  case object ShardChallengeEvidence extends ConsensusArtifactKind("gl0.shard.challenge-evidence")

  case object GlobalSettlementTransition extends ConsensusArtifactKind("gl0.settlement.transition")
  case object GlobalFollowDelta extends ConsensusArtifactKind("gl0.follow.global-delta")

  case object DecidedOptimisticAttestationEvidence extends ConsensusArtifactKind("gl0.finality.decided-optimistic-attestation")
  case object OptimisticTipAttestation extends ConsensusArtifactKind("gl0.finality.optimistic-tip-attestation")
  case object DepthK1Evidence extends ConsensusArtifactKind("gl0.finality.depth-k1-evidence")
  case object ForkChoiceEvidence extends ConsensusArtifactKind("gl0.finality.fork-choice-evidence")
  case object TowerProof extends ConsensusArtifactKind("gl0.tower.proof")

  case object GlobalMptKey extends ConsensusArtifactKind("gl0.mpt.key")
  case object GlobalMptValue extends ConsensusArtifactKind("gl0.mpt.value")
  case object MptNodeCommitment extends ConsensusArtifactKind("mpt.node.commitment")
  case object GlobalMptRoot extends ConsensusArtifactKind("gl0.mpt.root")
  case object MetagraphMptRoot extends ConsensusArtifactKind("gl0.mpt.metagraph-root")
  case object GlobalStateAccumulator extends ConsensusArtifactKind("gl0.mpt.state-accumulator")

  case object FinalityDurabilityRecord extends ConsensusArtifactKind("local.finality.record")
  case object MptDurabilityRecord extends ConsensusArtifactKind("local.mpt.record")
  case object ChainAndReorgDurabilityRecord extends ConsensusArtifactKind("local.chain-and-reorg.record")
  case object MigrationManifest extends ConsensusArtifactKind("migration.v4.manifest")

  val all: List[ConsensusArtifactKind] = List(
    ConsensusParameters,
    OperatorConsensusIdentity,
    NativeDagTransaction,
    NativeDagBlock,
    NativeAllowSpend,
    NativeAllowSpendBlock,
    NativeTokenLock,
    NativeTokenLockBlock,
    GlobalNodeParametersUpdate,
    GlobalDelegatedStakeUpdate,
    GlobalNodeCollateralUpdate,
    GlobalGenesisSnapshot,
    GlobalIncrementalSnapshot,
    GlobalSnapshotState,
    CurrencyGenesisSnapshot,
    CurrencyIncrementalSnapshot,
    CurrencySnapshotState,
    FrameworkCurrencyEnvelope,
    FrameworkCurrencyWithDataEnvelope,
    DataOnlyEnvelope,
    MetagraphSourceAuthentication,
    AdmissionCustodyReceipt,
    CustomDataCommitment,
    DataAvailabilityReceipt,
    FrameworkEconomicIntent,
    ShardCheckpointProposal,
    ShardNamespaceDiff,
    ShardExecutionSignature,
    ShardExecutionCertificate,
    PositiveWatchtowerCoverage,
    ShardChallengeEvidence,
    GlobalSettlementTransition,
    GlobalFollowDelta,
    DecidedOptimisticAttestationEvidence,
    OptimisticTipAttestation,
    DepthK1Evidence,
    ForkChoiceEvidence,
    TowerProof,
    GlobalMptKey,
    GlobalMptValue,
    MptNodeCommitment,
    GlobalMptRoot,
    MetagraphMptRoot,
    GlobalStateAccumulator,
    FinalityDurabilityRecord,
    MptDurabilityRecord,
    ChainAndReorgDurabilityRecord,
    MigrationManifest
  )
}
