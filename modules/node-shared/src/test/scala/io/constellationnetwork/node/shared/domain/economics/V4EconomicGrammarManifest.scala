package io.constellationnetwork.node.shared.domain.economics

/** Test-only, production-dark inventory of the framework-economic grammar inherited from Tessellation v4.0.0 and the fork surfaces that can
  * change its state.
  *
  * `V4Identity` answers whether a capability existed in upstream v4. `Activation` separately records what the current fork may safely do.
  * Keeping those axes separate prevents an unsafe implementation from silently deleting an intended v4 feature, and prevents feature
  * preservation from being mistaken for protocol approval.
  */
object V4EconomicGrammarManifest {

  sealed trait V4Identity extends Product with Serializable
  object V4Identity {
    case object Retained extends V4Identity
    case object RetainedVariant extends V4Identity
    case object RemovedWrongAuthority extends V4Identity
    case object ForkOnly extends V4Identity
    case object TargetOnly extends V4Identity
  }

  sealed trait Activation extends Product with Serializable
  object Activation {
    case object ActiveNeedsOracle extends Activation
    case object ActiveSourceProvenUnsafe extends Activation
    case object ActiveWithOpenConservationGate extends Activation
    case object RemovedFailClosed extends Activation
    case object MissingFailClosed extends Activation
    case object CarriageOnly extends Activation
  }

  sealed trait Authority extends Product with Serializable
  object Authority {
    case object SourceSignatureAndReference extends Authority
    case object SourceSignatureWithoutReference extends Authority
    case object RootedReferencedAllowSpend extends Authority
    case object DeterministicExpiry extends Authority
    case object RootedMetagraphOperatorThreshold extends Authority
    case object RootedNetworkAllowlist extends Authority
    case object ConsensusPinnedGlobalReplay extends Authority
    case object ExactDeterministicProtocolFee extends Authority
    case object AuthenticatedBinaryFeeClaimWithMinimum extends Authority
    case object AuthenticatedBinaryFeeClaimWithOwnerMessageAndMinimum extends Authority
    case object OptionalLocalConfigurationAllowlist extends Authority
    case object DeterministicRewardSchedule extends Authority
    case object ActiveEraGl0Protocol extends Authority
    case object ReplayVerifiedFraudEvidence extends Authority
    case object GenesisOrHardFork extends Authority
    case object AuthenticatedInclusionOnly extends Authority
    case object Missing extends Authority
  }

  sealed trait Effect extends Product with Serializable
  object Effect {
    case object BalanceDebit extends Effect
    case object BalanceCredit extends Effect
    case object FeeTransfer extends Effect
    case object Mint extends Effect
    case object Burn extends Effect
    case object Reserve extends Effect
    case object Release extends Effect
    case object ReferenceAdvance extends Effect
    case object NullifierWrite extends Effect
    case object SlashRegistryWrite extends Effect
    case object StakeBacking extends Effect
    case object CollateralBacking extends Effect
    case object RewardWeight extends Effect
    case object PriceState extends Effect
    case object ParameterState extends Effect
    case object Acknowledgement extends Effect
    case object DataCarriage extends Effect
  }

  final case class SourceAnchor(path: String, symbol: String, expectedDeclarations: Int = 1)

  final case class WireCarrier(anchor: SourceAnchor, operationIds: Set[String])

  final case class Operation(
    id: String,
    name: String,
    v4Identity: V4Identity,
    activation: Activation,
    intendedAuthority: Authority,
    currentAuthority: Authority,
    effects: Set[Effect],
    constructors: List[SourceAnchor],
    authoritySurfaces: Set[String],
    redObligations: Set[String]
  )

  sealed trait SurfaceRole extends Product with Serializable
  object SurfaceRole {
    case object Ingress extends SurfaceRole
    case object Codec extends SurfaceRole
    case object DecoderIngress extends SurfaceRole
    case object Validation extends SurfaceRole
    case object ConfigurationAuthority extends SurfaceRole
    case object GenesisIssuance extends SurfaceRole
    case object MigrationGate extends SurfaceRole
    case object RewardConstruction extends SurfaceRole
    case object FeeAuthority extends SurfaceRole
    case object BalanceWriter extends SurfaceRole
    case object ReservationWriter extends SurfaceRole
    case object ReferenceWriter extends SurfaceRole
    case object NullifierWriter extends SurfaceRole
    case object SlashRegistryWriter extends SurfaceRole
    case object StakeWriter extends SurfaceRole
    case object CollateralWriter extends SurfaceRole
    case object RewardWriter extends SurfaceRole
    case object PriceWriter extends SurfaceRole
    case object ParameterWriter extends SurfaceRole
  }

  /** A semantic fingerprint is over comment/whitespace-free Scala lexical tokens. It is intentionally stable under scalafmt and comment
    * edits. Any executable branch change requires the reviewer to reclassify this row and update the digest.
    */
  final case class ReviewedSource(
    path: String,
    roles: Set[SurfaceRole],
    operationIds: Set[String],
    semanticSha256: String
  )

  object Paths {
    val artifact = "modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala"
    val transaction = "modules/shared/src/main/scala/io/constellationnetwork/schema/transaction.scala"
    val swap = "modules/shared/src/main/scala/io/constellationnetwork/schema/swap.scala"
    val tokenLock = "modules/shared/src/main/scala/io/constellationnetwork/schema/tokenLock.scala"
    val delegatedStake = "modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala"
    val nodeCollateral = "modules/shared/src/main/scala/io/constellationnetwork/schema/nodeCollateral.scala"
    val currencyMessage = "modules/shared/src/main/scala/io/constellationnetwork/schema/currencyMessage.scala"
    val node = "modules/shared/src/main/scala/io/constellationnetwork/schema/node.scala"
    val dataApplication = "modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala"
    val currency = "modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala"
    val globalSync = "modules/shared/src/main/scala/io/constellationnetwork/currency/schema/globalSnapshotSync.scala"
    val stateChannelBinary = "modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala"
    val globalEvent =
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotEvent.scala"
    val block = "modules/shared/src/main/scala/io/constellationnetwork/schema/Block.scala"
    val currencyEvent = "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/snapshot/currency.scala"
    val globalSnapshot = "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshot.scala"
    val globalIncrementalSnapshot = "modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalIncrementalSnapshot.scala"
    val stateChannelOutput = "modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelOutput.scala"
    val shardCheckpoint = "modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala"
    val applicationConfig = "modules/node-shared/src/main/resources/application.conf"
  }

  import Activation._
  import Authority._
  import Effect._
  import Paths._
  import SurfaceRole._
  import V4Identity._

  val operations: List[Operation] = List(
    Operation(
      "ECO-TRANSFER-NATIVE",
      "native DAG transfer and embedded fee",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(BalanceDebit, BalanceCredit, Burn, ReferenceAdvance),
      List(SourceAnchor(transaction, "Transaction")),
      Set("native-block-acceptance", "global-transaction-reference"),
      Set("ECON-REF-001", "ECON-BAL-002", "ECON-F-005")
    ),
    Operation(
      "ECO-TRANSFER-CURRENCY",
      "currency-metagraph transfer and embedded fee",
      RetainedVariant,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(BalanceDebit, BalanceCredit, Burn, ReferenceAdvance),
      List(SourceAnchor(transaction, "Transaction")),
      Set("currency-block-acceptance", "currency-transaction-reference"),
      Set("ECON-REF-001", "ECON-BAL-002", "ECON-F-005")
    ),
    Operation(
      "ECO-REWARD-GLOBAL",
      "global reward issuance",
      Retained,
      ActiveSourceProvenUnsafe,
      DeterministicRewardSchedule,
      DeterministicRewardSchedule,
      Set(BalanceCredit, Mint, RewardWeight),
      List(SourceAnchor(transaction, "RewardTransaction")),
      Set("global-reward-construction", "global-reward-acceptance"),
      Set("ECON-G-001", "ECON-C-001")
    ),
    Operation(
      "ECO-REWARD-CURRENCY",
      "currency-metagraph reward issuance",
      RetainedVariant,
      MissingFailClosed,
      DeterministicRewardSchedule,
      Missing,
      Set(BalanceCredit, Mint, RewardWeight),
      List(SourceAnchor(transaction, "RewardTransaction")),
      Set("currency-reward-extension", "currency-reward-acceptance"),
      Set("ECON-G-001", "ECON-C-001")
    ),
    Operation(
      "ECO-ALLOW-CREATE",
      "allow-spend reservation create and fee",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(BalanceDebit, Reserve, Burn, ReferenceAdvance),
      List(SourceAnchor(swap, "AllowSpend")),
      Set("allow-spend-block-acceptance", "allow-spend-state"),
      Set("ECON-BAL-003", "ECON-REF-001", "ECON-F-005")
    ),
    Operation(
      "ECO-ALLOW-CONSUME",
      "allow-spend-backed spend",
      Retained,
      ActiveNeedsOracle,
      RootedReferencedAllowSpend,
      RootedReferencedAllowSpend,
      Set(Release, BalanceCredit, NullifierWrite),
      List(SourceAnchor(artifact, "SpendAction"), SourceAnchor(artifact, "SpendTransaction"), SourceAnchor(swap, "AllowSpend")),
      Set("rooted-allow-spend-authorization", "spend-action-validation", "spend-balance-application"),
      Set("ECON-BAL-003", "ECON-R-001")
    ),
    Operation(
      "ECO-MG-SOURCE-SPEND",
      "metagraph-source no-reference spend",
      RetainedVariant,
      ActiveSourceProvenUnsafe,
      RootedMetagraphOperatorThreshold,
      Missing,
      Set(BalanceDebit, BalanceCredit),
      List(SourceAnchor(artifact, "SpendAction"), SourceAnchor(artifact, "SpendTransaction")),
      Set("spend-action-no-ref-validation", "spend-balance-application"),
      Set("ECON-AUTH-002", "ECON-REPLAY-MGSPEND-001", "ECON-RESERVE-001")
    ),
    Operation(
      "ECO-ALLOW-EXPIRY",
      "allow-spend expiry and refund",
      Retained,
      ActiveNeedsOracle,
      DeterministicExpiry,
      DeterministicExpiry,
      Set(Release, BalanceCredit, Acknowledgement),
      List(SourceAnchor(artifact, "AllowSpendExpiration")),
      Set("allow-spend-expiry", "allow-spend-refund"),
      Set("ECON-F-001", "ECON-BAL-003")
    ),
    Operation(
      "ECO-TOKEN-LOCK-CREATE",
      "token-lock create and fee",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(BalanceDebit, Reserve, Burn, ReferenceAdvance),
      List(SourceAnchor(tokenLock, "TokenLock")),
      Set("token-lock-block-acceptance", "token-lock-state"),
      Set("ECON-BAL-002", "ECON-REF-001", "ECON-F-005")
    ),
    Operation(
      "ECO-TOKEN-LOCK-REPLACE",
      "token-lock replacement",
      RetainedVariant,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(Release, BalanceDebit, Reserve, Burn, ReferenceAdvance),
      List(SourceAnchor(tokenLock, "TokenLock")),
      Set("token-lock-replacement", "token-lock-state"),
      Set("ECON-BAL-003", "ECON-REF-001", "ECON-F-005")
    ),
    Operation(
      "ECO-TOKEN-LOCK-EXPIRY",
      "deterministic token-lock expiry",
      RetainedVariant,
      ActiveNeedsOracle,
      DeterministicExpiry,
      DeterministicExpiry,
      Set(Release, BalanceCredit),
      List(SourceAnchor(artifact, "TokenUnlock")),
      Set("token-lock-expiry", "token-lock-refund"),
      Set("ECON-AUTH-001", "ECON-F-001")
    ),
    Operation(
      "ECO-TOKEN-LOCK-MANUAL",
      "owner-triggered token unlock",
      Retained,
      ActiveSourceProvenUnsafe,
      SourceSignatureAndReference,
      Missing,
      Set(Release, BalanceCredit, NullifierWrite),
      List(SourceAnchor(artifact, "TokenUnlock")),
      Set("currency-token-unlock-intake", "data-application-artifact-extraction", "currency-token-unlock-refund"),
      Set("ECON-AUTH-001", "ECON-LANE-001", "ECON-R-001")
    ),
    Operation(
      "ECO-DATA-FEE",
      "framework fee bound to custom data",
      Retained,
      ActiveSourceProvenUnsafe,
      SourceSignatureAndReference,
      SourceSignatureWithoutReference,
      Set(BalanceDebit, BalanceCredit, FeeTransfer),
      List(SourceAnchor(dataApplication, "FeeTransaction")),
      Set("data-fee-validation", "currency-fee-acceptance"),
      Set("ECON-F-002", "ECON-F-003")
    ),
    Operation(
      "ECO-SNAPSHOT-FEE",
      "subsequent state-channel snapshot inclusion fee debit",
      Retained,
      ActiveSourceProvenUnsafe,
      ExactDeterministicProtocolFee,
      AuthenticatedBinaryFeeClaimWithOwnerMessageAndMinimum,
      Set(BalanceDebit, Burn, DataCarriage),
      List(SourceAnchor(currency, "SnapshotFee"), SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary")),
      Set("signed-state-channel-fee-claim", "owner-message-fee-payer", "global-state-channel-fee-debit"),
      Set("ECON-F-001", "ECON-F-004", "ECON-C-001", "ECON-R-001")
    ),
    Operation(
      "ECO-SNAPSHOT-FEE-NO-DEBIT",
      "genesis/full and first-incremental state-channel fee claim without ledger debit",
      RetainedVariant,
      ActiveSourceProvenUnsafe,
      ExactDeterministicProtocolFee,
      AuthenticatedBinaryFeeClaimWithMinimum,
      Set(DataCarriage),
      List(SourceAnchor(currency, "SnapshotFee"), SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary")),
      Set("signed-state-channel-fee-claim", "genesis-or-first-incremental-no-fee-debit"),
      Set("ECON-F-004A", "ECON-C-001")
    ),
    Operation(
      "ECO-PRICE",
      "allowlisted service-metagraph pricing update",
      Retained,
      ActiveSourceProvenUnsafe,
      RootedNetworkAllowlist,
      OptionalLocalConfigurationAllowlist,
      Set(PriceState),
      List(SourceAnchor(artifact, "PricingUpdate")),
      Set("pricing-update-validation", "price-state-update"),
      Set("ECON-G-002")
    ),
    Operation(
      "ECO-BALANCE-ADJUST-V4",
      "v4 metagraph-originated balance adjustment",
      RemovedWrongAuthority,
      RemovedFailClosed,
      Missing,
      Missing,
      Set(BalanceDebit, BalanceCredit),
      Nil,
      Set("removed-balance-adjustment-loader", "removed-currency-balance-adjuster"),
      Set("ECON-CORRECTION-001", "CORR-002")
    ),
    Operation(
      "ECO-GL0-CORRECTION",
      "active-era GL0 protocol correction",
      TargetOnly,
      MissingFailClosed,
      ActiveEraGl0Protocol,
      Missing,
      Set(BalanceDebit, BalanceCredit, Mint, Burn),
      Nil,
      Set("missing-protocol-correction"),
      Set("CORR-001", "CORR-001A", "CORR-002", "CORR-003")
    ),
    Operation(
      "ECO-GLOBAL-PROCESSED-ACK",
      "global SpendAction processing acknowledgement",
      Retained,
      ActiveSourceProvenUnsafe,
      ConsensusPinnedGlobalReplay,
      Missing,
      Set(Acknowledgement, ReferenceAdvance),
      List(SourceAnchor(artifact, "GlobalSnapshotsProcessed")),
      Set("global-spend-action-selection", "metagraph-sync-retirement"),
      Set("ECON-R-001")
    ),
    Operation(
      "ECO-CURRENCY-OWNER-MESSAGE",
      "currency owner address selection",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(ParameterState, ReferenceAdvance),
      List(SourceAnchor(currencyMessage, "CurrencyMessage")),
      Set("currency-message-validation", "currency-message-state"),
      Set("ECON-G-001", "ECON-R-001")
    ),
    Operation(
      "ECO-CURRENCY-STAKING-MESSAGE",
      "currency staking address selection",
      RetainedVariant,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(RewardWeight, ParameterState, ReferenceAdvance),
      List(SourceAnchor(currencyMessage, "CurrencyMessage")),
      Set("currency-message-validation", "currency-message-state"),
      Set("ECON-G-001", "ECON-R-001")
    ),
    Operation(
      "ECO-GLOBAL-SYNC",
      "currency exact global snapshot cursor",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(Acknowledgement, ReferenceAdvance),
      List(SourceAnchor(globalSync, "GlobalSnapshotSync")),
      Set("global-sync-validation", "global-sync-state"),
      Set("ECON-R-001", "FOLLOW-001")
    ),
    Operation(
      "ECO-DELEGATED-STAKE-CREATE",
      "delegated stake create/replacement",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(StakeBacking, ReferenceAdvance, RewardWeight),
      List(SourceAnchor(delegatedStake, "Create")),
      Set("delegated-stake-validation", "delegated-stake-state"),
      Set("ECON-G-001", "ECON-C-001", "ECON-F-006")
    ),
    Operation(
      "ECO-DELEGATED-STAKE-WITHDRAW",
      "delegated stake withdrawal request",
      RetainedVariant,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(StakeBacking, ReferenceAdvance, RewardWeight),
      List(SourceAnchor(delegatedStake, "Withdraw")),
      Set("delegated-stake-validation", "delegated-stake-state"),
      Set("ECON-G-001", "ECON-C-001", "ECON-F-006")
    ),
    Operation(
      "ECO-DELEGATED-STAKE-RELEASE",
      "delegated stake withdrawal expiry and token-lock release",
      RetainedVariant,
      ActiveNeedsOracle,
      DeterministicExpiry,
      DeterministicExpiry,
      Set(StakeBacking, Release, BalanceCredit),
      List(SourceAnchor(artifact, "TokenUnlock")),
      Set("delegated-stake-expiry", "generated-token-unlock", "token-lock-refund"),
      Set("ECON-G-001", "ECON-C-001", "ECON-F-006")
    ),
    Operation(
      "ECO-NODE-COLLATERAL-CREATE",
      "node collateral create/replacement",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(CollateralBacking, ReferenceAdvance),
      List(SourceAnchor(nodeCollateral, "Create")),
      Set("node-collateral-validation", "node-collateral-state"),
      Set("ECON-G-001", "ECON-C-001", "ECON-F-006")
    ),
    Operation(
      "ECO-NODE-COLLATERAL-WITHDRAW",
      "node collateral withdrawal request",
      RetainedVariant,
      ActiveSourceProvenUnsafe,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(CollateralBacking, ReferenceAdvance),
      List(SourceAnchor(nodeCollateral, "Withdraw")),
      Set("node-collateral-validation", "node-collateral-state"),
      Set("ECON-G-001", "ECON-C-001", "ECON-COLLATERAL-RELEASE-001", "ECON-F-006")
    ),
    Operation(
      "ECO-NODE-COLLATERAL-RELEASE",
      "node collateral withdrawal expiry and token-lock release",
      RetainedVariant,
      MissingFailClosed,
      DeterministicExpiry,
      Missing,
      Set(CollateralBacking, Release, BalanceCredit),
      Nil,
      Set("missing-node-collateral-token-unlock-release"),
      Set("ECON-COLLATERAL-RELEASE-001", "ECON-C-001")
    ),
    Operation(
      "ECO-NODE-PARAMETERS",
      "operator reward-fraction parameter update",
      Retained,
      ActiveNeedsOracle,
      SourceSignatureAndReference,
      SourceSignatureAndReference,
      Set(ParameterState, RewardWeight, ReferenceAdvance),
      List(SourceAnchor(node, "UpdateNodeParameters")),
      Set("node-parameter-validation", "node-parameter-state"),
      Set("ECON-G-001", "ECON-G-002")
    ),
    Operation(
      "ECO-CROSS-MG-NULLIFIER",
      "cross-metagraph allow-spend single-use settlement",
      ForkOnly,
      ActiveSourceProvenUnsafe,
      RootedReferencedAllowSpend,
      RootedReferencedAllowSpend,
      Set(NullifierWrite, BalanceCredit, Release, Acknowledgement),
      List(SourceAnchor(artifact, "SpendAction"), SourceAnchor(artifact, "SpendTransaction"), SourceAnchor(swap, "AllowSpend")),
      Set("rooted-allow-spend-authorization", "consumed-allow-spend-state", "metagraph-sync-retirement"),
      Set("ECON-R-001", "ECON-BAL-003", "XMG-001")
    ),
    Operation(
      "ECO-INVALID-STATE-SLASH",
      "invalid-state-proof backing-record slash and inflationary bounty",
      ForkOnly,
      ActiveWithOpenConservationGate,
      ReplayVerifiedFraudEvidence,
      ReplayVerifiedFraudEvidence,
      Set(StakeBacking, CollateralBacking, BalanceCredit, Mint, Burn, SlashRegistryWrite),
      Nil,
      Set("invalid-state-proof-slash", "watchtower-slash-application", "slash-registry"),
      Set("SLASH-PRINCIPAL-001", "ECON-C-001", "ECON-B-001")
    ),
    Operation(
      "ECO-OTHER-SLASH-TIERS",
      "checkpoint/metagraph equivocation and nonparticipation economic slash tiers",
      TargetOnly,
      MissingFailClosed,
      ReplayVerifiedFraudEvidence,
      Missing,
      Set(StakeBacking, CollateralBacking, BalanceCredit, Burn, SlashRegistryWrite),
      Nil,
      Set("typed-reasons-only-no-ledger-sink"),
      Set("SLASH-PRINCIPAL-001", "ECON-C-001")
    ),
    Operation(
      "ECO-GENESIS-ISSUANCE",
      "genesis balance allocation",
      Retained,
      ActiveNeedsOracle,
      GenesisOrHardFork,
      GenesisOrHardFork,
      Set(BalanceCredit, Mint),
      Nil,
      Set("global-genesis-balances", "currency-genesis-balances"),
      Set("ECON-C-001", "V4-GRAMMAR-PARITY-GENESIS")
    ),
    Operation(
      "ECO-MIGRATION-ISSUANCE",
      "offline upstream-v4 snapshot to new-chain genesis issuance",
      TargetOnly,
      MissingFailClosed,
      GenesisOrHardFork,
      Missing,
      Set(BalanceCredit, Mint, Burn, Reserve, NullifierWrite, StakeBacking, CollateralBacking),
      Nil,
      Set("missing-offline-v4-export-transform"),
      Set("MIG-001", "MIG-002", "MIG-003", "MIG-004", "MIG-005", "MIG-005A", "MIG-006")
    ),
    Operation(
      "ECO-GENESIS-DELEGATED-STAKE",
      "genesis delegated stake created without a backing token lock",
      RetainedVariant,
      ActiveSourceProvenUnsafe,
      GenesisOrHardFork,
      GenesisOrHardFork,
      Set(StakeBacking, RewardWeight, Mint),
      List(SourceAnchor(delegatedStake, "Create")),
      Set("genesis-signed-delegated-stake-fixture", "genesis-empty-token-lock-state"),
      Set("ECON-GENESIS-BACKING-001", "ECON-C-001")
    ),
    Operation(
      "ECO-GENESIS-NODE-COLLATERAL",
      "genesis node collateral created without a backing token lock",
      RetainedVariant,
      ActiveSourceProvenUnsafe,
      GenesisOrHardFork,
      GenesisOrHardFork,
      Set(CollateralBacking),
      List(SourceAnchor(nodeCollateral, "Create")),
      Set("genesis-signed-node-collateral-fixture", "genesis-empty-token-lock-state"),
      Set("ECON-GENESIS-BACKING-001", "ECON-C-001")
    ),
    Operation(
      "ECO-FRAMEWORK-DATA-CARRIAGE",
      "custom data carried inside a framework currency snapshot",
      Retained,
      CarriageOnly,
      AuthenticatedInclusionOnly,
      AuthenticatedInclusionOnly,
      Set(DataCarriage),
      List(SourceAnchor(dataApplication, "DataApplicationBlock")),
      Set("currency-with-data-application-lane"),
      Set("ECON-LANE-001", "V4-GRAMMAR-DOMAIN-001")
    ),
    Operation(
      "ECO-OPAQUE-CARRIAGE",
      "standalone opaque/data-only state-channel payload before fee activation or while fee-waived",
      Retained,
      CarriageOnly,
      AuthenticatedInclusionOnly,
      AuthenticatedInclusionOnly,
      Set(DataCarriage),
      List(SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary")),
      Set("state-channel-binary-ingress", "pre-fee-or-fee-waived-opaque-chain"),
      Set("ECON-LANE-001", "V4-GRAMMAR-DOMAIN-001")
    ),
    Operation(
      "ECO-OPAQUE-CARRIAGE-FEE-ERA",
      "standalone opaque/data-only state-channel payload after fee activation",
      RetainedVariant,
      MissingFailClosed,
      AuthenticatedInclusionOnly,
      Missing,
      Set(DataCarriage),
      List(SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary")),
      Set("fee-required-opaque-chain-rejected"),
      Set("ECON-OPAQUE-FEE-001", "ECON-LANE-001")
    ),
    Operation(
      "ECO-OPAQUE-CARRIAGE-SHARDED",
      "standalone opaque/data-only state-channel payload with execution sharding enabled",
      RetainedVariant,
      MissingFailClosed,
      AuthenticatedInclusionOnly,
      Missing,
      Set(DataCarriage),
      List(SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary")),
      Set("sharded-opaque-carriage-route-missing"),
      Set("ECON-OPAQUE-SHARD-001", "ECON-LANE-001")
    )
  )

  private val nativeTransferIds = Set("ECO-TRANSFER-NATIVE")
  private val currencyTransferIds = Set("ECO-TRANSFER-CURRENCY")
  private val transferIds = nativeTransferIds ++ currencyTransferIds
  private val globalRewardIds = Set("ECO-REWARD-GLOBAL")
  private val currencyRewardIds = Set("ECO-REWARD-CURRENCY")
  private val rewardIds = globalRewardIds ++ currencyRewardIds
  private val allowSpendIds = Set("ECO-ALLOW-CREATE", "ECO-ALLOW-CONSUME", "ECO-MG-SOURCE-SPEND", "ECO-ALLOW-EXPIRY")
  private val tokenLockIds = Set(
    "ECO-TOKEN-LOCK-CREATE",
    "ECO-TOKEN-LOCK-REPLACE",
    "ECO-TOKEN-LOCK-EXPIRY",
    "ECO-TOKEN-LOCK-MANUAL"
  )
  private val tokenUnlockIds = Set(
    "ECO-TOKEN-LOCK-EXPIRY",
    "ECO-TOKEN-LOCK-MANUAL",
    "ECO-DELEGATED-STAKE-RELEASE"
  )
  private val messageIds = Set("ECO-CURRENCY-OWNER-MESSAGE", "ECO-CURRENCY-STAKING-MESSAGE")
  private val delegatedStakeIds = Set("ECO-DELEGATED-STAKE-CREATE", "ECO-DELEGATED-STAKE-WITHDRAW")
  private val nodeCollateralIds = Set("ECO-NODE-COLLATERAL-CREATE", "ECO-NODE-COLLATERAL-WITHDRAW")
  private val delegatedStakeReleaseIds = Set("ECO-DELEGATED-STAKE-RELEASE")
  private val slashingIds = Set("ECO-INVALID-STATE-SLASH", "ECO-OTHER-SLASH-TIERS")
  private val genesisBackingIds = Set("ECO-GENESIS-DELEGATED-STAKE", "ECO-GENESIS-NODE-COLLATERAL")
  private val frameworkDataIds = Set("ECO-FRAMEWORK-DATA-CARRIAGE")
  private val opaqueDataIds =
    Set("ECO-OPAQUE-CARRIAGE", "ECO-OPAQUE-CARRIAGE-FEE-ERA", "ECO-OPAQUE-CARRIAGE-SHARDED")
  private val dataApplicationIds = Set("ECO-DATA-FEE") ++ frameworkDataIds
  private val snapshotFeeIds = Set("ECO-SNAPSHOT-FEE", "ECO-SNAPSHOT-FEE-NO-DEBIT")
  private val stateChannelIds = snapshotFeeIds ++ opaqueDataIds
  private val frameworkArtifactIds = Set(
    "ECO-ALLOW-CONSUME",
    "ECO-MG-SOURCE-SPEND",
    "ECO-ALLOW-EXPIRY",
    "ECO-TOKEN-LOCK-EXPIRY",
    "ECO-TOKEN-LOCK-MANUAL",
    "ECO-DELEGATED-STAKE-RELEASE",
    "ECO-PRICE",
    "ECO-CROSS-MG-NULLIFIER"
  )
  private val currencyFrameworkIds =
    currencyTransferIds ++ currencyRewardIds ++ allowSpendIds ++ tokenLockIds ++ messageIds ++ Set(
      "ECO-DATA-FEE",
      "ECO-DELEGATED-STAKE-RELEASE",
      "ECO-PRICE",
      "ECO-GLOBAL-PROCESSED-ACK",
      "ECO-GLOBAL-SYNC",
      "ECO-CROSS-MG-NULLIFIER"
    ) ++ frameworkDataIds
  private val currencyFullSnapshotIds = currencyTransferIds ++ currencyRewardIds ++ Set(
    "ECO-GENESIS-ISSUANCE",
    "ECO-GLOBAL-SYNC"
  ) ++ dataApplicationIds
  private val allStateChannelBinaryIds = currencyFullSnapshotIds ++ currencyFrameworkIds ++ stateChannelIds
  private val currencyEventIds = currencyTransferIds ++ Set(
    "ECO-ALLOW-CREATE",
    "ECO-TOKEN-LOCK-CREATE",
    "ECO-TOKEN-LOCK-REPLACE",
    "ECO-CURRENCY-OWNER-MESSAGE",
    "ECO-CURRENCY-STAKING-MESSAGE",
    "ECO-GLOBAL-SYNC"
  ) ++ dataApplicationIds
  private val currentlyExecutableIds = operations.collect {
    case operation if operation.activation != RemovedFailClosed && operation.activation != MissingFailClosed =>
      operation.id
  }.toSet
  private val genesisOnlyIds = genesisBackingIds ++ Set("ECO-GENESIS-ISSUANCE")
  private val currentRootedEconomicIds = currentlyExecutableIds -- opaqueDataIds -- frameworkDataIds -- genesisOnlyIds
  private val balanceEffects: Set[Effect] =
    Set(BalanceDebit, BalanceCredit, FeeTransfer, Mint, Burn, Reserve, Release)
  private val currentBalanceEffectIds = operations.collect {
    case operation if currentlyExecutableIds(operation.id) && operation.effects.exists(balanceEffects) => operation.id
  }.toSet
  private val stakeLifecycleIds =
    delegatedStakeIds ++ delegatedStakeReleaseIds ++ nodeCollateralIds ++ genesisBackingIds ++ globalRewardIds ++ slashingIds ++ Set(
      "ECO-NODE-PARAMETERS"
    )

  val wireCarriers: List[WireCarrier] = List(
    WireCarrier(SourceAnchor(block, "Block"), transferIds),
    WireCarrier(SourceAnchor(swap, "AllowSpendBlock"), Set("ECO-ALLOW-CREATE")),
    WireCarrier(SourceAnchor(tokenLock, "TokenLockBlock"), Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE")),
    WireCarrier(SourceAnchor(dataApplication, "DataApplicationBlock"), dataApplicationIds),
    WireCarrier(SourceAnchor(currency, "CurrencySnapshot"), currencyFullSnapshotIds),
    WireCarrier(SourceAnchor(currency, "CurrencyIncrementalSnapshot"), currencyFrameworkIds ++ dataApplicationIds),
    WireCarrier(SourceAnchor(globalSnapshot, "GlobalSnapshot"), nativeTransferIds ++ globalRewardIds ++ Set("ECO-GENESIS-ISSUANCE")),
    WireCarrier(
      SourceAnchor(globalIncrementalSnapshot, "GlobalIncrementalSnapshot"),
      currentRootedEconomicIds -- Set("ECO-GENESIS-ISSUANCE")
    ),
    WireCarrier(
      SourceAnchor(stateChannelBinary, "StateChannelSnapshotBinary"),
      allStateChannelBinaryIds
    ),
    WireCarrier(
      SourceAnchor(stateChannelOutput, "StateChannelOutput"),
      allStateChannelBinaryIds
    ),
    WireCarrier(
      SourceAnchor(shardCheckpoint, "ShardCheckpoint"),
      allStateChannelBinaryIds ++ slashingIds
    ),
    WireCarrier(SourceAnchor(currencyEvent, "CurrencySnapshotEvent"), currencyEventIds),
    WireCarrier(SourceAnchor(globalEvent, "GlobalSnapshotEvent"), currentRootedEconomicIds -- Set("ECO-GENESIS-ISSUANCE"))
  )

  /** Authority/writer files are deliberately narrower than every validator and route: these are the executable files that can derive or
    * commit an economic write. The companion guard independently discovers new writer-shaped files, so omission is fail-closed.
    */
  val reviewedSources: List[ReviewedSource] = List(
    ReviewedSource(
      artifact,
      Set(Ingress),
      Set(
        "ECO-ALLOW-CONSUME",
        "ECO-MG-SOURCE-SPEND",
        "ECO-ALLOW-EXPIRY",
        "ECO-TOKEN-LOCK-EXPIRY",
        "ECO-TOKEN-LOCK-MANUAL",
        "ECO-DELEGATED-STAKE-RELEASE",
        "ECO-PRICE",
        "ECO-GLOBAL-PROCESSED-ACK"
      ),
      "f7ed647aec2fbba61fee35fd8a4251966b594b2efc1d77c34e203ecdda2982ab"
    ),
    ReviewedSource(
      transaction,
      Set(Ingress),
      Set("ECO-TRANSFER-NATIVE", "ECO-TRANSFER-CURRENCY", "ECO-REWARD-GLOBAL", "ECO-REWARD-CURRENCY"),
      "61ec665b0f5aa76950f5e1569e3c24fffd7c2382aec8e63e61d2afecb6bb0f9d"
    ),
    ReviewedSource(
      swap,
      Set(Ingress, NullifierWriter),
      Set("ECO-ALLOW-CREATE", "ECO-CROSS-MG-NULLIFIER"),
      "8dc7c11aa80ddc9ff05fbdcae3f099f22b08ed4c8cb3172e0d6a8d51ab9c32d7"
    ),
    ReviewedSource(
      tokenLock,
      Set(Ingress),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "fc35b1d305bb776794613bdd3a1183e31c02549eae1e1a7d2874fd2ea4601d68"
    ),
    ReviewedSource(
      delegatedStake,
      Set(Ingress),
      delegatedStakeIds ++ delegatedStakeReleaseIds ++ Set("ECO-GENESIS-DELEGATED-STAKE"),
      "f43499e3fb57a81dd55ea09828fbe476467216f7ac42c480815be60cf85239a5"
    ),
    ReviewedSource(
      nodeCollateral,
      Set(Ingress),
      Set("ECO-NODE-COLLATERAL-CREATE", "ECO-NODE-COLLATERAL-WITHDRAW", "ECO-GENESIS-NODE-COLLATERAL"),
      "da6f7d65286e64872e90b1d98adb00aeee4807f51dff8e53aad74c5767fd6b48"
    ),
    ReviewedSource(
      currencyMessage,
      Set(Ingress),
      Set("ECO-CURRENCY-OWNER-MESSAGE", "ECO-CURRENCY-STAKING-MESSAGE"),
      "de4551d813a17a64776a7bfdd85182f4cf58b3737545d005b72e324c33f05cdf"
    ),
    ReviewedSource(
      node,
      Set(Ingress),
      Set("ECO-NODE-PARAMETERS"),
      "1d021ff0a5f225175469dd49dc7698b16c9db607662f22678b4bd57a22b3c140"
    ),
    ReviewedSource(
      dataApplication,
      Set(Ingress),
      dataApplicationIds,
      "10a1ca6f06a72a50499e0349299fd44e4ac816fc03c7cf62dc9ac937485f5794"
    ),
    ReviewedSource(
      currency,
      Set(Ingress),
      Set(
        "ECO-REWARD-CURRENCY",
        "ECO-DATA-FEE",
        "ECO-GLOBAL-PROCESSED-ACK",
        "ECO-FRAMEWORK-DATA-CARRIAGE"
      ) ++ snapshotFeeIds,
      "cd259568ec01191bfbe0275a22035eb4782d2cea76650dea6b7b1b5d95a197ec"
    ),
    ReviewedSource(
      globalSync,
      Set(Ingress, ReferenceWriter),
      Set("ECO-GLOBAL-SYNC"),
      "2f03514df2ac5c997f2d1d6b85fd9f2e3def722b6411dbc8f9fdf465288c2057"
    ),
    ReviewedSource(
      stateChannelBinary,
      Set(Ingress),
      allStateChannelBinaryIds,
      "1257ed7fdf28d3ac4857742435ed96156d1e1435f2ea84d379b569334d835f60"
    ),
    ReviewedSource(
      globalEvent,
      Set(Ingress),
      Set(
        "ECO-TRANSFER-NATIVE",
        "ECO-ALLOW-CREATE",
        "ECO-TOKEN-LOCK-CREATE",
        "ECO-NODE-PARAMETERS",
        "ECO-DELEGATED-STAKE-CREATE",
        "ECO-DELEGATED-STAKE-WITHDRAW",
        "ECO-NODE-COLLATERAL-CREATE",
        "ECO-NODE-COLLATERAL-WITHDRAW"
      ) ++ stateChannelIds,
      "4e9a5ead02b94eb2a9cf98a8045a8d2bc461921ff2f1156106f9c7c26768d225"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/block/processing/BlockAcceptanceLogic.scala",
      Set(Validation, BalanceWriter, ReferenceWriter),
      Set("ECO-TRANSFER-NATIVE", "ECO-TRANSFER-CURRENCY"),
      "5a079a33064a6fac2874b50f7c0b2d95128e3930dbc82bc6fbdc0620b8f39e77"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/BalanceOpsManager.scala",
      Set(Validation, BalanceWriter, RewardWriter, ReferenceWriter),
      Set("ECO-REWARD-CURRENCY", "ECO-DATA-FEE"),
      "c282a90b979f1a0b44e9cafadbfacb16279a7b9df477b59322f90163b70a72a4"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/AllowSpendOpsManager.scala",
      Set(Validation, BalanceWriter, ReservationWriter, ReferenceWriter),
      Set("ECO-ALLOW-CREATE", "ECO-ALLOW-CONSUME", "ECO-ALLOW-EXPIRY", "ECO-MG-SOURCE-SPEND"),
      "a98672e1c1559bdf295c0378471fd793654cf3a7a0cb3e74b4bd9b3c1091ec4c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala",
      Set(Validation, BalanceWriter, ReservationWriter, ReferenceWriter),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE") ++ tokenUnlockIds,
      "aebb1205223fc34ef43a1b2e1fb282f85cebf90200210d28d5a1aa370c509bcb"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala",
      Set(Ingress, Validation, BalanceWriter, ReservationWriter, ReferenceWriter, RewardWriter),
      Set(
        "ECO-TRANSFER-CURRENCY",
        "ECO-REWARD-CURRENCY",
        "ECO-ALLOW-CREATE",
        "ECO-ALLOW-CONSUME",
        "ECO-ALLOW-EXPIRY",
        "ECO-MG-SOURCE-SPEND",
        "ECO-TOKEN-LOCK-CREATE",
        "ECO-TOKEN-LOCK-REPLACE",
        "ECO-TOKEN-LOCK-EXPIRY",
        "ECO-TOKEN-LOCK-MANUAL",
        "ECO-DELEGATED-STAKE-RELEASE",
        "ECO-DATA-FEE",
        "ECO-GLOBAL-PROCESSED-ACK"
      ),
      "b57c1dca23c699cc1656aa581838d0797a569d38860345da16216c72c6b9d95e"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/GlobalSnapshotOpsManager.scala",
      Set(Validation, ReferenceWriter),
      Set("ECO-GLOBAL-PROCESSED-ACK", "ECO-CROSS-MG-NULLIFIER"),
      "3a5b81580889334f977c560e45c10391d6b6bfdad08e4d0b83c1d13106722276"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/RewardAcceptanceManager.scala",
      Set(BalanceWriter, RewardWriter),
      Set("ECO-REWARD-GLOBAL"),
      "126b3f5997d08d503ae214d4d464fc4d81b163ccc0cb1a14e9378f85cfb9fd4f"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/AllowSpendStateManager.scala",
      Set(BalanceWriter, ReservationWriter, ReferenceWriter),
      Set("ECO-ALLOW-CREATE", "ECO-ALLOW-CONSUME", "ECO-ALLOW-EXPIRY"),
      "8ebf1f0e84e6cab313049bb8963398136dac7b9209d10621b6c028279c115641"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/TokenLockStateManager.scala",
      Set(BalanceWriter, ReservationWriter, ReferenceWriter),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE", "ECO-TOKEN-LOCK-EXPIRY", "ECO-DELEGATED-STAKE-RELEASE"),
      "44cad4d7d2278c0d0017a6616683e00c7cf429dab3738f2a661ef377013521d7"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/SpendTransactionBalanceManager.scala",
      Set(Validation, BalanceWriter, NullifierWriter),
      Set("ECO-ALLOW-CONSUME", "ECO-MG-SOURCE-SPEND"),
      "12ec7cfa44ee9cd31538dac163d84bf74cd7352105b080de916b4aee3d4a762d"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ConsumedAllowSpendStateManager.scala",
      Set(BalanceWriter, NullifierWriter),
      Set("ECO-CROSS-MG-NULLIFIER"),
      "26e83b28306f5ff657047a25a08cfbc2a8149791cc56045f2cec2a4c9448d321"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/DelegatedStakeStateManager.scala",
      Set(BalanceWriter, StakeWriter, ReferenceWriter),
      delegatedStakeIds ++ delegatedStakeReleaseIds,
      "4f6ccfea2aceadd6707b8556982d52298353955651ebbc46b224387650699b35"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/NodeCollateralStateManager.scala",
      Set(BalanceWriter, CollateralWriter, ReferenceWriter),
      Set("ECO-NODE-COLLATERAL-CREATE", "ECO-NODE-COLLATERAL-WITHDRAW"),
      "935a0fa6bd1caab9ee7e2df6bf40891c6bb27c81d6b39297f7c168518f07f4d4"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/MetagraphSyncManager.scala",
      Set(ReferenceWriter, NullifierWriter),
      Set("ECO-GLOBAL-PROCESSED-ACK", "ECO-CROSS-MG-NULLIFIER"),
      "19433f80e7ab0d782f4044d31ff10be051b6637fdfdf6c1b3588cd5f889463e7"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala",
      Set(Ingress, Validation, BalanceWriter),
      allStateChannelBinaryIds,
      "09542005cd61c22a41941ef6f25c600dfb613af2859e16c3bcc1b4276e79de35"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala",
      Set(
        Ingress,
        Validation,
        BalanceWriter,
        ReservationWriter,
        ReferenceWriter,
        NullifierWriter,
        StakeWriter,
        CollateralWriter,
        RewardWriter,
        PriceWriter,
        ParameterWriter,
        SlashRegistryWriter
      ),
      currentRootedEconomicIds ++ allStateChannelBinaryIds,
      "4c72575e536974fe8f878016117b5a9d3c8568b8dbfa95d9c742d7e3dc986978"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala",
      Set(BalanceWriter, StakeWriter, CollateralWriter, SlashRegistryWriter),
      Set("ECO-INVALID-STATE-SLASH", "ECO-OTHER-SLASH-TIERS"),
      "40a53c79fd9b8f9a58715d7c8d2cb724313eee475c115e6875a766e6a247e871"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/priceOracle/PricingUpdateValidator.scala",
      Set(Validation),
      Set("ECO-PRICE"),
      "b557d5b2b5eefa8cfc28a253906e05b9a335a5a772e92465de8e3358e0088c9f"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/priceOracle/PriceStateUpdater.scala",
      Set(PriceWriter),
      Set("ECO-PRICE"),
      "4165a876cdf82e8db16b0070b24b98556d8c8e1cc4992c70aac238dd791ddf5b"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala",
      Set(Validation),
      Set("ECO-ALLOW-CONSUME", "ECO-MG-SOURCE-SPEND", "ECO-CROSS-MG-NULLIFIER"),
      "8099fc8699baf06a0dafcb795c7a21263eaa4df312bae918a026c59636ded090"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala",
      Set(BalanceWriter, ReservationWriter, ReferenceWriter, NullifierWriter, StakeWriter, CollateralWriter, PriceWriter),
      currentRootedEconomicIds,
      "18617676e7bb768b0d2267e4b60524f6cbc1c2c9bbc5342ffe966a90df7fb407"
    ),
    ReviewedSource(
      block,
      Set(Ingress),
      transferIds,
      "623818de60b765495fa1f7cb9762d9358e3f06e57d2054c7c5a9c1947630d98b"
    ),
    ReviewedSource(
      currencyEvent,
      Set(Ingress),
      currencyFrameworkIds,
      "961f9f74cfded6a7333d264787b55ffce2da2edf974c380619993f819ad8fdf3"
    ),
    ReviewedSource(
      globalSnapshot,
      Set(Ingress, GenesisIssuance),
      nativeTransferIds ++ globalRewardIds ++ Set("ECO-GENESIS-ISSUANCE"),
      "9da29bad1506e01d1b9747683ae6e845eb8638e4ce1ddb1721532cd3fd5fd3a1"
    ),
    ReviewedSource(
      globalIncrementalSnapshot,
      Set(Ingress),
      currentRootedEconomicIds -- Set("ECO-GENESIS-ISSUANCE"),
      "0bcc50329b570be6005aacc1bf486afc737a1dfa9ba4cab514c319f685717dcb"
    ),
    ReviewedSource(
      stateChannelOutput,
      Set(Ingress),
      allStateChannelBinaryIds,
      "e207ddc4189f152a703dce09c836c7be58d7f2bfc1ee204949a58fb591cc6cd5"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/ExpiryIndexKeys.scala",
      Set(Ingress, ReservationWriter, ReferenceWriter),
      allowSpendIds ++ tokenLockIds ++ delegatedStakeReleaseIds ++ Set("ECO-NODE-COLLATERAL-WITHDRAW"),
      "ef3634b1e1d239f9ff6c2c6431e2553b4e49c3037e0617549deb996b92e34934"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/slashing/InvalidStateProofEvidence.scala",
      Set(Ingress),
      Set("ECO-INVALID-STATE-SLASH"),
      "e36980db166625d433ceee7bfd9f561239346585ba6f554906d4bef3e9d62f54"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/slashing/SlashableEvidence.scala",
      Set(Ingress),
      slashingIds,
      "bbcb8d22c288652ae6133faa9a082eb0de7f1439e8c1928ee2ffb74bc20276ed"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/currency/http/Codecs.scala",
      Set(Codec, DecoderIngress),
      dataApplicationIds,
      "500acd40cd8327db644aa8f4b87d53ad24a379d30098c1d2c9078824e7ca42e5"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/AllowSpendCodec.scala",
      Set(Codec),
      Set("ECO-ALLOW-CREATE"),
      "18f790181994c65852df9fd840e03551ac03ed17d1b2337da4f112c641fd3f53"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/BlockCodec.scala",
      Set(Codec),
      transferIds,
      "d8a9ebaddeab7d40f96456f14e23280fae4cd6996283dc36e6d93bf1204ee567"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/ConsumedAllowSpendCodec.scala",
      Set(Codec),
      Set("ECO-CROSS-MG-NULLIFIER"),
      "fefb29389f4465beddd78a61809c0f899a64618015184de7a804b69bcf974f03"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/CurrencyAtomCodecs.scala",
      Set(Codec),
      messageIds ++ Set("ECO-DATA-FEE", "ECO-ALLOW-CONSUME", "ECO-MG-SOURCE-SPEND") ++ tokenLockIds ++ tokenUnlockIds,
      "8dbffd3fa5a355c25d57692cfa54e4503e6ca5de13a1ce7f37aaf38607f72d9b"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/CurrencyRecordCodecs.scala",
      Set(Codec),
      Set("ECO-GLOBAL-SYNC", "ECO-ALLOW-CREATE", "ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "c03102a80114b133e1e0715205c34d27e4e8b637806e04f6a3430f632b0044af"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/CurrencySnapshotCodecs.scala",
      Set(Codec),
      currencyFrameworkIds,
      "3a47a667e62697157ec7b0ba7fa93f888b484f3717c5487c8ee5bed4f6eb070b"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/CurrencySnapshotInfoCodecs.scala",
      Set(Codec),
      currencyFrameworkIds,
      "1332f030e3f618208cc45013e71da5ca81d4e3d29aee1ed11d2a5e70202d1fbd"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/DelegatedStakeCodecs.scala",
      Set(Codec),
      delegatedStakeIds ++ delegatedStakeReleaseIds ++ Set("ECO-INVALID-STATE-SLASH"),
      "0dfeaf4796e59aa6f7dca173033e440d9d93b7f586b1972ee6305002fdab2037"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotCodecs.scala",
      Set(Codec),
      currentRootedEconomicIds -- Set("ECO-GENESIS-ISSUANCE"),
      "83c3be5a8dfc480cfb17688bb4b8eea75868c99b99d87ffe9b59ef21a02948eb"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotInfoCodec.scala",
      Set(Codec),
      currentRootedEconomicIds,
      "4ff4cb7d0beaf698dcaeaa118016797bbd5daf85066d48f00fc2c3402c22f0a0"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalStateMptCodecs.scala",
      Set(Codec),
      currentRootedEconomicIds,
      "2402d90cf1c06c65a4b737722e6d9467b8c3422216ad4228c787abd723663236"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/NewtypeLongShapes.scala",
      Set(Codec),
      currentlyExecutableIds,
      "050be8475c08c5e5897440afdac81da550105fe4d733ff398dd8cb44e8644ca7"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/NodeCollateralCodecs.scala",
      Set(Codec),
      nodeCollateralIds ++ Set("ECO-INVALID-STATE-SLASH"),
      "b2614263ad3cd35b3cb4d04b489691ebceb090be988f38eb7d0a69a1a59a8645"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/PriceOracleCodecs.scala",
      Set(Codec),
      Set("ECO-PRICE", "ECO-REWARD-GLOBAL"),
      "4e2c26cd34e238aa0369802a826337269b4815ea797d5028a531d08f66ffc0af"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/RewardTransactionCodec.scala",
      Set(Codec),
      rewardIds,
      "0cbd54238b03d34d60ae0e859ef78a07f283be279e829463d9569706b22576a6"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/ShardingScodecCodecs.scala",
      Set(Codec),
      currencyFrameworkIds ++ stateChannelIds ++ Set("ECO-INVALID-STATE-SLASH"),
      "b5c3279c2bb4c3d3f53e7ac0e2ddf5fae36bb9b694fb12dec1ef39cd384184fa"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotStateRefCodec.scala",
      Set(Codec),
      allStateChannelBinaryIds ++ slashingIds,
      "770b2acf0cfce9ae744f26829883b58171fbf0160b0cee578c68e679d3291a6c"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/GlobalSnapshotStateRef.scala",
      Set(Ingress, Codec),
      currentRootedEconomicIds ++ allStateChannelBinaryIds,
      "09118a3b1d1e154ae6c25c36fdcaae7b170ca641c0c67ebb05ebe1016f20ff5f"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala",
      Set(Ingress, Codec),
      allStateChannelBinaryIds ++ slashingIds,
      "6e5505ebbf6319e3edc5e7798cd189f717c740887f01237ce8c046beae2c713c"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/SharedArtifactCodec.scala",
      Set(Codec),
      allowSpendIds ++ tokenLockIds ++ tokenUnlockIds ++ Set("ECO-PRICE", "ECO-GLOBAL-PROCESSED-ACK"),
      "a25b6f3d1f2d4f26dea35b2585ce8e835f7e0abccc400d662b893e87072d7c91"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/StateChangesAccumulatorCodec.scala",
      Set(Codec),
      currentRootedEconomicIds,
      "52c9f7f13a123cbc4b17a1b4aaff5d0ca0e5b803469340e31fbb9c7a6d0bdbe1"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/StateChannelSnapshotBinaryCodec.scala",
      Set(Codec),
      allStateChannelBinaryIds,
      "7f44bd643fe96afdfaf2cbc98b95919a9af8dfcc95f7078e3e2f2741e6e2a4db"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/TokenLockCodec.scala",
      Set(Codec),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "89469a4db532bca1bd27bc43d7199abb6e6637b711435fa67dccef5812836b69"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/TransactionCodec.scala",
      Set(Codec),
      transferIds,
      "5078a58987974220cf2efa6d01fcb97a284eebb34ed270c389f395f74ce61699"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/UpdateNodeParametersCodec.scala",
      Set(Codec),
      Set("ECO-NODE-PARAMETERS"),
      "d740d3eed85313acd44b4a53414999ca143e61d1b08c28efa10c12988e9691d4"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendStorage.scala",
      Set(Ingress),
      Set("ECO-ALLOW-CREATE"),
      "8a60334855bff5180b16292180918eaba6055880f8b9042b1c411c77bb0537a6"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/ContextualAllowSpendValidator.scala",
      Set(Validation),
      Set("ECO-ALLOW-CREATE"),
      "6b05abcb7bc62a3f96bfe19baffbad4167e4679deb8002b5e8f02d4f75d46186"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/consensus/Engine.scala",
      Set(Ingress, Validation),
      Set("ECO-ALLOW-CREATE"),
      "5c0531e2c731e7240023e2f2a1a4c9ae3fa8e2c53dd4ab3ffe5a6579ebe883d9"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/ContextualTokenLockValidator.scala",
      Set(Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "c8f086581deb6440043d5fa06aac95b07623c178c40dbc7feb0ec1c273419e97"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/TokenLockStorage.scala",
      Set(Ingress),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "d514757dbcf5afa58dc0d627d781f08af38d9093fd1adf6ac69085f295d8c29f"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/consensus/Engine.scala",
      Set(Ingress, Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "298ded0f23e1031d53df71318113fcf5e3e16033830b2584285f7f555d9e5984"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/MessageValidationOpsManager.scala",
      Set(Validation, ParameterWriter, ReferenceWriter),
      messageIds,
      "e55cd0cc523e1eee8174650000a692de13ea23e21fd0dc038afe2218c19d7238"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/http/routes/AllowSpendBlockRoutes.scala",
      Set(DecoderIngress),
      Set("ECO-ALLOW-CREATE"),
      "b1698117798cd67f721d6126871acc5e9d7bea1217ac2c85c53d29a622e25a65"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/http/routes/CurrencyBlockRoutes.scala",
      Set(DecoderIngress),
      currencyTransferIds,
      "ec9ac225baf5a05605fa37fdae6b31becacc5ea9b2506aefe62b703ebe3eefde"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/http/routes/CurrencyMessageRoutes.scala",
      Set(DecoderIngress),
      messageIds,
      "4d38148e5742dfb86c856096e3206e844fe75a23165025f4ca3d1b6214f2c93a"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/http/routes/DataBlockRoutes.scala",
      Set(DecoderIngress),
      dataApplicationIds,
      "3b9494666f9bad7c5d289391bc18ea7443e9ca3bbf6357afee57f3d8ffdc77df"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/http/routes/TokenLockBlockRoutes.scala",
      Set(DecoderIngress),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "576ebf174298bbd9952c8fdafff9264b8e6385f848ac59b436acff351a44befe"
    ),
    ReviewedSource(
      "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/http/DataApplicationRoutes.scala",
      Set(DecoderIngress),
      dataApplicationIds,
      "257e2bc79abf34c9977da1d1f4c137175085d349d74f47ae8191e8111100fdb8"
    ),
    ReviewedSource(
      "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/http/TransactionRoutes.scala",
      Set(DecoderIngress),
      currencyTransferIds,
      "9319562eeb09be745534758882399e8534311a2f6b30942f5b8214ed74582be7"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/DelegatedStakesRoutes.scala",
      Set(DecoderIngress),
      delegatedStakeIds,
      "73bf9d2281937eff68073a45eaab30fc4ce205aacc8632890e82e1fb51342cf8"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/NodeCollateralRoutes.scala",
      Set(DecoderIngress),
      nodeCollateralIds,
      "99db83e0d60a01225019b1c8bb0851e28638051e3b4e1f04ad546f13bd621d37"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/NodeParametersRoutes.scala",
      Set(DecoderIngress),
      Set("ECO-NODE-PARAMETERS"),
      "e3a5991cbf53e8fbfe0f602fa3f333712830f5da78168f355db5cd90c2c4d03f"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/http/routes/StateChannelRoutes.scala",
      Set(DecoderIngress),
      currencyFrameworkIds ++ stateChannelIds,
      "4498edd05984bcc577e7d3ce428acd009289ef14c44bf96bc7cab06d621b85c2"
    ),
    ReviewedSource(
      "modules/dag-l1/src/main/scala/io/constellationnetwork/dag/l1/http/Routes.scala",
      Set(DecoderIngress),
      nativeTransferIds,
      "0942687f632153d77c30686adf48e27e8e0cd70966560bf15acf5f98d187fd82"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/p2p/clients/L0GlobalSnapshotClient.scala",
      Set(DecoderIngress),
      currentRootedEconomicIds,
      "38ce4765af9251fee92538c69c9f21d0e0413e5a1bd9b7aa4d1e21d05c6bfd26"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/p2p/clients/StateChannelSnapshotClient.scala",
      Set(DecoderIngress),
      currencyFrameworkIds ++ stateChannelIds,
      "1a2fe0099133fe4e32c8f5f66e910fdb0e9632bca036371e4beaebd806de21b7"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/routes/AllowSpendRoutes.scala",
      Set(DecoderIngress),
      Set("ECO-ALLOW-CREATE"),
      "771686f0f1082a7ac29efe5798164831c8466e5784c67489fe8e70d83affa863"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/http/routes/TokenLockRoutes.scala",
      Set(DecoderIngress),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "3aa61eb2393712fd2d339e80327c3c2abc04da9fd62bf5a47ebbe929430be6d2"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSnapshotValidator.scala",
      Set(Validation),
      currentRootedEconomicIds,
      "b360a36d35aac46eb495390fac39ffa922000f75cc2e79055e9fe8514671ba56"
    ),
    ReviewedSource(
      "modules/dag-l1/src/main/scala/io/constellationnetwork/dag/l1/domain/transaction/ContextualTransactionValidator.scala",
      Set(Validation),
      nativeTransferIds,
      "9929ddabbd232ad5e7ccbe1dc4b47e2bd810e5510944059614d1503a6461eaa0"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/block/processing/BlockValidator.scala",
      Set(Validation),
      transferIds,
      "9c63f729751c54f0081d680e0285e58c690a0291892420e01b25cec81f87b392"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/delegatedStake/UpdateDelegatedStakeAcceptanceManager.scala",
      Set(Validation),
      delegatedStakeIds,
      "cd4e8e76cc8c2751e1b12d1042cf5e6074a448889836191ceb2f9e957abe84c7"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofValidator.scala",
      Set(Validation),
      Set("ECO-INVALID-STATE-SLASH"),
      "82592648f211ddf910ab088793daa7390fe73b03e4b5673498f1395ffd15ac14"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/PinnedCurrencyInfoReader.scala",
      Set(Validation),
      allStateChannelBinaryIds ++ slashingIds,
      "2d8d322027c3f70eedadaecb3316467a1ed836a4e1475d287e32d99adb41e965"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/node/UpdateNodeParametersAcceptanceManager.scala",
      Set(Validation, ParameterWriter, ReferenceWriter),
      Set("ECO-NODE-PARAMETERS"),
      "46baca602a650ef9740bdf86c99f1f82b22aa1e219be72bb90b2eb7aed35c39f"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/node/UpdateNodeParametersValidator.scala",
      Set(Validation),
      Set("ECO-NODE-PARAMETERS"),
      "6a8cd0adcd5ba758cef08d2c4c802caf5315da69e2643ead10d4a6a59c5fbdab"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nodeCollateral/UpdateNodeCollateralAcceptanceManager.scala",
      Set(Validation),
      nodeCollateralIds,
      "e2111b669584d3108d59a9c4dfb2df3a18660fd620398edf453fe1d8ff972306"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala",
      Set(Validation),
      currencyFrameworkIds ++ stateChannelIds,
      "be58869bc3f7f99438fc2a85d00d35ff76871066056c5ca9186c6aa2d07bf1d1"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendChainValidator.scala",
      Set(Validation),
      Set("ECO-ALLOW-CREATE"),
      "a68c2e2b40c5905d185727cd4ec3b78f3e4b504245b192bfe72c9a6678ce146b"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/AllowSpendValidator.scala",
      Set(Validation),
      Set("ECO-ALLOW-CREATE"),
      "a0583c14e528b97f0b58b7868a2347d5635e0a9514c35a6acb2bc51df5288c74"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/block/AllowSpendBlockAcceptanceManager.scala",
      Set(Validation),
      Set("ECO-ALLOW-CREATE"),
      "07fa0955415960600edeebee1902dc24ce236f6c95c7f591cf30fec144892134"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/block/AllowSpendBlockValidator.scala",
      Set(Validation),
      Set("ECO-ALLOW-CREATE"),
      "8be7d278f7d1e6a61fc9d6cd7e8b8f415c88d51a9d81b78f48ca4ae645b8a852"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/TokenLockChainValidator.scala",
      Set(Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "2e080ab803dc343d8d4d464281669fb9b1690588a19724b1f793ced41db90891"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/TokenLockValidator.scala",
      Set(Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "d435ae8907b51f99caea160845a17fa42ffc15627ff6f3c1e31b3cc99a33fcbe"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/block/TokenLockBlockAcceptanceManager.scala",
      Set(Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "29e26ddd5977b58c8ac1f1b8f5de8aa1c136efc6d51b746ba6beac55a6878d5d"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/block/TokenLockBlockValidator.scala",
      Set(Validation),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "ea281408a09ecbe2b855b20d490394adfb3b72929fe0f13a690a26b856799fc0"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/transaction/FeeTransactionValidator.scala",
      Set(Validation),
      Set("ECO-DATA-FEE"),
      "667686b69f1cf034e98ae70bb4bf1190ab75b267b51fd6135999fe536f3b32a9"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/transaction/TransactionChainValidator.scala",
      Set(Validation),
      transferIds,
      "c3b9156db5f9146d6899be37315044f190f3adccd396781762706830606417e1"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/transaction/TransactionValidator.scala",
      Set(Validation),
      transferIds,
      "c3f44f1f6dbe770bdc1d7ec22453ef4a1c630dad2b9b0fecaeb67c506e5f20c1"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/block/processing/BlockAcceptanceManager.scala",
      Set(Validation),
      transferIds,
      "66dc514529bef5a75d18a8974ac3a8ad0b6196ff21308f991ad243471ea24220"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/block/processing/BlockValidator.scala",
      Set(Validation),
      transferIds,
      "975a210cd5961c79dc8d0e1eda0ab436c595c476d02c275a54800608617fa58d"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencyMessageValidator.scala",
      Set(Validation),
      messageIds,
      "995b55b15dc43e9bc362a45fd7df4238ed468d5c8aa8af549380c489437f9491"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/GlobalSnapshotSyncValidator.scala",
      Set(Validation),
      Set("ECO-GLOBAL-SYNC"),
      "86a5284ce212463eb429a38ab26bb53fb008ca8ab15059a16c12f9ac17ace1da"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/DataApplicationSnapshotAcceptanceManager.scala",
      Set(Ingress, Validation),
      dataApplicationIds ++ frameworkArtifactIds,
      "9a7068755a2c049df54aace0b22de7c89c788a8a7e802cdd6fe371785ef983c2"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala",
      Set(Validation),
      allStateChannelBinaryIds,
      "836c97b5ac5193430cfa826811e996765f44bdc13201023c8bdb6399ef347984"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala",
      Set(Validation),
      allStateChannelBinaryIds,
      "fb6b76032d169aa23ca37c6dfad7ff015ee0cb786d25ff1bddb488cf4924dd8c"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/validator/GlobalSnapshotActiveEraValidator.scala",
      Set(Validation),
      currentRootedEconomicIds,
      "88cd3c85a099713b3350318149b9d52ec05a0788b262103c08f0c25010ec91e0"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/programs/Genesis.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE"),
      "59a92666433f7202bcd12f54df47bc2ed398e4d2e4acdaa50496a578086cd51e"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala",
      Set(ConfigurationAuthority, GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "b2951b1ef1030c08ce2f8a690378c75bfb2cb271d0c1fdf19d6cc1a5799d41be"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/config/types.scala",
      Set(ConfigurationAuthority),
      globalRewardIds,
      "25793021fdf92107964046c0d61ccd799ccd16ee0f56ccbee54f6ed38f7c0e59"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/rewards/RewardsDistributor.scala",
      Set(RewardConstruction),
      globalRewardIds,
      "e8433a5300fde53ae3c9c70d43a3b553d1be81da4efb54f993e96df57857b5bf"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/rewards/FacilitatorDistributor.scala",
      Set(RewardConstruction),
      globalRewardIds,
      "b33f7c53a403da99d6c89ed1ab14a6db37d79dcbc0efc3603b399084b99882f0"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/rewards/GlobalDelegatedRewardsDistributor.scala",
      Set(RewardConstruction),
      globalRewardIds ++ delegatedStakeIds ++ Set("ECO-NODE-PARAMETERS"),
      "b0977e465793c625e8d8379a2dadda6cb620ee2b203aee92e4bd42b2f9cf9199"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/rewards/ProgramsDistributor.scala",
      Set(RewardConstruction),
      globalRewardIds,
      "879a59c36e416d0db4677672bf251af157ef8c28f48da613b4f78e9b049c88dc"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/rewards/Rewards.scala",
      Set(RewardConstruction),
      globalRewardIds,
      "da1a10334388cd58cf43fc85739612ff9139f4fd7aa845a25172e28f80aaf482"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala",
      Set(Ingress, Validation, MigrationGate, RewardConstruction),
      currentRootedEconomicIds ++ allStateChannelBinaryIds,
      "6a8ca0d35529bf0768861e1e77674e963685d194f9f06fdb9241a27c2ecaba08"
    ),
    ReviewedSource(
      applicationConfig,
      Set(ConfigurationAuthority, MigrationGate),
      currentlyExecutableIds,
      "cf83b479aee7c4cdc24fbca2198e078b83f2fafa9870480462389363fd54ae11"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/DelegatedRewardsConfigProvider.scala",
      Set(ConfigurationAuthority),
      globalRewardIds ++ delegatedStakeIds ++ Set("ECO-NODE-PARAMETERS"),
      "81016c6ed96fbc84c0d8ddf6eda207877a5844201837e2e71bab80939cf26209"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/MainnetRewardsConfig.scala",
      Set(ConfigurationAuthority),
      globalRewardIds,
      "411dd257e3ca64d91b51b460caf00ba28358d77b5dd004a363a6ce94db02660d"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala",
      Set(ConfigurationAuthority, MigrationGate),
      currentlyExecutableIds,
      "be8ac9b455c5958e9c357e56c2df6c0ca0652c83d9787c426f2daf50ec52496e"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/GenesisFS.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "8c8f2bd27e11ae7c1532903584b01fc698e77bd173822a881a9c06d0863c42fc"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "2b4b5ab3d3a8276f8a821aef98f8c47f3d5d10b5e845ea64fbdf04e588251388"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/rewards/Rewards.scala",
      Set(RewardConstruction),
      globalRewardIds,
      "a471ab16b14c6bf56a3536caaa661846b6c1cb58ad3249f88e5e37cdcdfceb0d"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/GenesisFS.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "853635476e0760e2571fe590ee5a56374f4a7aafc72fefd5dfaadd941e564e2e"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisLoader.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "cf81236ad7057dc2f6f52fda951b0884d834d790480e1250d08f8006e5c619bc"
    ),
    ReviewedSource(
      "modules/tools/src/main/scala/io/constellationnetwork/tools/genesis/GenesisGenerator.scala",
      Set(GenesisIssuance),
      Set("ECO-GENESIS-ISSUANCE") ++ genesisBackingIds,
      "4101449e3926f27909eb1e55728dd08bf9124277bf8b20d11da75b2f375672b0"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/balance.scala",
      Set(Ingress, BalanceWriter),
      currentBalanceEffectIds,
      "290937401a30e2ee6e051ba46d51142a4f6d61d8edb63fbf7180197515d23d8b"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/StakeDistribution.scala",
      Set(Ingress, StakeWriter),
      stakeLifecycleIds,
      "7be5982837809f7994d100f496defc267567f13e72e8d3099025e9002e4154f0"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/priceOracle.scala",
      Set(Ingress),
      Set("ECO-PRICE"),
      "5534d33ed56679dbefa6ed05a264ed665b2b947ceeaa2d8d3ff7fd5569769fc7"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/cli/method.scala",
      Set(ConfigurationAuthority, GenesisIssuance, MigrationGate),
      currentlyExecutableIds,
      "2c9b6787e8699a82fe54e2f4add3ff61ec5ee81c9fed798817f5317d468d43bc"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusStateAdvancer.scala",
      Set(Ingress, Validation),
      currencyFrameworkIds ++ stateChannelIds,
      "93f993bbd77787776a1651025d4a8e729b1eea2ccfbfdee2c23cfaebaad0451c"
    ),
    ReviewedSource(
      "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/cli/method.scala",
      Set(ConfigurationAuthority),
      currentlyExecutableIds,
      "9c2f3e6455acd20ce98e6d35d9b503d3ac248dc2fed1b582e32964fac712c311"
    ),
    ReviewedSource(
      "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/snapshot/programs/CurrencySnapshotProcessor.scala",
      Set(Ingress, Validation),
      currentRootedEconomicIds,
      "a1cd8434fc5743dc64f2da8d0d311079d7d0e5a2cd764ff6c037254d9ee5d3ea"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala",
      Set(Ingress, Validation, ConfigurationAuthority),
      currentRootedEconomicIds ++ allStateChannelBinaryIds,
      "68d1a1071863a5155fda42966b9bc9cae7c726c811d08b7a776c7d0a31ae8b45"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoSyncDaemon.scala",
      Set(Ingress, Validation),
      currentRootedEconomicIds,
      "0451d732fdb08aaff580a54e85933bcd2391e10ae6296f04989a3191f54c7381"
    ),
    ReviewedSource(
      "modules/dag-l1/src/main/scala/io/constellationnetwork/dag/l1/domain/consensus/block/BlockConsensusCell.scala",
      Set(Ingress, Validation),
      nativeTransferIds,
      "2a467f20d257e02cd59d2443219746d7f9078748cecb1ff3d0754f6d9f688bcc"
    ),
    ReviewedSource(
      "modules/dag-l1/src/main/scala/io/constellationnetwork/dag/l1/domain/transaction/TransactionStorage.scala",
      Set(Ingress, Validation, ReferenceWriter),
      nativeTransferIds,
      "ac9416597d6a68a5f665cb939062cc947079762ea4b08bf495936e79c403fbc9"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/economics/BalanceReservationLedger.scala",
      Set(Validation, BalanceWriter, ReservationWriter, NullifierWriter),
      currentBalanceEffectIds,
      "1fa4f45cd1c286585d89e1220dff36bc2dbc07030e9c52217f0613e6b5c18f3a"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/EtaStateManager.scala",
      Set(Ingress, StakeWriter),
      stakeLifecycleIds,
      "62bdd04e987b7c4bf7b89efcbbadc5eb39d8f1680019e928bcfdcaedd24482dd"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/custody/MetagraphBinaryCustodyStore.scala",
      Set(Ingress),
      currencyFrameworkIds ++ stateChannelIds,
      "7ddcd05c1ae8a2d556baae115808a37b7420d16a20e14c5dee071a0c7f41fb30"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/overlay/MptOverlay.scala",
      Set(
        Ingress,
        BalanceWriter,
        ReservationWriter,
        ReferenceWriter,
        NullifierWriter,
        StakeWriter,
        CollateralWriter,
        RewardWriter,
        PriceWriter,
        ParameterWriter,
        SlashRegistryWriter
      ),
      currentRootedEconomicIds,
      "104306c6d40d51e97e57b14e5618563ac2d2a7091d80a97eca94ca20109c288c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/ShardCheckpointEquivocationValidator.scala",
      Set(Validation),
      slashingIds,
      "325203736aab445f6c5b73c2db1eaa2caec9495880a0bd175a1d89c81a3a3922"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashableEvidenceValidator.scala",
      Set(Validation),
      slashingIds,
      "e8c537def2ecad66d2a374de25aaca734483c60d8d63e266c535e300563463ec"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/snapshot/finality/FinalityDurableStore.scala",
      Set(Ingress),
      currentRootedEconomicIds,
      "be03e67cacd25c5a47e71e1acb1489095a55fb9a6ddb20a7d3db4075893a3db0"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/block/AllowSpendBlockStorage.scala",
      Set(Ingress),
      Set("ECO-ALLOW-CREATE"),
      "084c4ee632f8744b47e385385cf67b05d38d8467f4055e2ea3c86e16eae187fd"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/tokenlock/block/TokenLockBlockStorage.scala",
      Set(Ingress),
      Set("ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "4eefd4ba26c87ff8fc1ac435b73939465a3108b26d9c147b70c49569cf07cb49"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ShardWindowContinuation.scala",
      Set(Validation),
      allStateChannelBinaryIds,
      "cedb40b9b97eb101f274faf9917e8af10d186cd9dab3aa51da97936b501e3084"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/sharding/ShardCheckpointProducerDutyValidator.scala",
      Set(Validation),
      allStateChannelBinaryIds,
      "449302e62dbe5c48f770305bf3951fca1f20437de42f868e4f3ee3a25f42eb5c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointWiring.scala",
      Set(Ingress, Validation),
      allStateChannelBinaryIds ++ slashingIds,
      "8a8ca5c3a712940ae4cb007e35df26dae3e35eb2b037def9dacbccb51c128d92"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducer.scala",
      Set(Ingress, Validation),
      allStateChannelBinaryIds,
      "b34da0ac59e093a19536057ce1fe3a73d511cd84d7587471222570166c0315c5"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointWireCodecs.scala",
      Set(DecoderIngress, Codec, Validation),
      allStateChannelBinaryIds ++ slashingIds,
      "175fe5f6dd34851cdd02b14bb6c87afeab310b6cd23f3f7ffff139b283a5db37"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/WatchtowerFraudProofEmitter.scala",
      Set(Ingress, Validation),
      allStateChannelBinaryIds ++ Set("ECO-INVALID-STATE-SLASH"),
      "70455f823f4bb17a8c806e433dbfbed05cfe7e24cf7eff99e1bde71ade39f44c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotCreator.scala",
      Set(Ingress, Validation),
      currencyFrameworkIds ++ stateChannelIds,
      "e418e27ba155ad5432176cb409990205a2a54d68bde2444dc5269e8af7cbe84c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/DelegatedRewardsDistributor.scala",
      Set(RewardConstruction, RewardWriter, StakeWriter),
      globalRewardIds ++ delegatedStakeIds ++ Set("ECO-NODE-PARAMETERS"),
      "3b80fa5bd78ada4fa3057c611ad78b9d0a71ae4d494ff276b1428f3dd28653aa"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/GlobalSnapshotContextFunctions.scala",
      Set(
        Validation,
        BalanceWriter,
        ReservationWriter,
        ReferenceWriter,
        NullifierWriter,
        StakeWriter,
        CollateralWriter,
        RewardWriter,
        PriceWriter,
        ParameterWriter,
        SlashRegistryWriter
      ),
      currentRootedEconomicIds,
      "717dbfe90f5aa4ec86a4515995a48ac79424d7833916711bface1ec085f29cc3"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/BlockAcceptanceOpsManager.scala",
      Set(Validation, ReferenceWriter),
      currencyTransferIds ++ Set("ECO-ALLOW-CREATE", "ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "d03965d0672414d729ba6b041f9b545641cd7b46fa0287189d5ca3f723149e39"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/AllowSpendConsumeHandler.scala",
      Set(Validation, NullifierWriter),
      Set("ECO-ALLOW-CONSUME", "ECO-MG-SOURCE-SPEND", "ECO-CROSS-MG-NULLIFIER"),
      "b93f0d91d0ec37ab3ea18849b0dc49f54d962661ae9481e2f3628911113b769c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/BlockAcceptanceCoordinatorManager.scala",
      Set(Validation, ReferenceWriter),
      nativeTransferIds ++ Set("ECO-ALLOW-CREATE", "ECO-TOKEN-LOCK-CREATE", "ECO-TOKEN-LOCK-REPLACE"),
      "92c1e84f1d3fe5955cd7cd791999b92d3479ccdf83e9f9019e8fea0071d41a24"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/CrossShardMessageHandler.scala",
      Set(Validation, NullifierWriter),
      Set("ECO-CROSS-MG-NULLIFIER"),
      "94b340494421533d07c773e0412d09e0baf77ff42c6a6e4bf20e62d04cb10fd9"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/storage/SnapshotStorage.scala",
      Set(Ingress),
      currentRootedEconomicIds,
      "c879c06b4c708ffee6403fe3bb25125afa8c8ff58208df5f90c6e8d99551bde5"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedServices.scala",
      Set(Validation, ConfigurationAuthority),
      currentRootedEconomicIds ++ allStateChannelBinaryIds,
      "22666d0fb85dcc9a56aa794ca2d631268624c6be4fe71290337ca1d70d2eb0e6"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/follow/SyncedField.scala",
      Set(Ingress, BalanceWriter, ReservationWriter, ReferenceWriter),
      transferIds ++ allowSpendIds ++ tokenLockIds,
      "3c45ee5b6024bec87a8dcaaa61caa9b79e7c0fc5de3828547e76079c8dbc6499"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/security/mpt/DurableMptImageStore.scala",
      Set(Ingress),
      currentRootedEconomicIds,
      "d995bd8599b25a3189d70c991cf6b8cdf9f58566920c9b54be468381e10b5803"
    ),
    ReviewedSource(
      "modules/tools/src/main/scala/io/constellationnetwork/tools/Main.scala",
      Set(Ingress, GenesisIssuance),
      Set(
        "ECO-GENESIS-ISSUANCE",
        "ECO-TRANSFER-NATIVE",
        "ECO-OPAQUE-CARRIAGE",
        "ECO-OPAQUE-CARRIAGE-FEE-ERA"
      ) ++ snapshotFeeIds ++ genesisBackingIds,
      "11927b7f5b2bc787955c44bfe2851444ed5e41209c8d772a3569c5f9a58b2e12"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala",
      Set(Validation),
      currencyFrameworkIds,
      "3e66f6314e64e8443aa77aa43a19128de896717b1eb40a5af5eab7071456f214"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusFunctions.scala",
      Set(Validation),
      currencyFrameworkIds,
      "be32da7056fc80d741484f863ba304353168e6a7d55d70c203e4722811a6fcda"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/SnapshotConsensusFunctions.scala",
      Set(Validation),
      currentlyExecutableIds,
      "b08ef85c6a9fcd229cd0096fa550de1df39913c9b45b5fd826bc30434cdccaf7"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/FeeCalculator.scala",
      Set(ConfigurationAuthority, FeeAuthority),
      snapshotFeeIds,
      "732f0ea7de0b47b2666214bb6876454337b1f287ebaad057fd21935a10f765a7"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/snapshot/programs/SnapshotBinaryFeeCalculator.scala",
      Set(FeeAuthority),
      snapshotFeeIds,
      "00a7bef177982889dff99967cdc43c9403cbd198df25c58d5e5a3b033decdecb"
    ),
    ReviewedSource(
      "modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/StateChannelSnapshotService.scala",
      Set(Ingress, FeeAuthority),
      snapshotFeeIds,
      "325eb4c64de08facacd0f82287f5490b53bb87da3fff433e5b427b3eb9052732"
    ),
    ReviewedSource(
      "modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/dataApplication/consensus/Engine.scala",
      Set(Ingress, Validation),
      dataApplicationIds,
      "2d83ada2a331fa9131cab9d597e6b8aa1e2805bf5761442210a41b63970f769c"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/block/processing/BlockAcceptanceManager.scala",
      Set(Validation),
      transferIds,
      "80643ac5028cae57035a10bc8cb44059ed85fa335d389eb9bd30b8af5bbd22bb"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/consensus/ConsensusFunctions.scala",
      Set(Validation),
      currentlyExecutableIds,
      "ac5a29a41c353491bee4c5b8356af6efd56c7e00ab8e9e501158fab96a8eedb5"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/delegatedStake/UpdateDelegatedStakeValidator.scala",
      Set(Validation),
      delegatedStakeIds,
      "d51769688d1e3b332bcc96e2636deefd2c78c59230d15711c24e1e6ed31b89e0"
    ),
    ReviewedSource(
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nodeCollateral/UpdateNodeCollateralValidator.scala",
      Set(Validation),
      nodeCollateralIds,
      "0f116a691594aca8c7ebfcdd6eb33533efc22236269d4547d57f616342edb92a"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/currency/validations/DataTransactionsValidator.scala",
      Set(Validation),
      dataApplicationIds,
      "d4c6dd6e4fbd06e358637e26ea152645010ad31e52b2f819493aab14648925af"
    ),
    ReviewedSource(
      "modules/shared/src/main/scala/io/constellationnetwork/currency/validations/FeeTransactionValidator.scala",
      Set(Validation),
      Set("ECO-DATA-FEE"),
      "dbcbe20e441291950e20fd69aa8ad5af0531c90756e777c8f32564fc16ddfb9b"
    ),
    ReviewedSource(
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/domain/snapshot/programs/GlobalSnapshotEventCutter.scala",
      Set(FeeAuthority),
      snapshotFeeIds ++ nativeTransferIds,
      "e3aeececc22700e33e1a5fb0fb623030cc057d523df43870716e3886a7610019"
    )
  )
}
