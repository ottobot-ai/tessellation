package io.constellationnetwork.node.shared.domain.economics

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths => NioPaths}
import java.security.MessageDigest

import cats.effect.IO

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

import io.constellationnetwork.node.shared.domain.economics.V4EconomicGrammarManifest.Activation._
import io.constellationnetwork.node.shared.domain.economics.V4EconomicGrammarManifest.Authority._
import io.constellationnetwork.node.shared.domain.economics.V4EconomicGrammarManifest.Effect._
import io.constellationnetwork.node.shared.domain.economics.V4EconomicGrammarManifest.SurfaceRole._
import io.constellationnetwork.node.shared.domain.economics.V4EconomicGrammarManifest.{Paths => GrammarPaths}

import weaver.SimpleIOSuite

/** CI fuse for E2.9's economic-grammar inventory.
  *
  * This is not the economic oracle and passing it does not approve any operation. It makes the reviewed v4/fork surface closed under:
  *   - operation constructors and sealed framework event/artifact variants;
  *   - executable authority/writer sources; and
  *   - semantic branch changes inside a reviewed source.
  *
  * Semantic fingerprints ignore comments and whitespace, so scalafmt and documentation changes do not create approval churn. A new or
  * changed economic branch must update its structured manifest row in the same review.
  */
object V4EconomicGrammarCompletenessGuardSuite extends SimpleIOSuite {

  private final case class Source(path: String, contents: String) {
    lazy val tokens: Vector[String] = semanticTokens(contents, hashComments = path.endsWith(".conf"))
    lazy val tokenSet: Set[String] = tokens.toSet
    lazy val semanticDigest: String = sha256(tokens.mkString("\u001f"))
  }

  private val caseClassDeclaration: Regex =
    """(?m)\b(?:final\s+)?case\s+class\s+([A-Za-z_][A-Za-z0-9_]*)\b""".r

  private val suspiciousEconomicConstructor: Regex =
    """(?i).*(?:Transaction|Reward|Fee|AllowSpend|SpendAction|TokenLock|TokenUnlock|DelegatedStake|Stake|NodeCollateral|Collateral|Balance|ProtocolCorrection|Correction|Debit|Credit|PricingUpdate|Price|Mint|Burn|Supply|Emission|Issuance|Nullifier|Settlement|Slash|Treasury).*""".r

  private lazy val economicTypeSymbols: Set[String] =
    V4EconomicGrammarManifest.operations.flatMap(_.constructors.map(_.symbol)).toSet ++
      V4EconomicGrammarManifest.wireCarriers.map(_.anchor.symbol)

  private val expectedSharedArtifacts: Set[String] = Set(
    "SpendAction",
    "TokenUnlock",
    "AllowSpendExpiration",
    "PricingUpdate",
    "GlobalSnapshotsProcessed"
  )

  private val expectedGlobalEvents: Set[String] = Set(
    "DAGEvent",
    "StateChannelEvent",
    "AllowSpendEvent",
    "TokenLockEvent",
    "UpdateNodeParametersEvent",
    "CreateDelegatedStakeEvent",
    "WithdrawDelegatedStakeEvent",
    "CreateNodeCollateralEvent",
    "WithdrawNodeCollateralEvent",
    "KesRegistrationCertEvent"
  )

  private val expectedSlashReasons: Set[String] = Set(
    "InvalidStateProof",
    "CheckpointEquivocation",
    "MetagraphEquivocation",
    "NonParticipation"
  )

  pureTest("manifest separates v4 feature identity from current activation and has no dangling classification") {
    val operations = V4EconomicGrammarManifest.operations
    val ids = operations.map(_.id)
    val idSet = ids.toSet
    val sourceRows = V4EconomicGrammarManifest.reviewedSources
    val duplicateIds = ids.groupBy(identity).collect { case (id, occurrences) if occurrences.sizeCompare(1) > 0 => id }.toList.sorted
    val duplicateSources = sourceRows
      .groupBy(_.path)
      .collect { case (path, occurrences) if occurrences.sizeCompare(1) > 0 => path }
      .toList
      .sorted
    val danglingSourceIds = sourceRows.flatMap(_.operationIds).filterNot(idSet).distinct.sorted
    val invalidMissingAuthority = operations.collect {
      case operation if operation.intendedAuthority == Missing && operation.activation != RemovedFailClosed => operation.id
    }
    val unexplainedMissingCurrentAuthority = operations.collect {
      case operation
          if operation.currentAuthority == Missing &&
            operation.activation != ActiveSourceProvenUnsafe &&
            operation.activation != MissingFailClosed &&
            operation.activation != RemovedFailClosed =>
        operation.id
    }
    val unsafeWithoutRed = operations.collect {
      case operation if operation.activation == ActiveSourceProvenUnsafe && operation.redObligations.isEmpty => operation.id
    }
    val featureIdentityCollapsed = operations.collect {
      case operation
          if operation.v4Identity == V4EconomicGrammarManifest.V4Identity.RemovedWrongAuthority &&
            operation.activation != RemovedFailClosed =>
        operation.id
    }
    val frozenCounts = (operations.size, V4EconomicGrammarManifest.wireCarriers.size, sourceRows.size)

    val valid =
      duplicateIds.isEmpty && duplicateSources.isEmpty && danglingSourceIds.isEmpty && invalidMissingAuthority.isEmpty &&
        unexplainedMissingCurrentAuthority.isEmpty && unsafeWithoutRed.isEmpty && featureIdentityCollapsed.isEmpty &&
        frozenCounts == ((40, 12, 185))
    if (valid) success
    else
      failure(
        s"duplicateIds=$duplicateIds duplicateSources=$duplicateSources danglingSourceIds=$danglingSourceIds " +
          s"invalidMissingAuthority=$invalidMissingAuthority unexplainedMissingCurrentAuthority=$unexplainedMissingCurrentAuthority " +
          s"unsafeWithoutRed=$unsafeWithoutRed " +
          s"featureIdentityCollapsed=$featureIdentityCollapsed frozenCounts=$frozenCounts"
      )
  }

  pureTest("every state-channel checkpoint lifecycle source covers the complete binary grammar") {
    val binaryIds = V4EconomicGrammarManifest.wireCarriers
      .find(_.anchor.symbol == "StateChannelSnapshotBinary")
      .map(_.operationIds)
      .getOrElse(Set.empty)
    val requiredPaths = Set(
      GrammarPaths.stateChannelBinary,
      GrammarPaths.stateChannelOutput,
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/ShardCheckpointGl0AcceptanceManager.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ShardWindowContinuation.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointWiring.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointProducer.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/WatchtowerFraudProofEmitter.scala",
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusFunctions.scala",
      "modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensus.scala",
      "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedServices.scala"
    )
    val rowsByPath = V4EconomicGrammarManifest.reviewedSources.map(row => row.path -> row).toMap
    val missingRows = requiredPaths -- rowsByPath.keySet
    val underclassified = requiredPaths.toList.sorted.flatMap { path =>
      rowsByPath.get(path).flatMap { row =>
        val missingIds = binaryIds -- row.operationIds
        Option.when(missingIds.nonEmpty)(path -> missingIds.toList.sorted)
      }
    }

    if (binaryIds.nonEmpty && missingRows.isEmpty && underclassified.isEmpty) success
    else failure(s"binaryIds=$binaryIds missingRows=$missingRows underclassified=$underclassified")
  }

  test("every operation constructor and economic wire-carrier anchor resolves exactly and removed v4 adjustment authority stays absent") {
    productionSources.map { sources =>
      val operationErrors = V4EconomicGrammarManifest.operations.flatMap { operation =>
        operation.constructors.flatMap { anchor =>
          sources.get(anchor.path) match {
            case None => List(s"${operation.id}: missing ${anchor.path}")
            case Some(source) =>
              val count = caseClassDeclaration.findAllMatchIn(stripComments(source.contents)).count(_.group(1) == anchor.symbol)
              Option
                .when(count != anchor.expectedDeclarations)(
                  s"${operation.id}: ${anchor.path}#${anchor.symbol} expected=${anchor.expectedDeclarations} actual=$count"
                )
                .toList
          }
        }
      }
      val carrierErrors = V4EconomicGrammarManifest.wireCarriers.flatMap { carrier =>
        val anchor = carrier.anchor
        sources.get(anchor.path) match {
          case None => List(s"wire-carrier ${anchor.symbol}: missing ${anchor.path}")
          case Some(source) =>
            val count = typeDeclarationCount(source.contents, anchor.symbol)
            Option
              .when(count != anchor.expectedDeclarations)(
                s"wire-carrier ${anchor.path}#${anchor.symbol} expected=${anchor.expectedDeclarations} actual=$count"
              )
              .toList
        }
      }
      val errors = operationErrors ++ carrierErrors
      val forbiddenBalanceAdjustment = sources.values.toList.flatMap { source =>
        caseClassDeclaration
          .findAllMatchIn(stripComments(source.contents))
          .filter(_.group(1) == "BalanceAdjustment")
          .map(_ => source.path)
      }.sorted
      val removedRuntimeFiles = List(
        "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/BalanceAdjustmentLoader.scala",
        "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencyBalanceAdjustments.scala"
      ).filter(sources.contains)

      if (errors.isEmpty && forbiddenBalanceAdjustment.isEmpty && removedRuntimeFiles.isEmpty) success
      else
        failure(
          s"anchorErrors=${errors.mkString("; ")} balanceAdjustmentDeclarations=$forbiddenBalanceAdjustment " +
            s"removedRuntimeFiles=$removedRuntimeFiles"
        )
    }
  }

  pureTest("codec, decoder, validation, configuration, genesis, migration, reward, and writer surfaces are explicitly represented") {
    val rows = V4EconomicGrammarManifest.reviewedSources
    val representedRoles = rows.flatMap(_.roles).toSet
    val requiredRoles = Set(
      Codec,
      DecoderIngress,
      Validation,
      ConfigurationAuthority,
      GenesisIssuance,
      MigrationGate,
      RewardConstruction,
      FeeAuthority,
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
    )
    val missingRoles = requiredRoles -- representedRoles
    val migrationIssuance = operation("ECO-MIGRATION-ISSUANCE")
    val dataApplicationBlock = V4EconomicGrammarManifest.wireCarriers.find(_.anchor.symbol == "DataApplicationBlock")
    val currencyFull = V4EconomicGrammarManifest.wireCarriers.find(_.anchor.symbol == "CurrencySnapshot")
    val currencyIncremental = V4EconomicGrammarManifest.wireCarriers.find(_.anchor.symbol == "CurrencyIncrementalSnapshot")
    val stateChannelBinary = V4EconomicGrammarManifest.wireCarriers.find(_.anchor.symbol == "StateChannelSnapshotBinary")
    val dataApplicationAcceptance = rows.find(
      _.path.endsWith("/DataApplicationSnapshotAcceptanceManager.scala")
    )
    val genesisBackingIds = Set("ECO-GENESIS-DELEGATED-STAKE", "ECO-GENESIS-NODE-COLLATERAL")
    val allowSpendConsume = operation("ECO-ALLOW-CONSUME")
    val crossMetagraph = operation("ECO-CROSS-MG-NULLIFIER")

    expect.all(
      missingRoles.isEmpty,
      migrationIssuance.activation == MissingFailClosed,
      migrationIssuance.constructors.isEmpty,
      !rows.exists(_.operationIds(migrationIssuance.id)),
      dataApplicationBlock.exists(
        _.operationIds == Set("ECO-DATA-FEE", "ECO-FRAMEWORK-DATA-CARRIAGE")
      ),
      currencyFull.exists(
        _.operationIds == Set(
          "ECO-TRANSFER-CURRENCY",
          "ECO-REWARD-CURRENCY",
          "ECO-GENESIS-ISSUANCE",
          "ECO-GLOBAL-SYNC",
          "ECO-DATA-FEE",
          "ECO-FRAMEWORK-DATA-CARRIAGE"
        )
      ),
      dataApplicationAcceptance.exists(
        _.operationIds == Set(
          "ECO-DATA-FEE",
          "ECO-FRAMEWORK-DATA-CARRIAGE",
          "ECO-ALLOW-CONSUME",
          "ECO-MG-SOURCE-SPEND",
          "ECO-ALLOW-EXPIRY",
          "ECO-TOKEN-LOCK-EXPIRY",
          "ECO-TOKEN-LOCK-MANUAL",
          "ECO-DELEGATED-STAKE-RELEASE",
          "ECO-PRICE",
          "ECO-CROSS-MG-NULLIFIER"
        )
      ),
      currencyIncremental.exists(_.operationIds("ECO-PRICE")),
      currencyIncremental.exists(_.operationIds("ECO-DELEGATED-STAKE-RELEASE")),
      currencyIncremental.exists(_.operationIds("ECO-FRAMEWORK-DATA-CARRIAGE")),
      stateChannelBinary.exists(_.operationIds("ECO-PRICE")),
      stateChannelBinary.exists(_.operationIds("ECO-DELEGATED-STAKE-RELEASE")),
      stateChannelBinary.exists(_.operationIds("ECO-OPAQUE-CARRIAGE")),
      stateChannelBinary.exists(_.operationIds("ECO-OPAQUE-CARRIAGE-FEE-ERA")),
      stateChannelBinary.exists(_.operationIds("ECO-OPAQUE-CARRIAGE-SHARDED")),
      !stateChannelBinary.exists(_.operationIds("ECO-NODE-COLLATERAL-RELEASE")),
      !V4EconomicGrammarManifest.wireCarriers.exists(_.operationIds.exists(genesisBackingIds)),
      allowSpendConsume.intendedAuthority == RootedReferencedAllowSpend,
      allowSpendConsume.currentAuthority == RootedReferencedAllowSpend,
      allowSpendConsume.constructors.map(_.symbol).toSet == Set("SpendAction", "SpendTransaction", "AllowSpend"),
      crossMetagraph.intendedAuthority == RootedReferencedAllowSpend,
      crossMetagraph.currentAuthority == RootedReferencedAllowSpend,
      crossMetagraph.constructors.map(_.symbol).toSet == Set("SpendAction", "SpendTransaction", "AllowSpend"),
      !crossMetagraph.constructors.exists(_.symbol == "ConsumedAllowSpend")
    )
  }

  test("sealed framework artifacts, economic GL0 events, stake variants, collateral variants, and slash reasons are exhaustive") {
    productionSources.map { sources =>
      val artifact = sources(GrammarPaths.artifact).contents
      val globalEvent = sources(GrammarPaths.globalEvent).contents
      val delegatedStake = sources(GrammarPaths.delegatedStake).contents
      val nodeCollateral = sources(GrammarPaths.nodeCollateral).contents
      val slashManager = sources(
        "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala"
      ).contents

      val artifacts = directCaseClassSubtypes(artifact, Set("SharedArtifact"))
      val globalEvents = directCaseClassSubtypes(
        globalEvent,
        Set("GlobalSnapshotEvent", "UpdateDelegatedStakeEvent", "UpdateNodeCollateralEvent")
      )
      val delegatedVariants = directCaseClassSubtypes(delegatedStake, Set("UpdateDelegatedStake"))
      val collateralVariants = directCaseClassSubtypes(nodeCollateral, Set("UpdateNodeCollateral"))
      val slashReasons = directCaseObjectSubtypes(slashManager, Set("SlashReason"))

      val valid = artifacts == expectedSharedArtifacts && globalEvents == expectedGlobalEvents &&
        delegatedVariants == Set("Create", "Withdraw") && collateralVariants == Set("Create", "Withdraw") &&
        slashReasons == expectedSlashReasons
      if (valid) success
      else
        failure(
          s"SharedArtifact=$artifacts GlobalSnapshotEvent=$globalEvents UpdateDelegatedStake=$delegatedVariants " +
            s"UpdateNodeCollateral=$collateralVariants SlashReason=$slashReasons"
        )
    }
  }

  test("reviewed constructor and authority sources retain their semantic fingerprints") {
    productionSources.map { sources =>
      val errors = V4EconomicGrammarManifest.reviewedSources.flatMap { reviewed =>
        sources.get(reviewed.path) match {
          case None => List(s"missing:${reviewed.path}")
          case Some(source) if reviewed.semanticSha256 == "__FILL__" =>
            List(s"unfrozen:${reviewed.path}:${source.semanticDigest}")
          case Some(source) if source.semanticDigest != reviewed.semanticSha256 =>
            List(s"changed:${reviewed.path}:expected=${reviewed.semanticSha256}:actual=${source.semanticDigest}")
          case Some(_) => Nil
        }
      }

      if (errors.isEmpty) success else failure(errors.mkString("\n"))
    }
  }

  test("a new economic constructor, codec, decoder, authority, configuration, genesis, or migration source is unclassified") {
    productionSources.map { sources =>
      val reviewed = V4EconomicGrammarManifest.reviewedSources.map(_.path).toSet
      val unclassifiedConstructors = sources.values.toList.flatMap { source =>
        val declarations = suspiciousEconomicConstructors(source)
        Option.when(declarations.nonEmpty && isEconomicSchemaPath(source.path) && !reviewed(source.path))(
          s"${source.path}: ${declarations.distinct.sorted.mkString(",")}"
        )
      }.sorted
      val unclassifiedWriters = sources.values.toList
        .filter(isWriterShaped)
        .map(_.path)
        .filterNot(reviewed)
        .sorted
      val unclassifiedCodecs = sources.values.toList
        .filter(isEconomicCodec)
        .map(_.path)
        .filterNot(reviewed)
        .sorted
      val unclassifiedDecoders = sources.values.toList
        .filter(isEconomicDecoderIngress)
        .map(_.path)
        .filterNot(reviewed)
        .sorted
      val unclassifiedValidators = sources.values.toList
        .filter(isEconomicValidationBoundary)
        .map(_.path)
        .filterNot(reviewed)
        .sorted
      val unclassifiedIssuance = sources.values.toList
        .filter(isEconomicIssuanceOrConfiguration)
        .map(_.path)
        .filterNot(reviewed)
        .sorted
      val unclassifiedFeeAuthorities = sources.values.toList
        .filter(isEconomicFeeAuthority)
        .map(_.path)
        .filterNot(reviewed)
        .sorted

      if (
        unclassifiedConstructors.isEmpty && unclassifiedWriters.isEmpty && unclassifiedCodecs.isEmpty &&
        unclassifiedDecoders.isEmpty && unclassifiedValidators.isEmpty && unclassifiedIssuance.isEmpty &&
        unclassifiedFeeAuthorities.isEmpty
      ) success
      else
        failure(
          s"unclassifiedConstructors=${unclassifiedConstructors.mkString("; ")}\n" +
            s"unclassifiedWriters=${unclassifiedWriters.mkString("; ")}\n" +
            s"unclassifiedCodecs=${unclassifiedCodecs.mkString("; ")}\n" +
            s"unclassifiedDecoders=${unclassifiedDecoders.mkString("; ")}\n" +
            s"unclassifiedValidators=${unclassifiedValidators.mkString("; ")}\n" +
            s"unclassifiedIssuance=${unclassifiedIssuance.mkString("; ")}\n" +
            s"unclassifiedFeeAuthorities=${unclassifiedFeeAuthorities.mkString("; ")}"
        )
    }
  }

  pureTest("discovery catches correction writers, compound validators, consensus validation, and fee authority") {
    val correctionSchema = Source(
      "modules/shared/src/main/scala/io/constellationnetwork/schema/ProtocolCorrection.scala",
      "final case class ProtocolCorrection(account: Address, debit: Amount, credit: Amount)"
    )
    val externalWriter = Source(
      "modules/example/src/main/scala/io/constellationnetwork/example/domain/correction/ProtocolCorrectionWriter.scala",
      """object ProtocolCorrectionWriter {
        |  def apply(balances: Map[Address, Balance], correction: ProtocolCorrection) =
        |    balances.updated(correction.account, balances(correction.account).minus(correction.debit))
        |}""".stripMargin
    )
    val compoundValidator = Source(
      "modules/example/src/main/scala/io/constellationnetwork/example/CurrencySnapshotValidator.scala",
      "trait CurrencySnapshotValidator { def validateRecreateContent(snapshot: CurrencyIncrementalSnapshot): Boolean }"
    )
    val consensusValidator = Source(
      "modules/example/src/main/scala/io/constellationnetwork/example/CurrencySnapshotConsensusFunctions.scala",
      "trait CurrencySnapshotConsensusFunctions { def validateArtifact(snapshot: CurrencyIncrementalSnapshot): Boolean }"
    )
    val feeAuthority = Source(
      "modules/example/src/main/scala/io/constellationnetwork/example/SnapshotBinaryFeeCalculator.scala",
      "trait SnapshotBinaryFeeCalculator { def calculateRecommendedFee(snapshot: StateChannelSnapshotBinary): SnapshotFee }"
    )

    expect.all(
      isEconomicSchemaPath(correctionSchema.path),
      suspiciousEconomicConstructors(correctionSchema) == List("ProtocolCorrection"),
      isWriterShaped(externalWriter),
      isEconomicValidationBoundary(compoundValidator),
      isEconomicValidationBoundary(consensusValidator),
      isEconomicFeeAuthority(feeAuthority)
    )
  }

  test("RED ECON-AUTH-001: current manual TokenUnlock has no owner-signed intent or permanent replay identity") {
    productionSources.map { sources =>
      val schema = stripComments(sources(GrammarPaths.artifact).contents)
      val manager = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala"
        ).contents
      )
      val declaration = sectionAfter(schema, "case class TokenUnlock(", 500)
      val intake = sectionAfter(manager, "object TokenLockOpsManager {", 900)
      val row = operation("ECO-TOKEN-LOCK-MANUAL")

      expect.all(
        row.activation == ActiveSourceProvenUnsafe,
        row.intendedAuthority == SourceSignatureAndReference,
        row.currentAuthority == Missing,
        declaration.nonEmpty,
        !declaration.contains("Signed"),
        !declaration.contains("Signature"),
        !declaration.contains("intent"),
        intake.contains("incomingTokenUnlocks.filter"),
        intake.contains("tokenUnlock.source == tokenLock.source"),
        !intake.contains("verify"),
        !intake.contains("nullifier")
      )
    }
  }

  test("RED ECON-AUTH-002: current no-reference SpendTransaction checks balance but no rooted ML0 operator threshold") {
    productionSources.map { sources =>
      val artifact = stripComments(sources(GrammarPaths.artifact).contents)
      val validator = stripComments(
        sources("modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala").contents
      )
      val declaration = sectionAfter(artifact, "case class SpendTransaction(", 450)
      val dispatch = sectionAfter(validator, "spendTransaction.allowSpendRef match", 1800)
      val row = operation("ECO-MG-SOURCE-SPEND")

      expect.all(
        row.activation == ActiveSourceProvenUnsafe,
        row.intendedAuthority == RootedMetagraphOperatorThreshold,
        row.currentAuthority == Missing,
        row.effects == Set(BalanceDebit, BalanceCredit),
        declaration.contains("allowSpendRef: Option[Hash]"),
        dispatch.contains("case None =>"),
        dispatch.contains("validateBalanceSameShard"),
        dispatch.contains("validateBalanceCrossShard"),
        !dispatch.contains("operatorRegistry"),
        !dispatch.contains("registeredThreshold"),
        !dispatch.contains("MetagraphSpendIntent")
      )
    }
  }

  test("RED ECON-R-001: GlobalSnapshotsProcessed replay memory is reconstructed only from a caller-supplied bounded history") {
    productionSources.map { sources =>
      val ops = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/GlobalSnapshotOpsManager.scala"
        ).contents
      )
      val productionCalls = sources.values.toList.flatMap { source =>
        val count = "reconstructProcessedGlobalOrdinals".r.findAllMatchIn(stripComments(source.contents)).size
        List.fill(count)(source.path)
      }.sorted
      val row = operation("ECO-GLOBAL-PROCESSED-ACK")

      expect.all(
        row.activation == ActiveSourceProvenUnsafe,
        row.intendedAuthority == ConsensusPinnedGlobalReplay,
        row.currentAuthority == Missing,
        ops.contains("def reconstructProcessedGlobalOrdinals(snapshots: Iterable[CurrencyIncrementalSnapshot])"),
        productionCalls == List(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotCreator.scala",
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/GlobalSnapshotOpsManager.scala"
        ),
        !ops.contains("permanentNullifier"),
        !ops.contains("processedOrdinalHead")
      )
    }
  }

  test("RED ECON-F-003: framework data fees have no parent/reference head and acceptance never advances lastFeeTxRefs") {
    productionSources.map { sources =>
      val dataApplication = stripComments(sources(GrammarPaths.dataApplication).contents)
      val balanceOps = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/BalanceOpsManager.scala"
        ).contents
      )
      val acceptance = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala"
        ).contents
      )
      val declaration = sectionAfter(dataApplication, "case class FeeTransaction(", 500)
      val row = operation("ECO-DATA-FEE")

      expect.all(
        row.activation == ActiveSourceProvenUnsafe,
        row.intendedAuthority == SourceSignatureAndReference,
        row.currentAuthority == SourceSignatureWithoutReference,
        row.effects == Set(BalanceDebit, BalanceCredit, FeeTransfer),
        declaration.contains("dataUpdateRef: Hash"),
        !declaration.contains("parent"),
        !declaration.contains("ordinal"),
        balanceOps.contains("def acceptFeeTxs("),
        !balanceOps.contains("lastFeeTxRefs"),
        !acceptance.contains("lastFeeTxRefs")
      )
    }
  }

  test("RED ECON-F-004: a signed state-channel fee claim may exceed the deterministic minimum and is debited into no recipient") {
    productionSources.map { sources =>
      val processor = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala"
        ).contents
      )
      val validator = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala"
        ).contents
      )
      val feeBranch = compact(
        sectionAfter(processor, "case (_, Some(Right((lastIncremental, lastState)))) =>", 7000)
      )
      val globalAuthorization = compact(sectionAfter(processor, "def validateForGlobalExecution(", 2500))
      val firstIncrementalBranch = compact(sectionAfter(processor, "case (_, Some(Left(fullSnapshot))) =>", 4000))
      val feeValidation = compact(sectionAfter(validator, "private def validateSnapshotFee(", 1800))
      val optionalOwnerValidation = compact(sectionAfter(validator, "def validateIfAddressAlreadyUsed(", 1000))
      val debitRow = operation("ECO-SNAPSHOT-FEE")
      val noDebitRow = operation("ECO-SNAPSHOT-FEE-NO-DEBIT")

      expect.all(
        debitRow.activation == ActiveSourceProvenUnsafe,
        debitRow.intendedAuthority == ExactDeterministicProtocolFee,
        debitRow.currentAuthority == AuthenticatedBinaryFeeClaimWithOwnerMessageAndMinimum,
        debitRow.effects == Set(BalanceDebit, Burn, DataCarriage),
        debitRow.redObligations("ECON-F-004"),
        noDebitRow.activation == ActiveSourceProvenUnsafe,
        noDebitRow.intendedAuthority == ExactDeterministicProtocolFee,
        noDebitRow.currentAuthority == AuthenticatedBinaryFeeClaimWithMinimum,
        noDebitRow.effects == Set(DataCarriage),
        noDebitRow.redObligations("ECON-F-004A"),
        feeValidation.contains("minFee.value<=signedSC.fee.value"),
        !feeValidation.contains("minFee.value==signedSC.fee.value"),
        optionalOwnerValidation.contains("caseNone=>().validNec"),
        globalAuthorization.contains(
          "if(binary.value.lastSnapshotHash===Hash.empty)stateChannelValidator.validate(output,snapshotOrdinal,SnapshotFeesInfo.empty)"
        ),
        firstIncrementalBranch.contains("deriveNextCurrencyInfo("),
        !firstIncrementalBranch.contains("minus(head.fee)"),
        feeBranch.contains("state.lastMessages.flatMap(_.get(MessageType.Owner)).map(_.address)"),
        feeBranch.contains("balance.minus(head.fee)"),
        feeBranch.contains("balanceUpdate+(feeAddress->uBalance)"),
        !feeBranch.contains("plus(head.fee)"),
        !feeBranch.contains("feeRecipient"),
        !feeBranch.contains("totalSupply")
      )
    }
  }

  test("RED ECON-F-005/F-006: debit-only fees burn and pre-funded stake/collateral requests do not debit again") {
    productionSources.map { sources =>
      val blockAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/block/processing/BlockAcceptanceLogic.scala"
          ).contents
        )
      )
      val allowSpendOps = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/AllowSpendOpsManager.scala"
          ).contents
        )
      )
      val tokenLockOps = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala"
          ).contents
        )
      )
      val balanceOps = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/BalanceOpsManager.scala"
          ).contents
        )
      )
      val delegatedSchema = compact(stripComments(sources(GrammarPaths.delegatedStake).contents))
      val collateralSchema = compact(stripComments(sources(GrammarPaths.nodeCollateral).contents))
      val delegatedAcceptance = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/delegatedStake/UpdateDelegatedStakeAcceptanceManager.scala"
        ).contents
      )
      val collateralAcceptance = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nodeCollateral/UpdateNodeCollateralAcceptanceManager.scala"
        ).contents
      )
      val globalAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala"
          ).contents
        )
      )

      expect.all(
        operation("ECO-TRANSFER-NATIVE").effects == Set(BalanceDebit, BalanceCredit, Burn, ReferenceAdvance),
        operation("ECO-TRANSFER-CURRENCY").effects == Set(BalanceDebit, BalanceCredit, Burn, ReferenceAdvance),
        operation("ECO-ALLOW-CREATE").effects == Set(BalanceDebit, Reserve, Burn, ReferenceAdvance),
        operation("ECO-TOKEN-LOCK-CREATE").effects == Set(BalanceDebit, Reserve, Burn, ReferenceAdvance),
        operation("ECO-TOKEN-LOCK-REPLACE").effects == Set(Release, BalanceDebit, Reserve, Burn, ReferenceAdvance),
        operation("ECO-DATA-FEE").effects == Set(BalanceDebit, BalanceCredit, FeeTransfer),
        operation("ECO-CURRENCY-OWNER-MESSAGE").effects == Set(ParameterState, ReferenceAdvance),
        operation("ECO-DELEGATED-STAKE-CREATE").effects == Set(StakeBacking, ReferenceAdvance, RewardWeight),
        operation("ECO-DELEGATED-STAKE-WITHDRAW").effects == Set(StakeBacking, ReferenceAdvance, RewardWeight),
        operation("ECO-DELEGATED-STAKE-RELEASE").effects == Set(StakeBacking, Release, BalanceCredit),
        operation("ECO-NODE-COLLATERAL-CREATE").effects == Set(CollateralBacking, ReferenceAdvance),
        operation("ECO-NODE-COLLATERAL-WITHDRAW").effects == Set(CollateralBacking, ReferenceAdvance),
        operation("ECO-NODE-COLLATERAL-RELEASE").activation == MissingFailClosed,
        blockAcceptance.contains("minusFeeOps=sortedTxs.groupMap(_.source)(tx=>minusFn(tx.fee))"),
        blockAcceptance.contains("plusAmountOps=sortedTxs.groupMap(_.destination)(tx=>plusFn(tx.amount))"),
        !blockAcceptance.contains("plusFeeOps"),
        allowSpendOps.contains("balanceAfterFee<-balanceAfterAmount.minus(AllowSpendFee.toAmount(allowSpend.fee))"),
        allowSpendOps.contains("balanceAfterExpiredAmount<-currentBalance.plus(SwapAmount.toAmount(allowSpend.amount))"),
        tokenLockOps.contains("minusFee<-minusAmount.minus(TokenLockFee.toAmount(tokenLock.fee))"),
        tokenLockOps.contains("refunded<-bal.plus(TokenLockAmount.toAmount(tokenLock.amount))"),
        balanceOps.contains("updatedWith(tx.source)(existing=>(existing.getOrElse(BigInt(0L))-amount).some)"),
        balanceOps.contains("updatedWith(tx.destination)(existing=>(existing.getOrElse(BigInt(0L))+amount).some)"),
        delegatedSchema.contains("tokenLockRef:Hash"),
        collateralSchema.contains("tokenLockRef:Hash"),
        !delegatedAcceptance.contains(".minus("),
        !delegatedAcceptance.contains(".plus("),
        !delegatedAcceptance.contains(".fee"),
        !collateralAcceptance.contains(".minus("),
        !collateralAcceptance.contains(".plus("),
        !collateralAcceptance.contains(".fee"),
        globalAcceptance.contains("(unexpiredCreate,unexpiredWithdraw,_)=unexpiredNodeCollateralsRaw"),
        globalAcceptance.contains("generateTokenUnlocks(initialData.existingStakes.expired,"),
        !globalAcceptance.contains("generateTokenUnlocks(unexpiredNodeCollateralsRaw")
      )
    }
  }

  test("RED ECON-LANE-001: custom data execution can currently inject framework SharedArtifacts and unsigned TokenUnlocks") {
    productionSources.map { sources =>
      val acceptance = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/DataApplicationSnapshotAcceptanceManager.scala"
        ).contents
      )
      val currencyAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala"
          ).contents
        )
      )
      val manualUnlock = operation("ECO-TOKEN-LOCK-MANUAL")
      val opaque = operation("ECO-OPAQUE-CARRIAGE")

      expect.all(
        manualUnlock.activation == ActiveSourceProvenUnsafe,
        manualUnlock.redObligations("ECON-LANE-001"),
        opaque.activation == CarriageOnly,
        acceptance.contains("getTokenUnlocks(newDataState)"),
        acceptance.contains("sharedArtifacts = newDataState.sharedArtifacts ++ tokenUnlocks"),
        !acceptance.contains("Signed[TokenUnlock]"),
        !acceptance.contains("FrameworkArtifactIntent"),
        currencyAcceptance.contains("case_:GlobalSnapshotsProcessed=>false"),
        currencyAcceptance.contains("case_=>true"),
        currencyAcceptance.contains("acceptedSharedArtifacts=CurrencySnapshotAcceptanceManager.filterFrameworkGeneratedArtifacts("),
        currencyAcceptance.contains("SortedSet(GlobalSnapshotsProcessed(globalSnapshotsProcessed))")
      )
    }
  }

  test("RED ECON-G-002: an absent pricing allowlist currently authorizes every metagraph") {
    productionSources.map { sources =>
      val validator = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/priceOracle/PricingUpdateValidator.scala"
        ).contents
      )
      val row = operation("ECO-PRICE")

      expect.all(
        row.activation == ActiveSourceProvenUnsafe,
        row.intendedAuthority == RootedNetworkAllowlist,
        row.currentAuthority == OptionalLocalConfigurationAllowlist,
        validator.contains("allowedMetagraphIds: Option[List[Address]]"),
        validator.contains("allowedMetagraphIds.isEmpty || allowedMetagraphIds.exists(_.contains(metagraphId))"),
        !validator.contains("rootedAllowlist"),
        !validator.contains("activeEraAllowlist")
      )
    }
  }

  test("SLASH-PRINCIPAL-001 regression: invalid-state slashing changes backing records without consuming principal locks") {
    productionSources.map { sources =>
      val manager = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala"
        ).contents
      )
      val acceptance = stripComments(
        sources(
          "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala"
        ).contents
      )
      val row = operation("ECO-INVALID-STATE-SLASH")

      expect.all(
        row.activation == ActiveWithOpenConservationGate,
        row.effects == Set(StakeBacking, CollateralBacking, BalanceCredit, Mint, Burn, SlashRegistryWrite),
        manager.contains("rec.copy(currentAmount ="),
        manager.contains("priorNodeCollaterals.foldLeft"),
        manager.contains("if (isTarget(rec.event.value.nodeId)) (rs, s + rec.event.value.amount.value.value)"),
        acceptance.contains("slashedDelegatedStakes = res.slashedDelegatedStakes"),
        acceptance.contains("slashedNodeCollaterals = res.slashedNodeCollaterals"),
        acceptance.contains("bountyBalanceDelta.updated"),
        acceptance.contains("mpt.insert[SlashedRegistryEntry]"),
        !manager.contains("activeTokenLocks"),
        !manager.contains("updatedGlobalTokenLocks")
      )
    }
  }

  test("RED ECON-SUPPLY-001: supply is derived and invalid-state bounty credit is inflationary while backing locks remain") {
    productionSources.map { sources =>
      val snapshotInfo = stripComments(
        sources("modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala").contents
      )
      val fieldIds = sectionAfter(
        stripComments(sources("modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala").contents),
        "object GlobalStateFieldId {",
        9000
      )
      val addressService = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/services/AddressService.scala"
          ).contents
        )
      )
      val rewardAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/RewardAcceptanceManager.scala"
          ).contents
        )
      )
      val globalAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala"
          ).contents
        )
      )
      val delegatedStakeSchema = compact(
        stripComments(sources("modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala").contents)
      )
      val totalBurnedUses = "slashApplication\\.totalBurned".r.findAllMatchIn(globalAcceptance).size
      val slash = operation("ECO-INVALID-STATE-SLASH")

      expect.all(
        !snapshotInfo.contains("totalSupply"),
        !fieldIds.contains("TotalSupply"),
        addressService.contains("defgetTotalSupply"),
        addressService.contains("valsupply=balances.foldLeft"),
        rewardAcceptance.contains("current.plus(tx.amount)"),
        !rewardAcceptance.contains("totalSupply"),
        slash.effects == Set(StakeBacking, CollateralBacking, BalanceCredit, Mint, Burn, SlashRegistryWrite),
        delegatedStakeSchema.contains("caseclassDelegatedStakeRecord("),
        delegatedStakeSchema.contains("rewards:Amount"),
        addressService.contains("delegatedStakes.foldLeft"),
        addressService.contains("acc+BigInt(s.rewards.coerce.value)"),
        totalBurnedUses == 1,
        globalAcceptance.contains("burned=$" + "{slashApplication.totalBurned}"),
        globalAcceptance.contains("updatedGlobalTokenLocksCleaned=cleanedMapsResult.cleanedGlobalTokenLocks"),
        globalAcceptance.contains("postEconomicBalances++slashBountyBalanceDelta")
      )
    }
  }

  test("RED ECON-REWARD-CURRENCY-001: GL0 rejects non-empty currency rewards without a deterministic registered implementation") {
    productionSources.map { sources =>
      val validator = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala"
          ).contents
        )
      )
      val wiring = compact(
        stripComments(
          sources("modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedServices.scala").contents
        )
      )
      val row = operation("ECO-REWARD-CURRENCY")

      expect.all(
        row.activation == MissingFailClosed,
        row.intendedAuthority == DeterministicRewardSchedule,
        row.currentAuthority == Missing,
        validator.contains("valrewards=maybeRewards.orElse(Some{"),
        validator.contains("SortedSet.empty[transaction.RewardTransaction].pure[F]"),
        validator.contains("if(creationResult.artifact=!=expected)"),
        wiring.contains("validators.signedValidator,None,None")
      )
    }
  }

  test("RED ECON-OPAQUE-FEE-001: fee activation disables standalone opaque state-channel carriage") {
    productionSources.map { sources =>
      val processor = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala"
          ).contents
        )
      )
      val preFee = operation("ECO-OPAQUE-CARRIAGE")
      val feeEra = operation("ECO-OPAQUE-CARRIAGE-FEE-ERA")

      expect.all(
        preFee.activation == CarriageOnly,
        preFee.currentAuthority == AuthenticatedInclusionOnly,
        feeEra.activation == MissingFailClosed,
        feeEra.intendedAuthority == AuthenticatedInclusionOnly,
        feeEra.currentAuthority == Missing,
        processor.contains("if(isFeeRequired)none.asRight[Agg]"),
        processor.contains("caseNoneif!isFeeRequired"),
        !processor.contains("opaqueFeePayer")
      )
    }
  }

  test("RED ECON-OPAQUE-SHARD-001: total shard assignment removes the generic raw opaque carriage path") {
    productionSources.map { sources =>
      val globalAcceptance = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala"
          ).contents
        )
      )

      expect.all(
        globalAcceptance.contains("case(Some(cfg),Some(assignment))ifcfg.numShards>1=>"),
        globalAcceptance.contains("assignment.shardIdFor(out.address).map(_=>out->true)"),
        globalAcceptance.contains("annotated.collect{case(out,isSharded)if!isSharded=>out}"),
        globalAcceptance.contains("case_=>Async[F].pure(scEvents)"),
        globalAcceptance.contains(
          "processStateChannelEvents(ordinal,updatedGlobalBalances,priorLastStateChannelSnapshotHashes,priorLastCurrencySnapshots,baseScEvents,validationType,getGlobalSnapshotByOrdinal)"
        )
      )
    }
  }

  test("RED ECON-GENESIS-BACKING-001: genesis installs arbitrary active stake and collateral while token locks stay empty") {
    productionSources.map { sources =>
      val loader = compact(
        stripComments(
          sources(
            "modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisLoader.scala"
          ).contents
        )
      )
      val genesisTypes = compact(
        stripComments(
          sources("modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala").contents
        )
      )
      val snapshotInfo = compact(
        stripComments(sources("modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala").contents)
      )
      val stake = operation("ECO-GENESIS-DELEGATED-STAKE")
      val collateral = operation("ECO-GENESIS-NODE-COLLATERAL")

      expect.all(
        stake.activation == ActiveSourceProvenUnsafe,
        stake.effects == Set(StakeBacking, RewardWeight, Mint),
        collateral.activation == ActiveSourceProvenUnsafe,
        collateral.effects == Set(CollateralBacking),
        genesisTypes.contains("caseclassL0GenesisDelegatedStake(event:UpdateDelegatedStake.Create"),
        genesisTypes.contains("rewards:Long"),
        genesisTypes.contains("caseclassL0GenesisNodeCollateral(event:UpdateNodeCollateral.Create"),
        genesisTypes.contains("caseNone=>addr->stipend"),
        loader.contains("activeDelegatedStakes=Some(stakeMap)"),
        loader.contains("valrewardsAmount=Amount(NonNegLong.unsafeFrom(s.rewards))"),
        loader.contains("activeNodeCollaterals=Some(collMap)"),
        !sectionAfter(loader, "base.copy(", 500).contains("activeTokenLocks"),
        snapshotInfo.contains("Some(SortedMap.empty[Address,SortedSet[Signed[TokenLock]]])")
      )
    }
  }

  private def operation(id: String): V4EconomicGrammarManifest.Operation =
    V4EconomicGrammarManifest.operations.find(_.id == id).getOrElse(sys.error(s"Missing manifest operation $id"))

  private def productionSources: IO[Map[String, Source]] = IO.blocking {
    val root = repositoryRoot(NioPaths.get(sys.props("user.dir")).toAbsolutePath.normalize())
    val modules = root.resolve("modules")
    val stream = Files.walk(modules)
    try
      stream
        .iterator()
        .asScala
        .filter(Files.isRegularFile(_))
        .filter(path => path.toString.endsWith(".scala") || path.toString.endsWith(".conf"))
        .filter(path => path.iterator().asScala.exists(_.toString == "main"))
        .map { path =>
          val relative = root.relativize(path).toString.replace('\\', '/')
          relative -> Source(relative, new String(Files.readAllBytes(path), StandardCharsets.UTF_8))
        }
        .toMap
    finally stream.close()
  }

  private def directCaseClassSubtypes(source: String, parentTypes: Set[String]): Set[String] = {
    val tokens = semanticTokens(source)
    tokens.indices.flatMap { index =>
      val declaration = tokens.lift(index) -> tokens.lift(index + 1)
      if (declaration == (Some("case"), Some("class"))) {
        val name = tokens.lift(index + 2)
        val tail = tokens.slice(index + 3, math.min(tokens.size, index + 256))
        val boundary = tail.indexWhere(token => token == "case" || token == "class" || token == "trait" || token == "object")
        val declarationTail = if (boundary < 0) tail else tail.take(boundary)
        val parent = declarationTail.indexOf("extends") match {
          case extendsIndex if extendsIndex >= 0 => declarationTail.lift(extendsIndex + 1)
          case _                                 => None
        }
        name.filter(_ => parent.exists(parentTypes))
      } else None
    }.toSet
  }

  private def directCaseObjectSubtypes(source: String, parentTypes: Set[String]): Set[String] = {
    val pattern = """(?m)\bcase\s+object\s+([A-Za-z_][A-Za-z0-9_]*)\s+extends\s+([A-Za-z_][A-Za-z0-9_]*)""".r
    pattern
      .findAllMatchIn(stripComments(source))
      .collect { case matched if parentTypes(matched.group(2)) => matched.group(1) }
      .toSet
  }

  private def typeDeclarationCount(source: String, symbol: String): Int = {
    val tokens = semanticTokens(source)
    tokens.indices.count { index =>
      (tokens(index) == "class" || tokens(index) == "trait") && tokens.lift(index + 1).contains(symbol)
    }
  }

  private def isEconomicSchemaPath(path: String): Boolean =
    path.startsWith("modules/shared/src/main/scala/io/constellationnetwork/schema/") ||
      path.startsWith("modules/shared/src/main/scala/io/constellationnetwork/currency/") ||
      path.startsWith("modules/shared/src/main/scala/io/constellationnetwork/statechannel/")

  private def suspiciousEconomicConstructors(source: Source): List[String] =
    caseClassDeclaration
      .findAllMatchIn(stripComments(source.contents))
      .map(_.group(1))
      .filter(isSuspiciousEconomicSymbol)
      .toList

  private def isSuspiciousEconomicSymbol(symbol: String): Boolean =
    suspiciousEconomicConstructor.pattern.matcher(symbol).matches()

  private def mentionsEconomicType(source: Source): Boolean =
    source.tokenSet.exists(economicTypeSymbols)

  private def isEconomicCodec(source: Source): Boolean =
    source.path.contains("/serde/codecs/instances/") &&
      source.tokenSet("Codec") &&
      mentionsEconomicType(source)

  private def isEconomicDecoderIngress(source: Source): Boolean = {
    val decoderAtom =
      Set("Decoder", "EntityDecoder", "circeEntityDecoder", "decode", "deserialize", "fromBytes").exists(source.tokenSet)
    val ingressPath =
      source.path.contains("/http/") ||
        source.path.contains("/routes/") ||
        source.path.contains("/snapshot/services/") ||
        source.path.endsWith("/StateChannel.scala")

    ingressPath && decoderAtom && mentionsEconomicType(source)
  }

  private def isEconomicValidationBoundary(source: Source): Boolean = {
    val validationAtom = source.tokenSet.exists { token =>
      token.startsWith("validate") || token.startsWith("verify") || token.startsWith("accept")
    }
    val validationPath =
      source.path.contains("Validator.scala") ||
        source.path.contains("AcceptanceManager.scala") ||
        source.path.contains("OpsManager.scala") ||
        source.path.contains("/consensus/Engine.scala") ||
        source.path.endsWith("ConsensusFunctions.scala")

    validationPath && validationAtom && mentionsEconomicType(source)
  }

  private def isEconomicFeeAuthority(source: Source): Boolean = {
    val feeType = source.tokenSet.exists { token =>
      token.endsWith("Fee") || token.contains("FeeCalculator")
    }
    val feeDecision = source.tokenSet.exists { token =>
      token.startsWith("calculateFee") || token.startsWith("calculateRecommendedFee") || token.startsWith("isFeeRequired")
    }

    feeType && feeDecision && mentionsEconomicType(source)
  }

  private def isEconomicIssuanceOrConfiguration(source: Source): Boolean = {
    val genesis =
      Set("Balance", "initialBalances", "mkGenesis", "loadBalances").exists(source.tokenSet) &&
        (source.path.toLowerCase.contains("genesis") || source.tokenSet("mkGenesis"))
    val rewards =
      source.path.contains("/rewards/") &&
        Set("RewardTransaction", "rewardsPerEpoch", "EmissionConfigEntry", "distribute").exists(source.tokenSet)
    val economicConfig =
      (source.path.contains("/config/") || source.path.endsWith("application.conf")) &&
        Set(
          "ClassicRewardsConfig",
          "DelegatedRewardsConfig",
          "EmissionConfigEntry",
          "OneTimeReward",
          "InvalidStateProofSlashingConfig",
          "tessellation3Migration",
          "tessellation-3-migration",
          "slashFraction"
        ).exists(source.tokenSet) || source.path.endsWith("application.conf")
    val rewardMigrationSwitch =
      source.path.endsWith("GlobalSnapshotConsensusFunctions.scala") &&
        source.tokenSet("v3MigrationOrdinal") && source.tokenSet("classicRewardsFn")

    genesis || rewards || economicConfig || rewardMigrationSwitch
  }

  /** Conservative, package-independent source-shape detector for state-changing economic authority. False positives must be explicitly
    * reviewed. This is a tripwire rather than a static proof: the synthetic discovery test pins correction/debit/credit coverage, and a
    * semantic writer that does not match these broad economic and mutation shapes still requires human review.
    */
  private def isWriterShaped(source: Source): Boolean = {
    val mutationAtoms = Set(
      "plus",
      "minus",
      "update",
      "updated",
      "updatedWith",
      "insert",
      "remove",
      "copy",
      "debit",
      "credit",
      "mint",
      "burn",
      "reserve",
      "release",
      "adjust"
    )
    val authorityAtoms = Set(
      "accept",
      "apply",
      "consume",
      "expire",
      "withdraw",
      "create",
      "process",
      "distribute",
      "validate",
      "settle",
      "correct",
      "write"
    )
    val writerDeclaration = source.tokenSet.exists { token =>
      token.endsWith("Writer") || token.endsWith("Updater") || token.endsWith("AcceptanceManager") ||
      token.endsWith("StateManager") || token.endsWith("OpsManager") || token.endsWith("Processor")
    }

    source.tokenSet.exists(isSuspiciousEconomicSymbol) &&
    source.tokenSet.exists(mutationAtoms) &&
    (writerDeclaration || source.tokenSet.exists(authorityAtoms))
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString

  /** Small Scala lexer sufficient for stable review fingerprints. It drops nested comments and whitespace while preserving literal bytes,
    * identifiers, numeric tokens, punctuation, and operators.
    */
  private def semanticTokens(source: String, hashComments: Boolean = false): Vector[String] = {
    val out = Vector.newBuilder[String]
    var index = 0

    def starts(value: String): Boolean = source.regionMatches(index, value, 0, value.length)
    def takeQuoted(delimiter: String): Unit = {
      val start = index
      index += delimiter.length
      var escaped = false
      var done = false
      while (index < source.length && !done)
        if (delimiter == "\"\"\"") {
          if (starts(delimiter)) {
            index += delimiter.length
            done = true
          } else index += 1
        } else {
          val char = source.charAt(index)
          index += 1
          if (escaped) escaped = false
          else if (char == '\\') escaped = true
          else if (char.toString == delimiter) done = true
        }
      out += source.substring(start, index)
    }

    while (index < source.length) {
      val char = source.charAt(index)
      if (char.isWhitespace) index += 1
      else if (hashComments && char == '#') {
        index += 1
        while (index < source.length && source.charAt(index) != '\n') index += 1
      } else if (starts("//")) {
        index += 2
        while (index < source.length && source.charAt(index) != '\n') index += 1
      } else if (starts("/*")) {
        index += 2
        var depth = 1
        while (index < source.length && depth > 0)
          if (starts("/*")) {
            depth += 1
            index += 2
          } else if (starts("*/")) {
            depth -= 1
            index += 2
          } else index += 1
      } else if (starts("\"\"\"")) takeQuoted("\"\"\"")
      else if (char == '"') takeQuoted("\"")
      else if (char == '\'') takeQuoted("'")
      else if (char == '`') takeQuoted("`")
      else if (char.isLetterOrDigit || char == '_' || char == '$') {
        val start = index
        index += 1
        while (
          index < source.length && {
            val next = source.charAt(index)
            next.isLetterOrDigit || next == '_' || next == '$'
          }
        ) index += 1
        out += source.substring(start, index)
      } else {
        out += char.toString
        index += 1
      }
    }
    out.result()
  }

  private def stripComments(source: String): String = {
    val out = new StringBuilder(source.length)
    var index = 0

    def starts(value: String): Boolean = source.regionMatches(index, value, 0, value.length)
    def appendQuoted(delimiter: String): Unit = {
      var escaped = false
      var done = false
      out.append(delimiter)
      index += delimiter.length
      while (index < source.length && !done)
        if (delimiter == "\"\"\"" && starts(delimiter)) {
          out.append(delimiter)
          index += delimiter.length
          done = true
        } else {
          val char = source.charAt(index)
          out.append(char)
          index += 1
          if (delimiter != "\"\"\"") {
            if (escaped) escaped = false
            else if (char == '\\') escaped = true
            else if (char.toString == delimiter) done = true
          }
        }
    }

    while (index < source.length)
      if (starts("//")) {
        index += 2
        while (index < source.length && source.charAt(index) != '\n') index += 1
      } else if (starts("/*")) {
        index += 2
        var depth = 1
        while (index < source.length && depth > 0)
          if (starts("/*")) {
            depth += 1
            index += 2
          } else if (starts("*/")) {
            depth -= 1
            index += 2
          } else index += 1
      } else if (starts("\"\"\"")) appendQuoted("\"\"\"")
      else if (source.charAt(index) == '"') appendQuoted("\"")
      else if (source.charAt(index) == '\'') appendQuoted("'")
      else {
        out.append(source.charAt(index))
        index += 1
      }
    out.result()
  }

  private def sectionAfter(source: String, anchor: String, length: Int): String = {
    val index = source.indexOf(anchor)
    if (index < 0) "" else source.substring(index, math.min(source.length, index + length))
  }

  private def compact(source: String): String = source.replaceAll("\\s+", "")

  private def repositoryRoot(start: Path): Path = {
    @annotation.tailrec
    def loop(current: Path): Path =
      if (Files.exists(current.resolve("build.sbt"))) current
      else if (current.getParent == null) throw new IllegalStateException(s"Unable to locate repository root from $start")
      else loop(current.getParent)

    loop(start)
  }
}
