package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.util.UUID

import cats.data.Validated.Valid
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect._
import cats.effect.std.Supervisor
import cats.implicits.none
import cats.syntax.applicative._
import cats.syntax.list._
import cats.syntax.traverse._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.reflect.runtime.universe.TypeTag

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.dag.l0.domain.snapshot.programs.{
  GlobalSnapshotEventCutter,
  SnapshotBinaryFeeCalculator,
  UpdateNodeParametersCutter
}
import io.constellationnetwork.dag.l0.infrastructure.rewards.RewardsService
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.env.AppEnvironment.Dev
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.config.{DefaultDelegatedRewardsConfigProvider, DelegatedRewardsConfigProvider}
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{
  UpdateDelegatedStakeAcceptanceManager,
  UpdateDelegatedStakeAcceptanceResult,
  UpdateDelegatedStakeValidator
}
import io.constellationnetwork.node.shared.domain.fork.ForkInfo
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, GlobalStateReader, MptOverlay}
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.domain.node.{UpdateNodeParametersAcceptanceManager, UpdateNodeParametersValidator}
import io.constellationnetwork.node.shared.domain.nodeCollateral.{UpdateNodeCollateralAcceptanceManager, UpdateNodeCollateralValidator}
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger}
import io.constellationnetwork.node.shared.infrastructure.delegatedStake.{RewardsInfoCalculator, RewardsInfoStorage}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global._
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.schema.node.RewardFraction
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.schema.{GlobalStateProofSelector, _}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.SignedValidator.SignedValidationErrorOr
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{signedAllowSpendSetCodec, signedTokenLockSetCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}
import io.constellationnetwork.syntax.sortedCollection._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt, PosLong}
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

object GlobalSnapshotConsensusFunctionsSuite extends MutableIOSuite with Checkers {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  private val activeNativeStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  type Res = (Supervisor[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], Metrics[IO])

  def mkMockGossip[B](spreadRef: Ref[IO, List[B]]): Gossip[IO] =
    new Gossip[IO] {
      override def spread[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        spreadRef.update(rumorContent.asInstanceOf[B] :: _)

      override def spreadCommon[A: TypeTag: Encoder](rumorContent: A): IO[Unit] =
        IO.raiseError(new Exception("spreadCommon: Unexpected call"))

      override def spreadDirect[A: TypeTag: Encoder](rumorContent: A, targets: Set[PeerId]): IO[Unit] = IO.unit

      override def setDirectPushFn(fn: Gossip.DirectPushFn[IO]): IO[Unit] = IO.unit

      override def setSidecarPublishFn(fn: Gossip.SidecarPublishFn[IO]): IO[Unit] = IO.unit
    }

  def mkSignedArtifacts()(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(Signed[GlobalSnapshotArtifact], Signed[GlobalSnapshot])] = for {
    keyPair <- KeyPairGenerator.makeKeyPair[IO]

    genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
    signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair)

    lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
    signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)
  } yield (signedLastArtifact, signedGenesis)

  def sharedResource: Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    m <- Metrics.forAsync[IO](Seq((Metrics.unsafeLabelName("application"), name)))

  } yield (supervisor, j, h, sp, m)

  val bam: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {

    override def acceptBlocksIteratively(
      blocks: List[Signed[Block]],
      context: BlockAcceptanceContext[IO],
      ordinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true
    )(implicit hasher: Hasher[F]): IO[BlockAcceptanceResult] =
      BlockAcceptanceResult(
        BlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]

    override def acceptBlock(
      block: Signed[Block],
      context: BlockAcceptanceContext[IO],
      ordinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true
    )(implicit hasher: Hasher[F]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] = ???

  }

  private final case class NativeBlockAcceptanceCall(
    blocks: List[Signed[Block]],
    ordinal: SnapshotOrdinal,
    shouldPerformMetagraphSpecificValidations: Boolean
  )

  private def isExpectedNativeBlockAcceptance(
    calls: List[NativeBlockAcceptanceCall],
    block: Signed[Block],
    ordinal: SnapshotOrdinal
  ): Boolean =
    calls match {
      case List(call) =>
        call.blocks == List(block) &&
        call.ordinal == ordinal &&
        call.shouldPerformMetagraphSpecificValidations
      case _ => false
    }

  private def recordingNativeBlockAcceptanceManager(
    calls: Ref[IO, List[NativeBlockAcceptanceCall]],
    balanceUpdate: Map[Address, Balance],
    accept: Boolean = true
  ): BlockAcceptanceManager[IO] =
    new BlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[Block]],
        context: BlockAcceptanceContext[IO],
        ordinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true
      )(implicit hasher: Hasher[IO]): IO[BlockAcceptanceResult] =
        calls
          .update(_ :+ NativeBlockAcceptanceCall(blocks, ordinal, shouldPerformMetagraphSpecificValidations))
          .as(
            BlockAcceptanceResult(
              BlockAcceptanceContextUpdate(if (accept) balanceUpdate else Map.empty, Map.empty, Map.empty),
              if (accept) blocks.map(_ -> initUsageCount) else List.empty,
              List.empty
            )
          )

      override def acceptBlock(
        block: Signed[Block],
        context: BlockAcceptanceContext[IO],
        ordinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true
      )(implicit hasher: Hasher[IO]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] =
        IO.raiseError(new IllegalStateException("The global acceptance path must use iterative native block acceptance"))
    }

  private def mkNativeDagBlock(
    sourceKeyPair: java.security.KeyPair,
    destination: Address
  )(
    implicit sp: SecurityProvider[IO],
    hasher: Hasher[IO]
  ): IO[Signed[Block]] = {
    val source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
    val transaction = Transaction(
      source,
      destination,
      TransactionAmount(10L),
      TransactionFee(1L),
      TransactionReference.empty,
      TransactionSalt(1L)
    )

    for {
      signedTransaction <- Signed.forAsyncHasher[IO, Transaction](transaction, sourceKeyPair)
      block = Block(
        NonEmptyList.one(
          BlockReference(
            io.constellationnetwork.schema.height.Height.MinValue,
            io.constellationnetwork.security.hash.ProofsHash(Hash.empty.value)
          )
        ),
        NonEmptySet.one(signedTransaction)
      )
      signedBlock <- Signed.forAsyncHasher[IO, Block](block, sourceKeyPair)
    } yield signedBlock
  }

  private final case class NativeAllowSpendAcceptanceCall(
    blocks: List[Signed[swap.AllowSpendBlock]],
    ordinal: SnapshotOrdinal,
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastGlobalSnapshotEpochProgress: Option[EpochProgress]
  )

  private def isExpectedNativeAllowSpendAcceptance(
    calls: List[NativeAllowSpendAcceptanceCall],
    block: Signed[swap.AllowSpendBlock],
    ordinal: SnapshotOrdinal,
    epochProgress: EpochProgress
  ): Boolean =
    calls match {
      case List(call) =>
        call.blocks == List(block) &&
        call.ordinal == ordinal &&
        call.shouldPerformMetagraphSpecificValidations &&
        call.lastGlobalSnapshotEpochProgress.contains(epochProgress)
      case _ => false
    }

  private def recordingNativeAllowSpendAcceptanceManager(
    calls: Ref[IO, List[NativeAllowSpendAcceptanceCall]],
    reproducedBalances: Map[Address, Balance],
    reproducedRefs: Map[Address, swap.AllowSpendReference] = Map.empty,
    accept: Boolean = true
  ): AllowSpendBlockAcceptanceManager[IO] =
    new AllowSpendBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[swap.AllowSpendBlock]],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
        calls
          .update(
            _ :+ NativeAllowSpendAcceptanceCall(
              blocks,
              snapshotOrdinal,
              shouldPerformMetagraphSpecificValidations,
              lastGlobalSnapshotEpochProgress
            )
          )
          .as(
            AllowSpendBlockAcceptanceResult(
              AllowSpendBlockAcceptanceContextUpdate(
                if (accept) reproducedBalances else Map.empty,
                if (accept) reproducedRefs else Map.empty
              ),
              if (accept) blocks else List.empty,
              List.empty
            )
          )

      override def acceptBlock(
        block: Signed[swap.AllowSpendBlock],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
        IO.raiseError(new IllegalStateException("The global acceptance path must use iterative native allow-spend acceptance"))
    }

  private def mkNativeAllowSpendBlock(
    sourceKeyPair: java.security.KeyPair,
    destination: Address
  )(
    implicit sp: SecurityProvider[IO],
    hasher: Hasher[IO]
  ): IO[Signed[swap.AllowSpendBlock]] =
    for {
      signedAllowSpend <- Signed.forAsyncHasher[IO, swap.AllowSpend](
        swap.AllowSpend(
          source = PublicKeyOps(sourceKeyPair.getPublic).toAddress,
          destination = destination,
          currencyId = None,
          amount = swap.SwapAmount(PosLong.unsafeFrom(10L)),
          fee = swap.AllowSpendFee(1L),
          parent = swap.AllowSpendReference.empty,
          lastValidEpochProgress = EpochProgress(100L),
          approvers = List(destination)
        ),
        sourceKeyPair
      )
      block = swap.AllowSpendBlock(RoundId(new UUID(0L, 1L)), NonEmptySet.one(signedAllowSpend))
      signedBlock <- Signed.forAsyncHasher[IO, swap.AllowSpendBlock](block, sourceKeyPair)
    } yield signedBlock

  private final case class NativeTokenLockAcceptanceCall(
    blocks: List[Signed[TokenLockBlock]],
    ordinal: SnapshotOrdinal,
    shouldPerformMetagraphSpecificValidations: Boolean,
    lastGlobalSnapshotEpochProgress: Option[EpochProgress]
  )

  private def isExpectedNativeTokenLockAcceptance(
    calls: List[NativeTokenLockAcceptanceCall],
    block: Signed[TokenLockBlock],
    ordinal: SnapshotOrdinal,
    epochProgress: EpochProgress
  ): Boolean =
    calls match {
      case List(call) =>
        call.blocks == List(block) &&
        call.ordinal == ordinal &&
        call.shouldPerformMetagraphSpecificValidations &&
        call.lastGlobalSnapshotEpochProgress.contains(epochProgress)
      case _ => false
    }

  private def recordingNativeTokenLockAcceptanceManager(
    calls: Ref[IO, List[NativeTokenLockAcceptanceCall]],
    reproducedBalances: Map[Address, Balance],
    reproducedRefs: Map[Address, TokenLockReference] = Map.empty,
    accept: Boolean = true
  ): TokenLockBlockAcceptanceManager[IO] =
    new TokenLockBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[TokenLockBlock]],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
        calls
          .update(
            _ :+ NativeTokenLockAcceptanceCall(
              blocks,
              snapshotOrdinal,
              shouldPerformMetagraphSpecificValidations,
              lastGlobalSnapshotEpochProgress
            )
          )
          .as(
            TokenLockBlockAcceptanceResult(
              TokenLockBlockAcceptanceContextUpdate(
                if (accept) reproducedBalances else Map.empty,
                if (accept) reproducedRefs else Map.empty,
                Set.empty,
                Map.empty
              ),
              if (accept) blocks else List.empty,
              List.empty
            )
          )

      override def acceptBlock(
        block: Signed[TokenLockBlock],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean = true,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
        IO.raiseError(new IllegalStateException("The global acceptance path must use iterative native token-lock acceptance"))
    }

  private def mkNativeTokenLockBlock(
    sourceKeyPair: java.security.KeyPair
  )(
    implicit sp: SecurityProvider[IO],
    hasher: Hasher[IO]
  ): IO[Signed[TokenLockBlock]] =
    for {
      signedTokenLock <- Signed.forAsyncHasher[IO, TokenLock](
        TokenLock(
          source = PublicKeyOps(sourceKeyPair.getPublic).toAddress,
          amount = TokenLockAmount(PosLong.unsafeFrom(10L)),
          fee = TokenLockFee(1L),
          parent = TokenLockReference.empty,
          currencyId = None,
          unlockEpoch = Some(EpochProgress(100L)),
          replaceTokenLockRef = None
        ),
        sourceKeyPair
      )
      block = TokenLockBlock(RoundId(new UUID(0L, 1L)), NonEmptySet.one(signedTokenLock))
      signedBlock <- Signed.forAsyncHasher[IO, TokenLockBlock](block, sourceKeyPair)
    } yield signedBlock

  val asbam: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
    override def acceptBlock(
      block: Signed[swap.AllowSpendBlock],
      context: AllowSpendBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] = ???

    override def acceptBlocksIteratively(
      blocks: List[Signed[swap.AllowSpendBlock]],
      context: AllowSpendBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
      AllowSpendBlockAcceptanceResult(
        AllowSpendBlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]
  }

  val tlbam: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
    override def acceptBlock(
      block: Signed[TokenLockBlock],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] = ???

    override def acceptBlocksIteratively(
      blocks: List[Signed[TokenLockBlock]],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean = true,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
      TokenLockBlockAcceptanceResult(
        TokenLockBlockAcceptanceContextUpdate.empty,
        List.empty,
        List.empty
      ).pure[IO]
  }

  val scProcessor: GlobalSnapshotStateChannelEventsProcessor[IO] = new GlobalSnapshotStateChannelEventsProcessor[IO] {
    def process(
      snapshotOrdinal: GlobalSnapshotKey,
      currentBalances: SortedMap[Address, Balance],
      priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      events: List[StateChannelOutput],
      validationType: StateChannelValidationType,
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): IO[StateChannelAcceptanceResult] = IO(
      StateChannelAcceptanceResult(
        events.groupByNel(_.address).view.mapValues(_.map(_.snapshotBinary)).toSortedMap,
        priorLastCurrencySnapshots,
        Set.empty,
        SortedMap.empty,
        SortedMap.empty
      )
    )

    def processCurrencySnapshots(
      snapshotOrdinal: SnapshotOrdinal,
      currentBalances: SortedMap[Address, Balance],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): IO[
      SortedMap[
        Address,
        (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
      ]
    ] = IO.pure(
      events.map {
        case (metagraph, binaries) =>
          metagraph -> (binaries.map(_ -> priorLastCurrencySnapshots.get(metagraph)), SortedMap.empty[Address, Balance])
      }
    )

    def assembleAcceptanceResult(
      processed: SortedMap[
        Address,
        (NonEmptyList[(Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])], SortedMap[Address, Balance])
      ],
      priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
        CurrencySnapshot
      ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
      returned: Set[StateChannelOutput]
    ): StateChannelAcceptanceResult = {
      val recreated = SortedMap.from(processed.toList.flatMap {
        case (metagraph, (pairs, _)) => pairs.last._2.map(metagraph -> _)
      })
      val calculated = recreated.foldLeft(priorLastCurrencySnapshots) {
        case (acc, (metagraph, state)) => acc.updated(metagraph, state)
      }

      StateChannelAcceptanceResult(
        accepted = processed.map { case (metagraph, (pairs, _)) => metagraph -> pairs.map(_._1) },
        calculatedCurrencyState = calculated,
        returned = returned,
        balanceUpdate = SortedMap.empty,
        incomingCurrencySnapshotsWithState = SortedMap.empty
      )
    }

  }

  private val signedValidator = new SignedValidator[IO] {
    override def validateSignatures[A: Encoder](signed: Signed[A])(implicit hasher: Hasher[IO]): IO[SignedValidationErrorOr[Signed[A]]] =
      IO.pure(Valid(signed))

    override def validateUniqueSigners[A: Encoder](signed: Signed[A]): SignedValidationErrorOr[Signed[A]] = ???

    override def validateMinSignatureCount[A: Encoder](signed: Signed[A], minSignatureCount: PosInt): SignedValidationErrorOr[Signed[A]] =
      ???

    override def validateMaxSignatureCount[A: Encoder](signed: Signed[A], maxSignatureCount: PosInt): SignedValidationErrorOr[Signed[A]] =
      ???

    override def isSignedBy[A: Encoder](signed: Signed[A], signerAddress: Address): IO[SignedValidationErrorOr[Signed[A]]] = ???

    override def isSignedExclusivelyBy[A: Encoder](signed: Signed[A], signerAddress: Address): IO[SignedValidationErrorOr[Signed[A]]] =
      ???

    override def validateSignaturesWithSeedlist[A <: AnyRef](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???

    override def validateSignedBySeedlistMajority[A](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???

    override def validateAtLeastOneSignatureInSeedlist[A](
      seedlist: Option[Set[PeerId]],
      signed: Signed[A]
    ): SignedValidationErrorOr[Signed[A]] = ???
  }

  def updateNodeParametersAcceptanceManager(implicit txHasher: Hasher[IO]) = {
    val updateNodeParametersValidator =
      UpdateNodeParametersValidator.make(signedValidator, RewardFraction(5_000_000), RewardFraction(10_000_000), PosInt(140), None)
    UpdateNodeParametersAcceptanceManager.make(updateNodeParametersValidator)
  }

  def updateDelegatedStakeAcceptanceManager(implicit hasher: Hasher[IO], sp: SecurityProvider[IO]) = {
    val validator =
      UpdateDelegatedStakeValidator.make(signedValidator, None)
    UpdateDelegatedStakeAcceptanceManager.make(validator)
  }

  def updateNodeCollateralAcceptanceManager(implicit hasher: Hasher[IO], sp: SecurityProvider[IO]) = {
    val validator =
      UpdateNodeCollateralValidator.make(signedValidator, None)
    UpdateNodeCollateralAcceptanceManager.make(validator)
  }

  val collateral: Amount = Amount.empty

  val classicRewards: Rewards[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotEvent] =
    (_, _, _, _, _, _) => IO(SortedSet.empty)

  val delegatorRewards: DelegatedRewardsDistributor[F] = new DelegatedRewardsDistributor[F] {
    def calculateVariableInflation(
      epochProgress: EpochProgress,
      lastSnapshotContext: GlobalSnapshotContext
    ): GlobalSnapshotConsensusFunctionsSuite.F[Amount] = Amount(100L).pure[F]

    def distribute(
      lastSnapshotContext: GlobalSnapshotContext,
      trigger: ConsensusTrigger,
      epochProgress: EpochProgress,
      facilitators: List[(Address, PeerId)],
      delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
      partitionedRecords: PartitionedStakeUpdates
    ): GlobalSnapshotConsensusFunctionsSuite.F[DelegatedRewardsResult] =
      DelegatedRewardsResult(
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedSet.empty,
        SortedSet.empty,
        SortedSet.empty,
        Amount.empty
      )
        .pure[F]

    def getEmissionConfig(epochProgress: EpochProgress): GlobalSnapshotConsensusFunctionsSuite.F[EmissionConfigEntry] =
      delegatedRewardsConfigProvider
        .getConfig()
        .emissionConfig
        .head
        ._2(epochProgress)
        .pure[F]
  }

  val delegatedRewardsConfigProvider: DelegatedRewardsConfigProvider = new DelegatedRewardsConfigProvider {
    def getConfig(): DelegatedRewardsConfig =
      DelegatedRewardsConfig(
        flatInflationRate = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(0, 100),
        emissionConfig = Map.empty,
        percentDistribution = Map.empty,
        oneTimeRewards = Map.empty,
        priceOracleEpoch = Map.empty
      )
  }

  private final case class RootedParent(
    signedArtifact: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    ordinal: SnapshotOrdinal
  )

  private final case class GlobalConsensusFixture(
    functions: GlobalSnapshotConsensusFunctions[IO],
    mptStore: MptStore[IO, GlobalStateKey],
    overlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[IO, GlobalStateKey],
    pendingPostBytes: Ref[IO, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]]
  )

  private type DelegatedRewardsWiring =
    MptOverlay[IO, GlobalStateKey] => (
      DelegatedRewardsDistributor[IO],
      GlobalStateReader[IO] => DelegatedRewardsDistributor[IO]
    )

  private def markerDelegatedRewards(
    reader: GlobalStateReader[IO],
    markerKey: GlobalStateKey,
    rewardPeer: PeerId,
    rewardAddress: Address,
    configProvider: DelegatedRewardsConfigProvider
  ): DelegatedRewardsDistributor[IO] =
    new DelegatedRewardsDistributor[IO] {
      override def getEmissionConfig(epochProgress: EpochProgress): IO[EmissionConfigEntry] =
        configProvider.getConfig().emissionConfig(AppEnvironment.Dev)(epochProgress).pure[IO]

      override def calculateVariableInflation(
        epochProgress: EpochProgress,
        lastSnapshotContext: GlobalSnapshotInfo
      ): IO[Amount] = Amount.empty.pure[IO]

      override def distribute(
        lastSnapshotContext: GlobalSnapshotInfo,
        trigger: ConsensusTrigger,
        epochProgress: EpochProgress,
        facilitators: List[(Address, PeerId)],
        delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
        partitionedRecords: PartitionedStakeUpdates
      ): IO[DelegatedRewardsResult] =
        reader.get[Balance](markerKey).flatMap {
          case Some(marker) =>
            DelegatedRewardsResult(
              delegatorRewardsMap = SortedMap(rewardPeer -> Map(rewardAddress -> Amount(marker.value))),
              updatedCreateDelegatedStakes = partitionedRecords.unexpiredCreateDelegatedStakes,
              updatedWithdrawDelegatedStakes = partitionedRecords.unexpiredWithdrawalsDelegatedStaking,
              nodeOperatorRewards = SortedSet.empty,
              reservedAddressRewards = SortedSet.empty,
              withdrawalRewardTxs = SortedSet.empty,
              totalEmittedRewardsAmount = Amount.empty
            ).pure[IO]
          case None =>
            IO.raiseError(new IllegalStateException("Reward marker missing from selected proposal-parent branch"))
        }
    }

  private def mkRootedParent(
    balances: SortedMap[Address, Balance],
    lastStateChannelSnapshotHashes: SortedMap[Address, Hash] = SortedMap.empty,
    lastCurrencySnapshots: SortedMap[
      Address,
      Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
    ] = SortedMap.empty
  )(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[RootedParent] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      genesis = GlobalSnapshot.mkGenesis(balances, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair)
      legacyArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      parentOrdinal = SnapshotOrdinal.MinValue.next
      parentContext = signedGenesis.value.info.toGlobalSnapshotInfo.copy(
        lastStateChannelSnapshotHashes = lastStateChannelSnapshotHashes,
        lastCurrencySnapshots = lastCurrencySnapshots
      )
      parentProof <- GlobalSnapshotInfo.mptStateProof[IO](parentContext)
      artifact = legacyArtifact.copy(ordinal = parentOrdinal, stateProof = parentProof)
      signedArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](artifact, keyPair)
    } yield RootedParent(signedArtifact, parentContext, artifact.ordinal)

  private def mkGlobalSnapshotConsensusFixture(
    rootedParent: Option[RootedParent] = None,
    stateProofSelector: GlobalStateProofSelector = globalStateProofSelector,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[IO]
    ] = None,
    blockAcceptanceManager: BlockAcceptanceManager[IO] = bam,
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = asbam,
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[IO] = tlbam,
    overlayMode: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.OverlayMode =
      io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.OverlayMode.Passthrough,
    delegatedRewardsWiring: Option[DelegatedRewardsWiring] = None,
    rewardConfigProvider: DelegatedRewardsConfigProvider = delegatedRewardsConfigProvider
  )(
    implicit j: JsonSerializer[IO],
    sp: SecurityProvider[IO],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[GlobalConsensusFixture] = {
    implicit val hs = HasherSelector.forSyncAlwaysCurrent(h)

    val spendActionValidator = SpendActionValidator.make[IO]

    val pricingUpdateValidator = PricingUpdateValidator.make[IO](None, NonNegLong(0))
    val priceStateUpdater = PriceStateUpdater.make[IO](
      Dev,
      rewardConfigProvider,
      io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.empty[IO]
    )

    val feeCalculator = new SnapshotBinaryFeeCalculator[IO] {
      override def calculateFee(
        event: StateChannelEvent,
        ordinal: SnapshotOrdinal
      )(implicit hasher: Hasher[IO]): IO[NonNegLong] =
        event.value.snapshotBinary.value.fee.value.pure[IO]
    }

    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](
        mptProducer,
        GlobalStateKey.toHex[IO]
      )
      _ <- rootedParent.fold(IO.unit) { parent =>
        for {
          _ <- mptStore.syncFromGlobalSnapshotInfo(parent.context, parent.ordinal)
          parentBytes <- mptStore.allEntriesAsBytes
          parentRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](parentBytes)
          signedRoot <- IO.fromOption(parent.signedArtifact.value.stateProof.mptRoot)(
            new IllegalStateException("The rooted native fixture requires an MPT parent root")
          )
          _ <- IO.raiseUnless(parentRoot == signedRoot)(
            new IllegalStateException(s"Fixture MPT root $parentRoot does not match signed parent root $signedRoot")
          )
        } yield ()
      }
      pcTree <- io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree.make[IO]
      mptOverlay <- io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.make[IO, GlobalStateKey](
        mode = overlayMode,
        underlying = mptStore,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty)
      )
      rewardWiring = delegatedRewardsWiring
        .map(_(mptOverlay))
        .getOrElse(
          (
            delegatorRewards,
            (_: io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader[IO]) => delegatorRewards
          )
        )
      (readPathDelegatedRewards, exactParentDelegatedRewardsForReader) = rewardWiring
      dbLogger <- Slf4jLoggerBundle.makeUnsafe[IO]
      snapshotAcceptanceManager <- {
        implicit val globalStateProofSelector: GlobalStateProofSelector = stateProofSelector

        GlobalSnapshotAcceptanceManager.make[IO](
          FieldsAddedOrdinals(
            Map.empty,
            Map.empty,
            Map.empty,
            Map.empty,
            Map.empty,
            Map.empty,
            Map.empty,
            Map.empty
          ),
          MetagraphsSyncConfig(PosInt(100)),
          Dev,
          blockAcceptanceManager,
          allowSpendBlockAcceptanceManager,
          tokenLockBlockAcceptanceManager,
          scProcessor,
          updateNodeParametersAcceptanceManager,
          updateDelegatedStakeAcceptanceManager,
          updateNodeCollateralAcceptanceManager,
          spendActionValidator,
          pricingUpdateValidator,
          priceStateUpdater,
          collateral,
          EpochProgress(NonNegLong(136080L)),
          mptOverlay,
          dbLogger,
          etaRotationSnapshots = 2550L,
          invaliditySlashingConfig = InvalidStateProofSlashingConfig(
            watchtowerEnabled = true,
            slashFraction = io.constellationnetwork.numerics.Ratio.One,
            bountyFraction = io.constellationnetwork.numerics.Ratio(1, 20),
            cooldownEpochs = 100L
          ),
          shardingConfig = shardAcceptanceDeps.map(_.shardingConfig),
          shardCheckpointAcceptanceManager = shardAcceptanceDeps.map(_.acceptanceManager),
          shardAssignment = shardAcceptanceDeps.map(_.shardAssignment)
        )
      }
      rewardsInfoStorage <- RewardsInfoStorage.make
      // §G5: wire the same MPT-backed state managers used by the production code path. The test's
      // `mptStore` is empty initially, so the materializers return empty maps — identical to the
      // GSI-driven prior behaviour where `info.activeDelegatedStakes` defaulted to `SortedMap.empty`.
      g5Reader = io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[IO](mptStore)
      g5StakeManager = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.DelegatedStakeStateManager
        .make[IO](g5Reader)
      g5UnpReader = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.UpdateNodeParametersStateReader
        .make[IO](g5Reader)
      g5BalanceManager = io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.SpendTransactionBalanceManager
        .make[IO](g5Reader)
      rewardsInfoCalculator = RewardsInfoCalculator.make(readPathDelegatedRewards, g5StakeManager, g5UnpReader, g5BalanceManager)
      rewardsService = RewardsService[IO](classicRewards, readPathDelegatedRewards, rewardsInfoCalculator, rewardsInfoStorage)
      // Task #12 slice 2b — the producer's hash-keyed changeset-staging Ref (empty for this unit suite; the
      // promotion/finality path is exercised in the dag-l0 integration loop, not here).
      pendingAccumulatorsRef <- Ref.of[IO, Map[
        Hash,
        (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)
      ]](
        Map.empty
      )
      // 3c-A enabler — sibling signed-bytes staging Ref (empty for this unit suite; rides alongside pendingAccumulatorsRef).
      pendingPostBytesRef <- Ref.of[IO, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]](
        Map.empty
      )
      globalSnapshotConsensusFunction = GlobalSnapshotConsensusFunctions
        .make[IO](
          snapshotAcceptanceManager,
          collateral,
          rewardsService,
          exactParentDelegatedRewardsForReader,
          GlobalSnapshotEventCutter.make[IO](20_000_000, feeCalculator),
          UpdateNodeParametersCutter.make(100),
          AppEnvironment.Dev,
          rewardConfigProvider,
          SnapshotOrdinal.MinValue,
          SnapshotOrdinal.MinValue,
          SnapshotOrdinal.MinValue,
          mptStore,
          mptOverlay,
          shardAcceptanceDeps,
          pendingAccumulatorsRef,
          pendingPostBytesRef,
          // W3a — no-op fraud-proof pool (this suite does not exercise the watchtower path).
          fraudProofPool = io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool.noop[IO]
        )
    } yield GlobalConsensusFixture(globalSnapshotConsensusFunction, mptStore, mptOverlay, pendingPostBytesRef)
  }

  def mkGlobalSnapshotConsensusFunctions(
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[IO]
    ] = None,
    blockAcceptanceManager: BlockAcceptanceManager[IO] = bam,
    allowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = asbam,
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[IO] = tlbam
  )(
    implicit j: JsonSerializer[IO],
    sp: SecurityProvider[IO],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[GlobalSnapshotConsensusFunctions[IO]] =
    mkGlobalSnapshotConsensusFixture(
      shardAcceptanceDeps = shardAcceptanceDeps,
      blockAcceptanceManager = blockAcceptanceManager,
      allowSpendBlockAcceptanceManager = allowSpendBlockAcceptanceManager,
      tokenLockBlockAcceptanceManager = tokenLockBlockAcceptanceManager
    ).map(_.functions)

  private def stagedPostState(
    fixture: GlobalConsensusFixture,
    artifact: GlobalIncrementalSnapshot
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[(Map[Hex, Array[Byte]], Hash)] =
    for {
      artifactHash <- h.hash(artifact)
      staged <- fixture.pendingPostBytes.get
      postBytes <- IO.fromOption(staged.get(artifactHash).map(_._2))(
        new IllegalStateException(s"Missing staged post bytes for native artifact $artifactHash")
      )
      recomputedRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](postBytes)
      signedRoot <- IO.fromOption(artifact.stateProof.mptRoot)(
        new IllegalStateException("Native artifact did not commit an MPT root")
      )
      _ <- IO.raiseUnless(recomputedRoot == signedRoot)(
        new IllegalStateException(s"Staged root $recomputedRoot does not match signed native artifact root $signedRoot")
      )
    } yield (postBytes, recomputedRoot)

  private final case class NativeReplayResult(
    artifact: GlobalSnapshotArtifact,
    producerContext: GlobalSnapshotContext,
    followerContext: Option[GlobalSnapshotContext],
    followerAccepted: Boolean,
    receiptInstancesIsolated: Boolean
  )

  private def produceAndReplayNative(
    parent: RootedParent,
    producer: GlobalConsensusFixture,
    follower: GlobalConsensusFixture,
    events: Set[GlobalSnapshotEvent]
  )(implicit h: Hasher[IO]): IO[NativeReplayResult] =
    for {
      producerReceipt <- producer.functions.createProposalArtifactWithExecutionReceipt(
        parent.ordinal,
        parent.signedArtifact,
        parent.context,
        h,
        EventTrigger,
        events,
        Set.empty,
        _ => None.pure[IO]
      )
      wrongProducerReceipt = follower.functions.consumeProposalExecutionReceipt(producerReceipt)
      producerExecution <- IO.fromOption(producer.functions.consumeProposalExecutionReceipt(producerReceipt))(
        new IllegalStateException("Producer rejected its own native execution receipt")
      )
      (artifact, producerContext, _) = producerExecution
      followerResult <- follower.functions.validateArtifactWithReplayReceipt(
        parent.signedArtifact,
        parent.context,
        EventTrigger,
        artifact,
        Set.empty,
        _ => None.pure[IO]
      )
      wrongFollowerReceipt = followerResult.toOption.flatMap(producer.functions.consumeReplayReceipt)
      followerContext = followerResult.toOption.flatMap(follower.functions.consumeReplayReceipt).map(_._2)
    } yield
      NativeReplayResult(
        artifact,
        producerContext,
        followerContext,
        followerResult.isRight,
        wrongProducerReceipt.isEmpty && wrongFollowerReceipt.isEmpty
      )

  private val activeNativeShardingConfig: ShardingConfig =
    ShardingConfig(
      numShards = 4,
      retention = ShardCheckpointRetentionConfig(retainedCheckpoints = 8L),
      checkpoint = ShardCheckpointConfig(binaryBufferCap = 4096)
    )

  private final case class CheckpointCarriageRig(
    deps: ShardCheckpointWiring.AcceptanceDeps[IO],
    checkpoint: ShardCheckpoint,
    checkpointHash: Hash,
    metagraph: Address,
    verifyCalls: Ref[IO, Int]
  )

  /** Models a candidate that passes the leader's first deterministic checkpoint filter, but whose second GSAM verification cannot be
    * reproduced. The second result is deliberately node-local and carries no fraud-proof artifact; it may reject carriage, but it must not
    * create slash state that a follower cannot reconstruct from the resulting global snapshot.
    */
  private final class RecordedCheckpointManager(
    calls: Ref[IO, Int],
    decision: Int => ShardCheckpointAcceptResult
  ) extends ShardCheckpointGl0AcceptanceManager[IO] {

    private def nextDecision: IO[ShardCheckpointAcceptResult] =
      calls.modify(seen => (seen + 1, decision(seen)))

    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = nextDecision

    override def evaluateForSigning(
      checkpoint: ShardCheckpoint
    ): IO[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] =
      IO.pure(Left(VerifiedShardCheckpointFailure.Rejected("test stub cannot mint signing capabilities")))

    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = nextDecision

    override def verifyExecutionCertificate(checkpoint: ShardCheckpoint): IO[Either[String, Unit]] =
      IO.pure(Left("test stub has no portable execution certificate"))

    override def verifyCommitteeSignature(
      checkpoint: ShardCheckpoint,
      signature: CommitteeMemberSignature
    ): IO[Either[String, Unit]] = IO.pure(Right(()))

    override def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit

    override def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)

    override def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)

    override def lastAdoptedCheckpoint(shardId: ShardId): IO[Option[(ShardOrdinal, Hash)]] = IO.pure(None)

    override def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] = IO.pure(List.empty)
  }

  private def mkCheckpointCarriageRig()(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    m: Metrics[IO]
  ): IO[CheckpointCarriageRig] =
    for {
      checkpointKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      checkpointSigner = PeerId.fromPublic(checkpointKeyPair.getPublic)
      metagraph = Address.fromBytes("artifact-carriage-mg".getBytes("UTF-8"))
      assignment = ShardAssignment.make[IO](activeNativeShardingConfig.numShards)
      shardId <- assignment.shardIdFor(metagraph)
      binary <- Signed.forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(Hash.empty, "artifact-carriage-binary".getBytes("UTF-8"), SnapshotFee.MinValue),
        checkpointKeyPair
      )
      committeeSignature = CommitteeMemberSignature(
        peerId = checkpointSigner,
        vrfProof = Hex(""),
        ed25519Sig = Hex(""),
        kesProductSig = Hex(""),
        kesTreeStep = 0
      )
      checkpoint = ShardCheckpoint(
        shardId = shardId,
        parentCheckpointHash = Hash.empty,
        shardOrdinal = ShardOrdinal(1L),
        gl0AnchorOrdinal = SnapshotOrdinal.MinValue,
        slot = Slot.unsafeApply(1L),
        derivedStateDelta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(metagraph -> Hash("31" * 32)),
          includedSnapshots = SortedMap(metagraph -> NonEmptyList.one(binary))
        ),
        committeeSignatures = NonEmptyList.one(committeeSignature),
        epoch = EtaPeriod(0L),
        executionBase = GlobalSnapshotStateRef(
          SnapshotOrdinal.MinValue,
          Hash("41" * 32),
          Hash.empty,
          MptRoot(Hash("42" * 32))
        )
      )
      signedCheckpoint <- Signed.forAsyncHasher[IO, ShardCheckpoint](checkpoint, checkpointKeyPair)
      chainStore <- ShardChainStore.make[IO](shardId)
      stored <- chainStore.store(
        signedCheckpoint,
        checkpoint.parentCheckpointHash,
        checkpoint.shardOrdinal,
        checkpoint.slot.value.value,
        Array.fill[Byte](32)(1.toByte)
      )
      _ <- IO.raiseUnless(stored)(new IllegalStateException("checkpoint carriage fixture did not enter the shard chain"))
      tipTracker <- ShardTipTracker.make[IO](shardId, checkpointSigner)
      finalityTriggers <- ShardFinalityTriggers.make[IO](shardId, kQuorum = 1, chainStore, tipTracker)
      binaryBuffer <- ShardBinaryBuffer.make[IO](shardId, activeNativeShardingConfig.checkpoint.binaryBufferCap)
      verifyCalls <- Ref.of[IO, Int](0)
      manager = new RecordedCheckpointManager(
        verifyCalls,
        seen =>
          if (seen == 0) ShardCheckpointAcceptResult.Accepted
          else ShardCheckpointAcceptResult.RejectedReExecutionMismatch("second-pass local replay mismatch", List(checkpointSigner))
      )
      checkpointHash <- h.hash(checkpoint.signingPreimage)
      deps = ShardCheckpointWiring.AcceptanceDeps[IO](
        activeNativeShardingConfig,
        manager,
        assignment,
        Map(
          shardId -> ShardCheckpointWiring.ShardRegistryEntry(
            chainStore,
            tipTracker,
            finalityTriggers,
            binaryBuffer
          )
        ),
        (_, _) => IO.pure(Set(checkpointSigner))
      )
    } yield CheckpointCarriageRig(deps, checkpoint, checkpointHash, metagraph, verifyCalls)

  private final case class ContinueAlreadyLifecycleRig(
    parent: RootedParent,
    leaderDeps: ShardCheckpointWiring.AcceptanceDeps[IO],
    followerDeps: ShardCheckpointWiring.AcceptanceDeps[IO],
    checkpoint: ShardCheckpoint,
    alreadyMetagraph: Address,
    continuingMetagraph: Address,
    continuingSuffix: Signed[StateChannelSnapshotBinary],
    leaderVerifyCalls: Ref[IO, Int],
    followerVerifyCalls: Ref[IO, Int]
  )

  private def acceptedCheckpointDeps(
    checkpoint: ShardCheckpoint,
    signedCheckpoint: Signed[ShardCheckpoint],
    checkpointSigner: PeerId,
    assignment: ShardAssignment[IO]
  )(
    implicit h: Hasher[IO],
    m: Metrics[IO]
  ): IO[(ShardCheckpointWiring.AcceptanceDeps[IO], Ref[IO, Int])] =
    for {
      chainStore <- ShardChainStore.make[IO](checkpoint.shardId)
      stored <- chainStore.store(
        signedCheckpoint,
        checkpoint.parentCheckpointHash,
        checkpoint.shardOrdinal,
        checkpoint.slot.value.value,
        Array.fill[Byte](32)(2.toByte)
      )
      _ <- IO.raiseUnless(stored)(new IllegalStateException("successful checkpoint fixture did not enter the shard chain"))
      tipTracker <- ShardTipTracker.make[IO](checkpoint.shardId, checkpointSigner)
      finalityTriggers <- ShardFinalityTriggers.make[IO](checkpoint.shardId, kQuorum = 1, chainStore, tipTracker)
      binaryBuffer <- ShardBinaryBuffer.make[IO](checkpoint.shardId, activeNativeShardingConfig.checkpoint.binaryBufferCap)
      verifyCalls <- Ref.of[IO, Int](0)
      manager = new RecordedCheckpointManager(verifyCalls, _ => ShardCheckpointAcceptResult.Accepted)
      deps = ShardCheckpointWiring.AcceptanceDeps[IO](
        activeNativeShardingConfig,
        manager,
        assignment,
        Map(
          checkpoint.shardId -> ShardCheckpointWiring.ShardRegistryEntry(
            chainStore,
            tipTracker,
            finalityTriggers,
            binaryBuffer
          )
        ),
        (_, _) => IO.pure(Set(checkpointSigner))
      )
    } yield (deps, verifyCalls)

  private def mkContinueAlreadyLifecycleRig()(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    j: JsonSerializer[IO],
    m: Metrics[IO]
  ): IO[ContinueAlreadyLifecycleRig] =
    for {
      checkpointKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      checkpointSigner = PeerId.fromPublic(checkpointKeyPair.getPublic)
      assignment = ShardAssignment.make[IO](activeNativeShardingConfig.numShards)
      candidates <- (0 until 32).toList.traverse { index =>
        val address = Address.fromBytes(s"continue-already-$index".getBytes("UTF-8"))
        assignment.shardIdFor(address).map(shardId => (address, shardId))
      }
      sameShardPair <- IO.fromOption(
        candidates.combinations(2).collectFirst {
          case List((alreadyMetagraph, shardId), (continuingMetagraph, otherShardId)) if shardId == otherShardId =>
            (alreadyMetagraph, continuingMetagraph, shardId)
        }
      )(new IllegalStateException("could not find two deterministic metagraphs assigned to one shard"))
      (alreadyMetagraph, continuingMetagraph, shardId) = sameShardPair
      alreadyBinary <- Signed.forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(Hash.empty, "already-binary".getBytes("UTF-8"), SnapshotFee.MinValue),
        checkpointKeyPair
      )
      continuingPrefix <- Signed.forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(Hash.empty, "continuing-prefix".getBytes("UTF-8"), SnapshotFee.MinValue),
        checkpointKeyPair
      )
      continuingPrefixHash <- h.hash(continuingPrefix.value)
      continuingSuffix <- Signed.forAsyncHasher[IO, StateChannelSnapshotBinary](
        StateChannelSnapshotBinary(
          continuingPrefixHash,
          "continuing-suffix".getBytes("UTF-8"),
          SnapshotFee.MinValue
        ),
        checkpointKeyPair
      )
      alreadyTip <- h.hash(alreadyBinary.value)
      priorCurrency <- Signed.forAsyncHasher[IO, CurrencySnapshot](
        CurrencySnapshot.mkGenesis(Map.empty, None, None),
        checkpointKeyPair
      )
      recreatedState: CurrencySnapshotWithState = Left(priorCurrency)
      alreadyRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(alreadyMetagraph -> recreatedState))
      continuingRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(continuingMetagraph -> recreatedState))
      committeeSignature = CommitteeMemberSignature(
        peerId = checkpointSigner,
        vrfProof = Hex(""),
        ed25519Sig = Hex(""),
        kesProductSig = Hex(""),
        kesTreeStep = 0
      )
      checkpoint = ShardCheckpoint(
        shardId = shardId,
        parentCheckpointHash = Hash.empty,
        shardOrdinal = ShardOrdinal(1L),
        gl0AnchorOrdinal = SnapshotOrdinal.MinValue,
        slot = Slot.unsafeApply(1L),
        derivedStateDelta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(alreadyMetagraph -> alreadyRoot, continuingMetagraph -> continuingRoot),
          includedSnapshots = SortedMap(
            alreadyMetagraph -> NonEmptyList.one(alreadyBinary),
            continuingMetagraph -> NonEmptyList.of(continuingPrefix, continuingSuffix)
          )
        ),
        committeeSignatures = NonEmptyList.one(committeeSignature),
        epoch = EtaPeriod(0L),
        executionBase = GlobalSnapshotStateRef(
          SnapshotOrdinal.MinValue,
          Hash("43" * 32),
          Hash.empty,
          MptRoot(Hash("44" * 32))
        )
      )
      signedCheckpoint <- Signed.forAsyncHasher[IO, ShardCheckpoint](checkpoint, checkpointKeyPair)
      parent <- mkRootedParent(
        balances = SortedMap.empty,
        lastStateChannelSnapshotHashes = SortedMap(
          alreadyMetagraph -> alreadyTip,
          continuingMetagraph -> continuingPrefixHash
        ),
        lastCurrencySnapshots = SortedMap(
          alreadyMetagraph -> (Left(priorCurrency): CurrencySnapshotWithState),
          continuingMetagraph -> (Left(priorCurrency): CurrencySnapshotWithState)
        )
      )
      leaderPair <- acceptedCheckpointDeps(checkpoint, signedCheckpoint, checkpointSigner, assignment)
      followerPair <- acceptedCheckpointDeps(checkpoint, signedCheckpoint, checkpointSigner, assignment)
      (leaderDeps, leaderVerifyCalls) = leaderPair
      (followerDeps, followerVerifyCalls) = followerPair
    } yield
      ContinueAlreadyLifecycleRig(
        parent,
        leaderDeps,
        followerDeps,
        checkpoint,
        alreadyMetagraph,
        continuingMetagraph,
        continuingSuffix,
        leaderVerifyCalls,
        followerVerifyCalls
      )

  private def mkActiveNativeShardDeps(
    selfId: PeerId
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    m: Metrics[IO]
  ): IO[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[IO]] =
    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
      .acceptanceDeps[IO](
        cfg = activeNativeShardingConfig,
        etaRotationSnapshots = 100L,
        kDraw = 4,
        kQuorum = 3,
        selfPeerId = selfId,
        operatorKeyRegistry = io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry.empty[IO],
        activeValidators = IO.pure(Set(selfId)),
        etaForEpoch = (_: io.constellationnetwork.schema.nakamoto.EtaPeriod) => IO.pure(Array.fill[Byte](32)(0.toByte))
      )
      .flatMap(
        IO.fromOption(_)(new IllegalStateException("numShards=4 did not activate shard checkpoint acceptance dependencies"))
      )

  def getTestData(
    implicit sp: SecurityProvider[F],
    j: JsonSerializer[F],
    h: Hasher[IO],
    m: Metrics[IO]
  ): IO[(GlobalSnapshotConsensusFunctions[IO], Set[PeerId], Signed[GlobalSnapshotArtifact], Signed[GlobalSnapshot], StateChannelEvent)] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[F]

      gscf <- mkGlobalSnapshotConsensusFunctions()
      facilitators = Set.empty[PeerId]

      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[F, GlobalSnapshot](genesis, keyPair)

      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)

      scEvent <- mkStateChannelEvent()
    } yield (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent)

  test("validateArtifact - returns artifact for correct data") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent) <- getTestData

      (artifact, _, _) <- gscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      result <- gscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact,
        facilitators,
        _ => None.pure[IO]
      )

      expected = Right(NonEmptyList.one(scEvent.value.snapshotBinary))
      actual = result.map(_._1.stateChannelSnapshots(scEvent.value.address))
      expectation = expect.same(true, result.isRight) && expect.same(expected, actual)
    } yield expectation
  }

  test("delegated rewards are recreated from the exact proposal-parent branch, not ambient best tip") { res =>
    implicit val (_, j, h, sp, m) = res

    val activeRewardsConfig = DefaultDelegatedRewardsConfigProvider
    val siblingBranch = BranchId(Hash("b" * 64))

    for {
      rewardKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      rewardPeer = PeerId.fromPublic(rewardKeyPair.getPublic)
      rewardAddress = PublicKeyOps(rewardKeyPair.getPublic).toAddress
      markerKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, rewardAddress)
      parent <- mkRootedParent(SortedMap.empty)
      parentHash <- h.hash(parent.signedArtifact.value)
      parentBranch = BranchId(parentHash)
      seedFixture = (fixture: GlobalConsensusFixture) =>
        for {
          parentHandle <- fixture.overlay.checkout(BranchId.base)
          _ <- parentHandle.insert[Balance](markerKey, Balance(11L))
          _ <- fixture.overlay.commit(parentHandle, parentBranch, parent.ordinal)
          siblingHandle <- fixture.overlay.checkout(BranchId.base)
          _ <- siblingHandle.insert[Balance](markerKey, Balance(99L))
          _ <- fixture.overlay.commit(siblingHandle, siblingBranch, parent.ordinal)
        } yield ()
      rewardWiring: DelegatedRewardsWiring = overlay => {
        val exactParentFactory: GlobalStateReader[IO] => DelegatedRewardsDistributor[IO] =
          reader => markerDelegatedRewards(reader, markerKey, rewardPeer, rewardAddress, activeRewardsConfig)
        val ambientReader = GlobalStateReader.pending[IO](overlay, IO.pure(Some(siblingBranch)))
        exactParentFactory(ambientReader) -> exactParentFactory
      }
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        overlayMode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        delegatedRewardsWiring = Some(rewardWiring),
        rewardConfigProvider = activeRewardsConfig
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        overlayMode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        delegatedRewardsWiring = Some(rewardWiring),
        rewardConfigProvider = activeRewardsConfig
      )
      _ <- seedFixture(producer)
      _ <- seedFixture(follower)
      producerAmbient <- GlobalStateReader.pending[IO](producer.overlay, IO.pure(Some(siblingBranch))).get[Balance](markerKey)
      producerExact <- GlobalStateReader.fromOverlay[IO](producer.overlay, parentBranch).get[Balance](markerKey)
      followerAmbient <- GlobalStateReader.pending[IO](follower.overlay, IO.pure(Some(siblingBranch))).get[Balance](markerKey)
      followerExact <- GlobalStateReader.fromOverlay[IO](follower.overlay, parentBranch).get[Balance](markerKey)
      (artifact, _, _) <- producer.functions.createProposalArtifact(
        parent.ordinal,
        parent.signedArtifact,
        parent.context,
        h,
        EventTrigger,
        Set.empty,
        Set.empty,
        _ => None.pure[IO]
      )
      followerResult <- follower.functions.validateArtifact(
        parent.signedArtifact,
        parent.context,
        EventTrigger,
        artifact,
        Set.empty,
        _ => None.pure[IO]
      )
      expectedRewards = SortedMap(rewardPeer -> Map(rewardAddress -> Amount(11L)))
    } yield
      expect.eql(Some(Balance(99L)), producerAmbient) &&
        expect.eql(Some(Balance(11L)), producerExact) &&
        expect.eql(Some(Balance(99L)), followerAmbient) &&
        expect.eql(Some(Balance(11L)), followerExact) &&
        expect.eql(Some(expectedRewards), artifact.delegateRewards) &&
        expect(followerResult.isRight) &&
        expect.eql(Some(Some(expectedRewards)), followerResult.toOption.map(_._1.delegateRewards))
  }

  test("validateArtifact - returns invalid artifact error for incorrect data") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      (gscf, facilitators, signedLastArtifact, signedGenesis, scEvent) <- getTestData

      (artifact, _, _) <- gscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      result <- gscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact.copy(ordinal = artifact.ordinal.next),
        facilitators,
        _ => None.pure[IO]
      )
    } yield expect.same(true, result.isLeft)
  }

  test("reflected lower execution receipts with a wrong issuer cannot expose payload") { res =>
    implicit val (_, j, h, sp, m) = res

    def instantiate(className: String, args: Array[AnyRef]): AnyRef = {
      val constructor = Class.forName(className).getDeclaredConstructors.head
      constructor.newInstance(args: _*).asInstanceOf[AnyRef]
    }

    for {
      gscf <- mkGlobalSnapshotConsensusFunctions()
      forgedReplay = instantiate(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions$ReplayReceipt",
        Array(null, null, new Object)
      ).asInstanceOf[GlobalSnapshotReplayReceipt]
      forgedReplayWithNullIssuer = instantiate(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions$ReplayReceipt",
        Array(null, null, null)
      ).asInstanceOf[GlobalSnapshotReplayReceipt]
      forgedProposal = instantiate(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions$ProposalExecutionReceipt",
        Array(null, null, Set.empty[Any].asInstanceOf[AnyRef], new Object)
      ).asInstanceOf[GlobalSnapshotProposalExecutionReceipt]
      forgedProposalWithNullIssuer = instantiate(
        "io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensusFunctions$ProposalExecutionReceipt",
        Array(null, null, Set.empty[Any].asInstanceOf[AnyRef], null)
      ).asInstanceOf[GlobalSnapshotProposalExecutionReceipt]
    } yield
      expect.all(
        gscf.consumeReplayReceipt(null).isEmpty,
        gscf.consumeReplayReceipt(forgedReplay).isEmpty,
        gscf.consumeReplayReceipt(forgedReplayWithNullIssuer).isEmpty,
        gscf.consumeProposalExecutionReceipt(null).isEmpty,
        gscf.consumeProposalExecutionReceipt(forgedProposal).isEmpty,
        gscf.consumeProposalExecutionReceipt(forgedProposalWithNullIssuer).isEmpty
      )
  }

  test("native GL1 blocks are independently accepted and executed by GL0 producer and follower paths") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      block <- mkNativeDagBlock(sourceKeyPair, destination)
      parent <- mkRootedParent(SortedMap(source -> Balance(100L), destination -> Balance.empty))
      reproducedBalances = SortedMap(source -> Balance(89L), destination -> Balance(10L))
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerCalls, reproducedBalances)
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerCalls, reproducedBalances)
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(DAGEvent(block)))
      (_, producerRecomputedRoot) <- stagedPostState(producer, replay.artifact)
      (_, followerRecomputedRoot) <- stagedPostState(follower, replay.artifact)
      producerStoredSource <- producer.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      producerStoredDestination <- producer.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, destination))
      followerStoredSource <- follower.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      followerStoredDestination <- follower.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, destination))
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(replay.receiptInstancesIsolated) &&
        expect(replay.followerAccepted) &&
        expect(isExpectedNativeBlockAcceptance(observedProducerCalls, block, parent.ordinal.next)) &&
        expect(isExpectedNativeBlockAcceptance(observedFollowerCalls, block, parent.ordinal.next)) &&
        expect(replay.artifact.blocks.exists(_.block == block)) &&
        expect(replay.artifact.shardCheckpoints.isEmpty) &&
        expect(replay.artifact.stateChannelSnapshots.isEmpty) &&
        expect.eql(Some(Balance(89L)), producerStoredSource) &&
        expect.eql(Some(Balance(10L)), producerStoredDestination) &&
        expect.eql(producerStoredSource, followerStoredSource) &&
        expect.eql(producerStoredDestination, followerStoredDestination) &&
        expect(replay.artifact.stateProof.mptRoot.contains(producerRecomputedRoot)) &&
        expect.eql(producerRecomputedRoot, followerRecomputedRoot) &&
        expect.eql(reproducedBalances, replay.producerContext.balances) &&
        expect.eql(Some(reproducedBalances), replay.followerContext.map(_.balances))
  }

  test("a GL0 follower rejects a leader artifact when native GL1 execution differs") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      block <- mkNativeDagBlock(sourceKeyPair, destination)
      parent <- mkRootedParent(SortedMap(source -> Balance(100L), destination -> Balance.empty))
      producerBalances = SortedMap(source -> Balance(89L), destination -> Balance(10L))
      divergentFollowerBalances = SortedMap(source -> Balance(88L), destination -> Balance(11L))
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerCalls, producerBalances)
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerCalls, divergentFollowerBalances)
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(DAGEvent(block)))
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(isExpectedNativeBlockAcceptance(observedProducerCalls, block, parent.ordinal.next)) &&
        expect(isExpectedNativeBlockAcceptance(observedFollowerCalls, block, parent.ordinal.next)) &&
        expect(!replay.followerAccepted)
  }

  // ───────────────────────────────────────────────────────────────────────────────────────────────
  test("native GL1 allow-spends are independently accepted and executed by GL0 producer and follower paths") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      producerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      block <- mkNativeAllowSpendBlock(sourceKeyPair, destination)
      signedAllowSpend = block.value.transactions.head
      reproducedRef <- swap.AllowSpendReference.of(signedAllowSpend)
      parent <- mkRootedParent(SortedMap(source -> Balance(100L), destination -> Balance.empty))
      reproducedBalances = SortedMap(source -> Balance(89L), destination -> Balance.empty)
      reproducedRefs = Map(source -> reproducedRef)
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerBlockCalls, Map.empty),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          producerCalls,
          reproducedBalances,
          reproducedRefs
        )
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerBlockCalls, Map.empty),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          followerCalls,
          reproducedBalances,
          reproducedRefs
        )
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(AllowSpendEvent(block)))
      (_, producerRecomputedRoot) <- stagedPostState(producer, replay.artifact)
      (_, followerRecomputedRoot) <- stagedPostState(follower, replay.artifact)
      producerStoredBalance <- producer.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      producerStoredRef <- producer.mptStore
        .get[swap.AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, source))
      producerStoredActive <- producer.mptStore.get[SortedSet[Signed[swap.AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, source)
      )
      followerStoredBalance <- follower.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      followerStoredRef <- follower.mptStore
        .get[swap.AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, source))
      followerStoredActive <- follower.mptStore.get[SortedSet[Signed[swap.AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, source)
      )
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(replay.followerAccepted) &&
        expect(replay.receiptInstancesIsolated) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedProducerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedFollowerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(replay.artifact.allowSpendBlocks.exists(_.contains(block))) &&
        expect(replay.artifact.tokenLockBlocks.forall(_.isEmpty)) &&
        expect(replay.artifact.shardCheckpoints.isEmpty) &&
        expect(replay.artifact.stateChannelSnapshots.isEmpty) &&
        expect.eql(Some(Balance(89L)), producerStoredBalance) &&
        expect.eql(Some(reproducedRef), producerStoredRef) &&
        expect.eql(Some(SortedSet(signedAllowSpend)), producerStoredActive) &&
        expect.eql(producerStoredBalance, followerStoredBalance) &&
        expect.eql(producerStoredRef, followerStoredRef) &&
        expect.eql(producerStoredActive, followerStoredActive) &&
        expect(replay.artifact.stateProof.mptRoot.contains(producerRecomputedRoot)) &&
        expect.eql(producerRecomputedRoot, followerRecomputedRoot) &&
        expect.eql(reproducedBalances, replay.producerContext.balances) &&
        expect.eql(Some(reproducedBalances), replay.followerContext.map(_.balances))
  }

  test("a GL0 follower rejects same-parent native allow-spend replay when the accepted reference output differs") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      producerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      shardOperatorKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      shardOperator = PeerId.fromPublic(shardOperatorKeyPair.getPublic)
      block <- mkNativeAllowSpendBlock(sourceKeyPair, destination)
      signedAllowSpend = block.value.transactions.head
      producerRef <- swap.AllowSpendReference.of(signedAllowSpend)
      divergentFollowerRef = swap.AllowSpendReference(producerRef.ordinal, Hash("a" * 64))
      parent <- mkRootedParent(SortedMap(source -> Balance(100L), destination -> Balance.empty))
      producerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      followerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      acceptedBalances = SortedMap(source -> Balance(89L), destination -> Balance.empty)
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(producerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerBlockCalls, Map.empty),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          producerCalls,
          acceptedBalances,
          Map(source -> producerRef)
        )
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(followerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerBlockCalls, Map.empty),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          followerCalls,
          acceptedBalances,
          Map(source -> divergentFollowerRef)
        )
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(AllowSpendEvent(block)))
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(
        isExpectedNativeAllowSpendAcceptance(
          observedProducerCalls,
          block,
          parent.ordinal.next,
          EpochProgress.MinValue
        )
      ) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedFollowerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(producerRef != divergentFollowerRef) &&
        expect(!replay.followerAccepted)
  }

  test("native GL1 token-locks are independently accepted and executed by GL0 producer and follower paths") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      producerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      block <- mkNativeTokenLockBlock(sourceKeyPair)
      signedTokenLock = block.value.tokenLocks.head
      reproducedRef <- TokenLockReference.of(signedTokenLock)
      parent <- mkRootedParent(SortedMap(source -> Balance(100L)))
      reproducedBalances = SortedMap(source -> Balance(89L))
      reproducedRefs = Map(source -> reproducedRef)
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerBlockCalls, Map.empty),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          producerCalls,
          reproducedBalances,
          reproducedRefs
        )
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerBlockCalls, Map.empty),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          followerCalls,
          reproducedBalances,
          reproducedRefs
        )
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(TokenLockEvent(block)))
      (_, producerRecomputedRoot) <- stagedPostState(producer, replay.artifact)
      (_, followerRecomputedRoot) <- stagedPostState(follower, replay.artifact)
      producerStoredBalance <- producer.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      producerStoredRef <- producer.mptStore
        .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, source))
      producerStoredActive <- producer.mptStore.get[SortedSet[Signed[TokenLock]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      )
      followerStoredBalance <- follower.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      followerStoredRef <- follower.mptStore
        .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, source))
      followerStoredActive <- follower.mptStore.get[SortedSet[Signed[TokenLock]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      )
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(replay.followerAccepted) &&
        expect(replay.receiptInstancesIsolated) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedProducerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedFollowerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(replay.artifact.tokenLockBlocks.exists(_.contains(block))) &&
        expect(replay.artifact.allowSpendBlocks.forall(_.isEmpty)) &&
        expect(replay.artifact.shardCheckpoints.isEmpty) &&
        expect(replay.artifact.stateChannelSnapshots.isEmpty) &&
        expect.eql(Some(Balance(89L)), producerStoredBalance) &&
        expect.eql(Some(reproducedRef), producerStoredRef) &&
        expect.eql(Some(SortedSet(signedTokenLock)), producerStoredActive) &&
        expect.eql(producerStoredBalance, followerStoredBalance) &&
        expect.eql(producerStoredRef, followerStoredRef) &&
        expect.eql(producerStoredActive, followerStoredActive) &&
        expect(replay.artifact.stateProof.mptRoot.contains(producerRecomputedRoot)) &&
        expect.eql(producerRecomputedRoot, followerRecomputedRoot) &&
        expect.eql(reproducedBalances, replay.producerContext.balances) &&
        expect.eql(Some(reproducedBalances), replay.followerContext.map(_.balances))
  }

  test("a GL0 follower rejects same-parent native token-lock replay when the accepted reference output differs") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      followerCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      producerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      shardOperatorKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      shardOperator = PeerId.fromPublic(shardOperatorKeyPair.getPublic)
      block <- mkNativeTokenLockBlock(sourceKeyPair)
      signedTokenLock = block.value.tokenLocks.head
      producerRef <- TokenLockReference.of(signedTokenLock)
      divergentFollowerRef = TokenLockReference(producerRef.ordinal, Hash("b" * 64))
      parent <- mkRootedParent(SortedMap(source -> Balance(100L)))
      producerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      followerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      acceptedBalances = SortedMap(source -> Balance(89L))
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(producerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(producerBlockCalls, Map.empty),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          producerCalls,
          acceptedBalances,
          Map(source -> producerRef)
        )
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(followerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(followerBlockCalls, Map.empty),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          followerCalls,
          acceptedBalances,
          Map(source -> divergentFollowerRef)
        )
      )
      replay <- produceAndReplayNative(parent, producer, follower, Set(TokenLockEvent(block)))
      observedProducerCalls <- producerCalls.get
      observedFollowerCalls <- followerCalls.get
    } yield
      expect(
        isExpectedNativeTokenLockAcceptance(
          observedProducerCalls,
          block,
          parent.ordinal.next,
          EpochProgress.MinValue
        )
      ) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedFollowerCalls,
            block,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(producerRef != divergentFollowerRef) &&
        expect(!replay.followerAccepted)
  }

  test("a GL0 producer omits native DAG, allow-spend, and token-lock events that local execution rejects") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      blockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      allowSpendCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      tokenLockCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      dagBlock <- mkNativeDagBlock(sourceKeyPair, destination)
      allowSpendBlock <- mkNativeAllowSpendBlock(sourceKeyPair, destination)
      tokenLockBlock <- mkNativeTokenLockBlock(sourceKeyPair)
      parent <- mkRootedParent(SortedMap(source -> Balance(100L), destination -> Balance.empty))
      rejecting <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(blockCalls, Map.empty, accept = false),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          allowSpendCalls,
          Map.empty,
          accept = false
        ),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          tokenLockCalls,
          Map.empty,
          accept = false
        )
      )
      control <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector
      )
      (rejectedArtifact, rejectedContext, _) <- rejecting.functions.createProposalArtifact(
        parent.ordinal,
        parent.signedArtifact,
        parent.context,
        h,
        EventTrigger,
        Set(DAGEvent(dagBlock), AllowSpendEvent(allowSpendBlock), TokenLockEvent(tokenLockBlock)),
        Set.empty,
        _ => None.pure[IO]
      )
      (controlArtifact, controlContext, _) <- control.functions.createProposalArtifact(
        parent.ordinal,
        parent.signedArtifact,
        parent.context,
        h,
        EventTrigger,
        Set.empty,
        Set.empty,
        _ => None.pure[IO]
      )
      (_, recomputedRoot) <- stagedPostState(rejecting, rejectedArtifact)
      storedBalance <- rejecting.mptStore.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      storedAllowSpendRef <- rejecting.mptStore
        .get[swap.AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, source))
      storedAllowSpends <- rejecting.mptStore.get[SortedSet[Signed[swap.AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, source)
      )
      storedTokenLockRef <- rejecting.mptStore
        .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, source))
      storedTokenLocks <- rejecting.mptStore.get[SortedSet[Signed[TokenLock]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      )
      observedBlockCalls <- blockCalls.get
      observedAllowSpendCalls <- allowSpendCalls.get
      observedTokenLockCalls <- tokenLockCalls.get
    } yield
      expect(isExpectedNativeBlockAcceptance(observedBlockCalls, dagBlock, parent.ordinal.next)) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedAllowSpendCalls,
            allowSpendBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedTokenLockCalls,
            tokenLockBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(rejectedArtifact.blocks.forall(_.block != dagBlock)) &&
        expect(rejectedArtifact.allowSpendBlocks.forall(!_.contains(allowSpendBlock))) &&
        expect(rejectedArtifact.tokenLockBlocks.forall(!_.contains(tokenLockBlock))) &&
        expect.eql(controlArtifact, rejectedArtifact) &&
        expect.eql(controlContext, rejectedContext) &&
        expect.eql(Some(Balance(100L)), storedBalance) &&
        expect(storedAllowSpendRef.isEmpty) &&
        expect(storedAllowSpends.isEmpty) &&
        expect(storedTokenLockRef.isEmpty) &&
        expect(storedTokenLocks.isEmpty) &&
        expect(rejectedArtifact.stateProof.mptRoot.contains(recomputedRoot))
  }

  test("active execution sharding does not bypass universal GL0 replay of any native GL1 event family") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      producerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      followerBlockCalls <- Ref.of[IO, List[NativeBlockAcceptanceCall]](List.empty)
      producerAllowSpendCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      followerAllowSpendCalls <- Ref.of[IO, List[NativeAllowSpendAcceptanceCall]](List.empty)
      producerTokenLockCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      followerTokenLockCalls <- Ref.of[IO, List[NativeTokenLockAcceptanceCall]](List.empty)
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      shardOperatorKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = PublicKeyOps(sourceKeyPair.getPublic).toAddress
      destination = PublicKeyOps(destinationKeyPair.getPublic).toAddress
      shardOperator = PeerId.fromPublic(shardOperatorKeyPair.getPublic)
      dagBlock <- mkNativeDagBlock(sourceKeyPair, destination)
      allowSpendBlock <- mkNativeAllowSpendBlock(sourceKeyPair, destination)
      tokenLockBlock <- mkNativeTokenLockBlock(sourceKeyPair)
      signedAllowSpend = allowSpendBlock.value.transactions.head
      signedTokenLock = tokenLockBlock.value.tokenLocks.head
      allowSpendRef <- swap.AllowSpendReference.of(allowSpendBlock.value.transactions.head)
      tokenLockRef <- TokenLockReference.of(tokenLockBlock.value.tokenLocks.head)
      parent <- mkRootedParent(SortedMap(source -> Balance(300L), destination -> Balance.empty))
      producerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      followerShardDeps <- mkActiveNativeShardDeps(shardOperator)
      producer <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(producerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(
          producerBlockCalls,
          SortedMap(source -> Balance(289L), destination -> Balance(10L))
        ),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          producerAllowSpendCalls,
          SortedMap(source -> Balance(278L), destination -> Balance(10L)),
          Map(source -> allowSpendRef)
        ),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          producerTokenLockCalls,
          SortedMap(source -> Balance(267L), destination -> Balance(10L)),
          Map(source -> tokenLockRef)
        )
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(parent),
        stateProofSelector = activeNativeStateProofSelector,
        shardAcceptanceDeps = Some(followerShardDeps),
        blockAcceptanceManager = recordingNativeBlockAcceptanceManager(
          followerBlockCalls,
          SortedMap(source -> Balance(289L), destination -> Balance(10L))
        ),
        allowSpendBlockAcceptanceManager = recordingNativeAllowSpendAcceptanceManager(
          followerAllowSpendCalls,
          SortedMap(source -> Balance(278L), destination -> Balance(10L)),
          Map(source -> allowSpendRef)
        ),
        tokenLockBlockAcceptanceManager = recordingNativeTokenLockAcceptanceManager(
          followerTokenLockCalls,
          SortedMap(source -> Balance(267L), destination -> Balance(10L)),
          Map(source -> tokenLockRef)
        )
      )
      replay <- produceAndReplayNative(
        parent,
        producer,
        follower,
        Set[GlobalSnapshotEvent](DAGEvent(dagBlock), AllowSpendEvent(allowSpendBlock), TokenLockEvent(tokenLockBlock))
      )
      (_, producerRecomputedRoot) <- stagedPostState(producer, replay.artifact)
      (_, followerRecomputedRoot) <- stagedPostState(follower, replay.artifact)
      producerStoredSourceBalance <- producer.mptStore
        .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      producerStoredDestinationBalance <- producer.mptStore
        .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, destination))
      producerStoredAllowSpendRef <- producer.mptStore
        .get[swap.AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, source))
      producerStoredAllowSpends <- producer.mptStore.get[SortedSet[Signed[swap.AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, source)
      )
      producerStoredTokenLockRef <- producer.mptStore
        .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, source))
      producerStoredTokenLocks <- producer.mptStore.get[SortedSet[Signed[TokenLock]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      )
      followerStoredSourceBalance <- follower.mptStore
        .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source))
      followerStoredDestinationBalance <- follower.mptStore
        .get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, destination))
      followerStoredAllowSpendRef <- follower.mptStore
        .get[swap.AllowSpendReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, source))
      followerStoredAllowSpends <- follower.mptStore.get[SortedSet[Signed[swap.AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, None, source)
      )
      followerStoredTokenLockRef <- follower.mptStore
        .get[TokenLockReference](GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, source))
      followerStoredTokenLocks <- follower.mptStore.get[SortedSet[Signed[TokenLock]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      )
      observedProducerBlockCalls <- producerBlockCalls.get
      observedFollowerBlockCalls <- followerBlockCalls.get
      observedProducerAllowSpendCalls <- producerAllowSpendCalls.get
      observedFollowerAllowSpendCalls <- followerAllowSpendCalls.get
      observedProducerTokenLockCalls <- producerTokenLockCalls.get
      observedFollowerTokenLockCalls <- followerTokenLockCalls.get
    } yield
      expect(replay.followerAccepted) &&
        expect(replay.receiptInstancesIsolated) &&
        expect(isExpectedNativeBlockAcceptance(observedProducerBlockCalls, dagBlock, parent.ordinal.next)) &&
        expect(isExpectedNativeBlockAcceptance(observedFollowerBlockCalls, dagBlock, parent.ordinal.next)) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedProducerAllowSpendCalls,
            allowSpendBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeAllowSpendAcceptance(
            observedFollowerAllowSpendCalls,
            allowSpendBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedProducerTokenLockCalls,
            tokenLockBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(
          isExpectedNativeTokenLockAcceptance(
            observedFollowerTokenLockCalls,
            tokenLockBlock,
            parent.ordinal.next,
            EpochProgress.MinValue
          )
        ) &&
        expect(replay.artifact.blocks.exists(_.block == dagBlock)) &&
        expect(replay.artifact.allowSpendBlocks.exists(_.contains(allowSpendBlock))) &&
        expect(replay.artifact.tokenLockBlocks.exists(_.contains(tokenLockBlock))) &&
        expect(replay.artifact.shardCheckpoints.isEmpty) &&
        expect(replay.artifact.stateChannelSnapshots.isEmpty) &&
        expect.eql(Some(Balance(267L)), producerStoredSourceBalance) &&
        expect.eql(Some(Balance(10L)), producerStoredDestinationBalance) &&
        expect.eql(Some(allowSpendRef), producerStoredAllowSpendRef) &&
        expect.eql(Some(SortedSet(signedAllowSpend)), producerStoredAllowSpends) &&
        expect.eql(Some(tokenLockRef), producerStoredTokenLockRef) &&
        expect.eql(Some(SortedSet(signedTokenLock)), producerStoredTokenLocks) &&
        expect.eql(producerStoredSourceBalance, followerStoredSourceBalance) &&
        expect.eql(producerStoredDestinationBalance, followerStoredDestinationBalance) &&
        expect.eql(producerStoredAllowSpendRef, followerStoredAllowSpendRef) &&
        expect.eql(producerStoredAllowSpends, followerStoredAllowSpends) &&
        expect.eql(producerStoredTokenLockRef, followerStoredTokenLockRef) &&
        expect.eql(producerStoredTokenLocks, followerStoredTokenLocks) &&
        expect(replay.artifact.stateProof.mptRoot.contains(producerRecomputedRoot)) &&
        expect.eql(producerRecomputedRoot, followerRecomputedRoot)
  }

  test("a second-pass checkpoint mismatch without portable evidence is omitted without slash state and recreates on a follower") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      parent <- mkRootedParent(SortedMap.empty)
      leaderRig <- mkCheckpointCarriageRig()
      followerRig <- mkCheckpointCarriageRig()
      leader <- mkGlobalSnapshotConsensusFixture(rootedParent = Some(parent), shardAcceptanceDeps = Some(leaderRig.deps))
      follower <- mkGlobalSnapshotConsensusFixture(rootedParent = Some(parent), shardAcceptanceDeps = Some(followerRig.deps))

      (artifact, leaderContext, _) <- leader.functions.createProposalArtifact(
        parent.ordinal,
        parent.signedArtifact,
        parent.context,
        h,
        EventTrigger,
        Set.empty,
        Set.empty,
        _ => None.pure[IO]
      )
      leaderVerifyCalls <- leaderRig.verifyCalls.get
      locallySlashed <- io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader
        .fromMptStore[IO](leader.mptStore)
        .wasSlashed(leaderRig.checkpoint.shardId, leaderRig.checkpointHash)

      followerResult <- follower.functions.validateArtifact(
        parent.signedArtifact,
        parent.context,
        EventTrigger,
        artifact,
        Set.empty,
        _ => None.pure[IO]
      )
      followerVerifyCalls <- followerRig.verifyCalls.get
    } yield
      expect.all(
        leaderVerifyCalls == 2,
        artifact.shardCheckpoints.isEmpty,
        !artifact.stateChannelSnapshots.contains(leaderRig.metagraph),
        artifact.fraudProofs.isEmpty,
        !locallySlashed,
        followerVerifyCalls == 0,
        followerResult.exists { case (recreated, followerContext) => recreated == artifact && followerContext == leaderContext }
      )
  }

  test("a Continue+Already checkpoint embeds with only its continuing suffix and recreates byte-identically on a follower") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      rig <- mkContinueAlreadyLifecycleRig()
      leader <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(rig.parent),
        shardAcceptanceDeps = Some(rig.leaderDeps)
      )
      follower <- mkGlobalSnapshotConsensusFixture(
        rootedParent = Some(rig.parent),
        shardAcceptanceDeps = Some(rig.followerDeps)
      )
      (artifact, leaderContext, _) <- leader.functions.createProposalArtifact(
        rig.parent.ordinal,
        rig.parent.signedArtifact,
        rig.parent.context,
        h,
        EventTrigger,
        Set.empty,
        Set.empty,
        _ => None.pure[IO]
      )
      leaderCalls <- rig.leaderVerifyCalls.get
      continuingTip <- h.hash(rig.continuingSuffix.value)

      followerResult <- follower.functions.validateArtifact(
        rig.parent.signedArtifact,
        rig.parent.context,
        EventTrigger,
        artifact,
        Set.empty,
        _ => None.pure[IO]
      )
      followerCalls <- rig.followerVerifyCalls.get
      expectedSnapshots = SortedMap(rig.continuingMetagraph -> NonEmptyList.one(rig.continuingSuffix))
      expectedCheckpoints = SortedMap(rig.checkpoint.shardId -> rig.checkpoint)
    } yield
      expect.all(
        leaderCalls == 2,
        artifact.shardCheckpoints == expectedCheckpoints,
        artifact.stateChannelSnapshots == expectedSnapshots,
        !artifact.stateChannelSnapshots.contains(rig.alreadyMetagraph),
        leaderContext.lastStateChannelSnapshotHashes.get(rig.continuingMetagraph).contains(continuingTip),
        leaderContext.lastStateChannelSnapshotHashes.get(rig.alreadyMetagraph) ==
          rig.parent.context.lastStateChannelSnapshotHashes.get(rig.alreadyMetagraph),
        followerCalls == 2,
        followerResult.exists { case (recreated, followerContext) => recreated == artifact && followerContext == leaderContext }
      )
  }

  // With execution sharding active, a metagraph binary can enter GL0 only through a qualifying shard
  // checkpoint. A naked state-channel binary is filtered from GSAM's ordinary state-channel path, so the
  // active follower recreates a different artifact and rejects it. Native GL1 events remain on their
  // independent universal-GL0-execution path.
  test("active execution sharding rejects a naked state-channel binary without a shard checkpoint") { res =>
    implicit val (_, j, h, sp, m) = res

    val shardingCfg: ShardingConfig =
      ShardingConfig(
        numShards = 4,
        retention = ShardCheckpointRetentionConfig(retainedCheckpoints = 8L),
        checkpoint = ShardCheckpointConfig(binaryBufferCap = 4096)
      )

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[F]
      selfId = PeerId.fromPublic(keyPair.getPublic)
      // Leader: NO sharding deps (numShards=1 regression-bar leader). It folds NO shard checkpoints, so
      // its artifact's stateChannelSnapshots come purely from the SC event we pass.
      leaderGscf <- mkGlobalSnapshotConsensusFunctions()
      // Follower: ACTIVE sharding deps (numShards=4) with a fresh, EMPTY per-shard chain registry.
      followerDeps <- io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
        .acceptanceDeps[IO](
          cfg = shardingCfg,
          etaRotationSnapshots = 100L,
          kDraw = 4,
          kQuorum = 3,
          selfPeerId = selfId,
          operatorKeyRegistry = io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry.empty[IO],
          activeValidators = IO.pure(Set(selfId)),
          etaForEpoch = (_: io.constellationnetwork.schema.nakamoto.EtaPeriod) => IO.pure(Array.fill[Byte](32)(0.toByte))
        )
      followerGscf <- mkGlobalSnapshotConsensusFunctions(followerDeps)

      facilitators = Set.empty[PeerId]
      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[F, GlobalSnapshot](genesis, keyPair)
      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)
      scEvent <- mkStateChannelEvent()

      // Leader produces an artifact carrying the SC binary in `stateChannelSnapshots`.
      (artifact, _, _) <- leaderGscf.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )
      result <- followerGscf.validateArtifact(
        signedLastArtifact,
        signedGenesis.value.info.toGlobalSnapshotInfo,
        EventTrigger,
        artifact,
        facilitators,
        _ => None.pure[IO]
      )
      artifactMismatch = result match {
        case Left(GlobalArtifactMismatch(expected, found)) => Some((expected, found))
        case _                                             => None
      }
    } yield
      expect.eql(Some(artifact), artifactMismatch.map(_._1)) &&
        expect(artifact.shardCheckpoints.isEmpty) &&
        expect.eql(
          Some(NonEmptyList.one(scEvent.value.snapshotBinary)),
          artifact.stateChannelSnapshots.get(scEvent.value.address)
        ) &&
        expect.eql(
          Some(true),
          artifactMismatch.map {
            case (_, found) =>
              found.stateChannelSnapshots.get(scEvent.value.address).isEmpty
          }
        ) &&
        expect.eql(
          Some(true),
          artifactMismatch.map { case (_, found) => found.shardCheckpoints.isEmpty }
        )
  }

  test("gossip signed artifacts") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      gossiped <- Ref.of(List.empty[ForkInfo])
      mockGossip = mkMockGossip(gossiped)

      (signedLastArtifact, _) <- mkSignedArtifacts()

      _ <- SnapshotConsensusFunctions.gossipForkInfo(mockGossip, signedLastArtifact)

      expected <- h
        .hash(signedLastArtifact)
        .map { h =>
          List(ForkInfo(signedLastArtifact.value.ordinal, h))
        }
        .handleError(_ => List.empty)
      actual <- gossiped.get
    } yield expect.eql(expected, actual)
  }

  test("shouldUseDelegatedRewards - verifies reward selection logic based on ordinal and epoch thresholds") { res =>
    implicit val (_, j, h, sp, m) = res

    // Test the reward selection logic based on both conditions
    // This reproduces the logic in GlobalSnapshotConsensusFunctions
    def shouldUseDelegatedRewards(
      currentOrdinal: SnapshotOrdinal,
      currentEpochProgress: EpochProgress,
      v3MigrationOrdinal: SnapshotOrdinal,
      asOfEpoch: EpochProgress
    ): Boolean =
      currentOrdinal.value >= v3MigrationOrdinal.value &&
        currentEpochProgress.value.value >= asOfEpoch.value.value

    // Define test thresholds
    val migrationOrdinal = SnapshotOrdinal.unsafeApply(100L)
    val epochThreshold = EpochProgress(200L)

    // Define test cases - all combinations of before/after thresholds
    val beforeMigration = SnapshotOrdinal.unsafeApply(99L)
    val atMigration = SnapshotOrdinal.unsafeApply(100L)
    val afterMigration = SnapshotOrdinal.unsafeApply(101L)

    val beforeEpochThreshold = EpochProgress(199L)
    val atEpochThreshold = EpochProgress(200L)
    val afterEpochThreshold = EpochProgress(201L)

    // Determine which reward function would be called in each scenario
    val case1 = shouldUseDelegatedRewards(beforeMigration, beforeEpochThreshold, migrationOrdinal, epochThreshold)
    val case2 = shouldUseDelegatedRewards(afterMigration, beforeEpochThreshold, migrationOrdinal, epochThreshold)
    val case3 = shouldUseDelegatedRewards(beforeMigration, afterEpochThreshold, migrationOrdinal, epochThreshold)
    val case4 = shouldUseDelegatedRewards(afterMigration, afterEpochThreshold, migrationOrdinal, epochThreshold)

    // Edge cases at exact threshold values
    val atExactThresholds = shouldUseDelegatedRewards(atMigration, atEpochThreshold, migrationOrdinal, epochThreshold)

    // Check classic rewards selection (inverse of delegated)
    val useClassic1 = (beforeMigration.value < migrationOrdinal.value) ||
      (beforeEpochThreshold.value.value < epochThreshold.value.value)
    val useClassic4 = (afterMigration.value < migrationOrdinal.value) ||
      (afterEpochThreshold.value.value < epochThreshold.value.value)

    IO {
      // Case 1: Before migration, before epoch threshold - should NOT use delegated rewards
      expect(!case1) &&
      // Case 2: After migration, before epoch threshold - should NOT use delegated rewards
      expect(!case2) &&
      // Case 3: Before migration, after epoch threshold - should NOT use delegated rewards
      expect(!case3) &&
      // Case 4: After migration, after epoch threshold - should use delegated rewards
      expect(case4) &&
      // Exactly at thresholds - should use delegated rewards (inclusive thresholds)
      expect(atExactThresholds) &&
      // Verify classic rewards are used in cases 1-3 but not case 4
      expect(useClassic1) &&
      expect(!useClassic4)
    }
  }

//  ignore("rewards selection in GlobalSnapshotConsensusFunctions uses correct ordinal and epoch thresholds") { res =>
//    implicit val (_, ks, j, h, sp) = res
//
//    // Create a custom delegated rewards config provider with specific thresholds
//    val testEpochThreshold = EpochProgress(100L)
//    val testConfigProvider = new DelegatedRewardsConfigProvider {
//      def getConfig(): DelegatedRewardsConfig =
//        DelegatedRewardsConfig(
//          flatInflationRate = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(0, 100),
//          emissionConfig = Map(
//            AppEnvironment.Dev -> EmissionConfigEntry(
//              epochsPerYear = eu.timepit.refined.types.numeric.PosLong(12L),
//              asOfEpoch = testEpochThreshold, // Set specific epoch threshold for testing
//              iTarget = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(1, 100),
//              iInitial = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(2, 100),
//              lambda = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(1, 10),
//              iImpact = io.constellationnetwork.schema.NonNegFraction.unsafeFrom(5, 10),
//              totalSupply = Amount(100_00000000L),
//              dagPrices = SortedMap(
//                EpochProgress(100L) -> io.constellationnetwork.schema.NonNegFraction.unsafeFrom(10, 1)
//              )
//            )
//          ),
//          percentDistribution = Map.empty
//        )
//    }
//
//    // Create two implementations of Rewards to track which is called
//    var classicRewardsCalled = false
//    var delegatedRewardsCalled = false
//
//    val trackingClassicRewards: Rewards[F, GlobalSnapshotStateProof, GlobalIncrementalSnapshot, GlobalSnapshotEvent] =
//      (_, _, _, _, _, _) =>
//        IO {
//          classicRewardsCalled = true
//          SortedSet.empty
//        }
//
//    val trackingDelegatedRewards: DelegatedRewardsDistributor[F] = new DelegatedRewardsDistributor[F] {
//      def calculateTotalRewardsToMint(epochProgress: EpochProgress): F[Amount] = Amount(100L).pure[F]
//
//      def distribute(
//        lastSnapshotContext: GlobalSnapshotContext,
//        trigger: ConsensusTrigger,
//        epochProgress: EpochProgress,
//        facilitators: NonEmptySet[ID.Id],
//        delegatedStakeDiffs: UpdateDelegatedStakeAcceptanceResult,
//        partitionedRecords: PartitionedStakeUpdates
//      ): F[DelegationRewardsResult] = {
//        delegatedRewardsCalled = true
//        DelegationRewardsResult(
//          Map.empty,
//          SortedMap.empty,
//          SortedMap.empty,
//          SortedSet.empty,
//          SortedSet.empty,
//          Amount.empty
//        ).pure[F]
//      }
//    }
//
//    // Create a test-specific GlobalSnapshotConsensusFunctions with specific thresholds
//    def mkTestConsensusFunctions(migrationOrdinal: SnapshotOrdinal): GlobalSnapshotConsensusFunctions[IO] = {
//      implicit val hs = HasherSelector.forSyncAlwaysCurrent(h)
//
//      val spendActionValidator = SpendActionValidator.make[IO]
//
//      val snapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[IO] =
//        GlobalSnapshotAcceptanceManager
//          .make[IO](
//            SnapshotOrdinal.MinValue,
//            bam,
//            asbam,
//            tlbam,
//            scProcessor,
//            updateNodeParametersAcceptanceManager,
//            updateDelegatedStakeAcceptanceManager,
//            updateNodeCollateralAcceptanceManager,
//            spendActionValidator,
//            collateral,
//            EpochProgress(NonNegLong(136080L))
//          )
//
//      val feeCalculator = new SnapshotBinaryFeeCalculator[IO] {
//        override def calculateFee(
//          event: StateChannelEvent,
//          info: GlobalSnapshotContext,
//          ordinal: SnapshotOrdinal
//        ): IO[NonNegLong] =
//          event.value.snapshotBinary.value.fee.value.pure[IO]
//      }
//
//      GlobalSnapshotConsensusFunctions
//        .make[IO](
//          snapshotAcceptanceManager,
//          collateral,
//          trackingClassicRewards,
//          trackingDelegatedRewards,
//          GlobalSnapshotEventCutter.make[IO](20_000_000, feeCalculator),
//          UpdateNodeParametersCutter.make(100),
//          AppEnvironment.Dev,
//          testConfigProvider,
//          migrationOrdinal // Use custom migration ordinal
//        )
//    }
//
//    // Setup test values
//    val migrationOrdinal = SnapshotOrdinal.unsafeApply(100L)
//    val gscf = mkTestConsensusFunctions(migrationOrdinal)
//
//    for {
//      // Create test artifacts for different scenarios
//      keyPair <- KeyPairGenerator.makeKeyPair[IO]
//      facilitators = Set.empty[PeerId]
//
//      // Scenario 1: Before migration ordinal, before epoch threshold
//      genesis1 = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(50L))
//      signedGenesis1 <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis1, keyPair)
//      lastArtifact1 <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis1.value)
//      signedLastArtifact1 <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact1, keyPair)
//      // Ensure we're below the migration ordinal
//      beforeMigrationArtifact = signedLastArtifact1.copy(value = signedLastArtifact1.value.copy(ordinal = SnapshotOrdinal.unsafeApply(98L)))
//
//      // Scenario 2: After migration ordinal, after epoch threshold
//      genesis2 = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(150L))
//      signedGenesis2 <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis2, keyPair)
//      lastArtifact2 <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis2.value)
//      signedLastArtifact2 <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact2, keyPair)
//      // Ensure we're above the migration ordinal
//      afterMigrationArtifact = signedLastArtifact2.copy(value = signedLastArtifact2.value.copy(ordinal = SnapshotOrdinal.unsafeApply(101L)))
//
//      // Create test event
//      scEvent <- mkStateChannelEvent()
//
//      // Reset tracking flags
//      _ = { classicRewardsCalled = false; delegatedRewardsCalled = false }
//
//      // Execute scenario 1 - Should use classic rewards
//      _ <- gscf.createProposalArtifact(
//        SnapshotOrdinal.MinValue,
//        beforeMigrationArtifact,
//        signedGenesis1.value.info,
//        h,
//        trigger.TimeTrigger,
//        Set(scEvent),
//        facilitators,
//        none,
//        _ => None.pure[IO]
//      )
//
//      classicCalledScenario1 = classicRewardsCalled
//      delegatedCalledScenario1 = delegatedRewardsCalled
//
//      // Reset tracking flags
//      _ = { classicRewardsCalled = false; delegatedRewardsCalled = false }
//
//      // Execute scenario 2 - Should use delegated rewards
//      _ <- gscf.createProposalArtifact(
//        SnapshotOrdinal.MinValue,
//        afterMigrationArtifact,
//        signedGenesis2.value.info,
//        h,
//        trigger.TimeTrigger,
//        Set(scEvent),
//        facilitators,
//        none,
//        _ => None.pure[IO]
//      )
//
//      classicCalledScenario2 = classicRewardsCalled
//      delegatedCalledScenario2 = delegatedRewardsCalled
//    } yield {
//      // Verify that the correct rewards function was called in each scenario
//      expect(classicCalledScenario1).description("Classic rewards should be called for before migration") &&
//        expect(!delegatedCalledScenario1).description("Delegated rewards should not be called for before migration") &&
//
//      expect(!classicCalledScenario2).description("Classic rewards should not be called for after migration and epoch") &&
//      expect(delegatedCalledScenario2).description("Delegated rewards should be called for after migration and epoch")
//    }
//  }

  test("createProposalArtifact - deterministic: two independent calls with same inputs produce identical artifacts") { res =>
    implicit val (_, j, h, sp, m) = res

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]

      genesis = GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue)
      signedGenesis <- Signed.forAsyncHasher[IO, GlobalSnapshot](genesis, keyPair)

      lastArtifact <- GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](signedGenesis.value)
      signedLastArtifact <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](lastArtifact, keyPair)

      scEvent <- mkStateChannelEvent()

      facilitators = Set.empty[PeerId]
      context = signedGenesis.value.info.toGlobalSnapshotInfo

      // Build two independent consensus function instances (each with its own MptStore)
      // to ensure no shared mutable state affects the output
      gscf1 <- mkGlobalSnapshotConsensusFunctions()
      gscf2 <- mkGlobalSnapshotConsensusFunctions()

      (artifact1, ctx1, _) <- gscf1.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        context,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )

      (artifact2, ctx2, _) <- gscf2.createProposalArtifact(
        SnapshotOrdinal.MinValue,
        signedLastArtifact,
        context,
        h,
        EventTrigger,
        Set(scEvent),
        facilitators,
        _ => None.pure[IO]
      )

      hash1 <- h.hash(artifact1)
      hash2 <- h.hash(artifact2)
    } yield
      expect.same(hash1, hash2) &&
        expect.same(artifact1.ordinal, artifact2.ordinal) &&
        expect.same(artifact1.stateProof, artifact2.stateProof)
  }

  def mkStateChannelEvent()(implicit S: SecurityProvider[IO], H: Hasher[IO]): IO[StateChannelEvent] = for {
    keyPair <- KeyPairGenerator.makeKeyPair[IO]
    binary = StateChannelSnapshotBinary(Hash.empty, "test".getBytes, SnapshotFee.MinValue)
    signedSC <- forAsyncHasher(binary, keyPair)
  } yield StateChannelEvent(StateChannelOutput(keyPair.getPublic.toAddress, signedSC))

}
