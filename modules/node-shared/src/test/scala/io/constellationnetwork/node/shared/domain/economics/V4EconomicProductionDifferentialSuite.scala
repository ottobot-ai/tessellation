package io.constellationnetwork.node.shared.domain.economics

import java.security.KeyPair
import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshotContext, CurrencySnapshotInfo}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.types.AddressesConfig
import io.constellationnetwork.node.shared.domain.block.processing.{
  BlockAwaitReason => TransferBlockAwaitReason,
  BlockRejectionReason => TransferBlockRejectionReason,
  _
}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.swap.{AllowSpendChainValidator, AllowSpendValidator}
import io.constellationnetwork.node.shared.domain.tokenlock.block.{
  AddressBalanceOutOfRange => TokenLockAddressBalanceOutOfRange,
  InvalidTokenLock => InvalidTokenLockTransaction,
  ParentHashNotEqLastTxHash => TokenLockParentHashMismatch,
  ParentOrdinalBelowLastTxOrdinal => TokenLockParentOrdinalBelowLastTxOrdinal,
  ValidationFailed => TokenLockValidationFailed,
  _
}
import io.constellationnetwork.node.shared.domain.tokenlock.{TokenLockChainValidator, TokenLockValidator}
import io.constellationnetwork.node.shared.domain.transaction.{TransactionChainValidator, TransactionValidator}
import io.constellationnetwork.node.shared.infrastructure.block.processing.{BlockAcceptanceManager, BlockValidator}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.{
  AllowSpendOpsManager,
  BlockAcceptanceOpsManager,
  TokenLockOpsManager
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, AmountUnderflow, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import io.circe.Encoder
import weaver.MutableIOSuite

object V4EconomicProductionDifferentialSuite extends MutableIOSuite {
  import ReferenceBalanceScope.{Dag, Metagraph}
  import ReferenceDecision.{Accepted, Rejected}
  import ReferenceRejection.{AllowSpendEpochOutsideWindow, InsufficientBalance, InvalidAllowSpendApprovers, SelfTransfer}
  import TransferLane.{CurrencyCl1, NativeGl1}
  import V4EconomicProductionProjection._

  final case class Resources(
    currentHasher: Hasher[IO],
    transactionHasher: Hasher[IO],
    securityProvider: SecurityProvider[IO]
  )

  final case class Managers(
    signedValidator: SignedValidator[IO],
    blockAcceptanceManager: io.constellationnetwork.node.shared.domain.block.processing.BlockAcceptanceManager[IO],
    allowSpendAcceptanceManager: AllowSpendBlockAcceptanceManager[IO],
    tokenLockAcceptanceManager: TokenLockBlockAcceptanceManager[IO],
    currencyAcceptanceManager: BlockAcceptanceOpsManager[IO],
    globalAcceptanceManager: BlockAcceptanceCoordinatorManager[IO]
  )

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(kryo: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield Resources(Hasher.forJson[IO], Hasher.forKryo[IO], securityProvider)

  private val domain = ReferenceDomain(Hash("11" * 32), Hash("22" * 32), Hash("33" * 32))
  private val epochWindow = ReferenceAllowSpendEpochWindow(100, 5, 20)
  private val tokenLockEpochRule = ReferenceTokenLockEpochRule(100, 5)
  private val snapshotOrdinal = SnapshotOrdinal.MinValue
  private val parentA = BlockReference(Height(1L), ProofsHash("44" * 32))
  private val parentB = BlockReference(Height(1L), ProofsHash("55" * 32))
  private val blockParents = NonEmptyList.of(parentA, parentB)

  private def balance(value: Long): Balance = Balance(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))
  private def transactionAmount(value: Long): TransactionAmount = TransactionAmount(PosLong.unsafeFrom(value))
  private def transactionFee(value: Long): TransactionFee = TransactionFee(NonNegLong.unsafeFrom(value))
  private def swapAmount(value: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(value))
  private def allowSpendFee(value: Long): AllowSpendFee = AllowSpendFee(NonNegLong.unsafeFrom(value))
  private def tokenLockAmount(value: Long): TokenLockAmount = TokenLockAmount(PosLong.unsafeFrom(value))
  private def tokenLockFee(value: Long): TokenLockFee = TokenLockFee(NonNegLong.unsafeFrom(value))

  private def managers(resources: Resources): Managers = {
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher

    val signedValidator = SignedValidator.make[IO]
    val transactionValidator = TransactionValidator.make[IO](
      AddressesConfig(Set.empty),
      signedValidator,
      resources.transactionHasher
    )
    val blockValidator = BlockValidator.make[IO](
      signedValidator,
      TransactionChainValidator.make[IO](resources.transactionHasher),
      transactionValidator,
      resources.transactionHasher
    )
    val blockAcceptanceManager = BlockAcceptanceManager.make[IO](blockValidator, resources.transactionHasher)
    val allowSpendBlockValidator = AllowSpendBlockValidator.make[IO](
      signedValidator,
      AllowSpendChainValidator.make[IO],
      AllowSpendValidator.make[IO](AddressesConfig(Set.empty), signedValidator)
    )
    val allowSpendAcceptanceManager = AllowSpendBlockAcceptanceManager.make[IO](allowSpendBlockValidator)
    val tokenLockBlockValidator = TokenLockBlockValidator.make[IO](
      signedValidator,
      TokenLockChainValidator.make[IO],
      TokenLockValidator.make[IO](AddressesConfig(Set.empty), signedValidator)
    )
    val tokenLockAcceptanceManager = TokenLockBlockAcceptanceManager.make[IO](tokenLockBlockValidator)
    val currencyAcceptanceManager = BlockAcceptanceOpsManager.make[IO](
      blockAcceptanceManager,
      tokenLockAcceptanceManager,
      allowSpendAcceptanceManager,
      Amount.empty
    )
    val globalAcceptanceManager = BlockAcceptanceCoordinatorManager.make[IO](
      blockAcceptanceManager,
      allowSpendAcceptanceManager,
      tokenLockAcceptanceManager,
      TipUsageManager.make[IO](),
      Amount.empty,
      GlobalStateReader.empty[IO]
    )

    Managers(
      signedValidator,
      blockAcceptanceManager,
      allowSpendAcceptanceManager,
      tokenLockAcceptanceManager,
      currencyAcceptanceManager,
      globalAcceptanceManager
    )
  }

  private def sign[A: Encoder](value: A, keyPair: KeyPair, hasher: Hasher[IO])(
    implicit securityProvider: SecurityProvider[IO]
  ): IO[Signed[A]] = {
    implicit val scopedHasher: Hasher[IO] = hasher
    Signed.forAsyncHasher(value, keyPair)
  }

  private def signWithThree[A: Encoder](value: A, hasher: Hasher[IO])(implicit securityProvider: SecurityProvider[IO]): IO[Signed[A]] =
    for {
      firstKey <- KeyPairGenerator.makeKeyPair[IO]
      secondKey <- KeyPairGenerator.makeKeyPair[IO]
      thirdKey <- KeyPairGenerator.makeKeyPair[IO]
      first <- sign(value, firstKey, hasher)
      second <- sign(value, secondKey, hasher)
      third <- sign(value, thirdKey, hasher)
    } yield first.addProof(second.proofs.head).addProof(third.proofs.head)

  private def signedTransaction(
    source: Address,
    signerKey: KeyPair,
    destination: Address,
    parent: TransactionReference,
    amount: Long,
    fee: Long,
    salt: Long,
    transactionHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[Transaction]] =
    sign(
      Transaction(
        source,
        destination,
        transactionAmount(amount),
        transactionFee(fee),
        parent,
        TransactionSalt(salt)
      ),
      signerKey,
      transactionHasher
    )

  private def signedTransferBlock(
    transaction: Signed[Transaction],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[Block]] =
    signWithThree(Block(blockParents, NonEmptySet.one(transaction)), currentHasher)

  private def signedAllowSpend(
    source: Address,
    signerKey: KeyPair,
    destination: Address,
    currencyId: Option[CurrencyId],
    parent: AllowSpendReference,
    amount: Long,
    fee: Long,
    lastValidEpoch: Long,
    approvers: List[Address],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    sign(
      AllowSpend(
        source,
        destination,
        currencyId,
        swapAmount(amount),
        allowSpendFee(fee),
        parent,
        epoch(lastValidEpoch),
        approvers
      ),
      signerKey,
      currentHasher
    )

  private def signedAllowSpendBlock(
    allowSpend: Signed[AllowSpend],
    round: Long,
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpendBlock]] =
    signWithThree(
      AllowSpendBlock(RoundId(new UUID(0L, round)), NonEmptySet.one(allowSpend)),
      currentHasher
    )

  private def signedTokenLock(
    source: Address,
    signerKey: KeyPair,
    currencyId: Option[CurrencyId],
    parent: TokenLockReference,
    amount: Long,
    fee: Long,
    unlockEpoch: Option[Long],
    replaceTokenLockRef: Option[Hash],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[TokenLock]] =
    sign(
      TokenLock(
        source,
        tokenLockAmount(amount),
        tokenLockFee(fee),
        parent,
        currencyId,
        unlockEpoch.map(epoch),
        replaceTokenLockRef
      ),
      signerKey,
      currentHasher
    )

  private def signedTokenLockBlock(
    tokenLock: Signed[TokenLock],
    round: Long,
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Signed[TokenLockBlock]] =
    signWithThree(
      TokenLockBlock(RoundId(new UUID(1L, round)), NonEmptySet.one(tokenLock)),
      currentHasher
    )

  private def acceptNativeTransfer(
    transaction: Signed[Transaction],
    balances: SortedMap[Address, Balance],
    lastReference: TransactionReference,
    managers: Managers,
    resources: Resources
  )(implicit securityProvider: SecurityProvider[IO], currentHasher: Hasher[IO]) =
    for {
      block <- signedTransferBlock(transaction, resources.currentHasher)
      context = BlockAcceptanceContext.fromStaticData[IO](
        balances,
        Map(transaction.source -> lastReference),
        Map(parentA -> NonNegLong.unsafeFrom(0L), parentB -> NonNegLong.unsafeFrom(0L)),
        Amount.empty,
        TransactionReference.empty
      )
      result <- managers.blockAcceptanceManager.acceptBlock(block, context, snapshotOrdinal)
    } yield result

  private def acceptNativeAllowSpend(
    allowSpend: Signed[AllowSpend],
    balances: SortedMap[Address, Balance],
    lastReference: AllowSpendReference,
    round: Long,
    managers: Managers,
    resources: Resources
  )(implicit securityProvider: SecurityProvider[IO], currentHasher: Hasher[IO]) =
    for {
      block <- signedAllowSpendBlock(allowSpend, round, resources.currentHasher)
      context = AllowSpendBlockAcceptanceContext.fromStaticData[IO](
        balances,
        Map(allowSpend.source -> lastReference),
        Amount.empty,
        AllowSpendReference.empty
      )
      result <- managers.allowSpendAcceptanceManager.acceptBlock(
        block,
        context,
        snapshotOrdinal,
        lastGlobalSnapshotEpochProgress = epoch(100L).some
      )
    } yield result

  private def acceptNativeTokenLock(
    tokenLock: Signed[TokenLock],
    balances: SortedMap[Address, Balance],
    lastReference: TokenLockReference,
    round: Long,
    managers: Managers,
    resources: Resources
  )(
    implicit securityProvider: SecurityProvider[IO],
    currentHasher: Hasher[IO]
  ): IO[(Signed[TokenLockBlock], TokenLockBlockAcceptanceResult)] =
    for {
      block <- signedTokenLockBlock(tokenLock, round, resources.currentHasher)
      context = TokenLockBlockAcceptanceContext.fromStaticData[IO](
        balances,
        Map(tokenLock.source -> lastReference),
        Amount.empty,
        TokenLockReference.empty,
        List.empty,
        epoch(100L)
      )
      result <- managers.tokenLockAcceptanceManager.acceptBlocksIteratively(
        List(block),
        context,
        snapshotOrdinal,
        shouldPerformMetagraphSpecificValidations = true,
        lastGlobalSnapshotEpochProgress = epoch(100L).some
      )
    } yield block -> result

  private def acceptTokenLockBatch(
    lane: TransferLane,
    source: Address,
    blocks: List[Signed[TokenLockBlock]],
    balances: SortedMap[Address, Balance],
    lastReference: TokenLockReference,
    managers: Managers
  )(
    implicit currentHasher: Hasher[IO]
  ): IO[TokenLockBlockAcceptanceResult] =
    lane match {
      case NativeGl1 =>
        val context = TokenLockBlockAcceptanceContext.fromStaticData[IO](
          balances,
          Map(source -> lastReference),
          Amount.empty,
          TokenLockReference.empty,
          List.empty,
          epoch(100L)
        )
        managers.tokenLockAcceptanceManager.acceptBlocksIteratively(
          blocks,
          context,
          snapshotOrdinal,
          shouldPerformMetagraphSpecificValidations = true,
          lastGlobalSnapshotEpochProgress = epoch(100L).some
        )

      case CurrencyCl1(metagraphId) =>
        managers.currencyAcceptanceManager.acceptTokenLockBlocks(
          blocks,
          currencySnapshotContext(
            metagraphId,
            balances,
            lastTokenLockRefs = SortedMap(source -> lastReference)
          ),
          snapshotOrdinal,
          lastReference,
          shouldPerformMetagraphSpecificValidations = true,
          lastSyncGlobalSnapshotEpochProgress = epoch(100L)
        )
    }

  private def currencySnapshotContext(
    metagraphId: Address,
    balances: SortedMap[Address, Balance],
    lastTxRefs: SortedMap[Address, TransactionReference] = SortedMap.empty,
    lastAllowSpendRefs: SortedMap[Address, AllowSpendReference] = SortedMap.empty,
    lastTokenLockRefs: SortedMap[Address, TokenLockReference] = SortedMap.empty,
    activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = SortedMap.empty
  ): CurrencySnapshotContext =
    CurrencySnapshotContext(
      metagraphId,
      CurrencySnapshotInfo(
        lastTxRefs = lastTxRefs,
        balances = balances,
        lastMessages = none,
        lastFeeTxRefs = none,
        lastAllowSpendRefs = lastAllowSpendRefs.some,
        activeAllowSpends = none,
        globalSnapshotSyncView = none,
        lastTokenLockRefs = lastTokenLockRefs.some,
        activeTokenLocks = activeTokenLocks.some
      )
    )

  private def referenceBase(
    scope: ReferenceBalanceScope,
    balances: SortedMap[Address, Balance]
  ): IO[ReferenceState] =
    IO.fromEither(
      ReferenceState
        .initial(
          balances.iterator.map {
            case (address, value) =>
              ReferenceBalanceAccount(scope, address) -> BigInt(value.value.value)
          }.toMap
        )
        .leftMap(error => new AssertionError(error.toString))
    )

  private def observedBalanceMatchesReference(
    observed: SortedMap[Address, ObservedBalanceDelta],
    address: Address,
    expectedBefore: Balance,
    expectedAfter: Balance,
    account: ReferenceBalanceAccount,
    reference: ReferenceExecution
  ): Boolean =
    observed.get(address).exists { delta =>
      delta == ObservedBalanceDelta(expectedBefore, expectedAfter) &&
      reference.finalState.balanceOf(account) == BigInt(delta.after.value.value)
    }

  private def executeTransferReference(
    lane: TransferLane,
    base: ReferenceState,
    bindings: Vector[SourceValidatedTransfer]
  ): IO[ReferenceExecution] =
    IO.fromEither(
      executeTransfers(ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some), base, bindings)
        .leftMap(error => new AssertionError(error.toString))
    )

  private def executeAllowSpendReference(
    lane: TransferLane,
    base: ReferenceState,
    bindings: Vector[SourceValidatedAllowSpend]
  ): IO[ReferenceExecution] =
    IO.fromEither(
      executeAllowSpends(ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some), base, bindings)
        .leftMap(error => new AssertionError(error.toString))
    )

  private def executeTokenLockReference(
    lane: TransferLane,
    base: ReferenceState,
    bindings: Vector[SourceValidatedTokenLock]
  ): IO[ReferenceExecution] =
    IO.fromEither(
      executeTokenLocks(
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        bindings
      ).leftMap(error => new AssertionError(error.toString))
    )

  private def acceptedAt(execution: ReferenceExecution, index: Int): IO[Accepted] =
    IO.fromEither(
      execution.decisions.lift(index) match {
        case Some(value: Accepted) => Right(value)
        case other                 => Left(new AssertionError(s"Expected accepted decision at $index, got $other"))
      }
    )

  private def rejectedAt(execution: ReferenceExecution, index: Int): IO[Rejected] =
    IO.fromEither(
      execution.decisions.lift(index) match {
        case Some(value: Rejected) => Right(value)
        case other                 => Left(new AssertionError(s"Expected rejected decision at $index, got $other"))
      }
    )

  private def nativeActiveAllowSpends(
    spends: SortedSet[Signed[AllowSpend]],
    resources: Resources
  ): IO[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    AllowSpendStateManager
      .make[IO](GlobalStateReader.empty[IO])
      .acceptAllowSpendsWithExpired(
        epoch(100L),
        SortedMap.empty,
        spends.toVector.groupBy(_.source).view.mapValues(values => SortedSet.from(values)).to(SortedMap),
        SortedMap.empty,
        List.empty,
        SortedMap.empty,
        Map.empty
      )
      .flatMap { result =>
        IO.fromEither(
          result.fullState.toList match {
            case (None, nativeBySource) :: Nil => Right(nativeBySource)
            case other => Left(new AssertionError(s"expected exactly the native allow-spend scope, got ${other.iterator.map(_._1).toList}"))
          }
        )
      }
  }

  private def nativeAllowSpendBalances(
    balances: SortedMap[Address, Balance],
    spends: SortedSet[Signed[AllowSpend]],
    resources: Resources
  ): IO[SortedMap[Address, Balance]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val grouped = spends.toVector.groupBy(_.source).view.mapValues(values => SortedSet.from(values)).to(SortedMap)
    AllowSpendStateManager
      .make[IO](GlobalStateReader.empty[IO])
      .updateGlobalBalancesByAllowSpendsWithExpired(epoch(100L), balances, grouped, SortedMap.empty)
      .flatMap(result => IO.fromEither(result.leftMap(error => new AssertionError(error.toString))))
      .map(_._1)
  }

  private def currencyActiveAllowSpends(
    spends: SortedSet[Signed[AllowSpend]],
    resources: Resources
  ): IO[SortedSet[Signed[AllowSpend]]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val grouped = spends.toVector.groupBy(_.source).view.mapValues(values => SortedSet.from(values)).to(SortedMap)
    AllowSpendOpsManager
      .make[IO]
      .acceptCurrencyAllowSpends(epoch(100L), grouped, SortedMap.empty, List.empty)
      .flatMap(result => IO.pure(result.values.flatten.to(SortedSet)))
  }

  private def currencyAllowSpendBalances(
    balances: SortedMap[Address, Balance],
    spends: SortedSet[Signed[AllowSpend]],
    resources: Resources
  ): IO[SortedMap[Address, Balance]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val grouped = spends.toVector.groupBy(_.source).view.mapValues(values => SortedSet.from(values)).to(SortedMap)
    AllowSpendOpsManager
      .make[IO]
      .updateCurrencyBalancesByAllowSpends(epoch(100L), balances, grouped, SortedMap.empty, List.empty)
      .flatMap(result => IO.fromEither(result.leftMap(error => new AssertionError(error.toString))))
  }

  private def groupedTokenLocks(
    tokenLocks: SortedSet[Signed[TokenLock]]
  ): SortedMap[Address, SortedSet[Signed[TokenLock]]] =
    tokenLocks.toVector.groupBy(_.source).view.mapValues(values => SortedSet.from(values)).to(SortedMap)

  private def nativeActiveTokenLocks(
    tokenLocks: SortedSet[Signed[TokenLock]],
    resources: Resources
  ): IO[SortedMap[Address, SortedSet[Signed[TokenLock]]]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    TokenLockStateManager
      .make[IO](GlobalStateReader.empty[IO])
      .acceptTokenLocksWithExpired(
        epoch(100L),
        groupedTokenLocks(tokenLocks),
        SortedMap.empty,
        Map.empty,
        SortedMap.empty
      )
      .flatMap(result => IO.pure(result.fullState))
  }

  private def nativeTokenLockBalances(
    balances: SortedMap[Address, Balance],
    tokenLocks: SortedSet[Signed[TokenLock]],
    resources: Resources
  ): IO[SortedMap[Address, Balance]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    TokenLockStateManager
      .make[IO](GlobalStateReader.empty[IO])
      .updateGlobalBalancesByTokenLocksWithExpired(
        epoch(100L),
        balances,
        groupedTokenLocks(tokenLocks),
        Map.empty,
        SortedMap.empty
      )
      .flatMap(result => IO.fromEither(result.leftMap(error => new AssertionError(error.toString))))
      .map(_._1)
  }

  private def currencyActiveTokenLocks(
    tokenLocks: SortedSet[Signed[TokenLock]],
    resources: Resources
  ): IO[SortedMap[Address, SortedSet[Signed[TokenLock]]]] = {
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    TokenLockOpsManager
      .make[IO]
      .acceptTokenLocks(epoch(100L), groupedTokenLocks(tokenLocks), SortedMap.empty, SortedSet.empty)
      .flatMap(result => IO.pure(result._1))
  }

  private def currencyTokenLockBalances(
    balances: SortedMap[Address, Balance],
    tokenLocks: SortedSet[Signed[TokenLock]]
  ): IO[SortedMap[Address, Balance]] =
    IO.fromEither(
      TokenLockOpsManager
        .make[IO]
        .updateBalancesByTokenLocks(epoch(100L), balances, groupedTokenLocks(tokenLocks), SortedMap.empty, SortedSet.empty)
        .leftMap(error => new AssertionError(error.toString))
    )

  private def activeTokenLocksForLane(
    lane: TransferLane,
    tokenLocks: SortedSet[Signed[TokenLock]],
    resources: Resources
  ): IO[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
    lane match {
      case NativeGl1      => nativeActiveTokenLocks(tokenLocks, resources)
      case _: CurrencyCl1 => currencyActiveTokenLocks(tokenLocks, resources)
    }

  private def tokenLockBalancesForLane(
    lane: TransferLane,
    balances: SortedMap[Address, Balance],
    tokenLocks: SortedSet[Signed[TokenLock]],
    resources: Resources
  ): IO[SortedMap[Address, Balance]] =
    lane match {
      case NativeGl1      => nativeTokenLockBalances(balances, tokenLocks, resources)
      case _: CurrencyCl1 => currencyTokenLockBalances(balances, tokenLocks)
    }

  private def projectTokenLockBatchOrRaise(
    lane: TransferLane,
    correspondence: TokenLockReferenceCorrespondence,
    items: Vector[TokenLockBatchItem],
    balancesBefore: SortedMap[Address, Balance],
    production: TokenLockBlockAcceptanceResult,
    activeBefore: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    referenceBase: ReferenceState,
    managers: Managers,
    resources: Resources
  )(implicit securityProvider: SecurityProvider[IO]): IO[TokenLockBatchProjection] =
    projectTokenLockBatch(
      domain,
      lane,
      correspondence,
      items,
      balancesBefore,
      production,
      activeBefore,
      activeAfter,
      ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
      referenceBase,
      managers.signedValidator,
      resources.currentHasher
    ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))

  test("native zero-fee transfer compares exact production and reference semantic state and successor identity") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TransferReferenceCorrespondence.nativeGenesis
      transaction <- signedTransaction(source, sourceKey, destination, correspondence.production, 60L, 0L, 1L, resources.transactionHasher)
      production <- acceptNativeTransfer(transaction, initialBalances, correspondence.production, managerBundle, resources)
      productionAccepted <- IO.fromEither(production.leftMap(error => new AssertionError(error.toString)))
      (update, _) = productionAccepted
      binding <- bindTransfer(
        transaction,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      ).flatMap(result => IO.fromEither(result.leftMap(error => new AssertionError(error.toString))))
      observation <- IO.fromEither(
        observeAcceptedTransfer(binding, initialBalances, production).leftMap(error => new AssertionError(error.toString))
      )
      base <- referenceBase(Dag, initialBalances)
      reference <- executeTransferReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
      nextCorrespondence <- IO.fromEither(
        correspondence.advance(observation).leftMap(error => new AssertionError(error.toString))
      )
    } yield
      expect.all(
        binding.operationId == SupportedReferenceOperationId.NativeTransfer,
        binding.signerFromProof == source,
        binding.allProofOwners == Vector(source),
        binding.domainBinding == UnboundLegacyPayload,
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 40,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, destination)) == 60,
        reference.finalState.lastTxRefOf(ReferenceChainAccount(lane, source)) == binding.structuralSuccessor,
        observation.semanticDelta.referenceAfter == binding.productionSuccessor,
        observation.semanticDelta.balances.keySet == Set(source, destination),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Dag, source),
          reference
        ),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          destination,
          Balance.empty,
          balance(60L),
          ReferenceBalanceAccount(Dag, destination),
          reference
        ),
        nextCorrespondence.production == update.lastTxRefs.get(source).getOrElse(TransactionReference.empty),
        nextCorrespondence.structural == binding.structuralSuccessor,
        reference.rejected.isEmpty
      )
  }

  test("currency zero-fee transfer uses the real ML0 wrapper and exact successor reference") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      lane = CurrencyCl1(metagraphId)
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      initialReference <- TransactionReference.emptyCurrency[IO](metagraphId)
      correspondence <- TransferReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      transaction <- signedTransaction(source, sourceKey, destination, initialReference, 60L, 0L, 2L, resources.transactionHasher)
      block <- signedTransferBlock(transaction, resources.currentHasher)
      result <- managerBundle.currencyAcceptanceManager.acceptBlocks(
        List(block),
        currencySnapshotContext(metagraphId, initialBalances),
        snapshotOrdinal,
        SortedSet(
          ActiveTip(parentA, NonNegLong.unsafeFrom(0L), snapshotOrdinal),
          ActiveTip(parentB, NonNegLong.unsafeFrom(0L), snapshotOrdinal)
        ),
        SortedSet.empty,
        initialReference,
        shouldPerformMetagraphSpecificValidations = true
      )
      binding <- bindTransfer(
        transaction,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      unrelatedTransaction <- signedTransaction(
        source,
        sourceKey,
        destination,
        initialReference,
        1L,
        0L,
        200L,
        resources.transactionHasher
      )
      unrelatedBlock <- signedTransferBlock(unrelatedTransaction, resources.currentHasher)
      unrelatedAcceptedResult = result.copy(
        accepted = result.accepted.map { case (_, usage) => unrelatedBlock -> usage }
      )
      unrelatedObservation = observeBatchAcceptedTransfer(
        binding,
        unrelatedBlock,
        initialBalances,
        unrelatedAcceptedResult
      )
      unrelatedAdvance = unrelatedObservation.flatMap(correspondence.advance)
      observation <- IO.fromEither(
        observeBatchAcceptedTransfer(binding, block, initialBalances, result).leftMap(error => new AssertionError(error.toString))
      )
      base <- referenceBase(Metagraph(metagraphId), initialBalances)
      reference <- executeTransferReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
    } yield
      expect.all(
        result.accepted.map(_._1) == List(block),
        result.notAccepted.isEmpty,
        binding.operationId == SupportedReferenceOperationId.CurrencyTransfer,
        result.contextUpdate.lastTxRefs.get(source).contains(binding.productionSuccessor),
        unrelatedObservation == Left(TransferBatchPayloadMismatch(transaction, Vector(unrelatedTransaction))),
        unrelatedAdvance == Left(TransferBatchPayloadMismatch(transaction, Vector(unrelatedTransaction))),
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Metagraph(metagraphId), source)) == 40,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Metagraph(metagraphId), destination)) == 60,
        reference.finalState.lastTxRefOf(ReferenceChainAccount(lane, source)) == binding.structuralSuccessor,
        observation.semanticDelta.referenceAfter == binding.productionSuccessor,
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Metagraph(metagraphId), source),
          reference
        ),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          destination,
          Balance.empty,
          balance(60L),
          ReferenceBalanceAccount(Metagraph(metagraphId), destination),
          reference
        )
      )
  }

  test("native zero-fee allow-spend compares exact admission, reservation, and successor state") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = AllowSpendReferenceCorrespondence.nativeGenesis
      allowSpend <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        correspondence.production,
        60L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      production <- acceptNativeAllowSpend(
        allowSpend,
        initialBalances,
        correspondence.production,
        1L,
        managerBundle,
        resources
      )
      update <- IO.fromEither(production.leftMap(error => new AssertionError(error.toString)))
      binding <- bindAllowSpend(
        allowSpend,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      activeBySource <- nativeActiveAllowSpends(SortedSet(allowSpend), resources)
      active = activeBySource.valuesIterator.flatten.to(SortedSet)
      finalBalances <- nativeAllowSpendBalances(initialBalances, SortedSet(allowSpend), resources)
      observation <- IO.fromEither(
        observeAcceptedAllowSpend(binding, initialBalances, production, SortedSet.empty, active)
          .leftMap(error => new AssertionError(error.toString))
      )
      base <- referenceBase(Dag, initialBalances)
      reference <- executeAllowSpendReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
    } yield
      expect.all(
        binding.operationId == SupportedReferenceOperationId.AllowSpendCreation,
        binding.signerFromProof == source,
        binding.allProofOwners == Vector(source),
        update.balances == finalBalances,
        activeBySource == SortedMap(source -> SortedSet(allowSpend)),
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 40,
        reference.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(lane, source)) == binding.structuralSuccessor,
        reference.finalState.allowSpendReservationOf(binding.identity).contains(binding.reservation),
        observation.semanticDelta.activeAdded == SortedSet(allowSpend),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Dag, source),
          reference
        ),
        reference.rejected.isEmpty
      )
  }

  test("currency zero-fee allow-spend uses the real ML0 wrapper and exact active record") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      lane = CurrencyCl1(metagraphId)
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      initialReference <- AllowSpendReference.emptyCurrency[IO](metagraphId)
      correspondence <- AllowSpendReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      allowSpend <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        CurrencyId(metagraphId).some,
        initialReference,
        60L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      block <- signedAllowSpendBlock(allowSpend, 2L, resources.currentHasher)
      result <- managerBundle.currencyAcceptanceManager.acceptAllowSpendBlocks(
        List(block),
        currencySnapshotContext(metagraphId, initialBalances),
        snapshotOrdinal,
        initialReference,
        shouldPerformMetagraphSpecificValidations = true,
        lastSyncGlobalSnapshotEpochProgress = epoch(100L)
      )
      binding <- bindAllowSpend(
        allowSpend,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      unrelatedAllowSpend <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        CurrencyId(metagraphId).some,
        initialReference,
        1L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      unrelatedBlock <- signedAllowSpendBlock(unrelatedAllowSpend, 200L, resources.currentHasher)
      unrelatedAcceptedResult = result.copy(accepted = List(unrelatedBlock))
      unrelatedObservation = observeBatchAcceptedAllowSpend(
        binding,
        unrelatedBlock,
        initialBalances,
        unrelatedAcceptedResult,
        SortedSet.empty,
        SortedSet(allowSpend)
      )
      unrelatedAdvance = unrelatedObservation.flatMap(correspondence.advance)
      active <- currencyActiveAllowSpends(SortedSet(allowSpend), resources)
      finalBalances <- currencyAllowSpendBalances(initialBalances, SortedSet(allowSpend), resources)
      observation <- IO.fromEither(
        observeBatchAcceptedAllowSpend(binding, block, initialBalances, result, SortedSet.empty, active)
          .leftMap(error => new AssertionError(error.toString))
      )
      base <- referenceBase(Metagraph(metagraphId), initialBalances)
      reference <- executeAllowSpendReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
    } yield
      expect.all(
        result.accepted == List(block),
        result.notAccepted.isEmpty,
        result.contextUpdate.balances == finalBalances,
        result.contextUpdate.lastTxRefs.get(source).contains(binding.productionSuccessor),
        unrelatedObservation == Left(AllowSpendBatchPayloadMismatch(allowSpend, Vector(unrelatedAllowSpend))),
        unrelatedAdvance == Left(AllowSpendBatchPayloadMismatch(allowSpend, Vector(unrelatedAllowSpend))),
        active == SortedSet(allowSpend),
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Metagraph(metagraphId), source)) == 40,
        reference.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(lane, source)) == binding.structuralSuccessor,
        reference.finalState.allowSpendReservationOf(binding.identity).contains(binding.reservation),
        observation.semanticDelta.referenceAfter == binding.productionSuccessor,
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Metagraph(metagraphId), source),
          reference
        )
      )
  }

  test("claimed structural genesis is created only from the exact native or currency canonical reference") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      currencyLane = CurrencyCl1(metagraphId)
      currencyTransferGenesis <- {
        implicit val currentHasher: Hasher[IO] = resources.currentHasher
        TransactionReference.emptyCurrency[IO](metagraphId)
      }
      currencyAllowSpendGenesis <- {
        implicit val currentHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.emptyCurrency[IO](metagraphId)
      }
      forgedTransfer = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(0L)), Hash("88" * 32))
      forgedAllowSpend = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(0L)), Hash("99" * 32))
      nativeTransfer <- TransferReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        forgedTransfer,
        resources.currentHasher
      )
      currencyTransfer <- TransferReferenceCorrespondence.fromClaimedGenesis(
        currencyLane,
        forgedTransfer,
        resources.currentHasher
      )
      nativeAllowSpend <- AllowSpendReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        forgedAllowSpend,
        resources.currentHasher
      )
      currencyAllowSpend <- AllowSpendReferenceCorrespondence.fromClaimedGenesis(
        currencyLane,
        forgedAllowSpend,
        resources.currentHasher
      )
      canonicalNativeTransfer <- TransferReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        TransactionReference.empty,
        resources.currentHasher
      )
      canonicalCurrencyTransfer <- TransferReferenceCorrespondence.fromClaimedGenesis(
        currencyLane,
        currencyTransferGenesis,
        resources.currentHasher
      )
      canonicalNativeAllowSpend <- AllowSpendReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        AllowSpendReference.empty,
        resources.currentHasher
      )
      canonicalCurrencyAllowSpend <- AllowSpendReferenceCorrespondence.fromClaimedGenesis(
        currencyLane,
        currencyAllowSpendGenesis,
        resources.currentHasher
      )
    } yield
      expect.all(
        nativeTransfer == Left(InvalidTransferCanonicalGenesis(NativeGl1, TransactionReference.empty, forgedTransfer)),
        currencyTransfer == Left(InvalidTransferCanonicalGenesis(currencyLane, currencyTransferGenesis, forgedTransfer)),
        nativeAllowSpend == Left(InvalidAllowSpendCanonicalGenesis(NativeGl1, AllowSpendReference.empty, forgedAllowSpend)),
        currencyAllowSpend == Left(InvalidAllowSpendCanonicalGenesis(currencyLane, currencyAllowSpendGenesis, forgedAllowSpend)),
        canonicalNativeTransfer.exists(value =>
          value.production == TransactionReference.empty && value.structural == StructuralReference.genesis
        ),
        canonicalCurrencyTransfer.exists(value =>
          value.production == currencyTransferGenesis && value.structural == StructuralReference.genesis
        ),
        canonicalNativeAllowSpend.exists(value =>
          value.production == AllowSpendReference.empty && value.structural == StructuralAllowSpendReference.genesis
        ),
        canonicalCurrencyAllowSpend.exists(value =>
          value.production == currencyAllowSpendGenesis && value.structural == StructuralAllowSpendReference.genesis
        )
      )
  }

  test("wrong-owner production proofs reject before any transfer or allow-spend reference input can be minted") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    val managerBundle = managers(resources)

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      attackerKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      txCorrespondence = TransferReferenceCorrespondence.nativeGenesis
      forgedTransfer <- signedTransaction(
        source,
        attackerKey,
        destination,
        txCorrespondence.production,
        10L,
        0L,
        3L,
        resources.transactionHasher
      )
      transferProduction <- acceptNativeTransfer(
        forgedTransfer,
        initialBalances,
        txCorrespondence.production,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      transferBinding <- bindTransfer(
        forgedTransfer,
        domain,
        NativeGl1,
        txCorrespondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      )
      forgedTransferReference <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](forgedTransfer)
      }
      allowCorrespondence = AllowSpendReferenceCorrespondence.nativeGenesis
      forgedAllow <- signedAllowSpend(
        source,
        attackerKey,
        destination,
        none,
        allowCorrespondence.production,
        10L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      allowProduction <- acceptNativeAllowSpend(
        forgedAllow,
        initialBalances,
        allowCorrespondence.production,
        3L,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      allowBinding <- bindAllowSpend(
        forgedAllow,
        domain,
        NativeGl1,
        allowCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      forgedAllowReference <- {
        implicit val allowSpendHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](forgedAllow)
      }
    } yield
      expect.all(
        transferProduction == Left(
          io.constellationnetwork.node.shared.domain.block.processing.ValidationFailed(
            NonEmptyList.one(
              io.constellationnetwork.node.shared.domain.block.processing.InvalidTransaction(
                forgedTransferReference,
                TransactionValidator.NotSignedBySourceAddressOwner
              )
            )
          )
        ),
        transferBinding == Left(ProductionSourceOwnerValidationRejected("ECO-TRANSFER-NATIVE")),
        allowProduction == Left(
          io.constellationnetwork.node.shared.domain.swap.block.ValidationFailed(
            NonEmptyList.one(
              InvalidAllowSpend(
                forgedAllowReference,
                AllowSpendValidator.NotSignedBySourceAddressOwner
              )
            )
          )
        ),
        allowBinding == Left(ProductionSourceOwnerValidationRejected("ECO-ALLOW-CREATE"))
      )
  }

  test("same-ordinal wrong-parent hashes reject in production and at exact correspondence binding") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    val managerBundle = managers(resources)

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      txCorrespondence = TransferReferenceCorrespondence.nativeGenesis
      wrongTxParent = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(0L)), Hash("66" * 32))
      transfer <- signedTransaction(source, sourceKey, destination, wrongTxParent, 10L, 0L, 4L, resources.transactionHasher)
      transferProduction <- acceptNativeTransfer(
        transfer,
        initialBalances,
        txCorrespondence.production,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      transferBinding <- bindTransfer(
        transfer,
        domain,
        NativeGl1,
        txCorrespondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      )
      transferReference <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](transfer)
      }
      allowCorrespondence = AllowSpendReferenceCorrespondence.nativeGenesis
      wrongAllowParent = AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(0L)), Hash("77" * 32))
      allowSpend <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        wrongAllowParent,
        10L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      allowProduction <- acceptNativeAllowSpend(
        allowSpend,
        initialBalances,
        allowCorrespondence.production,
        4L,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      allowBinding <- bindAllowSpend(
        allowSpend,
        domain,
        NativeGl1,
        allowCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      allowSpendReference <- {
        implicit val allowSpendHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](allowSpend)
      }
    } yield
      expect.all(
        transferProduction == Left(
          RejectedTransaction(
            transferReference,
            io.constellationnetwork.node.shared.domain.block.processing.ParentHashNotEqLastTxHash(
              wrongTxParent.hash,
              txCorrespondence.production.hash
            )
          )
        ),
        transferBinding == Left(TransferParentBindingMismatch(TransactionReference.empty, wrongTxParent)),
        allowProduction == Left(
          RejectedAllowSpend(
            allowSpendReference,
            io.constellationnetwork.node.shared.domain.swap.block.ParentHashNotEqLastTxHash(
              wrongAllowParent.hash,
              allowCorrespondence.production.hash
            )
          )
        ),
        allowBinding == Left(AllowSpendParentBindingMismatch(AllowSpendReference.empty, wrongAllowParent))
      )
  }

  test("rejected validly signed operations cannot advance correspondence; insufficient balance divergence stays exact") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    val managerBundle = managers(resources)

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      otherApproverKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(10L))
      txCorrespondence = TransferReferenceCorrespondence.nativeGenesis
      insufficientTransfer <- signedTransaction(
        source,
        sourceKey,
        destination,
        txCorrespondence.production,
        11L,
        0L,
        5L,
        resources.transactionHasher
      )
      insufficientTransferProduction <- acceptNativeTransfer(
        insufficientTransfer,
        initialBalances,
        txCorrespondence.production,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      insufficientTransferBinding <- bindTransfer(
        insufficientTransfer,
        domain,
        NativeGl1,
        txCorrespondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      insufficientTransferId <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](insufficientTransfer)
      }
      insufficientTransferObservation =
        observeAcceptedTransfer(insufficientTransferBinding, initialBalances, insufficientTransferProduction)
      transferBase <- referenceBase(Dag, initialBalances)
      insufficientTransferReference <- executeTransferReference(NativeGl1, transferBase, Vector(insufficientTransferBinding))
      insufficientTransferRejected <- rejectedAt(insufficientTransferReference, 0)
      selfTransfer <- signedTransaction(
        source,
        sourceKey,
        source,
        txCorrespondence.production,
        1L,
        0L,
        6L,
        resources.transactionHasher
      )
      selfTransferProduction <- acceptNativeTransfer(
        selfTransfer,
        initialBalances,
        txCorrespondence.production,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      selfTransferBinding <- bindTransfer(
        selfTransfer,
        domain,
        NativeGl1,
        txCorrespondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      selfTransferId <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](selfTransfer)
      }
      selfTransferReference <- executeTransferReference(NativeGl1, transferBase, Vector(selfTransferBinding))
      selfTransferRejected <- rejectedAt(selfTransferReference, 0)
      allowCorrespondence = AllowSpendReferenceCorrespondence.nativeGenesis
      invalidApproverAllow <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        allowCorrespondence.production,
        1L,
        0L,
        110L,
        List(otherApproverKey.getPublic.toAddress),
        resources.currentHasher
      )
      invalidApproverProduction <- acceptNativeAllowSpend(
        invalidApproverAllow,
        initialBalances,
        allowCorrespondence.production,
        5L,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      invalidApproverBinding <- bindAllowSpend(
        invalidApproverAllow,
        domain,
        NativeGl1,
        allowCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      invalidApproverId <- {
        implicit val allowSpendHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](invalidApproverAllow)
      }
      allowBase <- referenceBase(Dag, initialBalances)
      invalidApproverReference <- executeAllowSpendReference(NativeGl1, allowBase, Vector(invalidApproverBinding))
      invalidApproverRejected <- rejectedAt(invalidApproverReference, 0)
      insufficientAllow <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        allowCorrespondence.production,
        11L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      insufficientAllowProduction <- acceptNativeAllowSpend(
        insufficientAllow,
        initialBalances,
        allowCorrespondence.production,
        6L,
        managerBundle,
        resources
      )(securityProvider, resources.currentHasher)
      insufficientAllowBinding <- bindAllowSpend(
        insufficientAllow,
        domain,
        NativeGl1,
        allowCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      insufficientAllowId <- {
        implicit val allowSpendHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](insufficientAllow)
      }
      insufficientAllowObservation = observeAcceptedAllowSpend(
        insufficientAllowBinding,
        initialBalances,
        insufficientAllowProduction,
        SortedSet.empty,
        SortedSet.empty
      )
      insufficientAllowReference <- executeAllowSpendReference(NativeGl1, allowBase, Vector(insufficientAllowBinding))
      insufficientAllowRejected <- rejectedAt(insufficientAllowReference, 0)
    } yield
      expect.all(
        insufficientTransferProduction == Left(
          io.constellationnetwork.node.shared.domain.block.processing.AddressBalanceOutOfRange(source, AmountUnderflow)
        ),
        insufficientTransferObservation == Left(
          ProductionTransferNotAccepted(
            insufficientTransferId,
            io.constellationnetwork.node.shared.domain.block.processing.AddressBalanceOutOfRange(source, AmountUnderflow)
          )
        ),
        insufficientTransferRejected.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 11, 10),
        insufficientTransferReference.finalState == transferBase,
        txCorrespondence.production == TransactionReference.empty,
        txCorrespondence.structural == StructuralReference.genesis,
        selfTransferProduction == Left(
          io.constellationnetwork.node.shared.domain.block.processing.ValidationFailed(
            NonEmptyList.one(
              io.constellationnetwork.node.shared.domain.block.processing.InvalidTransaction(
                selfTransferId,
                TransactionValidator.SameSourceAndDestinationAddress(source)
              )
            )
          )
        ),
        selfTransferRejected.reason == SelfTransfer(source),
        selfTransferReference.finalState == transferBase,
        invalidApproverProduction == Left(
          io.constellationnetwork.node.shared.domain.swap.block.ValidationFailed(
            NonEmptyList.one(
              InvalidAllowSpend(
                invalidApproverId,
                AllowSpendValidator.InvalidApprover(List(otherApproverKey.getPublic.toAddress), destination)
              )
            )
          )
        ),
        invalidApproverRejected.reason == InvalidAllowSpendApprovers(destination, Vector(otherApproverKey.getPublic.toAddress)),
        invalidApproverReference.finalState == allowBase,
        insufficientAllowProduction == Left(
          io.constellationnetwork.node.shared.domain.swap.block.AddressBalanceOutOfRange(source, AmountUnderflow)
        ),
        insufficientAllowObservation == Left(
          ProductionAllowSpendNotAccepted(
            insufficientAllowId,
            io.constellationnetwork.node.shared.domain.swap.block.AddressBalanceOutOfRange(source, AmountUnderflow)
          )
        ),
        insufficientAllowRejected.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 11, 10),
        insufficientAllowReference.finalState == allowBase,
        allowCorrespondence.production == AllowSpendReference.empty,
        allowCorrespondence.structural == StructuralAllowSpendReference.genesis
      )
  }

  test("native and currency allow-spend lane mismatches reject before semantic input creation") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      nativeCorrespondence = AllowSpendReferenceCorrespondence.nativeGenesis
      currencyInNative <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        CurrencyId(metagraphId).some,
        nativeCorrespondence.production,
        1L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      nativeBlock <- signedAllowSpendBlock(currencyInNative, 7L, resources.currentHasher)
      nativeProduction <- managerBundle.globalAcceptanceManager.acceptAllowSpendBlocks(
        List(nativeBlock),
        GlobalSnapshotInfo.empty,
        snapshotOrdinal,
        epoch(100L)
      )
      nativeBinding <- bindAllowSpend(
        currencyInNative,
        domain,
        NativeGl1,
        nativeCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      currencyInitial <- AllowSpendReference.emptyCurrency[IO](metagraphId)
      currencyCorrespondence <- AllowSpendReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      nativeInCurrency <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        currencyInitial,
        1L,
        0L,
        110L,
        List(destination),
        resources.currentHasher
      )
      currencyBlock <- signedAllowSpendBlock(nativeInCurrency, 8L, resources.currentHasher)
      currencyProduction <- managerBundle.currencyAcceptanceManager.acceptAllowSpendBlocks(
        List(currencyBlock),
        currencySnapshotContext(metagraphId, SortedMap(source -> balance(10L))),
        snapshotOrdinal,
        currencyInitial,
        shouldPerformMetagraphSpecificValidations = true,
        lastSyncGlobalSnapshotEpochProgress = epoch(100L)
      )
      currencyBinding <- bindAllowSpend(
        nativeInCurrency,
        domain,
        CurrencyCl1(metagraphId),
        currencyCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
    } yield
      expect.all(
        nativeProduction.accepted.isEmpty,
        nativeProduction.contextUpdate == AllowSpendBlockAcceptanceContextUpdate.empty,
        nativeProduction.notAccepted == List(nativeBlock -> InvalidGlobalAllowSpendLane),
        nativeBinding == Left(PayloadLaneMismatch(NativeGl1, CurrencyId(metagraphId).some)),
        currencyProduction.accepted.isEmpty,
        currencyProduction.contextUpdate == AllowSpendBlockAcceptanceContextUpdate.empty,
        currencyProduction.notAccepted == List(currencyBlock -> InvalidMetagraphAllowSpendLane(CurrencyId(metagraphId))),
        currencyBinding == Left(PayloadLaneMismatch(CurrencyCl1(metagraphId), none))
      )
  }

  test("nonzero fees fail at the helper boundary for all four lane-operation combinations") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      currencyTxInitial <- TransactionReference.emptyCurrency[IO](metagraphId)
      currencyTxCorrespondence <- TransferReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      nativeTx <- signedTransaction(source, sourceKey, destination, TransactionReference.empty, 1L, 1L, 7L, resources.transactionHasher)
      currencyTx <- signedTransaction(source, sourceKey, destination, currencyTxInitial, 1L, 1L, 8L, resources.transactionHasher)
      nativeTransferResult <- bindTransfer(
        nativeTx,
        domain,
        NativeGl1,
        TransferReferenceCorrespondence.nativeGenesis,
        managerBundle.signedValidator,
        resources.transactionHasher
      )
      currencyTransferResult <- bindTransfer(
        currencyTx,
        domain,
        CurrencyCl1(metagraphId),
        currencyTxCorrespondence,
        managerBundle.signedValidator,
        resources.transactionHasher
      )
      currencyAllowInitial <- AllowSpendReference.emptyCurrency[IO](metagraphId)
      currencyAllowCorrespondence <- AllowSpendReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      nativeAllow <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        AllowSpendReference.empty,
        1L,
        1L,
        110L,
        List(destination),
        resources.currentHasher
      )
      currencyAllow <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        CurrencyId(metagraphId).some,
        currencyAllowInitial,
        1L,
        1L,
        110L,
        List(destination),
        resources.currentHasher
      )
      nativeAllowResult <- bindAllowSpend(
        nativeAllow,
        domain,
        NativeGl1,
        AllowSpendReferenceCorrespondence.nativeGenesis,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      currencyAllowResult <- bindAllowSpend(
        currencyAllow,
        domain,
        CurrencyCl1(metagraphId),
        currencyAllowCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
    } yield
      expect.all(
        nativeTransferResult == Left(NonZeroFeeOutOfScope("ECO-TRANSFER-NATIVE", 1)),
        currencyTransferResult == Left(NonZeroFeeOutOfScope("ECO-TRANSFER-CURRENCY", 1)),
        nativeAllowResult == Left(NonZeroFeeOutOfScope("ECO-ALLOW-CREATE", 1)),
        currencyAllowResult == Left(NonZeroFeeOutOfScope("ECO-ALLOW-CREATE", 1))
      )
  }

  test("real transfer batch preserves accepted, rejected, dependent-valid, and awaiting outcomes with exact aggregate state") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      firstDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      thirdDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      awaitingDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      firstDestination = firstDestinationKey.getPublic.toAddress
      thirdDestination = thirdDestinationKey.getPublic.toAddress
      awaitingDestination = awaitingDestinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      genesis = TransferReferenceCorrespondence.nativeGenesis
      first <- signedTransaction(source, sourceKey, firstDestination, genesis.production, 30L, 0L, 9L, resources.transactionHasher)
      firstSuccessor <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](first)
      }
      rejected <- signedTransaction(
        source,
        sourceKey,
        source,
        firstSuccessor,
        1L,
        0L,
        10L,
        resources.transactionHasher
      )
      third <- signedTransaction(
        source,
        sourceKey,
        thirdDestination,
        firstSuccessor,
        20L,
        0L,
        11L,
        resources.transactionHasher
      )
      thirdSuccessor <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](third)
      }
      awaiting <- signedTransaction(
        source,
        sourceKey,
        awaitingDestination,
        thirdSuccessor,
        60L,
        0L,
        12L,
        resources.transactionHasher
      )
      firstBlock <- signedTransferBlock(first, resources.currentHasher)
      rejectedBlock <- signedTransferBlock(rejected, resources.currentHasher)
      thirdBlock <- signedTransferBlock(third, resources.currentHasher)
      awaitingBlock <- signedTransferBlock(awaiting, resources.currentHasher)
      context = BlockAcceptanceContext.fromStaticData[IO](
        initialBalances,
        Map(source -> genesis.production),
        Map(parentA -> NonNegLong.unsafeFrom(0L), parentB -> NonNegLong.unsafeFrom(0L)),
        Amount.empty,
        TransactionReference.empty
      )
      production <- {
        implicit val acceptanceHasher: Hasher[IO] = resources.currentHasher
        managerBundle.blockAcceptanceManager.acceptBlocksIteratively(
          List(firstBlock, rejectedBlock, thirdBlock, awaitingBlock),
          context,
          snapshotOrdinal
        )
      }
      base <- referenceBase(Dag, initialBalances)
      projection <- projectTransferBatch(
        domain,
        lane,
        genesis,
        Vector(
          TransferBatchItem(first, firstBlock),
          TransferBatchItem(rejected, rejectedBlock),
          TransferBatchItem(third, thirdBlock),
          TransferBatchItem(awaiting, awaitingBlock)
        ),
        initialBalances,
        production,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some),
        base,
        managerBundle.signedValidator,
        resources.transactionHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      mismatchedPayloadProjection <- projectTransferBatch(
        domain,
        lane,
        genesis,
        Vector(TransferBatchItem(rejected, firstBlock)),
        initialBalances,
        production,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some),
        base,
        managerBundle.signedValidator,
        resources.transactionHasher
      )
      reference = projection.referenceExecution
      rejectedDecision <- rejectedAt(reference, 1)
      awaitingDecision <- rejectedAt(reference, 3)
      rejectedReference <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](rejected)
      }
      awaitingReference <- {
        implicit val transactionHasher: Hasher[IO] = resources.transactionHasher
        TransactionReference.of[IO](awaiting)
      }
      productionReason <- IO.fromOption(
        production.notAccepted.collectFirst { case (`rejectedBlock`, reason) => reason }
      )(new AssertionError("missing rejected transfer block decision"))
      productionRejection <- productionReason match {
        case reason: TransferBlockRejectionReason => IO.pure(reason)
        case other                                => IO.raiseError(new AssertionError(s"expected transfer rejection, got $other"))
      }
      productionAwaitReason <- IO.fromOption(
        production.notAccepted.collectFirst { case (`awaitingBlock`, reason) => reason }
      )(new AssertionError("missing awaiting transfer block decision"))
      productionAwait <- productionAwaitReason match {
        case reason: TransferBlockAwaitReason => IO.pure(reason)
        case other                            => IO.raiseError(new AssertionError(s"expected transfer await reason, got $other"))
      }
      acceptedBindings = projection.bindings.zip(projection.dispositions).collect {
        case (binding, TransferBatchAccepted) => binding
      }
    } yield
      expect.all(
        production.accepted.map(_._1) == List(thirdBlock, firstBlock),
        production.notAccepted.map(_._1) == List(awaitingBlock, rejectedBlock),
        projection.dispositions == Vector(
          TransferBatchAccepted,
          TransferBatchRejected(rejectedReference, productionRejection),
          TransferBatchAccepted,
          TransferBatchAwaiting(awaitingReference, productionAwait)
        ),
        mismatchedPayloadProjection == Left(TransferBatchPayloadMismatch(rejected, Vector(first))),
        productionReason == io.constellationnetwork.node.shared.domain.block.processing.ValidationFailed(
          NonEmptyList.one(
            io.constellationnetwork.node.shared.domain.block.processing.InvalidTransaction(
              rejectedReference,
              TransactionValidator.SameSourceAndDestinationAddress(source)
            )
          )
        ),
        rejectedDecision.reason == SelfTransfer(source),
        productionAwaitReason == io.constellationnetwork.node.shared.domain.block.processing.AddressBalanceOutOfRange(
          source,
          AmountUnderflow
        ),
        awaitingDecision.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 60, 50),
        reference.acceptedIds == acceptedBindings.map(_.identity),
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 50,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, firstDestination)) == 30,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, thirdDestination)) == 20,
        projection.semanticDelta.get(source).exists(_.after == balance(50L)),
        projection.semanticDelta.get(firstDestination).exists(_.after == balance(30L)),
        projection.semanticDelta.get(thirdDestination).exists(_.after == balance(20L)),
        !projection.semanticDelta.contains(awaitingDestination),
        production.contextUpdate.lastTxRefs.get(source).contains(thirdSuccessor)
      )
  }

  test("real allow-spend batch preserves all outcomes and exact exposed aggregate balance/reference state") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      firstDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      rejectedDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      thirdDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      awaitingDestinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      firstDestination = firstDestinationKey.getPublic.toAddress
      rejectedDestination = rejectedDestinationKey.getPublic.toAddress
      thirdDestination = thirdDestinationKey.getPublic.toAddress
      awaitingDestination = awaitingDestinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      genesis = AllowSpendReferenceCorrespondence.nativeGenesis
      first <- signedAllowSpend(
        source,
        sourceKey,
        firstDestination,
        none,
        genesis.production,
        30L,
        0L,
        110L,
        List(firstDestination),
        resources.currentHasher
      )
      firstSuccessor <- {
        implicit val allowHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](first)
      }
      rejected <- signedAllowSpend(
        source,
        sourceKey,
        rejectedDestination,
        none,
        firstSuccessor,
        1L,
        0L,
        110L,
        List(firstDestination),
        resources.currentHasher
      )
      third <- signedAllowSpend(
        source,
        sourceKey,
        thirdDestination,
        none,
        firstSuccessor,
        20L,
        0L,
        110L,
        List(thirdDestination),
        resources.currentHasher
      )
      thirdSuccessor <- {
        implicit val allowSpendHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](third)
      }
      awaiting <- signedAllowSpend(
        source,
        sourceKey,
        awaitingDestination,
        none,
        thirdSuccessor,
        60L,
        0L,
        110L,
        List(awaitingDestination),
        resources.currentHasher
      )
      firstBlock <- signedAllowSpendBlock(first, 9L, resources.currentHasher)
      rejectedBlock <- signedAllowSpendBlock(rejected, 10L, resources.currentHasher)
      thirdBlock <- signedAllowSpendBlock(third, 11L, resources.currentHasher)
      awaitingBlock <- signedAllowSpendBlock(awaiting, 12L, resources.currentHasher)
      context = AllowSpendBlockAcceptanceContext.fromStaticData[IO](
        initialBalances,
        Map(source -> genesis.production),
        Amount.empty,
        AllowSpendReference.empty
      )
      production <- {
        implicit val acceptanceHasher: Hasher[IO] = resources.currentHasher
        managerBundle.allowSpendAcceptanceManager.acceptBlocksIteratively(
          List(firstBlock, rejectedBlock, thirdBlock, awaitingBlock),
          context,
          snapshotOrdinal,
          lastGlobalSnapshotEpochProgress = epoch(100L).some
        )
      }
      base <- referenceBase(Dag, initialBalances)
      projection <- projectAllowSpendBatch(
        domain,
        lane,
        genesis,
        Vector(
          AllowSpendBatchItem(first, firstBlock),
          AllowSpendBatchItem(rejected, rejectedBlock),
          AllowSpendBatchItem(third, thirdBlock),
          AllowSpendBatchItem(awaiting, awaitingBlock)
        ),
        initialBalances,
        production,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      mismatchedPayloadProjection <- projectAllowSpendBatch(
        domain,
        lane,
        genesis,
        Vector(AllowSpendBatchItem(rejected, firstBlock)),
        initialBalances,
        production,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      reference = projection.referenceExecution
      rejectedDecision <- rejectedAt(reference, 1)
      awaitingDecision <- rejectedAt(reference, 3)
      rejectedReference <- {
        implicit val allowHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](rejected)
      }
      awaitingReference <- {
        implicit val allowHasher: Hasher[IO] = resources.currentHasher
        AllowSpendReference.of[IO](awaiting)
      }
      productionReason <- IO.fromOption(
        production.notAccepted.collectFirst { case (`rejectedBlock`, reason) => reason }
      )(new AssertionError("missing rejected allow-spend block decision"))
      productionRejection <- productionReason match {
        case reason: AllowSpendBlockRejectionReason => IO.pure(reason)
        case other                                  => IO.raiseError(new AssertionError(s"expected allow-spend rejection, got $other"))
      }
      productionAwaitReason <- IO.fromOption(
        production.notAccepted.collectFirst { case (`awaitingBlock`, reason) => reason }
      )(new AssertionError("missing awaiting allow-spend block decision"))
      productionAwait <- productionAwaitReason match {
        case reason: AllowSpendBlockAwaitReason => IO.pure(reason)
        case other                              => IO.raiseError(new AssertionError(s"expected allow-spend await reason, got $other"))
      }
      acceptedBindings = projection.bindings.zip(projection.dispositions).collect {
        case (binding, AllowSpendBatchAccepted) => binding
      }
    } yield
      expect.all(
        production.accepted == List(thirdBlock, firstBlock),
        production.notAccepted.map(_._1) == List(awaitingBlock, rejectedBlock),
        projection.dispositions == Vector(
          AllowSpendBatchAccepted,
          AllowSpendBatchRejected(rejectedReference, productionRejection),
          AllowSpendBatchAccepted,
          AllowSpendBatchAwaiting(awaitingReference, productionAwait)
        ),
        mismatchedPayloadProjection == Left(AllowSpendBatchPayloadMismatch(rejected, Vector(first))),
        productionReason == io.constellationnetwork.node.shared.domain.swap.block.ValidationFailed(
          NonEmptyList.one(
            InvalidAllowSpend(
              rejectedReference,
              AllowSpendValidator.InvalidApprover(List(firstDestination), rejectedDestination)
            )
          )
        ),
        rejectedDecision.reason == InvalidAllowSpendApprovers(rejectedDestination, Vector(firstDestination)),
        productionAwaitReason == io.constellationnetwork.node.shared.domain.swap.block.AddressBalanceOutOfRange(
          source,
          AmountUnderflow
        ),
        awaitingDecision.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 60, 50),
        reference.acceptedIds == acceptedBindings.map(_.identity),
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 50,
        projection.semanticDelta.get(source).exists(_.after == balance(50L)),
        production.contextUpdate.lastTxRefs.get(source).contains(thirdSuccessor)
      )
  }

  test("GL0 allow-spend epoch-window divergence remains explicit and is not normalized") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      destinationKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      destination = destinationKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = AllowSpendReferenceCorrespondence.nativeGenesis
      allowSpend <- signedAllowSpend(
        source,
        sourceKey,
        destination,
        none,
        correspondence.production,
        1L,
        0L,
        102L,
        List(destination),
        resources.currentHasher
      )
      production <- acceptNativeAllowSpend(
        allowSpend,
        initialBalances,
        correspondence.production,
        12L,
        managerBundle,
        resources
      )
      binding <- bindAllowSpend(
        allowSpend,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      base <- referenceBase(Dag, initialBalances)
      reference <- executeAllowSpendReference(lane, base, Vector(binding))
      rejected <- rejectedAt(reference, 0)
    } yield
      expect.all(
        production.isRight,
        rejected.reason == AllowSpendEpochOutsideWindow(102, 105, 120),
        reference.finalState == base,
        binding.domainBinding == UnboundLegacyPayload
      )
  }

  test("native zero-fee nonreplacement token lock binds the exact accepted payload and complete active state") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      tokenLock <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        60L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      acceptedPair <- acceptNativeTokenLock(
        tokenLock,
        initialBalances,
        correspondence.production,
        20L,
        managerBundle,
        resources
      )
      (block, production) = acceptedPair
      binding <- bindTokenLock(
        tokenLock,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      activeAfter <- nativeActiveTokenLocks(SortedSet(tokenLock), resources)
      finalBalances <- nativeTokenLockBalances(initialBalances, SortedSet(tokenLock), resources)
      observation <- IO.fromEither(
        observeBatchAcceptedTokenLock(binding, block, initialBalances, production, SortedMap.empty, activeAfter)
          .leftMap(error => new AssertionError(error.toString))
      )
      nextCorrespondence <- IO.fromEither(correspondence.advance(observation).leftMap(error => new AssertionError(error.toString)))
      unrelated <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        1L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      unrelatedBlock <- signedTokenLockBlock(unrelated, 21L, resources.currentHasher)
      payloadMismatch = observeBatchAcceptedTokenLock(
        binding,
        unrelatedBlock,
        initialBalances,
        production.copy(accepted = List(unrelatedBlock)),
        SortedMap.empty,
        activeAfter
      )
      blockedAdvance = payloadMismatch.flatMap(correspondence.advance)
      extraAcceptedProduction = production.copy(accepted = List(block, unrelatedBlock))
      extraAcceptedObservation = observeBatchAcceptedTokenLock(
        binding,
        block,
        initialBalances,
        extraAcceptedProduction,
        SortedMap.empty,
        activeAfter
      )
      base <- referenceBase(Dag, initialBalances)
      reference <- executeTokenLockReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
    } yield
      expect.all(
        production.accepted == List(block),
        production.notAccepted.isEmpty,
        production.contextUpdate.balances == finalBalances,
        production.contextUpdate.lastTokenLocksRefs == Map(source -> binding.productionSuccessor),
        binding.operationId == SupportedReferenceOperationId.TokenLockCreation,
        binding.signerFromProof == source,
        binding.allProofOwners == Vector(source),
        binding.domainBinding == UnboundLegacyPayload,
        activeAfter == SortedMap(source -> SortedSet(tokenLock)),
        observation.semanticDelta.activeAfter == activeAfter,
        payloadMismatch == Left(TokenLockBatchPayloadMismatch(tokenLock, Vector(unrelated))),
        blockedAdvance == Left(TokenLockBatchPayloadMismatch(tokenLock, Vector(unrelated))),
        extraAcceptedObservation == Left(
          UnexpectedTokenLockBatchDecisions(block, List(block, unrelatedBlock), List.empty)
        ),
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 40,
        reference.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) == binding.structuralSuccessor,
        reference.finalState.activeTokenLocks == Vector(binding.activeTokenLock),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Dag, source),
          reference
        ),
        nextCorrespondence.production == binding.productionSuccessor,
        nextCorrespondence.structural == binding.structuralSuccessor,
        reference.rejected.isEmpty
      )
  }

  test("currency zero-fee nonreplacement token lock uses the ML0 wrapper and exact metagraph scope") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      lane = CurrencyCl1(metagraphId)
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence <- TokenLockReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      tokenLock <- signedTokenLock(
        source,
        sourceKey,
        CurrencyId(metagraphId).some,
        correspondence.production,
        60L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      block <- signedTokenLockBlock(tokenLock, 22L, resources.currentHasher)
      production <- managerBundle.currencyAcceptanceManager.acceptTokenLockBlocks(
        List(block),
        currencySnapshotContext(
          metagraphId,
          initialBalances,
          lastTokenLockRefs = SortedMap(source -> correspondence.production)
        ),
        snapshotOrdinal,
        correspondence.production,
        shouldPerformMetagraphSpecificValidations = true,
        lastSyncGlobalSnapshotEpochProgress = epoch(100L)
      )
      binding <- bindTokenLock(
        tokenLock,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      activeAfter <- currencyActiveTokenLocks(SortedSet(tokenLock), resources)
      finalBalances <- currencyTokenLockBalances(initialBalances, SortedSet(tokenLock))
      observation <- IO.fromEither(
        observeBatchAcceptedTokenLock(binding, block, initialBalances, production, SortedMap.empty, activeAfter)
          .leftMap(error => new AssertionError(error.toString))
      )
      base <- referenceBase(Metagraph(metagraphId), initialBalances)
      reference <- executeTokenLockReference(lane, base, Vector(binding))
      accepted <- acceptedAt(reference, 0)
    } yield
      expect.all(
        production.accepted == List(block),
        production.notAccepted.isEmpty,
        production.contextUpdate.balances == finalBalances,
        production.contextUpdate.lastTokenLocksRefs == Map(source -> binding.productionSuccessor),
        activeAfter == SortedMap(source -> SortedSet(tokenLock)),
        observation.semanticDelta.activeAfter == SortedMap(source -> SortedSet(tokenLock)),
        binding.signed.value.currencyId.contains(CurrencyId(metagraphId)),
        binding.activeTokenLock.scope == Metagraph(metagraphId),
        accepted.identity == binding.identity,
        reference.finalState.balanceOf(ReferenceBalanceAccount(Metagraph(metagraphId), source)) == 40,
        reference.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) == binding.structuralSuccessor,
        reference.finalState.activeTokenLocks == Vector(binding.activeTokenLock),
        observedBalanceMatchesReference(
          observation.semanticDelta.balances,
          source,
          balance(100L),
          balance(40L),
          ReferenceBalanceAccount(Metagraph(metagraphId), source),
          reference
        ),
        reference.rejected.isEmpty
      )
  }

  test("token-lock production authority checks and helper-only genesis, fee, and replacement boundaries fail closed") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      otherMetagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      attackerKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      otherMetagraphId = otherMetagraphKey.getPublic.toAddress
      source = sourceKey.getPublic.toAddress
      nativeCorrespondence = TokenLockReferenceCorrespondence.nativeGenesis
      currencyCorrespondence <- TokenLockReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      wrongOwner <- signedTokenLock(
        source,
        attackerKey,
        none,
        nativeCorrespondence.production,
        1L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      wrongOwnerProduction <- acceptNativeTokenLock(
        wrongOwner,
        SortedMap(source -> balance(100L)),
        nativeCorrespondence.production,
        23L,
        managerBundle,
        resources
      )
      wrongOwnerReference <- TokenLockReference.of[IO](wrongOwner)
      wrongOwnerBinding <- bindTokenLock(
        wrongOwner,
        domain,
        NativeGl1,
        nativeCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      laneMismatch <- signedTokenLock(
        source,
        sourceKey,
        CurrencyId(otherMetagraphId).some,
        currencyCorrespondence.production,
        1L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      laneMismatchBlock <- signedTokenLockBlock(laneMismatch, 24L, resources.currentHasher)
      laneMismatchProduction <- managerBundle.currencyAcceptanceManager.acceptTokenLockBlocks(
        List(laneMismatchBlock),
        currencySnapshotContext(metagraphId, SortedMap(source -> balance(100L))),
        snapshotOrdinal,
        currencyCorrespondence.production,
        shouldPerformMetagraphSpecificValidations = true,
        lastSyncGlobalSnapshotEpochProgress = epoch(100L)
      )
      globalLaneMismatchProduction <- managerBundle.globalAcceptanceManager.acceptTokenLockBlocks(
        List(laneMismatchBlock),
        GlobalSnapshotInfo.empty,
        snapshotOrdinal,
        epoch(100L)
      )
      laneMismatchBinding <- bindTokenLock(
        laneMismatch,
        domain,
        CurrencyCl1(metagraphId),
        currencyCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      forgedParent = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(0L)), Hash("ab" * 32))
      wrongParent <- signedTokenLock(source, sourceKey, none, forgedParent, 1L, 0L, 110L.some, none, resources.currentHasher)
      wrongParentProduction <- acceptNativeTokenLock(
        wrongParent,
        SortedMap(source -> balance(100L)),
        nativeCorrespondence.production,
        25L,
        managerBundle,
        resources
      )
      wrongParentReference <- TokenLockReference.of[IO](wrongParent)
      wrongParentBinding <- bindTokenLock(
        wrongParent,
        domain,
        NativeGl1,
        nativeCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      forgedNativeGenesis <- TokenLockReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        forgedParent,
        resources.currentHasher
      )
      forgedCurrencyGenesis <- TokenLockReferenceCorrespondence.fromClaimedGenesis(
        CurrencyCl1(metagraphId),
        forgedParent,
        resources.currentHasher
      )
      canonicalNativeGenesis <- TokenLockReferenceCorrespondence.fromClaimedGenesis(
        NativeGl1,
        TokenLockReference.empty,
        resources.currentHasher
      )
      canonicalCurrencyGenesis <- TokenLockReferenceCorrespondence.fromClaimedGenesis(
        CurrencyCl1(metagraphId),
        currencyCorrespondence.production,
        resources.currentHasher
      )
      nonzeroFee <- signedTokenLock(
        source,
        sourceKey,
        none,
        nativeCorrespondence.production,
        1L,
        1L,
        110L.some,
        none,
        resources.currentHasher
      )
      nonzeroFeeProjectionBinding <- bindTokenLock(
        nonzeroFee,
        domain,
        NativeGl1,
        nativeCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      replacementRef = Hash("cd" * 32)
      replacement <- signedTokenLock(
        source,
        sourceKey,
        none,
        nativeCorrespondence.production,
        2L,
        0L,
        110L.some,
        replacementRef.some,
        resources.currentHasher
      )
      replacementProjectionBinding <- bindTokenLock(
        replacement,
        domain,
        NativeGl1,
        nativeCorrespondence,
        managerBundle.signedValidator,
        resources.currentHasher
      )
    } yield
      expect.all(
        wrongOwnerProduction._2.accepted.isEmpty,
        wrongOwnerProduction._2.notAccepted == List(
          wrongOwnerProduction._1 -> TokenLockValidationFailed(
            NonEmptyList.one(
              InvalidTokenLockTransaction(wrongOwnerReference, TokenLockValidator.NotSignedBySourceAddressOwner)
            )
          )
        ),
        wrongOwnerBinding == Left(ProductionSourceOwnerValidationRejected("ECO-TOKEN-LOCK-CREATE")),
        laneMismatchProduction.accepted.isEmpty,
        laneMismatchProduction.notAccepted == List(
          laneMismatchBlock -> InvalidMetagraphTokenLockLane(CurrencyId(metagraphId))
        ),
        globalLaneMismatchProduction.accepted.isEmpty,
        globalLaneMismatchProduction.notAccepted == List(laneMismatchBlock -> InvalidGlobalTokenLockLane),
        laneMismatchBinding == Left(PayloadLaneMismatch(CurrencyCl1(metagraphId), CurrencyId(otherMetagraphId).some)),
        wrongParentProduction._2.accepted.isEmpty,
        wrongParentProduction._2.notAccepted == List(
          wrongParentProduction._1 -> RejectedTokenLock(
            wrongParentReference,
            TokenLockParentHashMismatch(forgedParent.hash, nativeCorrespondence.production.hash)
          )
        ),
        wrongParentBinding == Left(TokenLockParentBindingMismatch(nativeCorrespondence.production, forgedParent)),
        forgedNativeGenesis == Left(InvalidTokenLockCanonicalGenesis(NativeGl1, TokenLockReference.empty, forgedParent)),
        forgedCurrencyGenesis == Left(
          InvalidTokenLockCanonicalGenesis(CurrencyCl1(metagraphId), currencyCorrespondence.production, forgedParent)
        ),
        canonicalNativeGenesis.exists(value =>
          value.production == TokenLockReference.empty && value.structural == StructuralTokenLockReference.genesis
        ),
        canonicalCurrencyGenesis.exists(value =>
          value.production == currencyCorrespondence.production && value.structural == StructuralTokenLockReference.genesis
        ),
        nonzeroFeeProjectionBinding == Left(NonZeroFeeOutOfScope("ECO-TOKEN-LOCK-CREATE", 1)),
        replacementProjectionBinding == Left(TokenLockReplacementOutOfScope(replacementRef))
      )
  }

  test("native token-lock batch retries child-before-parent and matches every accepted prefix exactly") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      first <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        30L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      firstRef <- TokenLockReference.of[IO](first)
      child <- signedTokenLock(source, sourceKey, none, firstRef, 20L, 0L, 111L.some, none, resources.currentHasher)
      childRef <- TokenLockReference.of[IO](child)
      firstBlock <- signedTokenLockBlock(first, 2L, resources.currentHasher)
      childBlock <- signedTokenLockBlock(child, 1L, resources.currentHasher)
      items = Vector(TokenLockBatchItem(first, firstBlock), TokenLockBatchItem(child, childBlock))
      prefixProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      forwardProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock, childBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      reverseProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(childBlock, firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      prefixActive <- activeTokenLocksForLane(lane, SortedSet(first), resources)
      fullActive <- activeTokenLocksForLane(lane, SortedSet(first, child), resources)
      prefixBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first), resources)
      fullBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first, child), resources)
      base <- referenceBase(Dag, initialBalances)
      prefixProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items.take(1),
        initialBalances,
        prefixProduction,
        SortedMap.empty,
        prefixActive,
        base,
        managerBundle,
        resources
      )
      forwardProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        forwardProduction,
        SortedMap.empty,
        fullActive,
        base,
        managerBundle,
        resources
      )
      reverseProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        reverseProduction,
        SortedMap.empty,
        fullActive,
        base,
        managerBundle,
        resources
      )
      third <- signedTokenLock(source, sourceKey, none, childRef, 1L, 0L, 112L.some, none, resources.currentHasher)
      thirdBlock <- signedTokenLockBlock(third, 3L, resources.currentHasher)
      substituted <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        Vector(TokenLockBatchItem(child, firstBlock), TokenLockBatchItem(child, childBlock)),
        initialBalances,
        forwardProduction,
        SortedMap.empty,
        fullActive,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      missingDecisionProduction = forwardProduction.copy(accepted = List(firstBlock))
      missingDecision <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        items,
        initialBalances,
        missingDecisionProduction,
        SortedMap.empty,
        fullActive,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      extraDecisionProduction = forwardProduction.copy(accepted = forwardProduction.accepted :+ thirdBlock)
      extraDecision <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        items,
        initialBalances,
        extraDecisionProduction,
        SortedMap.empty,
        fullActive,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
    } yield
      expect.all(
        List(childBlock, firstBlock).sorted == List(childBlock, firstBlock),
        prefixProduction.accepted == List(firstBlock),
        prefixProduction.notAccepted.isEmpty,
        prefixProduction.contextUpdate.balances == prefixBalances,
        prefixProduction.contextUpdate.lastTokenLocksRefs == Map(source -> firstRef),
        forwardProduction.accepted == List(firstBlock, childBlock),
        forwardProduction.notAccepted.isEmpty,
        forwardProduction == reverseProduction,
        forwardProduction.contextUpdate.balances == fullBalances,
        forwardProduction.contextUpdate.lastTokenLocksRefs == Map(source -> childRef),
        forwardProduction.contextUpdate.claimedReplacementRefs.isEmpty,
        forwardProduction.contextUpdate.inRoundTokenLocksByHash.keySet ==
          forwardProjection.bindings.map(_.productionHashed.hash).toSet,
        prefixActive == SortedMap(source -> SortedSet(first)),
        fullActive == SortedMap(source -> SortedSet(first, child)),
        prefixProjection.bindings.map(_.signed) == Vector(first),
        prefixProjection.dispositions == Vector(TokenLockBatchAccepted),
        prefixProjection.semanticDelta == SortedMap(source -> ObservedBalanceDelta(balance(100L), balance(70L))),
        prefixProjection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 70,
        prefixProjection.referenceExecution.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) ==
          prefixProjection.bindings.last.structuralSuccessor,
        prefixProjection.referenceExecution.finalState.activeTokenLocks == prefixProjection.bindings.map(_.activeTokenLock),
        forwardProjection.bindings.map(_.signed) == Vector(first, child),
        forwardProjection.dispositions == Vector(TokenLockBatchAccepted, TokenLockBatchAccepted),
        forwardProjection.semanticDelta == SortedMap(source -> ObservedBalanceDelta(balance(100L), balance(50L))),
        forwardProjection.activeAfter == fullActive,
        forwardProjection.referenceExecution.rejected.isEmpty,
        forwardProjection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 50,
        forwardProjection.referenceExecution.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) ==
          forwardProjection.bindings.last.structuralSuccessor,
        forwardProjection.referenceExecution.finalState.activeTokenLocks == forwardProjection.bindings.map(_.activeTokenLock),
        forwardProjection.referenceExecution.finalState.acceptedTokenLockHistory == forwardProjection.bindings.map(_.identity),
        reverseProjection.bindings.map(_.signed) == forwardProjection.bindings.map(_.signed),
        reverseProjection.dispositions == forwardProjection.dispositions,
        reverseProjection.semanticDelta == forwardProjection.semanticDelta,
        reverseProjection.activeAfter == forwardProjection.activeAfter,
        reverseProjection.referenceExecution.finalState == forwardProjection.referenceExecution.finalState,
        substituted == Left(TokenLockBatchPayloadMismatch(child, Vector(first))),
        missingDecision == Left(
          UnexpectedTokenLockBatchDecisionSet(items.map(_.block), missingDecisionProduction.accepted, List.empty)
        ),
        extraDecision == Left(
          UnexpectedTokenLockBatchDecisionSet(items.map(_.block), extraDecisionProduction.accepted, List.empty)
        )
      )
  }

  test("currency token-lock batch preserves ML0 scope, retry order, and accepted-prefix parity") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)

    for {
      metagraphKey <- KeyPairGenerator.makeKeyPair[IO]
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      metagraphId = metagraphKey.getPublic.toAddress
      lane = CurrencyCl1(metagraphId)
      scope = Metagraph(metagraphId)
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence <- TokenLockReferenceCorrespondence.currencyGenesis(metagraphId, resources.currentHasher)
      first <- signedTokenLock(
        source,
        sourceKey,
        CurrencyId(metagraphId).some,
        correspondence.production,
        25L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      firstRef <- TokenLockReference.of[IO](first)
      child <- signedTokenLock(
        source,
        sourceKey,
        CurrencyId(metagraphId).some,
        firstRef,
        15L,
        0L,
        111L.some,
        none,
        resources.currentHasher
      )
      childRef <- TokenLockReference.of[IO](child)
      firstBlock <- signedTokenLockBlock(first, 4L, resources.currentHasher)
      childBlock <- signedTokenLockBlock(child, 3L, resources.currentHasher)
      items = Vector(TokenLockBatchItem(first, firstBlock), TokenLockBatchItem(child, childBlock))
      prefixProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      forwardProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock, childBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      reverseProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(childBlock, firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      prefixActive <- activeTokenLocksForLane(lane, SortedSet(first), resources)
      fullActive <- activeTokenLocksForLane(lane, SortedSet(first, child), resources)
      prefixBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first), resources)
      fullBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first, child), resources)
      base <- referenceBase(scope, initialBalances)
      prefixProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items.take(1),
        initialBalances,
        prefixProduction,
        SortedMap.empty,
        prefixActive,
        base,
        managerBundle,
        resources
      )
      forwardProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        forwardProduction,
        SortedMap.empty,
        fullActive,
        base,
        managerBundle,
        resources
      )
      reverseProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        reverseProduction,
        SortedMap.empty,
        fullActive,
        base,
        managerBundle,
        resources
      )
    } yield
      expect.all(
        List(childBlock, firstBlock).sorted == List(childBlock, firstBlock),
        prefixProduction.accepted == List(firstBlock),
        prefixProduction.notAccepted.isEmpty,
        prefixProduction.contextUpdate.balances == prefixBalances,
        prefixProduction.contextUpdate.lastTokenLocksRefs == Map(source -> firstRef),
        prefixProduction.contextUpdate.inRoundTokenLocksByHash.isEmpty,
        forwardProduction.accepted == List(firstBlock, childBlock),
        forwardProduction.notAccepted.isEmpty,
        forwardProduction == reverseProduction,
        forwardProduction.contextUpdate.balances == fullBalances,
        forwardProduction.contextUpdate.lastTokenLocksRefs == Map(source -> childRef),
        forwardProduction.contextUpdate.claimedReplacementRefs.isEmpty,
        forwardProduction.contextUpdate.inRoundTokenLocksByHash.isEmpty,
        prefixActive == SortedMap(source -> SortedSet(first)),
        fullActive == SortedMap(source -> SortedSet(first, child)),
        prefixProjection.bindings.map(_.signed) == Vector(first),
        prefixProjection.dispositions == Vector(TokenLockBatchAccepted),
        prefixProjection.semanticDelta == SortedMap(source -> ObservedBalanceDelta(balance(100L), balance(75L))),
        prefixProjection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(scope, source)) == 75,
        prefixProjection.referenceExecution.finalState.activeTokenLocks == prefixProjection.bindings.map(_.activeTokenLock),
        forwardProjection.bindings.map(_.signed) == Vector(first, child),
        forwardProjection.bindings.forall(_.activeTokenLock.scope == scope),
        forwardProjection.dispositions == Vector(TokenLockBatchAccepted, TokenLockBatchAccepted),
        forwardProjection.semanticDelta == SortedMap(source -> ObservedBalanceDelta(balance(100L), balance(60L))),
        forwardProjection.activeAfter == fullActive,
        forwardProjection.referenceExecution.rejected.isEmpty,
        forwardProjection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(scope, source)) == 60,
        forwardProjection.referenceExecution.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) ==
          forwardProjection.bindings.last.structuralSuccessor,
        forwardProjection.referenceExecution.finalState.activeTokenLocks == forwardProjection.bindings.map(_.activeTokenLock),
        reverseProjection.bindings.map(_.signed) == forwardProjection.bindings.map(_.signed),
        reverseProjection.dispositions == forwardProjection.dispositions,
        reverseProjection.semanticDelta == forwardProjection.semanticDelta,
        reverseProjection.activeAfter == forwardProjection.activeAfter,
        reverseProjection.referenceExecution.finalState == forwardProjection.referenceExecution.finalState
      )
  }

  test("token-lock batch preserves typed production awaiting versus reference rejection without partial state") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      first <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        60L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      firstRef <- TokenLockReference.of[IO](first)
      second <- signedTokenLock(source, sourceKey, none, firstRef, 60L, 0L, 111L.some, none, resources.currentHasher)
      secondRef <- TokenLockReference.of[IO](second)
      firstBlock <- signedTokenLockBlock(first, 5L, resources.currentHasher)
      secondBlock <- signedTokenLockBlock(second, 6L, resources.currentHasher)
      items = Vector(TokenLockBatchItem(first, firstBlock), TokenLockBatchItem(second, secondBlock))
      production <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock, secondBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      activeAfter <- activeTokenLocksForLane(lane, SortedSet(first), resources)
      finalBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first), resources)
      base <- referenceBase(Dag, initialBalances)
      projection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        production,
        SortedMap.empty,
        activeAfter,
        base,
        managerBundle,
        resources
      )
      referenceRejection <- rejectedAt(projection.referenceExecution, 1)
    } yield
      expect.all(
        production.accepted == List(firstBlock),
        production.notAccepted == List(secondBlock -> TokenLockAddressBalanceOutOfRange(source, AmountUnderflow)),
        production.contextUpdate.balances == finalBalances,
        production.contextUpdate.balances == Map(source -> balance(40L)),
        production.contextUpdate.lastTokenLocksRefs == Map(source -> firstRef),
        production.contextUpdate.claimedReplacementRefs.isEmpty,
        production.contextUpdate.inRoundTokenLocksByHash.keySet == Set(projection.bindings.head.productionHashed.hash),
        projection.bindings.map(_.signed) == Vector(first, second),
        projection.dispositions == Vector(
          TokenLockBatchAccepted,
          TokenLockBatchAwaiting(secondRef, TokenLockAddressBalanceOutOfRange(source, AmountUnderflow))
        ),
        referenceRejection.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 60, 40),
        projection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 40,
        projection.referenceExecution.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) ==
          projection.bindings.head.structuralSuccessor,
        projection.referenceExecution.finalState.activeTokenLocks == Vector(projection.bindings.head.activeTokenLock),
        projection.referenceExecution.finalState.acceptedTokenLockHistory == Vector(projection.bindings.head.identity),
        projection.activeAfter == SortedMap(source -> SortedSet(first)),
        projection.semanticDelta == SortedMap(source -> ObservedBalanceDelta(balance(100L), balance(40L)))
      )
  }

  test("token-lock batch preserves exact accepted then mixed awaiting and rejected production order") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      rejectedBlockKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      first <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        60L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      firstRef <- TokenLockReference.of[IO](first)
      awaiting <- signedTokenLock(source, sourceKey, none, firstRef, 60L, 0L, 111L.some, none, resources.currentHasher)
      rejected <- signedTokenLock(source, sourceKey, none, firstRef, 70L, 0L, 112L.some, none, resources.currentHasher)
      awaitingRef <- TokenLockReference.of[IO](awaiting)
      rejectedRef <- TokenLockReference.of[IO](rejected)
      firstBlock <- signedTokenLockBlock(first, 21L, resources.currentHasher)
      awaitingBlock <- signedTokenLockBlock(awaiting, 22L, resources.currentHasher)
      rejectedBlock <- sign(
        TokenLockBlock(RoundId(new UUID(1L, 20L)), NonEmptySet.one(rejected)),
        rejectedBlockKey,
        resources.currentHasher
      )
      items = Vector(
        TokenLockBatchItem(first, firstBlock),
        TokenLockBatchItem(awaiting, awaitingBlock),
        TokenLockBatchItem(rejected, rejectedBlock)
      )
      production <- acceptTokenLockBatch(
        lane,
        source,
        List(awaitingBlock, rejectedBlock, firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      validationRejection = TokenLockValidationFailed(
        NonEmptyList.one(
          io.constellationnetwork.node.shared.domain.tokenlock.block.InvalidSigned(
            SignedValidator.NotEnoughSignatures(1L, 3)
          )
        )
      )
      expectedNotAccepted = List(
        awaitingBlock -> TokenLockAddressBalanceOutOfRange(source, AmountUnderflow),
        rejectedBlock -> validationRejection
      )
      activeAfter <- activeTokenLocksForLane(lane, SortedSet(first), resources)
      finalBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first), resources)
      base <- referenceBase(Dag, initialBalances)
      projection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        items,
        initialBalances,
        production,
        SortedMap.empty,
        activeAfter,
        base,
        managerBundle,
        resources
      )
      duplicateAcceptedProduction = production.copy(accepted = production.accepted :+ firstBlock)
      duplicateAccepted <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        items,
        initialBalances,
        duplicateAcceptedProduction,
        SortedMap.empty,
        activeAfter,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      duplicateNotAcceptedProduction = production.copy(notAccepted = production.notAccepted :+ production.notAccepted.head)
      duplicateNotAccepted <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        items,
        initialBalances,
        duplicateNotAcceptedProduction,
        SortedMap.empty,
        activeAfter,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      overlappingProduction = production.copy(accepted = production.accepted :+ awaitingBlock)
      overlap <- projectTokenLockBatch(
        domain,
        lane,
        correspondence,
        items,
        initialBalances,
        overlappingProduction,
        SortedMap.empty,
        activeAfter,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        base,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      awaitingReferenceRejection <- rejectedAt(projection.referenceExecution, 1)
      rejectedReferenceRejection <- rejectedAt(projection.referenceExecution, 2)
    } yield
      expect.all(
        List(rejectedBlock, firstBlock, awaitingBlock).sorted == List(rejectedBlock, firstBlock, awaitingBlock),
        production.accepted == List(firstBlock),
        production.notAccepted == expectedNotAccepted,
        production.contextUpdate.balances == finalBalances,
        production.contextUpdate.lastTokenLocksRefs == Map(source -> firstRef),
        projection.bindings.map(_.signed) == Vector(first, awaiting, rejected),
        projection.dispositions == Vector(
          TokenLockBatchAccepted,
          TokenLockBatchAwaiting(awaitingRef, TokenLockAddressBalanceOutOfRange(source, AmountUnderflow)),
          TokenLockBatchRejected(rejectedRef, validationRejection)
        ),
        projection.referenceExecution.finalState.balanceOf(ReferenceBalanceAccount(Dag, source)) == 40,
        projection.referenceExecution.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(lane, source)) ==
          projection.bindings.head.structuralSuccessor,
        awaitingReferenceRejection.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 60, 40),
        rejectedReferenceRejection.reason == InsufficientBalance(ReferenceBalanceAccount(Dag, source), 70, 40),
        projection.activeAfter == SortedMap(source -> SortedSet(first)),
        duplicateAccepted == Left(
          UnexpectedTokenLockBatchDecisionSet(items.map(_.block), duplicateAcceptedProduction.accepted, expectedNotAccepted)
        ),
        duplicateNotAccepted == Left(
          UnexpectedTokenLockBatchDecisionSet(
            items.map(_.block),
            production.accepted,
            duplicateNotAcceptedProduction.notAccepted
          )
        ),
        overlap == Left(
          UnexpectedTokenLockBatchDecisionSet(items.map(_.block), overlappingProduction.accepted, expectedNotAccepted)
        )
      )
  }

  test("token-lock batch stale and wrong-parent decisions are permanent and cannot mint reference inputs") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      first <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        30L,
        0L,
        110L.some,
        none,
        resources.currentHasher
      )
      firstBlock <- signedTokenLockBlock(first, 7L, resources.currentHasher)
      firstProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(firstBlock),
        initialBalances,
        correspondence.production,
        managerBundle
      )
      firstActive <- activeTokenLocksForLane(lane, SortedSet(first), resources)
      firstBalances <- tokenLockBalancesForLane(lane, initialBalances, SortedSet(first), resources)
      base <- referenceBase(Dag, initialBalances)
      firstProjection <- projectTokenLockBatchOrRaise(
        lane,
        correspondence,
        Vector(TokenLockBatchItem(first, firstBlock)),
        initialBalances,
        firstProduction,
        SortedMap.empty,
        firstActive,
        base,
        managerBundle,
        resources
      )
      firstBinding = firstProjection.bindings.head
      firstObservation <- IO.fromEither(
        observeBatchAcceptedTokenLock(firstBinding, firstBlock, initialBalances, firstProduction, SortedMap.empty, firstActive)
          .leftMap(error => new AssertionError(error.toString))
      )
      advanced <- IO.fromEither(correspondence.advance(firstObservation).leftMap(error => new AssertionError(error.toString)))
      stale <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        1L,
        0L,
        111L.some,
        none,
        resources.currentHasher
      )
      forgedParent = TokenLockReference(advanced.production.ordinal, Hash("ef" * 32))
      wrongParent <- signedTokenLock(source, sourceKey, none, forgedParent, 1L, 0L, 111L.some, none, resources.currentHasher)
      staleRef <- TokenLockReference.of[IO](stale)
      wrongParentRef <- TokenLockReference.of[IO](wrongParent)
      staleBlock <- signedTokenLockBlock(stale, 8L, resources.currentHasher)
      wrongParentBlock <- signedTokenLockBlock(wrongParent, 9L, resources.currentHasher)
      staleProduction <- acceptTokenLockBatch(lane, source, List(staleBlock), firstBalances, advanced.production, managerBundle)
      wrongParentProduction <- acceptTokenLockBatch(
        lane,
        source,
        List(wrongParentBlock),
        firstBalances,
        advanced.production,
        managerBundle
      )
      staleProjection <- projectTokenLockBatch(
        domain,
        lane,
        advanced,
        Vector(TokenLockBatchItem(stale, staleBlock)),
        firstBalances,
        staleProduction,
        firstActive,
        firstActive,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        firstProjection.referenceExecution.finalState,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      wrongParentProjection <- projectTokenLockBatch(
        domain,
        lane,
        advanced,
        Vector(TokenLockBatchItem(wrongParent, wrongParentBlock)),
        firstBalances,
        wrongParentProduction,
        firstActive,
        firstActive,
        ReferenceContext(domain, lane, SortedSet.empty, epochWindow.some, tokenLockEpochRule.some),
        firstProjection.referenceExecution.finalState,
        managerBundle.signedValidator,
        resources.currentHasher
      )
      staleReason = RejectedTokenLock(
        staleRef,
        TokenLockParentOrdinalBelowLastTxOrdinal(correspondence.production.ordinal, advanced.production.ordinal)
      )
      wrongParentReason = RejectedTokenLock(
        wrongParentRef,
        TokenLockParentHashMismatch(forgedParent.hash, advanced.production.hash)
      )
    } yield
      expect.all(
        staleProduction.accepted.isEmpty,
        staleProduction.notAccepted == List(staleBlock -> staleReason),
        TokenLockBlockNotAcceptedReason.isPermanent(staleReason),
        staleProduction.contextUpdate == TokenLockBlockAcceptanceContextUpdate.empty,
        staleProjection == Left(TokenLockParentBindingMismatch(advanced.production, correspondence.production)),
        wrongParentProduction.accepted.isEmpty,
        wrongParentProduction.notAccepted == List(wrongParentBlock -> wrongParentReason),
        TokenLockBlockNotAcceptedReason.isPermanent(wrongParentReason),
        wrongParentProduction.contextUpdate == TokenLockBlockAcceptanceContextUpdate.empty,
        wrongParentProjection == Left(TokenLockParentBindingMismatch(advanced.production, forgedParent))
      )
  }

  test("snapshot token-lock acceptance remains weaker than the target contextual minimum-duration rule") { resources =>
    implicit val securityProvider: SecurityProvider[IO] = resources.securityProvider
    implicit val currentHasher: Hasher[IO] = resources.currentHasher
    val managerBundle = managers(resources)
    val lane = NativeGl1

    for {
      sourceKey <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKey.getPublic.toAddress
      initialBalances = SortedMap(source -> balance(100L))
      correspondence = TokenLockReferenceCorrespondence.nativeGenesis
      tokenLock <- signedTokenLock(
        source,
        sourceKey,
        none,
        correspondence.production,
        1L,
        0L,
        104L.some,
        none,
        resources.currentHasher
      )
      production <- acceptNativeTokenLock(
        tokenLock,
        initialBalances,
        correspondence.production,
        26L,
        managerBundle,
        resources
      )
      binding <- bindTokenLock(
        tokenLock,
        domain,
        lane,
        correspondence,
        managerBundle.signedValidator,
        resources.currentHasher
      ).flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))
      base <- referenceBase(Dag, initialBalances)
      reference <- executeTokenLockReference(lane, base, Vector(binding))
      rejected <- rejectedAt(reference, 0)
    } yield
      expect.all(
        production._2.accepted == List(production._1),
        production._2.notAccepted.isEmpty,
        rejected.reason == ReferenceRejection.TokenLockUnlockEpochTooShort(104, 105),
        reference.finalState == base,
        binding.domainBinding == UnboundLegacyPayload
      )
  }
}
