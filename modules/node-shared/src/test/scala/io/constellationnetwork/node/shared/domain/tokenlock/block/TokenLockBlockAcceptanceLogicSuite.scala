package io.constellationnetwork.node.shared.domain.tokenlock.block

import java.security.KeyPair
import java.util.UUID

import cats.data.{EitherT, NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.tokenlock.TokenLockChainValidator.TokenLockNel
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.TokenLockStateManager
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.TokenUnlock
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object TokenLockBlockAcceptanceLogicSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, sp)

  private def balance(value: Long): Balance = Balance(NonNegLong.unsafeFrom(value))
  private def amount(value: Long): TokenLockAmount = TokenLockAmount(PosLong.unsafeFrom(value))
  private def fee(value: Long): TokenLockFee = TokenLockFee(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def makeTokenLock(
    sourceKeyPair: KeyPair,
    amountValue: Long,
    feeValue: Long,
    parent: TokenLockReference = TokenLockReference.empty,
    replacementRef: Option[Hash] = none,
    currencyId: Option[CurrencyId] = none,
    unlockEpochValue: Long = 100L
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLock]] =
    Signed.forAsyncHasher(
      TokenLock(
        source = sourceKeyPair.getPublic.toAddress,
        amount = amount(amountValue),
        fee = fee(feeValue),
        parent = parent,
        currencyId = currencyId,
        unlockEpoch = epoch(unlockEpochValue).some,
        replaceTokenLockRef = replacementRef
      ),
      sourceKeyPair
    )

  private def makeBlock(
    txs: List[Signed[TokenLock]],
    id: Long
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLockBlock]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { blockKeyPair =>
      val block = TokenLockBlock(
        RoundId(new UUID(0L, id)),
        NonEmptySet.fromSetUnsafe(SortedSet.from(txs))
      )
      Signed.forAsyncHasher(block, blockKeyPair)
    }

  private def chains(txs: List[Signed[TokenLock]]): Map[Address, NonEmptyList[Signed[TokenLock]]] =
    txs.groupBy(_.source).view.mapValues(values => NonEmptyList.fromListUnsafe(values.sortBy(_.ordinal))).toMap

  private def accept(
    logic: TokenLockBlockAcceptanceLogic[IO],
    block: Signed[TokenLockBlock],
    context: TokenLockBlockAcceptanceContext[IO],
    update: TokenLockBlockAcceptanceContextUpdate
  )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
    logic
      .acceptBlock(
        block,
        chains(block.value.tokenLocks.toList),
        context,
        update,
        shouldPerformMetagraphSpecificValidations = false
      )
      .value

  private def context(
    source: Address,
    sourceBalance: Long,
    replacementCandidates: List[Hashed[TokenLock]],
    currentEpochProgress: EpochProgress = epoch(1L)
  ): TokenLockBlockAcceptanceContext[IO] =
    TokenLockBlockAcceptanceContext.fromStaticData[IO](
      balances = Map(source -> balance(sourceBalance)),
      lastTxRefs = Map.empty,
      collateral = io.constellationnetwork.schema.balance.Amount.empty,
      initialTxRef = TokenLockReference.empty,
      toBeReplacedHashedTokenLocks = replacementCandidates,
      currentEpochProgress = currentEpochProgress
    )

  private def readerWithBalance(source: Address, sourceBalance: Balance): GlobalStateReader[IO] =
    new GlobalStateReader[IO] {
      private val balanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source)

      def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] =
        Option.when(key == balanceKey)(sourceBalance.asInstanceOf[V]).pure[IO]

      def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = Map.empty[GlobalStateKey, V].pure[IO]

      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = Map.empty[Hex, V].pure[IO]
    }

  test("only a replacement referenced by the current block releases capacity and final application matches admission") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      existing <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      existingHashed <- existing.toHashed[IO]
      unrelated <- makeTokenLock(sourceKeyPair, amountValue = 20L, feeValue = 0L)
      replacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 5L,
        replacementRef = existingHashed.hash.some
      )
      unrelatedBlock <- makeBlock(List(unrelated), 1L)
      replacementBlock <- makeBlock(List(replacement), 2L)
      acceptanceContext = context(source, sourceBalance = 10L, List(existingHashed))
      logic = TokenLockBlockAcceptanceLogic.make[IO]
      unrelatedResult <- accept(logic, unrelatedBlock, acceptanceContext, TokenLockBlockAcceptanceContextUpdate.empty)
      replacementResult <- accept(logic, replacementBlock, acceptanceContext, TokenLockBlockAcceptanceContextUpdate.empty)
      replacementUpdate <- IO.fromEither(replacementResult.leftMap(new AssertionError(_)))

      stateManager = TokenLockStateManager.make[IO](GlobalStateReader.empty[IO])
      generatedUnlocks <- IO.fromEither(
        stateManager
          .generateTokenUnlocks(SortedMap.empty, List(replacement), Map(existingHashed.hash -> existing))
          .leftMap(new AssertionError(_))
      )
      finalBalances <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(10L)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(replacement)),
        generatedTokenUnlocksByAddress = generatedUnlocks,
        expiredGlobalTokenLocks = SortedMap.empty
      )
      applied <- IO.fromEither(finalBalances.leftMap(new AssertionError(_)))
    } yield expect.all(
      unrelatedResult.left.exists {
        case AddressBalanceOutOfRange(address, _) => address === source
        case _                                    => false
      },
      replacementUpdate.balances.get(source).contains(Balance.empty),
      replacementUpdate.claimedReplacementRefs == Set(existingHashed.hash),
      applied._1.get(source).contains(Balance.empty)
    )
  }

  test("an unavailable replacement reference awaits without advancing balances or claims") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      currencyKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      missingRef = Hash("ab" * 32)
      replacement <- makeTokenLock(sourceKeyPair, amountValue = 55L, feeValue = 5L, replacementRef = missingRef.some)
      currencyReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 5L,
        currencyId = CurrencyId(currencyKeyPair.getPublic.toAddress).some,
        replacementRef = missingRef.some
      )
      block <- makeBlock(List(replacement), 3L)
      currencyBlock <- makeBlock(List(currencyReplacement), 30L)
      result <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        block,
        context(source, sourceBalance = 1_000L, List.empty),
        TokenLockBlockAcceptanceContextUpdate.empty
      )
      currencyResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        currencyBlock,
        context(source, sourceBalance = 1_000L, List.empty),
        TokenLockBlockAcceptanceContextUpdate.empty
      )
      txRef <- TokenLockReference.of(replacement)
      currencyTxRef <- TokenLockReference.of(currencyReplacement)
    } yield expect.all(
      result == AwaitingReplacementTokenLock(txRef, missingRef).asLeft,
      currencyResult == AwaitingReplacementTokenLock(currencyTxRef, missingRef).asLeft,
      !TokenLockBlockNotAcceptedReason.isPermanent(AwaitingReplacementTokenLock(txRef, missingRef))
    )
  }

  test("one replacement reference contributes capacity at most once within a block") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      existing <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      existingHashed <- existing.toHashed[IO]
      first <- makeTokenLock(sourceKeyPair, amountValue = 55L, feeValue = 5L, replacementRef = existingHashed.hash.some)
      firstRef <- TokenLockReference.of(first)
      duplicate <- makeTokenLock(
        sourceKeyPair,
        amountValue = 56L,
        feeValue = 4L,
        parent = firstRef,
        replacementRef = existingHashed.hash.some
      )
      block <- makeBlock(List(first, duplicate), 4L)
      result <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        block,
        context(source, sourceBalance = 10L, List(existingHashed)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )
      reason = ReplacementTokenLockAlreadyClaimed(existingHashed.hash)
    } yield expect.all(
      result == reason.asLeft,
      TokenLockBlockNotAcceptedReason.isPermanent(reason)
    )
  }

  test("found replacement targets with wrong source, currency, or amount are permanent rejections") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      otherKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      currencyKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      otherSource = otherKeyPair.getPublic.toAddress
      currencyId = CurrencyId(currencyKeyPair.getPublic.toAddress)

      wrongSourceTarget <- makeTokenLock(otherKeyPair, amountValue = 50L, feeValue = 0L)
      wrongSourceHashed <- wrongSourceTarget.toHashed[IO]
      wrongSourceReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 0L,
        replacementRef = wrongSourceHashed.hash.some
      )
      wrongSourceRef <- TokenLockReference.of(wrongSourceReplacement)
      wrongSourceBlock <- makeBlock(List(wrongSourceReplacement), 31L)
      wrongSourceResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        wrongSourceBlock,
        context(source, 100L, List(wrongSourceHashed)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )

      wrongCurrencyTarget <- makeTokenLock(
        sourceKeyPair,
        amountValue = 50L,
        feeValue = 0L,
        currencyId = currencyId.some
      )
      wrongCurrencyHashed <- wrongCurrencyTarget.toHashed[IO]
      wrongCurrencyReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 0L,
        replacementRef = wrongCurrencyHashed.hash.some
      )
      wrongCurrencyRef <- TokenLockReference.of(wrongCurrencyReplacement)
      wrongCurrencyBlock <- makeBlock(List(wrongCurrencyReplacement), 32L)
      wrongCurrencyResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        wrongCurrencyBlock,
        context(source, 100L, List(wrongCurrencyHashed)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )

      equalAmountTarget <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      equalAmountHashed <- equalAmountTarget.toHashed[IO]
      equalAmountReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 50L,
        feeValue = 0L,
        replacementRef = equalAmountHashed.hash.some
      )
      equalAmountRef <- TokenLockReference.of(equalAmountReplacement)
      equalAmountBlock <- makeBlock(List(equalAmountReplacement), 33L)
      equalAmountResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        equalAmountBlock,
        context(source, 100L, List(equalAmountHashed)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )

      reasons = List(
        RejectedReplacementTokenLock(
          wrongSourceRef,
          wrongSourceHashed.hash,
          ReplacementSourceMismatch(source, otherSource)
        ),
        RejectedReplacementTokenLock(
          wrongCurrencyRef,
          wrongCurrencyHashed.hash,
          ReplacementCurrencyMismatch(none, currencyId.some)
        ),
        RejectedReplacementTokenLock(
          equalAmountRef,
          equalAmountHashed.hash,
          ReplacementAmountNotIncreased(equalAmountReplacement.amount, equalAmountTarget.amount)
        )
      )
    } yield expect.all(
      List(wrongSourceResult, wrongCurrencyResult, equalAmountResult) == reasons.map(_.asLeft),
      reasons.forall(TokenLockBlockNotAcceptedReason.isPermanent)
    )
  }

  test("replacement target expiry uses the strict less-than execution boundary") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      expired <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L, unlockEpochValue = 0L)
      expiredHashed <- expired.toHashed[IO]
      expiredReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 0L,
        replacementRef = expiredHashed.hash.some
      )
      expiredReplacementRef <- TokenLockReference.of(expiredReplacement)
      expiredBlock <- makeBlock(List(expiredReplacement), 34L)
      expiredResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        expiredBlock,
        context(source, 10L, List(expiredHashed), epoch(1L)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )

      boundary <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L, unlockEpochValue = 1L)
      boundaryHashed <- boundary.toHashed[IO]
      boundaryReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 0L,
        replacementRef = boundaryHashed.hash.some
      )
      boundaryBlock <- makeBlock(List(boundaryReplacement), 35L)
      boundaryResult <- accept(
        TokenLockBlockAcceptanceLogic.make[IO],
        boundaryBlock,
        context(source, 10L, List(boundaryHashed), epoch(1L)),
        TokenLockBlockAcceptanceContextUpdate.empty
      )
      boundaryUpdate <- IO.fromEither(boundaryResult.leftMap(new AssertionError(_)))
      expiredReason = RejectedReplacementTokenLock(
        expiredReplacementRef,
        expiredHashed.hash,
        ReplacementTargetExpired(epoch(0L), epoch(1L))
      )
    } yield expect.all(
      expiredResult == expiredReason.asLeft,
      TokenLockBlockNotAcceptedReason.isPermanent(expiredReason),
      boundaryUpdate.balances.get(source).contains(balance(5L)),
      boundaryUpdate.claimedReplacementRefs == Set(boundaryHashed.hash)
    )
  }

  test("final application releases each ref once and rejects conflicting duplicate payloads") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      target <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      targetHashed <- target.toHashed[IO]
      replacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 5L,
        replacementRef = targetHashed.hash.some
      )
      unlock = TokenUnlock(targetHashed.hash, target.amount, target.currencyId, target.source)
      stateManager = TokenLockStateManager.make[IO](GlobalStateReader.empty[IO])
      duplicateResult <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(10L)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(replacement)),
        generatedTokenUnlocksByAddress = Map(source -> List(unlock, unlock)),
        expiredGlobalTokenLocks = SortedMap.empty
      )
      duplicateApplied <- IO.fromEither(duplicateResult.leftMap(new AssertionError(_)))

      expiredTarget <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L, unlockEpochValue = 0L)
      expiredTargetHashed <- expiredTarget.toHashed[IO]
      expiredUnlock = TokenUnlock(expiredTargetHashed.hash, expiredTarget.amount, expiredTarget.currencyId, expiredTarget.source)
      expiredReplacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 5L,
        replacementRef = expiredTargetHashed.hash.some
      )
      expiryAndReplacementResult <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(10L)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(expiredReplacement)),
        generatedTokenUnlocksByAddress = Map(source -> List(expiredUnlock)),
        expiredGlobalTokenLocks = SortedMap(source -> SortedSet(expiredTarget))
      )
      expiryAndReplacementApplied <- IO.fromEither(expiryAndReplacementResult.leftMap(new AssertionError(_)))

      conflictingUnlock = unlock.copy(amount = amount(51L))
      conflicting <- stateManager
        .updateGlobalBalancesByTokenLocksWithExpired(
          epochProgress = epoch(1L),
          currentBalances = SortedMap(source -> balance(10L)),
          acceptedGlobalTokenLocks = SortedMap.empty,
          generatedTokenUnlocksByAddress = Map(source -> List(unlock, conflictingUnlock)),
          expiredGlobalTokenLocks = SortedMap.empty
        )
        .attempt
      conflictingExpiredUnlock = expiredUnlock.copy(amount = amount(51L))
      conflictingWithExpiry <- stateManager
        .updateGlobalBalancesByTokenLocksWithExpired(
          epochProgress = epoch(1L),
          currentBalances = SortedMap(source -> balance(10L)),
          acceptedGlobalTokenLocks = SortedMap.empty,
          generatedTokenUnlocksByAddress = Map(source -> List(conflictingExpiredUnlock)),
          expiredGlobalTokenLocks = SortedMap(source -> SortedSet(expiredTarget))
        )
        .attempt
    } yield expect.all(
      duplicateApplied._1.get(source).contains(Balance.empty),
      expiryAndReplacementApplied._1.get(source).contains(Balance.empty),
      conflicting.left.exists(_.isInstanceOf[IllegalStateException]),
      conflictingWithExpiry.left.exists(_.isInstanceOf[IllegalStateException])
    )
  }

  test("final application rejects a newly accepted expired lock and debits the equality boundary") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      expired <- makeTokenLock(sourceKeyPair, amountValue = 10L, feeValue = 2L, unlockEpochValue = 0L)
      boundary <- makeTokenLock(sourceKeyPair, amountValue = 10L, feeValue = 2L, unlockEpochValue = 1L)
      stateManager = TokenLockStateManager.make[IO](GlobalStateReader.empty[IO])
      expiredResult <- stateManager
        .updateGlobalBalancesByTokenLocksWithExpired(
          epochProgress = epoch(1L),
          currentBalances = SortedMap(source -> balance(100L)),
          acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(expired)),
          generatedTokenUnlocksByAddress = Map.empty,
          expiredGlobalTokenLocks = SortedMap.empty
        )
        .attempt
      boundaryResult <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(100L)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(boundary)),
        generatedTokenUnlocksByAddress = Map.empty,
        expiredGlobalTokenLocks = SortedMap.empty
      )
      boundaryApplied <- IO.fromEither(boundaryResult.leftMap(new AssertionError(_)))
    } yield expect.all(
      expiredResult.left.exists(_.isInstanceOf[IllegalStateException]),
      boundaryApplied._1.get(source).contains(balance(88L))
    )
  }

  test("sibling replacement blocks cannot reuse one release across either submission order") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      existing <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      existingHashed <- existing.toHashed[IO]
      first <- makeTokenLock(sourceKeyPair, amountValue = 55L, feeValue = 5L, replacementRef = existingHashed.hash.some)
      firstRef <- TokenLockReference.of(first)
      sibling <- makeTokenLock(
        sourceKeyPair,
        amountValue = 56L,
        feeValue = 4L,
        parent = firstRef,
        replacementRef = existingHashed.hash.some
      )
      firstBlock <- makeBlock(List(first), 41L)
      siblingBlock <- makeBlock(List(sibling), 42L)
      acceptanceContext = context(source, sourceBalance = 10L, List(existingHashed))
      logic = TokenLockBlockAcceptanceLogic.make[IO]

      firstAccepted <- accept(logic, firstBlock, acceptanceContext, TokenLockBlockAcceptanceContextUpdate.empty)
      firstUpdate <- IO.fromEither(firstAccepted.leftMap(new AssertionError(_)))
      siblingAfterFirst <- accept(logic, siblingBlock, acceptanceContext, firstUpdate)

      siblingBeforeFirst <- accept(logic, siblingBlock, acceptanceContext, TokenLockBlockAcceptanceContextUpdate.empty)
      firstAfterAwait <- accept(logic, firstBlock, acceptanceContext, TokenLockBlockAcceptanceContextUpdate.empty)
      retryBase <- IO.fromEither(firstAfterAwait.leftMap(new AssertionError(_)))
      siblingRetry <- accept(logic, siblingBlock, acceptanceContext, retryBase)

      stateManager = TokenLockStateManager.make[IO](GlobalStateReader.empty[IO])
      generatedUnlocks <- IO.fromEither(
        stateManager
          .generateTokenUnlocks(SortedMap.empty, List(first), Map(existingHashed.hash -> existing))
          .leftMap(new AssertionError(_))
      )
      finalBalances <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(10L)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet(first)),
        generatedTokenUnlocksByAddress = generatedUnlocks,
        expiredGlobalTokenLocks = SortedMap.empty
      )
      applied <- IO.fromEither(finalBalances.leftMap(new AssertionError(_)))
    } yield expect.all(
      siblingAfterFirst == ReplacementTokenLockAlreadyClaimed(existingHashed.hash).asLeft,
      siblingBeforeFirst.left.exists(_.isInstanceOf[TokenLockBlockAwaitReason]),
      siblingRetry == ReplacementTokenLockAlreadyClaimed(existingHashed.hash).asLeft,
      retryBase.balances == firstUpdate.balances,
      applied._1.get(source).contains(Balance.empty)
    )
  }

  test("replacement claims survive block permutation/retry and an in-round target remains replaceable") { res =>
    implicit val (hasher, securityProvider) = res

    def runWithRetry(
      logic: TokenLockBlockAcceptanceLogic[IO],
      acceptanceContext: TokenLockBlockAcceptanceContext[IO],
      blocks: List[Signed[TokenLockBlock]]
    ): IO[(TokenLockBlockAcceptanceContextUpdate, List[Signed[TokenLockBlock]])] = {
      def pass(
        pending: List[Signed[TokenLockBlock]],
        update: TokenLockBlockAcceptanceContextUpdate,
        accepted: List[Signed[TokenLockBlock]]
      ): IO[(TokenLockBlockAcceptanceContextUpdate, List[Signed[TokenLockBlock]], List[Signed[TokenLockBlock]])] =
        pending.foldLeftM((update, accepted, List.empty[Signed[TokenLockBlock]])) {
          case ((current, acceptedBlocks, retry), block) =>
            accept(logic, block, acceptanceContext, current).map {
              case Right(next)                          => (next, acceptedBlocks :+ block, retry)
              case Left(_: TokenLockBlockAwaitReason) => (current, acceptedBlocks, retry :+ block)
              case Left(_)                             => (current, acceptedBlocks, retry)
            }
        }

      pass(blocks, TokenLockBlockAcceptanceContextUpdate.empty, List.empty).flatMap {
        case (firstUpdate, firstAccepted, retry) =>
          pass(retry, firstUpdate, firstAccepted).map { case (update, accepted, _) => (update, accepted) }
      }
    }

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      initial <- makeTokenLock(sourceKeyPair, amountValue = 50L, feeValue = 0L)
      initialHashed <- initial.toHashed[IO]
      initialRef <- TokenLockReference.of(initial)
      replacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = 55L,
        feeValue = 5L,
        parent = initialRef,
        replacementRef = initialHashed.hash.some
      )
      initialBlock <- makeBlock(List(initial), 5L)
      replacementBlock <- makeBlock(List(replacement), 6L)
      acceptanceContext = context(source, sourceBalance = 60L, List.empty)
      logic = TokenLockBlockAcceptanceLogic.make[IO]
      forward <- runWithRetry(logic, acceptanceContext, List(initialBlock, replacementBlock))
      reverse <- runWithRetry(logic, acceptanceContext, List(replacementBlock, initialBlock))
    } yield expect.all(
      forward._2 == List(initialBlock, replacementBlock),
      reverse._2 == List(initialBlock, replacementBlock),
      forward._1.balances == reverse._1.balances,
      forward._1.balances.get(source).contains(Balance.empty)
    )
  }

  test("manager retries B-before-A and state application preserves order, refs, and Long.MaxValue net balance") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      first <- makeTokenLock(sourceKeyPair, amountValue = 1L, feeValue = 0L)
      firstRef <- TokenLockReference.of(first)
      firstHashed <- first.toHashed[IO]
      replacement <- makeTokenLock(
        sourceKeyPair,
        amountValue = Long.MaxValue,
        feeValue = 0L,
        parent = firstRef,
        replacementRef = firstHashed.hash.some
      )
      replacementRef <- TokenLockReference.of(replacement)
      replacementBlock <- makeBlock(List(replacement), 1L)
      firstBlock <- makeBlock(List(first), 2L)
      calls <- Ref.of[IO, List[RoundId]](List.empty)
      baseLogic = TokenLockBlockAcceptanceLogic.make[IO]
      recordingLogic = new TokenLockBlockAcceptanceLogic[IO] {
        def acceptBlock(
          block: Signed[TokenLockBlock],
          txChains: Map[Address, NonEmptyList[Signed[TokenLock]]],
          acceptanceContext: TokenLockBlockAcceptanceContext[IO],
          contextUpdate: TokenLockBlockAcceptanceContextUpdate,
          shouldPerformMetagraphSpecificValidations: Boolean
        )(implicit hasher: Hasher[IO]) =
          EitherT(
            calls.update(_ :+ block.value.roundId) >>
              baseLogic
                .acceptBlock(block, txChains, acceptanceContext, contextUpdate, shouldPerformMetagraphSpecificValidations)
                .value
          )
      }
      validator = new TokenLockBlockValidator[IO] {
        def validate(
          signedBlock: Signed[TokenLockBlock],
          snapshotOrdinal: SnapshotOrdinal,
          params: TokenLockBlockValidationParams,
          lastGlobalSnapshotEpochProgress: Option[EpochProgress]
        )(implicit hasher: Hasher[IO]): IO[TokenLockBlockValidationErrorOr[(Signed[TokenLockBlock], Map[Address, TokenLockNel])]] =
          (signedBlock, chains(signedBlock.tokenLocks.toList)).validNec.pure[IO]
      }
      manager = TokenLockBlockAcceptanceManager.make[IO](recordingLogic, validator)
      result <- manager.acceptBlocksIteratively(
        List(replacementBlock, firstBlock),
        context(source, Long.MaxValue, List.empty),
        SnapshotOrdinal.MinValue,
        shouldPerformMetagraphSpecificValidations = false,
        epoch(1L).some
      )
      callOrder <- calls.get
      canonicalTxs = result.accepted.flatMap(_.tokenLocks.toList).sortBy(tx => (tx.source, tx.ordinal, tx))
      stateManager = TokenLockStateManager.make[IO](readerWithBalance(source, balance(Long.MaxValue)))
      stateAccepted <- stateManager.acceptReplacementTokenLocks(canonicalTxs, GlobalSnapshotInfo.empty)
      inRoundByHash <- stateAccepted.traverse(lock => lock.toHashed.map(hashed => hashed.hash -> lock)).map(_.toMap)
      generatedUnlocks <- IO.fromEither(
        stateManager
          .generateTokenUnlocks(SortedMap.empty, stateAccepted, inRoundByHash)
          .leftMap(new AssertionError(_))
      )
      applied <- stateManager.updateGlobalBalancesByTokenLocksWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(Long.MaxValue)),
        acceptedGlobalTokenLocks = SortedMap(source -> SortedSet.from(stateAccepted)),
        generatedTokenUnlocksByAddress = generatedUnlocks,
        expiredGlobalTokenLocks = SortedMap.empty
      )
      finalBalances <- IO.fromEither(applied.leftMap(new AssertionError(_)))
    } yield expect.all(
      List(replacementBlock, firstBlock).sorted == List(replacementBlock, firstBlock),
      callOrder == List(replacementBlock.value.roundId, firstBlock.value.roundId, replacementBlock.value.roundId),
      result.notAccepted.isEmpty,
      result.accepted == List(firstBlock, replacementBlock),
      result.contextUpdate.lastTokenLocksRefs.get(source).contains(replacementRef),
      result.contextUpdate.balances.get(source).contains(Balance.empty),
      stateAccepted == canonicalTxs,
      finalBalances._1.get(source).contains(Balance.empty)
    )
  }
}
