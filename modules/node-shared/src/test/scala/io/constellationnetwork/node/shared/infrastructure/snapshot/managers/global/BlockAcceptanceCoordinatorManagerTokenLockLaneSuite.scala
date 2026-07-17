package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair
import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.economics.StakeBackingValidator
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.tokenlock.TokenLockChainValidator.TokenLockNel
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.{delegatedStakeRecordSetCodec, signedTokenLockSetCodec}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.SimpleIOSuite

object BlockAcceptanceCoordinatorManagerTokenLockLaneSuite extends SimpleIOSuite {

  private def tokenLock(
    sourceKeyPair: KeyPair,
    amount: Long,
    currencyId: Option[CurrencyId],
    replacementRef: Option[Hash] = none,
    unlockEpoch: Option[EpochProgress] = EpochProgress(100L).some
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLock]] =
    Signed.forAsyncHasher(
      TokenLock(
        sourceKeyPair.getPublic.toAddress,
        TokenLockAmount(PosLong.unsafeFrom(amount)),
        TokenLockFee(0L),
        TokenLockReference.empty,
        currencyId,
        unlockEpoch,
        replacementRef
      ),
      sourceKeyPair
    )

  private def block(
    round: Long,
    tokenLocks: NonEmptySet[Signed[TokenLock]],
    blockKeyPair: KeyPair
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLockBlock]] =
    Signed.forAsyncHasher(TokenLockBlock(RoundId(new UUID(0L, round)), tokenLocks), blockKeyPair)

  private final class CountingBackingReader(
    source: io.constellationnetwork.schema.address.Address,
    activeLock: Signed[TokenLock],
    balance: Balance,
    delegated: Option[DelegatedStakeRecord],
    captures: Ref[IO, Int],
    failOnCapture: Boolean
  )(implicit hasher: Hasher[IO])
      extends GlobalStateReader[IO] {

    private val activeLockKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
    private val balanceKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, source)
    private val delegatedKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveDelegatedStakes, source)
    private val activeLocks = SortedSet(activeLock)
    private val delegatedRecords = delegated.fold(SortedSet.empty[DelegatedStakeRecord])(SortedSet(_))

    def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = {
      val value =
        if (key == activeLockKey) activeLocks.some
        else if (key == balanceKey) balance.some
        else if (key == delegatedKey && delegated.nonEmpty) delegatedRecords.some
        else none

      value.map(_.asInstanceOf[V]).pure[IO]
    }

    def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] =
      get[V](key).map {
        case Some(value) => StrictMptRead.Present(value, ImmutableCodec[V].immutableBytes(value))
        case None        => StrictMptRead.Absent
      }

    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] =
      keys.traverse(key => get[V](key).map(_.map(key -> _))).map(_.flatten.toMap)

    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] =
      Map.empty[Hex, V].pure[IO]

    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      List.empty[StrictMptEntry[V]].pure[IO]

    override def captureRawPrefixesStrict(prefixes: List[Hex]): Option[IO[Map[Hex, List[StrictMptRawEntry]]]] =
      Some(
        captures.update(_ + 1) >>
          IO.raiseWhen(failOnCapture)(new AssertionError("backing partitions were captured before cheap rejection")) >>
          (for {
            tokenLockPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[IO](GlobalStateFieldId.ActiveTokenLocks)
            delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[IO](GlobalStateFieldId.ActiveDelegatedStakes)
            tokenLockPhysicalKey <- GlobalStateKey.toHex[IO](activeLockKey)
            delegatedPhysicalKey <- GlobalStateKey.toHex[IO](delegatedKey)
          } yield {
            val empty = prefixes.distinct.map(_ -> List.empty[StrictMptRawEntry]).toMap
            val withLock = empty.updated(
              tokenLockPrefix,
              List(StrictMptRawEntry(tokenLockPhysicalKey, signedTokenLockSetCodec.immutableBytes(activeLocks).some))
            )
            delegated.fold(withLock)(_ =>
              withLock.updated(
                delegatedPrefix,
                List(
                  StrictMptRawEntry(
                    delegatedPhysicalKey,
                    delegatedStakeRecordSetCodec.immutableBytes(delegatedRecords).some
                  )
                )
              )
            )
          })
      )
  }

  test("GL0 forwards only all-native token-lock blocks to shared acceptance") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          currencyKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          blockKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          currencyId = CurrencyId(currencyKeyPair.getPublic.toAddress)
          nativeTx <- tokenLock(sourceKeyPair, 10L, none)
          currencyTx <- tokenLock(sourceKeyPair, 11L, currencyId.some)
          currencyReplacementTx <- tokenLock(sourceKeyPair, 12L, currencyId.some, Hash("ab" * 32).some)
          validNative <- block(1L, NonEmptySet.one(nativeTx), blockKeyPair)
          plainCurrency <- block(2L, NonEmptySet.one(currencyTx), blockKeyPair)
          currencyReplacement <- block(3L, NonEmptySet.one(currencyReplacementTx), blockKeyPair)
          mixed <- block(4L, NonEmptySet.of(nativeTx, currencyTx), blockKeyPair)
          seen <- Ref.of[IO, List[Signed[TokenLockBlock]]](List.empty)
          seenEpoch <- Ref.of[IO, Option[EpochProgress]](EpochProgress(999L).some)
          tokenLockManager = new TokenLockBlockAcceptanceManager[IO] {
            def acceptBlocksIteratively(
              blocks: List[Signed[TokenLockBlock]],
              context: TokenLockBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
              seen.set(blocks) *> seenEpoch.set(lastGlobalSnapshotEpochProgress) *> TokenLockBlockAcceptanceResult(
                TokenLockBlockAcceptanceContextUpdate.empty,
                blocks,
                List.empty
              ).pure[IO]

            def acceptBlock(
              block: Signed[TokenLockBlock],
              context: TokenLockBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]) =
              IO.raiseError(new AssertionError("single-block acceptance must not be used"))
          }
          manager = BlockAcceptanceCoordinatorManager.make[IO](
            blockAcceptanceManager = null,
            allowSpendBlockAcceptanceManager = null,
            tokenLockBlockAcceptanceManager = tokenLockManager,
            tipUsageManager = null,
            collateral = Amount.empty,
            reader = GlobalStateReader.empty[IO]
          )
          result <- manager.acceptTokenLockBlocks(
            List(plainCurrency, currencyReplacement, mixed, validNative),
            GlobalSnapshotInfo.empty,
            SnapshotOrdinal.MinValue,
            EpochProgress.MinValue
          )
          passedToShared <- seen.get
          epochPassedToShared <- seenEpoch.get
          invalidBlocks = Set(plainCurrency, currencyReplacement, mixed)
        } yield
          expect.all(
            passedToShared == List(validNative),
            epochPassedToShared.contains(EpochProgress.MinValue),
            result.accepted == List(validNative),
            result.contextUpdate == TokenLockBlockAcceptanceContextUpdate.empty,
            result.notAccepted.map(_._1).toSet == invalidBlocks,
            result.notAccepted.forall { case (_, reason) => reason == InvalidGlobalTokenLockLane },
            TokenLockBlockNotAcceptedReason.isPermanent(InvalidGlobalTokenLockLane)
          )
      }
    }
  }

  test("cheaply invalid replacement never captures the five backing partitions") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          source = sourceKeyPair.getPublic.toAddress
          existing <- tokenLock(sourceKeyPair, 50L, none, unlockEpoch = none)
          existingHashed <- existing.toHashed
          invalid <- tokenLock(
            sourceKeyPair,
            50L,
            none,
            replacementRef = existingHashed.hash.some,
            unlockEpoch = none
          )
          invalidBlock <- block(10L, NonEmptySet.one(invalid), sourceKeyPair)
          captures <- Ref.of[IO, Int](0)
          reader = new CountingBackingReader(
            source,
            existing,
            Balance(NonNegLong.unsafeFrom(1_000L)),
            none,
            captures,
            failOnCapture = true
          )
          validator = new TokenLockBlockValidator[IO] {
            def validate(
              signedBlock: Signed[TokenLockBlock],
              snapshotOrdinal: SnapshotOrdinal,
              params: TokenLockBlockValidationParams,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[TokenLockBlockValidationErrorOr[
              (
                Signed[TokenLockBlock],
                Map[
                  io.constellationnetwork.schema.address.Address,
                  TokenLockNel
                ]
              )
            ]] =
              (
                signedBlock,
                Map(source -> NonEmptyList.one(signedBlock.value.tokenLocks.head))
              ).validNec.pure[IO]
          }
          tokenLockManager = TokenLockBlockAcceptanceManager.make[IO](
            TokenLockBlockAcceptanceLogic.make[IO],
            validator
          )
          coordinator = BlockAcceptanceCoordinatorManager.make[IO](
            blockAcceptanceManager = null,
            allowSpendBlockAcceptanceManager = null,
            tokenLockBlockAcceptanceManager = tokenLockManager,
            tipUsageManager = null,
            collateral = Amount.empty,
            reader = reader
          )
          result <- coordinator.acceptTokenLockBlocksWithBackingState(
            List(invalidBlock),
            GlobalSnapshotInfo.empty,
            SnapshotOrdinal.MinValue,
            EpochProgress.MinValue,
            none
          )
          captureCount <- captures.get
        } yield
          expect.all(
            result.result.accepted.isEmpty,
            result.result.notAccepted.map(_._1) == List(invalidBlock),
            result.parentBackingState.isEmpty,
            captureCount == 0
          )
      }
    }
  }

  test("one atomic backing capture is reused through replacement filtering and final transition validation") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          source = sourceKeyPair.getPublic.toAddress
          existing <- tokenLock(sourceKeyPair, 50L, none, unlockEpoch = none)
          existingRef <- TokenLockReference.of(existing)
          delegatedCreate <- Signed.forAsyncHasher(
            UpdateDelegatedStake.Create(
              source = source,
              nodeId = PeerId.fromPublic(sourceKeyPair.getPublic),
              amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(50L)),
              fee = DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
              tokenLockRef = existingRef.hash,
              parent = DelegatedStakeReference.empty
            ),
            sourceKeyPair
          )
          delegatedRecord = DelegatedStakeRecord(
            delegatedCreate,
            SnapshotOrdinal.MinValue,
            Amount.empty
          )
          replacement <- tokenLock(
            sourceKeyPair,
            60L,
            none,
            replacementRef = existingRef.hash.some,
            unlockEpoch = none
          )
          replacementRef <- TokenLockReference.of(replacement)
          replacementBlock <- block(11L, NonEmptySet.one(replacement), sourceKeyPair)
          captures <- Ref.of[IO, Int](0)
          reader = new CountingBackingReader(
            source,
            existing,
            Balance(NonNegLong.unsafeFrom(10L)),
            delegatedRecord.some,
            captures,
            failOnCapture = false
          )
          tokenLockManager = new TokenLockBlockAcceptanceManager[IO] {
            def acceptBlocksIteratively(
              blocks: List[Signed[TokenLockBlock]],
              context: TokenLockBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
              for {
                first <- context.getBackingReplacementRequirement(existingRef.hash)
                second <- context.getBackingReplacementRequirement(existingRef.hash)
                _ <- IO.raiseUnless(
                  first.contains(StakeBackingValidator.DelegatedBackingRequirement(source, 50L, 50L)) &&
                    second == first
                )(new AssertionError("backing lookup did not resolve the exact active delegated binding"))
              } yield
                TokenLockBlockAcceptanceResult(
                  TokenLockBlockAcceptanceContextUpdate.empty,
                  blocks,
                  List.empty
                )

            def acceptBlock(
              block: Signed[TokenLockBlock],
              context: TokenLockBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]) =
              IO.raiseError(new AssertionError("single-block acceptance must not be used"))
          }
          coordinator = BlockAcceptanceCoordinatorManager.make[IO](
            blockAcceptanceManager = null,
            allowSpendBlockAcceptanceManager = null,
            tokenLockBlockAcceptanceManager = tokenLockManager,
            tipUsageManager = null,
            collateral = Amount.empty,
            reader = reader
          )
          accepted <- coordinator.acceptTokenLockBlocksWithBackingState(
            List(replacementBlock),
            GlobalSnapshotInfo.empty,
            SnapshotOrdinal.MinValue,
            EpochProgress.MinValue,
            none
          )
          afterCoordinator <- captures.get
          parent <- accepted.parentBackingState.liftTo[IO](
            new AssertionError("coordinator did not return its captured parent backing state")
          )
          stateAccepted <- TokenLockStateManager
            .make[IO](reader)
            .acceptReplacementTokenLocks(
              List(replacement),
              GlobalSnapshotInfo.empty,
              accepted.parentBackingState
            )
          afterStateFiltering <- captures.get
          updatedDelegatedRecord = delegatedRecord.copy(
            currentTokenLockRef = replacementRef.hash.some,
            currentAmount = DelegatedStakeAmount(NonNegLong.unsafeFrom(60L)).some
          )
          _ <- StakeBackingValidator.validateTransition[IO](
            parent,
            SortedMap(source -> SortedSet(replacement)),
            SortedMap(source -> SortedSet(updatedDelegatedRecord)),
            SortedMap.empty,
            SortedMap.empty,
            SortedMap.empty
          )
          afterTransition <- captures.get
        } yield
          expect.all(
            accepted.result.accepted == List(replacementBlock),
            stateAccepted == List(replacement),
            afterCoordinator == 1,
            afterStateFiltering == 1,
            afterTransition == 1
          )
      }
    }
  }
}
