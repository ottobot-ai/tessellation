package io.constellationnetwork.node.shared.domain.delegatedStake

import java.security.KeyPair

import cats.Order
import cats.data.{NonEmptyChain, NonEmptySet}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeValidator._
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeValidatorSuite.{
  mkGlobalContext,
  pointReaderFromContextForTest
}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.infrastructure.snapshot.{DelegatedRewardsDistributor, PartitionedStakeUpdates}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.node._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.all.PosLong
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object UpdateDelegatedStakeAcceptanceManagerSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], KeyPair, Address)

  def sharedResource: Resource[IO, Res] = for {
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    kp <- KeyPairGenerator.makeKeyPair[IO].asResource
    sourceAddress <- kp.getPublic.toId.toAddress.asResource
  } yield (j, h, sp, kp, sourceAddress)

  private def activeDelegatedStakeReader(
    source: Address,
    records: SortedSet[DelegatedStakeRecord]
  )(implicit hasher: Hasher[IO]): IO[GlobalStateReader[IO]] = {
    val key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveDelegatedStakes, source)
    val present = StrictMptRead.Present(records, delegatedStakeRecordSetCodec.immutableBytes(records))

    GlobalStateKey.toHex[IO](key).map { physicalKey =>
      new GlobalStateReader[IO] {
        def get[V: ImmutableCodec](requested: GlobalStateKey): IO[Option[V]] = IO.pure(None)

        def getStrict[V: ImmutableCodec](requested: GlobalStateKey): IO[StrictMptRead[V]] =
          IO.pure(
            if (requested === key) present.asInstanceOf[StrictMptRead[V]]
            else StrictMptRead.Absent
          )

        def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)

        def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)

        def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
          IO.pure(
            Option
              .when(physicalKey.value.startsWith(prefix.value))(StrictMptEntry(physicalKey, present))
              .toList
              .asInstanceOf[List[StrictMptEntry[V]]]
          )
      }
    }
  }

  private def stubValidator(
    createValidation: Signed[UpdateDelegatedStake.Create] => UpdateDelegatedStakeValidationErrorOr[Signed[UpdateDelegatedStake.Create]] =
      (signed: Signed[UpdateDelegatedStake.Create]) => signed.validNec,
    withdrawValidation: Signed[UpdateDelegatedStake.Withdraw] => UpdateDelegatedStakeValidationErrorOr[
      Signed[UpdateDelegatedStake.Withdraw]
    ] = (signed: Signed[UpdateDelegatedStake.Withdraw]) => signed.validNec
  ): UpdateDelegatedStakeValidator[IO] =
    new UpdateDelegatedStakeValidator[IO] {
      def validateCreateDelegatedStake(
        signed: Signed[UpdateDelegatedStake.Create],
        parentStateReader: GlobalStateReader[IO]
      ): IO[UpdateDelegatedStakeValidationErrorOr[Signed[UpdateDelegatedStake.Create]]] =
        IO.pure(createValidation(signed))

      def validateWithdrawDelegatedStake(
        signed: Signed[UpdateDelegatedStake.Withdraw],
        parentStateReader: GlobalStateReader[IO]
      ): IO[UpdateDelegatedStakeValidationErrorOr[Signed[UpdateDelegatedStake.Withdraw]]] =
        IO.pure(withdrawValidation(signed))
    }

  test("distinct sources may both accept the empty delegated-stake parent") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res

    for {
      secondKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      secondSource = secondKeyPair.getPublic.toAddress
      first <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, Hash("11" * 32)), kp)
      second <- Signed.forAsyncHasher(testCreateDelegatedStake(secondKeyPair, secondSource, 200L, Hash("22" * 32)), secondKeyPair)
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](stubValidator())
      result <- manager.accept(
        creates = List(second, first),
        withdrawals = List.empty,
        parentStateReader = GlobalStateReader.empty[IO],
        currentGlobalEpochProgress = EpochProgress.MinValue,
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
    } yield
      expect.all(
        result.acceptedCreates.values.flatten.map(_._1).toSet == Set(first, second),
        result.notAcceptedCreates.isEmpty
      )
  }

  test("canonical earlier invalid create does not reserve its parent or token lock against a later valid create") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res

    for {
      first <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, Hash("33" * 32)), kp)
      second <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 200L, Hash("33" * 32)), kp)
      ordered = List(first, second).sorted(Signed.ordering(Order[UpdateDelegatedStake.Create].toOrdering))
      invalid = ordered.head
      valid = ordered.last
      validator = stubValidator(createValidation =
        signed => if (signed == invalid) InvalidParent(signed.parent).invalidNec else signed.validNec
      )
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](validator)
      result <- manager.accept(
        creates = List(first, second),
        withdrawals = List.empty,
        parentStateReader = GlobalStateReader.empty[IO],
        currentGlobalEpochProgress = EpochProgress.MinValue,
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
    } yield
      expect.all(
        result.acceptedCreates.values.flatten.map(_._1).toList == List(valid),
        result.notAcceptedCreates.map(_._1) == List(invalid),
        result.notAcceptedCreates.headOption.exists(_._2 == NonEmptyChain.of(InvalidParent(invalid.parent)))
      )
  }

  test("canonical earlier invalid withdrawal does not reserve its stake ref against a later valid withdrawal") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res

    for {
      secondKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      unsigned = testWithdrawDelegatedStake(sourceAddress, Hash("55" * 32))
      first <- Signed.forAsyncHasher(unsigned, kp)
      second <- Signed.forAsyncHasher(unsigned, secondKeyPair)
      ordered = List(first, second).sorted(Signed.ordering(Order[UpdateDelegatedStake.Withdraw].toOrdering))
      invalid = ordered.head
      valid = ordered.last
      validator = stubValidator(withdrawValidation =
        signed => if (signed == invalid) InvalidStake(signed.stakeRef).invalidNec else signed.validNec
      )
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](validator)
      result <- manager.accept(
        creates = List.empty,
        withdrawals = List(first, second),
        parentStateReader = GlobalStateReader.empty[IO],
        currentGlobalEpochProgress = EpochProgress.MinValue,
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
    } yield
      expect.all(
        result.acceptedWithdrawals.values.flatten.map(_._1).toList == List(valid),
        result.notAcceptedWithdrawals.map(_._1) == List(invalid),
        result.notAcceptedWithdrawals.headOption.exists(_._2 == NonEmptyChain.of(InvalidStake(invalid.stakeRef)))
      )
  }

  test("accepted same-batch stake replacement prevents the replaced stake from entering withdrawal") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val originalTokenLockRef = Hash("65" * 32)
    val currentTokenLockRef = Hash("66" * 32)

    for {
      successorNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      original <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, originalTokenLockRef), kp)
      originalRef <- DelegatedStakeReference.of(original)
      originalRecord =
        DelegatedStakeRecord(original, SnapshotOrdinal.MinValue, Amount.empty, currentTokenLockRef.some, None)
      parentStateReader <- activeDelegatedStakeReader(sourceAddress, SortedSet(originalRecord))
      successor <- Signed.forAsyncHasher(
        testCreateDelegatedStake(successorNodeKeyPair, sourceAddress, 100L, currentTokenLockRef, originalRef),
        kp
      )
      withdrawal <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, originalRef.hash), kp)
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](stubValidator())
      result <- manager.accept(
        creates = List(successor),
        withdrawals = List(withdrawal),
        parentStateReader = parentStateReader,
        currentGlobalEpochProgress = EpochProgress(NonNegLong(1L)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
      partitionedRecords = PartitionedStakeUpdates(
        unexpiredCreateDelegatedStakes = SortedMap(sourceAddress -> SortedSet(originalRecord)),
        unexpiredWithdrawalsDelegatedStaking = SortedMap.empty,
        expiredWithdrawalsDelegatedStaking = SortedMap.empty
      )
      activeStakes <- DelegatedRewardsDistributor.getUpdatedCreateDelegatedStakes[IO](
        Map.empty,
        result,
        partitionedRecords
      )
      pendingWithdrawals <- DelegatedRewardsDistributor.getUpdatedWithdrawalDelegatedStakes[IO](
        parentStateReader,
        result,
        partitionedRecords
      )
    } yield {
      val resultingStakes = activeStakes.getOrElse(sourceAddress, SortedSet.empty[DelegatedStakeRecord])

      expect.all(
        result.acceptedCreates.values.flatten.map(_._1).toList == List(successor),
        result.acceptedWithdrawals.isEmpty,
        result.notAcceptedWithdrawals.map(_._1) == List(withdrawal),
        result.notAcceptedWithdrawals.headOption.exists(
          _._2 == NonEmptyChain.of(ConflictingStakeTransition(originalRef.hash))
        ),
        resultingStakes.size == 1,
        resultingStakes.headOption.exists(record => record.event == successor && record.tokenLockRef == currentTokenLockRef),
        pendingWithdrawals.isEmpty
      )
    }
  }

  test("invalid same-batch stake replacement does not reserve the backing lock against withdrawal") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val tokenLockRef = Hash("77" * 32)

    for {
      successorNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      original <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, tokenLockRef), kp)
      originalRef <- DelegatedStakeReference.of(original)
      originalRecord = DelegatedStakeRecord(original, SnapshotOrdinal.MinValue, Amount.empty, None, None)
      parentStateReader <- activeDelegatedStakeReader(sourceAddress, SortedSet(originalRecord))
      successor <- Signed.forAsyncHasher(
        testCreateDelegatedStake(successorNodeKeyPair, sourceAddress, 100L, tokenLockRef, originalRef),
        kp
      )
      withdrawal <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, originalRef.hash), kp)
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](
        stubValidator(createValidation = signed => InvalidParent(signed.parent).invalidNec)
      )
      result <- manager.accept(
        creates = List(successor),
        withdrawals = List(withdrawal),
        parentStateReader = parentStateReader,
        currentGlobalEpochProgress = EpochProgress(NonNegLong(1L)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
    } yield
      expect.all(
        result.acceptedCreates.isEmpty,
        result.notAcceptedCreates.map(_._1) == List(successor),
        result.notAcceptedCreates.headOption.exists(_._2 == NonEmptyChain.of(InvalidParent(successor.parent))),
        result.acceptedWithdrawals.values.flatten.map(_._1).toList == List(withdrawal),
        result.notAcceptedWithdrawals.isEmpty
      )
  }

  test("delegated-stake materialization rejects contradictory accepted replacement and withdrawal") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val tokenLockRef = Hash("88" * 32)

    for {
      successorNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      original <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, tokenLockRef), kp)
      originalRef <- DelegatedStakeReference.of(original)
      originalRecord = DelegatedStakeRecord(original, SnapshotOrdinal.MinValue, Amount.empty, None, None)
      parentStateReader <- activeDelegatedStakeReader(sourceAddress, SortedSet(originalRecord))
      successor <- Signed.forAsyncHasher(
        testCreateDelegatedStake(successorNodeKeyPair, sourceAddress, 100L, tokenLockRef, originalRef),
        kp
      )
      withdrawal <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, originalRef.hash), kp)
      contradictoryResult = UpdateDelegatedStakeAcceptanceResult(
        acceptedCreates = SortedMap(sourceAddress -> List(successor -> SnapshotOrdinal.unsafeApply(2L))),
        notAcceptedCreates = List.empty,
        acceptedWithdrawals = SortedMap(sourceAddress -> List(withdrawal -> EpochProgress(NonNegLong(1L)))),
        notAcceptedWithdrawals = List.empty
      )
      partitionedRecords = PartitionedStakeUpdates(
        unexpiredCreateDelegatedStakes = SortedMap(sourceAddress -> SortedSet(originalRecord)),
        unexpiredWithdrawalsDelegatedStaking = SortedMap.empty,
        expiredWithdrawalsDelegatedStaking = SortedMap.empty
      )
      createResult <- DelegatedRewardsDistributor
        .getUpdatedCreateDelegatedStakes[IO](Map.empty, contradictoryResult, partitionedRecords)
        .attempt
      withdrawalResult <- DelegatedRewardsDistributor
        .getUpdatedWithdrawalDelegatedStakes[IO](parentStateReader, contradictoryResult, partitionedRecords)
        .attempt
    } yield {
      def isExpectedConflict(result: Either[Throwable, _]): Boolean =
        result.left.exists {
          case conflict: DelegatedRewardsDistributor.ConflictingAcceptedStakeTransitions =>
            conflict.source == sourceAddress &&
            conflict.stakeRef == originalRef.hash &&
            conflict.tokenLockRef == tokenLockRef
          case _ => false
        }

      expect.all(
        isExpectedConflict(createResult),
        isExpectedConflict(withdrawalResult)
      )
    }
  }

  test("should reject stakes with the same parent") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val acceptanceManager =
      UpdateDelegatedStakeAcceptanceManager.make[IO](UpdateDelegatedStakeValidator.make[IO](SignedValidator.make[IO], None))
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      ((ref1, ref2), ctx) <- mkValidGlobalContext(kp, kp1, kp)
      parent1 <- Signed.forAsyncHasher(testCreateDelegatedStake(kp2, sourceAddress, 100L), kp)
      lastRef1 <- DelegatedStakeReference.of(parent1)
      context = ctx.copy(activeDelegatedStakes =
        Some(
          SortedMap(
            sourceAddress ->
              SortedSet(
                DelegatedStakeRecord(parent1, SnapshotOrdinal.MinValue, Amount(NonNegLong(0L)), None, None)
              )
          )
        )
      )
      valid1 <- Signed.forAsyncHasher(testCreateDelegatedStake(kp1, sourceAddress, 100L, tokenLockReference = ref1, parent = lastRef1), kp)
      invalid <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 200L, tokenLockReference = ref2, parent = lastRef1), kp)
      res <- acceptanceManager.accept(
        creates = List(valid1, invalid),
        withdrawals = List.empty,
        parentStateReader = pointReaderFromContextForTest(context),
        currentGlobalEpochProgress = EpochProgress.MinValue,
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2),
        acceptedTokenLocks = List.empty
      )
    } yield {
      // After defensive sorting, the first-wins duplicate resolution may accept either one.
      // We verify that exactly one is accepted and the other is rejected with DuplicatedParent.
      val allAcceptedCreates = res.acceptedCreates.values.flatten.map(_._1).toList
      val allRejectedCreates = res.notAcceptedCreates.map(_._1)
      expect.all(
        allAcceptedCreates.size == 1,
        allRejectedCreates.size == 1,
        Set(valid1, invalid).contains(allAcceptedCreates.head),
        Set(valid1, invalid).contains(allRejectedCreates.head),
        allAcceptedCreates.head != allRejectedCreates.head,
        res.notAcceptedCreates.head._2 == NonEmptyChain.of(DuplicatedParent(lastRef1)),
        res.acceptedWithdrawals.isEmpty,
        res.notAcceptedWithdrawals.isEmpty
      )
    }
  }

  test("should reject withdrawals with the same parent") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val acceptanceManager =
      UpdateDelegatedStakeAcceptanceManager.make[IO](UpdateDelegatedStakeValidator.make[IO](SignedValidator.make[IO], None))
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      (_, ctx) <- mkValidGlobalContext(kp, kp1, kp)
      parent1 <- Signed.forAsyncHasher(testCreateDelegatedStake(kp2, sourceAddress, 100L), kp)
      lastRef1 <- DelegatedStakeReference.of(parent1)
      context = ctx.copy(activeDelegatedStakes =
        Some(
          SortedMap(
            sourceAddress ->
              SortedSet(
                DelegatedStakeRecord(parent1, SnapshotOrdinal.MinValue, Amount(NonNegLong(0L)), None, None)
              )
          )
        )
      )
      withdraw1 <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, lastRef1.hash), kp)
      withdraw2 <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, lastRef1.hash), kp)
      res <- acceptanceManager.accept(
        creates = List.empty,
        withdrawals = List(withdraw1, withdraw2),
        parentStateReader = pointReaderFromContextForTest(context),
        currentGlobalEpochProgress = EpochProgress.apply(NonNegLong(1)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2),
        acceptedTokenLocks = List.empty
      )
    } yield {
      // After defensive sorting, first-wins duplicate resolution may accept either one.
      val allAcceptedWithdrawals = res.acceptedWithdrawals.values.flatten.map(_._1).toList
      val allRejectedWithdrawals = res.notAcceptedWithdrawals.map(_._1)
      expect.all(
        res.acceptedCreates.isEmpty,
        res.notAcceptedCreates.isEmpty,
        allAcceptedWithdrawals.size == 1,
        allRejectedWithdrawals.size == 1,
        Set(withdraw1, withdraw2).contains(allAcceptedWithdrawals.head),
        Set(withdraw1, withdraw2).contains(allRejectedWithdrawals.head),
        allAcceptedWithdrawals.head != allRejectedWithdrawals.head,
        res.notAcceptedWithdrawals.head._2 == NonEmptyChain.of(DuplicatedStake(withdraw1.stakeRef))
      )
    }
  }

  test("should accept withdrawals with different parents") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val acceptanceManager =
      UpdateDelegatedStakeAcceptanceManager.make[IO](UpdateDelegatedStakeValidator.make[IO](SignedValidator.make[IO], None))
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      ((ref1, ref2), ctx) <- mkValidGlobalContext(kp, kp1, kp)
      parent1 <- Signed.forAsyncHasher(testCreateDelegatedStake(kp2, sourceAddress, 100L, ref1), kp)
      parent2 <- Signed.forAsyncHasher(testCreateDelegatedStake(kp1, sourceAddress, 100L, ref2), kp)
      lastRef1 <- DelegatedStakeReference.of(parent1)
      lastRef2 <- DelegatedStakeReference.of(parent2)
      context = ctx.copy(activeDelegatedStakes =
        Some(
          SortedMap(
            sourceAddress ->
              SortedSet(
                DelegatedStakeRecord(parent1, SnapshotOrdinal.MinValue, Amount(NonNegLong(0L)), None, None),
                DelegatedStakeRecord(parent2, SnapshotOrdinal.MinValue, Amount(NonNegLong(0L)), None, None)
              )
          )
        )
      )
      valid1 <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, lastRef1.hash), kp)
      valid2 <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, lastRef2.hash), kp)
      res <- acceptanceManager.accept(
        creates = List.empty,
        withdrawals = List(valid1, valid2),
        parentStateReader = pointReaderFromContextForTest(context),
        currentGlobalEpochProgress = EpochProgress.apply(NonNegLong(1)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2),
        acceptedTokenLocks = List.empty
      )
    } yield {
      val allAcceptedWithdrawals = res.acceptedWithdrawals.values.flatten.map(_._1).toSet
      expect.all(
        res.acceptedCreates.isEmpty,
        res.notAcceptedCreates.isEmpty,
        allAcceptedWithdrawals.size == 2,
        allAcceptedWithdrawals.contains(valid1),
        allAcceptedWithdrawals.contains(valid2),
        res.notAcceptedWithdrawals.isEmpty
      )
    }
  }

  test("uses strict parent MPT stake state for replacement validation") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res
    val tokenLockRef = Hash("ab" * 32)

    for {
      create <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L, tokenLockRef), kp)
      stakeRef <- DelegatedStakeReference.of(create)
      record = DelegatedStakeRecord(create, SnapshotOrdinal.MinValue, Amount.empty, None, None)
      reader <- activeDelegatedStakeReader(sourceAddress, SortedSet(record))
      validator = UpdateDelegatedStakeValidator.make[IO](SignedValidator.make[IO], None)
      acceptanceManager = UpdateDelegatedStakeAcceptanceManager.make[IO](validator)
      withdrawal <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, stakeRef.hash), kp)
      replacement <- Signed.forAsyncHasher(
        TokenLock(
          source = sourceAddress,
          amount = TokenLockAmount(PosLong.unsafeFrom(100L)),
          fee = TokenLockFee(NonNegLong(0L)),
          parent = TokenLockReference.empty,
          currencyId = None,
          unlockEpoch = None,
          replaceTokenLockRef = tokenLockRef.some
        ),
        kp
      )
      result <- acceptanceManager.accept(
        creates = List.empty,
        withdrawals = List(withdrawal),
        parentStateReader = reader,
        currentGlobalEpochProgress = EpochProgress(NonNegLong(1L)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List(replacement)
      )
    } yield
      expect.all(
        result.acceptedWithdrawals.isEmpty,
        result.notAcceptedWithdrawals.map(_._1) == List(withdrawal),
        result.notAcceptedWithdrawals.headOption.exists(
          _._2 == NonEmptyChain.of(UpdateDelegatedStakeValidator.OutdatedTokenLock(tokenLockRef))
        )
      )
  }

  test("same-ordinal sibling validation is bound to the supplied proposal-parent reader") { res =>
    implicit val (_, h, sp, kp, sourceAddress) = res

    for {
      create <- Signed.forAsyncHasher(testCreateDelegatedStake(kp, sourceAddress, 100L), kp)
      stakeRef <- DelegatedStakeReference.of(create)
      record = DelegatedStakeRecord(create, SnapshotOrdinal.MinValue, Amount.empty, None, None)
      siblingWithStake <- activeDelegatedStakeReader(sourceAddress, SortedSet(record))
      siblingWithoutStake = GlobalStateReader.empty[IO]
      withdrawal <- Signed.forAsyncHasher(testWithdrawDelegatedStake(sourceAddress, stakeRef.hash), kp)
      manager = UpdateDelegatedStakeAcceptanceManager.make[IO](
        UpdateDelegatedStakeValidator.make[IO](SignedValidator.make[IO], None)
      )
      accepted <- manager.accept(
        creates = List.empty,
        withdrawals = List(withdrawal),
        parentStateReader = siblingWithStake,
        currentGlobalEpochProgress = EpochProgress(NonNegLong(1L)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
      rejected <- manager.accept(
        creates = List.empty,
        withdrawals = List(withdrawal),
        parentStateReader = siblingWithoutStake,
        currentGlobalEpochProgress = EpochProgress(NonNegLong(1L)),
        currentSnapshotOrdinal = SnapshotOrdinal.unsafeApply(2L),
        acceptedTokenLocks = List.empty
      )
    } yield
      expect.all(
        accepted.acceptedWithdrawals.values.flatten.map(_._1).toList == List(withdrawal),
        accepted.notAcceptedWithdrawals.isEmpty,
        rejected.acceptedWithdrawals.isEmpty,
        rejected.notAcceptedWithdrawals.map(_._1) == List(withdrawal),
        rejected.notAcceptedWithdrawals.headOption.exists(_._2 == NonEmptyChain.of(InvalidStake(stakeRef.hash)))
      )
  }

  def testCreateDelegatedStake(
    keyPair: KeyPair,
    sourceAddress: Address,
    amount: Long,
    tokenLockReference: Hash = Hash.empty,
    parent: DelegatedStakeReference = DelegatedStakeReference.empty
  ): UpdateDelegatedStake.Create = UpdateDelegatedStake.Create(
    source = sourceAddress,
    nodeId = PeerId.fromPublic(keyPair.getPublic),
    amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
    tokenLockRef = tokenLockReference,
    parent = parent
  )

  def testWithdrawDelegatedStake(
    sourceAddress: Address,
    stakeRef: Hash = DelegatedStakeReference.empty.hash
  ): UpdateDelegatedStake.Withdraw = UpdateDelegatedStake.Withdraw(
    source = sourceAddress,
    stakeRef = stakeRef
  )

  def testTokenLock(
    keyPair: KeyPair,
    amount: Long,
    tokenLockUnlockEpoch: Option[EpochProgress] = None,
    parent: TokenLockReference = TokenLockReference.empty
  )(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO]
  ) = {
    val testTokenLock = TokenLock(
      source = keyPair.getPublic.toAddress,
      amount = TokenLockAmount(PosLong.unsafeFrom(amount)),
      fee = TokenLockFee(NonNegLong(0L)),
      parent = parent,
      currencyId = None,
      unlockEpoch = tokenLockUnlockEpoch,
      replaceTokenLockRef = None
    )
    for {
      signed <- forAsyncHasher(testTokenLock, keyPair)
      ref <- TokenLockReference.of(signed)
    } yield (ref, SortedMap(keyPair.getPublic.toAddress -> SortedSet(signed)))
  }

  def mkValidGlobalContext(
    keyPair1: KeyPair,
    keyPair2: KeyPair,
    tokenLockKeyPair: KeyPair,
    tokenLockUnlockEpoch: Option[EpochProgress] = None
  )(
    implicit sp: SecurityProvider[IO],
    h: Hasher[IO]
  ) =
    for {
      (ref1, tokenLocks1) <- testTokenLock(tokenLockKeyPair, 100L, tokenLockUnlockEpoch)
      (ref2, tokenLocks2) <- testTokenLock(tokenLockKeyPair, 200L, tokenLockUnlockEpoch, ref1)
      address1 = keyPair1.getPublic.toAddress
      nodeId1 = keyPair1.getPublic.toId
      address2 = keyPair2.getPublic.toAddress
      nodeId2 = keyPair2.getPublic.toId
      nodeParams = SortedMap(
        nodeId1 -> (
          Signed(
            UpdateNodeParameters(
              address1,
              delegatedStakeRewardParameters = DelegatedStakeRewardParameters(
                RewardFraction.unsafeFrom(80000000) // 80% to delegator
              ),
              NodeMetadataParameters("", ""),
              UpdateNodeParametersReference(UpdateNodeParametersOrdinal(NonNegLong.unsafeFrom(0)), Hash.empty)
            ),
            NonEmptySet.one[SignatureProof](SignatureProof(nodeId1, Signature(Hex(Hash.empty.value))))
          ),
          SnapshotOrdinal.unsafeApply(1L)
        ),
        nodeId2 -> (
          Signed(
            UpdateNodeParameters(
              address2,
              delegatedStakeRewardParameters = DelegatedStakeRewardParameters(
                RewardFraction.unsafeFrom(80000000) // 80% to delegator
              ),
              NodeMetadataParameters("", ""),
              UpdateNodeParametersReference(UpdateNodeParametersOrdinal(NonNegLong.unsafeFrom(0)), Hash.empty)
            ),
            NonEmptySet.one[SignatureProof](SignatureProof(nodeId2, Signature(Hex(Hash.empty.value))))
          ),
          SnapshotOrdinal.unsafeApply(1L)
        )
      )
      tokenLocks = tokenLocks1(address1) ++ tokenLocks2(address1)
    } yield ((ref1.hash, ref2.hash), mkGlobalContext(tokenLocks = SortedMap(address1 -> tokenLocks), nodeParams = nodeParams))

}
