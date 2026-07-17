package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.Order
import cats.data.{NonEmptyChain, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeAmount, UpdateDelegatedStake}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object UpdateNodeCollateralAcceptanceManagerSuite extends MutableIOSuite {

  type Res = (SecurityProvider[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(jsonSerializer: JsonSerializer[IO]) <- Resource.eval(JsonSerializer.forAsync[IO])
      hasher = Hasher.forJson[IO]
    } yield securityProvider -> hasher

  private val acceptAll = new UpdateNodeCollateralValidator[IO] {
    def validateCreateNodeCollateral(
      signed: Signed[UpdateNodeCollateral.Create],
      parentStateReader: GlobalStateReader[IO]
    ): IO[UpdateNodeCollateralValidator.UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
      signed.validNec[UpdateNodeCollateralValidationError].pure[IO]

    def validateWithdrawNodeCollateral(
      signed: Signed[UpdateNodeCollateral.Withdraw],
      parentStateReader: GlobalStateReader[IO]
    ): IO[UpdateNodeCollateralValidator.UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]] =
      signed.validNec[UpdateNodeCollateralValidationError].pure[IO]
  }

  test("same-round duplicate creates cannot multiply one token lock into multiple collateral stake") {
    case (securityProvider, hasher) =>
      implicit val sp: SecurityProvider[IO] = securityProvider
      implicit val h: Hasher[IO] = hasher

      for {
        keyPair <- KeyPairGenerator.makeKeyPair[IO]
        secondNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        source = keyPair.getPublic.toAddress
        firstNodeId = PeerId.fromPublic(keyPair.getPublic)
        secondNodeId = PeerId.fromPublic(secondNodeKeyPair.getPublic)
        tokenLockRef = Hash("a" * 64)
        parent = NodeCollateralReference.empty
        firstCreate = UpdateNodeCollateral.Create(
          source,
          firstNodeId,
          NodeCollateralAmount(NonNegLong.unsafeFrom(100L)),
          NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
          tokenLockRef,
          parent
        )
        secondCreate = firstCreate.copy(
          nodeId = secondNodeId,
          fee = NodeCollateralFee(NonNegLong.unsafeFrom(1L))
        )
        signed1 = Signed(firstCreate, NonEmptySet.one(SignatureProof(firstNodeId.toId, Signature(Hex("0" * 64)))))
        signed2 = Signed(secondCreate, NonEmptySet.one(SignatureProof(secondNodeId.toId, Signature(Hex("1" * 64)))))
        manager = UpdateNodeCollateralAcceptanceManager.make[IO](acceptAll)
        result <- manager.accept(
          creates = List(signed1, signed2),
          withdrawals = List.empty,
          parentStateReader = GlobalStateReader.empty[IO],
          lastGlobalEpochProgress = EpochProgress.MinValue,
          lastSnapshotOrdinal = SnapshotOrdinal.MinValue,
          updateDelegatedStakeAcceptanceResult = UpdateDelegatedStakeAcceptanceResult(
            SortedMap.empty,
            List.empty,
            SortedMap.empty,
            List.empty
          ),
          acceptedTokenLocks = List.empty
        )
      } yield {
        val accepted = result.acceptedCreates.values.flatten.map(_._1).toList
        val acceptedStake = accepted.map(_.amount.value.value).sum
        val rejected = result.notAcceptedCreates.head
        expect.all(
          firstCreate != secondCreate,
          signed1 != signed2,
          accepted.size == 1,
          acceptedStake == 100L,
          result.notAcceptedCreates.size == 1,
          rejected._2 == NonEmptyChain.of(DuplicatedCreate(source, rejected._1.nodeId, tokenLockRef, parent)),
          result.acceptedWithdrawals.isEmpty,
          result.notAcceptedWithdrawals.isEmpty
        )
      }
  }

  test("same-batch proof variants of one withdrawal accept exactly one canonical first event") {
    case (securityProvider, hasher) =>
      implicit val sp: SecurityProvider[IO] = securityProvider
      implicit val h: Hasher[IO] = hasher

      for {
        keyPair <- KeyPairGenerator.makeKeyPair[IO]
        source = keyPair.getPublic.toAddress
        nodeId = PeerId.fromPublic(keyPair.getPublic)
        collateralRef = Hash("b" * 64)
        withdrawal = UpdateNodeCollateral.Withdraw(source, collateralRef)
        signed1 = Signed(withdrawal, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("2" * 64)))))
        signed2 = Signed(withdrawal, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("3" * 64)))))
        canonical = List(signed1, signed2).sorted(Signed.ordering(Order[UpdateNodeCollateral.Withdraw].toOrdering))
        manager = UpdateNodeCollateralAcceptanceManager.make[IO](acceptAll)
        result <- manager.accept(
          creates = List.empty,
          withdrawals = List(signed2, signed1),
          parentStateReader = GlobalStateReader.empty[IO],
          lastGlobalEpochProgress = EpochProgress.MinValue,
          lastSnapshotOrdinal = SnapshotOrdinal.MinValue,
          updateDelegatedStakeAcceptanceResult = UpdateDelegatedStakeAcceptanceResult(
            SortedMap.empty,
            List.empty,
            SortedMap.empty,
            List.empty
          ),
          acceptedTokenLocks = List.empty
        )
      } yield {
        val accepted = result.acceptedWithdrawals.values.flatten.map(_._1).toList
        val rejected = result.notAcceptedWithdrawals

        expect.all(
          signed1 =!= signed2,
          accepted == List(canonical.head),
          rejected.map(_._1) == List(canonical.last),
          rejected.head._2 == NonEmptyChain.of(DuplicatedWithdrawal(source, collateralRef))
        )
      }
  }

  test("same-batch collateral successor create rejects withdrawal of its predecessor") {
    case (securityProvider, hasher) =>
      implicit val sp: SecurityProvider[IO] = securityProvider
      implicit val h: Hasher[IO] = hasher

      for {
        keyPair <- KeyPairGenerator.makeKeyPair[IO]
        successorNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        source = keyPair.getPublic.toAddress
        nodeId = PeerId.fromPublic(successorNodeKeyPair.getPublic)
        predecessorRef = Hash("e" * 64)
        backingRef = Hash("f" * 64)
        create = UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong.unsafeFrom(100L)),
          NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
          backingRef,
          NodeCollateralReference(NodeCollateralOrdinal.first, predecessorRef)
        )
        withdrawal = UpdateNodeCollateral.Withdraw(source, predecessorRef)
        signedCreate = Signed(create, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("7" * 64)))))
        signedWithdrawal = Signed(
          withdrawal,
          NonEmptySet.one(SignatureProof(PeerId.fromPublic(keyPair.getPublic).toId, Signature(Hex("8" * 64))))
        )
        manager = UpdateNodeCollateralAcceptanceManager.make[IO](acceptAll)
        result <- manager.accept(
          creates = List(signedCreate),
          withdrawals = List(signedWithdrawal),
          parentStateReader = GlobalStateReader.empty[IO],
          lastGlobalEpochProgress = EpochProgress.MinValue,
          lastSnapshotOrdinal = SnapshotOrdinal.MinValue,
          updateDelegatedStakeAcceptanceResult = UpdateDelegatedStakeAcceptanceResult(
            SortedMap.empty,
            List.empty,
            SortedMap.empty,
            List.empty
          ),
          acceptedTokenLocks = List.empty
        )
      } yield
        expect.same(List(signedCreate), result.acceptedCreates.values.flatten.map(_._1).toList) &&
          expect(result.acceptedWithdrawals.isEmpty) &&
          expect.same(List(signedWithdrawal), result.notAcceptedWithdrawals.map(_._1)) &&
          expect.same(
            NonEmptyChain.of(ConflictingCollateralTransition(predecessorRef)),
            result.notAcceptedWithdrawals.head._2
          )
  }

  test("same-batch create reusing a non-latest collateral lock rejects withdrawal of that lock owner") {
    case (securityProvider, hasher) =>
      implicit val sp: SecurityProvider[IO] = securityProvider
      implicit val h: Hasher[IO] = hasher

      for {
        sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        firstNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        latestNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        replacementNodeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        source = sourceKeyPair.getPublic.toAddress
        firstNode = PeerId.fromPublic(firstNodeKeyPair.getPublic)
        latestNode = PeerId.fromPublic(latestNodeKeyPair.getPublic)
        replacementNode = PeerId.fromPublic(replacementNodeKeyPair.getPublic)
        reusedLock = Hash("9" * 64)
        latestLock = Hash("a" * 64)
        first = UpdateNodeCollateral.Create(
          source,
          firstNode,
          NodeCollateralAmount(NonNegLong.unsafeFrom(100L)),
          NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
          reusedLock,
          NodeCollateralReference.empty
        )
        signedFirst = Signed(first, NonEmptySet.one(SignatureProof(firstNode.toId, Signature(Hex("9" * 64)))))
        firstRef <- NodeCollateralReference.of(signedFirst)
        latest = first.copy(nodeId = latestNode, tokenLockRef = latestLock, parent = firstRef)
        signedLatest = Signed(latest, NonEmptySet.one(SignatureProof(latestNode.toId, Signature(Hex("a" * 64)))))
        latestRef <- NodeCollateralReference.of(signedLatest)
        replacement = first.copy(nodeId = replacementNode, parent = latestRef)
        signedReplacement = Signed(
          replacement,
          NonEmptySet.one(SignatureProof(replacementNode.toId, Signature(Hex("b" * 64))))
        )
        withdrawal = UpdateNodeCollateral.Withdraw(source, firstRef.hash)
        signedWithdrawal = Signed(
          withdrawal,
          NonEmptySet.one(SignatureProof(PeerId.fromPublic(sourceKeyPair.getPublic).toId, Signature(Hex("c" * 64))))
        )
        context = UpdateNodeCollateralValidatorSuite.mkGlobalContext(
          SortedMap(
            source -> SortedSet(
              NodeCollateralRecord(signedFirst, SnapshotOrdinal.MinValue),
              NodeCollateralRecord(signedLatest, SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
            )
          )
        )
        manager = UpdateNodeCollateralAcceptanceManager.make[IO](acceptAll)
        result <- manager.accept(
          creates = List(signedReplacement),
          withdrawals = List(signedWithdrawal),
          parentStateReader = UpdateNodeCollateralValidatorSuite.pointReaderFromContextForTest(context),
          lastGlobalEpochProgress = EpochProgress.MinValue,
          lastSnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(2L)),
          updateDelegatedStakeAcceptanceResult = UpdateDelegatedStakeAcceptanceResult(
            SortedMap.empty,
            List.empty,
            SortedMap.empty,
            List.empty
          ),
          acceptedTokenLocks = List.empty
        )
      } yield
        expect.same(List(signedReplacement), result.acceptedCreates.values.flatten.map(_._1).toList) &&
          expect(result.acceptedWithdrawals.isEmpty) &&
          expect.same(List(signedWithdrawal), result.notAcceptedWithdrawals.map(_._1)) &&
          expect.same(
            NonEmptyChain.of(ConflictingCollateralTransition(firstRef.hash)),
            result.notAcceptedWithdrawals.head._2
          )
  }

  test("delegated-stake-conflicting canonical C1 does not reserve context against valid C2") {
    case (securityProvider, hasher) =>
      implicit val sp: SecurityProvider[IO] = securityProvider
      implicit val h: Hasher[IO] = hasher

      for {
        keyPair <- KeyPairGenerator.makeKeyPair[IO]
        source = keyPair.getPublic.toAddress
        nodeId = PeerId.fromPublic(keyPair.getPublic)
        parent = NodeCollateralReference.empty
        createA = UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong.unsafeFrom(100L)),
          NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
          Hash("c" * 64),
          parent
        )
        createB = createA.copy(
          fee = NodeCollateralFee(NonNegLong.unsafeFrom(1L)),
          tokenLockRef = Hash("d" * 64)
        )
        signedA = Signed(createA, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("4" * 64)))))
        signedB = Signed(createB, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("5" * 64)))))
        canonical = List(signedA, signedB).sorted(Signed.ordering(Order[UpdateNodeCollateral.Create].toOrdering))
        conflicting = canonical.head
        valid = canonical.last
        delegatedCreate = UpdateDelegatedStake.Create(
          source = source,
          nodeId = nodeId,
          amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(100L)),
          tokenLockRef = conflicting.tokenLockRef
        )
        signedDelegated =
          Signed(delegatedCreate, NonEmptySet.one(SignatureProof(nodeId.toId, Signature(Hex("6" * 64)))))
        delegatedResult = UpdateDelegatedStakeAcceptanceResult(
          SortedMap(source -> List(signedDelegated -> SnapshotOrdinal.MinValue)),
          List.empty,
          SortedMap.empty,
          List.empty
        )
        manager = UpdateNodeCollateralAcceptanceManager.make[IO](acceptAll)
        result <- manager.accept(
          creates = List(valid, conflicting),
          withdrawals = List.empty,
          parentStateReader = GlobalStateReader.empty[IO],
          lastGlobalEpochProgress = EpochProgress.MinValue,
          lastSnapshotOrdinal = SnapshotOrdinal.MinValue,
          updateDelegatedStakeAcceptanceResult = delegatedResult,
          acceptedTokenLocks = List.empty
        )
      } yield {
        val accepted = result.acceptedCreates.values.flatten.map(_._1).toList

        expect.all(
          accepted == List(valid),
          result.notAcceptedCreates.map(_._1) == List(conflicting),
          result.notAcceptedCreates.headOption.exists(
            _._2 == NonEmptyChain.of(DelegatedStakeTokenLockConflict(conflicting.tokenLockRef))
          )
        )
      }
  }
}
