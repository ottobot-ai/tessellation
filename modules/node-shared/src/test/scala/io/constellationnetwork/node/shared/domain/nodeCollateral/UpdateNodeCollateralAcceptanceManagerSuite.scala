package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.Order
import cats.data.{NonEmptyChain, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

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
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object UpdateNodeCollateralAcceptanceManagerSuite extends MutableIOSuite {

  type Res = SecurityProvider[IO]

  override def sharedResource: Resource[IO, Res] = SecurityProvider.forAsync[IO]

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

  test("same-round duplicate creates cannot multiply one token lock into multiple collateral stake") { securityProvider =>
    implicit val sp: SecurityProvider[IO] = securityProvider

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
        )
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

  test("same-batch proof variants of one withdrawal accept exactly one canonical first event") { securityProvider =>
    implicit val sp: SecurityProvider[IO] = securityProvider

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
        )
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

  test("delegated-stake-conflicting canonical C1 does not reserve context against valid C2") { securityProvider =>
    implicit val sp: SecurityProvider[IO] = securityProvider

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
        updateDelegatedStakeAcceptanceResult = delegatedResult
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
