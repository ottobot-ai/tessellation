package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.data.{NonEmptyChain, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator.{
  DuplicatedCreate,
  UpdateNodeCollateralValidationError
}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
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
      lastContext: GlobalSnapshotInfo
    ): IO[UpdateNodeCollateralValidator.UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
      signed.validNec[UpdateNodeCollateralValidationError].pure[IO]

    def validateWithdrawNodeCollateral(
      signed: Signed[UpdateNodeCollateral.Withdraw],
      lastContext: GlobalSnapshotInfo
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
        lastSnapshotContext = GlobalSnapshotInfo.empty,
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
}
