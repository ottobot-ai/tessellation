package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair
import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.{IO, Ref}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosLong
import weaver.SimpleIOSuite

object BlockAcceptanceCoordinatorManagerAllowSpendLaneSuite extends SimpleIOSuite {

  private def allowSpend(
    sourceKeyPair: KeyPair,
    destination: io.constellationnetwork.schema.address.Address,
    amount: Long,
    currencyId: Option[CurrencyId]
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    Signed.forAsyncHasher(
      AllowSpend(
        source = sourceKeyPair.getPublic.toAddress,
        destination = destination,
        currencyId = currencyId,
        amount = SwapAmount(PosLong.unsafeFrom(amount)),
        fee = AllowSpendFee(0L),
        parent = AllowSpendReference.empty,
        lastValidEpochProgress = EpochProgress(100L),
        approvers = List(destination)
      ),
      sourceKeyPair
    )

  private def block(
    round: Long,
    allowSpends: NonEmptySet[Signed[AllowSpend]],
    blockKeyPair: KeyPair
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpendBlock]] =
    Signed.forAsyncHasher(AllowSpendBlock(RoundId(new UUID(0L, round)), allowSpends), blockKeyPair)

  test("GL0 forwards only all-native allow-spend blocks to shared acceptance") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          currencyKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          blockKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          currencyId = CurrencyId(currencyKeyPair.getPublic.toAddress)
          nativeTx <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 10L, none)
          currencyTx <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 11L, currencyId.some)
          validNative <- block(1L, NonEmptySet.one(nativeTx), blockKeyPair)
          wrongCurrency <- block(2L, NonEmptySet.one(currencyTx), blockKeyPair)
          mixed <- block(3L, NonEmptySet.of(nativeTx, currencyTx), blockKeyPair)
          seen <- Ref.of[IO, List[Signed[AllowSpendBlock]]](List.empty)
          allowSpendManager = new AllowSpendBlockAcceptanceManager[IO] {
            def acceptBlocksIteratively(
              blocks: List[Signed[AllowSpendBlock]],
              context: AllowSpendBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
              seen.set(blocks) *> AllowSpendBlockAcceptanceResult(
                AllowSpendBlockAcceptanceContextUpdate.empty,
                blocks,
                List.empty
              ).pure[IO]

            def acceptBlock(
              block: Signed[AllowSpendBlock],
              context: AllowSpendBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
              IO.raiseError(new AssertionError("single-block acceptance must not be used"))
          }
          manager = BlockAcceptanceCoordinatorManager.make[IO](
            blockAcceptanceManager = null,
            allowSpendBlockAcceptanceManager = allowSpendManager,
            tokenLockBlockAcceptanceManager = null,
            tipUsageManager = null,
            collateral = Amount.empty,
            reader = GlobalStateReader.empty[IO]
          )
          result <- manager.acceptAllowSpendBlocks(
            List(wrongCurrency, validNative, mixed),
            GlobalSnapshotInfo.empty,
            SnapshotOrdinal.MinValue,
            EpochProgress.MinValue
          )
          passedToShared <- seen.get
          expectedInvalid = List(wrongCurrency, mixed).sorted
        } yield
          expect.all(
            passedToShared == List(validNative),
            result.accepted == List(validNative),
            result.contextUpdate == AllowSpendBlockAcceptanceContextUpdate.empty,
            result.notAccepted.map(_._1) == expectedInvalid,
            result.notAccepted.forall { case (_, reason) => reason == InvalidGlobalAllowSpendLane },
            AllowSpendBlockNotAcceptedReason.isPermanent(InvalidGlobalAllowSpendLane)
          )
      }
    }
  }
}
