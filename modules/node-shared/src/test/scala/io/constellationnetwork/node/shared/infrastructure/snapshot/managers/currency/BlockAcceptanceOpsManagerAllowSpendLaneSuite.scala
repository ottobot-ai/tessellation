package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import java.security.KeyPair
import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.{IO, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshotContext, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosLong
import weaver.SimpleIOSuite

object BlockAcceptanceOpsManagerAllowSpendLaneSuite extends SimpleIOSuite {

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

  test("ML0 forwards only exact metagraph allow-spend blocks even when metagraph validations are disabled") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        for {
          metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          otherMetagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          blockKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          metagraphId = metagraphKeyPair.getPublic.toAddress
          expectedCurrencyId = CurrencyId(metagraphId)
          otherCurrencyId = CurrencyId(otherMetagraphKeyPair.getPublic.toAddress)
          validTx <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 10L, expectedCurrencyId.some)
          nativeTx <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 11L, none)
          wrongCurrencyTx <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 12L, otherCurrencyId.some)
          valid <- block(1L, NonEmptySet.one(validTx), blockKeyPair)
          native <- block(2L, NonEmptySet.one(nativeTx), blockKeyPair)
          wrongCurrency <- block(3L, NonEmptySet.one(wrongCurrencyTx), blockKeyPair)
          mixed <- block(4L, NonEmptySet.of(validTx, nativeTx), blockKeyPair)
          seen <- Ref.of[IO, List[Signed[AllowSpendBlock]]](List.empty)
          seenValidationFlag <- Ref.of[IO, Option[Boolean]](none)
          allowSpendManager = new AllowSpendBlockAcceptanceManager[IO] {
            def acceptBlocksIteratively(
              blocks: List[Signed[AllowSpendBlock]],
              context: AllowSpendBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
              seen.set(blocks) *> seenValidationFlag.set(shouldPerformMetagraphSpecificValidations.some) *>
                AllowSpendBlockAcceptanceResult(AllowSpendBlockAcceptanceContextUpdate.empty, blocks, List.empty).pure[IO]

            def acceptBlock(
              block: Signed[AllowSpendBlock],
              context: AllowSpendBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
              IO.raiseError(new AssertionError("single-block acceptance must not be used"))
          }
          manager = new BlockAcceptanceOpsManager[IO](null, null, allowSpendManager, Amount.empty)
          snapshotInfo = CurrencySnapshotInfo(
            SortedMap.empty,
            SortedMap.empty,
            none,
            none,
            none,
            none,
            none,
            none,
            none
          )
          result <- manager.acceptAllowSpendBlocks(
            List(native, wrongCurrency, valid, mixed),
            CurrencySnapshotContext(metagraphId, snapshotInfo),
            SnapshotOrdinal.MinValue,
            AllowSpendReference.empty,
            shouldPerformMetagraphSpecificValidations = false,
            EpochProgress(42L)
          )
          passedToShared <- seen.get
          validationFlag <- seenValidationFlag.get
          expectedInvalid = List(native, wrongCurrency, mixed).sorted
        } yield
          expect.all(
            BlockAcceptanceOpsManager.isValidMetagraphAllowSpendBlock(valid, expectedCurrencyId),
            !BlockAcceptanceOpsManager.isValidMetagraphAllowSpendBlock(native, expectedCurrencyId),
            !BlockAcceptanceOpsManager.isValidMetagraphAllowSpendBlock(wrongCurrency, expectedCurrencyId),
            !BlockAcceptanceOpsManager.isValidMetagraphAllowSpendBlock(mixed, expectedCurrencyId),
            passedToShared == List(valid),
            validationFlag.contains(false),
            result.accepted == List(valid),
            result.contextUpdate == AllowSpendBlockAcceptanceContextUpdate.empty,
            result.notAccepted.map(_._1) == expectedInvalid,
            result.notAccepted.forall {
              case (_, reason @ InvalidMetagraphAllowSpendLane(`expectedCurrencyId`)) =>
                AllowSpendBlockNotAcceptedReason.isPermanent(reason)
              case _ => false
            }
          )
      }
    }
  }
}
