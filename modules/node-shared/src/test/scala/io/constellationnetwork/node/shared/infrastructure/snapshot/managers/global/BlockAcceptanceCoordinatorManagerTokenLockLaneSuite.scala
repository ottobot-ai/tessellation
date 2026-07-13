package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair
import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.{IO, Ref}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosLong
import weaver.SimpleIOSuite

object BlockAcceptanceCoordinatorManagerTokenLockLaneSuite extends SimpleIOSuite {

  private def tokenLock(
    sourceKeyPair: KeyPair,
    amount: Long,
    currencyId: Option[CurrencyId],
    replacementRef: Option[Hash] = none
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLock]] =
    Signed.forAsyncHasher(
      TokenLock(
        sourceKeyPair.getPublic.toAddress,
        TokenLockAmount(PosLong.unsafeFrom(amount)),
        TokenLockFee(0L),
        TokenLockReference.empty,
        currencyId,
        EpochProgress(100L).some,
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
        } yield expect.all(
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
}
