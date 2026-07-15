package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import java.util.UUID

import cats.data.NonEmptySet
import cats.effect.{IO, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencySnapshotContext, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import weaver.SimpleIOSuite

object BlockAcceptanceOpsManagerTokenLockLaneSuite extends SimpleIOSuite {

  test("ML0 passes only exact metagraph token locks without replacement markers to shared acceptance") {
    SecurityProvider.forAsync[IO].use { implicit securityProvider =>
      JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]

        def makeBlock(currencyId: Option[CurrencyId], replacementRef: Option[Hash], id: Long): IO[Signed[TokenLockBlock]] =
          for {
            sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
            blockKeyPair <- KeyPairGenerator.makeKeyPair[IO]
            tokenLock <- Signed.forAsyncHasher(
              TokenLock(
                sourceKeyPair.getPublic.toAddress,
                TokenLockAmount(10L),
                TokenLockFee(0L),
                TokenLockReference.empty,
                currencyId,
                EpochProgress(100L).some,
                replacementRef
              ),
              sourceKeyPair
            )
            block <- Signed.forAsyncHasher(
              TokenLockBlock(RoundId(new UUID(0L, id)), NonEmptySet.one(tokenLock)),
              blockKeyPair
            )
          } yield block

        for {
          metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          otherMetagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
          metagraphId = metagraphKeyPair.getPublic.toAddress
          expectedCurrencyId = CurrencyId(metagraphId)
          valid <- makeBlock(expectedCurrencyId.some, none, 1L)
          native <- makeBlock(none, none, 2L)
          wrongCurrency <- makeBlock(CurrencyId(otherMetagraphKeyPair.getPublic.toAddress).some, none, 3L)
          replacement <- makeBlock(expectedCurrencyId.some, Hash("ab" * 32).some, 4L)
          seen <- Ref.of[IO, List[Signed[TokenLockBlock]]](List.empty)
          seenEpoch <- Ref.of[IO, Option[EpochProgress]](none)
          tokenLockManager = new TokenLockBlockAcceptanceManager[IO] {
            def acceptBlocksIteratively(
              blocks: List[Signed[TokenLockBlock]],
              context: TokenLockBlockAcceptanceContext[IO],
              snapshotOrdinal: SnapshotOrdinal,
              shouldPerformMetagraphSpecificValidations: Boolean,
              lastGlobalSnapshotEpochProgress: Option[EpochProgress]
            )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
              seen.set(blocks) >> seenEpoch.set(lastGlobalSnapshotEpochProgress) >> TokenLockBlockAcceptanceResult(
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
            )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
              IO.raiseError(new AssertionError("single-block acceptance is not used by this test"))
          }
          manager = new BlockAcceptanceOpsManager[IO](null, tokenLockManager, null, Amount.empty)
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
          result <- manager.acceptTokenLockBlocks(
            List(native, replacement, valid, wrongCurrency),
            CurrencySnapshotContext(metagraphId, snapshotInfo),
            SnapshotOrdinal.MinValue,
            TokenLockReference.empty,
            shouldPerformMetagraphSpecificValidations = false,
            EpochProgress(42L)
          )
          passedToShared <- seen.get
          epochPassedToShared <- seenEpoch.get
          invalidBlocks = Set(native, wrongCurrency, replacement)
        } yield
          expect.all(
            passedToShared == List(valid),
            epochPassedToShared.contains(EpochProgress(42L)),
            result.accepted == List(valid),
            result.contextUpdate.balances.isEmpty,
            result.contextUpdate.lastTokenLocksRefs.isEmpty,
            result.notAccepted.map(_._1).toSet == invalidBlocks,
            result.notAccepted.forall {
              case (_, reason @ InvalidMetagraphTokenLockLane(`expectedCurrencyId`)) =>
                TokenLockBlockNotAcceptedReason.isPermanent(reason)
              case _ => false
            }
          )
      }
    }
  }
}
