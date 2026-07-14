package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object BlockAcceptanceCoordinatorAllowSpendEpochSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val unusedBlockManager: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {
    def acceptBlocksIteratively(
      blocks: List[Signed[Block]],
      context: BlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean
    )(implicit hasher: Hasher[IO]): IO[BlockAcceptanceResult] =
      IO.raiseError(new AssertionError("block admission must not run"))

    def acceptBlock(
      block: Signed[Block],
      context: BlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean
    )(implicit hasher: Hasher[IO]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] =
      IO.raiseError(new AssertionError("block admission must not run"))
  }

  private val unusedTokenLockManager: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
    def acceptBlocksIteratively(
      blocks: List[Signed[TokenLockBlock]],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
      IO.raiseError(new AssertionError("token-lock admission must not run"))

    def acceptBlock(
      block: Signed[TokenLockBlock],
      context: TokenLockBlockAcceptanceContext[IO],
      snapshotOrdinal: SnapshotOrdinal,
      shouldPerformMetagraphSpecificValidations: Boolean,
      lastGlobalSnapshotEpochProgress: Option[EpochProgress]
    )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
      IO.raiseError(new AssertionError("token-lock admission must not run"))
  }

  test("GL0 allow-spend admission always receives the exact candidate epoch") { res =>
    implicit val hasher: Hasher[IO] = res._1
    for {
      observedEpoch <- Ref.of[IO, Option[EpochProgress]](None)
      allowSpendManager = new AllowSpendBlockAcceptanceManager[IO] {
        def acceptBlocksIteratively(
          blocks: List[Signed[AllowSpendBlock]],
          context: AllowSpendBlockAcceptanceContext[IO],
          snapshotOrdinal: SnapshotOrdinal,
          shouldPerformMetagraphSpecificValidations: Boolean,
          lastGlobalSnapshotEpochProgress: Option[EpochProgress]
        )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
          observedEpoch.set(lastGlobalSnapshotEpochProgress) *>
            AllowSpendBlockAcceptanceResult(AllowSpendBlockAcceptanceContextUpdate.empty, List.empty, List.empty).pure[IO]

        def acceptBlock(
          block: Signed[AllowSpendBlock],
          context: AllowSpendBlockAcceptanceContext[IO],
          snapshotOrdinal: SnapshotOrdinal,
          shouldPerformMetagraphSpecificValidations: Boolean,
          lastGlobalSnapshotEpochProgress: Option[EpochProgress]
        )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
          IO.raiseError(new AssertionError("single-block admission must not run"))
      }
      coordinator = BlockAcceptanceCoordinatorManager.make[IO](
        unusedBlockManager,
        allowSpendManager,
        unusedTokenLockManager,
        TipUsageManager.make[IO](),
        Amount.empty,
        GlobalStateReader.empty[IO]
      )
      exactEpoch = EpochProgress(NonNegLong.unsafeFrom(17L))
      _ <- coordinator.acceptAllowSpendBlocks(
        List.empty,
        GlobalSnapshotInfo.empty,
        SnapshotOrdinal.MinValue,
        exactEpoch
      )
      observed <- observedEpoch.get
    } yield expect(observed.contains(exactEpoch))
  }
}
