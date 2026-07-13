package io.constellationnetwork.dag.l1.domain.tokenlock.block

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.control.NoStackTrace

import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.dag.l1.domain.address.storage.AddressStorage
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.tokenlock.TokenLockStorage
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockBlock, TokenLockReference}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.ProofsHash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

trait TokenLockBlockService[F[_]] {
  def accept(signedBlock: Signed[TokenLockBlock], snapshotOrdinal: SnapshotOrdinal)(
    implicit hasher: Hasher[F]
  ): F[Unit]
}

object TokenLockBlockService {

  private[block] def replacementCandidates[F[_]: Async](
    block: Signed[TokenLockBlock],
    activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit hasher: Hasher[F]): F[List[Hashed[TokenLock]]] = {
    val replacementRefs = block.tokenLocks.toList.flatMap(_.replaceTokenLockRef).toSet

    if (replacementRefs.isEmpty) List.empty[Hashed[TokenLock]].pure[F]
    else
      activeTokenLocks.values.toList
        .flatMap(_.toList)
        .traverse(_.toHashed)
        .map(_.filter(lock => replacementRefs.contains(lock.hash)).sortBy(_.hash))
  }

  def make[F[_]: Async, P <: StateProof, S <: Snapshot, SI <: SnapshotInfo[P]](
    tokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[F],
    addressStorage: AddressStorage[F],
    tokenLockBlockStorage: TokenLockBlockStorage[F],
    tokenLockStorage: TokenLockStorage[F],
    collateral: Amount,
    lastSnapshotStorage: LastSnapshotStorage[F, S, SI] with LatestBalances[F]
  ): TokenLockBlockService[F] =
    new TokenLockBlockService[F] {

      def accept(signedBlock: Signed[TokenLockBlock], snapshotOrdinal: SnapshotOrdinal)(
        implicit hasher: Hasher[F]
      ): F[Unit] =
        for {
          combined <- lastSnapshotStorage.getCombined
          lastGlobalEpochProgress = combined match {
            case Some((snapshot, _)) =>
              snapshot.signed.value match {
                case cis: CurrencyIncrementalSnapshot =>
                  cis.globalSyncView.map(_.epochProgress).getOrElse(EpochProgress.MinValue)
                case gis: GlobalIncrementalSnapshot =>
                  gis.epochProgress
                case _ =>
                  EpochProgress.MinValue
              }
            case None =>
              EpochProgress.MinValue
          }
          candidates <- replacementCandidates(
            signedBlock,
            combined.fold(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])(_._2.getActiveTokenLocks)
          )
          result <- signedBlock.toHashed.flatMap { hashedBlock =>
            EitherT(
              tokenLockBlockAcceptanceManager
                .acceptBlock(
                  signedBlock,
                  context(candidates, lastGlobalEpochProgress),
                  snapshotOrdinal,
                  shouldPerformMetagraphSpecificValidations = true,
                  lastGlobalEpochProgress.some
                )
            )
              .leftSemiflatMap(processAcceptanceError(hashedBlock))
              .semiflatMap(processAcceptanceSuccess(hashedBlock))
              .rethrowT
          }
        } yield result

      private def context(
        replacementCandidates: List[Hashed[TokenLock]],
        currentEpochProgress: EpochProgress
      ): TokenLockBlockAcceptanceContext[F] =
        new TokenLockBlockAcceptanceContext[F] {

          def getBalance(address: Address): F[Option[Balance]] =
            addressStorage.getBalance(address).map(_.some)

          def getLastTxRef(address: Address): F[Option[TokenLockReference]] =
            tokenLockStorage.getLastProcessedTokenLock(address).map(_.ref.some)

          def getInitialTxRef: TokenLockReference =
            tokenLockStorage.getInitialTx.ref

          def getCollateral: Amount = collateral

          def getCurrentEpochProgress: EpochProgress = currentEpochProgress

          def getToBeReplacedHashedTokenLocks: List[Hashed[TokenLock]] = replacementCandidates
        }

      private def processAcceptanceSuccess(
        hashedBlock: Hashed[TokenLockBlock]
      )(contextUpdate: TokenLockBlockAcceptanceContextUpdate)(implicit hasher: Hasher[F]): F[Unit] =
        for {
          hashedTransactions <- hashedBlock.signed.tokenLocks.toNonEmptyList
            .sortBy(_.ordinal)
            .traverse(_.toHashed)

          _ <- hashedTransactions.traverse(tokenLockStorage.accept)
          _ <- tokenLockBlockStorage.accept(hashedBlock)
          _ <- addressStorage.updateBalances(contextUpdate.balances)
        } yield ()

      private def processAcceptanceError(
        hashedBlock: Hashed[TokenLockBlock]
      )(reason: TokenLockBlockNotAcceptedReason): F[TokenLockBlockAcceptanceError] = {
        val handle =
          if (TokenLockBlockNotAcceptedReason.isPermanent(reason)) tokenLockBlockStorage.dropWaiting(hashedBlock.proofsHash)
          else tokenLockBlockStorage.postpone(hashedBlock)
        handle.as(TokenLockBlockAcceptanceError(hashedBlock.proofsHash, reason))
      }

    }

  case class TokenLockBlockAcceptanceError(blockReference: ProofsHash, reason: TokenLockBlockNotAcceptedReason) extends NoStackTrace {
    override def getMessage: String = s"Block ${blockReference.show} could not be accepted ${reason.show}"
  }
}
