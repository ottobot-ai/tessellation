package io.constellationnetwork.node.shared.domain.tokenlock.block

import cats.data.{EitherT, NonEmptyList}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.tokenlock.TokenLockChainValidator.TokenLockNel
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance._
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockBlock, TokenLockReference}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong

trait TokenLockBlockAcceptanceLogic[F[_]] {
  def acceptBlock(
    block: Signed[TokenLockBlock],
    txChains: Map[Address, TokenLockNel],
    context: TokenLockBlockAcceptanceContext[F],
    contextUpdate: TokenLockBlockAcceptanceContextUpdate,
    shouldPerformMetagraphSpecificValidations: Boolean
  )(implicit hasher: Hasher[F]): EitherT[F, TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]

}

object TokenLockBlockAcceptanceLogic {

  def make[F[_]: Async: SecurityProvider]: TokenLockBlockAcceptanceLogic[F] =
    new TokenLockBlockAcceptanceLogic[F] {

      def acceptBlock(
        signedBlock: Signed[TokenLockBlock],
        txChains: Map[Address, TokenLockNel],
        context: TokenLockBlockAcceptanceContext[F],
        contextUpdate: TokenLockBlockAcceptanceContextUpdate,
        shouldPerformMetagraphSpecificValidations: Boolean
      )(implicit hasher: Hasher[F]): EitherT[F, TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate] =
        for {
          _ <- processSignatures(signedBlock, context, shouldPerformMetagraphSpecificValidations)
          contextUpdate1 <- processLastTxRefs(txChains, context, contextUpdate)
          contextUpdate2 <- processBalances(signedBlock, context, contextUpdate1)
        } yield contextUpdate2

      def processLastTxRefs(
        txChains: Map[Address, TokenLockNel],
        context: TokenLockBlockAcceptanceContext[F],
        contextUpdate: TokenLockBlockAcceptanceContextUpdate
      )(implicit hasher: Hasher[F]): EitherT[F, TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate] =
        txChains.toList
          .foldLeft((contextUpdate.lastTokenLocksRefs, none[TokenLockBlockAwaitReason]).asRight[RejectedTokenLock].toEitherT[F]) {
            case (acc, (address, txChain)) =>
              acc.flatMap {
                case (lastTxRefsUpdate, maybeAwaitingBlock) =>
                  val rejectionOrUpdate
                    : F[Either[RejectedTokenLock, (Map[Address, TokenLockReference], Option[TokenLockBlockAwaitReason])]] =
                    for {
                      lastTxRef <- contextUpdate.lastTokenLocksRefs
                        .get(address)
                        .toOptionT[F]
                        .orElseF(context.getLastTxRef(address))
                        .getOrElse(context.getInitialTxRef)

                      headTxChainRef <- TokenLockReference.of(txChain.head)
                      lastTxChainRef <- TokenLockReference.of(txChain.last)

                      result =
                        if (txChain.head.parent.ordinal < lastTxRef.ordinal)
                          RejectedTokenLock(
                            headTxChainRef,
                            ParentOrdinalBelowLastTxOrdinal(txChain.head.parent.ordinal, lastTxRef.ordinal)
                          ).asLeft
                        else if (txChain.head.parent.ordinal > lastTxRef.ordinal)
                          (
                            lastTxRefsUpdate,
                            maybeAwaitingBlock.orElse(
                              AwaitingTokenLock(
                                headTxChainRef,
                                ParentOrdinalAboveLastTxOrdinal(txChain.head.parent.ordinal, lastTxRef.ordinal)
                              ).some
                            )
                          ).asRight
                        else if (txChain.head.parent.hash =!= lastTxRef.hash) // ordinals are equal
                          RejectedTokenLock(
                            headTxChainRef,
                            ParentHashNotEqLastTxHash(txChain.head.parent.hash, lastTxRef.hash)
                          ).asLeft
                        else // hashes and ordinals are equal
                          (lastTxRefsUpdate.updated(address, lastTxChainRef), maybeAwaitingBlock).asRight

                    } yield result

                  EitherT(rejectionOrUpdate)
              }
          }
          .leftWiden[TokenLockBlockNotAcceptedReason]
          .flatMap {
            case (update, maybeAwaitReason) =>
              maybeAwaitReason
                .widen[TokenLockBlockNotAcceptedReason]
                .toLeft(update)
                .toEitherT[F]
          }
          .map { lastTxRefsUpdate =>
            contextUpdate.copy(lastTokenLocksRefs = lastTxRefsUpdate)
          }

      private def processBalances(
        block: Signed[TokenLockBlock],
        context: TokenLockBlockAcceptanceContext[F],
        contextUpdate: TokenLockBlockAcceptanceContextUpdate
      )(implicit hasher: Hasher[F]): EitherT[F, TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate] = {
        val parentTokenLocksByHash = context.getToBeReplacedHashedTokenLocks.map(tl => tl.hash -> tl).toMap
        val sortedTxs = block.tokenLocks.toList.sortBy(tx => (tx.source, tx.ordinal, tx))

        def checkedBalance(value: BigInt): Either[BalanceArithmeticError, Balance] =
          if (value < 0) AmountUnderflow.asLeft
          else if (value > BigInt(Long.MaxValue)) AmountOverflow.asLeft
          else Balance(NonNegLong.unsafeFrom(value.longValue)).asRight

        sortedTxs.foldLeft(contextUpdate.asRight[TokenLockBlockNotAcceptedReason].toEitherT[F]) { (acc, signedTx) =>
          acc.flatMap { update =>
            val tx = signedTx.value

            val replacementOrError: F[Either[TokenLockBlockNotAcceptedReason, Option[Hashed[TokenLock]]]] = tx.replaceTokenLockRef match {
              case Some(replacementRef) if update.claimedReplacementRefs.contains(replacementRef) =>
                (ReplacementTokenLockAlreadyClaimed(replacementRef): TokenLockBlockNotAcceptedReason)
                  .asLeft[Option[Hashed[TokenLock]]]
                  .pure[F]
              case Some(replacementRef) =>
                TokenLockReference.of(signedTx).map { txRef =>
                  update.inRoundTokenLocksByHash.get(replacementRef).orElse(parentTokenLocksByHash.get(replacementRef)) match {
                    case None =>
                      AwaitingReplacementTokenLock(txRef, replacementRef).asLeft
                    case Some(existing) if existing.source =!= tx.source =>
                      RejectedReplacementTokenLock(
                        txRef,
                        replacementRef,
                        ReplacementSourceMismatch(tx.source, existing.source)
                      ).asLeft
                    case Some(existing) if existing.currencyId =!= tx.currencyId =>
                      RejectedReplacementTokenLock(
                        txRef,
                        replacementRef,
                        ReplacementCurrencyMismatch(tx.currencyId, existing.currencyId)
                      ).asLeft
                    case Some(existing) if existing.unlockEpoch.exists(_ < context.getCurrentEpochProgress) =>
                      RejectedReplacementTokenLock(
                        txRef,
                        replacementRef,
                        ReplacementTargetExpired(existing.unlockEpoch.get, context.getCurrentEpochProgress)
                      ).asLeft
                    case Some(existing) if existing.amount >= tx.amount =>
                      RejectedReplacementTokenLock(
                        txRef,
                        replacementRef,
                        ReplacementAmountNotIncreased(tx.amount, existing.amount)
                      ).asLeft
                    case Some(existing) => existing.some.asRight
                  }
                }
              case None => none[Hashed[TokenLock]].asRight[TokenLockBlockNotAcceptedReason].pure[F]
            }

            EitherT(replacementOrError).flatMap { maybeReplacement =>
              val balanceUpdate = update.balances
                .get(tx.source)
                .toOptionT[F]
                .orElseF(context.getBalance(tx.source))
                .getOrElse(Balance.empty)
                .map { balance =>
                  val replacementCredit = maybeReplacement.fold(BigInt(0))(replacement => BigInt(replacement.amount.value.value))
                  val nextValue =
                    BigInt(balance.value.value) + replacementCredit - BigInt(tx.amount.value.value) - BigInt(tx.fee.value.value)

                  checkedBalance(nextValue)
                    .leftMap(AddressBalanceOutOfRange(tx.source, _): TokenLockBlockNotAcceptedReason)
                    .map { nextBalance =>
                      update.copy(
                        balances = update.balances.updated(tx.source, nextBalance),
                        claimedReplacementRefs = update.claimedReplacementRefs ++ maybeReplacement.map(_.hash)
                      )
                    }
                }

              EitherT(balanceUpdate).semiflatMap { balanceUpdated =>
                if (tx.currencyId.isEmpty)
                  signedTx.toHashed.map { hashed =>
                    balanceUpdated.copy(inRoundTokenLocksByHash = balanceUpdated.inRoundTokenLocksByHash.updated(hashed.hash, hashed))
                  }
                else balanceUpdated.pure[F]
              }
            }
          }
        }
      }
    }

  def processSignatures[F[_]: Async: SecurityProvider](
    signedBlock: Signed[TokenLockBlock],
    context: TokenLockBlockAcceptanceContext[F],
    shouldPerformMetagraphSpecificValidations: Boolean = true
  ): EitherT[F, TokenLockBlockNotAcceptedReason, Unit] =
    EitherT(
      if (!shouldPerformMetagraphSpecificValidations) {
        ().asRight[TokenLockBlockNotAcceptedReason].pure
      } else {
        signedBlock.proofs
          .map(_.id.toPeerId)
          .toList
          .traverse(_.toAddress)
          .flatMap(
            _.filterA(address =>
              context.getBalance(address).map { balances =>
                !balances.getOrElse(Balance.empty).satisfiesCollateral(context.getCollateral)
              }
            )
          )
          .map(list =>
            NonEmptyList
              .fromList(list)
              .map(nel => SigningPeerBelowCollateral(nel).asLeft[Unit])
              .getOrElse(().asRight[TokenLockBlockNotAcceptedReason])
          )
      }
    )
}
