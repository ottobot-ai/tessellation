package io.constellationnetwork.node.shared.domain.tokenlock.block

import cats.data.NonEmptyList

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.BalanceArithmeticError
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock.{TokenLockAmount, TokenLockOrdinal, TokenLockReference}
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.derive

@derive(eqv, show)
sealed trait TokenLockBlockNotAcceptedReason

object TokenLockBlockNotAcceptedReason {

  /** Permanent rejections — chain has advanced past this token-lock's chain position; postponing causes the block-acceptance loop to
    * re-promote it from Postponed → Waiting and retry forever. Mirror of `BlockNotAcceptedReason.isPermanent`.
    */
  def isPermanent(reason: TokenLockBlockNotAcceptedReason): Boolean = reason match {
    case RejectedTokenLock(_, _: ParentOrdinalBelowLastTxOrdinal) => true
    case RejectedTokenLock(_, _: ParentHashNotEqLastTxHash)       => true
    case _: ReplacementTokenLockAlreadyClaimed                    => true
    case _: RejectedReplacementTokenLock                          => true
    case InvalidGlobalTokenLockLane                               => true
    case _: InvalidMetagraphTokenLockLane                         => true
    case _                                                        => false
  }
}

@derive(eqv, show)
sealed trait TokenLockBlockRejectionReason extends TokenLockBlockNotAcceptedReason

@derive(eqv, show)
case class ValidationFailed(reasons: NonEmptyList[TokenLockBlockValidationError]) extends TokenLockBlockRejectionReason

@derive(eqv, show)
case class RejectedTokenLock(tx: TokenLockReference, reason: TokenLockRejectionReason) extends TokenLockBlockRejectionReason

@derive(eqv, show)
sealed trait TokenLockBlockAwaitReason extends TokenLockBlockNotAcceptedReason

@derive(eqv, show)
case class AwaitingTokenLock(tx: TokenLockReference, reason: TokenLockAwaitReason) extends TokenLockBlockAwaitReason

@derive(eqv, show)
case class AwaitingReplacementTokenLock(tx: TokenLockReference, replaceTokenLockRef: Hash) extends TokenLockBlockAwaitReason

@derive(eqv, show)
case class AddressBalanceOutOfRange(address: Address, error: BalanceArithmeticError) extends TokenLockBlockAwaitReason

@derive(eqv, show)
case class SigningPeerBelowCollateral(peerIds: NonEmptyList[Address]) extends TokenLockBlockAwaitReason

@derive(eqv, show)
case class ReplacementTokenLockAlreadyClaimed(replaceTokenLockRef: Hash) extends TokenLockBlockRejectionReason

@derive(eqv, show)
case class RejectedReplacementTokenLock(
  tx: TokenLockReference,
  replaceTokenLockRef: Hash,
  reason: ReplacementTokenLockRejectionReason
) extends TokenLockBlockRejectionReason

@derive(eqv, show)
case object InvalidGlobalTokenLockLane extends TokenLockBlockRejectionReason

@derive(eqv, show)
case class InvalidMetagraphTokenLockLane(expectedCurrencyId: CurrencyId) extends TokenLockBlockRejectionReason

@derive(eqv, show)
sealed trait ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class ReplacementSourceMismatch(expectedSource: Address, actualSource: Address) extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class ReplacementCurrencyMismatch(
  replacementCurrencyId: Option[CurrencyId],
  targetCurrencyId: Option[CurrencyId]
) extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class ReplacementAmountNotIncreased(replacementAmount: TokenLockAmount, targetAmount: TokenLockAmount)
    extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class ReplacementTargetExpired(unlockEpoch: EpochProgress, currentEpochProgress: EpochProgress)
    extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case object CollateralBackingReplacementRejected extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case object PendingBackingReplacementRejected extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class SlashedDelegatedBackingReplacementUnsupported(effectiveAmount: Long, backingAmount: Long)
    extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case object BackingReplacementMustBeIndefinite extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class BackingReplacementSourceMismatch(expectedSource: Address, actualSource: Address) extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
case class BackingReplacementAmountShortfall(required: Long, actual: Long) extends ReplacementTokenLockRejectionReason

@derive(eqv, show)
sealed trait TokenLockAwaitReason

@derive(eqv, show)
case class ParentOrdinalAboveLastTxOrdinal(parentOrdinal: TokenLockOrdinal, lastTxOrdinal: TokenLockOrdinal) extends TokenLockAwaitReason

@derive(eqv, show)
sealed trait TokenLockRejectionReason

@derive(eqv, show)
case class ParentHashNotEqLastTxHash(parentHash: Hash, lastTxHash: Hash) extends TokenLockRejectionReason

@derive(eqv, show)
case class ParentOrdinalBelowLastTxOrdinal(parentOrdinal: TokenLockOrdinal, lastTxOrdinal: TokenLockOrdinal)
    extends TokenLockRejectionReason
