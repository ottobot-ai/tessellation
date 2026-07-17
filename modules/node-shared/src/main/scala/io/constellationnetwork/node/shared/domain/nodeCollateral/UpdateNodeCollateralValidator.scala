package io.constellationnetwork.node.shared.domain.nodeCollateral

import cats.data.{NonEmptySet, ValidatedNec}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.syntax.validated._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralValidator.UpdateNodeCollateralValidationErrorOr
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.SignedValidator.SignedValidationError
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, SignedValidator}

import derevo.cats.{eqv, show}
import derevo.derive

trait UpdateNodeCollateralValidator[F[_]] {
  def validateCreateNodeCollateral(
    signed: Signed[UpdateNodeCollateral.Create],
    parentStateReader: GlobalStateReader[F]
  ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]]

  def validateWithdrawNodeCollateral(
    signed: Signed[UpdateNodeCollateral.Withdraw],
    parentStateReader: GlobalStateReader[F]
  ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]]
}

object UpdateNodeCollateralValidator {
  def make[F[_]: Async](
    signedValidator: SignedValidator[F],
    seedlist: Option[Set[SeedlistEntry]]
  )(
    implicit hasher: Hasher[F]
  ): UpdateNodeCollateralValidator[F] =
    new UpdateNodeCollateralValidator[F] {

      def validateCreateNodeCollateral(
        signed: Signed[UpdateNodeCollateral.Create],
        parentStateReader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
        for {
          numberOfSignaturesV <- validateNumberOfSignatures(signed)
          signaturesV <- signedValidator
            .validateSignatures(signed)
            .map(_.errorMap[UpdateNodeCollateralValidationError](InvalidSigned))
          isSignedExclusivelyBySource <- signedValidator
            .isSignedExclusivelyBy(signed, signed.source)
            .map(_.errorMap[UpdateNodeCollateralValidationError](InvalidSigned))
          authorizedNodeIdV = validateAuthorizedNodeId(signed)
          nodeIdV <- validateNodeId(signed, parentStateReader)
          parentV <- validateParent(signed, parentStateReader)
          tokenLockV <- validateTokenLock(signed, parentStateReader)
          pendingWithdrawalV <- validatePendingWithdrawal(signed, parentStateReader)
        } yield
          numberOfSignaturesV
            .productR(signaturesV)
            .productR(authorizedNodeIdV)
            .productR(isSignedExclusivelyBySource)
            .productR(nodeIdV)
            .productR(parentV)
            .productR(tokenLockV)
            .productR(pendingWithdrawalV)

      def validateWithdrawNodeCollateral(
        signed: Signed[UpdateNodeCollateral.Withdraw],
        parentStateReader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]] =
        for {
          numberOfSignaturesV <- validateNumberOfSignatures(signed)
          signaturesV <- signedValidator
            .validateSignatures(signed)
            .map(_.errorMap[UpdateNodeCollateralValidationError](InvalidSigned))
          isSignedExclusivelyBySource <- signedValidator
            .isSignedExclusivelyBy(signed, signed.source)
            .map(_.errorMap[UpdateNodeCollateralValidationError](InvalidSigned))
          withdrawV <- validateWithdrawal(signed, parentStateReader)
        } yield
          numberOfSignaturesV
            .productR(signaturesV)
            .productR(isSignedExclusivelyBySource)
            .productR(withdrawV)

      private def validateNumberOfSignatures[A <: UpdateNodeCollateral](
        signed: Signed[A]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[A]]] = {
        val result = if (signed.proofs.size == 1) {
          signed.validNec[UpdateNodeCollateralValidationError]
        } else {
          TooManySignatures(signed.proofs).invalidNec
        }
        result.pure[F]
      }

      private def validateNodeId(
        signed: Signed[UpdateNodeCollateral.Create],
        reader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
        reader.getNodeCollaterals(signed.source).map { maybeCollaterals =>
          val activeNodeCollaterals = maybeCollaterals.getOrElse(SortedSet.empty[NodeCollateralRecord]).toList
          if (activeNodeCollaterals.exists(s => s.event.nodeId == signed.nodeId)) {
            StakeExistsForNode(signed.nodeId).invalidNec
          } else {
            signed.validNec
          }
        }

      private def validateParent(
        signed: Signed[UpdateNodeCollateral.Create],
        reader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
        for {
          maybeCollaterals <- reader.getNodeCollaterals(signed.source)
          lastRef <- maybeCollaterals
            .getOrElse(SortedSet.empty[NodeCollateralRecord])
            .toList
            .sortBy(_.event.ordinal)
            .lastOption
            .traverse(collateral => NodeCollateralReference.of(collateral.event))
            .map(_.getOrElse(NodeCollateralReference.empty))
        } yield
          if (lastRef === signed.parent) {
            signed.validNec
          } else {
            InvalidParent(signed.parent).invalidNec
          }

      private def validateAuthorizedNodeId(
        signed: Signed[UpdateNodeCollateral.Create]
      ): UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]] =
        if (seedlist.forall(_.exists(_.peerId === signed.nodeId))) {
          signed.validNec
        } else {
          UnauthorizedNode(signed.nodeId).invalidNec
        }

      private def validatePendingWithdrawal(
        signed: Signed[UpdateNodeCollateral.Create],
        reader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] =
        reader.getNodeCollateralWithdrawals(signed.source).map { maybeWithdrawals =>
          val withdrawalRef = maybeWithdrawals
            .getOrElse(SortedSet.empty[PendingNodeCollateralWithdrawal])
            .find(w => w.event.tokenLockRef == signed.value.tokenLockRef)
          if (withdrawalRef.isEmpty) {
            signed.validNec
          } else {
            AlreadyWithdrawn(signed.parent.hash).invalidNec
          }
        }

      private def validateWithdrawal(
        signed: Signed[UpdateNodeCollateral.Withdraw],
        reader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]] = {

        def validateUniqueness(address: Address): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]] =
          reader.getNodeCollateralWithdrawals(address).flatMap { maybeWithdrawals =>
            val withdrawals = maybeWithdrawals.getOrElse(SortedSet.empty[PendingNodeCollateralWithdrawal])
            for {
              refs <- withdrawals.toList.traverse(w => NodeCollateralReference.of(w.event))
            } yield
              if (refs.exists(_.hash === signed.collateralRef)) {
                AlreadyWithdrawn(signed.collateralRef).invalidNec
              } else {
                signed.validNec
              }
          }

        def validateCreate(address: Address): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Withdraw]]] =
          reader.getNodeCollaterals(address).flatMap { maybeCollaterals =>
            getParent(maybeCollaterals.getOrElse(SortedSet.empty), signed).map {
              case Some(nodeCollateral) =>
                if (nodeCollateral.source =!= signed.source)
                  InvalidSourceAddress(signed.collateralRef).invalidNec
                else
                  signed.validNec
              case _ =>
                InvalidCollateral(signed.collateralRef).invalidNec
            }
          }

        for {
          parentV <- validateCreate(signed.source)
          uniqueV <- validateUniqueness(signed.source)
        } yield uniqueV.productR(parentV)
      }

      private def validateTokenLock(
        signed: Signed[UpdateNodeCollateral.Create],
        reader: GlobalStateReader[F]
      ): F[UpdateNodeCollateralValidationErrorOr[Signed[UpdateNodeCollateral.Create]]] = {

        def tokenLockAvailable(address: Address): F[Boolean] =
          for {
            maybeDelegatedStakes <- reader.getDelegatedStakes(signed.source)
            maybeNodeCollaterals <- reader.getNodeCollaterals(address)
          } yield {
            val maybeExistingStake = maybeDelegatedStakes
              .getOrElse(SortedSet.empty[DelegatedStakeRecord])
              .find(_.event.tokenLockRef === signed.tokenLockRef)
              .map(_.event)
            val maybeExistingCollateral = maybeNodeCollaterals
              .getOrElse(SortedSet.empty[NodeCollateralRecord])
              .find(_.event.tokenLockRef === signed.tokenLockRef)
              .map(_.event)
            // A collateral create may replace the prior record backed by the same lock, but it
            // cannot duplicate collateral for the same node or reuse a delegated-stake lock.
            maybeExistingStake.isEmpty && maybeExistingCollateral.forall(_.nodeId =!= signed.nodeId)
          }

        // Verify the token lock belongs to the signing address (address === tokenLock.source),
        // consistent with UpdateDelegatedStakeValidator.tokenLockValid.
        def tokenLockValid(address: Address): F[Boolean] =
          reader.getActiveTokenLocks(address).flatMap { maybeTokenLocks =>
            val tokenLocks = maybeTokenLocks.getOrElse(SortedSet.empty[Signed[TokenLock]])
            for {
              tokenLocksWithReferences <- tokenLocks.toList.traverse(t => TokenLockReference.of(t).map(r => (t, r)))
            } yield
              tokenLocksWithReferences.find { case (_, r) => r.hash === signed.tokenLockRef } match {
                case Some((tokenLock, _)) =>
                  signed.amount.value.value === tokenLock.amount.value.value &&
                  tokenLock.unlockEpoch.isEmpty &&
                  address === tokenLock.source
                case None => false
              }
          }

        for {
          available <- tokenLockAvailable(signed.source)
          valid <- if (available) tokenLockValid(signed.source) else available.pure[F]
        } yield if (valid) signed.validNec else InvalidTokenLock(signed.tokenLockRef).invalidNec
      }

      private def getParent(
        nodeCollaterals: SortedSet[NodeCollateralRecord],
        signed: Signed[UpdateNodeCollateral.Withdraw]
      ): F[Option[Signed[UpdateNodeCollateral.Create]]] =
        for {
          maybeParent <- nodeCollaterals.findM { s =>
            NodeCollateralReference.of(s.event).map(_.hash === signed.collateralRef)
          }
        } yield maybeParent.map(_.event)
    }

  @derive(eqv, show)
  sealed trait UpdateNodeCollateralValidationError

  case class InvalidSigned(error: SignedValidationError) extends UpdateNodeCollateralValidationError

  case class TooManySignatures(proofs: NonEmptySet[SignatureProof]) extends UpdateNodeCollateralValidationError

  case class StakeExistsForNode(peerId: PeerId) extends UpdateNodeCollateralValidationError

  case class UnauthorizedNode(peerId: PeerId) extends UpdateNodeCollateralValidationError

  case class InvalidCollateral(collateralRef: Hash) extends UpdateNodeCollateralValidationError

  case class InvalidSourceAddress(collateralRef: Hash) extends UpdateNodeCollateralValidationError

  case class InvalidTokenLock(tokenLockReference: Hash) extends UpdateNodeCollateralValidationError

  case class AlreadyWithdrawn(collateralRef: Hash) extends UpdateNodeCollateralValidationError

  case class InvalidParent(parent: NodeCollateralReference) extends UpdateNodeCollateralValidationError

  case class DuplicatedCreate(
    source: Address,
    nodeId: PeerId,
    tokenLockReference: Hash,
    parent: NodeCollateralReference
  ) extends UpdateNodeCollateralValidationError

  case class DuplicatedWithdrawal(source: Address, collateralRef: Hash) extends UpdateNodeCollateralValidationError

  case class DelegatedStakeTokenLockConflict(tokenLockReference: Hash) extends UpdateNodeCollateralValidationError

  type UpdateNodeCollateralValidationErrorOr[A] = ValidatedNec[UpdateNodeCollateralValidationError, A]
}
