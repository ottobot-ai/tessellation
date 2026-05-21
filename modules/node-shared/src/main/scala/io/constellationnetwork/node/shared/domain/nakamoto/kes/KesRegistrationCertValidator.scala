package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.data.{NonEmptySet, ValidatedNec}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.syntax.validated._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.signature.SignedValidator.SignedValidationError
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import derevo.cats.{eqv, show}
import derevo.derive

/** Validator for [[KesRegistrationCert]] — §1.2 Slice 10 (#179) runtime KES master-VK registration certs.
  *
  * Rejection paths (validator is fail-closed across all of them; each is exercised in `KesRegistrationCertValidatorSuite`):
  *
  *   1. '''InvalidSigned''' — envelope signature does not verify under the operator's long-term key, OR the cert is signed by more than one
  *      party. Delegated to [[SignedValidator]] (`validateSignatures` + `isSignedExclusivelyBy` against the operator's address).
  *   1. '''TooManySignatures''' — the `Signed` envelope carries more than one proof. Reused from the node-collateral pattern.
  *   1. '''UnauthorizedOperator''' — the cert's `operatorPeerId` is not in the L0 seedlist (or there is no seedlist at all, in which case
  *      we accept). Same gate the node-collateral validator uses.
  *   1. '''SignerOperatorMismatch''' — the `Signed` proof was made by a key that does not match `operatorPeerId`. Operator self-binds: the
  *      cert is identifying its subject, and that subject must be the one signing it.
  *   1. '''NonMonotonicOrdinal''' — the cert's `ordinal` is `<= lastSeenOrdinal` for this operator. Replay / out-of-order delivery is
  *      rejected. The first cert from a fresh operator must use `KesRegistrationOrdinal.first`.
  *   1. '''InvalidParent''' — the cert's `parent` reference does not match the last accepted cert for this operator. Mirrors the
  *      `NodeCollateralValidator.validateParent` chain-link check.
  *   1. '''NotForwardActivation''' — `effectiveFromEpoch <= currentEpoch`. Retroactive registration would skip the N-2-style staggering
  *      window receivers need to observe finality before a new VK becomes load-bearing.
  *   1. '''NonMonotonicEffectiveFromEpoch''' — the cert's `effectiveFromEpoch` is `<= lastSeen.effectiveFromEpoch` for this operator.
  *      Closes the Risk-5 lookup hole: without this check, a cert with a LOWER `effectiveFromEpoch` could be accepted at a higher ordinal,
  *      meaning the pointer-only lookup in `MutableKesRegistry.getKesVk` would falsely fall through to genesis when an earlier (lower-
  *      ordinal) cert IS active at `currentEpoch`. By enforcing strict monotonicity, the latest accepted cert is guaranteed to have the
  *      highest `effectiveFromEpoch`, so pointer-only lookup is correct: if the pointer's cert is effective, return it; if not, the genesis
  *      fallback is unambiguous.
  *   1. '''MalformedVk''' — the `kesMasterVK` bytes are empty, or `kesMasterVKStep < 0`, or `offset < 0`. Cryptographic well-formedness of
  *      the bytes (that they form a valid super × sub Merkle root) is intentionally not checked here — that's the verifier-side check at
  *      sig-presentation time. We catch only the obvious structural breakage.
  */
trait KesRegistrationCertValidator[F[_]] {

  /** Validate a new cert against the operator's last-seen state.
    *
    * @param signed
    *   the candidate cert wrapped in its long-term-key signature
    * @param lastRef
    *   the previously-accepted [[KesRegistrationReference]] for this operator, or [[KesRegistrationReference.empty]] for a fresh operator
    * @param lastEffectiveFromEpoch
    *   the `effectiveFromEpoch` of the previously-accepted cert for this operator. Used by the monotonic-effective-epoch check (Risk 5).
    *   `EpochProgress(0)` for a fresh operator (matches `KesRegistrationReference.empty`'s implicit zero baseline).
    * @param currentEpoch
    *   the snapshot epoch the cert is being evaluated against
    */
  def validate(
    signed: Signed[KesRegistrationCert],
    lastRef: KesRegistrationReference,
    lastEffectiveFromEpoch: EpochProgress,
    currentEpoch: EpochProgress
  ): F[KesRegistrationCertValidator.KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]]
}

object KesRegistrationCertValidator {

  /** Validator that always rejects. Used in modules that don't carry the long-term seedlist needed to verify the operator binding (mirrors
    * the `UpdateNodeCollateralValidator.rejectAll` fallback).
    */
  def rejectAll[F[_]: Async]: KesRegistrationCertValidator[F] = new KesRegistrationCertValidator[F] {
    def validate(
      signed: Signed[KesRegistrationCert],
      lastRef: KesRegistrationReference,
      lastEffectiveFromEpoch: EpochProgress,
      currentEpoch: EpochProgress
    ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] =
      (Rejected: KesRegistrationCertValidationError).invalidNec[Signed[KesRegistrationCert]].pure[F]
  }

  def make[F[_]: Async: SecurityProvider](
    signedValidator: SignedValidator[F],
    l0Seedlist: Option[Set[SeedlistEntry]]
  )(implicit hasher: Hasher[F]): KesRegistrationCertValidator[F] =
    new KesRegistrationCertValidator[F] {

      def validate(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference,
        lastEffectiveFromEpoch: EpochProgress,
        currentEpoch: EpochProgress
      ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] =
        for {
          numberOfSignaturesV <- validateNumberOfSignatures(signed)
          signaturesV <- signedValidator
            .validateSignatures(signed)
            .map(_.errorMap[KesRegistrationCertValidationError](InvalidSigned))
          signerMatchV <- validateSignerMatchesOperator(signed)
          authorizedV = validateAuthorizedOperator(signed)
          monotonicV = validateOrdinalMonotonic(signed, lastRef)
          parentV = validateParentLink(signed, lastRef)
          activationV = validateForwardActivation(signed, currentEpoch)
          monotonicEpochV = validateMonotonicEffectiveFromEpoch(signed, lastRef, lastEffectiveFromEpoch)
          wellFormedV = validateVkWellFormedness(signed)
        } yield
          numberOfSignaturesV
            .productR(signaturesV)
            .productR(signerMatchV)
            .productR(authorizedV)
            .productR(monotonicV)
            .productR(parentV)
            .productR(activationV)
            .productR(monotonicEpochV)
            .productR(wellFormedV)

      private def validateNumberOfSignatures(
        signed: Signed[KesRegistrationCert]
      ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] = {
        val result =
          if (signed.proofs.size == 1) signed.validNec[KesRegistrationCertValidationError]
          else TooManySignatures(signed.proofs).invalidNec
        result.pure[F]
      }

      private def validateSignerMatchesOperator(
        signed: Signed[KesRegistrationCert]
      ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] = {
        val signerPeerIds: Set[PeerId] = signed.proofs.toSortedSet.toList.map(p => PeerId.fromId(p.id)).toSet
        val operator = signed.value.operatorPeerId
        val result =
          if (signerPeerIds.contains(operator) && signerPeerIds.sizeIs == 1) signed.validNec[KesRegistrationCertValidationError]
          else SignerOperatorMismatch(operator, signerPeerIds).invalidNec
        result.pure[F]
      }

      private def validateAuthorizedOperator(
        signed: Signed[KesRegistrationCert]
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (l0Seedlist.forall(_.exists(_.peerId === signed.value.operatorPeerId))) signed.validNec
        else UnauthorizedOperator(signed.value.operatorPeerId).invalidNec

      private def validateOrdinalMonotonic(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (signed.value.ordinal > lastRef.ordinal) signed.validNec
        else NonMonotonicOrdinal(signed.value.ordinal, lastRef.ordinal).invalidNec

      private def validateParentLink(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (signed.value.parent === lastRef) signed.validNec
        else InvalidParent(signed.value.parent).invalidNec

      private def validateForwardActivation(
        signed: Signed[KesRegistrationCert],
        currentEpoch: EpochProgress
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (signed.value.effectiveFromEpoch > currentEpoch) signed.validNec
        else NotForwardActivation(signed.value.effectiveFromEpoch, currentEpoch).invalidNec

      /** Risk-5 fix: enforce that `effectiveFromEpoch` is strictly increasing across the operator's per-peer cert chain. Skipped when this
        * is the operator's first cert (`lastRef == KesRegistrationReference.empty`) — no prior effectiveFromEpoch to compare against.
        * Closes the bug where the pointer-only lookup in [[MutableKesRegistry.getKesVk]] would fall through to genesis when the latest
        * pointer's cert was pending (eff > current) but an earlier (lower-ordinal) cert was active. Mirrors the per-peer
        * `validateOrdinalMonotonic` shape.
        */
      private def validateMonotonicEffectiveFromEpoch(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference,
        lastEffectiveFromEpoch: EpochProgress
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (lastRef === KesRegistrationReference.empty) signed.validNec
        else if (signed.value.effectiveFromEpoch > lastEffectiveFromEpoch) signed.validNec
        else
          NonMonotonicEffectiveFromEpoch(
            signed.value.ordinal,
            signed.value.effectiveFromEpoch,
            lastRef.ordinal,
            lastEffectiveFromEpoch
          ).invalidNec

      private def validateVkWellFormedness(
        signed: Signed[KesRegistrationCert]
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] = {
        val cert = signed.value
        val vkBytes = scala.util.Try(cert.kesMasterVK.toBytes).toOption.getOrElse(Array.emptyByteArray)
        if (vkBytes.nonEmpty && cert.kesMasterVKStep >= 0 && cert.offset >= 0) signed.validNec
        else MalformedVk(cert.kesMasterVK).invalidNec
      }
    }

  @derive(eqv, show)
  sealed trait KesRegistrationCertValidationError

  case class InvalidSigned(error: SignedValidationError) extends KesRegistrationCertValidationError

  case class TooManySignatures(proofs: NonEmptySet[SignatureProof]) extends KesRegistrationCertValidationError

  case class UnauthorizedOperator(operatorPeerId: PeerId) extends KesRegistrationCertValidationError

  case class SignerOperatorMismatch(operatorPeerId: PeerId, signerPeerIds: Set[PeerId]) extends KesRegistrationCertValidationError

  case class NonMonotonicOrdinal(certOrdinal: KesRegistrationOrdinal, lastOrdinal: KesRegistrationOrdinal)
      extends KesRegistrationCertValidationError

  case class InvalidParent(parent: KesRegistrationReference) extends KesRegistrationCertValidationError

  case class NotForwardActivation(effectiveFromEpoch: EpochProgress, currentEpoch: EpochProgress) extends KesRegistrationCertValidationError

  /** Risk-5 (#179) — the operator's per-peer cert chain must have strictly-increasing `effectiveFromEpoch`. Without this constraint, the
    * pointer-only lookup in [[MutableKesRegistry.getKesVk]] (which checks ONLY the latest cert's effective-epoch) could fall through to
    * genesis when the highest-ordinal cert is pending, even though an earlier cert IS active at `currentEpoch`. Enforced strictly so
    * (ordinal, effective) are jointly monotone.
    */
  case class NonMonotonicEffectiveFromEpoch(
    certOrdinal: KesRegistrationOrdinal,
    certEffectiveFromEpoch: EpochProgress,
    lastOrdinal: KesRegistrationOrdinal,
    lastEffectiveFromEpoch: EpochProgress
  ) extends KesRegistrationCertValidationError

  case class MalformedVk(kesMasterVK: io.constellationnetwork.security.hex.Hex) extends KesRegistrationCertValidationError

  case object Rejected extends KesRegistrationCertValidationError

  type KesRegistrationCertValidationErrorOr[A] = ValidatedNec[KesRegistrationCertValidationError, A]
}
