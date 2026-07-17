package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.data.{NonEmptySet, ValidatedNec}
import cats.effect.Async
import cats.syntax.all._

import scala.util.Try

import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.ext.cats.syntax.validated._
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.SignedValidator.SignedValidationError
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, SignedValidator}

import derevo.cats.{eqv, show}
import derevo.derive

/** Validator for the current unified KES+VRF [[KesRegistrationCert]] candidate.
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
  *   1. '''InvalidRegistrationOrdinal''' — the cert's `ordinal` is not exactly the next per-operator ordinal. Replays, skips, and
  *      out-of-order delivery are rejected; the first cert must use `KesRegistrationOrdinal.first`.
  *   1. '''RegistrationOrdinalOverflow''' — computing the exact next ordinal overflowed. The cert fails closed.
  *   1. '''InvalidParent''' — the cert's `parent` reference does not match the last accepted cert for this operator. Mirrors the
  *      `NodeCollateralValidator.validateParent` chain-link check.
  *   1. '''InvalidRegistrationParent''' — the signed GL0 branch parent differs from the exact candidate parent being evaluated.
  *   1. '''InsufficientActivationDelay''' — `effectiveFromPeriod < currentPeriod + 2`. This is the period-index rule used by the historical
  *      resolver: period `N` may use only a pair present in the exact canonical `N-2` registry view, while eta comes from `N-1`. It does
  *      not claim that two full wall-clock periods elapsed after a late-`N-2` inclusion.
  *   1. '''ActivationPeriodOverflow''' — the checked `currentPeriod + 2` calculation overflowed. The cert fails closed.
  *   1. '''InvalidInclusionPeriod''' — a negative period was supplied for canonical inclusion/evaluation. The cert fails closed.
  *   1. '''NonMonotonicEffectiveFromPeriod''' — the cert's `effectiveFromPeriod` is `<= lastSeen.effectiveFromPeriod` for this operator.
  *      Rotations cannot backdate a higher ordinal into an earlier activation period. Runtime lookup also walks the exact pointer-selected
  *      chain so a later pending rotation preserves the prior active runtime key.
  *   1. '''MalformedVk''' — the `kesMasterVK` is not exactly the 32-byte Blake2b-256 product-tree root used by the active KES
  *      implementation, or is invalid hex.
  *   1. '''InconsistentKesActivation''' — a runtime master tree does not start at step 0 or its nonnegative offset differs from
  *      `effectiveFromPeriod`. KES and VRF therefore cannot activate under different period interpretations.
  *   1. '''MalformedVrfPublicKey''' — `vrfPublicKey` is not exactly 32 decoded bytes.
  *
  * This validator does not itself make the registration canonical. Its `currentPeriod` argument must be the candidate's canonical
  * inclusion/evaluation eta period when the GSAM path is wired; route-time validation is only a preliminary filter. Network/genesis and
  * signature-domain binding, global cross-operator KES/VRF-key uniqueness, canonical MPT inclusion, and exact branch-historical consumer
  * lookup remain separate mandatory gates.
  */
trait KesRegistrationCertValidator[F[_]] {

  /** Validate a new cert against the operator's last-seen state.
    *
    * @param signed
    *   the candidate cert wrapped in its long-term-key signature
    * @param lastRef
    *   the previously-accepted [[KesRegistrationReference]] for this operator, or [[KesRegistrationReference.empty]] for a fresh operator
    * @param lastEffectiveFromPeriod
    *   the `effectiveFromPeriod` of the previously-accepted cert for this operator. Used by the monotonic-effective-period check.
    *   `EtaPeriod.Zero` for a fresh operator (matches `KesRegistrationReference.empty`'s implicit zero baseline).
    * @param context
    *   exact GL0 candidate-parent hash and its canonical inclusion/evaluation eta period
    */
  def validate(
    signed: Signed[KesRegistrationCert],
    lastRef: KesRegistrationReference,
    lastEffectiveFromPeriod: EtaPeriod,
    context: KesRegistrationCertValidator.RegistrationEvaluationContext
  ): F[KesRegistrationCertValidator.KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]]
}

object KesRegistrationCertValidator {

  val ActivationDelayPeriods: Long = 2L
  val KesMasterVerificationKeyLength: Int = 32

  case class RegistrationEvaluationContext(candidateParentHash: Hash, inclusionPeriod: EtaPeriod)

  /** Validator that always rejects. Used in modules that don't carry the long-term seedlist needed to verify the operator binding. */
  def rejectAll[F[_]: Async]: KesRegistrationCertValidator[F] = new KesRegistrationCertValidator[F] {
    def validate(
      signed: Signed[KesRegistrationCert],
      lastRef: KesRegistrationReference,
      lastEffectiveFromPeriod: EtaPeriod,
      context: RegistrationEvaluationContext
    ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] =
      (Rejected: KesRegistrationCertValidationError).invalidNec[Signed[KesRegistrationCert]].pure[F]
  }

  def make[F[_]: Async](
    signedValidator: SignedValidator[F],
    l0Seedlist: Option[Set[SeedlistEntry]]
  )(implicit hasher: Hasher[F]): KesRegistrationCertValidator[F] =
    new KesRegistrationCertValidator[F] {

      def validate(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference,
        lastEffectiveFromPeriod: EtaPeriod,
        context: RegistrationEvaluationContext
      ): F[KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]]] =
        for {
          numberOfSignaturesV <- validateNumberOfSignatures(signed)
          signaturesV <- signedValidator
            .validateSignatures(signed)
            .map(_.errorMap[KesRegistrationCertValidationError](InvalidSigned))
          signerMatchV <- validateSignerMatchesOperator(signed)
          authorizedV = validateAuthorizedOperator(signed)
          ordinalContinuityV = validateOrdinalContinuity(signed, lastRef)
          parentV = validateParentLink(signed, lastRef)
          registrationParentV = validateRegistrationParent(signed, context.candidateParentHash)
          activationV = validateForwardActivation(signed, context.inclusionPeriod)
          monotonicPeriodV = validateMonotonicEffectiveFromPeriod(signed, lastRef, lastEffectiveFromPeriod)
          kesWellFormedV = validateKesVkWellFormedness(signed)
          kesActivationV = validateKesActivation(signed)
          vrfWellFormedV = validateVrfPublicKeyWellFormedness(signed)
        } yield
          numberOfSignaturesV
            .productR(signaturesV)
            .productR(signerMatchV)
            .productR(authorizedV)
            .productR(ordinalContinuityV)
            .productR(parentV)
            .productR(registrationParentV)
            .productR(activationV)
            .productR(monotonicPeriodV)
            .productR(kesWellFormedV)
            .productR(kesActivationV)
            .productR(vrfWellFormedV)

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

      private def validateOrdinalContinuity(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] = {
        val expectedOrdinal =
          if (lastRef === KesRegistrationReference.empty) Some(KesRegistrationOrdinal.first)
          else lastRef.ordinal.next

        expectedOrdinal match {
          case Some(expected) if signed.value.ordinal === expected => signed.validNec
          case Some(expected)                                      => InvalidRegistrationOrdinal(signed.value.ordinal, expected).invalidNec
          case None                                                => RegistrationOrdinalOverflow(lastRef.ordinal).invalidNec
        }
      }

      private def validateParentLink(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (signed.value.parent === lastRef) signed.validNec
        else InvalidParent(signed.value.parent).invalidNec

      private def validateRegistrationParent(
        signed: Signed[KesRegistrationCert],
        candidateParentHash: Hash
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (signed.value.registrationParentHash === candidateParentHash) signed.validNec
        else InvalidRegistrationParent(signed.value.registrationParentHash, candidateParentHash).invalidNec

      private def validateForwardActivation(
        signed: Signed[KesRegistrationCert],
        currentPeriod: EtaPeriod
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (currentPeriod.value < 0L) InvalidInclusionPeriod(currentPeriod).invalidNec
        else
          Try(Math.addExact(currentPeriod.value, ActivationDelayPeriods)).toOption match {
            case Some(minimumValue) if signed.value.effectiveFromPeriod >= EtaPeriod(minimumValue) => signed.validNec
            case Some(minimumValue) =>
              InsufficientActivationDelay(signed.value.effectiveFromPeriod, currentPeriod, EtaPeriod(minimumValue)).invalidNec
            case None => ActivationPeriodOverflow(currentPeriod).invalidNec
          }

      /** Enforce that `effectiveFromPeriod` is strictly increasing across the operator's per-peer cert chain. Skipped when this is the
        * operator's first cert (`lastRef == KesRegistrationReference.empty`) because there is no prior activation to compare. This forbids
        * a higher-ordinal rotation from backdating itself ahead of its predecessor.
        */
      private def validateMonotonicEffectiveFromPeriod(
        signed: Signed[KesRegistrationCert],
        lastRef: KesRegistrationReference,
        lastEffectiveFromPeriod: EtaPeriod
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
        if (lastRef === KesRegistrationReference.empty) signed.validNec
        else if (signed.value.effectiveFromPeriod > lastEffectiveFromPeriod) signed.validNec
        else
          NonMonotonicEffectiveFromPeriod(
            signed.value.ordinal,
            signed.value.effectiveFromPeriod,
            lastRef.ordinal,
            lastEffectiveFromPeriod
          ).invalidNec

      private def validateKesVkWellFormedness(
        signed: Signed[KesRegistrationCert]
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] = {
        val cert = signed.value
        val vkBytes = Try(cert.kesMasterVK.toBytes).toOption
        if (vkBytes.exists(_.length == KesMasterVerificationKeyLength)) signed.validNec
        else MalformedVk(cert.kesMasterVK).invalidNec
      }

      private def validateKesActivation(
        signed: Signed[KesRegistrationCert]
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] = {
        val cert = signed.value
        if (
          cert.kesMasterVKStep == 0 && cert.offset >= 0L && cert.effectiveFromPeriod.value >= 0L &&
          cert.offset == cert.effectiveFromPeriod.value
        ) signed.validNec
        else InconsistentKesActivation(cert.kesMasterVKStep, cert.offset, cert.effectiveFromPeriod).invalidNec
      }

      private def validateVrfPublicKeyWellFormedness(
        signed: Signed[KesRegistrationCert]
      ): KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] = {
        val vrfPublicKeyBytes = Try(signed.value.vrfPublicKey.toBytes).toOption
        if (vrfPublicKeyBytes.exists(_.length == VrfPublicKey.ExpectedLength)) signed.validNec
        else MalformedVrfPublicKey(signed.value.vrfPublicKey).invalidNec
      }
    }

  @derive(eqv, show)
  sealed trait KesRegistrationCertValidationError

  case class InvalidSigned(error: SignedValidationError) extends KesRegistrationCertValidationError

  case class TooManySignatures(proofs: NonEmptySet[SignatureProof]) extends KesRegistrationCertValidationError

  case class UnauthorizedOperator(operatorPeerId: PeerId) extends KesRegistrationCertValidationError

  case class SignerOperatorMismatch(operatorPeerId: PeerId, signerPeerIds: Set[PeerId]) extends KesRegistrationCertValidationError

  case class InvalidRegistrationOrdinal(certOrdinal: KesRegistrationOrdinal, expectedOrdinal: KesRegistrationOrdinal)
      extends KesRegistrationCertValidationError

  case class RegistrationOrdinalOverflow(lastOrdinal: KesRegistrationOrdinal) extends KesRegistrationCertValidationError

  case class ConflictingOperatorRegistrations(operatorPeerId: PeerId, ordinals: List[KesRegistrationOrdinal])
      extends KesRegistrationCertValidationError

  case class KesKeyAlreadyRegistered(
    kesMasterVK: io.constellationnetwork.security.hex.Hex,
    claimant: PeerId,
    registeredOperators: List[PeerId]
  ) extends KesRegistrationCertValidationError

  case class VrfKeyAlreadyRegistered(
    vrfPublicKey: io.constellationnetwork.security.hex.Hex,
    claimant: PeerId,
    registeredOperators: List[PeerId]
  ) extends KesRegistrationCertValidationError

  case class InvalidParent(parent: KesRegistrationReference) extends KesRegistrationCertValidationError

  case class InvalidRegistrationParent(registrationParentHash: Hash, candidateParentHash: Hash) extends KesRegistrationCertValidationError

  case class InsufficientActivationDelay(
    effectiveFromPeriod: EtaPeriod,
    currentPeriod: EtaPeriod,
    minimumEffectivePeriod: EtaPeriod
  ) extends KesRegistrationCertValidationError

  case class ActivationPeriodOverflow(currentPeriod: EtaPeriod) extends KesRegistrationCertValidationError

  case class InvalidInclusionPeriod(currentPeriod: EtaPeriod) extends KesRegistrationCertValidationError

  /** The operator's per-peer cert chain must have strictly increasing `effectiveFromPeriod`; ordinal and activation order cannot diverge.
    */
  case class NonMonotonicEffectiveFromPeriod(
    certOrdinal: KesRegistrationOrdinal,
    certEffectiveFromPeriod: EtaPeriod,
    lastOrdinal: KesRegistrationOrdinal,
    lastEffectiveFromPeriod: EtaPeriod
  ) extends KesRegistrationCertValidationError

  case class MalformedVk(kesMasterVK: io.constellationnetwork.security.hex.Hex) extends KesRegistrationCertValidationError

  case class InconsistentKesActivation(kesMasterVKStep: Int, offset: Long, effectiveFromPeriod: EtaPeriod)
      extends KesRegistrationCertValidationError

  case class MalformedVrfPublicKey(vrfPublicKey: io.constellationnetwork.security.hex.Hex) extends KesRegistrationCertValidationError

  case object Rejected extends KesRegistrationCertValidationError

  type KesRegistrationCertValidationErrorOr[A] = ValidatedNec[KesRegistrationCertValidationError, A]
}
