package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, ValidatedNec}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.Try

import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Accepts or rejects unified KES+VRF registration candidates for inclusion in a global snapshot.
  *
  * The acceptance manager is the equivalent of `UpdateNodeCollateralAcceptanceManager` for the Slice 10 cert family. It takes the candidate
  * certs, the per-operator `lastRef` map, and the exact candidate-parent context; returns the partitioned (accepted/rejected) result and
  * the `SortedMap[PeerId, KesRegistrationRecord]` representing the new per-operator latest accepted cert that the GSAM wiring writes into
  * the mutable [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] partition.
  *
  * '''Determinism''': Input certs are defensively sorted by `show`, grouped by operator, validated independently, and partitioned via
  * `foldLeft`. More than one candidate for the same operator in one batch rejects that operator's entire group before validation; a map
  * conversion never chooses among conflicting candidates.
  */
trait KesRegistrationCertAcceptanceManager[F[_]] {

  /** Validate the input certs against the per-operator `lastRefs` and `lastEffectiveFromPeriods`, and return the accepted records
    * partitioned from those that failed validation.
    *
    * @param lastEffectiveFromPeriods
    *   per-operator `effectiveFromPeriod` of the most-recently accepted cert. Required by the strict-monotonicity check
    *   ([[KesRegistrationCertValidator.NonMonotonicEffectiveFromPeriod]]). Operators without a prior accepted cert default to
    *   `EtaPeriod.Zero`, which is consistent with `KesRegistrationReference.empty`'s zero baseline.
    * @param registeredKeyOwnership
    *   validated permanent ownership of every paired genesis key and every key in the exact parent branch's retained runtime histories.
    *   Construction failure is consensus-state unavailability and must be handled before calling `accept`.
    */
  def accept(
    certs: List[Signed[KesRegistrationCert]],
    lastRefs: SortedMap[PeerId, KesRegistrationReference],
    lastEffectiveFromPeriods: SortedMap[PeerId, EtaPeriod],
    registeredKeyOwnership: RegisteredConsensusKeyOwnership,
    context: RegistrationEvaluationContext,
    snapshotOrdinal: SnapshotOrdinal
  ): F[KesRegistrationCertAcceptanceResult]
}

case class KesRegistrationCertAcceptanceResult(
  accepted: SortedMap[PeerId, KesRegistrationRecord],
  notAccepted: List[(Signed[KesRegistrationCert], NonEmptyChain[KesRegistrationCertValidationError])]
)

object KesRegistrationCertAcceptanceManager {

  def make[F[_]: Async](validator: KesRegistrationCertValidator[F]): KesRegistrationCertAcceptanceManager[F] =
    new KesRegistrationCertAcceptanceManager[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

      def accept(
        certs: List[Signed[KesRegistrationCert]],
        lastRefs: SortedMap[PeerId, KesRegistrationReference],
        lastEffectiveFromPeriods: SortedMap[PeerId, EtaPeriod],
        registeredKeyOwnership: RegisteredConsensusKeyOwnership,
        context: RegistrationEvaluationContext,
        snapshotOrdinal: SnapshotOrdinal
      ): F[KesRegistrationCertAcceptanceResult] = {
        val sorted = certs.sortBy(_.show)
        val batchKeyOwners = CandidateBatchKeyOwners.from(sorted)
        val conflicts = sorted
          .groupBy(_.value.operatorPeerId)
          .collect {
            case (peerId, candidates) if candidates.sizeCompare(1) > 0 =>
              peerId -> candidates.map(_.value.ordinal).sorted
          }
        for {
          validated <- sorted.traverse { c =>
            val lastRef = lastRefs.getOrElse(c.value.operatorPeerId, KesRegistrationReference.empty)
            val lastEffective = lastEffectiveFromPeriods.getOrElse(c.value.operatorPeerId, EtaPeriod.Zero)
            conflicts.get(c.value.operatorPeerId) match {
              case Some(ordinals) =>
                val rejection: KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]] =
                  (ConflictingOperatorRegistrations(c.value.operatorPeerId, ordinals): KesRegistrationCertValidationError)
                    .invalidNec[Signed[KesRegistrationCert]]
                (c, rejection).pure[F]
              case None =>
                validator
                  .validate(c, lastRef, lastEffective, context)
                  .map(_.productR(validateKeyUniqueness(c, registeredKeyOwnership, batchKeyOwners)))
                  .map((c, _))
            }
          }
          partitioned = partition(validated)
          accepted = partitioned._1
            .map(c => (c.value.operatorPeerId, KesRegistrationRecord(c, snapshotOrdinal)))
            .to(SortedMap)
          _ <- logger.info(
            s"[OPERATOR_KEY_REGISTRATION] ordinal=${snapshotOrdinal.show} period=${context.inclusionPeriod.show} " +
              s"input=${certs.size} accepted=${accepted.size} rejected=${partitioned._2.size}"
          )
        } yield KesRegistrationCertAcceptanceResult(accepted, partitioned._2)
      }

      private def partition(
        validated: List[(Signed[KesRegistrationCert], KesRegistrationCertValidationErrorOr[Signed[KesRegistrationCert]])]
      ): (List[Signed[KesRegistrationCert]], List[(Signed[KesRegistrationCert], NonEmptyChain[KesRegistrationCertValidationError])]) =
        validated.foldLeft(
          (
            List.empty[Signed[KesRegistrationCert]],
            List.empty[(Signed[KesRegistrationCert], NonEmptyChain[KesRegistrationCertValidationError])]
          )
        ) {
          case ((accepted, notAccepted), (signed, validated)) =>
            validated match {
              case Valid(a)   => (a :: accepted, notAccepted)
              case Invalid(e) => (accepted, (signed, e) :: notAccepted)
            }
        }

      private def validateKeyUniqueness(
        candidate: Signed[KesRegistrationCert],
        registered: RegisteredConsensusKeyOwnership,
        batch: CandidateBatchKeyOwners
      ): ValidatedNec[KesRegistrationCertValidationError, Signed[KesRegistrationCert]] =
        DecodedConsensusKeys.from(candidate.value) match {
          case None => candidate.validNec
          case Some(keys) =>
            val claimant = candidate.value.operatorPeerId
            val kesOwners =
              (registered.kesOwner(keys.kes).toList ++ batch.kesOwners(keys.kes)).filterNot(_ === claimant).distinct.sorted
            val vrfOwners =
              (registered.vrfOwner(keys.vrf).toList ++ batch.vrfOwners(keys.vrf)).filterNot(_ === claimant).distinct.sorted
            val errors =
              Option
                .when(kesOwners.nonEmpty)(
                  KesKeyAlreadyRegistered(Hex.fromBytes(keys.kes.toArray), claimant, kesOwners): KesRegistrationCertValidationError
                )
                .toList ++
                Option
                  .when(vrfOwners.nonEmpty)(
                    VrfKeyAlreadyRegistered(Hex.fromBytes(keys.vrf.toArray), claimant, vrfOwners): KesRegistrationCertValidationError
                  )
                  .toList

            NonEmptyChain.fromSeq(errors).fold(candidate.validNec[KesRegistrationCertValidationError])(_.invalid)
        }
    }

}

/** One already-canonical operator/key claim used to construct the global ownership view. `anchored` claims must include every paired
  * genesis operator. `runtimeRecords` passed to [[RegisteredConsensusKeyOwnership.fromState]] must contain complete retained histories, not
  * only latest active pointers: a key is never transferable between operators because its former holder retains the secret.
  */
final case class RegisteredConsensusKeyClaim(operatorPeerId: PeerId, kesMasterVK: Hex, vrfPublicKey: Hex)

/** A validated global ownership index for every KES and VRF key already registered on the exact candidate-parent branch.
  *
  * The constructor is private: callers must use [[RegisteredConsensusKeyOwnership.fromState]], which normalizes key text to decoded bytes,
  * rejects malformed state, rejects an MPT record stored under another operator, and rejects any pre-existing cross-operator collision.
  */
final class RegisteredConsensusKeyOwnership private (
  private val kes: Map[Vector[Byte], PeerId],
  private val vrf: Map[Vector[Byte], PeerId]
) {
  private[kes] def kesOwner(key: Vector[Byte]): Option[PeerId] = kes.get(key)
  private[kes] def vrfOwner(key: Vector[Byte]): Option[PeerId] = vrf.get(key)
}

object RegisteredConsensusKeyOwnership {

  private[kes] val empty: RegisteredConsensusKeyOwnership = new RegisteredConsensusKeyOwnership(Map.empty, Map.empty)

  /** Build the ownership index from the paired genesis anchor plus complete runtime histories at the candidate parent. A `Left` is
    * consensus-state unavailability/corruption and must defer or reject proposal construction; it must never be replaced with `empty`.
    */
  def fromState(
    anchored: Iterable[RegisteredConsensusKeyClaim],
    runtimeRecords: SortedMap[PeerId, SortedSet[KesRegistrationRecord]]
  ): Either[NonEmptyChain[RegisteredConsensusKeyOwnershipError], RegisteredConsensusKeyOwnership] = {
    val anchoredSources = anchored.toList.sortBy(_.operatorPeerId).map(OwnershipSource.Anchored)
    val runtimeSources = runtimeRecords.toList.flatMap {
      case (stateOperator, records) => records.toList.map(OwnershipSource.Runtime(stateOperator, _))
    }
    val decoded = (anchoredSources ++ runtimeSources).map(decodeSource)
    val decodeErrors = decoded.collect { case Left(error) => error }
    val claims = decoded.collect { case Right(claim) => claim }
    val kesClaims = claims.map(claim => claim.keys.kes -> claim.operatorPeerId)
    val vrfClaims = claims.map(claim => claim.keys.vrf -> claim.operatorPeerId)
    val ownershipErrors = conflictingOwners(kesClaims).map {
      case (key, owners) => ConflictingRegisteredKesKeyOwners(Hex.fromBytes(key.toArray), owners): RegisteredConsensusKeyOwnershipError
    } ++ conflictingOwners(vrfClaims).map {
      case (key, owners) => ConflictingRegisteredVrfKeyOwners(Hex.fromBytes(key.toArray), owners): RegisteredConsensusKeyOwnershipError
    }

    NonEmptyChain.fromSeq(decodeErrors ++ ownershipErrors).toLeft {
      new RegisteredConsensusKeyOwnership(uniqueOwners(kesClaims), uniqueOwners(vrfClaims))
    }
  }

  private def decodeSource(source: OwnershipSource): Either[RegisteredConsensusKeyOwnershipError, DecodedKeyClaim] = {
    val (stateOperator, claim, ordinal) = source match {
      case OwnershipSource.Anchored(value) => (value.operatorPeerId, value, none[KesRegistrationOrdinal])
      case OwnershipSource.Runtime(keyedOperator, record) =>
        val cert = record.event.value
        (keyedOperator, RegisteredConsensusKeyClaim(cert.operatorPeerId, cert.kesMasterVK, cert.vrfPublicKey), cert.ordinal.some)
    }

    if (stateOperator =!= claim.operatorPeerId)
      Left(RegistrationStateOperatorMismatch(stateOperator, claim.operatorPeerId, ordinal))
    else
      DecodedConsensusKeys
        .from(claim)
        .toRight(MalformedRegisteredConsensusKeys(claim.operatorPeerId, ordinal))
        .map(DecodedKeyClaim(claim.operatorPeerId, _))
  }

  private def conflictingOwners(claims: List[(Vector[Byte], PeerId)]): List[(Vector[Byte], List[PeerId])] =
    claims
      .groupMap(_._1)(_._2)
      .toList
      .map { case (key, owners) => key -> owners.distinct.sorted }
      .filter(_._2.sizeCompare(1) > 0)
      .sortBy { case (key, _) => Hex.fromBytes(key.toArray).value }

  private def uniqueOwners(claims: List[(Vector[Byte], PeerId)]): Map[Vector[Byte], PeerId] =
    Map.from(claims.groupMap(_._1)(_._2).collect {
      case (key, owners) if owners.distinct.sizeIs == 1 => key -> owners.head
    })

  private sealed trait OwnershipSource
  private object OwnershipSource {
    final case class Anchored(value: RegisteredConsensusKeyClaim) extends OwnershipSource
    final case class Runtime(stateOperator: PeerId, record: KesRegistrationRecord) extends OwnershipSource
  }

  private final case class DecodedKeyClaim(operatorPeerId: PeerId, keys: DecodedConsensusKeys)
}

sealed trait RegisteredConsensusKeyOwnershipError
final case class RegistrationStateOperatorMismatch(
  stateOperator: PeerId,
  recordOperator: PeerId,
  ordinal: Option[KesRegistrationOrdinal]
) extends RegisteredConsensusKeyOwnershipError
final case class MalformedRegisteredConsensusKeys(operatorPeerId: PeerId, ordinal: Option[KesRegistrationOrdinal])
    extends RegisteredConsensusKeyOwnershipError
final case class ConflictingRegisteredKesKeyOwners(kesMasterVK: Hex, operators: List[PeerId]) extends RegisteredConsensusKeyOwnershipError
final case class ConflictingRegisteredVrfKeyOwners(vrfPublicKey: Hex, operators: List[PeerId]) extends RegisteredConsensusKeyOwnershipError

private final case class DecodedConsensusKeys(kes: Vector[Byte], vrf: Vector[Byte])

private object DecodedConsensusKeys {
  def from(cert: KesRegistrationCert): Option[DecodedConsensusKeys] =
    from(RegisteredConsensusKeyClaim(cert.operatorPeerId, cert.kesMasterVK, cert.vrfPublicKey))

  def from(claim: RegisteredConsensusKeyClaim): Option[DecodedConsensusKeys] = {
    val kes = Try(claim.kesMasterVK.toBytes).toOption.filter(_.length == KesRegistrationCertValidator.KesMasterVerificationKeyLength)
    val vrf =
      Try(claim.vrfPublicKey.toBytes).toOption.filter(_.length == io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.ExpectedLength)
    (kes, vrf).mapN((k, v) => DecodedConsensusKeys(k.toVector, v.toVector))
  }
}

private final case class CandidateBatchKeyOwners(
  kes: Map[Vector[Byte], List[PeerId]],
  vrf: Map[Vector[Byte], List[PeerId]]
) {
  def kesOwners(key: Vector[Byte]): List[PeerId] = kes.getOrElse(key, Nil)
  def vrfOwners(key: Vector[Byte]): List[PeerId] = vrf.getOrElse(key, Nil)
}

private object CandidateBatchKeyOwners {
  def from(candidates: List[Signed[KesRegistrationCert]]): CandidateBatchKeyOwners = {
    val claims = candidates.flatMap(candidate => DecodedConsensusKeys.from(candidate.value).map(candidate.value.operatorPeerId -> _))
    CandidateBatchKeyOwners(
      claims.groupMap(_._2.kes)(_._1).view.mapValues(_.distinct.sorted).toMap,
      claims.groupMap(_._2.vrf)(_._1).view.mapValues(_.distinct.sorted).toMap
    )
  }
}
