package io.constellationnetwork.node.shared.domain.nakamoto.kes

import cats.data.NonEmptyChain
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.{
  KesRegistrationCertValidationError,
  KesRegistrationCertValidationErrorOr
}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.signature.Signed

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Accepts or rejects runtime KES registration certs for inclusion in a global snapshot.
  *
  * The acceptance manager is the equivalent of `UpdateNodeCollateralAcceptanceManager` for the Slice 10 cert family. It takes the candidate
  * certs, the per-operator `lastRef` map, and the current epoch; returns the partitioned (accepted/rejected) result and the
  * `SortedMap[PeerId, KesRegistrationRecord]` representing the new per-operator latest accepted cert that the GSAM wiring writes into the
  * mutable [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] partition.
  *
  * '''Determinism''': Input certs are defensively sorted by `show`, validated independently, and partitioned via `foldLeft` — same pattern
  * as `UpdateNodeCollateralAcceptanceManager`. When the same operator submits multiple certs in one snapshot, only the first one in sorted
  * order whose `ordinal === lastRef.ordinal.next` will pass; subsequent ones fail `NonMonotonicOrdinal` (the validator runs against the
  * pre-acceptance `lastRef`, which is correct: the GSAM acceptance pipeline never accepts more than one cert per operator per snapshot
  * unless the validator chain-link allows it).
  */
trait KesRegistrationCertAcceptanceManager[F[_]] {

  /** Validate the input certs against the per-operator `lastRefs` and `lastEffectiveFromEpochs`, and return the accepted records
    * partitioned from those that failed validation.
    *
    * @param lastEffectiveFromEpochs
    *   per-operator `effectiveFromEpoch` of the most-recently accepted cert. Required by the Risk-5 strict-monotonicity check
    *   ([[KesRegistrationCertValidator.NonMonotonicEffectiveFromEpoch]]). Operators without a prior accepted cert default to
    *   `EpochProgress(0)`, which is consistent with `KesRegistrationReference.empty`'s zero baseline.
    */
  def accept(
    certs: List[Signed[KesRegistrationCert]],
    lastRefs: SortedMap[PeerId, KesRegistrationReference],
    lastEffectiveFromEpochs: SortedMap[PeerId, EpochProgress],
    currentEpoch: EpochProgress,
    snapshotOrdinal: SnapshotOrdinal
  ): F[KesRegistrationCertAcceptanceResult]
}

case class KesRegistrationCertAcceptanceResult(
  accepted: SortedMap[PeerId, KesRegistrationRecord],
  notAccepted: List[(Signed[KesRegistrationCert], NonEmptyChain[KesRegistrationCertValidationError])]
)

object KesRegistrationCertAcceptanceManager {

  def make[F[_]: Async: SecurityProvider](validator: KesRegistrationCertValidator[F]): KesRegistrationCertAcceptanceManager[F] =
    new KesRegistrationCertAcceptanceManager[F] {

      private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

      def accept(
        certs: List[Signed[KesRegistrationCert]],
        lastRefs: SortedMap[PeerId, KesRegistrationReference],
        lastEffectiveFromEpochs: SortedMap[PeerId, EpochProgress],
        currentEpoch: EpochProgress,
        snapshotOrdinal: SnapshotOrdinal
      ): F[KesRegistrationCertAcceptanceResult] = {
        val sorted = certs.sortBy(_.show)
        for {
          validated <- sorted.traverse { c =>
            val lastRef = lastRefs.getOrElse(c.value.operatorPeerId, KesRegistrationReference.empty)
            val lastEffective = lastEffectiveFromEpochs.getOrElse(c.value.operatorPeerId, EpochProgress.MinValue)
            validator.validate(c, lastRef, lastEffective, currentEpoch).map((c, _))
          }
          partitioned = partition(validated)
          accepted = partitioned._1
            .map(c => (c.value.operatorPeerId, KesRegistrationRecord(c, snapshotOrdinal)))
            .to(SortedMap)
          _ <- logger.info(
            s"[KES_REGISTRATION] ordinal=${snapshotOrdinal.show} epoch=${currentEpoch.show} " +
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
    }

  // Used by SortedSet/SortedMap of records by ordinal — kept here for symmetry with NodeCollateralRecord.
  private[kes] val _ordering: Ordering[KesRegistrationRecord] = KesRegistrationRecord.ordering
  private[kes] val _unusedSortedSet: SortedSet[KesRegistrationRecord] = SortedSet.empty[KesRegistrationRecord]
}
