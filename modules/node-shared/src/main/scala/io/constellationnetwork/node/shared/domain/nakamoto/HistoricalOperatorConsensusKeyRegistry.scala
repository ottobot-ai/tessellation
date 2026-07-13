package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.Try

import io.constellationnetwork.node.shared.domain.nakamoto.kes._
import io.constellationnetwork.schema.kes.KesRegistrationCert.{
  KesRegistrationOrdinal,
  KesRegistrationRecord,
  KesRegistrationReference
}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.{Hasher, HasherSelector}

/** An exact, already-authenticated candidate-parent context.
  *
  * Implementations must return the `GlobalSnapshotInfo` reproduced while validating the signed snapshot identified by `parentHash`.
  * A best-tip context, an ordinal-only lookup, an unverified disk placeholder, or an unknown-branch fallback does not satisfy this
  * contract. Missing authenticated history is represented by `None` and makes consensus work defer/recover; callers must never fall back
  * to a current registry.
  */
final case class HistoricalOperatorRegistryView(
  parentHash: Hash,
  parentOrdinal: SnapshotOrdinal,
  info: GlobalSnapshotInfo
)

trait HistoricalOperatorRegistryViewSource[F[_]] {
  def get(candidateParent: Hash): F[Option[HistoricalOperatorRegistryView]]
}

/** A canonical operator-membership snapshot, independently authenticated against the same candidate branch.
  *
  * Key registration is deliberately absent from this type: owning a valid KES+VRF pair does not confer validator status. The current GSI
  * roots delayed stake amounts but does not root the seedlist/operator roster that gates those amounts, so production code must return
  * `None` until an authenticated active-era roster exists.
  */
final case class CanonicalOperatorRosterSnapshot(
  candidateParent: Hash,
  period: EtaPeriod,
  operators: SortedSet[PeerId]
)

trait CanonicalOperatorRosterSource[F[_]] {
  def get(candidateParent: Hash, period: EtaPeriod): F[Option[CanonicalOperatorRosterSnapshot]]
}

object CanonicalOperatorRosterSource {
  def unavailable[F[_]: Async]: CanonicalOperatorRosterSource[F] = new CanonicalOperatorRosterSource[F] {
    def get(candidateParent: Hash, period: EtaPeriod): F[Option[CanonicalOperatorRosterSnapshot]] = none.pure[F]
  }
}

/** Separately authenticated genesis eligibility population. This is intentionally not derived from the genesis key registry. */
final case class CanonicalGenesisOperatorPopulation(
  operators: SortedSet[PeerId],
  stakes: StakeDistribution
)

final case class CanonicalActiveKeySet(
  candidateParent: Hash,
  parentOrdinal: SnapshotOrdinal,
  eligibilityPeriod: EtaPeriod,
  registryCutoffPeriod: EtaPeriod,
  keys: SortedMap[PeerId, OperatorConsensusKeys]
)

final case class CanonicalEligibleOperatorSet(
  candidateParent: Hash,
  parentOrdinal: SnapshotOrdinal,
  eligibilityPeriod: EtaPeriod,
  populationPeriod: EtaPeriod,
  operators: SortedMap[PeerId, OperatorConsensusKeys],
  stakes: StakeDistribution
)

sealed trait HistoricalOperatorRegistryError extends Product with Serializable
final case class CandidateParentRegistryUnavailable(candidateParent: Hash) extends HistoricalOperatorRegistryError
final case class CandidateParentRegistryIdentityMismatch(requested: Hash, returned: Hash) extends HistoricalOperatorRegistryError
final case class InvalidEligibilityPeriod(period: EtaPeriod) extends HistoricalOperatorRegistryError
final case class EligibilityPeriodAheadOfCandidateParent(
  requested: EtaPeriod,
  candidateParent: Hash,
  parentOrdinal: SnapshotOrdinal,
  maximum: EtaPeriod
) extends HistoricalOperatorRegistryError
final case class HistoricalRegistryArithmeticOverflow(operation: String) extends HistoricalOperatorRegistryError
final case class InvalidEtaRotationSnapshots(value: Long) extends HistoricalOperatorRegistryError
final case class CorruptHistoricalOperatorRegistry(candidateParent: Hash, reasons: NonEmptyList[String])
    extends HistoricalOperatorRegistryError
final case class DelayedStakePopulationUnavailable(candidateParent: Hash, period: EtaPeriod)
    extends HistoricalOperatorRegistryError
final case class CanonicalOperatorRosterUnavailable(candidateParent: Hash, period: EtaPeriod)
    extends HistoricalOperatorRegistryError
final case class CanonicalOperatorRosterIdentityMismatch(
  requestedParent: Hash,
  requestedPeriod: EtaPeriod,
  returnedParent: Hash,
  returnedPeriod: EtaPeriod
) extends HistoricalOperatorRegistryError

/** Exact-parent historical resolver for the atomically registered KES+VRF identity.
  *
  * An epoch-N key is selected only from the registry prefix that existed at the end of N-2. Runtime records accepted later on the same
  * branch are not visible to that epoch even if a malformed record claims an earlier activation. Every retained history and pointer is
  * validated as one complete append-only chain before any key is returned. Candidate-carried keys are not inputs.
  */
trait HistoricalOperatorConsensusKeyRegistry[F[_]] {

  /** Resolve one operator's active pair for `period`, or `None` when it was not preregistered and active at the frozen N-2 cutoff. */
  def activeKeysAt(
    operator: PeerId,
    candidateParent: Hash,
    period: EtaPeriod
  ): F[Either[HistoricalOperatorRegistryError, Option[OperatorConsensusKeys]]]

  /** Enumerate key pairs only. This is not an eligible-validator population. */
  def activeKeyPairsAt(
    candidateParent: Hash,
    period: EtaPeriod
  ): F[Either[HistoricalOperatorRegistryError, CanonicalActiveKeySet]]

  /** Intersect active key pairs with the separately authenticated N-2 operator roster and stake population. */
  def eligibleOperatorsAt(
    candidateParent: Hash,
    period: EtaPeriod
  ): F[Either[HistoricalOperatorRegistryError, CanonicalEligibleOperatorSet]]
}

object HistoricalOperatorConsensusKeyRegistry {

  def make[F[_]: Async: HasherSelector](
    genesisKeys: OperatorConsensusKeyRegistry[F],
    genesisPopulation: CanonicalGenesisOperatorPopulation,
    views: HistoricalOperatorRegistryViewSource[F],
    rosters: CanonicalOperatorRosterSource[F],
    etaRotationSnapshots: Long
  ): HistoricalOperatorConsensusKeyRegistry[F] =
    new HistoricalOperatorConsensusKeyRegistry[F] {

      def activeKeysAt(
        operator: PeerId,
        candidateParent: Hash,
        period: EtaPeriod
      ): F[Either[HistoricalOperatorRegistryError, Option[OperatorConsensusKeys]]] =
        resolve(candidateParent, period).map(_.map(state => activeFor(operator, state, period)))

      def activeKeyPairsAt(
        candidateParent: Hash,
        period: EtaPeriod
      ): F[Either[HistoricalOperatorRegistryError, CanonicalActiveKeySet]] =
        resolve(candidateParent, period).map(_.map { state =>
          val operators = state.genesis.keySet ++ state.runtimeChains.keySet
          val active = operators.toList.sorted.flatMap { operator =>
            activeFor(operator, state, period).map(operator -> _)
          }.to(SortedMap)
          CanonicalActiveKeySet(
            state.view.parentHash,
            state.view.parentOrdinal,
            period,
            period.minus(KesRegistrationCertValidator.ActivationDelayPeriods),
            active
          )
        })

      def eligibleOperatorsAt(
        candidateParent: Hash,
        period: EtaPeriod
      ): F[Either[HistoricalOperatorRegistryError, CanonicalEligibleOperatorSet]] =
        resolve(candidateParent, period).flatMap {
          case Left(error) => error.asLeft[CanonicalEligibleOperatorSet].pure[F]
          case Right(state) =>
            val populationPeriod = period.minus(KesRegistrationCertValidator.ActivationDelayPeriods)
            populationAt(state, populationPeriod).map(_.map { case (roster, distribution) =>
              val active = (state.genesis.keySet ++ state.runtimeChains.keySet).toList.sorted.flatMap { operator =>
                activeFor(operator, state, period).map(operator -> _)
              }.toMap
              val eligiblePeers = roster.operators.iterator.filter { operator =>
                distribution.stakeOf(operator) > 0 && active.contains(operator)
              }.toList.sorted
              val eligibleKeys = eligiblePeers.iterator.map(operator => operator -> active(operator)).to(SortedMap)
              val eligibleStakes = StakeDistribution(
                eligiblePeers.iterator.map(operator => operator -> distribution.stakeOf(operator)).to(SortedMap)
              )

              CanonicalEligibleOperatorSet(
                state.view.parentHash,
                state.view.parentOrdinal,
                period,
                populationPeriod,
                eligibleKeys,
                eligibleStakes
              )
            })
        }

      private def populationAt(
        state: ValidatedHistoricalRegistry,
        period: EtaPeriod
      ): F[Either[HistoricalOperatorRegistryError, (CanonicalOperatorRosterSnapshot, StakeDistribution)]] =
        if (period.value < 0L) {
          val roster = CanonicalOperatorRosterSnapshot(state.view.parentHash, period, genesisPopulation.operators)
          (roster -> genesisPopulation.stakes).asRight[HistoricalOperatorRegistryError].pure[F]
        } else
          state.view.info.historicalStakeSnapshots.get(period) match {
            case None =>
              (DelayedStakePopulationUnavailable(state.view.parentHash, period): HistoricalOperatorRegistryError)
                .asLeft[(CanonicalOperatorRosterSnapshot, StakeDistribution)]
                .pure[F]
            case Some(stakeSnapshot) if stakeSnapshot.stakes.stakes.valuesIterator.exists(_ < 0) =>
              (CorruptHistoricalOperatorRegistry(
                state.view.parentHash,
                NonEmptyList.one(s"negative delayed stake amount at period=${period.value}")
              ): HistoricalOperatorRegistryError).asLeft[(CanonicalOperatorRosterSnapshot, StakeDistribution)].pure[F]
            case Some(stakeSnapshot) =>
              rosters.get(state.view.parentHash, period).map {
                case None => CanonicalOperatorRosterUnavailable(state.view.parentHash, period).asLeft
                case Some(roster)
                    if roster.candidateParent =!= state.view.parentHash || roster.period =!= period =>
                  CanonicalOperatorRosterIdentityMismatch(
                    state.view.parentHash,
                    period,
                    roster.candidateParent,
                    roster.period
                  ).asLeft
                case Some(roster) => (roster -> stakeSnapshot.stakes).asRight
              }
          }

      private def resolve(
        candidateParent: Hash,
        period: EtaPeriod
      ): F[Either[HistoricalOperatorRegistryError, ValidatedHistoricalRegistry]] =
        validateQueryPeriod(period).fold(
          _.asLeft[ValidatedHistoricalRegistry].pure[F],
          _ =>
            views.get(candidateParent).flatMap {
              case None =>
                (CandidateParentRegistryUnavailable(candidateParent): HistoricalOperatorRegistryError)
                  .asLeft[ValidatedHistoricalRegistry]
                  .pure[F]
              case Some(view) if view.parentHash =!= candidateParent =>
                (CandidateParentRegistryIdentityMismatch(candidateParent, view.parentHash): HistoricalOperatorRegistryError)
                  .asLeft[ValidatedHistoricalRegistry]
                  .pure[F]
              case Some(view) =>
                maximumEligibilityPeriod(view) match {
                  case Left(error) => error.asLeft[ValidatedHistoricalRegistry].pure[F]
                  case Right(maximum) if period > maximum =>
                    (EligibilityPeriodAheadOfCandidateParent(
                      period,
                      candidateParent,
                      view.parentOrdinal,
                      maximum
                    ): HistoricalOperatorRegistryError).asLeft[ValidatedHistoricalRegistry].pure[F]
                  case Right(_) =>
                    registryCutoffOrdinal(period) match {
                      case Left(error)   => error.asLeft[ValidatedHistoricalRegistry].pure[F]
                      case Right(cutoff) => validateView(view, cutoff)
                    }
                }
            }
        )

      private def validateQueryPeriod(period: EtaPeriod): Either[HistoricalOperatorRegistryError, Unit] =
        if (etaRotationSnapshots <= 0L) InvalidEtaRotationSnapshots(etaRotationSnapshots).asLeft
        else Either.cond(period.value >= 0L, (), InvalidEligibilityPeriod(period))

      private def maximumEligibilityPeriod(
        view: HistoricalOperatorRegistryView
      ): Either[HistoricalOperatorRegistryError, EtaPeriod] =
        checkedAdd(view.parentOrdinal.value.value, 1L, "candidate parent ordinal + 1").map { childOrdinal =>
          EtaPeriod(EtaCalculation.rotationPeriod(childOrdinal, etaRotationSnapshots))
        }

      private def validateView(
        view: HistoricalOperatorRegistryView,
        registryCutoffOrdinal: Option[Long]
      ): F[Either[HistoricalOperatorRegistryError, ValidatedHistoricalRegistry]] =
        genesisKeys.list.flatMap { genesisMap =>
          val genesis = genesisMap.iterator.to(SortedMap)
          val genesisErrors = validateGenesis(genesis)
          val keySetErrors =
            Option.when(view.info.kesRegistrationCerts.keySet =!= view.info.lastKesRegistrationRefs.keySet)(
              s"history/pointer operator sets differ: histories=${view.info.kesRegistrationCerts.keySet.mkString(",")}, " +
                s"pointers=${view.info.lastKesRegistrationRefs.keySet.mkString(",")}"
            ).toList

          view.info.kesRegistrationCerts.toList.traverse { case (operator, records) =>
            validateChain(view, operator, records, view.info.lastKesRegistrationRefs.get(operator))
          }.map { chainResults =>
            val chainErrors = chainResults.collect { case Left(errors) => errors }.flatten
            val chains = chainResults.collect { case Right((operator, chain)) => operator -> chain }.to(SortedMap)
            val anchoredClaims = genesis.valuesIterator.map { keys =>
              RegisteredConsensusKeyClaim(
                keys.operatorPeerId,
                Hex.fromBytes(keys.kes.vk.value),
                Hex.fromBytes(keys.vrfPublicKey.toBytes)
              )
            }.toList
            val ownershipErrors = RegisteredConsensusKeyOwnership
              .fromState(anchoredClaims, view.info.kesRegistrationCerts)
              .leftMap(_.toList.map(error => s"permanent key ownership violation: $error"))
              .swap
              .toOption
              .getOrElse(Nil)
            val errors = genesisErrors ++ keySetErrors ++ chainErrors ++ ownershipErrors

            NonEmptyList.fromList(errors) match {
              case Some(reasons) => CorruptHistoricalOperatorRegistry(view.parentHash, reasons).asLeft
              case None          => ValidatedHistoricalRegistry(view, genesis, chains, registryCutoffOrdinal).asRight
            }
          }
        }

      private def validateGenesis(genesis: SortedMap[PeerId, OperatorConsensusKeys]): List[String] = {
        val entryErrors = genesis.toList.flatMap { case (operator, keys) =>
          val malformed =
            operator =!= keys.operatorPeerId || keys.registration.nonEmpty || keys.effectiveFromPeriod =!= EtaPeriod.Zero ||
              keys.kes.vk.step != 0 || keys.kes.offset != 0L ||
              keys.kes.vk.value.length != KesRegistrationCertValidator.KesMasterVerificationKeyLength ||
              keys.vrfPublicKey.toBytes.length != io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.ExpectedLength
          Option.when(malformed)(s"malformed atomic genesis key pair for $operator").toList
        }
        val negativeStakeErrors = genesisPopulation.stakes.stakes.toList.collect {
          case (operator, amount) if amount < 0 => s"negative genesis stake for $operator"
        }
        entryErrors ++ negativeStakeErrors
      }

      private def validateChain(
        view: HistoricalOperatorRegistryView,
        operator: PeerId,
        records: SortedSet[KesRegistrationRecord],
        pointer: Option[KesRegistrationReference]
      ): F[Either[List[String], (PeerId, List[KesRegistrationRecord])]] = {
        val recordList = records.toList
        recordList.traverse(record => referenceAt(record).map(record -> _)).map { referenced =>
          val prefixes = s"operator=$operator"
          val basicErrors =
            Option.when(recordList.isEmpty)(s"$prefixes has an empty retained history").toList ++
              Option.when(pointer.isEmpty)(s"$prefixes has history but no latest pointer").toList ++
              recordList.flatMap(record => validateRecord(view, operator, record).map(reason => s"$prefixes $reason"))
          val byReference = referenced.groupMap(_._2)(_._1)
          val duplicateReferenceErrors = byReference.toList.collect {
            case (reference, matches) if matches.sizeCompare(1) > 0 =>
              s"$prefixes has ${matches.size} records for reference=$reference"
          }

          val walked = pointer.toRight(List(s"$prefixes cannot select a chain without a pointer")).flatMap { latest =>
            byReference.get(latest) match {
              case Some(record :: Nil) => walkChain(operator, record, latest, byReference, Set.empty, Nil)
              case Some(matches)       => Left(List(s"$prefixes latest pointer selects ${matches.size} records"))
              case None                => Left(List(s"$prefixes latest pointer does not select a retained record"))
            }
          }

          val chainErrors = walked.left.getOrElse(Nil)
          val chain = walked.toOption.getOrElse(Nil)
          val completenessErrors = Option.when(chain.size != recordList.size || chain.toSet != recordList.toSet)(
            s"$prefixes retained history contains records outside the pointer-selected chain"
          ).toList
          val orderingErrors = chain.sliding(2).toList.flatMap {
            case previous :: current :: Nil =>
              val acceptedMonotonic = previous.acceptedAt.value.value < current.acceptedAt.value.value
              val effectiveMonotonic = previous.event.value.effectiveFromPeriod < current.event.value.effectiveFromPeriod
              Option.when(!acceptedMonotonic)(s"$prefixes acceptedAt is not strictly increasing").toList ++
                Option.when(!effectiveMonotonic)(s"$prefixes effectiveFromPeriod is not strictly increasing").toList
            case _ => Nil
          }
          val errors = basicErrors ++ duplicateReferenceErrors ++ chainErrors ++ completenessErrors ++ orderingErrors

          Either.cond(errors.isEmpty, operator -> chain, errors)
        }
      }

      @annotation.tailrec
      private def walkChain(
        operator: PeerId,
        current: KesRegistrationRecord,
        currentReference: KesRegistrationReference,
        byReference: Map[KesRegistrationReference, List[KesRegistrationRecord]],
        seen: Set[KesRegistrationReference],
        acc: List[KesRegistrationRecord]
      ): Either[List[String], List[KesRegistrationRecord]] = {
        val cert = current.event.value
        if (seen.contains(currentReference)) Left(List(s"operator=$operator registration chain contains a cycle"))
        else if (cert.parent === KesRegistrationReference.empty)
          Either.cond(
            cert.ordinal === KesRegistrationOrdinal.first,
            current :: acc,
            List(s"operator=$operator registration chain does not start at ordinal one")
          )
        else {
          val ordinalIsNext = checkedAdd(cert.parent.ordinal.value.value, 1L, "registration ordinal + 1")
            .toOption
            .contains(cert.ordinal.value.value)
          if (!ordinalIsNext) Left(List(s"operator=$operator registration ordinal/parent gap"))
          else
            byReference.get(cert.parent) match {
              case Some(parent :: Nil) if parent.event.value.operatorPeerId === operator =>
                walkChain(operator, parent, cert.parent, byReference, seen + currentReference, current :: acc)
              case Some(parent :: Nil) =>
                Left(List(s"operator=$operator registration parent belongs to ${parent.event.value.operatorPeerId}"))
              case Some(matches) => Left(List(s"operator=$operator registration parent selects ${matches.size} records"))
              case None          => Left(List(s"operator=$operator registration parent is missing"))
            }
        }
      }

      private def validateRecord(
        view: HistoricalOperatorRegistryView,
        operator: PeerId,
        record: KesRegistrationRecord
      ): List[String] = {
        val cert = record.event.value
        val kesBytes = Try(cert.kesMasterVK.toBytes).toOption
        val vrfBytes = Try(cert.vrfPublicKey.toBytes).toOption
        val inclusionPeriod = EtaPeriod(EtaCalculation.rotationPeriod(record.acceptedAt.value.value, etaRotationSnapshots))
        val minimumActivation = checkedAdd(
          inclusionPeriod.value,
          KesRegistrationCertValidator.ActivationDelayPeriods,
          "registration inclusion period + activation delay"
        ).toOption
        val signerIds = record.event.proofs.toSortedSet.toList.map(proof => PeerId.fromId(proof.id)).toSet

        Option.when(cert.operatorPeerId =!= operator)(s"record body belongs to ${cert.operatorPeerId}").toList ++
          Option.when(record.acceptedAt.value.value > view.parentOrdinal.value.value)(s"record accepted after candidate parent").toList ++
          Option.when(record.event.proofs.size != 1 || signerIds != Set(operator))(s"record is not exclusively signed by its operator").toList ++
          Option.when(kesBytes.forall(_.length != KesRegistrationCertValidator.KesMasterVerificationKeyLength))(
            s"record has malformed KES master key"
          ).toList ++
          Option.when(vrfBytes.forall(_.length != io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.ExpectedLength))(
            s"record has malformed VRF public key"
          ).toList ++
          Option.when(cert.kesMasterVKStep != 0 || cert.offset < 0L || cert.offset != cert.effectiveFromPeriod.value)(
            s"record has inconsistent KES activation"
          ).toList ++
          Option.when(cert.effectiveFromPeriod.value < 0L)(s"record has negative activation period").toList ++
          Option.when(minimumActivation.forall(cert.effectiveFromPeriod.value < _))(
            s"record violates the two-period preregistration delay"
          ).toList
      }

      private def referenceAt(record: KesRegistrationRecord): F[KesRegistrationReference] =
        HasherSelector[F].forOrdinal(record.acceptedAt) { selected =>
          implicit val hasher: Hasher[F] = selected
          KesRegistrationReference.of[F](record.event)
        }

      private def activeFor(
        operator: PeerId,
        state: ValidatedHistoricalRegistry,
        period: EtaPeriod
      ): Option[OperatorConsensusKeys] = {
        val runtime = state.registryCutoffOrdinal.flatMap { cutoff =>
          state.runtimeChains
            .getOrElse(operator, Nil)
            .filter(record => record.acceptedAt.value.value <= cutoff && record.event.value.effectiveFromPeriod <= period)
            .lastOption
            .flatMap(toConsensusKeys)
        }
        runtime.orElse(state.genesis.get(operator).map(copyKeys))
      }

      private def registryCutoffOrdinal(
        period: EtaPeriod
      ): Either[HistoricalOperatorRegistryError, Option[Long]] = {
        val cutoffPeriod = period.minus(KesRegistrationCertValidator.ActivationDelayPeriods)
        if (cutoffPeriod.value < 0L) none[Long].asRight
        else
          for {
            nextPeriod <- checkedAdd(cutoffPeriod.value, 1L, "registry cutoff period + 1")
            exclusive <- checkedMultiply(nextPeriod, etaRotationSnapshots, "registry cutoff period * rotation length")
            inclusive <- checkedAdd(exclusive, -1L, "registry cutoff exclusive ordinal - 1")
          } yield inclusive.some
      }

      private def checkedAdd(
        left: Long,
        right: Long,
        operation: String
      ): Either[HistoricalOperatorRegistryError, Long] =
        Try(Math.addExact(left, right)).toEither.leftMap(_ => HistoricalRegistryArithmeticOverflow(operation))

      private def checkedMultiply(
        left: Long,
        right: Long,
        operation: String
      ): Either[HistoricalOperatorRegistryError, Long] =
        Try(Math.multiplyExact(left, right)).toEither.leftMap(_ => HistoricalRegistryArithmeticOverflow(operation))
    }

  private final case class ValidatedHistoricalRegistry(
    view: HistoricalOperatorRegistryView,
    genesis: SortedMap[PeerId, OperatorConsensusKeys],
    runtimeChains: SortedMap[PeerId, List[KesRegistrationRecord]],
    registryCutoffOrdinal: Option[Long]
  )

  private def toConsensusKeys(record: KesRegistrationRecord): Option[OperatorConsensusKeys] = {
    val cert = record.event.value
    (Try(cert.kesMasterVK.toBytes).toOption, Try(cert.vrfPublicKey.toBytes).toOption).mapN { (kes, vrf) =>
      OperatorConsensusKeys(
        cert.operatorPeerId,
        KesRegistryEntry(VerificationKeyKesProduct(kes.clone(), cert.kesMasterVKStep), cert.offset),
        io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.fromBytes(vrf),
        cert.effectiveFromPeriod,
        record.some
      )
    }
  }

  private def copyKeys(keys: OperatorConsensusKeys): OperatorConsensusKeys =
    keys.copy(
      kes = KesRegistryEntry(VerificationKeyKesProduct(keys.kes.vk.value.clone(), keys.kes.vk.step), keys.kes.offset),
      vrfPublicKey = io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.fromBytes(keys.vrfPublicKey.toBytes)
    )
}
