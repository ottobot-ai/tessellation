package io.constellationnetwork.node.shared.domain.economics

/** Nonactivating reference model for the unresolved allow-spend terminal-order gate.
  *
  * This model deliberately does not choose consume/expiry precedence or apply any economic write. It only normalizes already validated
  * reservation claims, identifies which expiry claims are eligible at one supplied consensus epoch, and fails the entire batch when an
  * unfrozen order would be required. Consume and expiry therefore remain outside [[SupportedReferenceOperationId]]. The `Map` and `Set`
  * results below are semantic test containers only; they do not claim canonical byte encoding or traversal order.
  */
private[economics] object AllowSpendTerminalConflictReferenceModel {

  final case class TerminalEpoch private (value: BigInt)

  object TerminalEpoch {
    private val MaxEpoch = BigInt(Long.MaxValue)

    def from(value: BigInt): Either[TerminalEpochOutOfRange, TerminalEpoch] =
      Either.cond(value >= 0 && value <= MaxEpoch, TerminalEpoch(value), TerminalEpochOutOfRange(value))
  }

  final case class TerminalEpochOutOfRange(value: BigInt)

  sealed trait Claim extends Product with Serializable {
    def reservation: ReferenceAllowSpendReservation

    final def identity: AllowSpendSemanticIdentity = reservation.identity
  }

  object Claim {
    final case class Created(reservation: ReferenceAllowSpendReservation) extends Claim
    final case class Consume(reservation: ReferenceAllowSpendReservation) extends Claim
    final case class Expiry(reservation: ReferenceAllowSpendReservation) extends Claim
  }

  sealed trait Conflict extends Product with Serializable

  sealed trait ReservationBindingField extends Product with Serializable

  object ReservationBindingField {
    case object ScopeFromIdentityLane extends ReservationBindingField
    case object Source extends ReservationBindingField
    case object Destination extends ReservationBindingField
    case object Amount extends ReservationBindingField
    case object LastValidEpochProgress extends ReservationBindingField
    case object Approvers extends ReservationBindingField

    val all: Vector[ReservationBindingField] =
      Vector(ScopeFromIdentityLane, Source, Destination, Amount, LastValidEpochProgress, Approvers)
  }

  object Conflict {
    final case class MultipleCreationClaims(count: Int) extends Conflict
    final case class MultipleConsumeClaims(count: Int) extends Conflict
    final case class MultipleExpiryClaims(count: Int) extends Conflict
    final case class InconsistentReservationClaims(count: Int) extends Conflict
    final case class IdentityReservationMismatch(fields: Vector[ReservationBindingField]) extends Conflict
    case object CreationVersusTerminal extends Conflict
    case object ConsumeVersusEligibleExpiry extends Conflict
    case object EligibleConsumeWithoutExhaustiveExpiryEnumeration extends Conflict
  }

  final case class UnresolvedTerminalOrder(
    conflicts: Map[AllowSpendSemanticIdentity, Set[Conflict]]
  )

  /** Characterizes the current v4 expiry boundary only. It does not ratify this boundary as the target terminal-order rule. */
  final case class CharacterizedV4ExpiryEligibility(
    candidateEpoch: TerminalEpoch,
    lastValidEpochProgress: BigInt,
    eligible: Boolean
  )

  final case class NormalizedClaims(
    reservation: ReferenceAllowSpendReservation,
    creation: Option[Claim.Created],
    consume: Option[Claim.Consume],
    expiry: Option[(Claim.Expiry, CharacterizedV4ExpiryEligibility)]
  ) {
    val terminalCandidateCount: Int = consume.size + expiry.count(_._2.eligible)
  }

  final case class NormalizedBatch(
    byIdentity: Map[AllowSpendSemanticIdentity, NormalizedClaims]
  ) {
    val terminalCandidateCountByIdentity: Map[AllowSpendSemanticIdentity, Int] =
      byIdentity.view.mapValues(_.terminalCandidateCount).toMap
  }

  def normalize(
    candidateEpoch: TerminalEpoch,
    claims: Vector[Claim]
  ): Either[UnresolvedTerminalOrder, NormalizedBatch] = {
    val grouped = claims.groupBy(_.identity)
    val conflicts = grouped.iterator.flatMap {
      case (identity, claimsForIdentity) =>
        val creations = claimsForIdentity.collect { case claim: Claim.Created => claim }
        val consumes = claimsForIdentity.collect { case claim: Claim.Consume => claim }
        val expiries = claimsForIdentity.collect { case claim: Claim.Expiry => claim }
        val distinctReservations = claimsForIdentity.iterator.map(_.reservation).toSet
        val hasCharacterizedV4EligibleExpiry = expiries.exists(_.reservation.lastValidEpochProgress < candidateEpoch.value)
        val hasEligibleConsumeWithoutExpiryEnumeration =
          expiries.isEmpty && consumes.exists(_.reservation.lastValidEpochProgress < candidateEpoch.value)
        val bindingMismatches = ReservationBindingField.all.filter { field =>
          claimsForIdentity.exists(claim => reservationMismatchesIdentity(claim.reservation, field))
        }

        val reasons = Set.newBuilder[Conflict]
        if (creations.sizeCompare(1) > 0) reasons += Conflict.MultipleCreationClaims(creations.size)
        if (consumes.sizeCompare(1) > 0) reasons += Conflict.MultipleConsumeClaims(consumes.size)
        if (expiries.sizeCompare(1) > 0) reasons += Conflict.MultipleExpiryClaims(expiries.size)
        if (distinctReservations.sizeCompare(1) > 0)
          reasons += Conflict.InconsistentReservationClaims(distinctReservations.size)
        if (bindingMismatches.nonEmpty)
          reasons += Conflict.IdentityReservationMismatch(bindingMismatches)
        if (creations.nonEmpty && (consumes.nonEmpty || expiries.nonEmpty))
          reasons += Conflict.CreationVersusTerminal
        if (consumes.nonEmpty && hasCharacterizedV4EligibleExpiry)
          reasons += Conflict.ConsumeVersusEligibleExpiry
        if (hasEligibleConsumeWithoutExpiryEnumeration)
          reasons += Conflict.EligibleConsumeWithoutExhaustiveExpiryEnumeration

        val result = reasons.result()
        Option.when(result.nonEmpty)(identity -> result)
    }.toMap

    if (conflicts.nonEmpty) Left(UnresolvedTerminalOrder(conflicts))
    else
      Right(
        NormalizedBatch(
          grouped.iterator.map {
            case (identity, claimsForIdentity) =>
              val reservation = claimsForIdentity.head.reservation
              val creation = claimsForIdentity.collectFirst { case claim: Claim.Created => claim }
              val consume = claimsForIdentity.collectFirst { case claim: Claim.Consume => claim }
              val expiry = claimsForIdentity.collectFirst {
                case claim: Claim.Expiry =>
                  claim -> CharacterizedV4ExpiryEligibility(
                    candidateEpoch,
                    reservation.lastValidEpochProgress,
                    reservation.lastValidEpochProgress < candidateEpoch.value
                  )
              }

              identity -> NormalizedClaims(reservation, creation, consume, expiry)
          }.toMap
        )
      )
  }

  private def reservationMismatchesIdentity(
    reservation: ReferenceAllowSpendReservation,
    field: ReservationBindingField
  ): Boolean = {
    val atom = reservation.identity.atom

    field match {
      case ReservationBindingField.ScopeFromIdentityLane => reservation.scope != atom.lane.scope
      case ReservationBindingField.Source                => reservation.source != atom.source
      case ReservationBindingField.Destination           => reservation.destination != atom.destination
      case ReservationBindingField.Amount                => reservation.amount != atom.amount
      case ReservationBindingField.LastValidEpochProgress =>
        reservation.lastValidEpochProgress != atom.lastValidEpochProgress
      case ReservationBindingField.Approvers => reservation.approvers != atom.approvers
    }
  }
}
