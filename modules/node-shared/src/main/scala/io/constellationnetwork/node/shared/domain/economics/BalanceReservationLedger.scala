package io.constellationnetwork.node.shared.domain.economics

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.BalanceReservationCanonical._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.security.hash.Hash

sealed trait BalanceScope extends Product with Serializable

object BalanceScope {
  case object Dag extends BalanceScope
  final case class Metagraph(currencyId: CurrencyId) extends BalanceScope

  implicit val ordering: Ordering[BalanceScope] = new Ordering[BalanceScope] {
    def compare(left: BalanceScope, right: BalanceScope): Int =
      (left, right) match {
        case (Dag, Dag)                   => 0
        case (Dag, _: Metagraph)          => -1
        case (_: Metagraph, Dag)          => 1
        case (Metagraph(a), Metagraph(b)) => Ordering[CurrencyId].compare(a, b)
      }
  }
}

final case class BalanceAccount(scope: BalanceScope, address: Address)

object BalanceAccount {
  implicit val ordering: Ordering[BalanceAccount] = new Ordering[BalanceAccount] {
    def compare(left: BalanceAccount, right: BalanceAccount): Int = {
      val scopeComparison = Ordering[BalanceScope].compare(left.scope, right.scope)
      if (scopeComparison != 0) scopeComparison else Ordering[Address].compare(left.address, right.address)
    }
  }
}

final case class OutgoingDebit(account: BalanceAccount, amount: Amount)
final case class OrdinaryCredit(account: BalanceAccount, amount: Amount)
final case class ReleasedCredit(account: BalanceAccount, amount: Amount)

sealed trait ReservationSemantic extends Product with Serializable

object ReservationSemantic {
  case object AllowSpend extends ReservationSemantic
  case object TokenLock extends ReservationSemantic

  implicit val ordering: Ordering[ReservationSemantic] = new Ordering[ReservationSemantic] {
    def compare(left: ReservationSemantic, right: ReservationSemantic): Int =
      (left, right) match {
        case (AllowSpend, AllowSpend) => 0
        case (AllowSpend, TokenLock)  => -1
        case (TokenLock, AllowSpend)  => 1
        case (TokenLock, TokenLock)   => 0
      }
  }
}

sealed trait NonLiquidDisposition extends Product with Serializable {
  def scope: BalanceScope
  def amount: Amount
}

object NonLiquidDisposition {
  final case class Reservation(scope: BalanceScope, semantic: ReservationSemantic, semanticAnchor: Hash, amount: Amount)
      extends NonLiquidDisposition
  final case class FeeSink(scope: BalanceScope, recipient: Address, policyAnchor: Hash, amount: Amount) extends NonLiquidDisposition

  implicit val ordering: Ordering[NonLiquidDisposition] = new Ordering[NonLiquidDisposition] {
    def compare(left: NonLiquidDisposition, right: NonLiquidDisposition): Int =
      (left, right) match {
        case (a: Reservation, b: Reservation) =>
          firstNonZero(
            Ordering[BalanceScope].compare(a.scope, b.scope),
            Ordering[ReservationSemantic].compare(a.semantic, b.semantic),
            Ordering[Hash].compare(a.semanticAnchor, b.semanticAnchor),
            java.lang.Long.compare(a.amount.value.value, b.amount.value.value)
          )
        case (_: Reservation, _: FeeSink) => -1
        case (_: FeeSink, _: Reservation) => 1
        case (a: FeeSink, b: FeeSink) =>
          firstNonZero(
            Ordering[BalanceScope].compare(a.scope, b.scope),
            Ordering[Address].compare(a.recipient, b.recipient),
            Ordering[Hash].compare(a.policyAnchor, b.policyAnchor),
            java.lang.Long.compare(a.amount.value.value, b.amount.value.value)
          )
      }
  }

  private[economics] def sameIdentity(left: NonLiquidDisposition, right: NonLiquidDisposition): Boolean =
    (left, right) match {
      case (a: Reservation, b: Reservation) =>
        a.scope == b.scope && a.semantic == b.semantic && a.semanticAnchor == b.semanticAnchor
      case (a: FeeSink, b: FeeSink) =>
        a.scope == b.scope && a.recipient == b.recipient && a.policyAnchor == b.policyAnchor
      case _ => false
    }

  private def firstNonZero(values: Int*): Int = values.find(_ != 0).getOrElse(0)
}

/** An operation ID that callers cannot detach from or inject into a claim body. */
final case class BalanceOperationId private (sourceOperationHash: Hash, commitment: Hash)

object BalanceOperationId {
  implicit val ordering: Ordering[BalanceOperationId] = new Ordering[BalanceOperationId] {
    def compare(left: BalanceOperationId, right: BalanceOperationId): Int = {
      val sourceComparison = Ordering[Hash].compare(left.sourceOperationHash, right.sourceOperationHash)
      if (sourceComparison != 0) sourceComparison else Ordering[Hash].compare(left.commitment, right.commitment)
    }
  }

  private[economics] def bind(sourceOperationHash: Hash, commitment: Hash): BalanceOperationId =
    new BalanceOperationId(sourceOperationHash, commitment)
}

final case class BalanceReleaseId private (hash: Hash)

object BalanceReleaseId {
  def fromHash(hash: Hash): BalanceReleaseId = new BalanceReleaseId(hash)

  implicit val ordering: Ordering[BalanceReleaseId] = Ordering.by(_.hash)
}

sealed trait AuthenticatedReleaseSource extends Product with Serializable {
  def anchor: Hash
}

object AuthenticatedReleaseSource {
  final case class AllowSpend(anchor: Hash) extends AuthenticatedReleaseSource
  final case class TokenLock(anchor: Hash) extends AuthenticatedReleaseSource
}

/** Canonical, conserved claim body.
  *
  * Construction derives both gross outgoing debits and final liquid-balance deltas from typed components. This dark slice has no issuance
  * or implicit-burn component: every outgoing debit names an ordinary liquid credit, an exact semantic reservation anchor, or an exact fee
  * recipient/policy sink. The ledger does not yet persist those non-liquid dispositions, so they are declarations checked by this
  * projection kernel, not rooted state transitions. Authenticated releases separately return value from a caller-authenticated non-liquid
  * reservation and may fund gross outgoing checks in the same operation.
  */
final case class BalanceReservationClaim private[economics] (
  sourceOperationHash: Hash,
  scope: BalanceScope,
  outgoingDebits: SortedMap[BalanceAccount, BigInt],
  ordinaryCredits: SortedMap[BalanceAccount, BigInt],
  nonLiquidDispositions: SortedSet[NonLiquidDisposition],
  releaseIds: SortedSet[BalanceReleaseId],
  operationId: BalanceOperationId
)

object BalanceReservationClaim {
  import BalanceReservationError._

  def create(
    sourceOperationHash: Hash,
    scope: BalanceScope,
    outgoingDebits: Iterable[OutgoingDebit],
    ordinaryCredits: Iterable[OrdinaryCredit],
    nonLiquidDispositions: Iterable[NonLiquidDisposition] = Iterable.empty,
    releaseIds: Iterable[BalanceReleaseId] = Iterable.empty
  ): Either[BalanceReservationError, BalanceReservationClaim] = {
    val canonicalOutgoing = aggregate(outgoingDebits.iterator.map(d => d.account -> amountOf(d.amount)))
    val canonicalCredits = aggregate(ordinaryCredits.iterator.map(c => c.account -> amountOf(c.amount)))
    val sortedReleaseIds = releaseIds.iterator.toVector.sorted(BalanceReleaseId.ordering)
    val duplicateRelease = sortedReleaseIds.sliding(2).collectFirst {
      case Vector(left, right) if left == right => left
    }
    val canonicalReleaseIds = canonicalSet(sortedReleaseIds)(BalanceReleaseId.ordering)
    val sortedDispositions = nonLiquidDispositions.iterator
      .filter(disposition => amountOf(disposition.amount) != 0)
      .toVector
      .sorted(NonLiquidDisposition.ordering)
    val duplicateDisposition = sortedDispositions.sliding(2).collectFirst {
      case Vector(left, right) if NonLiquidDisposition.sameIdentity(left, right) => (left, right)
    }
    val canonicalDispositions = canonicalSet(sortedDispositions)(NonLiquidDisposition.ordering)
    val componentAccounts = canonicalSet(Iterator(canonicalOutgoing.keysIterator, canonicalCredits.keysIterator).flatten)(
      BalanceAccount.ordering
    )

    for {
      _ <- duplicateRelease.fold[Either[BalanceReservationError, Unit]](Right(()))(id => Left(DuplicateReleaseIdInClaim(id)))
      _ <- firstError(componentAccounts.iterator) { account =>
        Option.when(account.scope != scope)(ClaimComponentScopeMismatch(scope, account))
      }
      _ <- duplicateDisposition.fold[Either[BalanceReservationError, Unit]](Right(())) {
        case (left, right) =>
          Left(DuplicateDispositionIdentity(left, right))
      }
      _ <- firstError(canonicalDispositions.iterator) { disposition =>
        Option.when(disposition.scope != scope)(DispositionScopeMismatch(scope, disposition))
      }
      totalOutgoing = canonicalOutgoing.valuesIterator.sum
      totalCredits = canonicalCredits.valuesIterator.sum
      totalDisposed = canonicalDispositions.iterator.map(d => amountOf(d.amount)).sum
      _ <- Either.cond(
        totalOutgoing != 0 || totalCredits != 0 || totalDisposed != 0 || canonicalReleaseIds.nonEmpty,
        (),
        EmptyEconomicClaim(sourceOperationHash)
      )
      _ <- Either.cond(
        totalOutgoing == totalCredits + totalDisposed,
        (),
        UnbalancedEconomicFlow(totalOutgoing, totalCredits, totalDisposed)
      )
      commitment = ClaimCommitment.compute(
        sourceOperationHash,
        scope,
        canonicalOutgoing,
        canonicalCredits,
        canonicalDispositions,
        canonicalReleaseIds
      )
      operationId = BalanceOperationId.bind(sourceOperationHash, commitment)
    } yield
      BalanceReservationClaim(
        sourceOperationHash,
        scope,
        canonicalOutgoing,
        canonicalCredits,
        canonicalDispositions,
        canonicalReleaseIds,
        operationId
      )
  }

  private def aggregate(entries: Iterator[(BalanceAccount, BigInt)]): SortedMap[BalanceAccount, BigInt] =
    entries
      .foldLeft(SortedMap.empty[BalanceAccount, BigInt](BalanceAccount.ordering)) {
        case (acc, (account, amount)) =>
          acc.updated(account, acc.getOrElse(account, BigInt(0)) + amount)
      }
      .filter { case (_, amount) => amount != 0 }

  private def amountOf(amount: Amount): BigInt = BigInt(amount.value.value)
}

/** A rooted release record authorizing one exact claim to return exact credits in one scope. */
final case class AuthenticatedRelease private[economics] (
  releaseId: BalanceReleaseId,
  authorizedOperationId: BalanceOperationId,
  scope: BalanceScope,
  credits: SortedMap[BalanceAccount, BigInt],
  source: AuthenticatedReleaseSource
)

object AuthenticatedRelease {
  import BalanceReservationError._

  def create(
    releaseId: BalanceReleaseId,
    authorizedClaim: BalanceReservationClaim,
    credits: Iterable[ReleasedCredit],
    source: AuthenticatedReleaseSource
  ): Either[BalanceReservationError, AuthenticatedRelease] = {
    val canonicalCredits = credits.iterator
      .foldLeft(SortedMap.empty[BalanceAccount, BigInt](BalanceAccount.ordering)) { (acc, credit) =>
        val amount = BigInt(credit.amount.value.value)
        acc.updated(credit.account, acc.getOrElse(credit.account, BigInt(0)) + amount)
      }
      .filter { case (_, amount) => amount != 0 }

    for {
      _ <- Either.cond(
        authorizedClaim.releaseIds.contains(releaseId),
        (),
        ReleaseNotDeclaredByAuthorizedClaim(releaseId, authorizedClaim.operationId)
      )
      _ <- Either.cond(canonicalCredits.nonEmpty, (), EmptyAuthenticatedRelease(releaseId))
      _ <- firstError(canonicalCredits.keysIterator) { account =>
        Option.when(account.scope != authorizedClaim.scope)(ReleaseCreditScopeMismatch(authorizedClaim.scope, account))
      }
    } yield
      AuthenticatedRelease(
        releaseId,
        authorizedClaim.operationId,
        authorizedClaim.scope,
        canonicalCredits,
        source
      )
  }
}

sealed trait BalanceReservationError extends Product with Serializable

object BalanceReservationError {
  final case class ClaimComponentScopeMismatch(expected: BalanceScope, account: BalanceAccount) extends BalanceReservationError
  final case class DispositionScopeMismatch(expected: BalanceScope, disposition: NonLiquidDisposition) extends BalanceReservationError
  final case class DuplicateDispositionIdentity(left: NonLiquidDisposition, right: NonLiquidDisposition) extends BalanceReservationError
  final case class EmptyEconomicClaim(sourceOperationHash: Hash) extends BalanceReservationError
  final case class UnbalancedEconomicFlow(totalOutgoing: BigInt, totalCredits: BigInt, totalDisposed: BigInt)
      extends BalanceReservationError
  final case class DuplicateReleaseIdInClaim(releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class ReleaseNotDeclaredByAuthorizedClaim(releaseId: BalanceReleaseId, operationId: BalanceOperationId)
      extends BalanceReservationError
  final case class EmptyAuthenticatedRelease(releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class ReleaseCreditScopeMismatch(expected: BalanceScope, account: BalanceAccount) extends BalanceReservationError
  final case class InitialBalanceOutOfRange(account: BalanceAccount, balance: BigInt) extends BalanceReservationError
  final case class ReleaseRegistryKeyMismatch(key: BalanceReleaseId, releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class RestoredReleaseCreditOutOfRange(releaseId: BalanceReleaseId, account: BalanceAccount, credit: BigInt)
      extends BalanceReservationError
  final case class OperationHistorySourceMismatch(key: Hash, operationId: BalanceOperationId) extends BalanceReservationError
  final case class OperationHistoryEquivocation(source: Hash, left: BalanceOperationId, right: BalanceOperationId)
      extends BalanceReservationError
  final case class ConsumedUnknownRelease(releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class ConsumedReleaseOperationMissing(releaseId: BalanceReleaseId, operationId: BalanceOperationId)
      extends BalanceReservationError
  final case class ReleaseOperationHistoryConflict(releaseId: BalanceReleaseId, expected: BalanceOperationId, actual: BalanceOperationId)
      extends BalanceReservationError
  final case class AcceptedOperationMissingReleaseConsumption(releaseId: BalanceReleaseId, operationId: BalanceOperationId)
      extends BalanceReservationError
  final case class DuplicateOperationId(operationId: BalanceOperationId) extends BalanceReservationError
  final case class OperationClaimEquivocation(source: Hash, accepted: BalanceOperationId, attempted: BalanceOperationId)
      extends BalanceReservationError
  final case class OperationConflictsWithRegisteredRelease(
    releaseId: BalanceReleaseId,
    registered: BalanceOperationId,
    attempted: BalanceOperationId
  ) extends BalanceReservationError
  final case class DuplicateReleaseId(releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class UnknownReleaseId(releaseId: BalanceReleaseId) extends BalanceReservationError
  final case class ReleaseScopeMismatch(releaseId: BalanceReleaseId, expected: BalanceScope, actual: BalanceScope)
      extends BalanceReservationError
  final case class ReleaseOperationMismatch(releaseId: BalanceReleaseId, expected: BalanceOperationId, actual: BalanceOperationId)
      extends BalanceReservationError
  final case class InsufficientGrossBalance(account: BalanceAccount, required: BigInt, available: BigInt) extends BalanceReservationError
  final case class ProjectedBalanceOutOfRange(account: BalanceAccount, projected: BigInt) extends BalanceReservationError
}

/** Immutable state for the dark balance-reservation kernel.
  *
  * This type has no runtime integration and no wire/serde instances. The release registry is trusted input to this pure kernel; activation
  * requires content-derived release IDs and its source anchors and records to be authenticated in rooted state. Complete sealed
  * framework-operation compilers must derive canonical network/era/type-bound source identities and prove each disposition is the exact
  * rooted lock, allow-spend, or fee transition it names. This state does not persist dispositions or register their future releases; those
  * atomic non-liquid transitions, authorization checks, and rooted replay/restore wiring remain blockers. Issuance and burn are
  * intentionally unsupported.
  */
final case class BalanceReservationState private[economics] (
  balances: SortedMap[BalanceAccount, BigInt],
  releaseRegistry: SortedMap[BalanceReleaseId, AuthenticatedRelease],
  consumedOperations: SortedMap[Hash, BalanceOperationId],
  consumedReleaseIds: SortedSet[BalanceReleaseId]
) {
  def balanceOf(account: BalanceAccount): BigInt = balances.getOrElse(account, BigInt(0))

  def consumedOperationIds: SortedSet[BalanceOperationId] =
    canonicalSet(consumedOperations.valuesIterator)(BalanceOperationId.ordering)
}

object BalanceReservationState {
  import BalanceReservationError._

  private val MaxBalance: BigInt = BigInt(Long.MaxValue)

  /** Greenfield genesis accepts liquid balances only. It cannot smuggle release or replay history into ordinal zero. */
  def genesis(balances: Map[BalanceAccount, BigInt]): Either[BalanceReservationError, BalanceReservationState] =
    canonicalBalances(balances).map(
      BalanceReservationState(
        _,
        SortedMap.empty(BalanceReleaseId.ordering),
        SortedMap.empty(Ordering[Hash]),
        SortedSet.empty(BalanceReleaseId.ordering)
      )
    )

  /** Validated restart/import boundary for already-rooted reservation and replay state. */
  def restore(
    balances: Map[BalanceAccount, BigInt],
    releaseRegistry: Map[BalanceReleaseId, AuthenticatedRelease],
    consumedOperationIds: Set[BalanceOperationId],
    consumedReleaseIds: Set[BalanceReleaseId]
  ): Either[BalanceReservationError, BalanceReservationState] = {
    val canonicalRegistry = canonicalMap(releaseRegistry)(BalanceReleaseId.ordering).map {
      case (releaseId, release) =>
        releaseId -> release.copy(credits = canonicalMap(release.credits)(BalanceAccount.ordering))
    }
    val canonicalConsumedReleases = canonicalSet(consumedReleaseIds)(BalanceReleaseId.ordering)

    for {
      canonicalBalanceState <- canonicalBalances(balances)
      _ <- firstError(canonicalRegistry.iterator) {
        case (key, release) =>
          Option.when(key != release.releaseId)(ReleaseRegistryKeyMismatch(key, release.releaseId))
      }
      _ <- firstError(canonicalRegistry.valuesIterator) { release =>
        Option.when(release.credits.isEmpty)(EmptyAuthenticatedRelease(release.releaseId))
      }
      _ <- firstError(canonicalRegistry.valuesIterator.flatMap(release => release.credits.keysIterator.map(release -> _))) {
        case (release, account) =>
          Option.when(account.scope != release.scope)(ReleaseCreditScopeMismatch(release.scope, account))
      }
      _ <- firstError(
        canonicalRegistry.valuesIterator.flatMap(release =>
          release.credits.iterator.map { case (account, credit) => (release, account, credit) }
        )
      ) {
        case (release, account, credit) =>
          Option.when(credit <= 0 || !inBalanceRange(credit))(
            RestoredReleaseCreditOutOfRange(release.releaseId, account, credit)
          )
      }
      canonicalOperations <- canonicalOperationHistory(consumedOperationIds)
      _ <- firstError(canonicalConsumedReleases.iterator) { releaseId =>
        Option.when(!canonicalRegistry.contains(releaseId))(ConsumedUnknownRelease(releaseId))
      }
      _ <- firstError(canonicalConsumedReleases.iterator) { releaseId =>
        val release = canonicalRegistry(releaseId)
        val consumed = canonicalOperations.get(release.authorizedOperationId.sourceOperationHash)
        Option.when(!consumed.contains(release.authorizedOperationId))(
          ConsumedReleaseOperationMissing(releaseId, release.authorizedOperationId)
        )
      }
      _ <- validateReleaseHistory(canonicalRegistry, canonicalOperations, canonicalConsumedReleases)
    } yield BalanceReservationState(canonicalBalanceState, canonicalRegistry, canonicalOperations, canonicalConsumedReleases)
  }

  private def canonicalBalances(
    balances: Map[BalanceAccount, BigInt]
  ): Either[BalanceReservationError, SortedMap[BalanceAccount, BigInt]] = {
    val canonical = canonicalMap(balances)(BalanceAccount.ordering)
    firstError(canonical.iterator) {
      case (account, balance) =>
        Option.when(!inBalanceRange(balance))(InitialBalanceOutOfRange(account, balance))
    }.map(_ => canonical.filter { case (_, balance) => balance != 0 })
  }

  private def canonicalOperationHistory(
    operationIds: Set[BalanceOperationId]
  ): Either[BalanceReservationError, SortedMap[Hash, BalanceOperationId]] = {
    val canonicalIds = canonicalSet(operationIds)(BalanceOperationId.ordering)

    canonicalIds.iterator.foldLeft[Either[BalanceReservationError, SortedMap[Hash, BalanceOperationId]]](
      Right(SortedMap.empty(Ordering[Hash]))
    ) { (acc, operationId) =>
      acc.flatMap { bySource =>
        bySource.get(operationId.sourceOperationHash) match {
          case Some(existing) if existing != operationId =>
            Left(OperationHistoryEquivocation(operationId.sourceOperationHash, existing, operationId))
          case _ => Right(bySource.updated(operationId.sourceOperationHash, operationId))
        }
      }
    }
  }

  private def validateReleaseHistory(
    registry: SortedMap[BalanceReleaseId, AuthenticatedRelease],
    operations: SortedMap[Hash, BalanceOperationId],
    consumedReleases: SortedSet[BalanceReleaseId]
  ): Either[BalanceReservationError, Unit] =
    firstError(registry.iterator) {
      case (releaseId, release) =>
        operations.get(release.authorizedOperationId.sourceOperationHash).flatMap { accepted =>
          if (accepted != release.authorizedOperationId)
            Some(ReleaseOperationHistoryConflict(releaseId, release.authorizedOperationId, accepted))
          else if (!consumedReleases.contains(releaseId))
            Some(AcceptedOperationMissingReleaseConsumption(releaseId, accepted))
          else None
        }
    }

  private[economics] def inBalanceRange(value: BigInt): Boolean = value >= 0 && value <= MaxBalance
}

/** Pure, atomic transition over [[BalanceReservationState]]. */
object BalanceReservationLedger {
  import BalanceReservationError._

  def attempt(
    state: BalanceReservationState,
    claim: BalanceReservationClaim
  ): Either[BalanceReservationError, BalanceReservationState] =
    for {
      _ <- validateOperationIdentity(state, claim)
      _ <- validateRegisteredReleaseClaims(state, claim)
      releases <- resolveReleases(state, claim)
      releaseCredits = aggregateReleaseCredits(releases)
      _ <- firstError(claim.outgoingDebits.iterator) {
        case (account, required) =>
          val available = state.balanceOf(account) + releaseCredits.getOrElse(account, BigInt(0))
          Option.when(required > available)(InsufficientGrossBalance(account, required, available))
      }
      projected <- projectBalances(state, claim, releaseCredits)
    } yield
      BalanceReservationState(
        projected,
        state.releaseRegistry,
        state.consumedOperations.updated(claim.sourceOperationHash, claim.operationId),
        state.consumedReleaseIds ++ claim.releaseIds
      )

  private def validateOperationIdentity(
    state: BalanceReservationState,
    claim: BalanceReservationClaim
  ): Either[BalanceReservationError, Unit] =
    state.consumedOperations.get(claim.sourceOperationHash) match {
      case Some(existing) if existing == claim.operationId => Left(DuplicateOperationId(claim.operationId))
      case Some(existing) =>
        Left(OperationClaimEquivocation(claim.sourceOperationHash, existing, claim.operationId))
      case None => Right(())
    }

  private def validateRegisteredReleaseClaims(
    state: BalanceReservationState,
    claim: BalanceReservationClaim
  ): Either[BalanceReservationError, Unit] =
    firstError(state.releaseRegistry.iterator) {
      case (releaseId, release) =>
        Option.when(
          release.authorizedOperationId.sourceOperationHash == claim.sourceOperationHash &&
            release.authorizedOperationId != claim.operationId
        )(OperationConflictsWithRegisteredRelease(releaseId, release.authorizedOperationId, claim.operationId))
    }

  private def resolveReleases(
    state: BalanceReservationState,
    claim: BalanceReservationClaim
  ): Either[BalanceReservationError, SortedMap[BalanceReleaseId, AuthenticatedRelease]] =
    claim.releaseIds.iterator.foldLeft[Either[BalanceReservationError, SortedMap[BalanceReleaseId, AuthenticatedRelease]]](
      Right(SortedMap.empty(BalanceReleaseId.ordering))
    ) { (acc, releaseId) =>
      acc.flatMap { resolved =>
        if (state.consumedReleaseIds.contains(releaseId)) Left(DuplicateReleaseId(releaseId))
        else
          state.releaseRegistry.get(releaseId) match {
            case None => Left(UnknownReleaseId(releaseId))
            case Some(release) =>
              if (release.scope != claim.scope) Left(ReleaseScopeMismatch(releaseId, claim.scope, release.scope))
              else if (release.authorizedOperationId != claim.operationId)
                Left(ReleaseOperationMismatch(releaseId, claim.operationId, release.authorizedOperationId))
              else Right(resolved.updated(releaseId, release))
          }
      }
    }

  private def aggregateReleaseCredits(
    releases: SortedMap[BalanceReleaseId, AuthenticatedRelease]
  ): SortedMap[BalanceAccount, BigInt] =
    releases.valuesIterator.foldLeft(SortedMap.empty[BalanceAccount, BigInt](BalanceAccount.ordering)) { (acc, release) =>
      release.credits.foldLeft(acc) {
        case (credits, (account, amount)) =>
          credits.updated(account, credits.getOrElse(account, BigInt(0)) + amount)
      }
    }

  private def projectBalances(
    state: BalanceReservationState,
    claim: BalanceReservationClaim,
    releaseCredits: SortedMap[BalanceAccount, BigInt]
  ): Either[BalanceReservationError, SortedMap[BalanceAccount, BigInt]] = {
    val touchedAccounts = canonicalSet(
      Iterator(claim.outgoingDebits.keysIterator, claim.ordinaryCredits.keysIterator, releaseCredits.keysIterator).flatten
    )(BalanceAccount.ordering)

    touchedAccounts.iterator.foldLeft[Either[BalanceReservationError, SortedMap[BalanceAccount, BigInt]]](Right(state.balances)) {
      case (acc, account) =>
        acc.flatMap { projected =>
          val next = state.balanceOf(account) - claim.outgoingDebits.getOrElse(account, BigInt(0)) +
            claim.ordinaryCredits.getOrElse(account, BigInt(0)) + releaseCredits.getOrElse(account, BigInt(0))
          if (!BalanceReservationState.inBalanceRange(next)) Left(ProjectedBalanceOutOfRange(account, next))
          else if (next == 0) Right(projected.removed(account))
          else Right(projected.updated(account, next))
        }
    }
  }
}

private object ClaimCommitment {
  private val Domain = "tessellation/balance-reservation-claim/v1".getBytes(StandardCharsets.US_ASCII)

  def compute(
    sourceOperationHash: Hash,
    scope: BalanceScope,
    outgoing: SortedMap[BalanceAccount, BigInt],
    credits: SortedMap[BalanceAccount, BigInt],
    dispositions: SortedSet[NonLiquidDisposition],
    releaseIds: SortedSet[BalanceReleaseId]
  ): Hash = {
    val bytes = new ByteArrayOutputStream()
    putBytes(bytes, Domain)
    putHash(bytes, sourceOperationHash)
    putScope(bytes, scope)
    putAccountAmounts(bytes, outgoing)
    putAccountAmounts(bytes, credits)
    putInt(bytes, dispositions.size)
    dispositions.foreach(putDisposition(bytes, _))
    putInt(bytes, releaseIds.size)
    releaseIds.foreach(id => putHash(bytes, id.hash))
    Hash.fromBytes(bytes.toByteArray)
  }

  private def putDisposition(bytes: ByteArrayOutputStream, disposition: NonLiquidDisposition): Unit =
    disposition match {
      case NonLiquidDisposition.Reservation(scope, semantic, anchor, amount) =>
        bytes.write(0)
        putScope(bytes, scope)
        bytes.write(
          semantic match {
            case ReservationSemantic.AllowSpend => 0
            case ReservationSemantic.TokenLock  => 1
          }
        )
        putHash(bytes, anchor)
        putBytes(bytes, BigInt(amount.value.value).toByteArray)
      case NonLiquidDisposition.FeeSink(scope, recipient, policyAnchor, amount) =>
        bytes.write(1)
        putScope(bytes, scope)
        putAddress(bytes, recipient)
        putHash(bytes, policyAnchor)
        putBytes(bytes, BigInt(amount.value.value).toByteArray)
    }

  private def putAccountAmounts(bytes: ByteArrayOutputStream, values: SortedMap[BalanceAccount, BigInt]): Unit = {
    putInt(bytes, values.size)
    values.foreach {
      case (account, amount) =>
        putScope(bytes, account.scope)
        putAddress(bytes, account.address)
        putBytes(bytes, amount.toByteArray)
    }
  }

  private def putScope(bytes: ByteArrayOutputStream, scope: BalanceScope): Unit =
    scope match {
      case BalanceScope.Dag => bytes.write(0)
      case BalanceScope.Metagraph(currencyId) =>
        bytes.write(1)
        putAddress(bytes, currencyId.value)
    }

  private def putAddress(bytes: ByteArrayOutputStream, address: Address): Unit =
    putBytes(bytes, address.value.value.getBytes(StandardCharsets.UTF_8))

  private def putHash(bytes: ByteArrayOutputStream, hash: Hash): Unit =
    putBytes(bytes, hash.value.getBytes(StandardCharsets.UTF_8))

  private def putBytes(target: ByteArrayOutputStream, value: Array[Byte]): Unit = {
    putInt(target, value.length)
    target.write(value, 0, value.length)
  }

  private def putInt(target: ByteArrayOutputStream, value: Int): Unit = {
    target.write((value >>> 24) & 0xff)
    target.write((value >>> 16) & 0xff)
    target.write((value >>> 8) & 0xff)
    target.write(value & 0xff)
  }
}

private[economics] object BalanceReservationCanonical {
  def canonicalMap[K, V](values: Map[K, V])(ordering: Ordering[K]): SortedMap[K, V] =
    values.iterator.foldLeft(SortedMap.empty[K, V](ordering)) { case (acc, (key, value)) => acc.updated(key, value) }

  def canonicalSet[A](values: IterableOnce[A])(ordering: Ordering[A]): SortedSet[A] =
    values.iterator.foldLeft(SortedSet.empty[A](ordering))(_ + _)

  def firstError[A](
    values: Iterator[A]
  )(f: A => Option[BalanceReservationError]): Either[BalanceReservationError, Unit] =
    values.flatMap(f).take(1).toList.headOption.toLeft(())
}
