package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.AuthenticatedReleaseSource.TokenLock
import io.constellationnetwork.node.shared.domain.economics.BalanceReservationError._
import io.constellationnetwork.node.shared.domain.economics.BalanceScope.{Dag, Metagraph}
import io.constellationnetwork.node.shared.domain.economics.NonLiquidDisposition.{FeeSink, Reservation}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

object BalanceReservationLedgerSuite extends FunSuite {

  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")
  private def amount(n: Long): Amount = Amount(NonNegLong.unsafeFrom(n))
  private def releaseId(n: Int): BalanceReleaseId = BalanceReleaseId.fromHash(hash(1000 + n))

  private val alice = address("alice")
  private val bob = address("bob")
  private val carol = address("carol")
  private val dagAlice = BalanceAccount(Dag, alice)
  private val dagBob = BalanceAccount(Dag, bob)
  private val dagCarol = BalanceAccount(Dag, carol)

  private def debit(account: BalanceAccount, value: Long): OutgoingDebit = OutgoingDebit(account, amount(value))
  private def credit(account: BalanceAccount, value: Long): OrdinaryCredit = OrdinaryCredit(account, amount(value))

  private def claimE(
    source: Int,
    scope: BalanceScope = Dag,
    debits: Iterable[OutgoingDebit] = Iterable.empty,
    credits: Iterable[OrdinaryCredit] = Iterable.empty,
    dispositions: Iterable[NonLiquidDisposition] = Iterable.empty,
    releases: Iterable[BalanceReleaseId] = Iterable.empty
  ): Either[BalanceReservationError, BalanceReservationClaim] =
    BalanceReservationClaim.create(hash(source), scope, debits, credits, dispositions, releases)

  private def claim(
    source: Int,
    scope: BalanceScope = Dag,
    debits: Iterable[OutgoingDebit] = Iterable.empty,
    credits: Iterable[OrdinaryCredit] = Iterable.empty,
    dispositions: Iterable[NonLiquidDisposition] = Iterable.empty,
    releases: Iterable[BalanceReleaseId] = Iterable.empty
  ): BalanceReservationClaim = claimE(source, scope, debits, credits, dispositions, releases).toOption.get

  private def genesis(entries: (BalanceAccount, BigInt)*): BalanceReservationState =
    BalanceReservationState.genesis(Map(entries: _*)).toOption.get

  private def restore(
    balances: Map[BalanceAccount, BigInt],
    releases: Map[BalanceReleaseId, AuthenticatedRelease],
    operations: Set[BalanceOperationId] = Set.empty,
    consumedReleases: Set[BalanceReleaseId] = Set.empty
  ): BalanceReservationState =
    BalanceReservationState.restore(balances, releases, operations, consumedReleases).toOption.get

  private def authenticatedRelease(
    id: BalanceReleaseId,
    authorizedClaim: BalanceReservationClaim,
    credits: Iterable[ReleasedCredit],
    anchor: Int
  ): AuthenticatedRelease =
    AuthenticatedRelease.create(id, authorizedClaim, credits, TokenLock(hash(anchor))).toOption.get

  test("sequential transfer and token-lock reservation cannot spend 120 from 100") {
    val initial = genesis(dagAlice -> BigInt(100))
    val transfer = claim(1, debits = List(debit(dagAlice, 60)), credits = List(credit(dagBob, 60)))
    val lock = claim(
      2,
      debits = List(debit(dagAlice, 60)),
      dispositions = List(Reservation(Dag, ReservationSemantic.TokenLock, hash(200), amount(60)))
    )

    val afterTransfer = BalanceReservationLedger.attempt(initial, transfer).toOption.get
    val rejectedLock = BalanceReservationLedger.attempt(afterTransfer, lock)
    val correctedLock = claim(
      2,
      debits = List(debit(dagAlice, 40)),
      dispositions = List(Reservation(Dag, ReservationSemantic.TokenLock, hash(200), amount(40)))
    )

    expect(afterTransfer.balanceOf(dagAlice) == 40)
      .and(expect(afterTransfer.balanceOf(dagBob) == 60))
      .and(expect(rejectedLock == Left(InsufficientGrossBalance(dagAlice, 60, 40))))
      .and(expect(lock.operationId != correctedLock.operationId))
      .and(expect(BalanceReservationLedger.attempt(afterTransfer, correctedLock).toOption.exists(_.balanceOf(dagAlice) == 0)))
  }

  test("multi-account failure is atomic and ordinary credits cannot fund gross outgoing checks") {
    val initial = genesis(dagAlice -> BigInt(100), dagBob -> BigInt(10))
    val multiAccount = claim(
      3,
      debits = List(debit(dagAlice, 50), debit(dagBob, 20)),
      credits = List(credit(dagCarol, 70))
    )
    val selfCredit = claim(4, debits = List(debit(dagBob, 20)), credits = List(credit(dagBob, 20)))

    val rejected = BalanceReservationLedger.attempt(initial, multiAccount)

    expect(rejected == Left(InsufficientGrossBalance(dagBob, 20, 10)))
      .and(expect(BalanceReservationLedger.attempt(initial, selfCredit) == Left(InsufficientGrossBalance(dagBob, 20, 10))))
      .and(expect(initial.balanceOf(dagAlice) == 100))
      .and(expect(initial.balanceOf(dagBob) == 10))
      .and(expect(initial.consumedOperationIds.isEmpty))
  }

  test("every debit requires a liquid, reservation, or fee-sink destination and issuance is absent") {
    val bareDebit = claimE(5, debits = List(debit(dagAlice, 10)))
    val reservation = claimE(
      5,
      debits = List(debit(dagAlice, 10)),
      dispositions = List(Reservation(Dag, ReservationSemantic.AllowSpend, hash(201), amount(10)))
    )
    val fee = claimE(
      6,
      debits = List(debit(dagAlice, 10)),
      dispositions = List(FeeSink(Dag, bob, hash(202), amount(10)))
    )
    val unbackedCredit = claimE(7, credits = List(credit(dagBob, 1)))
    val empty = claimE(70)

    expect(bareDebit == Left(UnbalancedEconomicFlow(10, 0, 0)))
      .and(expect(reservation.isRight))
      .and(expect(fee.isRight))
      .and(expect(unbackedCredit == Left(UnbalancedEconomicFlow(0, 1, 0))))
      .and(expect(empty == Left(EmptyEconomicClaim(hash(70)))))
  }

  test("duplicate semantic dispositions and duplicate release IDs fail during canonical claim construction") {
    val id = releaseId(1)
    val dispositionA = Reservation(Dag, ReservationSemantic.TokenLock, hash(203), amount(5))
    val dispositionB = Reservation(Dag, ReservationSemantic.TokenLock, hash(203), amount(6))
    val duplicateDisposition = claimE(
      8,
      debits = List(debit(dagAlice, 11)),
      dispositions = List(dispositionB, dispositionA)
    )
    val duplicateRelease = claimE(9, releases = List(id, id))

    expect(duplicateDisposition == Left(DuplicateDispositionIdentity(dispositionA, dispositionB)))
      .and(expect(duplicateRelease == Left(DuplicateReleaseIdInClaim(id))))
  }

  test("an authenticated release funds only its exact operation and is credited once") {
    val id = releaseId(2)
    val authorized = claim(
      10,
      debits = List(debit(dagAlice, 10)),
      credits = List(credit(dagBob, 10)),
      releases = List(id)
    )
    val release = authenticatedRelease(id, authorized, List(ReleasedCredit(dagAlice, amount(10))), anchor = 300)
    val initial = restore(Map.empty, Map(id -> release))

    val accepted = BalanceReservationLedger.attempt(initial, authorized).toOption.get
    val replay = BalanceReservationLedger.attempt(accepted, authorized)
    val differentOperation = claim(11, releases = List(id))

    expect(accepted.balanceOf(dagAlice) == 0)
      .and(expect(accepted.balanceOf(dagBob) == 10))
      .and(expect(accepted.consumedReleaseIds == SortedSet(id)))
      .and(expect(replay == Left(DuplicateOperationId(authorized.operationId))))
      .and(expect(BalanceReservationLedger.attempt(accepted, differentOperation) == Left(DuplicateReleaseId(id))))
  }

  test("wrong-operation and cross-scope release attempts fail closed") {
    val id = releaseId(3)
    val authorized = claim(12, releases = List(id))
    val release = authenticatedRelease(id, authorized, List(ReleasedCredit(dagAlice, amount(10))), anchor = 301)
    val initial = restore(Map.empty, Map(id -> release))
    val wrongOperation = claim(13, releases = List(id))
    val currency = CurrencyId(address("currency-a"))
    val wrongScope = claim(14, scope = Metagraph(currency), releases = List(id))
    val mgAlice = BalanceAccount(Metagraph(currency), alice)
    val crossScopeRelease = AuthenticatedRelease.create(
      id,
      authorized,
      List(ReleasedCredit(mgAlice, amount(10))),
      TokenLock(hash(301))
    )

    expect(
      BalanceReservationLedger.attempt(initial, wrongOperation) ==
        Left(ReleaseOperationMismatch(id, wrongOperation.operationId, authorized.operationId))
    ).and(
      expect(
        BalanceReservationLedger.attempt(initial, wrongScope) ==
          Left(ReleaseScopeMismatch(id, wrongScope.scope, release.scope))
      )
    ).and(expect(crossScopeRelease == Left(ReleaseCreditScopeMismatch(Dag, mgAlice))))
  }

  test("a rejected exact release claim consumes neither its operation nor release") {
    val id = releaseId(4)
    val authorized = claim(
      15,
      debits = List(debit(dagAlice, 20)),
      credits = List(credit(dagBob, 20)),
      releases = List(id)
    )
    val release = authenticatedRelease(id, authorized, List(ReleasedCredit(dagAlice, amount(10))), anchor = 302)
    val underfunded = restore(Map.empty, Map(id -> release))
    val funded = restore(Map(dagAlice -> BigInt(10)), Map(id -> release))

    val rejected = BalanceReservationLedger.attempt(underfunded, authorized)
    val acceptedOnValidBase = BalanceReservationLedger.attempt(funded, authorized)

    expect(rejected == Left(InsufficientGrossBalance(dagAlice, 20, 10)))
      .and(expect(underfunded.consumedOperationIds.isEmpty))
      .and(expect(underfunded.consumedReleaseIds.isEmpty))
      .and(expect(acceptedOnValidBase.isRight))
  }

  test("operation IDs commit to normalized exact bodies and accepted source operations cannot equivocate") {
    val original = claim(16, debits = List(debit(dagAlice, 10)), credits = List(credit(dagBob, 10)))
    val reordered = claim(16, debits = List(debit(dagAlice, 10)), credits = List(credit(dagBob, 10)))
    val altered = claim(16, debits = List(debit(dagAlice, 10)), credits = List(credit(dagCarol, 10)))
    val accepted = BalanceReservationLedger.attempt(genesis(dagAlice -> BigInt(20)), original).toOption.get

    expect(original.operationId == reordered.operationId)
      .and(expect(original.operationId != altered.operationId))
      .and(
        expect(
          BalanceReservationLedger.attempt(accepted, altered) ==
            Left(OperationClaimEquivocation(hash(16), original.operationId, altered.operationId))
        )
      )
  }

  test("custom reverse orderings are canonicalized and zero entries have one representation") {
    val accounts = List(dagAlice, dagBob, dagCarol).sorted(BalanceAccount.ordering)
    val reverseAccounts = BalanceAccount.ordering.reverse
    val reverseBalances = List(dagCarol -> BigInt(30), dagAlice -> BigInt(10), dagBob -> BigInt(0))
      .foldLeft(SortedMap.empty[BalanceAccount, BigInt](reverseAccounts)) {
        case (acc, (account, balance)) =>
          acc.updated(account, balance)
      }
    val restored = BalanceReservationState.restore(reverseBalances, Map.empty, Set.empty, Set.empty).toOption.get
    val forward = claim(
      17,
      debits = List(debit(dagAlice, 3), debit(dagAlice, 7)),
      credits = List(credit(dagCarol, 10)),
      dispositions = List(Reservation(Dag, ReservationSemantic.TokenLock, hash(400), amount(0)))
    )
    val reversed = claim(
      17,
      debits = List(debit(dagAlice, 7), debit(dagAlice, 3), debit(dagBob, 0)),
      credits = List(credit(dagCarol, 10), credit(dagBob, 0))
    )

    expect(restored.balances.keys.toList == accounts.filterNot(_ == dagBob))
      .and(expect(!restored.balances.contains(dagBob)))
      .and(expect(forward.operationId == reversed.operationId))
      .and(expect(forward.outgoingDebits == SortedMap(dagAlice -> BigInt(10))))
      .and(expect(forward.nonLiquidDispositions.isEmpty))
  }

  test("invalid restart balances report the canonical first account, independent of input ordering") {
    val reverse = BalanceAccount.ordering.reverse
    val invalid = List(dagAlice -> BigInt(-1), dagCarol -> BigInt(-2))
      .foldLeft(SortedMap.empty[BalanceAccount, BigInt](reverse)) {
        case (acc, (account, balance)) =>
          acc.updated(account, balance)
      }
    val expectedFirst = List(dagAlice, dagCarol).min(BalanceAccount.ordering)
    val expectedValue = invalid(expectedFirst)

    expect(
      BalanceReservationState.genesis(invalid) ==
        Left(InitialBalanceOutOfRange(expectedFirst, expectedValue))
    )
  }

  test("scope namespaces isolate DAG and two metagraph balances") {
    val currencyA = CurrencyId(address("currency-a"))
    val currencyB = CurrencyId(address("currency-b"))
    val scopeA = Metagraph(currencyA)
    val mgAliceA = BalanceAccount(scopeA, alice)
    val mgBobA = BalanceAccount(scopeA, bob)
    val mgAliceB = BalanceAccount(Metagraph(currencyB), alice)
    val initial = genesis(dagAlice -> BigInt(100), mgAliceA -> BigInt(100), mgAliceB -> BigInt(100))
    val mgTransfer = claim(
      18,
      scope = scopeA,
      debits = List(debit(mgAliceA, 20)),
      credits = List(credit(mgBobA, 20))
    )
    val result = BalanceReservationLedger.attempt(initial, mgTransfer).toOption.get

    expect(result.balanceOf(dagAlice) == 100)
      .and(expect(result.balanceOf(mgAliceA) == 80))
      .and(expect(result.balanceOf(mgBobA) == 20))
      .and(expect(result.balanceOf(mgAliceB) == 100))
  }

  test("Long.MaxValue overflow rejects atomically") {
    val max = BigInt(Long.MaxValue)
    val initial = genesis(dagAlice -> BigInt(1), dagBob -> max)
    val overflow = claim(19, debits = List(debit(dagAlice, 1)), credits = List(credit(dagBob, 1)))
    val result = BalanceReservationLedger.attempt(initial, overflow)

    expect(result == Left(ProjectedBalanceOutOfRange(dagBob, max + 1)))
      .and(expect(initial.balanceOf(dagAlice) == 1))
      .and(expect(initial.balanceOf(dagBob) == max))
      .and(expect(initial.consumedOperationIds.isEmpty))
  }

  test("validated restart preserves replay protection and rejects inconsistent histories") {
    val id = releaseId(5)
    val authorized = claim(20, releases = List(id))
    val release = authenticatedRelease(id, authorized, List(ReleasedCredit(dagAlice, amount(10))), anchor = 303)
    val accepted = BalanceReservationLedger.attempt(restore(Map.empty, Map(id -> release)), authorized).toOption.get

    val reverseRegistry = SortedMap.empty[BalanceReleaseId, AuthenticatedRelease](BalanceReleaseId.ordering.reverse) + (id -> release)
    val reverseOperations = SortedSet.empty[BalanceOperationId](BalanceOperationId.ordering.reverse) ++ accepted.consumedOperationIds
    val reverseReleases = SortedSet.empty[BalanceReleaseId](BalanceReleaseId.ordering.reverse) ++ accepted.consumedReleaseIds
    val restarted = BalanceReservationState
      .restore(accepted.balances, reverseRegistry, reverseOperations, reverseReleases)
      .toOption
      .get
    val unknown = releaseId(99)

    expect(BalanceReservationLedger.attempt(restarted, authorized) == Left(DuplicateOperationId(authorized.operationId)))
      .and(expect(restarted.releaseRegistry.keys.toList == List(id)))
      .and(expect(restarted.consumedOperationIds == SortedSet(authorized.operationId)))
      .and(
        expect(
          BalanceReservationState.restore(Map.empty, Map.empty, Set.empty, Set(unknown)) ==
            Left(ConsumedUnknownRelease(unknown))
        )
      )
      .and(
        expect(
          BalanceReservationState.restore(Map.empty, Map(id -> release), Set.empty, Set(id)) ==
            Left(ConsumedReleaseOperationMissing(id, authorized.operationId))
        )
      )
      .and(
        expect(
          BalanceReservationState.restore(Map.empty, Map(id -> release), Set(authorized.operationId), Set.empty) ==
            Left(AcceptedOperationMissingReleaseConsumption(id, authorized.operationId))
        )
      )
  }

  test("validated restart rejects malformed release credits and canonicalizes their ordering") {
    val id = releaseId(6)
    val authorized = claim(21, releases = List(id))
    val valid = authenticatedRelease(id, authorized, List(ReleasedCredit(dagAlice, amount(10))), anchor = 304)
    val currency = CurrencyId(address("currency-restore"))
    val mgAlice = BalanceAccount(Metagraph(currency), alice)
    val wrongScope = valid.copy(credits = SortedMap(mgAlice -> BigInt(10)))
    val negative = valid.copy(credits = SortedMap(dagAlice -> BigInt(-1)))
    val empty = valid.copy(credits = SortedMap.empty)
    val reverse = BalanceAccount.ordering.reverse
    val reverseCredits = List(dagBob -> BigInt(4), dagAlice -> BigInt(6)).foldLeft(SortedMap.empty[BalanceAccount, BigInt](reverse)) {
      case (acc, entry) => acc + entry
    }
    val normalized = valid.copy(credits = reverseCredits)
    val restored = BalanceReservationState.restore(Map.empty, Map(id -> normalized), Set.empty, Set.empty).toOption.get

    expect(
      BalanceReservationState.restore(Map.empty, Map(id -> wrongScope), Set.empty, Set.empty) ==
        Left(ReleaseCreditScopeMismatch(Dag, mgAlice))
    ).and(
      expect(
        BalanceReservationState.restore(Map.empty, Map(id -> negative), Set.empty, Set.empty) ==
          Left(RestoredReleaseCreditOutOfRange(id, dagAlice, -1))
      )
    ).and(
      expect(
        BalanceReservationState.restore(Map.empty, Map(id -> empty), Set.empty, Set.empty) ==
          Left(EmptyAuthenticatedRelease(id))
      )
    ).and(expect(restored.releaseRegistry(id).credits.keys.toList == List(dagAlice, dagBob).sorted(BalanceAccount.ordering)))
  }
}
