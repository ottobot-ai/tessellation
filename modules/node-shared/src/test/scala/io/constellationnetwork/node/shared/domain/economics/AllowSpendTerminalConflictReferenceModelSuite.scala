package io.constellationnetwork.node.shared.domain.economics

import io.constellationnetwork.node.shared.domain.economics.AllowSpendTerminalConflictReferenceModel.Claim.{Consume, Created, Expiry}
import io.constellationnetwork.node.shared.domain.economics.AllowSpendTerminalConflictReferenceModel.Conflict._
import io.constellationnetwork.node.shared.domain.economics.AllowSpendTerminalConflictReferenceModel.ReservationBindingField._
import io.constellationnetwork.node.shared.domain.economics.AllowSpendTerminalConflictReferenceModel._
import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.{Dag, Metagraph}
import io.constellationnetwork.node.shared.domain.economics.TransferLane.{CurrencyCl1, NativeGl1}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import weaver.FunSuite

object AllowSpendTerminalConflictReferenceModelSuite extends FunSuite {
  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val alice = address("terminal-alice")
  private val bob = address("terminal-bob")
  private val carol = address("terminal-carol")
  private val metagraph = address("terminal-metagraph")
  private val domain = ReferenceDomain(hash(501), hash(502), hash(503))

  private def epoch(value: BigInt): TerminalEpoch = TerminalEpoch.from(value).toOption.get

  private def reservation(
    lane: TransferLane = NativeGl1,
    source: Address = alice,
    destination: Address = bob,
    amount: BigInt = BigInt(60),
    lastValidEpochProgress: BigInt = BigInt(100)
  ): ReferenceAllowSpendReservation = {
    val preimage = AllowSpendPreimage(
      StructuralAllowSpendReference.genesis,
      AllowSpendAtom(
        domain,
        lane,
        source,
        destination,
        amount,
        fee = BigInt(0),
        lastValidEpochProgress,
        Vector(destination)
      )
    )

    ReferenceAllowSpendReservation(
      AllowSpendSemanticIdentity.derive(preimage),
      lane.scope,
      source,
      destination,
      amount,
      lastValidEpochProgress,
      Vector(destination)
    )
  }

  test("normalization observes creation without manufacturing a terminal effect") {
    val created = reservation()
    val result = normalize(epoch(100), Vector(Created(created)))

    expect(result.exists(_.byIdentity(created.identity).creation.contains(Created(created))))
      .and(expect(result.exists(_.terminalCandidateCountByIdentity(created.identity) == 0)))
  }

  test("one consume is only a normalized candidate and does not activate consume semantics") {
    val active = reservation()
    val result = normalize(epoch(100), Vector(Consume(active)))

    expect(result.exists(_.byIdentity(active.identity).consume.contains(Consume(active))))
      .and(expect(result.exists(_.terminalCandidateCountByIdentity(active.identity) == 1)))
  }

  test("a v4-expiry-eligible consume is unresolved without typed exhaustive expiry enumeration") {
    val eligible = reservation(lastValidEpochProgress = 99)
    val result = normalize(epoch(100), Vector(Consume(eligible)))

    expect(
      result == Left(
        UnresolvedTerminalOrder(
          Map(eligible.identity -> Set[Conflict](EligibleConsumeWithoutExhaustiveExpiryEnumeration))
        )
      )
    )
  }

  test("current v4 expiry is ineligible at exact last-valid equality and eligible only after it") {
    val active = reservation(lastValidEpochProgress = 100)
    val atBoundary = normalize(epoch(100), Vector(Expiry(active)))
    val afterBoundary = normalize(epoch(101), Vector(Expiry(active)))

    expect(atBoundary.exists(_.byIdentity(active.identity).expiry.exists(!_._2.eligible)))
      .and(expect(atBoundary.exists(_.terminalCandidateCountByIdentity(active.identity) == 0)))
      .and(expect(afterBoundary.exists(_.byIdentity(active.identity).expiry.exists(_._2.eligible))))
      .and(expect(afterBoundary.exists(_.terminalCandidateCountByIdentity(active.identity) == 1)))
  }

  test("consume versus eligible expiry has the same semantic conflict for every permutation, without claiming canonical bytes") {
    val active = reservation(lastValidEpochProgress = 100)
    val claims: Vector[Claim] = Vector(Consume(active), Expiry(active))
    val results = claims.permutations.map(permutation => normalize(epoch(101), permutation)).toSet
    val expected = Left(
      UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](ConsumeVersusEligibleExpiry)))
    )

    expect(results == Set(expected))
  }

  test("same-batch creation versus consume or expiry remains typed and unresolved") {
    val active = reservation(lastValidEpochProgress = 100)
    val createConsume = normalize(epoch(100), Vector(Created(active), Consume(active)))
    val createExpiry = normalize(epoch(101), Vector(Created(active), Expiry(active)))

    expect(
      createConsume == Left(
        UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](CreationVersusTerminal)))
      )
    ).and(
      expect(
        createExpiry == Left(
          UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](CreationVersusTerminal)))
        )
      )
    )
  }

  test("duplicate consume and expiry claims fail closed instead of selecting by arrival order") {
    val active = reservation()
    val duplicateConsumes = normalize(epoch(100), Vector(Consume(active), Consume(active)))
    val duplicateExpiries = normalize(epoch(101), Vector(Expiry(active), Expiry(active)))

    expect(
      duplicateConsumes == Left(
        UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](MultipleConsumeClaims(2))))
      )
    ).and(
      expect(
        duplicateExpiries == Left(
          UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](MultipleExpiryClaims(2))))
        )
      )
    )
  }

  test("duplicate creation claims return the typed creation conflict") {
    val active = reservation()
    val result = normalize(epoch(100), Vector(Created(active), Created(active)))

    expect(
      result == Left(
        UnresolvedTerminalOrder(Map(active.identity -> Set[Conflict](MultipleCreationClaims(2))))
      )
    )
  }

  test("claims that reuse an identity with different reservation data fail typed and closed") {
    val active = reservation(lastValidEpochProgress = 120)
    val inconsistent = active.copy(amount = active.amount + 1)
    val result = normalize(epoch(100), Vector(Consume(active), Expiry(inconsistent)))

    expect(
      result == Left(
        UnresolvedTerminalOrder(
          Map(
            active.identity -> Set[Conflict](
              InconsistentReservationClaims(2),
              IdentityReservationMismatch(Vector(Amount))
            )
          )
        )
      )
    )
  }

  test("every reservation field is checked against its supplied identity for a single claim") {
    val active = reservation(lastValidEpochProgress = 120)
    val cases: Vector[(ReservationBindingField, Claim)] = Vector(
      ScopeFromIdentityLane -> Created(active.copy(scope = Metagraph(metagraph))),
      Source -> Consume(active.copy(source = carol)),
      Destination -> Expiry(active.copy(destination = carol)),
      Amount -> Created(active.copy(amount = active.amount + 1)),
      LastValidEpochProgress -> Consume(active.copy(lastValidEpochProgress = active.lastValidEpochProgress + 1)),
      Approvers -> Expiry(active.copy(approvers = Vector(carol)))
    )

    cases.foldLeft(expect(true)) {
      case (result, (field, claim)) =>
        result.and(
          expect(
            normalize(epoch(100), Vector(claim)) == Left(
              UnresolvedTerminalOrder(
                Map(active.identity -> Set[Conflict](IdentityReservationMismatch(Vector(field))))
              )
            )
          )
        )
    }
  }

  test("one conflict rejects the whole batch without exposing a normalized partial result") {
    val conflicted = reservation(lastValidEpochProgress = 100)
    val unrelated = reservation(
      lane = CurrencyCl1(metagraph),
      source = carol,
      destination = alice,
      amount = 20,
      lastValidEpochProgress = 120
    )
    val result = normalize(
      epoch(101),
      Vector(Consume(unrelated), Consume(conflicted), Expiry(conflicted))
    )

    expect(
      result == Left(
        UnresolvedTerminalOrder(Map(conflicted.identity -> Set[Conflict](ConsumeVersusEligibleExpiry)))
      )
    )
  }

  test("nonconflicting semantic results are permutation-independent without asserting canonical traversal or bytes") {
    val consumed = reservation()
    val notYetExpired = reservation(
      lane = CurrencyCl1(metagraph),
      source = carol,
      destination = alice,
      amount = 20,
      lastValidEpochProgress = 120
    )
    val claims: Vector[Claim] = Vector(Consume(consumed), Expiry(notYetExpired))
    val results = claims.permutations.map(permutation => normalize(epoch(100), permutation)).toSet

    expect(results.size == 1)
      .and(expect(results.head.exists(_.terminalCandidateCountByIdentity.values.forall(_ <= 1))))
      .and(expect(results.head.exists(_.byIdentity(notYetExpired.identity).expiry.exists(!_._2.eligible))))
  }

  test("consume and expiry stay unsupported, and cancel is currently unmodeled and unsupported") {
    val manifestIds = V4EconomicGrammarManifest.operations.iterator.map(_.id).toSet
    val blockedIds = Set("ECO-ALLOW-CONSUME", "ECO-ALLOW-EXPIRY")
    val unmodeledCancelId = "ECO-ALLOW-CANCEL"

    expect(blockedIds.forall(SupportedReferenceOperationId.fromValue(_).isEmpty))
      .and(expect(blockedIds.forall(UnsupportedReferenceOperationId.fromValue(_).isRight)))
      .and(expect(!manifestIds.contains(unmodeledCancelId)))
      .and(expect(SupportedReferenceOperationId.fromValue(unmodeledCancelId).isEmpty))
      .and(expect(UnsupportedReferenceOperationId.fromValue(unmodeledCancelId).isRight))
  }

  test("terminal epochs are bounded before normalization") {
    expect(TerminalEpoch.from(-1).isLeft)
      .and(expect(TerminalEpoch.from(BigInt(Long.MaxValue) + 1).isLeft))
      .and(expect(TerminalEpoch.from(0).isRight))
      .and(expect(TerminalEpoch.from(Long.MaxValue).isRight))
  }

  test("reservation scope is retained exactly during normalization") {
    val native = reservation()
    val currency = reservation(lane = CurrencyCl1(metagraph), source = carol)
    val result = normalize(epoch(100), Vector(Consume(native), Consume(currency)))

    expect(result.exists(_.byIdentity(native.identity).reservation.scope == Dag))
      .and(expect(result.exists(_.byIdentity(currency.identity).reservation.scope == Metagraph(metagraph))))
  }
}
