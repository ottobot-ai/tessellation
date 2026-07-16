package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.{Dag, Metagraph}
import io.constellationnetwork.node.shared.domain.economics.ReferenceDecision.{Accepted, Rejected}
import io.constellationnetwork.node.shared.domain.economics.ReferenceInput.{AllowSpendCreate, Transfer}
import io.constellationnetwork.node.shared.domain.economics.ReferenceRejection._
import io.constellationnetwork.node.shared.domain.economics.ReferenceWrite.{AllowSpendReference => AllowSpendReferenceWrite, Balance => BalanceWrite, _}
import io.constellationnetwork.node.shared.domain.economics.TransferLane.{CurrencyCl1, NativeGl1}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import weaver.FunSuite

object V4EconomicReferenceAllowSpendSuite extends FunSuite {
  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val alice = address("allow-alice")
  private val bob = address("allow-bob")
  private val carol = address("allow-carol")
  private val mgA = address("allow-metagraph-a")
  private val mgB = address("allow-metagraph-b")
  private val defaultDomain = ReferenceDomain(hash(101), hash(102), hash(103))
  private val otherDomain = ReferenceDomain(hash(101), hash(102), hash(104))
  private val defaultEpochWindow = ReferenceAllowSpendEpochWindow(
    currentEpochProgress = BigInt(100),
    minOffset = BigInt(5),
    maxOffset = BigInt(20)
  )

  private def context(
    lane: TransferLane,
    executionDomain: ReferenceDomain = defaultDomain,
    locked: SortedSet[Address] = SortedSet.empty[Address],
    epochWindow: Option[ReferenceAllowSpendEpochWindow] = Some(defaultEpochWindow)
  ): ReferenceContext =
    ReferenceContext(executionDomain, lane, locked, epochWindow)

  private def account(scope: ReferenceBalanceScope, address: Address): ReferenceBalanceAccount =
    ReferenceBalanceAccount(scope, address)

  private def state(entries: (ReferenceBalanceAccount, BigInt)*): ReferenceState =
    ReferenceState.initial(Map(entries: _*)).toOption.get

  private def preimage(
    lane: TransferLane,
    source: Address = alice,
    destination: Address = bob,
    amount: BigInt = BigInt(60),
    fee: BigInt = BigInt(0),
    parent: StructuralAllowSpendReference = StructuralAllowSpendReference.genesis,
    lastValidEpochProgress: BigInt = BigInt(110),
    approvers: Option[Vector[Address]] = None,
    executionDomain: ReferenceDomain = defaultDomain
  ): AllowSpendPreimage = {
    val atom = AllowSpendAtom(
      executionDomain,
      lane,
      source,
      destination,
      amount,
      fee,
      lastValidEpochProgress,
      approvers.getOrElse(Vector(destination))
    )
    AllowSpendPreimage(parent, atom)
  }

  private def create(preimage: AllowSpendPreimage, signer: Address = alice): AllowSpendCreate =
    AllowSpendCreate(preimage, StructurallyBoundAllowSpendSourceProof(signer, preimage))

  private def execute(
    context: ReferenceContext,
    base: ReferenceState,
    inputs: ReferenceInput*
  ): ReferenceExecution =
    V4EconomicReferenceInterpreter.execute(context, base, inputs.toVector).toOption.get

  test("zero-fee native allow-spend creation reserves principal, advances its own reference, and never credits destination") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val p = preimage(NativeGl1)
    val identity = AllowSpendSemanticIdentity.derive(p)
    val successor = StructuralAllowSpendReference(BigInt(1), Vector(p.atom))
    val result = execute(context(NativeGl1), base, create(p))
    val accepted = result.decisions.head.asInstanceOf[Accepted]
    val reservation = result.finalState.allowSpendReservationOf(identity)

    expect(result.acceptedIds == Vector(identity))
      .and(expect(result.rejected.isEmpty))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 0))
      .and(expect(result.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(NativeGl1, alice)) == successor))
      .and(expect(reservation.exists(_.amount == 60)))
      .and(expect(reservation.exists(_.destination == bob)))
      .and(expect(result.conservedTotals == SortedMap[ReferenceBalanceScope, BigInt](Dag -> BigInt(100))))
      .and(expect(accepted.writes.collect { case write: BalanceWrite => write.account } == Vector(account(Dag, alice))))
      .and(expect(accepted.writes.exists(_.isInstanceOf[AllowSpendReservationCreated])))
      .and(expect(accepted.writes.exists(_.isInstanceOf[AllowSpendReferenceWrite])))
      .and(expect(accepted.writes.contains(ReplayIdentity(identity))))
  }

  test("currency allow-spend creation changes only its exact owning-metagraph namespace") {
    val laneA = CurrencyCl1(mgA)
    val laneB = CurrencyCl1(mgB)
    val scopeA = Metagraph(mgA)
    val scopeB = Metagraph(mgB)
    val base = state(
      account(Dag, alice) -> BigInt(100),
      account(scopeA, alice) -> BigInt(100),
      account(scopeB, alice) -> BigInt(100)
    )
    val p = preimage(laneA, amount = 25)
    val identity = AllowSpendSemanticIdentity.derive(p)
    val result = execute(context(laneA), base, create(p))

    expect(result.finalState.balanceOf(account(scopeA, alice)) == 75)
      .and(expect(result.finalState.balanceOf(account(scopeA, bob)) == 0))
      .and(expect(result.finalState.balanceOf(account(scopeB, alice)) == 100))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 100))
      .and(expect(result.finalState.allowSpendReservationOf(identity).exists(_.scope == scopeA)))
      .and(expect(result.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(laneB, alice)) == StructuralAllowSpendReference.genesis))
      .and(expect(result.conservedTotals(scopeA) == 100))
      .and(expect(result.conservedTotals(scopeB) == 100))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("allow-spend proof binds the complete preimage and exclusively binds its source") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val expected = preimage(NativeGl1)
    val altered = expected.copy(atom = expected.atom.copy(destination = carol))
    val wrongBinding = AllowSpendCreate(expected, StructurallyBoundAllowSpendSourceProof(alice, altered))
    val wrongSigner = AllowSpendCreate(expected, StructurallyBoundAllowSpendSourceProof(carol, expected))
    val invalidApprovers = preimage(NativeGl1, approvers = Some(Vector(carol)))
    val bindingResult = execute(context(NativeGl1), base, wrongBinding)
    val signerResult = execute(context(NativeGl1), base, wrongSigner)
    val approverResult = execute(context(NativeGl1), base, create(invalidApprovers))

    expect(bindingResult.rejected.head.reason == AllowSpendProofPreimageMismatch(expected, altered))
      .and(expect(bindingResult.finalState == base))
      .and(expect(signerResult.rejected.head.reason == AllowSpendProofSignerMismatch(alice, carol)))
      .and(expect(signerResult.finalState == base))
      .and(expect(approverResult.rejected.head.reason == InvalidAllowSpendApprovers(bob, Vector(carol))))
      .and(expect(approverResult.finalState == base))
  }

  test("domain, exact lane, and locked-source checks fail before state mutation") {
    val laneA = CurrencyCl1(mgA)
    val laneB = CurrencyCl1(mgB)
    val base = state(account(Metagraph(mgA), alice) -> BigInt(100), account(Metagraph(mgB), alice) -> BigInt(100))
    val wrongDomain = preimage(laneA, executionDomain = otherDomain)
    val wrongLane = preimage(laneB)
    val domainResult = execute(context(laneA), base, create(wrongDomain))
    val laneResult = execute(context(laneA), base, create(wrongLane))
    val lockedResult = execute(context(laneA, locked = SortedSet(alice)), base, create(preimage(laneA)))

    expect(domainResult.rejected.head.reason == UnexpectedDomain(defaultDomain, otherDomain))
      .and(expect(laneResult.rejected.head.reason == UnexpectedLane(laneA, laneB)))
      .and(expect(lockedResult.rejected.head.reason == LockedSource(alice)))
      .and(expect(domainResult.finalState == base))
      .and(expect(laneResult.finalState == base))
      .and(expect(lockedResult.finalState == base))
  }

  test("exact replay, same-parent sibling, and phantom parent cannot advance an allow-spend chain") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val first = preimage(NativeGl1, amount = 20)
    val firstIdentity = AllowSpendSemanticIdentity.derive(first)
    val acceptedState = execute(context(NativeGl1), base, create(first)).finalState
    val expectedParent = StructuralAllowSpendReference(BigInt(1), Vector(first.atom))
    val replay = execute(context(NativeGl1), acceptedState, create(first))
    val sibling = preimage(NativeGl1, destination = carol, amount = 20)
    val siblingResult = execute(context(NativeGl1), acceptedState, create(sibling))
    val phantom = preimage(NativeGl1, destination = carol, amount = 99)
    val phantomParent = StructuralAllowSpendReference(BigInt(1), Vector(phantom.atom))
    val phantomChild = preimage(NativeGl1, destination = carol, amount = 1, parent = phantomParent)
    val phantomResult = execute(context(NativeGl1), base, create(phantomChild))

    expect(replay.rejected.head.reason == DuplicateSemanticIdentity(firstIdentity))
      .and(expect(replay.finalState == acceptedState))
      .and(expect(siblingResult.rejected.head.reason == AllowSpendParentReferenceMismatch(expectedParent, StructuralAllowSpendReference.genesis)))
      .and(expect(siblingResult.finalState == acceptedState))
      .and(expect(phantomResult.rejected.head.reason == AllowSpendParentReferenceMismatch(StructuralAllowSpendReference.genesis, phantomParent)))
      .and(expect(phantomResult.finalState == base))
  }

  test("a valid successor advances once while an insufficient sibling leaves the accepted prefix intact") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val first = preimage(NativeGl1, amount = 60)
    val parent = StructuralAllowSpendReference(BigInt(1), Vector(first.atom))
    val insufficient = preimage(NativeGl1, destination = carol, amount = 50, parent = parent)
    val successor = preimage(NativeGl1, destination = carol, amount = 30, parent = parent)
    val result = execute(context(NativeGl1), base, create(first), create(insufficient), create(successor))
    val finalReference = StructuralAllowSpendReference(BigInt(2), Vector(first.atom, successor.atom))

    expect(result.decisions.map(_.inputIndex) == Vector(0, 1, 2))
      .and(expect(result.decisions.head.isInstanceOf[Accepted]))
      .and(expect(result.decisions(1).isInstanceOf[Rejected]))
      .and(expect(result.decisions(2).isInstanceOf[Accepted]))
      .and(expect(result.rejected.head.reason == InsufficientBalance(account(Dag, alice), required = 50, available = 40)))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 10))
      .and(expect(result.finalState.activeAllowSpendReservations.map(_.amount) == Vector(BigInt(60), BigInt(30))))
      .and(expect(result.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(NativeGl1, alice)) == finalReference))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("epoch eligibility uses only the explicit checked reference window") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val missing = execute(context(NativeGl1, epochWindow = None), base, create(preimage(NativeGl1)))
    val expired = execute(context(NativeGl1), base, create(preimage(NativeGl1, lastValidEpochProgress = 100)))
    val below = execute(context(NativeGl1), base, create(preimage(NativeGl1, lastValidEpochProgress = 104)))
    val above = execute(context(NativeGl1), base, create(preimage(NativeGl1, lastValidEpochProgress = 121)))
    val invalidWindow = ReferenceAllowSpendEpochWindow(100, 20, 5)
    val invalid = execute(context(NativeGl1, epochWindow = Some(invalidWindow)), base, create(preimage(NativeGl1)))
    val overflowWindow = ReferenceAllowSpendEpochWindow(BigInt(Long.MaxValue), 1, 1)
    val overflow = execute(context(NativeGl1, epochWindow = Some(overflowWindow)), base, create(preimage(NativeGl1)))

    expect(missing.rejected.head.reason == MissingAllowSpendEpochWindow)
      .and(expect(expired.rejected.head.reason == AllowSpendAlreadyExpired(100, 100)))
      .and(expect(below.rejected.head.reason == AllowSpendEpochOutsideWindow(104, 105, 120)))
      .and(expect(above.rejected.head.reason == AllowSpendEpochOutsideWindow(121, 105, 120)))
      .and(expect(invalid.rejected.head.reason == InvalidAllowSpendEpochWindow(invalidWindow)))
      .and(expect(overflow.rejected.head.reason == InvalidAllowSpendEpochWindow(overflowWindow)))
      .and(expect(Vector(missing, expired, below, above, invalid, overflow).forall(_.finalState == base)))
  }

  test("fee policy, scalar bounds, malformed parent, and insufficient source fail atomically") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val malformedParent = StructuralAllowSpendReference(BigInt(2), Vector(preimage(NativeGl1).atom))
    val cases = Vector[(AllowSpendPreimage, ReferenceRejection)](
      preimage(NativeGl1, fee = 1) -> FeeDispositionUnfrozen(1),
      preimage(NativeGl1, fee = -1) -> ScalarOutOfRange("fee", -1),
      preimage(NativeGl1, amount = 0) -> AmountNotPositive(0),
      preimage(NativeGl1, amount = -1) -> AmountNotPositive(-1),
      preimage(NativeGl1, amount = BigInt(Long.MaxValue) + 1) -> ScalarOutOfRange("amount", BigInt(Long.MaxValue) + 1),
      preimage(NativeGl1, lastValidEpochProgress = BigInt(Long.MaxValue) + 1) -> ScalarOutOfRange(
        "lastValidEpochProgress",
        BigInt(Long.MaxValue) + 1
      ),
      preimage(NativeGl1, amount = 101) -> InsufficientBalance(account(Dag, alice), required = 101, available = 100),
      preimage(NativeGl1, parent = malformedParent) -> MalformedAllowSpendParentReference(2, 1)
    )

    val results = cases.map {
      case (p, expected) =>
        val result = execute(context(NativeGl1), base, create(p))
        expect(result.rejected.head.reason == expected).and(expect(result.finalState == base))
    }

    results.reduce(_ and _)
  }

  test("an allow-spend reservation cannot phantom-fund its destination") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val reservation = preimage(NativeGl1, amount = 100)
    val unfunded = preimage(NativeGl1, source = bob, destination = carol, amount = 1)
    val result = execute(context(NativeGl1), base, create(reservation), create(unfunded, signer = bob))

    expect(result.decisions.head.isInstanceOf[Accepted])
      .and(expect(result.decisions(1).isInstanceOf[Rejected]))
      .and(expect(result.rejected.head.reason == InsufficientBalance(account(Dag, bob), required = 1, available = 0)))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 0))
      .and(expect(result.finalState.balanceOf(account(Dag, carol)) == 0))
      .and(expect(result.finalState.activeAllowSpendReservations.map(_.amount) == Vector(BigInt(100))))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("transfer and allow-spend creation share one ordered spendable-balance accumulator") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val transferPreimage = TransferPreimage(
      StructuralReference.genesis,
      TransferAtom(defaultDomain, NativeGl1, alice, bob, amount = 60, fee = 0, salt = 1L)
    )
    val transfer = Transfer(transferPreimage, StructurallyBoundSourceProof(alice, transferPreimage))
    val reservation = preimage(NativeGl1, destination = carol, amount = 50)
    val result = execute(context(NativeGl1), base, transfer, create(reservation))

    expect(result.decisions.head.isInstanceOf[Accepted])
      .and(expect(result.decisions(1).isInstanceOf[Rejected]))
      .and(expect(result.rejected.head.reason == InsufficientBalance(account(Dag, alice), required = 50, available = 40)))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 60))
      .and(expect(result.finalState.balanceOf(account(Dag, carol)) == 0))
      .and(expect(result.finalState.activeAllowSpendReservations.isEmpty))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("a transfer from reservation-bearing state preserves the reserved principal in conservation") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val reservation = preimage(NativeGl1, destination = carol, amount = 20)
    val reservationState = execute(context(NativeGl1), base, create(reservation)).finalState
    val transferPreimage = TransferPreimage(
      StructuralReference.genesis,
      TransferAtom(defaultDomain, NativeGl1, alice, bob, amount = 30, fee = 0, salt = 2L)
    )
    val transfer = Transfer(transferPreimage, StructurallyBoundSourceProof(alice, transferPreimage))
    val result = execute(context(NativeGl1), reservationState, transfer)

    expect(result.decisions.head.isInstanceOf[Accepted])
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 50))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 30))
      .and(expect(result.finalState.balanceOf(account(Dag, carol)) == 0))
      .and(expect(result.finalState.activeAllowSpendReservations.map(_.amount) == Vector(BigInt(20))))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("allow-spend structural identity changes with every authority-bearing field") {
    val original = preimage(NativeGl1)
    val otherParent = StructuralAllowSpendReference(BigInt(1), Vector(original.atom))
    val variants = Vector(
      original.copy(parent = otherParent),
      original.copy(atom = original.atom.copy(domain = otherDomain)),
      original.copy(atom = original.atom.copy(lane = CurrencyCl1(mgA))),
      original.copy(atom = original.atom.copy(source = carol)),
      original.copy(atom = original.atom.copy(destination = carol)),
      original.copy(atom = original.atom.copy(amount = original.atom.amount + 1)),
      original.copy(atom = original.atom.copy(fee = original.atom.fee + 1)),
      original.copy(atom = original.atom.copy(lastValidEpochProgress = original.atom.lastValidEpochProgress + 1)),
      original.copy(atom = original.atom.copy(approvers = Vector.empty))
    )
    val identity = AllowSpendSemanticIdentity.derive(original)

    expect(variants.forall(variant => AllowSpendSemanticIdentity.derive(variant) != identity))
  }
}
