package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.InputProvenance.CustomData
import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.{Dag, Metagraph}
import io.constellationnetwork.node.shared.domain.economics.ReferenceDecision.{Accepted, Rejected}
import io.constellationnetwork.node.shared.domain.economics.ReferenceInput.{Transfer, UnsupportedManifestOperation}
import io.constellationnetwork.node.shared.domain.economics.ReferenceRejection._
import io.constellationnetwork.node.shared.domain.economics.TransferLane.{CurrencyCl1, NativeGl1}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import shapeless.test.illTyped
import weaver.FunSuite

object V4EconomicReferenceInterpreterSuite extends FunSuite {
  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val alice = address("alice")
  private val bob = address("bob")
  private val carol = address("carol")
  private val mgA = address("metagraph-a")
  private val mgB = address("metagraph-b")
  private val defaultDomain = ReferenceDomain(hash(1), hash(2), hash(3))
  private val otherDomain = ReferenceDomain(hash(1), hash(2), hash(4))

  private def context(
    lane: TransferLane,
    executionDomain: ReferenceDomain = defaultDomain,
    locked: SortedSet[Address] = SortedSet.empty[Address]
  ): ReferenceContext = ReferenceContext(executionDomain, lane, locked)

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
    parent: StructuralReference = StructuralReference.genesis,
    salt: Long = 1L,
    executionDomain: ReferenceDomain = defaultDomain
  ): TransferPreimage =
    TransferPreimage(parent, TransferAtom(executionDomain, lane, source, destination, amount, fee, salt))

  private def transfer(preimage: TransferPreimage, signer: Address = alice): Transfer =
    Transfer(preimage, StructurallyBoundSourceProof(signer, preimage))

  private def execute(
    context: ReferenceContext,
    base: ReferenceState,
    inputs: ReferenceInput*
  ): ReferenceExecution =
    V4EconomicReferenceInterpreter.execute(context, base, inputs.toVector).toOption.get

  test("reference state construction is opaque even to same-package callers") {
    illTyped("""new ReferenceState(None, null, null, null)""")
    illTyped("""null.asInstanceOf[ReferenceState].copy()""")
    illTyped("""ReferenceState.apply(null, null, null, null)""")
    illTyped("""ReferenceState.accepted(null, null, null, null, null)""")

    expect(true)
  }

  test("zero-fee native transfer derives identity, reference, writes, and conserved totals") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val p = preimage(NativeGl1)
    val result = execute(context(NativeGl1), base, transfer(p))
    val expectedIdentity = StructuralSemanticIdentity.derive(p)
    val successor = StructuralReference(BigInt(1), Vector(p.atom))

    expect(result.acceptedIds == Vector(expectedIdentity))
      .and(expect(result.rejected.isEmpty))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 60))
      .and(expect(result.finalState.lastTxRefOf(ReferenceChainAccount(NativeGl1, alice)) == successor))
      .and(expect(result.conservedTotals == SortedMap[ReferenceBalanceScope, BigInt](Dag -> BigInt(100))))
      .and(expect(result.decisions.head.asInstanceOf[Accepted].conservedTotals == result.conservedTotals))
  }

  test("currency transfer changes only its exact metagraph namespace") {
    val laneA = CurrencyCl1(mgA)
    val laneB = CurrencyCl1(mgB)
    val scopeA = Metagraph(mgA)
    val scopeB = Metagraph(mgB)
    val base = state(
      account(Dag, alice) -> BigInt(100),
      account(scopeA, alice) -> BigInt(100),
      account(scopeB, alice) -> BigInt(100)
    )
    val result = execute(context(laneA), base, transfer(preimage(laneA, amount = 25)))

    expect(result.finalState.balanceOf(account(scopeA, alice)) == 75)
      .and(expect(result.finalState.balanceOf(account(scopeA, bob)) == 25))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 100))
      .and(expect(result.finalState.balanceOf(account(scopeB, alice)) == 100))
      .and(expect(result.conservedTotals(scopeA) == 100))
      .and(expect(result.conservedTotals(scopeB) == 100))
      .and(expect(result.conservedTotals(Dag) == 100))
      .and(expect(result.finalState.lastTxRefOf(ReferenceChainAccount(laneB, alice)) == StructuralReference.genesis))
  }

  test("proof must bind the complete structural preimage and the source") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val expected = preimage(NativeGl1)
    val altered = expected.copy(atom = expected.atom.copy(destination = carol))
    val wrongBinding = Transfer(expected, StructurallyBoundSourceProof(alice, altered))
    val wrongSigner = Transfer(expected, StructurallyBoundSourceProof(carol, expected))
    val bindingResult = execute(context(NativeGl1), base, wrongBinding)
    val signerResult = execute(context(NativeGl1), base, wrongSigner)

    expect(bindingResult.rejected.head.reason == ProofPreimageMismatch(expected, altered))
      .and(expect(bindingResult.finalState == base))
      .and(expect(signerResult.rejected.head.reason == ProofSignerMismatch(alice, carol)))
      .and(expect(signerResult.finalState == base))
  }

  test("structural identity changes with every authority-bearing transfer field") {
    val original = preimage(NativeGl1)
    val otherParent = StructuralReference(BigInt(1), Vector(original.atom))
    val variants = Vector(
      original.copy(parent = otherParent),
      original.copy(atom = original.atom.copy(domain = otherDomain)),
      original.copy(atom = original.atom.copy(lane = CurrencyCl1(mgA))),
      original.copy(atom = original.atom.copy(source = carol)),
      original.copy(atom = original.atom.copy(destination = carol)),
      original.copy(atom = original.atom.copy(amount = original.atom.amount + 1)),
      original.copy(atom = original.atom.copy(fee = original.atom.fee + 1)),
      original.copy(atom = original.atom.copy(salt = original.atom.salt + 1))
    )
    val identity = StructuralSemanticIdentity.derive(original)

    expect(variants.forall(variant => StructuralSemanticIdentity.derive(variant) != identity))
  }

  test("domain and execution lane are checked before reference or balance lookup") {
    val base = state(account(Dag, alice) -> BigInt(100), account(Metagraph(mgA), alice) -> BigInt(100))
    val wrongDomainInput = transfer(preimage(NativeGl1, executionDomain = otherDomain))
    val currencyInput = preimage(CurrencyCl1(mgA))
    val wrongMgInput = preimage(CurrencyCl1(mgB))
    val domainResult = execute(context(NativeGl1), base, wrongDomainInput)
    val nativeVsCurrency = execute(context(NativeGl1), base, transfer(currencyInput))
    val crossMg = execute(context(CurrencyCl1(mgA)), base, transfer(wrongMgInput))

    expect(domainResult.rejected.head.reason == UnexpectedDomain(defaultDomain, otherDomain))
      .and(expect(nativeVsCurrency.rejected.head.reason == UnexpectedLane(NativeGl1, CurrencyCl1(mgA))))
      .and(expect(crossMg.rejected.head.reason == UnexpectedLane(CurrencyCl1(mgA), CurrencyCl1(mgB))))
      .and(expect(domainResult.finalState == base))
      .and(expect(nativeVsCurrency.finalState == base))
      .and(expect(crossMg.finalState == base))
  }

  test("exact replay is distinct from a newly signed same-parent sibling") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val first = preimage(NativeGl1, amount = 20, salt = 1L)
    val firstResult = execute(context(NativeGl1), base, transfer(first))
    val acceptedState = firstResult.finalState
    val replayResult = execute(context(NativeGl1), acceptedState, transfer(first))
    val sibling = preimage(NativeGl1, amount = 20, salt = 2L)
    val siblingResult = execute(context(NativeGl1), acceptedState, transfer(sibling))

    expect(replayResult.rejected.head.reason == DuplicateSemanticIdentity(StructuralSemanticIdentity.derive(first)))
      .and(expect(replayResult.finalState == acceptedState))
      .and(
        expect(
          siblingResult.rejected.head.reason == ParentReferenceMismatch(
            StructuralReference(BigInt(1), Vector(first.atom)),
            StructuralReference.genesis
          )
        )
      )
      .and(expect(siblingResult.finalState == acceptedState))
  }

  test("a phantom parent prefix cannot be installed or continued") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val phantom = preimage(NativeGl1, destination = bob, amount = 99, salt = 20L)
    val phantomParent = StructuralReference(BigInt(1), Vector(phantom.atom))
    val child = preimage(NativeGl1, destination = carol, amount = 10, parent = phantomParent, salt = 21L)
    val result = execute(context(NativeGl1), base, transfer(child))

    expect(result.rejected.head.reason == ParentReferenceMismatch(StructuralReference.genesis, phantomParent))
      .and(expect(result.finalState == base))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 100))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 0))
      .and(expect(result.finalState.balanceOf(account(Dag, carol)) == 0))
      .and(expect(result.finalState.lastTxRefOf(ReferenceChainAccount(NativeGl1, alice)) == StructuralReference.genesis))
      .and(expect(result.finalState.acceptedHistory.isEmpty))
  }

  test("scalar, fee-policy, malformed-parent, and self-transfer failures are atomic") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val malformed = StructuralReference(BigInt(2), Vector(preimage(NativeGl1).atom))
    val cases = Vector[(TransferPreimage, ReferenceRejection)](
      preimage(NativeGl1, amount = 0) -> AmountNotPositive(0),
      preimage(NativeGl1, amount = -1) -> AmountNotPositive(-1),
      preimage(NativeGl1, amount = BigInt(Long.MaxValue) + 1) -> ScalarOutOfRange("amount", BigInt(Long.MaxValue) + 1),
      preimage(NativeGl1, fee = -1) -> ScalarOutOfRange("fee", -1),
      preimage(NativeGl1, fee = 1) -> FeeDispositionUnfrozen(1),
      preimage(NativeGl1, destination = alice) -> SelfTransfer(alice),
      preimage(NativeGl1, parent = malformed) -> MalformedParentReference(2, 1)
    )

    val assertions = cases.map {
      case (p, expected) =>
        val result = execute(context(NativeGl1), base, transfer(p))
        expect(result.rejected.head.reason == expected).and(expect(result.finalState == base))
    }

    assertions.reduce(_ and _)
  }

  test("insufficient source, destination overflow, and locked source reject without partial writes") {
    val ordinaryBase = state(account(Dag, alice) -> BigInt(10))
    val insufficient = execute(context(NativeGl1), ordinaryBase, transfer(preimage(NativeGl1, amount = 11)))
    val overflowBase = state(account(Dag, alice) -> BigInt(1), account(Dag, bob) -> BigInt(Long.MaxValue))
    val overflow = execute(context(NativeGl1), overflowBase, transfer(preimage(NativeGl1, amount = 1)))
    val locked = execute(
      context(NativeGl1, locked = SortedSet(alice)),
      ordinaryBase,
      transfer(preimage(NativeGl1, amount = 1))
    )

    expect(
      insufficient.rejected.head.reason == InsufficientBalance(account(Dag, alice), required = 11, available = 10)
    ).and(expect(insufficient.finalState == ordinaryBase))
      .and(
        expect(
          overflow.rejected.head.reason == ProjectedBalanceOutOfRange(account(Dag, bob), BigInt(Long.MaxValue) + 1)
        )
      )
      .and(expect(overflow.finalState == overflowBase))
      .and(expect(locked.rejected.head.reason == LockedSource(alice)))
      .and(expect(locked.finalState == ordinaryBase))
  }

  test("ordered prefixes continue from accepted state after an atomic rejection") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val first = preimage(NativeGl1, amount = 60, salt = 10L)
    val firstSuccessor = StructuralReference(BigInt(1), Vector(first.atom))
    val second = preimage(NativeGl1, destination = carol, amount = 50, parent = firstSuccessor, salt = 11L)
    val third = preimage(NativeGl1, destination = carol, amount = 30, parent = firstSuccessor, salt = 12L)
    val thirdSuccessor = StructuralReference(BigInt(2), Vector(first.atom, third.atom))
    val result = execute(context(NativeGl1), base, transfer(first), transfer(second), transfer(third))

    expect(result.decisions.head.isInstanceOf[Accepted])
      .and(expect(result.decisions(1).isInstanceOf[Rejected]))
      .and(expect(result.decisions(2).isInstanceOf[Accepted]))
      .and(expect(result.decisions.map(_.inputIndex) == Vector(0, 1, 2)))
      .and(
        expect(
          result.rejected.head.reason == InsufficientBalance(account(Dag, alice), required = 50, available = 40)
        )
      )
      .and(expect(result.acceptedIds == Vector(StructuralSemanticIdentity.derive(first), StructuralSemanticIdentity.derive(third))))
      .and(expect(!result.finalState.acceptedHistory.contains(StructuralSemanticIdentity.derive(second))))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 10))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 60))
      .and(expect(result.finalState.balanceOf(account(Dag, carol)) == 30))
      .and(expect(result.finalState.lastTxRefOf(ReferenceChainAccount(NativeGl1, alice)) == thirdSuccessor))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("initial state rejects negative and greater-than-Long-MaxValue balances") {
    val negative = ReferenceState.initial(Map(account(Dag, alice) -> BigInt(-1)))
    val oversized = ReferenceState.initial(Map(account(Dag, alice) -> (BigInt(Long.MaxValue) + 1)))

    expect(negative == Left(ReferenceStateError.InitialBalanceOutOfRange(account(Dag, alice), -1)))
      .and(
        expect(
          oversized == Left(
            ReferenceStateError.InitialBalanceOutOfRange(account(Dag, alice), BigInt(Long.MaxValue) + 1)
          )
        )
      )
  }

  test("an accepted state rejects execution under a different structural domain") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val accepted = execute(context(NativeGl1), base, transfer(preimage(NativeGl1, amount = 10))).finalState
    val result = V4EconomicReferenceInterpreter.execute(context(NativeGl1, otherDomain), accepted, Vector.empty)

    expect(result == Left(ReferenceStateError.StoredStateDomainMismatch(otherDomain, defaultDomain)))
  }

  test("custom-origin framework claim has no executable constructor and fails closed") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val input = UnsupportedManifestOperation.fromValue("ECO-TOKEN-LOCK-MANUAL", CustomData).toOption.get
    val result = execute(context(NativeGl1), base, input)

    expect(result.rejected.head.reason == UnsupportedOperation("ECO-TOKEN-LOCK-MANUAL", CustomData))
      .and(expect(result.rejected.head.identity.isEmpty))
      .and(expect(result.finalState == base))
      .and(expect(result.acceptedIds.isEmpty))
  }
}
