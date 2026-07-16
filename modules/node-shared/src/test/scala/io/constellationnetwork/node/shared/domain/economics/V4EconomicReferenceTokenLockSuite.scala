package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.{Dag, Metagraph}
import io.constellationnetwork.node.shared.domain.economics.ReferenceDecision.Accepted
import io.constellationnetwork.node.shared.domain.economics.ReferenceInput.{AllowSpendCreate, TokenLockCreate, Transfer}
import io.constellationnetwork.node.shared.domain.economics.ReferenceRejection._
import io.constellationnetwork.node.shared.domain.economics.ReferenceWrite.{
  Balance => BalanceWrite,
  TokenLockReference => TokenLockReferenceWrite,
  _
}
import io.constellationnetwork.node.shared.domain.economics.TransferLane.{CurrencyCl1, NativeGl1}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import weaver.FunSuite

object V4EconomicReferenceTokenLockSuite extends FunSuite {
  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val alice = address("token-lock-alice")
  private val bob = address("token-lock-bob")
  private val carol = address("token-lock-carol")
  private val mgA = address("token-lock-metagraph-a")
  private val mgB = address("token-lock-metagraph-b")
  private val defaultDomain = ReferenceDomain(hash(201), hash(202), hash(203))
  private val otherDomain = ReferenceDomain(hash(201), hash(202), hash(204))
  private val defaultAllowSpendWindow = ReferenceAllowSpendEpochWindow(100, 5, 20)
  private val defaultTokenLockRule = ReferenceTokenLockEpochRule(100, 5)

  private def context(
    lane: TransferLane,
    executionDomain: ReferenceDomain = defaultDomain,
    locked: SortedSet[Address] = SortedSet.empty[Address],
    tokenLockRule: Option[ReferenceTokenLockEpochRule] = Some(defaultTokenLockRule),
    allowSpendWindow: Option[ReferenceAllowSpendEpochWindow] = Some(defaultAllowSpendWindow)
  ): ReferenceContext =
    ReferenceContext(executionDomain, lane, locked, allowSpendWindow, tokenLockRule)

  private def account(scope: ReferenceBalanceScope, address: Address): ReferenceBalanceAccount =
    ReferenceBalanceAccount(scope, address)

  private def state(entries: (ReferenceBalanceAccount, BigInt)*): ReferenceState =
    ReferenceState.initial(Map(entries: _*)).toOption.get

  private def preimage(
    lane: TransferLane,
    source: Address = alice,
    amount: BigInt = BigInt(60),
    fee: BigInt = BigInt(0),
    parent: StructuralTokenLockReference = StructuralTokenLockReference.genesis,
    unlockEpoch: Option[BigInt] = Some(BigInt(110)),
    replaceTokenLockRef: Option[Hash] = None,
    executionDomain: ReferenceDomain = defaultDomain
  ): TokenLockPreimage =
    TokenLockPreimage(
      parent,
      TokenLockAtom(executionDomain, lane, source, amount, fee, unlockEpoch, replaceTokenLockRef)
    )

  private def create(preimage: TokenLockPreimage, signer: Address = alice): TokenLockCreate =
    TokenLockCreate(preimage, StructurallyBoundTokenLockSourceProof(signer, preimage))

  private def transfer(
    lane: TransferLane,
    source: Address,
    destination: Address,
    amount: BigInt,
    salt: Long
  ): Transfer = {
    val preimage = TransferPreimage(
      StructuralReference.genesis,
      TransferAtom(defaultDomain, lane, source, destination, amount, fee = 0, salt = salt)
    )
    Transfer(preimage, StructurallyBoundSourceProof(source, preimage))
  }

  private def allowSpend(lane: TransferLane, amount: BigInt): AllowSpendCreate = {
    val preimage = AllowSpendPreimage(
      StructuralAllowSpendReference.genesis,
      AllowSpendAtom(
        defaultDomain,
        lane,
        alice,
        carol,
        amount,
        fee = 0,
        lastValidEpochProgress = 110,
        approvers = Vector(carol)
      )
    )
    AllowSpendCreate(preimage, StructurallyBoundAllowSpendSourceProof(alice, preimage))
  }

  private def execute(
    context: ReferenceContext,
    base: ReferenceState,
    inputs: ReferenceInput*
  ): ReferenceExecution =
    V4EconomicReferenceInterpreter.execute(context, base, inputs.toVector).toOption.get

  test("zero-fee native token-lock creation reserves principal and advances only the token-lock reference") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val p = preimage(NativeGl1)
    val identity = TokenLockSemanticIdentity.derive(p)
    val successor = StructuralTokenLockReference(BigInt(1), Vector(p.atom))
    val chainAccount = ReferenceTokenLockChainAccount(NativeGl1, alice)
    val activeTokenLock = ReferenceActiveTokenLock(identity, Dag, alice, BigInt(60), Some(BigInt(110)))
    val result = execute(context(NativeGl1), base, create(p))
    val accepted = result.decisions.head.asInstanceOf[Accepted]

    expect(result.acceptedIds == Vector(identity))
      .and(expect(result.rejected.isEmpty))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(result.finalState.activeTokenLockOf(identity).exists(_.amount == 60)))
      .and(expect(result.finalState.activeTokenLockOf(identity).exists(_.unlockEpoch.contains(BigInt(110)))))
      .and(expect(result.finalState.lastTokenLockRefOf(chainAccount) == successor))
      .and(expect(result.finalState.lastTxRefOf(ReferenceChainAccount(NativeGl1, alice)) == StructuralReference.genesis))
      .and(
        expect(
          result.finalState.lastAllowSpendRefOf(ReferenceAllowSpendChainAccount(NativeGl1, alice)) ==
            StructuralAllowSpendReference.genesis
        )
      )
      .and(expect(result.conservedTotals == SortedMap[ReferenceBalanceScope, BigInt](Dag -> BigInt(100))))
      .and(
        expect(
          accepted.writes == Vector(
            BalanceWrite(account(Dag, alice), before = BigInt(100), after = BigInt(40)),
            ActiveTokenLockCreated(activeTokenLock),
            TokenLockReferenceWrite(chainAccount, StructuralTokenLockReference.genesis, successor),
            ReplayIdentity(identity)
          )
        )
      )
  }

  test("currency token-lock creation is confined to its exact metagraph lane") {
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
    val identity = TokenLockSemanticIdentity.derive(p)
    val result = execute(context(laneA), base, create(p))

    expect(result.finalState.balanceOf(account(scopeA, alice)) == 75)
      .and(expect(result.finalState.balanceOf(account(scopeB, alice)) == 100))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 100))
      .and(expect(result.finalState.activeTokenLockOf(identity).exists(_.scope == scopeA)))
      .and(
        expect(
          result.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(laneB, alice)) ==
            StructuralTokenLockReference.genesis
        )
      )
      .and(expect(result.conservedTotals(scopeA) == 100))
      .and(expect(result.conservedTotals(scopeB) == 100))
      .and(expect(result.conservedTotals(Dag) == 100))
  }

  test("token-lock proof binds the complete preimage and exclusively binds its source") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val expected = preimage(NativeGl1)
    val altered = expected.copy(atom = expected.atom.copy(unlockEpoch = Some(111)))
    val wrongBinding = TokenLockCreate(expected, StructurallyBoundTokenLockSourceProof(alice, altered))
    val wrongSigner = TokenLockCreate(expected, StructurallyBoundTokenLockSourceProof(carol, expected))
    val bindingResult = execute(context(NativeGl1), base, wrongBinding)
    val signerResult = execute(context(NativeGl1), base, wrongSigner)

    expect(bindingResult.rejected.head.reason == TokenLockProofPreimageMismatch(expected, altered))
      .and(expect(bindingResult.finalState == base))
      .and(expect(signerResult.rejected.head.reason == TokenLockProofSignerMismatch(alice, carol)))
      .and(expect(signerResult.finalState == base))
  }

  test("domain, exact lane, and locked-source checks fail before state mutation") {
    val laneA = CurrencyCl1(mgA)
    val laneB = CurrencyCl1(mgB)
    val base = state(account(Metagraph(mgA), alice) -> BigInt(100), account(Metagraph(mgB), alice) -> BigInt(100))
    val wrongDomain = execute(context(laneA), base, create(preimage(laneA, executionDomain = otherDomain)))
    val wrongLane = execute(context(laneA), base, create(preimage(laneB)))
    val locked = execute(context(laneA, locked = SortedSet(alice)), base, create(preimage(laneA)))

    expect(wrongDomain.rejected.head.reason == UnexpectedDomain(defaultDomain, otherDomain))
      .and(expect(wrongLane.rejected.head.reason == UnexpectedLane(laneA, laneB)))
      .and(expect(locked.rejected.head.reason == LockedSource(alice)))
      .and(expect(Vector(wrongDomain, wrongLane, locked).forall(_.finalState == base)))
  }

  test("replay, same-parent sibling, and phantom parent cannot advance the separate token-lock chain") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val first = preimage(NativeGl1, amount = 20)
    val firstIdentity = TokenLockSemanticIdentity.derive(first)
    val acceptedState = execute(context(NativeGl1), base, create(first)).finalState
    val expectedParent = StructuralTokenLockReference(BigInt(1), Vector(first.atom))
    val replay = execute(context(NativeGl1), acceptedState, create(first))
    val sibling = preimage(NativeGl1, amount = 10, unlockEpoch = Some(115))
    val siblingResult = execute(context(NativeGl1), acceptedState, create(sibling))
    val phantom = preimage(NativeGl1, source = carol, amount = 1)
    val phantomParent = StructuralTokenLockReference(BigInt(1), Vector(phantom.atom))
    val phantomChild = preimage(NativeGl1, amount = 1, parent = phantomParent)
    val phantomResult = execute(context(NativeGl1), base, create(phantomChild))
    val successor = preimage(NativeGl1, amount = 10, parent = expectedParent, unlockEpoch = Some(115))
    val successorResult = execute(context(NativeGl1), acceptedState, create(successor))

    expect(replay.rejected.head.reason == DuplicateSemanticIdentity(firstIdentity))
      .and(expect(replay.finalState == acceptedState))
      .and(
        expect(siblingResult.rejected.head.reason == TokenLockParentReferenceMismatch(expectedParent, StructuralTokenLockReference.genesis))
      )
      .and(expect(siblingResult.finalState == acceptedState))
      .and(
        expect(phantomResult.rejected.head.reason == TokenLockParentReferenceMismatch(StructuralTokenLockReference.genesis, phantomParent))
      )
      .and(expect(phantomResult.finalState == base))
      .and(expect(successorResult.decisions.head.isInstanceOf[Accepted]))
      .and(
        expect(
          successorResult.finalState.lastTokenLockRefOf(ReferenceTokenLockChainAccount(NativeGl1, alice)) ==
            StructuralTokenLockReference(BigInt(2), Vector(first.atom, successor.atom))
        )
      )
  }

  test("unlock eligibility uses an explicit epoch rule with checked addition") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val missing = execute(context(NativeGl1, tokenLockRule = None), base, create(preimage(NativeGl1)))
    val alreadyUnlocked = execute(context(NativeGl1), base, create(preimage(NativeGl1, unlockEpoch = Some(100))))
    // Target semantics combine the contextual minimum with TokenLockValidator's strict `unlockEpoch > current` rule.
    // Snapshot block acceptance does not yet invoke the contextual validator, so production differential coverage remains open.
    val zeroMinimumRule = ReferenceTokenLockEpochRule(100, 0)
    val zeroMinimumEquality = execute(
      context(NativeGl1, tokenLockRule = Some(zeroMinimumRule)),
      base,
      create(preimage(NativeGl1, unlockEpoch = Some(100)))
    )
    val tooShort = execute(context(NativeGl1), base, create(preimage(NativeGl1, unlockEpoch = Some(104))))
    val boundary = execute(context(NativeGl1), base, create(preimage(NativeGl1, unlockEpoch = Some(105))))
    val permanent = execute(context(NativeGl1), base, create(preimage(NativeGl1, unlockEpoch = None)))
    val overflowRule = ReferenceTokenLockEpochRule(BigInt(Long.MaxValue), 1)
    val overflow = execute(context(NativeGl1, tokenLockRule = Some(overflowRule)), base, create(preimage(NativeGl1)))
    val currentOutOfRangeRule = ReferenceTokenLockEpochRule(BigInt(Long.MaxValue) + 1, 0)
    val currentOutOfRange =
      execute(context(NativeGl1, tokenLockRule = Some(currentOutOfRangeRule)), base, create(preimage(NativeGl1)))
    val minimumOutOfRangeRule = ReferenceTokenLockEpochRule(0, BigInt(Long.MaxValue) + 1)
    val minimumOutOfRange =
      execute(context(NativeGl1, tokenLockRule = Some(minimumOutOfRangeRule)), base, create(preimage(NativeGl1)))

    expect(missing.rejected.head.reason == MissingTokenLockEpochRule)
      .and(expect(alreadyUnlocked.rejected.head.reason == TokenLockAlreadyUnlocked(100, 100)))
      .and(expect(zeroMinimumEquality.rejected.head.reason == TokenLockAlreadyUnlocked(100, 100)))
      .and(expect(tooShort.rejected.head.reason == TokenLockUnlockEpochTooShort(104, 105)))
      .and(expect(boundary.decisions.head.isInstanceOf[Accepted]))
      .and(expect(permanent.decisions.head.isInstanceOf[Accepted]))
      .and(expect(overflow.rejected.head.reason == InvalidTokenLockEpochRule(overflowRule)))
      .and(expect(currentOutOfRange.rejected.head.reason == ScalarOutOfRange("currentEpochProgress", BigInt(Long.MaxValue) + 1)))
      .and(expect(minimumOutOfRange.rejected.head.reason == ScalarOutOfRange("minEpochProgressesToLock", BigInt(Long.MaxValue) + 1)))
      .and(
        expect(
          Vector(missing, alreadyUnlocked, zeroMinimumEquality, tooShort, overflow, currentOutOfRange, minimumOutOfRange).forall(
            _.finalState == base
          )
        )
      )
  }

  test("replacement, fee, scalar, parent, and balance violations fail atomically") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val replacementRef = hash(999)
    val malformedParent = StructuralTokenLockReference(BigInt(2), Vector(preimage(NativeGl1).atom))
    val cases = Vector[(TokenLockPreimage, ReferenceRejection)](
      preimage(NativeGl1, replaceTokenLockRef = Some(replacementRef)) -> TokenLockReplacementUnsupported(replacementRef),
      preimage(NativeGl1, fee = 1) -> FeeDispositionUnfrozen(1),
      preimage(NativeGl1, fee = -1) -> ScalarOutOfRange("fee", -1),
      preimage(NativeGl1, amount = 0) -> AmountNotPositive(0),
      preimage(NativeGl1, amount = -1) -> AmountNotPositive(-1),
      preimage(NativeGl1, amount = BigInt(Long.MaxValue) + 1) -> ScalarOutOfRange("amount", BigInt(Long.MaxValue) + 1),
      preimage(NativeGl1, unlockEpoch = Some(BigInt(Long.MaxValue) + 1)) -> ScalarOutOfRange(
        "unlockEpoch",
        BigInt(Long.MaxValue) + 1
      ),
      preimage(NativeGl1, amount = 101) -> InsufficientBalance(account(Dag, alice), required = 101, available = 100),
      preimage(NativeGl1, parent = malformedParent) -> MalformedTokenLockParentReference(2, 1)
    )

    cases.map {
      case (p, expected) =>
        val result = execute(context(NativeGl1), base, create(p))
        expect(result.rejected.head.reason == expected).and(expect(result.finalState == base))
    }
      .reduce(_ and _)
  }

  test("transfers, allow-spends, and token locks share one ordered spendable-balance ledger") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val transferThenLock = execute(
      context(NativeGl1),
      base,
      transfer(NativeGl1, alice, bob, amount = 60, salt = 1L),
      create(preimage(NativeGl1, amount = 50))
    )
    val lockThenTransfer = execute(
      context(NativeGl1),
      base,
      create(preimage(NativeGl1, amount = 60)),
      transfer(NativeGl1, alice, bob, amount = 50, salt = 2L)
    )
    val allowSpendThenLock = execute(
      context(NativeGl1),
      base,
      allowSpend(NativeGl1, amount = 60),
      create(preimage(NativeGl1, amount = 50))
    )

    expect(transferThenLock.decisions.map(_.isInstanceOf[Accepted]) == Vector(true, false))
      .and(expect(transferThenLock.rejected.head.reason == InsufficientBalance(account(Dag, alice), 50, 40)))
      .and(expect(transferThenLock.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(transferThenLock.finalState.balanceOf(account(Dag, bob)) == 60))
      .and(expect(lockThenTransfer.decisions.map(_.isInstanceOf[Accepted]) == Vector(true, false)))
      .and(expect(lockThenTransfer.rejected.head.reason == InsufficientBalance(account(Dag, alice), 50, 40)))
      .and(expect(lockThenTransfer.finalState.activeTokenLocks.map(_.amount) == Vector(BigInt(60))))
      .and(expect(allowSpendThenLock.decisions.map(_.isInstanceOf[Accepted]) == Vector(true, false)))
      .and(expect(allowSpendThenLock.rejected.head.reason == InsufficientBalance(account(Dag, alice), 50, 40)))
      .and(expect(allowSpendThenLock.finalState.activeAllowSpendReservations.map(_.amount) == Vector(BigInt(60))))
      .and(expect(Vector(transferThenLock, lockThenTransfer, allowSpendThenLock).forall(_.conservedTotals(Dag) == 100)))
  }

  test("active token-lock principal participates in conservation with other economic rows") {
    val base = state(account(Dag, alice) -> BigInt(100))
    val tokenLock = preimage(NativeGl1, amount = 30)
    val result = execute(
      context(NativeGl1),
      base,
      allowSpend(NativeGl1, amount = 20),
      create(tokenLock),
      transfer(NativeGl1, alice, bob, amount = 10, salt = 3L)
    )

    expect(result.decisions.forall(_.isInstanceOf[Accepted]))
      .and(expect(result.finalState.balanceOf(account(Dag, alice)) == 40))
      .and(expect(result.finalState.balanceOf(account(Dag, bob)) == 10))
      .and(expect(result.finalState.activeAllowSpendReservations.map(_.amount) == Vector(BigInt(20))))
      .and(expect(result.finalState.activeTokenLocks.map(_.amount) == Vector(BigInt(30))))
      .and(expect(result.conservedTotals(Dag) == 100))
      .and(
        expect(
          result.decisions.collect { case accepted: Accepted => accepted.conservedTotals(Dag) } == Vector(
            BigInt(100),
            BigInt(100),
            BigInt(100)
          )
        )
      )
  }

  test("token-lock semantic identity changes with every authority-bearing field") {
    val original = preimage(NativeGl1)
    val otherParent = StructuralTokenLockReference(BigInt(1), Vector(original.atom))
    val variants = Vector(
      original.copy(parent = otherParent),
      original.copy(atom = original.atom.copy(domain = otherDomain)),
      original.copy(atom = original.atom.copy(lane = CurrencyCl1(mgA))),
      original.copy(atom = original.atom.copy(source = carol)),
      original.copy(atom = original.atom.copy(amount = original.atom.amount + 1)),
      original.copy(atom = original.atom.copy(fee = original.atom.fee + 1)),
      original.copy(atom = original.atom.copy(unlockEpoch = None)),
      original.copy(atom = original.atom.copy(replaceTokenLockRef = Some(hash(997))))
    )
    val identity = TokenLockSemanticIdentity.derive(original)

    expect(variants.forall(variant => TokenLockSemanticIdentity.derive(variant) != identity))
  }
}
