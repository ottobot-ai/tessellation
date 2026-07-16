package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

sealed trait ReferenceBalanceScope extends Product with Serializable

object ReferenceBalanceScope {
  case object Dag extends ReferenceBalanceScope
  final case class Metagraph(metagraphId: Address) extends ReferenceBalanceScope

  implicit val ordering: Ordering[ReferenceBalanceScope] = new Ordering[ReferenceBalanceScope] {
    def compare(left: ReferenceBalanceScope, right: ReferenceBalanceScope): Int =
      (left, right) match {
        case (Dag, Dag)                              => 0
        case (Dag, _: Metagraph)                     => -1
        case (_: Metagraph, Dag)                     => 1
        case (Metagraph(leftId), Metagraph(rightId)) => Ordering[Address].compare(leftId, rightId)
      }
  }
}

final case class ReferenceBalanceAccount(scope: ReferenceBalanceScope, address: Address)

object ReferenceBalanceAccount {
  implicit val ordering: Ordering[ReferenceBalanceAccount] = new Ordering[ReferenceBalanceAccount] {
    def compare(left: ReferenceBalanceAccount, right: ReferenceBalanceAccount): Int = {
      val scopeComparison = Ordering[ReferenceBalanceScope].compare(left.scope, right.scope)
      if (scopeComparison != 0) scopeComparison else Ordering[Address].compare(left.address, right.address)
    }
  }
}

final case class ReferenceDomain(networkId: Hash, genesisHash: Hash, protocolEra: Hash)

sealed trait TransferLane extends Product with Serializable {
  def scope: ReferenceBalanceScope
}

object TransferLane {
  case object NativeGl1 extends TransferLane {
    val scope: ReferenceBalanceScope = ReferenceBalanceScope.Dag
  }

  final case class CurrencyCl1(metagraphId: Address) extends TransferLane {
    val scope: ReferenceBalanceScope = ReferenceBalanceScope.Metagraph(metagraphId)
  }

  implicit val ordering: Ordering[TransferLane] = new Ordering[TransferLane] {
    def compare(left: TransferLane, right: TransferLane): Int =
      (left, right) match {
        case (NativeGl1, NativeGl1)                      => 0
        case (NativeGl1, _: CurrencyCl1)                 => -1
        case (_: CurrencyCl1, NativeGl1)                 => 1
        case (CurrencyCl1(leftId), CurrencyCl1(rightId)) => Ordering[Address].compare(leftId, rightId)
      }
  }
}

final case class ReferenceContext(
  domain: ReferenceDomain,
  lane: TransferLane,
  lockedAddresses: SortedSet[Address]
)

final case class TransferAtom(
  domain: ReferenceDomain,
  lane: TransferLane,
  source: Address,
  destination: Address,
  amount: BigInt,
  fee: BigInt,
  salt: Long
)

final case class StructuralReference(ordinal: BigInt, lineage: Vector[TransferAtom])

object StructuralReference {
  val genesis: StructuralReference = StructuralReference(BigInt(0), Vector.empty)
}

final case class TransferPreimage(parent: StructuralReference, atom: TransferAtom)

/** Reference-model evidence that binds one source identity to one complete structural preimage.
  *
  * This is deliberately not cryptographic evidence. A production differential adapter may construct the corresponding verified binding only
  * after checking the real signature against the exact active domain and bytes.
  */
final case class StructurallyBoundSourceProof(signer: Address, signedPreimage: TransferPreimage)

final case class StructuralSemanticIdentity private (parent: StructuralReference, atom: TransferAtom)

object StructuralSemanticIdentity {
  def derive(preimage: TransferPreimage): StructuralSemanticIdentity =
    StructuralSemanticIdentity(preimage.parent, preimage.atom)
}

sealed trait InputProvenance extends Product with Serializable

object InputProvenance {
  case object FrameworkLane extends InputProvenance
  case object CustomData extends InputProvenance
  case object OuterStateChannelBinary extends InputProvenance
  case object MetagraphClaim extends InputProvenance
}

sealed trait ReferenceInput extends Product with Serializable

object ReferenceInput {
  final case class Transfer(preimage: TransferPreimage, proof: StructurallyBoundSourceProof) extends ReferenceInput
  final case class UnsupportedManifestOperation(operationId: String, provenance: InputProvenance) extends ReferenceInput
}

final case class ReferenceChainAccount(lane: TransferLane, source: Address)

object ReferenceChainAccount {
  implicit val ordering: Ordering[ReferenceChainAccount] = new Ordering[ReferenceChainAccount] {
    def compare(left: ReferenceChainAccount, right: ReferenceChainAccount): Int = {
      val laneComparison = Ordering[TransferLane].compare(left.lane, right.lane)
      if (laneComparison != 0) laneComparison else Ordering[Address].compare(left.source, right.source)
    }
  }
}

sealed trait ReferenceStateError extends Product with Serializable

object ReferenceStateError {
  final case class InitialBalanceOutOfRange(account: ReferenceBalanceAccount, balance: BigInt) extends ReferenceStateError
  final case class StoredStateDomainMismatch(expected: ReferenceDomain, actual: ReferenceDomain) extends ReferenceStateError
}

final class ReferenceState private (
  private val acceptedDomain: Option[ReferenceDomain],
  val balances: SortedMap[ReferenceBalanceAccount, BigInt],
  val lastTxRefs: SortedMap[ReferenceChainAccount, StructuralReference],
  val acceptedHistory: Vector[StructuralSemanticIdentity]
) extends Serializable {
  def balanceOf(account: ReferenceBalanceAccount): BigInt = balances.getOrElse(account, BigInt(0))

  def lastTxRefOf(account: ReferenceChainAccount): StructuralReference =
    lastTxRefs.getOrElse(account, StructuralReference.genesis)

  override def equals(other: Any): Boolean =
    other match {
      case that: ReferenceState =>
        acceptedDomain == that.acceptedDomain && balances == that.balances && lastTxRefs == that.lastTxRefs &&
        acceptedHistory == that.acceptedHistory
      case _ => false
    }

  override def hashCode(): Int = (acceptedDomain, balances, lastTxRefs, acceptedHistory).##

  override def toString: String =
    s"ReferenceState($acceptedDomain,$balances,$lastTxRefs,$acceptedHistory)"
}

object ReferenceState {
  import ReferenceDecision._
  import ReferenceInput._
  import ReferenceRejection._
  import ReferenceWrite._

  private val MaxBalance = BigInt(Long.MaxValue)

  def initial(
    balances: Map[ReferenceBalanceAccount, BigInt]
  ): Either[ReferenceStateError, ReferenceState] = {
    val canonical = SortedMap.from(balances)(ReferenceBalanceAccount.ordering)
    canonical.collectFirst {
      case (account, balance) if balance < 0 || balance > MaxBalance =>
        ReferenceStateError.InitialBalanceOutOfRange(account, balance)
    } match {
      case Some(error) => Left(error)
      case None =>
        Right(
          new ReferenceState(
            None,
            canonical.filter { case (_, balance) => balance != 0 },
            SortedMap.empty(ReferenceChainAccount.ordering),
            Vector.empty
          )
        )
    }
  }

  private[economics] def execute(
    context: ReferenceContext,
    base: ReferenceState,
    inputs: Vector[ReferenceInput]
  ): Either[ReferenceStateError, ReferenceExecution] =
    validate(context, base).map { _ =>
      val conservedTotals = totals(base.balances)
      val (finalState, decisions) = inputs.zipWithIndex.foldLeft((base, Vector.empty[ReferenceDecision])) {
        case ((state, accumulated), (input, inputIndex)) =>
          val (nextState, decision) = attempt(context, state, conservedTotals, input, inputIndex)
          (nextState, accumulated :+ decision)
      }

      ReferenceExecution(decisions, finalState, conservedTotals)
    }

  private def validate(
    context: ReferenceContext,
    state: ReferenceState
  ): Either[ReferenceStateError, Unit] =
    state.acceptedDomain match {
      case Some(actual) if actual != context.domain =>
        Left(ReferenceStateError.StoredStateDomainMismatch(context.domain, actual))
      case _ => Right(())
    }

  private def attempt(
    context: ReferenceContext,
    state: ReferenceState,
    conservedTotals: SortedMap[ReferenceBalanceScope, BigInt],
    input: ReferenceInput,
    inputIndex: Int
  ): (ReferenceState, ReferenceDecision) =
    input match {
      case UnsupportedManifestOperation(operationId, provenance) =>
        state -> Rejected(inputIndex, None, UnsupportedOperation(operationId, provenance))

      case Transfer(preimage, proof) =>
        val identity = StructuralSemanticIdentity.derive(preimage)
        validateTransfer(context, state, conservedTotals, preimage, proof, identity) match {
          case Left(reason) => state -> Rejected(inputIndex, Some(identity), reason)
          case Right((nextState, writes)) =>
            nextState -> Accepted(inputIndex, identity, writes, conservedTotals)
        }
    }

  private def validateTransfer(
    context: ReferenceContext,
    state: ReferenceState,
    conservedTotals: SortedMap[ReferenceBalanceScope, BigInt],
    preimage: TransferPreimage,
    proof: StructurallyBoundSourceProof,
    identity: StructuralSemanticIdentity
  ): Either[ReferenceRejection, (ReferenceState, Vector[ReferenceWrite])] = {
    val atom = preimage.atom
    val chainAccount = ReferenceChainAccount(atom.lane, atom.source)
    val sourceAccount = ReferenceBalanceAccount(atom.lane.scope, atom.source)
    val destinationAccount = ReferenceBalanceAccount(atom.lane.scope, atom.destination)

    for {
      _ <- Either.cond(!state.acceptedHistory.contains(identity), (), DuplicateSemanticIdentity(identity))
      _ <- Either.cond(atom.domain == context.domain, (), UnexpectedDomain(context.domain, atom.domain))
      _ <- Either.cond(atom.lane == context.lane, (), UnexpectedLane(context.lane, atom.lane))
      _ <- Either.cond(proof.signedPreimage == preimage, (), ProofPreimageMismatch(preimage, proof.signedPreimage))
      _ <- Either.cond(proof.signer == atom.source, (), ProofSignerMismatch(atom.source, proof.signer))
      _ <- Either.cond(atom.amount > 0, (), AmountNotPositive(atom.amount))
      _ <- validateScalar("amount", atom.amount)
      _ <- validateScalar("fee", atom.fee)
      _ <- Either.cond(atom.fee == 0, (), FeeDispositionUnfrozen(atom.fee))
      _ <- Either.cond(!context.lockedAddresses.contains(atom.source), (), LockedSource(atom.source))
      _ <- Either.cond(atom.source != atom.destination, (), SelfTransfer(atom.source))
      _ <- Either.cond(
        preimage.parent.ordinal == BigInt(preimage.parent.lineage.size),
        (),
        MalformedParentReference(preimage.parent.ordinal, preimage.parent.lineage.size)
      )
      expectedParent = state.lastTxRefOf(chainAccount)
      _ <- Either.cond(preimage.parent == expectedParent, (), ParentReferenceMismatch(expectedParent, preimage.parent))
      sourceBefore = state.balanceOf(sourceAccount)
      destinationBefore = state.balanceOf(destinationAccount)
      gross = atom.amount + atom.fee
      _ <- Either.cond(
        sourceBefore >= gross,
        (),
        InsufficientBalance(sourceAccount, gross, sourceBefore)
      )
      sourceAfter = sourceBefore - gross
      destinationAfter = destinationBefore + atom.amount
      _ <- validateProjected(sourceAccount, sourceAfter)
      _ <- validateProjected(destinationAccount, destinationAfter)
      projectedBalances = state.balances
        .updated(sourceAccount, sourceAfter)
        .updated(destinationAccount, destinationAfter)
      projectedTotals = totals(projectedBalances)
      _ <- Either.cond(projectedTotals == conservedTotals, (), ConservationViolation(conservedTotals, projectedTotals))
      successor = StructuralReference(preimage.parent.ordinal + 1, preimage.parent.lineage :+ atom)
      nextState = new ReferenceState(
        Some(atom.domain),
        projectedBalances.filter { case (_, balance) => balance != 0 },
        state.lastTxRefs.updated(chainAccount, successor),
        state.acceptedHistory :+ identity
      )
      balanceWrites = Vector(
        Balance(sourceAccount, sourceBefore, sourceAfter),
        Balance(destinationAccount, destinationBefore, destinationAfter)
      ).sortBy {
        case Balance(account, _, _) => account
      }(ReferenceBalanceAccount.ordering)
      writes = balanceWrites ++ Vector(
        TransactionReference(chainAccount, expectedParent, successor),
        ReplayIdentity(identity)
      )
    } yield (nextState, writes)
  }

  private def validateScalar(field: String, value: BigInt): Either[ReferenceRejection, Unit] =
    Either.cond(value >= 0 && value <= MaxBalance, (), ScalarOutOfRange(field, value))

  private def validateProjected(
    account: ReferenceBalanceAccount,
    value: BigInt
  ): Either[ReferenceRejection, Unit] =
    Either.cond(value >= 0 && value <= MaxBalance, (), ProjectedBalanceOutOfRange(account, value))

  private def totals(
    balances: SortedMap[ReferenceBalanceAccount, BigInt]
  ): SortedMap[ReferenceBalanceScope, BigInt] =
    balances
      .foldLeft(SortedMap.empty[ReferenceBalanceScope, BigInt](ReferenceBalanceScope.ordering)) {
        case (acc, (account, balance)) =>
          acc.updated(account.scope, acc.getOrElse(account.scope, BigInt(0)) + balance)
      }
      .filter { case (_, total) => total != 0 }
}

sealed trait ReferenceRejection extends Product with Serializable

object ReferenceRejection {
  final case class DuplicateSemanticIdentity(identity: StructuralSemanticIdentity) extends ReferenceRejection
  final case class UnexpectedDomain(expected: ReferenceDomain, actual: ReferenceDomain) extends ReferenceRejection
  final case class UnexpectedLane(expected: TransferLane, actual: TransferLane) extends ReferenceRejection
  final case class ProofPreimageMismatch(expected: TransferPreimage, actual: TransferPreimage) extends ReferenceRejection
  final case class ProofSignerMismatch(expected: Address, actual: Address) extends ReferenceRejection
  final case class AmountNotPositive(amount: BigInt) extends ReferenceRejection
  final case class ScalarOutOfRange(field: String, value: BigInt) extends ReferenceRejection
  final case class FeeDispositionUnfrozen(fee: BigInt) extends ReferenceRejection
  final case class LockedSource(source: Address) extends ReferenceRejection
  final case class SelfTransfer(source: Address) extends ReferenceRejection
  final case class MalformedParentReference(ordinal: BigInt, lineageSize: Int) extends ReferenceRejection
  final case class ParentReferenceMismatch(expected: StructuralReference, actual: StructuralReference) extends ReferenceRejection
  final case class InsufficientBalance(account: ReferenceBalanceAccount, required: BigInt, available: BigInt) extends ReferenceRejection
  final case class ProjectedBalanceOutOfRange(account: ReferenceBalanceAccount, projected: BigInt) extends ReferenceRejection
  final case class ConservationViolation(
    before: SortedMap[ReferenceBalanceScope, BigInt],
    after: SortedMap[ReferenceBalanceScope, BigInt]
  ) extends ReferenceRejection
  final case class UnsupportedOperation(operationId: String, provenance: InputProvenance) extends ReferenceRejection
}

sealed trait ReferenceWrite extends Product with Serializable

object ReferenceWrite {
  final case class Balance(
    account: ReferenceBalanceAccount,
    before: BigInt,
    after: BigInt
  ) extends ReferenceWrite
  final case class TransactionReference(
    account: ReferenceChainAccount,
    before: StructuralReference,
    after: StructuralReference
  ) extends ReferenceWrite
  final case class ReplayIdentity(identity: StructuralSemanticIdentity) extends ReferenceWrite
}

sealed trait ReferenceDecision extends Product with Serializable {
  def inputIndex: Int
}

object ReferenceDecision {
  final case class Accepted(
    inputIndex: Int,
    identity: StructuralSemanticIdentity,
    writes: Vector[ReferenceWrite],
    conservedTotals: SortedMap[ReferenceBalanceScope, BigInt]
  ) extends ReferenceDecision

  final case class Rejected(
    inputIndex: Int,
    identity: Option[StructuralSemanticIdentity],
    reason: ReferenceRejection
  ) extends ReferenceDecision
}

final case class ReferenceExecution(
  decisions: Vector[ReferenceDecision],
  finalState: ReferenceState,
  conservedTotals: SortedMap[ReferenceBalanceScope, BigInt]
) {
  def acceptedIds: Vector[StructuralSemanticIdentity] = decisions.collect {
    case accepted: ReferenceDecision.Accepted => accepted.identity
  }

  def rejected: Vector[ReferenceDecision.Rejected] = decisions.collect {
    case rejection: ReferenceDecision.Rejected => rejection
  }
}

/** Independent, test-only transition oracle for the first bounded E2.1 tranche.
  *
  * It intentionally supports only zero-fee native and currency transfers. It performs no production hashing, signature verification,
  * balance arithmetic, transition-manager calls, serialization, MPT writes, or root computation.
  */
object V4EconomicReferenceInterpreter {
  def execute(
    context: ReferenceContext,
    base: ReferenceState,
    inputs: Vector[ReferenceInput]
  ): Either[ReferenceStateError, ReferenceExecution] =
    ReferenceState.execute(context, base, inputs)
}
