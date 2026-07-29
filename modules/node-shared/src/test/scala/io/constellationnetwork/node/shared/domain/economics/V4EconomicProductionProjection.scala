package io.constellationnetwork.node.shared.domain.economics

import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.schema.Block
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{Transaction, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.{Signed, SignedValidator}
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder

/** Bounded, test-only binding between v4 production payloads and the independent structural reference model.
  *
  * Legacy payload signatures omit network, genesis, protocol era, and an explicit execution lane. Every capability remains
  * `UnboundLegacyPayload`: supplied domain/lane values are conditional test parameters, never runtime authorization evidence.
  *
  * The adapter exposes no raw `ReferenceInput`. It verifies production signatures under the production hasher, resolves proof identities,
  * binds the exact production parent to a lane-derived canonical correspondence, and retains that capability through reference execution.
  * Production acceptance APIs expose aggregate semantic state updates, not execution write order or a distinct replay-ID write. This helper
  * therefore compares only fields actually observed from production. In particular, batch allow-spend acceptance exposes balances and
  * references but no active-record delta, so the batch projection makes no active-record parity claim. Token-lock coverage is restricted to
  * zero-fee, nonreplacement payloads; final active state is observed through the current native/currency state manager after exact block
  * acceptance. The bounded token-lock batch projection checks aggregate state and exact emitted accepted-then-not-accepted decision order,
  * but production still exposes neither an independent write-order trace nor a replay-ID write. It makes no full-GL0-path, E2.8-completion,
  * or cross-platform claim.
  */
object V4EconomicProductionProjection {

  sealed trait PayloadDomainBinding extends Product with Serializable
  case object UnboundLegacyPayload extends PayloadDomainBinding

  sealed trait ProjectionError extends Product with Serializable
  final case class NonZeroFeeOutOfScope(operationId: String, fee: BigInt) extends ProjectionError
  final case class PayloadLaneMismatch(lane: TransferLane, currencyId: Option[CurrencyId]) extends ProjectionError
  final case class ProductionSignatureValidationRejected(operationId: String) extends ProjectionError
  final case class ProductionSourceOwnerValidationRejected(operationId: String) extends ProjectionError
  final case class ResolvedProofOwnerMismatch(operationId: String, expected: Address, actual: Vector[Address]) extends ProjectionError
  final case class TransferParentBindingMismatch(expected: TransactionReference, actual: TransactionReference) extends ProjectionError
  final case class AllowSpendParentBindingMismatch(expected: AllowSpendReference, actual: AllowSpendReference) extends ProjectionError
  final case class TokenLockParentBindingMismatch(expected: TokenLockReference, actual: TokenLockReference) extends ProjectionError
  final case class TokenLockReplacementOutOfScope(replaceTokenLockRef: Hash) extends ProjectionError
  final case class InvalidTransferCanonicalGenesis(
    lane: TransferLane,
    expected: TransactionReference,
    actual: TransactionReference
  ) extends ProjectionError
  final case class InvalidAllowSpendCanonicalGenesis(
    lane: TransferLane,
    expected: AllowSpendReference,
    actual: AllowSpendReference
  ) extends ProjectionError
  final case class InvalidTokenLockCanonicalGenesis(
    lane: TransferLane,
    expected: TokenLockReference,
    actual: TokenLockReference
  ) extends ProjectionError
  final case class TransferCorrespondenceAdvanceMismatch(
    expected: TransactionReference,
    actual: TransactionReference
  ) extends ProjectionError
  final case class AllowSpendCorrespondenceAdvanceMismatch(
    expected: AllowSpendReference,
    actual: AllowSpendReference
  ) extends ProjectionError
  final case class TokenLockCorrespondenceAdvanceMismatch(
    expected: TokenLockReference,
    actual: TokenLockReference
  ) extends ProjectionError
  final case class ProductionTransferNotAccepted(target: TransactionReference, reason: BlockNotAcceptedReason) extends ProjectionError
  final case class ProductionAllowSpendNotAccepted(target: AllowSpendReference, reason: AllowSpendBlockNotAcceptedReason)
      extends ProjectionError
  final case class ProductionTokenLockNotAccepted(target: TokenLockReference, reason: TokenLockBlockNotAcceptedReason)
      extends ProjectionError
  final case class TransferBlockNotAcceptedByBatch(block: Signed[Block]) extends ProjectionError
  final case class AllowSpendBlockNotAcceptedByBatch(block: Signed[AllowSpendBlock]) extends ProjectionError
  final case class TokenLockBlockNotAcceptedByBatch(block: Signed[TokenLockBlock]) extends ProjectionError
  final case class UnexpectedTokenLockBatchDecisions(
    expected: Signed[TokenLockBlock],
    accepted: List[Signed[TokenLockBlock]],
    notAccepted: List[(Signed[TokenLockBlock], TokenLockBlockNotAcceptedReason)]
  ) extends ProjectionError
  final case class UnexpectedTransferBalanceKeys(expected: Set[Address], actual: Set[Address]) extends ProjectionError
  final case class UnexpectedTransferReferenceUpdates(
    expected: Map[Address, TransactionReference],
    actual: Map[Address, TransactionReference]
  ) extends ProjectionError
  final case class UnexpectedAllowSpendBalanceKeys(expected: Set[Address], actual: Set[Address]) extends ProjectionError
  final case class UnexpectedAllowSpendReferenceUpdates(
    expected: Map[Address, AllowSpendReference],
    actual: Map[Address, AllowSpendReference]
  ) extends ProjectionError
  final case class UnexpectedActiveAllowSpendDelta(
    expected: SortedSet[Signed[AllowSpend]],
    actual: SortedSet[Signed[AllowSpend]]
  ) extends ProjectionError
  final case class UnexpectedTokenLockBalanceKeys(expected: Set[Address], actual: Set[Address]) extends ProjectionError
  final case class UnexpectedTokenLockReferenceUpdates(
    expected: Map[Address, TokenLockReference],
    actual: Map[Address, TokenLockReference]
  ) extends ProjectionError
  final case class UnexpectedTokenLockClaimedReplacementRefs(expected: Set[Hash], actual: Set[Hash]) extends ProjectionError
  final case class UnexpectedInRoundTokenLockState(
    expected: Map[Hash, Hashed[TokenLock]],
    actual: Map[Hash, Hashed[TokenLock]]
  ) extends ProjectionError
  final case class UnexpectedActiveTokenLockState(
    expected: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    actual: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  ) extends ProjectionError
  final case class TransferBatchDecisionMissing(block: Signed[Block]) extends ProjectionError
  final case class AllowSpendBatchDecisionMissing(block: Signed[AllowSpendBlock]) extends ProjectionError
  final case class TransferBatchPayloadMismatch(
    expected: Signed[Transaction],
    actual: Vector[Signed[Transaction]]
  ) extends ProjectionError
  final case class AllowSpendBatchPayloadMismatch(
    expected: Signed[AllowSpend],
    actual: Vector[Signed[AllowSpend]]
  ) extends ProjectionError
  final case class TokenLockBatchPayloadMismatch(
    expected: Signed[TokenLock],
    actual: Vector[Signed[TokenLock]]
  ) extends ProjectionError
  final case class UnexpectedTokenLockBatchDecisionSet(
    expected: Vector[Signed[TokenLockBlock]],
    accepted: List[Signed[TokenLockBlock]],
    notAccepted: List[(Signed[TokenLockBlock], TokenLockBlockNotAcceptedReason)]
  ) extends ProjectionError
  final case class TokenLockBatchDecisionMissing(block: Signed[TokenLockBlock]) extends ProjectionError
  final case class TokenLockBatchRequiresSingleSource(actual: Set[Address]) extends ProjectionError
  final case class UnexpectedTokenLockBatchBalance(address: Address, expected: BigInt, actual: BigInt) extends ProjectionError
  final case class UnexpectedTokenLockBatchReference(
    expected: StructuralTokenLockReference,
    actual: StructuralTokenLockReference
  ) extends ProjectionError
  final case class TransferBatchRequiresSingleSource(actual: Set[Address]) extends ProjectionError
  final case class AllowSpendBatchRequiresSingleSource(actual: Set[Address]) extends ProjectionError
  final case class ReferenceExecutionProjectionFailed(error: ReferenceStateError) extends ProjectionError

  final class TransferReferenceCorrespondence private[V4EconomicProductionProjection] (
    val lane: TransferLane,
    val production: TransactionReference,
    val structural: StructuralReference
  ) {
    def advance(observation: AcceptedTransferObservation): Either[ProjectionError, TransferReferenceCorrespondence] = {
      val binding = observation.binding
      if (binding.lane == lane && binding.productionParent == production && binding.structuralParent == structural)
        Right(new TransferReferenceCorrespondence(lane, binding.productionSuccessor, binding.structuralSuccessor))
      else Left(TransferCorrespondenceAdvanceMismatch(production, binding.productionParent))
    }
  }

  object TransferReferenceCorrespondence {
    val nativeGenesis: TransferReferenceCorrespondence =
      new TransferReferenceCorrespondence(TransferLane.NativeGl1, TransactionReference.empty, StructuralReference.genesis)

    def currencyGenesis(metagraphId: Address, currentHasher: Hasher[IO]): IO[TransferReferenceCorrespondence] = {
      implicit val scopedHasher: Hasher[IO] = currentHasher
      TransactionReference
        .emptyCurrency[IO](metagraphId)
        .map(reference =>
          new TransferReferenceCorrespondence(TransferLane.CurrencyCl1(metagraphId), reference, StructuralReference.genesis)
        )
    }

    def fromClaimedGenesis(
      lane: TransferLane,
      claimed: TransactionReference,
      currentHasher: Hasher[IO]
    ): IO[Either[ProjectionError, TransferReferenceCorrespondence]] =
      canonicalTransferGenesis(lane, currentHasher).map { expected =>
        Either.cond(
          claimed == expected,
          new TransferReferenceCorrespondence(lane, expected, StructuralReference.genesis),
          InvalidTransferCanonicalGenesis(lane, expected, claimed)
        )
      }
  }

  final class AllowSpendReferenceCorrespondence private[V4EconomicProductionProjection] (
    val lane: TransferLane,
    val production: AllowSpendReference,
    val structural: StructuralAllowSpendReference
  ) {
    def advance(observation: AcceptedAllowSpendObservation): Either[ProjectionError, AllowSpendReferenceCorrespondence] = {
      val binding = observation.binding
      if (binding.lane == lane && binding.productionParent == production && binding.structuralParent == structural)
        Right(new AllowSpendReferenceCorrespondence(lane, binding.productionSuccessor, binding.structuralSuccessor))
      else Left(AllowSpendCorrespondenceAdvanceMismatch(production, binding.productionParent))
    }
  }

  object AllowSpendReferenceCorrespondence {
    val nativeGenesis: AllowSpendReferenceCorrespondence =
      new AllowSpendReferenceCorrespondence(TransferLane.NativeGl1, AllowSpendReference.empty, StructuralAllowSpendReference.genesis)

    def currencyGenesis(metagraphId: Address, currentHasher: Hasher[IO]): IO[AllowSpendReferenceCorrespondence] = {
      implicit val scopedHasher: Hasher[IO] = currentHasher
      AllowSpendReference
        .emptyCurrency[IO](metagraphId)
        .map(reference =>
          new AllowSpendReferenceCorrespondence(TransferLane.CurrencyCl1(metagraphId), reference, StructuralAllowSpendReference.genesis)
        )
    }

    def fromClaimedGenesis(
      lane: TransferLane,
      claimed: AllowSpendReference,
      currentHasher: Hasher[IO]
    ): IO[Either[ProjectionError, AllowSpendReferenceCorrespondence]] =
      canonicalAllowSpendGenesis(lane, currentHasher).map { expected =>
        Either.cond(
          claimed == expected,
          new AllowSpendReferenceCorrespondence(lane, expected, StructuralAllowSpendReference.genesis),
          InvalidAllowSpendCanonicalGenesis(lane, expected, claimed)
        )
      }
  }

  final class TokenLockReferenceCorrespondence private[V4EconomicProductionProjection] (
    val lane: TransferLane,
    val production: TokenLockReference,
    val structural: StructuralTokenLockReference
  ) {
    def advance(observation: AcceptedTokenLockObservation): Either[ProjectionError, TokenLockReferenceCorrespondence] = {
      val binding = observation.binding
      if (binding.lane == lane && binding.productionParent == production && binding.structuralParent == structural)
        Right(new TokenLockReferenceCorrespondence(lane, binding.productionSuccessor, binding.structuralSuccessor))
      else Left(TokenLockCorrespondenceAdvanceMismatch(production, binding.productionParent))
    }
  }

  object TokenLockReferenceCorrespondence {
    val nativeGenesis: TokenLockReferenceCorrespondence =
      new TokenLockReferenceCorrespondence(TransferLane.NativeGl1, TokenLockReference.empty, StructuralTokenLockReference.genesis)

    def currencyGenesis(metagraphId: Address, currentHasher: Hasher[IO]): IO[TokenLockReferenceCorrespondence] = {
      implicit val scopedHasher: Hasher[IO] = currentHasher
      TokenLockReference
        .emptyCurrency[IO](metagraphId)
        .map(reference =>
          new TokenLockReferenceCorrespondence(TransferLane.CurrencyCl1(metagraphId), reference, StructuralTokenLockReference.genesis)
        )
    }

    def fromClaimedGenesis(
      lane: TransferLane,
      claimed: TokenLockReference,
      currentHasher: Hasher[IO]
    ): IO[Either[ProjectionError, TokenLockReferenceCorrespondence]] =
      canonicalTokenLockGenesis(lane, currentHasher).map { expected =>
        Either.cond(
          claimed == expected,
          new TokenLockReferenceCorrespondence(lane, expected, StructuralTokenLockReference.genesis),
          InvalidTokenLockCanonicalGenesis(lane, expected, claimed)
        )
      }
  }

  sealed trait SourceValidatedTransfer {
    def signed: Signed[Transaction]
    def lane: TransferLane
    def operationId: SupportedReferenceOperationId
    def signerFromProof: Address
    def allProofOwners: Vector[Address]
    def identity: StructuralSemanticIdentity
    def productionParent: TransactionReference
    def productionSuccessor: TransactionReference
    def structuralParent: StructuralReference
    def structuralSuccessor: StructuralReference
    final def domainBinding: PayloadDomainBinding = UnboundLegacyPayload
  }

  private final class SourceValidatedTransferImpl(
    val signed: Signed[Transaction],
    val lane: TransferLane,
    val operationId: SupportedReferenceOperationId,
    val signerFromProof: Address,
    val allProofOwners: Vector[Address],
    private[V4EconomicProductionProjection] val referenceInput: ReferenceInput.Transfer,
    val identity: StructuralSemanticIdentity,
    val productionParent: TransactionReference,
    val productionSuccessor: TransactionReference,
    val structuralParent: StructuralReference,
    val structuralSuccessor: StructuralReference
  ) extends SourceValidatedTransfer

  sealed trait SourceValidatedAllowSpend {
    def signed: Signed[AllowSpend]
    def lane: TransferLane
    def operationId: SupportedReferenceOperationId
    def signerFromProof: Address
    def allProofOwners: Vector[Address]
    def identity: AllowSpendSemanticIdentity
    def reservation: ReferenceAllowSpendReservation
    def productionParent: AllowSpendReference
    def productionSuccessor: AllowSpendReference
    def structuralParent: StructuralAllowSpendReference
    def structuralSuccessor: StructuralAllowSpendReference
    final def domainBinding: PayloadDomainBinding = UnboundLegacyPayload
  }

  private final class SourceValidatedAllowSpendImpl(
    val signed: Signed[AllowSpend],
    val lane: TransferLane,
    val operationId: SupportedReferenceOperationId,
    val signerFromProof: Address,
    val allProofOwners: Vector[Address],
    private[V4EconomicProductionProjection] val referenceInput: ReferenceInput.AllowSpendCreate,
    val identity: AllowSpendSemanticIdentity,
    val reservation: ReferenceAllowSpendReservation,
    val productionParent: AllowSpendReference,
    val productionSuccessor: AllowSpendReference,
    val structuralParent: StructuralAllowSpendReference,
    val structuralSuccessor: StructuralAllowSpendReference
  ) extends SourceValidatedAllowSpend

  sealed trait SourceValidatedTokenLock {
    def signed: Signed[TokenLock]
    def lane: TransferLane
    def operationId: SupportedReferenceOperationId
    def signerFromProof: Address
    def allProofOwners: Vector[Address]
    def productionHashed: Hashed[TokenLock]
    def identity: TokenLockSemanticIdentity
    def activeTokenLock: ReferenceActiveTokenLock
    def productionParent: TokenLockReference
    def productionSuccessor: TokenLockReference
    def structuralParent: StructuralTokenLockReference
    def structuralSuccessor: StructuralTokenLockReference
    final def domainBinding: PayloadDomainBinding = UnboundLegacyPayload
  }

  private final class SourceValidatedTokenLockImpl(
    val signed: Signed[TokenLock],
    val lane: TransferLane,
    val operationId: SupportedReferenceOperationId,
    val signerFromProof: Address,
    val allProofOwners: Vector[Address],
    val productionHashed: Hashed[TokenLock],
    private[V4EconomicProductionProjection] val referenceInput: ReferenceInput.TokenLockCreate,
    val identity: TokenLockSemanticIdentity,
    val activeTokenLock: ReferenceActiveTokenLock,
    val productionParent: TokenLockReference,
    val productionSuccessor: TokenLockReference,
    val structuralParent: StructuralTokenLockReference,
    val structuralSuccessor: StructuralTokenLockReference
  ) extends SourceValidatedTokenLock

  /** Test-only candidate ordering for the mixed reservation differential.
    *
    * This capability is private so a caller cannot turn an unvalidated payload into a reference input. The candidate heterogeneous rank is
    * transfer, allow-spend creation, then token-lock creation; input order is retained within each class. Live GL0 currently applies those
    * three classes in that order, while ML0 applies token locks before allow-spends. The rank is therefore evidence scaffolding, not a
    * production protocol decision.
    */
  private sealed trait MixedOperationCapability extends Product with Serializable {
    def heterogeneousRank: Int
    def withinClassIndex: Int
    def input: ReferenceInput
  }

  private object MixedOperationCapability {
    final case class Transfer(binding: SourceValidatedTransfer, withinClassIndex: Int) extends MixedOperationCapability {
      val heterogeneousRank: Int = 0
      val input: ReferenceInput = transferInput(binding)
    }

    final case class AllowSpendCreate(binding: SourceValidatedAllowSpend, withinClassIndex: Int) extends MixedOperationCapability {
      val heterogeneousRank: Int = 1
      val input: ReferenceInput = allowSpendInput(binding)
    }

    final case class TokenLockCreate(binding: SourceValidatedTokenLock, withinClassIndex: Int) extends MixedOperationCapability {
      val heterogeneousRank: Int = 2
      val input: ReferenceInput = tokenLockInput(binding)
    }
  }

  final case class ObservedBalanceDelta(before: Balance, after: Balance)
  final case class ObservedTransferSemanticDelta(
    balances: SortedMap[Address, ObservedBalanceDelta],
    referenceBefore: TransactionReference,
    referenceAfter: TransactionReference
  )
  final case class ObservedAllowSpendSemanticDelta(
    balances: SortedMap[Address, ObservedBalanceDelta],
    referenceBefore: AllowSpendReference,
    referenceAfter: AllowSpendReference,
    activeAdded: SortedSet[Signed[AllowSpend]]
  )
  final case class ObservedTokenLockSemanticDelta(
    balances: SortedMap[Address, ObservedBalanceDelta],
    referenceBefore: TokenLockReference,
    referenceAfter: TokenLockReference,
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )

  sealed trait AcceptedTransferObservation extends Product with Serializable {
    def binding: SourceValidatedTransfer
    def semanticDelta: ObservedTransferSemanticDelta
  }

  private final case class AcceptedTransferObservationImpl(
    binding: SourceValidatedTransfer,
    semanticDelta: ObservedTransferSemanticDelta
  ) extends AcceptedTransferObservation

  sealed trait AcceptedAllowSpendObservation extends Product with Serializable {
    def binding: SourceValidatedAllowSpend
    def semanticDelta: ObservedAllowSpendSemanticDelta
  }

  private final case class AcceptedAllowSpendObservationImpl(
    binding: SourceValidatedAllowSpend,
    semanticDelta: ObservedAllowSpendSemanticDelta
  ) extends AcceptedAllowSpendObservation

  sealed trait AcceptedTokenLockObservation extends Product with Serializable {
    def binding: SourceValidatedTokenLock
    def semanticDelta: ObservedTokenLockSemanticDelta
  }

  private final case class AcceptedTokenLockObservationImpl(
    binding: SourceValidatedTokenLock,
    semanticDelta: ObservedTokenLockSemanticDelta
  ) extends AcceptedTokenLockObservation

  sealed trait TransferBatchDisposition extends Product with Serializable
  case object TransferBatchAccepted extends TransferBatchDisposition
  final case class TransferBatchRejected(target: TransactionReference, reason: BlockRejectionReason) extends TransferBatchDisposition
  final case class TransferBatchAwaiting(target: TransactionReference, reason: BlockAwaitReason) extends TransferBatchDisposition

  sealed trait AllowSpendBatchDisposition extends Product with Serializable
  case object AllowSpendBatchAccepted extends AllowSpendBatchDisposition
  final case class AllowSpendBatchRejected(target: AllowSpendReference, reason: AllowSpendBlockRejectionReason)
      extends AllowSpendBatchDisposition
  final case class AllowSpendBatchAwaiting(target: AllowSpendReference, reason: AllowSpendBlockAwaitReason)
      extends AllowSpendBatchDisposition

  sealed trait TokenLockBatchDisposition extends Product with Serializable
  case object TokenLockBatchAccepted extends TokenLockBatchDisposition
  final case class TokenLockBatchRejected(target: TokenLockReference, reason: TokenLockBlockRejectionReason)
      extends TokenLockBatchDisposition
  final case class TokenLockBatchAwaiting(target: TokenLockReference, reason: TokenLockBlockAwaitReason) extends TokenLockBatchDisposition

  final case class TransferBatchItem(signed: Signed[Transaction], block: Signed[Block])
  final case class AllowSpendBatchItem(signed: Signed[AllowSpend], block: Signed[AllowSpendBlock])
  final case class TokenLockBatchItem(signed: Signed[TokenLock], block: Signed[TokenLockBlock])

  sealed trait TransferBatchProjection extends Product with Serializable {
    def bindings: Vector[SourceValidatedTransfer]
    def dispositions: Vector[TransferBatchDisposition]
    def semanticDelta: SortedMap[Address, ObservedBalanceDelta]
    def referenceExecution: ReferenceExecution
  }

  private final case class TransferBatchProjectionImpl(
    bindings: Vector[SourceValidatedTransfer],
    dispositions: Vector[TransferBatchDisposition],
    semanticDelta: SortedMap[Address, ObservedBalanceDelta],
    referenceExecution: ReferenceExecution
  ) extends TransferBatchProjection

  sealed trait AllowSpendBatchProjection extends Product with Serializable {
    def bindings: Vector[SourceValidatedAllowSpend]
    def dispositions: Vector[AllowSpendBatchDisposition]
    def semanticDelta: SortedMap[Address, ObservedBalanceDelta]
    def referenceExecution: ReferenceExecution
  }

  private final case class AllowSpendBatchProjectionImpl(
    bindings: Vector[SourceValidatedAllowSpend],
    dispositions: Vector[AllowSpendBatchDisposition],
    semanticDelta: SortedMap[Address, ObservedBalanceDelta],
    referenceExecution: ReferenceExecution
  ) extends AllowSpendBatchProjection

  sealed trait TokenLockBatchProjection extends Product with Serializable {
    def bindings: Vector[SourceValidatedTokenLock]
    def dispositions: Vector[TokenLockBatchDisposition]
    def semanticDelta: SortedMap[Address, ObservedBalanceDelta]
    def activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]]
    def referenceExecution: ReferenceExecution
  }

  private final case class TokenLockBatchProjectionImpl(
    bindings: Vector[SourceValidatedTokenLock],
    dispositions: Vector[TokenLockBatchDisposition],
    semanticDelta: SortedMap[Address, ObservedBalanceDelta],
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    referenceExecution: ReferenceExecution
  ) extends TokenLockBatchProjection

  def bindTransfer(
    signed: Signed[Transaction],
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: TransferReferenceCorrespondence,
    signedValidator: SignedValidator[IO],
    transactionHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, SourceValidatedTransfer]] = {
    val operationId = SupportedReferenceOperationId.transfer(lane)
    val transaction = signed.value
    val fee = BigInt(transaction.fee.value.value)

    if (fee != 0) IO.pure(Left(NonZeroFeeOutOfScope(operationId.value, fee)))
    else if (lane != correspondence.lane)
      IO.pure(Left(TransferParentBindingMismatch(correspondence.production, transaction.parent)))
    else if (transaction.parent != correspondence.production)
      IO.pure(Left(TransferParentBindingMismatch(correspondence.production, transaction.parent)))
    else
      verifySourceProof(signed, transaction.source, operationId.value, signedValidator, transactionHasher).flatMap {
        case Left(error) => IO.pure(Left(error))
        case Right((signer, allProofOwners)) =>
          val atom = TransferAtom(
            domain,
            lane,
            transaction.source,
            transaction.destination,
            BigInt(transaction.amount.value.value),
            fee,
            transaction.salt.value
          )
          val preimage = TransferPreimage(correspondence.structural, atom)
          val input = ReferenceInput.Transfer(preimage, StructurallyBoundSourceProof(signer, preimage))

          implicit val scopedHasher: Hasher[IO] = transactionHasher
          TransactionReference.of[IO](signed).map { productionSuccessor =>
            val structuralSuccessor = StructuralReference(
              correspondence.structural.ordinal + 1,
              correspondence.structural.lineage :+ atom
            )
            Right(
              new SourceValidatedTransferImpl(
                signed,
                lane,
                operationId,
                signer,
                allProofOwners,
                input,
                StructuralSemanticIdentity.derive(preimage),
                correspondence.production,
                productionSuccessor,
                correspondence.structural,
                structuralSuccessor
              )
            )
          }
      }
  }

  def bindAllowSpend(
    signed: Signed[AllowSpend],
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: AllowSpendReferenceCorrespondence,
    signedValidator: SignedValidator[IO],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, SourceValidatedAllowSpend]] = {
    val operationId = SupportedReferenceOperationId.AllowSpendCreation
    val allowSpend = signed.value
    val fee = BigInt(allowSpend.fee.value.value)
    val laneMatches = lane match {
      case TransferLane.NativeGl1                => allowSpend.currencyId.isEmpty
      case TransferLane.CurrencyCl1(metagraphId) => allowSpend.currencyId.contains(CurrencyId(metagraphId))
    }

    if (!laneMatches) IO.pure(Left(PayloadLaneMismatch(lane, allowSpend.currencyId)))
    else if (fee != 0) IO.pure(Left(NonZeroFeeOutOfScope(operationId.value, fee)))
    else if (lane != correspondence.lane)
      IO.pure(Left(AllowSpendParentBindingMismatch(correspondence.production, allowSpend.parent)))
    else if (allowSpend.parent != correspondence.production)
      IO.pure(Left(AllowSpendParentBindingMismatch(correspondence.production, allowSpend.parent)))
    else
      verifySourceProof(signed, allowSpend.source, operationId.value, signedValidator, currentHasher).flatMap {
        case Left(error) => IO.pure(Left(error))
        case Right((signer, allProofOwners)) =>
          val atom = AllowSpendAtom(
            domain,
            lane,
            allowSpend.source,
            allowSpend.destination,
            BigInt(allowSpend.amount.value.value),
            fee,
            BigInt(allowSpend.lastValidEpochProgress.value.value),
            allowSpend.approvers.toVector
          )
          val preimage = AllowSpendPreimage(correspondence.structural, atom)
          val identity = AllowSpendSemanticIdentity.derive(preimage)
          val input = ReferenceInput.AllowSpendCreate(preimage, StructurallyBoundAllowSpendSourceProof(signer, preimage))
          val reservation = ReferenceAllowSpendReservation(
            identity,
            lane.scope,
            allowSpend.source,
            allowSpend.destination,
            BigInt(allowSpend.amount.value.value),
            BigInt(allowSpend.lastValidEpochProgress.value.value),
            allowSpend.approvers.toVector
          )

          implicit val scopedHasher: Hasher[IO] = currentHasher
          AllowSpendReference.of[IO](signed).map { productionSuccessor =>
            val structuralSuccessor = StructuralAllowSpendReference(
              correspondence.structural.ordinal + 1,
              correspondence.structural.lineage :+ atom
            )
            Right(
              new SourceValidatedAllowSpendImpl(
                signed,
                lane,
                operationId,
                signer,
                allProofOwners,
                input,
                identity,
                reservation,
                correspondence.production,
                productionSuccessor,
                correspondence.structural,
                structuralSuccessor
              )
            )
          }
      }
  }

  def bindTokenLock(
    signed: Signed[TokenLock],
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: TokenLockReferenceCorrespondence,
    signedValidator: SignedValidator[IO],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, SourceValidatedTokenLock]] = {
    val operationId = SupportedReferenceOperationId.TokenLockCreation
    val tokenLock = signed.value
    val fee = BigInt(tokenLock.fee.value.value)
    val laneMatches = lane match {
      case TransferLane.NativeGl1                => tokenLock.currencyId.isEmpty
      case TransferLane.CurrencyCl1(metagraphId) => tokenLock.currencyId.contains(CurrencyId(metagraphId))
    }

    if (!laneMatches) IO.pure(Left(PayloadLaneMismatch(lane, tokenLock.currencyId)))
    else if (fee != 0) IO.pure(Left(NonZeroFeeOutOfScope(operationId.value, fee)))
    else
      tokenLock.replaceTokenLockRef match {
        case Some(reference) => IO.pure(Left(TokenLockReplacementOutOfScope(reference)))
        case None if lane != correspondence.lane =>
          IO.pure(Left(TokenLockParentBindingMismatch(correspondence.production, tokenLock.parent)))
        case None if tokenLock.parent != correspondence.production =>
          IO.pure(Left(TokenLockParentBindingMismatch(correspondence.production, tokenLock.parent)))
        case None =>
          verifySourceProof(signed, tokenLock.source, operationId.value, signedValidator, currentHasher).flatMap {
            case Left(error) => IO.pure(Left(error))
            case Right((signer, allProofOwners)) =>
              val atom = TokenLockAtom(
                domain,
                lane,
                tokenLock.source,
                BigInt(tokenLock.amount.value.value),
                fee,
                tokenLock.unlockEpoch.map(value => BigInt(value.value.value)),
                tokenLock.replaceTokenLockRef
              )
              val preimage = TokenLockPreimage(correspondence.structural, atom)
              val identity = TokenLockSemanticIdentity.derive(preimage)
              val input = ReferenceInput.TokenLockCreate(preimage, StructurallyBoundTokenLockSourceProof(signer, preimage))
              val activeTokenLock = ReferenceActiveTokenLock(
                identity,
                lane.scope,
                tokenLock.source,
                BigInt(tokenLock.amount.value.value),
                tokenLock.unlockEpoch.map(value => BigInt(value.value.value))
              )

              implicit val scopedHasher: Hasher[IO] = currentHasher
              signed.toHashed[IO].map { productionHashed =>
                val productionSuccessor = TokenLockReference.of(productionHashed)
                val structuralSuccessor = StructuralTokenLockReference(
                  correspondence.structural.ordinal + 1,
                  correspondence.structural.lineage :+ atom
                )
                Right(
                  new SourceValidatedTokenLockImpl(
                    signed,
                    lane,
                    operationId,
                    signer,
                    allProofOwners,
                    productionHashed,
                    input,
                    identity,
                    activeTokenLock,
                    correspondence.production,
                    productionSuccessor,
                    correspondence.structural,
                    structuralSuccessor
                  )
                )
              }
          }
      }
  }

  def observeAcceptedTransfer(
    binding: SourceValidatedTransfer,
    balancesBefore: SortedMap[Address, Balance],
    production: Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, NonNegLong)]
  ): Either[ProjectionError, AcceptedTransferObservation] =
    production
      .leftMap(ProductionTransferNotAccepted(binding.productionSuccessor, _))
      .flatMap { case (update, _) => observeAcceptedTransferUpdate(binding, balancesBefore, update) }

  def observeBatchAcceptedTransfer(
    binding: SourceValidatedTransfer,
    block: Signed[Block],
    balancesBefore: SortedMap[Address, Balance],
    production: BlockAcceptanceResult
  ): Either[ProjectionError, AcceptedTransferObservation] = {
    val blockTransactions = block.value.transactions.toNonEmptyList.toList.toVector

    Either
      .cond(
        blockTransactions == Vector(binding.signed),
        (),
        TransferBatchPayloadMismatch(binding.signed, blockTransactions)
      )
      .flatMap(_ =>
        Either.cond(
          production.accepted.exists(_._1 == block) && !production.notAccepted.exists(_._1 == block),
          (),
          TransferBlockNotAcceptedByBatch(block)
        )
      )
      .flatMap(_ => observeAcceptedTransferUpdate(binding, balancesBefore, production.contextUpdate))
  }

  def observeAcceptedAllowSpend(
    binding: SourceValidatedAllowSpend,
    balancesBefore: SortedMap[Address, Balance],
    production: Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate],
    activeBefore: SortedSet[Signed[AllowSpend]],
    activeAfter: SortedSet[Signed[AllowSpend]]
  ): Either[ProjectionError, AcceptedAllowSpendObservation] =
    production
      .leftMap(ProductionAllowSpendNotAccepted(binding.productionSuccessor, _))
      .flatMap(update => observeAcceptedAllowSpendUpdate(binding, balancesBefore, update, activeBefore, activeAfter))

  def observeBatchAcceptedAllowSpend(
    binding: SourceValidatedAllowSpend,
    block: Signed[AllowSpendBlock],
    balancesBefore: SortedMap[Address, Balance],
    production: AllowSpendBlockAcceptanceResult,
    activeBefore: SortedSet[Signed[AllowSpend]],
    activeAfter: SortedSet[Signed[AllowSpend]]
  ): Either[ProjectionError, AcceptedAllowSpendObservation] = {
    val blockTransactions = block.value.transactions.toNonEmptyList.toList.toVector

    Either
      .cond(
        blockTransactions == Vector(binding.signed),
        (),
        AllowSpendBatchPayloadMismatch(binding.signed, blockTransactions)
      )
      .flatMap(_ =>
        Either.cond(
          production.accepted.contains(block) && !production.notAccepted.exists(_._1 == block),
          (),
          AllowSpendBlockNotAcceptedByBatch(block)
        )
      )
      .flatMap(_ =>
        observeAcceptedAllowSpendUpdate(
          binding,
          balancesBefore,
          production.contextUpdate,
          activeBefore,
          activeAfter
        )
      )
  }

  def observeBatchAcceptedTokenLock(
    binding: SourceValidatedTokenLock,
    block: Signed[TokenLockBlock],
    balancesBefore: SortedMap[Address, Balance],
    production: TokenLockBlockAcceptanceResult,
    activeBefore: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  ): Either[ProjectionError, AcceptedTokenLockObservation] = {
    val blockTokenLocks = block.value.tokenLocks.toNonEmptyList.toList.toVector

    Either
      .cond(
        blockTokenLocks == Vector(binding.signed),
        (),
        TokenLockBatchPayloadMismatch(binding.signed, blockTokenLocks)
      )
      .flatMap(_ =>
        Either.cond(
          production.accepted == List(block) && production.notAccepted.isEmpty,
          (),
          UnexpectedTokenLockBatchDecisions(block, production.accepted, production.notAccepted)
        )
      )
      .flatMap(_ => observeAcceptedTokenLockUpdate(binding, balancesBefore, production.contextUpdate, activeBefore, activeAfter))
  }

  def projectTransferBatch(
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: TransferReferenceCorrespondence,
    items: Vector[TransferBatchItem],
    balancesBefore: SortedMap[Address, Balance],
    production: BlockAcceptanceResult,
    referenceContext: ReferenceContext,
    referenceBase: ReferenceState,
    signedValidator: SignedValidator[IO],
    transactionHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, TransferBatchProjection]] = {
    final case class BatchCursor(
      production: TransactionReference,
      structural: StructuralReference
    )

    def loop(
      remaining: List[TransferBatchItem],
      cursor: BatchCursor,
      bindings: Vector[SourceValidatedTransfer],
      dispositions: Vector[TransferBatchDisposition]
    ): IO[Either[ProjectionError, (BatchCursor, Vector[SourceValidatedTransfer], Vector[TransferBatchDisposition])]] =
      remaining match {
        case Nil => IO.pure(Right((cursor, bindings, dispositions)))
        case item :: tail =>
          val blockTransactions = item.block.value.transactions.toNonEmptyList.toList.toVector
          if (blockTransactions != Vector(item.signed))
            IO.pure(Left(TransferBatchPayloadMismatch(item.signed, blockTransactions)))
          else {
            val claimed = new TransferReferenceCorrespondence(lane, cursor.production, cursor.structural)
            bindTransfer(item.signed, domain, lane, claimed, signedValidator, transactionHasher).flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(binding) =>
                transferBatchDisposition(item.block, binding.productionSuccessor, production) match {
                  case Left(error) => IO.pure(Left(error))
                  case Right(disposition) =>
                    val next = disposition match {
                      case TransferBatchAccepted =>
                        BatchCursor(binding.productionSuccessor, binding.structuralSuccessor)
                      case _ => cursor
                    }
                    loop(tail, next, bindings :+ binding, dispositions :+ disposition)
                }
            }
          }
      }

    val initialCursor = BatchCursor(correspondence.production, correspondence.structural)
    loop(items.toList, initialCursor, Vector.empty, Vector.empty).map {
      _.flatMap {
        case (finalCursor, bindings, dispositions) =>
          val sources = items.iterator.map(_.signed.value.source).toSet
          for {
            source <- sources.toList match {
              case only :: Nil => Right(only)
              case _           => Left(TransferBatchRequiresSingleSource(sources))
            }
            expectedReferences = Map(source -> finalCursor.production)
            _ <- Either.cond(
              production.contextUpdate.lastTxRefs == expectedReferences,
              (),
              UnexpectedTransferReferenceUpdates(expectedReferences, production.contextUpdate.lastTxRefs)
            )
            acceptedTransactions = items.zip(dispositions).collect {
              case (item, TransferBatchAccepted) => item.signed.value
            }
            expectedBalanceKeys = acceptedTransactions.iterator.flatMap(tx => Iterator(tx.source, tx.destination)).toSet
            _ <- Either.cond(
              production.contextUpdate.balances.keySet == expectedBalanceKeys,
              (),
              UnexpectedTransferBalanceKeys(expectedBalanceKeys, production.contextUpdate.balances.keySet)
            )
            semanticDelta = SortedMap.from(production.contextUpdate.balances.iterator.map {
              case (address, after) =>
                address -> ObservedBalanceDelta(balancesBefore.getOrElse(address, Balance.empty), after)
            })
            execution <- executeTransfers(referenceContext, referenceBase, bindings).leftMap(ReferenceExecutionProjectionFailed)
          } yield TransferBatchProjectionImpl(bindings, dispositions, semanticDelta, execution)
      }
    }
  }

  def projectAllowSpendBatch(
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: AllowSpendReferenceCorrespondence,
    items: Vector[AllowSpendBatchItem],
    balancesBefore: SortedMap[Address, Balance],
    production: AllowSpendBlockAcceptanceResult,
    referenceContext: ReferenceContext,
    referenceBase: ReferenceState,
    signedValidator: SignedValidator[IO],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, AllowSpendBatchProjection]] = {
    final case class BatchCursor(
      production: AllowSpendReference,
      structural: StructuralAllowSpendReference
    )

    def loop(
      remaining: List[AllowSpendBatchItem],
      cursor: BatchCursor,
      bindings: Vector[SourceValidatedAllowSpend],
      dispositions: Vector[AllowSpendBatchDisposition]
    ): IO[Either[ProjectionError, (BatchCursor, Vector[SourceValidatedAllowSpend], Vector[AllowSpendBatchDisposition])]] =
      remaining match {
        case Nil => IO.pure(Right((cursor, bindings, dispositions)))
        case item :: tail =>
          val blockTransactions = item.block.value.transactions.toNonEmptyList.toList.toVector
          if (blockTransactions != Vector(item.signed))
            IO.pure(Left(AllowSpendBatchPayloadMismatch(item.signed, blockTransactions)))
          else {
            val claimed = new AllowSpendReferenceCorrespondence(lane, cursor.production, cursor.structural)
            bindAllowSpend(item.signed, domain, lane, claimed, signedValidator, currentHasher).flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(binding) =>
                allowSpendBatchDisposition(item.block, binding.productionSuccessor, production) match {
                  case Left(error) => IO.pure(Left(error))
                  case Right(disposition) =>
                    val next = disposition match {
                      case AllowSpendBatchAccepted =>
                        BatchCursor(binding.productionSuccessor, binding.structuralSuccessor)
                      case _ => cursor
                    }
                    loop(tail, next, bindings :+ binding, dispositions :+ disposition)
                }
            }
          }
      }

    val initialCursor = BatchCursor(correspondence.production, correspondence.structural)
    loop(items.toList, initialCursor, Vector.empty, Vector.empty).map {
      _.flatMap {
        case (finalCursor, bindings, dispositions) =>
          val sources = items.iterator.map(_.signed.value.source).toSet
          for {
            source <- sources.toList match {
              case only :: Nil => Right(only)
              case _           => Left(AllowSpendBatchRequiresSingleSource(sources))
            }
            expectedReferences = Map(source -> finalCursor.production)
            _ <- Either.cond(
              production.contextUpdate.lastTxRefs == expectedReferences,
              (),
              UnexpectedAllowSpendReferenceUpdates(expectedReferences, production.contextUpdate.lastTxRefs)
            )
            expectedBalanceKeys = Set(source)
            _ <- Either.cond(
              production.contextUpdate.balances.keySet == expectedBalanceKeys,
              (),
              UnexpectedAllowSpendBalanceKeys(expectedBalanceKeys, production.contextUpdate.balances.keySet)
            )
            semanticDelta = SortedMap.from(production.contextUpdate.balances.iterator.map {
              case (address, after) =>
                address -> ObservedBalanceDelta(balancesBefore.getOrElse(address, Balance.empty), after)
            })
            execution <- executeAllowSpends(referenceContext, referenceBase, bindings).leftMap(ReferenceExecutionProjectionFailed)
          } yield AllowSpendBatchProjectionImpl(bindings, dispositions, semanticDelta, execution)
      }
    }
  }

  def projectTokenLockBatch(
    domain: ReferenceDomain,
    lane: TransferLane,
    correspondence: TokenLockReferenceCorrespondence,
    items: Vector[TokenLockBatchItem],
    balancesBefore: SortedMap[Address, Balance],
    production: TokenLockBlockAcceptanceResult,
    activeBefore: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    referenceContext: ReferenceContext,
    referenceBase: ReferenceState,
    signedValidator: SignedValidator[IO],
    currentHasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, TokenLockBatchProjection]] = {
    final case class BatchCursor(
      production: TokenLockReference,
      structural: StructuralTokenLockReference
    )

    def loop(
      remaining: List[TokenLockBatchItem],
      cursor: BatchCursor,
      bindings: Vector[SourceValidatedTokenLock],
      dispositions: Vector[TokenLockBatchDisposition]
    ): IO[Either[ProjectionError, (BatchCursor, Vector[SourceValidatedTokenLock], Vector[TokenLockBatchDisposition])]] =
      remaining match {
        case Nil => IO.pure(Right((cursor, bindings, dispositions)))
        case item :: tail =>
          val blockTokenLocks = item.block.value.tokenLocks.toNonEmptyList.toList.toVector
          if (blockTokenLocks != Vector(item.signed))
            IO.pure(Left(TokenLockBatchPayloadMismatch(item.signed, blockTokenLocks)))
          else {
            val claimed = new TokenLockReferenceCorrespondence(lane, cursor.production, cursor.structural)
            bindTokenLock(item.signed, domain, lane, claimed, signedValidator, currentHasher).flatMap {
              case Left(error) => IO.pure(Left(error))
              case Right(binding) =>
                tokenLockBatchDisposition(item.block, binding.productionSuccessor, production) match {
                  case Left(error) => IO.pure(Left(error))
                  case Right(disposition) =>
                    val next = disposition match {
                      case TokenLockBatchAccepted =>
                        BatchCursor(binding.productionSuccessor, binding.structuralSuccessor)
                      case _ => cursor
                    }
                    loop(tail, next, bindings :+ binding, dispositions :+ disposition)
                }
            }
          }
      }

    val expectedBlocks = items.map(_.block)
    val acceptedBlocks = production.accepted.toVector
    val notAcceptedBlocks = production.notAccepted.map(_._1).toVector
    val exactDecisionSet =
      expectedBlocks.nonEmpty &&
        expectedBlocks.distinct.size == expectedBlocks.size &&
        acceptedBlocks.distinct.size == acceptedBlocks.size &&
        notAcceptedBlocks.distinct.size == notAcceptedBlocks.size &&
        acceptedBlocks.toSet.intersect(notAcceptedBlocks.toSet).isEmpty &&
        (acceptedBlocks ++ notAcceptedBlocks).toSet == expectedBlocks.toSet &&
        acceptedBlocks.size + notAcceptedBlocks.size == expectedBlocks.size

    if (!exactDecisionSet)
      IO.pure(Left(UnexpectedTokenLockBatchDecisionSet(expectedBlocks, production.accepted, production.notAccepted)))
    else {
      val itemsByBlock = items.iterator.map(item => item.block -> item).toMap
      val acceptedItems = production.accepted.iterator.map(itemsByBlock).toVector
      val notAcceptedItems = production.notAccepted.iterator.map { case (block, _) => itemsByBlock(block) }.toVector
      val orderedItems = acceptedItems ++ notAcceptedItems
      val initialCursor = BatchCursor(correspondence.production, correspondence.structural)

      loop(orderedItems.toList, initialCursor, Vector.empty, Vector.empty).map {
        _.flatMap {
          case (_, bindings, dispositions) =>
            val sources = items.iterator.map(_.signed.value.source).toSet
            for {
              source <- sources.toList match {
                case only :: Nil => Right(only)
                case _           => Left(TokenLockBatchRequiresSingleSource(sources))
              }
              acceptedBindings = bindings.zip(dispositions).collect {
                case (binding, TokenLockBatchAccepted) => binding
              }
              expectedReferences = acceptedBindings.lastOption
                .map(binding => Map(source -> binding.productionSuccessor))
                .getOrElse(Map.empty[Address, TokenLockReference])
              _ <- Either.cond(
                production.contextUpdate.lastTokenLocksRefs == expectedReferences,
                (),
                UnexpectedTokenLockReferenceUpdates(expectedReferences, production.contextUpdate.lastTokenLocksRefs)
              )
              expectedBalanceKeys = acceptedBindings.iterator.map(_.signed.value.source).toSet
              _ <- Either.cond(
                production.contextUpdate.balances.keySet == expectedBalanceKeys,
                (),
                UnexpectedTokenLockBalanceKeys(expectedBalanceKeys, production.contextUpdate.balances.keySet)
              )
              expectedClaimedReplacementRefs = Set.empty[Hash]
              _ <- Either.cond(
                production.contextUpdate.claimedReplacementRefs == expectedClaimedReplacementRefs,
                (),
                UnexpectedTokenLockClaimedReplacementRefs(
                  expectedClaimedReplacementRefs,
                  production.contextUpdate.claimedReplacementRefs
                )
              )
              expectedInRoundTokenLocks = lane match {
                case TransferLane.NativeGl1 =>
                  acceptedBindings.iterator.map(binding => binding.productionHashed.hash -> binding.productionHashed).toMap
                case _: TransferLane.CurrencyCl1 => Map.empty[Hash, Hashed[TokenLock]]
              }
              _ <- Either.cond(
                production.contextUpdate.inRoundTokenLocksByHash == expectedInRoundTokenLocks,
                (),
                UnexpectedInRoundTokenLockState(expectedInRoundTokenLocks, production.contextUpdate.inRoundTokenLocksByHash)
              )
              expectedActiveAfter = acceptedBindings.foldLeft(activeBefore) { (acc, binding) =>
                val bindingSource = binding.signed.value.source
                acc.updated(
                  bindingSource,
                  acc.getOrElse(bindingSource, SortedSet.empty[Signed[TokenLock]]) + binding.signed
                )
              }
              _ <- Either.cond(
                activeAfter == expectedActiveAfter,
                (),
                UnexpectedActiveTokenLockState(expectedActiveAfter, activeAfter)
              )
              execution <- executeTokenLocks(referenceContext, referenceBase, bindings)
                .leftMap(ReferenceExecutionProjectionFailed)
              expectedStructuralReference = acceptedBindings.lastOption
                .map(_.structuralSuccessor)
                .getOrElse(correspondence.structural)
              actualStructuralReference = execution.finalState.lastTokenLockRefOf(
                ReferenceTokenLockChainAccount(lane, source)
              )
              _ <- Either.cond(
                actualStructuralReference == expectedStructuralReference,
                (),
                UnexpectedTokenLockBatchReference(expectedStructuralReference, actualStructuralReference)
              )
              _ <- acceptedBindings.lastOption match {
                case None => Right(())
                case Some(_) =>
                  val referenceBalance = execution.finalState.balanceOf(ReferenceBalanceAccount(lane.scope, source))
                  production.contextUpdate.balances
                    .get(source)
                    .toRight(UnexpectedTokenLockBalanceKeys(Set(source), production.contextUpdate.balances.keySet))
                    .flatMap { productionBalance =>
                      val actual = BigInt(productionBalance.value.value)
                      Either.cond(
                        actual == referenceBalance,
                        (),
                        UnexpectedTokenLockBatchBalance(source, referenceBalance, actual)
                      )
                    }
              }
              semanticDelta = SortedMap.from(production.contextUpdate.balances.iterator.map {
                case (address, after) =>
                  address -> ObservedBalanceDelta(balancesBefore.getOrElse(address, Balance.empty), after)
              })
            } yield TokenLockBatchProjectionImpl(bindings, dispositions, semanticDelta, activeAfter, execution)
        }
      }
    }
  }

  def executeTransfers(
    context: ReferenceContext,
    base: ReferenceState,
    bindings: Vector[SourceValidatedTransfer]
  ): Either[ReferenceStateError, ReferenceExecution] =
    V4EconomicReferenceInterpreter.execute(context, base, bindings.map(transferInput))

  def executeAllowSpends(
    context: ReferenceContext,
    base: ReferenceState,
    bindings: Vector[SourceValidatedAllowSpend]
  ): Either[ReferenceStateError, ReferenceExecution] =
    V4EconomicReferenceInterpreter.execute(context, base, bindings.map(allowSpendInput))

  def executeTokenLocks(
    context: ReferenceContext,
    base: ReferenceState,
    bindings: Vector[SourceValidatedTokenLock]
  ): Either[ReferenceStateError, ReferenceExecution] =
    V4EconomicReferenceInterpreter.execute(context, base, bindings.map(tokenLockInput))

  def executeCandidateMixedReservations(
    context: ReferenceContext,
    base: ReferenceState,
    transfers: Vector[SourceValidatedTransfer],
    allowSpendCreates: Vector[SourceValidatedAllowSpend],
    tokenLockCreates: Vector[SourceValidatedTokenLock]
  ): Either[ReferenceStateError, ReferenceExecution] = {
    import MixedOperationCapability._

    val capabilities: Vector[MixedOperationCapability] =
      transfers.zipWithIndex.map { case (binding, index) => Transfer(binding, index) } ++
        allowSpendCreates.zipWithIndex.map { case (binding, index) => AllowSpendCreate(binding, index) } ++
        tokenLockCreates.zipWithIndex.map { case (binding, index) => TokenLockCreate(binding, index) }
    val orderedInputs = capabilities.sortBy(capability => (capability.heterogeneousRank, capability.withinClassIndex)).map(_.input)

    V4EconomicReferenceInterpreter.execute(context, base, orderedInputs)
  }

  private def observeAcceptedTransferUpdate(
    binding: SourceValidatedTransfer,
    balancesBefore: SortedMap[Address, Balance],
    update: BlockAcceptanceContextUpdate
  ): Either[ProjectionError, AcceptedTransferObservation] = {
    val transaction = binding.signed.value
    val expectedBalanceKeys = Set(transaction.source, transaction.destination)
    val expectedReferenceUpdates = Map(transaction.source -> binding.productionSuccessor)

    for {
      _ <- Either.cond(
        update.balances.keySet == expectedBalanceKeys,
        (),
        UnexpectedTransferBalanceKeys(expectedBalanceKeys, update.balances.keySet)
      )
      _ <- Either.cond(
        update.lastTxRefs == expectedReferenceUpdates,
        (),
        UnexpectedTransferReferenceUpdates(expectedReferenceUpdates, update.lastTxRefs)
      )
      observedBalances = SortedMap.from(update.balances.iterator.map {
        case (address, after) =>
          address -> ObservedBalanceDelta(balancesBefore.getOrElse(address, Balance.empty), after)
      })
      semanticDelta = ObservedTransferSemanticDelta(
        observedBalances,
        binding.productionParent,
        binding.productionSuccessor
      )
    } yield AcceptedTransferObservationImpl(binding, semanticDelta)
  }

  private def observeAcceptedAllowSpendUpdate(
    binding: SourceValidatedAllowSpend,
    balancesBefore: SortedMap[Address, Balance],
    update: AllowSpendBlockAcceptanceContextUpdate,
    activeBefore: SortedSet[Signed[AllowSpend]],
    activeAfter: SortedSet[Signed[AllowSpend]]
  ): Either[ProjectionError, AcceptedAllowSpendObservation] = {
    val allowSpend = binding.signed.value
    val expectedBalanceKeys = Set(allowSpend.source)
    val expectedReferenceUpdates = Map(allowSpend.source -> binding.productionSuccessor)
    val expectedActiveDelta = SortedSet(binding.signed)
    val actualActiveDelta = activeAfter -- activeBefore

    for {
      _ <- Either.cond(
        update.balances.keySet == expectedBalanceKeys,
        (),
        UnexpectedAllowSpendBalanceKeys(expectedBalanceKeys, update.balances.keySet)
      )
      _ <- Either.cond(
        update.lastTxRefs == expectedReferenceUpdates,
        (),
        UnexpectedAllowSpendReferenceUpdates(expectedReferenceUpdates, update.lastTxRefs)
      )
      _ <- Either.cond(
        actualActiveDelta == expectedActiveDelta,
        (),
        UnexpectedActiveAllowSpendDelta(expectedActiveDelta, actualActiveDelta)
      )
      after <- update.balances
        .get(allowSpend.source)
        .toRight(UnexpectedAllowSpendBalanceKeys(expectedBalanceKeys, update.balances.keySet))
      observedBalances = SortedMap(
        allowSpend.source -> ObservedBalanceDelta(balancesBefore.getOrElse(allowSpend.source, Balance.empty), after)
      )
      semanticDelta = ObservedAllowSpendSemanticDelta(
        observedBalances,
        binding.productionParent,
        binding.productionSuccessor,
        actualActiveDelta
      )
    } yield AcceptedAllowSpendObservationImpl(binding, semanticDelta)
  }

  private def observeAcceptedTokenLockUpdate(
    binding: SourceValidatedTokenLock,
    balancesBefore: SortedMap[Address, Balance],
    update: TokenLockBlockAcceptanceContextUpdate,
    activeBefore: SortedMap[Address, SortedSet[Signed[TokenLock]]],
    activeAfter: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  ): Either[ProjectionError, AcceptedTokenLockObservation] = {
    val tokenLock = binding.signed.value
    val expectedBalanceKeys = Set(tokenLock.source)
    val expectedReferenceUpdates = Map(tokenLock.source -> binding.productionSuccessor)
    val expectedClaimedReplacementRefs = Set.empty[Hash]
    val expectedInRoundTokenLocks = binding.lane match {
      case TransferLane.NativeGl1      => Map(binding.productionHashed.hash -> binding.productionHashed)
      case _: TransferLane.CurrencyCl1 => Map.empty[Hash, Hashed[TokenLock]]
    }
    val expectedActiveAfter = activeBefore.updated(
      tokenLock.source,
      activeBefore.getOrElse(tokenLock.source, SortedSet.empty[Signed[TokenLock]]) + binding.signed
    )

    for {
      _ <- Either.cond(
        update.balances.keySet == expectedBalanceKeys,
        (),
        UnexpectedTokenLockBalanceKeys(expectedBalanceKeys, update.balances.keySet)
      )
      _ <- Either.cond(
        update.lastTokenLocksRefs == expectedReferenceUpdates,
        (),
        UnexpectedTokenLockReferenceUpdates(expectedReferenceUpdates, update.lastTokenLocksRefs)
      )
      _ <- Either.cond(
        update.claimedReplacementRefs == expectedClaimedReplacementRefs,
        (),
        UnexpectedTokenLockClaimedReplacementRefs(expectedClaimedReplacementRefs, update.claimedReplacementRefs)
      )
      _ <- Either.cond(
        update.inRoundTokenLocksByHash == expectedInRoundTokenLocks,
        (),
        UnexpectedInRoundTokenLockState(expectedInRoundTokenLocks, update.inRoundTokenLocksByHash)
      )
      _ <- Either.cond(
        activeAfter == expectedActiveAfter,
        (),
        UnexpectedActiveTokenLockState(expectedActiveAfter, activeAfter)
      )
      after <- update.balances
        .get(tokenLock.source)
        .toRight(UnexpectedTokenLockBalanceKeys(expectedBalanceKeys, update.balances.keySet))
      observedBalances = SortedMap(
        tokenLock.source -> ObservedBalanceDelta(balancesBefore.getOrElse(tokenLock.source, Balance.empty), after)
      )
      semanticDelta = ObservedTokenLockSemanticDelta(
        observedBalances,
        binding.productionParent,
        binding.productionSuccessor,
        activeAfter
      )
    } yield AcceptedTokenLockObservationImpl(binding, semanticDelta)
  }

  private def transferInput(binding: SourceValidatedTransfer): ReferenceInput.Transfer =
    binding match {
      case value: SourceValidatedTransferImpl => value.referenceInput
    }

  private def allowSpendInput(binding: SourceValidatedAllowSpend): ReferenceInput.AllowSpendCreate =
    binding match {
      case value: SourceValidatedAllowSpendImpl => value.referenceInput
    }

  private def tokenLockInput(binding: SourceValidatedTokenLock): ReferenceInput.TokenLockCreate =
    binding match {
      case value: SourceValidatedTokenLockImpl => value.referenceInput
    }

  private def transferBatchDisposition(
    block: Signed[Block],
    target: TransactionReference,
    production: BlockAcceptanceResult
  ): Either[ProjectionError, TransferBatchDisposition] = {
    val accepted = production.accepted.exists(_._1 == block)
    val notAccepted = production.notAccepted.collectFirst { case (`block`, reason) => reason }
    (accepted, notAccepted) match {
      case (true, None)                                => Right(TransferBatchAccepted)
      case (false, Some(reason: BlockRejectionReason)) => Right(TransferBatchRejected(target, reason))
      case (false, Some(reason: BlockAwaitReason))     => Right(TransferBatchAwaiting(target, reason))
      case _                                           => Left(TransferBatchDecisionMissing(block))
    }
  }

  private def allowSpendBatchDisposition(
    block: Signed[AllowSpendBlock],
    target: AllowSpendReference,
    production: AllowSpendBlockAcceptanceResult
  ): Either[ProjectionError, AllowSpendBatchDisposition] = {
    val accepted = production.accepted.contains(block)
    val notAccepted = production.notAccepted.collectFirst { case (`block`, reason) => reason }
    (accepted, notAccepted) match {
      case (true, None)                                          => Right(AllowSpendBatchAccepted)
      case (false, Some(reason: AllowSpendBlockRejectionReason)) => Right(AllowSpendBatchRejected(target, reason))
      case (false, Some(reason: AllowSpendBlockAwaitReason))     => Right(AllowSpendBatchAwaiting(target, reason))
      case _                                                     => Left(AllowSpendBatchDecisionMissing(block))
    }
  }

  private def tokenLockBatchDisposition(
    block: Signed[TokenLockBlock],
    target: TokenLockReference,
    production: TokenLockBlockAcceptanceResult
  ): Either[ProjectionError, TokenLockBatchDisposition] = {
    val accepted = production.accepted.contains(block)
    val notAccepted = production.notAccepted.collectFirst { case (`block`, reason) => reason }
    (accepted, notAccepted) match {
      case (true, None)                                         => Right(TokenLockBatchAccepted)
      case (false, Some(reason: TokenLockBlockRejectionReason)) => Right(TokenLockBatchRejected(target, reason))
      case (false, Some(reason: TokenLockBlockAwaitReason))     => Right(TokenLockBatchAwaiting(target, reason))
      case _                                                    => Left(TokenLockBatchDecisionMissing(block))
    }
  }

  private def canonicalTransferGenesis(lane: TransferLane, currentHasher: Hasher[IO]): IO[TransactionReference] =
    lane match {
      case TransferLane.NativeGl1 => IO.pure(TransactionReference.empty)
      case TransferLane.CurrencyCl1(metagraphId) =>
        implicit val scopedHasher: Hasher[IO] = currentHasher
        TransactionReference.emptyCurrency[IO](metagraphId)
    }

  private def canonicalAllowSpendGenesis(lane: TransferLane, currentHasher: Hasher[IO]): IO[AllowSpendReference] =
    lane match {
      case TransferLane.NativeGl1 => IO.pure(AllowSpendReference.empty)
      case TransferLane.CurrencyCl1(metagraphId) =>
        implicit val scopedHasher: Hasher[IO] = currentHasher
        AllowSpendReference.emptyCurrency[IO](metagraphId)
    }

  private def canonicalTokenLockGenesis(lane: TransferLane, currentHasher: Hasher[IO]): IO[TokenLockReference] =
    lane match {
      case TransferLane.NativeGl1 => IO.pure(TokenLockReference.empty)
      case TransferLane.CurrencyCl1(metagraphId) =>
        implicit val scopedHasher: Hasher[IO] = currentHasher
        TokenLockReference.emptyCurrency[IO](metagraphId)
    }

  private def verifySourceProof[A: Encoder](
    signed: Signed[A],
    source: Address,
    operationId: String,
    signedValidator: SignedValidator[IO],
    hasher: Hasher[IO]
  )(implicit securityProvider: SecurityProvider[IO]): IO[Either[ProjectionError, (Address, Vector[Address])]] = {
    implicit val scopedHasher: Hasher[IO] = hasher

    signedValidator.validateSignatures(signed).flatMap { signatureValidation =>
      if (signatureValidation.isInvalid) IO.pure(Left(ProductionSignatureValidationRejected(operationId)))
      else
        signedValidator.isSignedExclusivelyBy(signed, source).flatMap { sourceValidation =>
          if (sourceValidation.isInvalid) IO.pure(Left(ProductionSourceOwnerValidationRejected(operationId)))
          else
            signed.proofs.toNonEmptyList.traverse(_.id.toAddress[IO]).map { owners =>
              val resolved = owners.toList.toVector
              if (resolved.forall(_ == source)) Right(owners.head -> resolved)
              else Left(ResolvedProofOwnerMismatch(operationId, source, resolved))
            }
        }
    }
  }
}
