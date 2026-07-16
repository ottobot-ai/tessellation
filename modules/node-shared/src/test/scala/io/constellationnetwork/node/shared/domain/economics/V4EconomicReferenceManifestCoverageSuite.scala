package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.SortedSet

import io.constellationnetwork.node.shared.domain.economics.InputProvenance.FrameworkLane
import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.Dag
import io.constellationnetwork.node.shared.domain.economics.ReferenceInput._
import io.constellationnetwork.node.shared.domain.economics.ReferenceRejection.UnsupportedOperation
import io.constellationnetwork.node.shared.domain.economics.SupportedReferenceOperationId._
import io.constellationnetwork.node.shared.domain.economics.TransferLane.{CurrencyCl1, NativeGl1}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import weaver.FunSuite

object V4EconomicReferenceManifestCoverageSuite extends FunSuite {
  private val supportedOperationIds = SupportedReferenceOperationId.all.iterator.map(_.value).toSet

  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val domain = ReferenceDomain(hash(11), hash(12), hash(13))
  private val context = ReferenceContext(domain, NativeGl1, SortedSet.empty[Address])
  private val alice = address("manifest-alice")
  private val bob = address("manifest-bob")
  private val metagraphId = address("manifest-metagraph")
  private val account = ReferenceBalanceAccount(Dag, alice)
  private val base = ReferenceState.initial(Map(account -> BigInt(100))).toOption.get

  private def transferInput(lane: TransferLane): Transfer = {
    val preimage = TransferPreimage(
      StructuralReference.genesis,
      TransferAtom(domain, lane, alice, bob, BigInt(1), BigInt(0), salt = 1L)
    )
    Transfer(preimage, StructurallyBoundSourceProof(alice, preimage))
  }

  private val allowSpendInput = {
    val preimage = AllowSpendPreimage(
      StructuralAllowSpendReference.genesis,
      AllowSpendAtom(domain, NativeGl1, alice, bob, BigInt(1), BigInt(0), BigInt(1), Vector(bob))
    )
    AllowSpendCreate(preimage, StructurallyBoundAllowSpendSourceProof(alice, preimage))
  }

  private val tokenLockInput = {
    val preimage = TokenLockPreimage(
      StructuralTokenLockReference.genesis,
      TokenLockAtom(domain, NativeGl1, alice, BigInt(1), BigInt(0), Some(BigInt(1)), None)
    )
    TokenLockCreate(preimage, StructurallyBoundTokenLockSourceProof(alice, preimage))
  }

  test("the bounded positive subset names exactly the four refrozen reference rows") {
    val manifestIds = V4EconomicGrammarManifest.operations.iterator.map(_.id).toSet

    expect(supportedOperationIds.subsetOf(manifestIds))
      .and(expect(supportedOperationIds.size == 4))
  }

  test("each supported manifest ID is owned by exactly one positive input constructor mapping") {
    val positiveMappings = Vector(
      transferInput(NativeGl1).operationId -> NativeTransfer,
      transferInput(CurrencyCl1(metagraphId)).operationId -> CurrencyTransfer,
      allowSpendInput.operationId -> AllowSpendCreation,
      tokenLockInput.operationId -> TokenLockCreation
    )

    expect(positiveMappings.forall { case (actual, expected) => actual == expected })
      .and(expect(positiveMappings.map(_._1.value).toSet == supportedOperationIds))
      .and(expect(positiveMappings.map(_._1).distinct.size == positiveMappings.size))
  }

  test("supported IDs cannot be represented by the fail-closed unsupported constructor") {
    val classifications = supportedOperationIds.iterator
      .map(UnsupportedManifestOperation.fromValue(_, FrameworkLane))
      .toVector

    expect(classifications.forall(_.isLeft))
      .and(expect(classifications.flatMap(_.left.toOption.map(_.value)).toSet == supportedOperationIds))
  }

  test("every dynamically discovered unsupported manifest row fails closed without state mutation") {
    val unsupported = V4EconomicGrammarManifest.operations.filterNot(operation => supportedOperationIds.contains(operation.id))
    val inputs = unsupported.map { operation =>
      UnsupportedManifestOperation.fromValue(operation.id, FrameworkLane).toOption.get
    }.toVector
    val result = V4EconomicReferenceInterpreter.execute(context, base, inputs).toOption.get
    val rejectedIds = result.rejected.map {
      case rejection =>
        rejection.reason match {
          case UnsupportedOperation(operationId, FrameworkLane) => operationId
          case other                                            => sys.error(s"Unexpected manifest rejection: $other")
        }
    }

    expect(rejectedIds == unsupported.map(_.id).toVector)
      .and(expect(result.decisions.size == unsupported.size))
      .and(expect(result.acceptedIds.isEmpty))
      .and(expect(result.finalState == base))
  }

  test("manifest additions cannot disappear between the supported and fail-closed partitions") {
    val manifestIds = V4EconomicGrammarManifest.operations.iterator.map(_.id).toSet
    val unsupportedIds = V4EconomicGrammarManifest.operations.iterator
      .map(_.id)
      .filterNot(supportedOperationIds)
      .toSet

    expect((supportedOperationIds ++ unsupportedIds) == manifestIds)
      .and(expect(supportedOperationIds.intersect(unsupportedIds).isEmpty))
  }
}
