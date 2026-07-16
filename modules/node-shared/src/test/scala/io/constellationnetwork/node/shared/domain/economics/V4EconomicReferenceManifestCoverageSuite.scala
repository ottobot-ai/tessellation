package io.constellationnetwork.node.shared.domain.economics

import scala.collection.immutable.SortedSet

import io.constellationnetwork.node.shared.domain.economics.InputProvenance.FrameworkLane
import io.constellationnetwork.node.shared.domain.economics.ReferenceBalanceScope.Dag
import io.constellationnetwork.node.shared.domain.economics.ReferenceInput.UnsupportedManifestOperation
import io.constellationnetwork.node.shared.domain.economics.ReferenceRejection.UnsupportedOperation
import io.constellationnetwork.node.shared.domain.economics.TransferLane.NativeGl1
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import weaver.FunSuite

object V4EconomicReferenceManifestCoverageSuite extends FunSuite {
  private val supportedTransferIds = Set("ECO-TRANSFER-NATIVE", "ECO-TRANSFER-CURRENCY")

  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def hash(n: Int): Hash = Hash(f"$n%064x")

  private val domain = ReferenceDomain(hash(11), hash(12), hash(13))
  private val context = ReferenceContext(domain, NativeGl1, SortedSet.empty[Address])
  private val alice = address("manifest-alice")
  private val account = ReferenceBalanceAccount(Dag, alice)
  private val base = ReferenceState.initial(Map(account -> BigInt(100))).toOption.get

  test("the bounded positive subset names exactly the two refrozen transfer rows") {
    val manifestIds = V4EconomicGrammarManifest.operations.iterator.map(_.id).toSet

    expect(supportedTransferIds.subsetOf(manifestIds))
      .and(expect(supportedTransferIds.size == 2))
  }

  test("every dynamically discovered non-transfer manifest row fails closed without state mutation") {
    val unsupported = V4EconomicGrammarManifest.operations.filterNot(operation => supportedTransferIds.contains(operation.id))
    val inputs = unsupported.map(operation => UnsupportedManifestOperation(operation.id, FrameworkLane)).toVector
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
      .filterNot(supportedTransferIds)
      .toSet

    expect((supportedTransferIds ++ unsupportedIds) == manifestIds)
      .and(expect((supportedTransferIds intersect unsupportedIds).isEmpty))
  }
}
