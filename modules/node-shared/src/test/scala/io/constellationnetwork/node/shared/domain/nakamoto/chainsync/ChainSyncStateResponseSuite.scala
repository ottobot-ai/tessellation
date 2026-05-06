package io.constellationnetwork.node.shared.domain.nakamoto.chainsync

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.security.hash.Hash

import weaver.SimpleIOSuite

/** Tests for the `ChainSyncStateResponse` ADT (#56.8). Exhaustive-match coverage and `fold` semantics. The ADT itself is purely
  * structural — wire round-tripping happens at the gRPC parse boundary in `ChainSyncManager`, not here.
  */
object ChainSyncStateResponseSuite extends SimpleIOSuite {

  private val sampleHash: Hash = Hash("a" * 64)
  private val sampleBranch: BranchId = BranchId(Hash("b" * 64))

  pureTest("Finalized fold dispatches to onFinalized") {
    val resp: ChainSyncStateResponse[String] = ChainSyncStateResponse.Finalized("payload")
    val result = resp.fold(
      onFinalized = state => s"final=$state",
      onProvisional = (state, branch) => s"prov=$state@$branch",
      onNotFound = h => s"notfound=${h.value}"
    )
    expect.same("final=payload", result)
  }

  pureTest("Provisional fold dispatches to onProvisional with the branch id") {
    val resp: ChainSyncStateResponse[String] = ChainSyncStateResponse.Provisional("payload", sampleBranch)
    val result = resp.fold(
      onFinalized = state => s"final=$state",
      onProvisional = (state, branch) => s"prov=$state@${branch.value.value.take(4)}",
      onNotFound = h => s"notfound=${h.value}"
    )
    expect.same("prov=payload@bbbb", result)
  }

  pureTest("NotFound fold dispatches to onNotFound with the requested hash") {
    val resp: ChainSyncStateResponse[String] = ChainSyncStateResponse.NotFound(sampleHash)
    val result = resp.fold(
      onFinalized = state => s"final=$state",
      onProvisional = (state, branch) => s"prov=$state@$branch",
      onNotFound = h => s"notfound=${h.value.take(4)}"
    )
    expect.same("notfound=aaaa", result)
  }

  pureTest("ADT is sealed — exhaustive match works without default") {
    // This compiles only if the trait is sealed and all variants are covered. The runtime check is incidental;
    // the real test is that `match` without a default arm survives compilation.
    val responses: List[ChainSyncStateResponse[Int]] = List(
      ChainSyncStateResponse.Finalized(1),
      ChainSyncStateResponse.Provisional(2, sampleBranch),
      ChainSyncStateResponse.NotFound(sampleHash)
    )
    val tags = responses.map {
      case ChainSyncStateResponse.Finalized(_)      => "F"
      case ChainSyncStateResponse.Provisional(_, _) => "P"
      case ChainSyncStateResponse.NotFound(_)       => "N"
    }
    expect.same(List("F", "P", "N"), tags)
  }

  pureTest("Provisional carries the branch id verbatim through fold") {
    val resp: ChainSyncStateResponse[Int] = ChainSyncStateResponse.Provisional(99, sampleBranch)
    val branch = resp.fold(
      onFinalized = _ => BranchId(Hash.empty),
      onProvisional = (_, b) => b,
      onNotFound = _ => BranchId(Hash.empty)
    )
    expect.same(sampleBranch, branch)
  }
}
